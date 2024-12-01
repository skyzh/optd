use std::sync::Arc;

use async_recursion::async_recursion;
use itertools::Itertools;
use tracing::trace;

use super::memo::MemoPlanNode;
use super::rule_match::match_and_pick_expr;
use super::{optimizer::RuleId, CascadesOptimizer, ExprId, GroupId, Memo, SubGoalId};
use crate::cascades::{
    memo::{Winner, WinnerExpr, WinnerInfo},
    RelNodeContext,
};
use crate::cost::{Cost, Statistics};
use crate::nodes::{ArcPredNode, PlanNode, PlanNodeOrGroup};
use crate::physical_property::PhysicalProperty;
use crate::{nodes::NodeType, rules::RuleMatcher};

struct SearchContext {
    group_id: GroupId,
    subgoal_id: SubGoalId,
    upper_bound: Option<f64>,
}

pub struct TaskContext<'a, T: NodeType, M: Memo<T>> {
    optimizer: &'a mut CascadesOptimizer<T, M>,
    steps: usize,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TaskDesc {
    OptimizeExpr(ExprId, SubGoalId, GroupId),
    OptimizeInput(ExprId, SubGoalId, GroupId),
}

impl<'a, T: NodeType, M: Memo<T>> TaskContext<'a, T, M> {
    pub fn new(optimizer: &'a mut CascadesOptimizer<T, M>) -> Self {
        Self {
            optimizer,
            steps: 0,
        }
    }

    pub async fn fire_optimize(&mut self, group_id: GroupId, subgoal_id: SubGoalId) {
        self.optimize_group(SearchContext {
            group_id,
            subgoal_id,
            upper_bound: None,
        })
        .await;
    }

    #[async_recursion]
    async fn optimize_group(&mut self, ctx: SearchContext) {
        self.steps += 1;
        let SearchContext {
            group_id,
            subgoal_id,
            ..
        } = ctx;
        trace!(event = "task_begin", task = "optimize_group", group_id = %group_id, subgoal_id = %subgoal_id);

        if self.optimizer.is_group_explored(group_id, subgoal_id) {
            trace!(
                event = "task_finish",
                task = "optimize_group",
                group_id = %group_id,
                subgoal_id = %subgoal_id,
                outcome = "already explored, skipping",
            );
            return;
        }
        self.optimizer.mark_group_explored(group_id, subgoal_id);

        // The Columbia optimizer will stop if we have a full winner, but given that we implement
        // 2-stage optimization, we will continue to optimize the group even if we have a full winner.

        let exprs = self.optimizer.get_all_exprs_in_group(group_id);
        // First, optimize all physical expressions
        for &expr_id in &exprs {
            let expr = self.optimizer.get_expr_memoed(expr_id);
            if !expr.typ.is_logical() {
                self.optimize_input(
                    SearchContext {
                        group_id,
                        subgoal_id,
                        upper_bound: ctx.upper_bound,
                    },
                    expr_id,
                )
                .await;
            }
        }
        // Then, optimize all logical expressions
        for &expr_id in &exprs {
            let typ = self.optimizer.get_expr_memoed(expr_id).typ.clone();
            if typ.is_logical() {
                self.optimize_expr(
                    SearchContext {
                        group_id,
                        subgoal_id,
                        upper_bound: ctx.upper_bound,
                    },
                    expr_id,
                    false,
                )
                .await
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %group_id, subgoal_id=%subgoal_id);
    }

    #[async_recursion]
    async fn optimize_expr(&mut self, ctx: SearchContext, expr_id: ExprId, exploring: bool) {
        self.steps += 1;
        let SearchContext {
            group_id,
            subgoal_id,
            ..
        } = ctx;
        let desc = TaskDesc::OptimizeExpr(expr_id, subgoal_id, group_id);
        if self.optimizer.has_task_started(&desc) {
            trace!(event = "task_skip", task = "optimize_expr", expr_id = %expr_id, subgoal_id = %subgoal_id);
            return;
        }
        self.optimizer.mark_task_start(&desc);

        fn top_matches<T: NodeType>(matcher: &RuleMatcher<T>, match_typ: T) -> bool {
            match matcher {
                RuleMatcher::MatchNode { typ, .. } => typ == &match_typ,
                RuleMatcher::MatchDiscriminant {
                    typ_discriminant, ..
                } => std::mem::discriminant(&match_typ) == *typ_discriminant,
                _ => panic!("IR should have root node of match"),
            }
        }
        let expr = self.optimizer.get_expr_memoed(expr_id);
        assert!(expr.typ.is_logical());
        trace!(event = "task_begin", task = "optimize_expr", expr_id = %expr_id, subgoal_id = %subgoal_id, expr = %expr);
        for (rule_id, rule) in self.optimizer.rules().iter().enumerate() {
            if self.optimizer.is_rule_fired(expr_id, rule_id) {
                continue;
            }
            // Skip impl rules when exploring
            if exploring && rule.is_impl_rule() {
                continue;
            }
            // Skip transformation rules when budget is used
            if self.optimizer.ctx.budget_used && !rule.is_impl_rule() {
                continue;
            }
            if top_matches(rule.matcher(), expr.typ.clone()) {
                for &input_group_id in &expr.children {
                    let child_subgoal_id = self.optimizer.create_or_get_subgroup(
                        input_group_id,
                        self.optimizer
                            .memo()
                            .get_physical_property_builders()
                            .default_many()
                            .into(),
                    );
                    self.explore_group(SearchContext {
                        group_id: input_group_id,
                        subgoal_id: child_subgoal_id,
                        upper_bound: ctx.upper_bound,
                    })
                    .await;
                }
                self.apply_rule(
                    SearchContext {
                        group_id,
                        subgoal_id,
                        upper_bound: ctx.upper_bound,
                    },
                    rule_id,
                    expr_id,
                    exploring,
                )
                .await;
            }
        }

        let predicates = expr
            .predicates
            .iter()
            .map(|x| self.optimizer.get_pred(*x))
            .collect_vec();
        let goal = self
            .optimizer
            .memo()
            .get_subgroup_goal(group_id, subgoal_id);
        for (idx, prop) in goal.iter().enumerate() {
            // See if we can turn any prop into a search goal
            let Some(new_prop) = self.optimizer.memo().get_physical_property_builders().0[idx]
                .search_goal_any(expr.typ.clone(), &predicates, prop.as_ref())
            else {
                continue;
            };
            let mut new_goal = goal.iter().map(|x| x.to_boxed()).collect_vec();
            // Change the requirement on that property
            new_goal[idx] = new_prop;
            assert_eq!(expr.children.len(), 1);
            let child_group_id = expr.children[0];
            let new_goal_id = self
                .optimizer
                .create_or_get_subgroup(child_group_id, new_goal.into());
            trace!(event = "new_goal", task = "optimize_expr", expr_id = %expr_id, subgoal_id = %subgoal_id, expr = %expr, child_group_id = %child_group_id, new_goal_id = %new_goal_id);
            self.optimize_group(SearchContext {
                group_id: child_group_id,
                subgoal_id: new_goal_id,
                upper_bound: ctx.upper_bound,
            })
            .await;
            if let Some(winner) = self
                .optimizer
                .get_group_winner(child_group_id, new_goal_id)
                .as_full_winner()
            {
                let winner_info = WinnerInfo {
                    expr_id: WinnerExpr::Propagate {
                        group_id: child_group_id,
                        subgoal_id: new_goal_id,
                    },
                    ..winner.clone()
                };
                self.update_winner_if_better(group_id, subgoal_id, winner_info);
            }
        }
        self.optimizer.mark_task_end(&desc);
        trace!(event = "task_end", task = "optimize_expr", expr_id = %expr_id, subgoal_id = %subgoal_id, expr = %expr);
    }

    #[async_recursion]
    async fn explore_group(&mut self, ctx: SearchContext) {
        self.steps += 1;
        let SearchContext {
            group_id,
            subgoal_id,
            ..
        } = ctx;
        trace!(event = "task_begin", task = "explore_group", group_id = %group_id, subgoal_id = %subgoal_id);
        let exprs = self.optimizer.get_all_exprs_in_group(group_id);
        for expr in exprs {
            let typ = self.optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                self.optimize_expr(
                    SearchContext {
                        group_id,
                        subgoal_id,
                        upper_bound: ctx.upper_bound,
                    },
                    expr,
                    true,
                )
                .await;
            }
        }
        trace!(
            event = "task_finish",
            task = "explore_group",
            group_id = %group_id,
            subgoal_id = %subgoal_id,
            outcome = "expanded group"
        );
    }

    #[async_recursion]
    async fn apply_rule(
        &mut self,
        ctx: SearchContext,
        rule_id: RuleId,
        expr_id: ExprId,
        exploring: bool,
    ) {
        self.steps += 1;
        let SearchContext {
            group_id,
            subgoal_id,
            ..
        } = ctx;
        trace!(event = "task_begin", task = "apply_rule", expr_id = %expr_id, subgoal_id = %subgoal_id, exploring = %exploring);
        if self.optimizer.is_rule_fired(expr_id, rule_id) {
            trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, subgoal_id = %subgoal_id, exploring = %exploring, outcome = "rule already fired");
            return;
        }

        if self.optimizer.is_rule_disabled(rule_id) {
            trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, subgoal_id = %subgoal_id, exploring = %exploring, outcome = "rule disabled");
            return;
        }

        self.optimizer.mark_rule_fired(expr_id, rule_id);

        let rule = self.optimizer.rules()[rule_id].clone();

        let binding_exprs = match_and_pick_expr(rule.matcher(), expr_id, self.optimizer);
        if binding_exprs.len() >= 100 {
            tracing::warn!(
                event = "rule_application",
                task = "apply_rule",
                expr_id = %expr_id,
                rule_id = %rule_id,
                outcome = "too_many_bindings",
                num_bindings = %binding_exprs.len()
            );
        }
        for binding in binding_exprs {
            if !self.optimizer.ctx.budget_used {
                let plan_space = self.optimizer.memo().estimated_plan_space();
                let step = self.steps;
                if let Some(partial_explore_space) = self.optimizer.prop.partial_explore_space {
                    if plan_space > partial_explore_space {
                        println!(
                            "plan space size budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.optimizer.ctx.budget_used = true;
                        if self.optimizer.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                }
                if let Some(partial_explore_iter) = self.optimizer.prop.partial_explore_iter {
                    if step > partial_explore_iter {
                        println!(
                            "iter budget used, not applying logical rules any more. current iter: {}",
                            step
                        );
                        self.optimizer.ctx.budget_used = true;
                        if self.optimizer.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                }
            }

            if self.optimizer.ctx.budget_used && !rule.is_impl_rule() {
                continue;
            }

            trace!(event = "before_apply_rule", task = "apply_rule", input_binding=%binding);
            let applied = rule.apply(self.optimizer, binding);
            for expr in applied {
                trace!(event = "after_apply_rule", task = "apply_rule", output_binding=%expr);
                // TODO: remove clone in the below line
                if let Some(expr_id) = self.optimizer.add_expr_to_group(expr.clone(), group_id) {
                    let typ = expr.unwrap_typ();
                    if typ.is_logical() {
                        self.optimize_expr(
                            SearchContext {
                                group_id,
                                subgoal_id,
                                upper_bound: ctx.upper_bound,
                            },
                            expr_id,
                            exploring,
                        )
                        .await;
                    } else {
                        self.optimize_input(
                            SearchContext {
                                group_id,
                                subgoal_id,
                                upper_bound: ctx.upper_bound,
                            },
                            expr_id,
                        )
                        .await;
                    }
                    trace!(event = "apply_rule", expr_id = %expr_id, rule_id = %rule_id, new_expr_id = %expr_id);
                } else {
                    trace!(event = "apply_rule", expr_id = %expr_id, rule_id = %rule_id, "triggered group merge");
                }
            }
        }
        trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, rule_id = %rule_id);
    }

    fn update_winner_if_better(
        &mut self,
        group_id: GroupId,
        subgoal_id: SubGoalId,
        proposed_winner: WinnerInfo,
    ) {
        let mut update_cost = false;
        let current_winner = self.optimizer.get_group_winner(group_id, subgoal_id);
        if let Some(winner) = current_winner.as_full_winner() {
            if winner.total_weighted_cost > proposed_winner.total_weighted_cost {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            tracing::trace!(
                event = "update_winner",
                task = "optimize_inputs",
                subgoal_id = %subgoal_id,
                expr_id = ?proposed_winner.expr_id,
                total_weighted_cost = %proposed_winner.total_weighted_cost,
                operation_weighted_cost = %proposed_winner.operation_weighted_cost,
            );
            self.optimizer
                .update_group_winner(group_id, subgoal_id, Winner::Full(proposed_winner));
        }
    }

    #[allow(clippy::type_complexity)]
    fn gather_statistics_and_costs(
        &mut self,
        group_id: GroupId,
        expr_id: ExprId,
        expr: &MemoPlanNode<T>,
        children_subgoal_ids: &[SubGoalId],
        predicates: &[ArcPredNode<T>],
    ) -> (
        Vec<Option<Arc<Statistics>>>,
        Cost,
        Cost,
        Vec<Option<Arc<[Box<dyn PhysicalProperty>]>>>,
    ) {
        let context = RelNodeContext {
            expr_id,
            group_id,
            children_group_ids: expr.children.clone(),
        };
        let mut input_stats = Vec::with_capacity(expr.children.len());
        let mut input_props = Vec::with_capacity(expr.children.len());
        let mut input_cost = Vec::with_capacity(expr.children.len());
        let cost = self.optimizer.cost();
        #[allow(clippy::needless_range_loop)]
        for idx in 0..expr.children.len() {
            let winner = self
                .optimizer
                .get_group_winner(expr.children[idx], children_subgoal_ids[idx])
                .as_full_winner();
            let stats = winner.map(|x| x.statistics.clone());
            input_stats.push(stats.clone());
            input_props.push(winner.map(|x| x.derived_physical_properties.clone()));
            input_cost.push(winner.map(|x| x.total_cost.clone()).unwrap_or_else(|| {
                // TODO: cache the result so that we don't need to compute each time
                match stats {
                    Some(stats) => cost.compute_lower_bound_cost(
                        context.clone(),
                        stats.as_ref(),
                        self.optimizer,
                    ),
                    None => cost.zero(),
                }
            }));
        }
        let input_stats_ref = input_stats
            .iter()
            .map(|x| x.as_ref().map(|y| y.as_ref()))
            .collect_vec();
        let operation_cost = cost.compute_operation_cost(
            &expr.typ,
            predicates,
            &input_stats_ref,
            context.clone(),
            self.optimizer,
        );
        let total_cost = cost.sum(&operation_cost, &input_cost);
        (input_stats, total_cost, operation_cost, input_props)
    }

    #[async_recursion]
    async fn optimize_input(&mut self, ctx: SearchContext, expr_id: ExprId) {
        self.steps += 1;
        let SearchContext {
            group_id,
            subgoal_id,
            ..
        } = ctx;
        let desc = TaskDesc::OptimizeInput(expr_id, subgoal_id, group_id);
        if self.optimizer.has_task_started(&desc) {
            trace!(event = "task_skip", task = "optimize_input", subgoal_id = %subgoal_id, expr_id = %expr_id);
            return;
        }
        self.optimizer.mark_task_start(&desc);

        trace!(event = "task_begin", task = "optimize_inputs", subgoal_id = %subgoal_id, expr_id = %expr_id);

        // TODO: assert this plan node satisfies subgoal

        let expr = self.optimizer.get_expr_memoed(expr_id);
        let cost = self.optimizer.cost();

        let predicates = expr
            .predicates
            .iter()
            .map(|pred_id| self.optimizer.get_pred(*pred_id))
            .collect_vec();

        let goal = self
            .optimizer
            .memo()
            .get_subgroup_goal(group_id, subgoal_id);

        // The upper bound of the search is the minimum of cost of the current best plan AND the
        // upper bound of the context.
        let winner_upper_bound = self
            .optimizer
            .memo()
            .get_group_winner(group_id, subgoal_id)
            .as_full_winner()
            .map(|winner| winner.total_weighted_cost);

        let upper_bound = match (ctx.upper_bound, winner_upper_bound) {
            (Some(ub), Some(wub)) => Some(ub.min(wub)),
            (Some(ub), None) => Some(ub),
            (None, Some(wub)) => Some(wub),
            (None, None) => None,
        };

        if !self
            .optimizer
            .memo()
            .get_physical_property_builders()
            .can_passthrough_any_many(expr.typ.clone(), &predicates, &goal)
        {
            // The expression cannot passthrough all required physical properties,
            // so we remove one goal at a time and re-optimize
            for (idx, _) in goal.iter().enumerate() {
                let mut new_goal = goal.iter().map(|x| x.to_boxed()).collect_vec();
                let builder = &self.optimizer.memo().get_physical_property_builders().0[idx];
                let default_prop = builder.default_any();
                if builder.exactly_eq_any(&*new_goal[idx], &*default_prop) {
                    continue;
                }
                // Remove the requirement on that property
                new_goal[idx] = default_prop;
                let new_goal_id = self
                    .optimizer
                    .create_or_get_subgroup(group_id, new_goal.into());
                self.optimize_input(
                    SearchContext {
                        group_id,
                        subgoal_id: new_goal_id,
                        upper_bound,
                    },
                    expr_id,
                )
                .await;
                if let Some(child_winner) = self
                    .optimizer
                    .get_group_winner(group_id, new_goal_id)
                    .as_full_winner()
                {
                    let child_winner = child_winner.clone();
                    let builder = &self.optimizer.memo().get_physical_property_builders().0[idx];
                    let (e_typ, e_preds) = builder.enforce_any(goal[idx].as_ref());
                    let enforcer_node = Arc::new(PlanNode {
                        typ: e_typ.clone(),
                        predicates: e_preds.clone(),
                        children: vec![PlanNodeOrGroup::Group(group_id)],
                    });
                    // We will/might create a new group for the enforcer expr, but it won't be explored
                    // as part of the cascades process. In the future, we should eliminate/distinguish
                    // such groups.
                    let (_, enforcer_expr_id) = self.optimizer.add_new_expr(enforcer_node);
                    // Compute the cost of the enforcer node
                    let cost_model = self.optimizer.cost();
                    let operation_cost = cost_model.compute_operation_cost(
                        &e_typ,
                        &e_preds,
                        &[Some(child_winner.statistics.as_ref())],
                        RelNodeContext {
                            group_id,
                            // TODO: does the cost model rely on something that will make this incorrect?a
                            expr_id: enforcer_expr_id,
                            children_group_ids: vec![group_id],
                        },
                        self.optimizer,
                    );
                    let total_cost =
                        cost_model.sum(&operation_cost, &[child_winner.total_cost.clone()]);
                    let derived_physical_properties = self
                        .optimizer
                        .memo()
                        .get_physical_property_builders()
                        .derive_many(
                            e_typ.clone(),
                            &e_preds,
                            &[child_winner.derived_physical_properties.clone()],
                            1,
                        );
                    // Assume enforcer doesn't change statistics
                    let winner_info = WinnerInfo {
                        expr_id: WinnerExpr::Enforcer {
                            expr_id: enforcer_expr_id,
                            child_goal_id: new_goal_id,
                        },
                        total_weighted_cost: cost.weighted_cost(&total_cost),
                        operation_weighted_cost: cost.weighted_cost(&operation_cost),
                        total_cost,
                        operation_cost,
                        statistics: child_winner.statistics.clone(),
                        derived_physical_properties: derived_physical_properties.into(),
                    };
                    self.update_winner_if_better(group_id, subgoal_id, winner_info);
                }
            }
            self.optimizer.mark_task_end(&desc);
            return;
        }

        let child_group_passthrough_properties = self
            .optimizer
            .memo()
            .get_physical_property_builders()
            .passthrough_many(expr.typ.clone(), &predicates, &goal, expr.children.len());

        assert_eq!(
            child_group_passthrough_properties.len(),
            expr.children.len()
        );

        let children_subgoal_ids = expr
            .children
            .iter()
            .zip(child_group_passthrough_properties)
            .map(|(child_group_id, required_props)| {
                self.optimizer
                    .create_or_get_subgroup(*child_group_id, required_props.into())
            })
            .collect_vec();

        for (input_group_idx, _) in expr.children.iter().enumerate() {
            // Before optimizing each of the child, infer a current lower bound cost
            let (_, total_cost, _, _) = self.gather_statistics_and_costs(
                group_id,
                expr_id,
                &expr,
                &children_subgoal_ids,
                &predicates,
            );

            let child_upper_bound = if !self.optimizer.prop.disable_pruning {
                let cost_so_far = cost.weighted_cost(&total_cost);
                // TODO: also adds up lower-bound cost
                trace!(
                    event = "compute_cost",
                    task = "optimize_inputs",
                    expr_id = %expr_id,
                    weighted_cost_so_far = cost_so_far,
                    upper_bound = ?upper_bound,
                    current_processing = %input_group_idx,
                    total_child_groups = %expr.children.len());
                if let Some(upper_bound) = upper_bound {
                    if upper_bound <= cost_so_far {
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %expr_id, result = "pruned");
                        self.optimizer.mark_task_end(&desc);
                        return;
                    }
                    Some(upper_bound - cost_so_far)
                } else {
                    None
                }
            } else {
                None
            };

            let child_subgoal_id = children_subgoal_ids[input_group_idx];
            let child_group_id = expr.children[input_group_idx];
            let child_group_winner = self
                .optimizer
                .get_group_winner(child_group_id, child_subgoal_id);
            if !child_group_winner.has_full_winner() {
                self.optimize_group(SearchContext {
                    group_id: child_group_id,
                    subgoal_id: child_subgoal_id,
                    upper_bound: child_upper_bound,
                })
                .await;
                if let Winner::Unknown = self
                    .optimizer
                    .get_group_winner(child_group_id, child_subgoal_id)
                {
                    self.optimizer.mark_task_end(&desc);
                    trace!(event = "task_finish", task = "optimize_inputs", expr_id = %expr_id, result = "impossible");
                    return;
                }
            }
        }

        // Compute everything again
        let (input_stats, total_cost, operation_cost, child_phys_props) = self
            .gather_statistics_and_costs(
                group_id,
                expr_id,
                &expr,
                &children_subgoal_ids,
                &predicates,
            );
        let input_stats_ref = input_stats
            .iter()
            .map(|x| {
                x.as_ref()
                    .expect("stats should be available for full winners")
                    .as_ref()
            })
            .collect_vec();
        let child_phys_props = child_phys_props
            .iter()
            .map(|x| x.as_ref().expect("phys props should be available").as_ref())
            .collect_vec();
        let statistics = Arc::new(cost.derive_statistics(
            &expr.typ,
            &predicates,
            &input_stats_ref,
            RelNodeContext {
                expr_id,
                group_id,
                children_group_ids: expr.children.clone(),
            },
            self.optimizer,
        ));
        let derived_physical_properties = self
            .optimizer
            .memo()
            .get_physical_property_builders()
            .derive_many(
                expr.typ.clone(),
                &predicates,
                &child_phys_props,
                expr.children.len(),
            );
        let proposed_winner = WinnerInfo {
            expr_id: WinnerExpr::Expr { expr_id },
            total_cost: total_cost.clone(),
            operation_cost: operation_cost.clone(),
            total_weighted_cost: cost.weighted_cost(&total_cost),
            operation_weighted_cost: cost.weighted_cost(&operation_cost),
            statistics,
            derived_physical_properties: derived_physical_properties.into(),
        };
        self.update_winner_if_better(group_id, subgoal_id, proposed_winner);
        trace!(event = "task_finish", task = "optimize_inputs", subgoal_id = %subgoal_id, expr_id = %expr_id, result = "resolved");
        self.optimizer.mark_task_end(&desc);
    }
}

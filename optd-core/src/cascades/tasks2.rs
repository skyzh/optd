use async_recursion::async_recursion;
use itertools::Itertools;
use tracing::trace;

use super::rule_match::match_and_pick_expr;
use super::{optimizer::RuleId, CascadesOptimizer, ExprId, GroupId, Memo, SubGoalId};
use crate::cascades::{
    memo::{Winner, WinnerExpr, WinnerInfo},
    RelNodeContext,
};
use crate::{nodes::NodeType, rules::RuleMatcher};

pub struct TaskContext<'a, T: NodeType, M: Memo<T>> {
    optimizer: &'a mut CascadesOptimizer<T, M>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TaskDesc {
    OptimizeExpr(ExprId, SubGoalId, GroupId),
    OptimizeInput(ExprId, SubGoalId, GroupId),
}

impl<'a, T: NodeType, M: Memo<T>> TaskContext<'a, T, M> {
    pub fn new(optimizer: &'a mut CascadesOptimizer<T, M>) -> Self {
        Self { optimizer }
    }

    #[async_recursion]
    pub async fn optimize_group(&mut self, group_id: GroupId, subgoal_id: SubGoalId) {
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

        let winner = self.optimizer.get_group_winner(group_id, subgoal_id);
        if winner.has_decided() {
            trace!(
                event = "task_finish",
                task = "optimize_group",
                outcome = "winner decided"
            );
            return;
        }
        let exprs = self.optimizer.get_all_exprs_in_group(group_id);
        // First, optimize all physical expressions
        for &expr_id in &exprs {
            let expr = self.optimizer.get_expr_memoed(expr_id);
            if !expr.typ.is_logical() {
                self.optimize_input(group_id, expr_id, subgoal_id).await;
            }
        }
        // Then, optimize all logical expressions
        for &expr_id in &exprs {
            let typ = self.optimizer.get_expr_memoed(expr_id).typ.clone();
            if typ.is_logical() {
                self.optimize_expr(expr_id, subgoal_id, group_id, false)
                    .await
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %group_id, subgoal_id=%subgoal_id);
    }

    #[async_recursion]
    async fn optimize_expr(
        &mut self,
        expr_id: ExprId,
        subgoal_id: SubGoalId,
        group_id: GroupId,
        exploring: bool,
    ) {
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
                    self.explore_group(input_group_id, child_subgoal_id).await;
                }
                self.apply_rule(rule_id, expr_id, group_id, subgoal_id, exploring)
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
            self.explore_group(child_group_id, new_goal_id).await;
            if let Some(winner) = self
                .optimizer
                .get_group_winner(child_group_id, new_goal_id)
                .as_full_winner()
            {
                let current_winner = self.optimizer.get_group_winner(group_id, subgoal_id);
                if let Some(current_winner) = current_winner.as_full_winner() {
                    if current_winner.total_weighted_cost > winner.total_weighted_cost {
                        self.optimizer.update_group_winner(
                            group_id,
                            subgoal_id,
                            Winner::Full(winner.clone()),
                        );
                    }
                } else {
                    // The winner is an expression from another group
                    self.optimizer.update_group_winner(
                        group_id,
                        subgoal_id,
                        Winner::Full(winner.clone()),
                    );
                }
            }
        }
        self.optimizer.mark_task_end(&desc);
        trace!(event = "task_end", task = "optimize_expr", expr_id = %expr_id, subgoal_id = %subgoal_id, expr = %expr);
    }

    #[async_recursion]
    async fn explore_group(&mut self, group_id: GroupId, subgoal_id: SubGoalId) {
        trace!(event = "task_begin", task = "explore_group", group_id = %group_id, subgoal_id = %subgoal_id);
        let exprs = self.optimizer.get_all_exprs_in_group(group_id);
        for expr in exprs {
            let typ = self.optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                self.optimize_expr(expr, subgoal_id, group_id, true).await;
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
        rule_id: RuleId,
        expr_id: ExprId,
        group_id: GroupId,
        subgoal_id: SubGoalId,
        exploring: bool,
    ) {
        trace!(event = "task_begin", task = "apply_rule", expr_id = %expr_id, subgoal_id = %subgoal_id, exploring = %exploring);
        if self.optimizer.is_rule_fired(expr_id, rule_id) {
            trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, subgoal_id = %subgoal_id, exploring = %exploring, outcome = "rule already fired");
            return;
        }

        if self.optimizer.is_rule_disabled(rule_id) {
            self.optimizer.mark_rule_fired(expr_id, rule_id);
            trace!(event = "task_begin", task = "apply_rule", expr_id = %expr_id, subgoal_id = %subgoal_id, exploring = %exploring, outcome = "rule disabled");
            return;
        }

        self.optimizer.mark_rule_fired(expr_id, rule_id);

        let rule = self.optimizer.rules()[rule_id].clone();

        let binding_exprs = match_and_pick_expr(rule.matcher(), expr_id, self.optimizer);
        for binding in binding_exprs {
            trace!(event = "before_apply_rule", task = "apply_rule", input_binding=%binding);
            let applied = rule.apply(self.optimizer, binding);
            for expr in applied {
                trace!(event = "after_apply_rule", task = "apply_rule", output_binding=%expr);

                if !self.optimizer.ctx.budget_used {
                    let plan_space = self.optimizer.memo().estimated_plan_space();
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
                }

                // TODO: remove clone in the below line
                if let Some(expr_id) = self.optimizer.add_expr_to_group(expr.clone(), group_id) {
                    let typ = expr.unwrap_typ();
                    if typ.is_logical() {
                        self.optimize_expr(expr_id, subgoal_id, group_id, exploring)
                            .await;
                    } else {
                        self.optimize_input(group_id, expr_id, subgoal_id).await;
                    }
                    trace!(event = "apply_rule", expr_id = %expr_id, rule_id = %rule_id, new_expr_id = %expr_id);
                } else {
                    trace!(event = "apply_rule", expr_id = %expr_id, rule_id = %rule_id, "triggered group merge");
                }
            }
        }
        trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, rule_id = %rule_id);
    }

    #[async_recursion]
    async fn optimize_input(&mut self, group_id: GroupId, expr_id: ExprId, subgoal_id: SubGoalId) {
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
                let default_prop =
                    self.optimizer.memo().get_physical_property_builders().0[idx].default_any();
                if self.optimizer.memo().get_physical_property_builders().0[idx]
                    .exactly_eq_any(&*new_goal[idx], &*default_prop)
                {
                    continue;
                }
                // Remove the requirement on that property
                new_goal[idx] = default_prop;
                let new_goal_id = self
                    .optimizer
                    .create_or_get_subgroup(group_id, new_goal.into());
                self.optimize_input(group_id, expr_id, new_goal_id).await;
                // TODO: update winner
            }
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
                let child_subgoal_id = self
                    .optimizer
                    .create_or_get_subgroup(*child_group_id, required_props.into());
                (*child_group_id, child_subgoal_id)
            })
            .collect_vec();
        let children_group_ids = children_subgoal_ids
            .iter()
            .map(|(group_id, _)| *group_id)
            .collect_vec();

        for (input_group_idx, _) in expr.children.iter().enumerate() {
            // Before optimizing each of the child, infer a current lower bound cost
            let context = RelNodeContext {
                expr_id: expr_id,
                group_id,
                children_group_ids: children_group_ids.clone(),
            };
            let input_statistics = children_subgoal_ids
                .iter()
                .map(|&(group_id, subgoal_id)| {
                    self.optimizer
                        .get_group_winner(group_id, subgoal_id)
                        .as_full_winner()
                        .map(|x| x.statistics.clone())
                })
                .collect::<Vec<_>>();
            let input_statistics_ref = input_statistics
                .iter()
                .map(|x| x.as_deref())
                .collect::<Vec<_>>();
            let input_cost = children_subgoal_ids
                .iter()
                .map(|&(group_id, subgoal_id)| {
                    self.optimizer
                        .get_group_winner(group_id, subgoal_id)
                        .as_full_winner()
                        .map(|x| x.total_cost.clone())
                        .unwrap_or_else(|| cost.zero())
                })
                .collect::<Vec<_>>();
            let operation_cost = cost.compute_operation_cost(
                &expr.typ,
                &predicates,
                &input_statistics_ref,
                context.clone(),
                self.optimizer,
            );
            let total_cost = cost.sum(&operation_cost, &input_cost);

            if !self.optimizer.prop.disable_pruning {
                let winner = self.optimizer.get_group_winner(group_id, subgoal_id);
                fn trace_fmt(winner: &Winner) -> String {
                    match winner {
                        Winner::Full(winner) => winner.total_weighted_cost.to_string(),
                        Winner::Impossible => "impossible".to_string(),
                        Winner::Unknown => "unknown".to_string(),
                    }
                }
                trace!(
                    event = "compute_cost",
                    task = "optimize_inputs",
                    expr_id = %expr_id,
                    weighted_cost_so_far = cost.weighted_cost(&total_cost),
                    winner_weighted_cost = %trace_fmt(winner),
                    current_processing = %input_group_idx,
                    total_child_groups = %children_group_ids.len());
                if let Some(winner) = winner.as_full_winner() {
                    let cost_so_far = cost.weighted_cost(&total_cost);
                    if winner.total_weighted_cost <= cost_so_far {
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %expr_id, result = "pruned");
                        self.optimizer.mark_task_end(&desc);
                        return;
                    }
                }
            }

            let (child_group_id, child_subgoal_id) = children_subgoal_ids[input_group_idx];
            let child_group_winner = self
                .optimizer
                .get_group_winner(child_group_id, child_subgoal_id);
            if !child_group_winner.has_full_winner() {
                self.optimize_group(child_group_id, child_subgoal_id).await;
                if let Winner::Impossible | Winner::Unknown = self
                    .optimizer
                    .get_group_winner(child_group_id, child_subgoal_id)
                {
                    self.optimizer
                        .update_group_winner(group_id, subgoal_id, Winner::Impossible);
                    self.optimizer.mark_task_end(&desc);
                    trace!(event = "task_finish", task = "optimize_inputs", expr_id = %expr_id, result = "impossible");
                    return;
                }
            }
        }

        // Compute everything again
        let context = RelNodeContext {
            expr_id: expr_id,
            group_id,
            children_group_ids: children_group_ids.clone(),
        };
        let input_statistics = children_subgoal_ids
            .iter()
            .map(|&(group_id, subgoal_id)| {
                Some(
                    self.optimizer
                        .get_group_winner(group_id, subgoal_id)
                        .as_full_winner()
                        .unwrap()
                        .statistics
                        .clone(),
                )
            })
            .collect::<Vec<_>>();
        let input_statistics_ref = input_statistics
            .iter()
            .map(|x| x.as_ref().map(|y| y.as_ref()))
            .collect::<Vec<_>>();
        let input_cost = children_subgoal_ids
            .iter()
            .map(|&(group_id, subgoal_id)| {
                self.optimizer
                    .get_group_winner(group_id, subgoal_id)
                    .as_full_winner()
                    .unwrap()
                    .total_cost
                    .clone()
            })
            .collect::<Vec<_>>();
        let operation_cost = cost.compute_operation_cost(
            &expr.typ,
            &predicates,
            &input_statistics_ref,
            context.clone(),
            self.optimizer,
        );
        let total_cost = cost.sum(&operation_cost, &input_cost);

        // We've optimized all the inputs and get the winner of the input groups
        let current_winner = self.optimizer.get_group_winner(group_id, subgoal_id);
        let operation_weighted_cost = cost.weighted_cost(&operation_cost);
        let total_weighted_cost = cost.weighted_cost(&total_cost);
        let mut update_cost = false;
        if let Some(winner) = current_winner.as_full_winner() {
            if winner.total_weighted_cost > total_weighted_cost {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }

        if update_cost {
            let statistics = cost.derive_statistics(
                &expr.typ,
                &predicates,
                &input_statistics_ref
                    .iter()
                    .map(|x| x.expect("child winner should always have statistics?"))
                    .collect::<Vec<_>>(),
                RelNodeContext {
                    group_id,
                    expr_id,
                    children_group_ids: expr.children.clone(),
                },
                self.optimizer,
            );
            self.optimizer.update_group_winner(
                group_id,
                subgoal_id,
                Winner::Full(WinnerInfo {
                    expr_id: WinnerExpr::Expr { expr_id },
                    total_weighted_cost,
                    operation_weighted_cost,
                    total_cost,
                    operation_cost,
                    statistics: statistics.into(),
                }),
            );
            trace!(event = "task_finish", task = "optimize_inputs", subgoal_id = %subgoal_id, expr_id = %expr_id, result = "winner");
        } else {
            trace!(event = "task_finish", task = "optimize_inputs", subgoal_id = %subgoal_id, expr_id = %expr_id, result = "optimized");
        }
        self.optimizer.mark_task_end(&desc);
    }
}

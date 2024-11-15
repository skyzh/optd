// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use anyhow::Result;
use itertools::Itertools;
use tracing::trace;

use super::Task;
use crate::cascades::memo::{Winner, WinnerExpr, WinnerInfo};
use crate::cascades::optimizer::ExprId;
use crate::cascades::tasks::OptimizeGroupTask;
use crate::cascades::{CascadesOptimizer, GroupId, Memo, RelNodeContext, SubGroupId};
use crate::cost::{Cost, Statistics};
use crate::nodes::NodeType;

#[derive(Debug, Clone)]
struct ContinueTask {
    next_group_idx: usize,
    return_from_optimize_group: bool,
    return_from_inverse_enforce: bool,
    children_subgroup_ids: Arc<[(GroupId, SubGroupId)]>,
    children_group_ids: Arc<[GroupId]>,
}

struct ContinueTaskDisplay<'a>(&'a Option<ContinueTask>);

impl std::fmt::Display for ContinueTaskDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(x) => {
                if x.return_from_inverse_enforce {
                    write!(f, "return_enforce,next_group_idx={}", x.next_group_idx)
                } else if x.return_from_optimize_group {
                    write!(f, "return,next_group_idx={}", x.next_group_idx)
                } else {
                    write!(f, "enter,next_group_idx={}", x.next_group_idx)
                }
            }
            None => write!(f, "none"),
        }
    }
}

pub struct OptimizeInputsTask {
    group_id: GroupId,
    subgroup_id: SubGroupId,
    expr_id: ExprId,
    continue_from: Option<ContinueTask>,
    pruning: bool,
}

impl OptimizeInputsTask {
    pub fn new(group_id: GroupId, expr_id: ExprId, pruning: bool, subgroup_id: SubGroupId) -> Self {
        Self {
            group_id,
            subgroup_id,
            expr_id,
            continue_from: None,
            pruning,
        }
    }

    fn continue_from(&self, cont: ContinueTask, pruning: bool) -> Self {
        Self {
            group_id: self.group_id,
            subgroup_id: self.subgroup_id,
            expr_id: self.expr_id,
            continue_from: Some(cont),
            pruning,
        }
    }

    fn update_winner_impossible<T: NodeType, M: Memo<T>>(
        &self,
        optimizer: &mut CascadesOptimizer<T, M>,
    ) {
        if let Winner::Unknown = optimizer.get_group_winner(self.group_id, self.subgroup_id) {
            optimizer.update_group_winner(self.group_id, self.subgroup_id, Winner::Impossible);
        }
    }

    fn update_winner<T: NodeType, M: Memo<T>>(
        &self,
        input_statistics: Vec<Option<&Statistics>>,
        operation_cost: Cost,
        total_cost: Cost,
        winner_expr: WinnerExpr,
        optimizer: &mut CascadesOptimizer<T, M>,
    ) {
        let group_id = optimizer.get_group_id(self.expr_id);
        let winner = optimizer.get_group_winner(group_id, self.subgroup_id);
        let cost = optimizer.cost();
        let operation_weighted_cost = cost.weighted_cost(&operation_cost);
        let total_weighted_cost = cost.weighted_cost(&total_cost);
        let mut update_cost = false;
        if let Some(winner) = winner.as_full_winner() {
            if winner.total_weighted_cost > total_weighted_cost {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            let expr = optimizer.get_expr_memoed(self.expr_id);
            let preds = expr
                .predicates
                .iter()
                .map(|pred_id| optimizer.get_pred(*pred_id))
                .collect_vec();
            let statistics = cost.derive_statistics(
                &expr.typ,
                &preds,
                &input_statistics
                    .iter()
                    .map(|x| x.expect("child winner should always have statistics?"))
                    .collect::<Vec<_>>(),
                Some(RelNodeContext {
                    group_id,
                    expr_id: self.expr_id,
                    children_group_ids: expr.children.clone(),
                }),
                Some(optimizer),
            );
            optimizer.update_group_winner(
                group_id,
                self.subgroup_id,
                Winner::Full(WinnerInfo {
                    expr_id: winner_expr,
                    total_weighted_cost,
                    operation_weighted_cost,
                    total_cost,
                    operation_cost,
                    statistics: statistics.into(),
                }),
            );
        }
    }

    fn update_winner_direct<T: NodeType, M: Memo<T>>(
        &self,
        statistics: Arc<Statistics>,
        operation_cost: Cost,
        total_cost: Cost,
        winner_expr: WinnerExpr,
        optimizer: &mut CascadesOptimizer<T, M>,
    ) {
        let winner = optimizer.get_group_winner(self.group_id, self.subgroup_id);
        let cost = optimizer.cost();
        let operation_weighted_cost = cost.weighted_cost(&operation_cost);
        let total_weighted_cost = cost.weighted_cost(&total_cost);
        let mut update_cost = false;
        if let Some(winner) = winner.as_full_winner() {
            if winner.total_weighted_cost > total_weighted_cost {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            optimizer.update_group_winner(
                self.group_id,
                self.subgroup_id,
                Winner::Full(WinnerInfo {
                    expr_id: winner_expr,
                    total_weighted_cost,
                    operation_weighted_cost,
                    total_cost,
                    operation_cost,
                    statistics: statistics.into(),
                }),
            );
        }
    }
}

impl<T: NodeType, M: Memo<T>> Task<T, M> for OptimizeInputsTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) -> Result<Vec<Box<dyn Task<T, M>>>> {
        if self.continue_from.is_none() {
            if optimizer.is_expr_explored(self.expr_id, self.subgroup_id) {
                // skip optimize_inputs to avoid dead-loop: consider join commute being fired twice
                // that produces two projections, therefore having groups like
                // projection1 -> projection2 -> join = projection1.
                trace!(event = "task_skip", task = "optimize_inputs", expr_id = %self.expr_id, subgroup_id = %self.subgroup_id);
                return Ok(vec![]);
            }
            optimizer.mark_expr_explored(self.expr_id, self.subgroup_id);
        }

        let group_id = optimizer.get_group_id(self.expr_id);
        let expr = optimizer.get_expr_memoed(self.expr_id);
        let cost = optimizer.cost();

        trace!(event = "task_begin", task = "optimize_inputs", subgroup_id = %self.subgroup_id, expr_id = %self.expr_id, continue_from = %ContinueTaskDisplay(&self.continue_from), total_children = %expr.children.len());

        if let Some(ContinueTask {
            next_group_idx,
            return_from_optimize_group,
            return_from_inverse_enforce,
            children_group_ids,
            children_subgroup_ids,
        }) = self.continue_from.clone()
        {
            let context = RelNodeContext {
                expr_id: self.expr_id,
                group_id,
                children_group_ids: children_group_ids.to_vec(),
            };
            let input_statistics = children_subgroup_ids
                .iter()
                .map(|&(group_id, subgroup_id)| {
                    optimizer
                        .get_group_winner(group_id, subgroup_id)
                        .as_full_winner()
                        .map(|x| x.statistics.clone())
                })
                .collect::<Vec<_>>();
            let input_statistics_ref = input_statistics
                .iter()
                .map(|x| x.as_deref())
                .collect::<Vec<_>>();
            let input_cost = children_subgroup_ids
                .iter()
                .map(|&(group_id, subgroup_id)| {
                    optimizer
                        .get_group_winner(group_id, subgroup_id)
                        .as_full_winner()
                        .map(|x| x.total_cost.clone())
                        .unwrap_or_else(|| cost.zero())
                })
                .collect::<Vec<_>>();
            let preds = expr
                .predicates
                .iter()
                .map(|pred_id| optimizer.get_pred(*pred_id))
                .collect_vec();
            let operation_cost = cost.compute_operation_cost(
                &expr.typ,
                &preds,
                &input_statistics_ref,
                Some(context.clone()),
                Some(optimizer),
            );
            let total_cost = cost.sum(&operation_cost, &input_cost);

            if self.pruning {
                let winner = optimizer.get_group_winner(group_id, self.subgroup_id);
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
                    expr_id = %self.expr_id,
                    weighted_cost_so_far = cost.weighted_cost(&total_cost),
                    winner_weighted_cost = %trace_fmt(winner),
                    current_processing = %next_group_idx,
                    total_child_groups = %children_group_ids.len());
                if let Some(winner) = winner.as_full_winner() {
                    let cost_so_far = cost.weighted_cost(&total_cost);
                    if winner.total_weighted_cost <= cost_so_far && !return_from_inverse_enforce
                    /* this is a hack... */
                    {
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id, result = "pruned");
                        return Ok(vec![]);
                    }
                }
            }

            if next_group_idx < children_group_ids.len() {
                let (child_group_id, child_subgroup_id) = children_subgroup_ids[next_group_idx];
                let group_idx = next_group_idx;
                let child_group_winner =
                    optimizer.get_group_winner(child_group_id, child_subgroup_id);
                if !child_group_winner.has_full_winner() {
                    if !return_from_optimize_group {
                        trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx, yield_to = "optimize_group", optimize_group_id = %child_group_id);
                        return Ok(vec![
                            Box::new(self.continue_from(
                                ContinueTask {
                                    next_group_idx,
                                    return_from_optimize_group: true,
                                    return_from_inverse_enforce: false,
                                    children_subgroup_ids: children_subgroup_ids.clone(),
                                    children_group_ids: children_group_ids.clone(),
                                },
                                self.pruning,
                            )) as Box<dyn Task<T, M>>,
                            Box::new(OptimizeGroupTask::new(child_group_id, child_subgroup_id))
                                as Box<dyn Task<T, M>>,
                        ]);
                    } else {
                        self.update_winner_impossible(optimizer);
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id, result = "impossible");
                        return Ok(vec![]);
                    }
                }
                trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx, yield_to = "next_optimize_input");
                Ok(vec![Box::new(self.continue_from(
                    ContinueTask {
                        next_group_idx: group_idx + 1,
                        return_from_optimize_group: false,
                        return_from_inverse_enforce: false,
                        children_subgroup_ids: children_subgroup_ids.clone(),
                        children_group_ids: children_group_ids.clone(),
                    },
                    self.pruning,
                )) as Box<dyn Task<T, M>>])
            } else {
                let expr = optimizer.get_expr_memoed(self.expr_id);
                if return_from_inverse_enforce {
                    if let Some(winner) = optimizer
                        .get_group_winner(children_subgroup_ids[0].0, children_subgroup_ids[0].1)
                        .as_full_winner()
                    {
                        self.update_winner_direct(
                            winner.statistics.clone(),
                            cost.zero(),
                            winner.total_cost.clone(),
                            WinnerExpr::Propagate {
                                group_id: expr.children[0],
                            },
                            optimizer,
                        );
                    }
                } else {
                    self.update_winner(
                        input_statistics_ref,
                        operation_cost,
                        total_cost,
                        WinnerExpr::Expr {
                            expr_id: self.expr_id,
                        },
                        optimizer,
                    );
                }

                // Apply reverse-enforcer rules if possible only when search goal is default
                let goal = optimizer
                    .memo()
                    .get_subgroup_goal(group_id, self.subgroup_id);
                let phys_prop_builders = optimizer.memo().get_physical_property_builders();
                if !return_from_inverse_enforce
                    && phys_prop_builders.exactly_eq(goal, phys_prop_builders.default_many())
                {
                    let predicates = expr
                        .predicates
                        .iter()
                        .map(|pred_id| optimizer.get_pred(*pred_id))
                        .collect_vec();
                    for (idx, phys_prop_builder) in optimizer
                        .memo()
                        .get_physical_property_builders()
                        .0
                        .iter()
                        .enumerate()
                    {
                        if let Some(prop) =
                            phys_prop_builder.search_goal_any(expr.typ.clone(), &predicates)
                        {
                            assert_eq!(expr.children.len(), 1);
                            trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, yield_to = "optimize_group_reverse_enforce");
                            let child_group_id = expr.children[0];
                            let mut required_props = phys_prop_builders.default_many();
                            required_props[idx] = prop;
                            let child_subgroup_id = optimizer
                                .create_or_get_subgroup(child_group_id, required_props.into());
                            return Ok(vec![
                                Box::new(self.continue_from(
                                    ContinueTask {
                                        next_group_idx,
                                        return_from_optimize_group: true,
                                        return_from_inverse_enforce: true,
                                        children_subgroup_ids: Arc::new([(
                                            child_group_id,
                                            child_subgroup_id,
                                        )]),
                                        children_group_ids: Arc::new([child_group_id]),
                                    },
                                    self.pruning,
                                )) as Box<dyn Task<T, M>>,
                                Box::new(OptimizeGroupTask::new(child_group_id, child_subgroup_id))
                                    as Box<dyn Task<T, M>>,
                            ]);
                        }
                    }
                }

                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id, result = "optimized");
                Ok(vec![])
            }
        } else {
            trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id);

            let expr = optimizer.get_expr_memoed(self.expr_id);
            let predicates = expr
                .predicates
                .iter()
                .map(|pred_id| optimizer.get_pred(*pred_id))
                .collect_vec();
            let group_id = optimizer.get_group_id(self.expr_id);
            let goal = optimizer
                .memo()
                .get_subgroup_goal(group_id, self.subgroup_id);
            let child_group_passthrough_properties = optimizer
                .memo()
                .get_physical_property_builders()
                .passthrough_many(expr.typ.clone(), &predicates, &goal, expr.children.len());
            assert_eq!(
                child_group_passthrough_properties.len(),
                expr.children.len()
            );
            let children_subgroup_ids = expr
                .children
                .iter()
                .zip(child_group_passthrough_properties)
                .map(|(child_group_id, required_props)| {
                    let child_subgroup_id =
                        optimizer.create_or_get_subgroup(*child_group_id, required_props.into());
                    (*child_group_id, child_subgroup_id)
                })
                .collect_vec();
            let children_group_ids = children_subgroup_ids
                .iter()
                .map(|(group_id, _)| *group_id)
                .collect_vec();

            Ok(vec![Box::new(self.continue_from(
                ContinueTask {
                    next_group_idx: 0,
                    return_from_optimize_group: false,
                    return_from_inverse_enforce: false,
                    children_subgroup_ids: children_subgroup_ids.into(),
                    children_group_ids: children_group_ids.into(),
                },
                self.pruning,
            )) as Box<dyn Task<T, M>>])
        }
    }

    fn describe(&self) -> String {
        format!("optimize_inputs {}", self.expr_id)
    }
}

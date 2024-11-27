// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use anyhow::Result;
use tracing::trace;

use super::Task;
use crate::cascades::memo::{Winner, WinnerExpr, WinnerInfo};
use crate::cascades::{CascadesOptimizer, ExprId, GroupId, Memo, RelNodeContext, SubGroupId};
use crate::cost::{Cost, Statistics};
use crate::nodes::NodeType;

/// If optimize group cannot find any expression that can always pass through the required properties, this task will
/// update the winner of the group as enforcer, and compute the cost of the enforcer.
pub struct OptimizeInputFinalizeTask {
    group_id: GroupId,
    subgroup_id: SubGroupId,
}

impl OptimizeInputFinalizeTask {
    pub fn new(group_id: GroupId, subgroup_id: SubGroupId) -> Self {
        Self {
            group_id,
            subgroup_id,
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
        statistics: Arc<Statistics>,
        operation_cost: Cost,
        total_cost: Cost,
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
                    expr_id: WinnerExpr::Enforcer {},
                    total_weighted_cost,
                    operation_weighted_cost,
                    total_cost,
                    operation_cost,
                    statistics,
                }),
            );
        }
    }
}

impl<T: NodeType, M: Memo<T>> Task<T, M> for OptimizeInputFinalizeTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) -> Result<Vec<Box<dyn Task<T, M>>>> {
        trace!(event = "task_begin", task = "optimize_input_finalize", subgroup_id = %self.subgroup_id);
        let default_goal = optimizer
            .memo()
            .get_physical_property_builders()
            .default_many();
        let default_goal_subgroup =
            optimizer.create_or_get_subgroup(self.group_id, default_goal.into());
        let cost = optimizer.cost();
        // check if we have a winner for the default goal subgroup
        match optimizer.get_group_winner(self.group_id, default_goal_subgroup) {
            Winner::Full(winner) => {
                // compute the cost of enforce the property
                let goal = optimizer
                    .memo()
                    .get_subgroup_goal(self.group_id, self.subgroup_id);
                let statistics = winner.statistics.clone();
                let mut operation_cost = winner.operation_cost.clone();
                let mut total_cost = winner.total_cost.clone();
                for (idx, builder) in optimizer
                    .memo()
                    .get_physical_property_builders()
                    .0
                    .iter()
                    .enumerate()
                {
                    let (typ, predicates) = builder.enforce_any(goal[idx].as_ref());
                    // assume statistics doesn't change when applying enforcers
                    let op_cost = cost.compute_operation_cost(
                        &typ,
                        &predicates,
                        &[Some(&statistics)],
                        RelNodeContext {
                            group_id: self.group_id,
                            expr_id: ExprId(0),
                            children_group_ids: vec![],
                        },
                        optimizer,
                    );
                    cost.accumulate(&mut operation_cost, &op_cost);
                    cost.accumulate(&mut total_cost, &op_cost);
                }
                trace!(
                    event = "compute_cost",
                    task = "optimize_input_finalize",
                    subgroup_id = %self.subgroup_id,
                    weighted_cost_so_far = cost.weighted_cost(&total_cost));
                self.update_winner(statistics, operation_cost, total_cost, optimizer);
            }
            Winner::Impossible => {
                self.update_winner_impossible(optimizer);
            }
            Winner::Unknown => {
                unreachable!("winner should be known at this point")
            }
        }
        trace!(event = "task_end", task = "optimize_input_finalize", subgroup_id = %self.subgroup_id);
        Ok(Vec::new())
    }

    fn describe(&self) -> String {
        format!("optimize_inputs_finalize {}", self.subgroup_id)
    }
}

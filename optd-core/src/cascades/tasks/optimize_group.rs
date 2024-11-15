// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use anyhow::Result;
use tracing::trace;

use super::Task;
use crate::cascades::optimizer::GroupId;
use crate::cascades::tasks::optimize_expression::OptimizeExpressionTask;
use crate::cascades::tasks::OptimizeInputsTask;
use crate::cascades::{CascadesOptimizer, Memo, SubGroupId};
use crate::nodes::NodeType;

pub struct OptimizeGroupTask {
    group_id: GroupId,
    subgroup_id: SubGroupId,
}

impl OptimizeGroupTask {
    pub fn new(group_id: GroupId, subgroup_id: SubGroupId) -> Self {
        Self {
            group_id,
            subgroup_id,
        }
    }
}

impl<T: NodeType, M: Memo<T>> Task<T, M> for OptimizeGroupTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) -> Result<Vec<Box<dyn Task<T, M>>>> {
        trace!(event = "task_begin", task = "optimize_group", group_id = %self.group_id, subgroup_id = %self.subgroup_id);
        let winner = optimizer.get_group_winner(self.group_id, self.subgroup_id);
        if winner.has_decided() {
            trace!(event = "task_finish", task = "optimize_group");
            return Ok(vec![]);
        }
        let exprs = optimizer.get_all_exprs_in_group(self.group_id);
        let mut tasks = vec![];
        let exprs_cnt = exprs.len();
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                tasks.push(
                    Box::new(OptimizeExpressionTask::new(expr, self.subgroup_id, false))
                        as Box<dyn Task<T, M>>,
                );
            }
        }
        // optimize with the required properties
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if !typ.is_logical() {
                tasks.push(Box::new(OptimizeInputsTask::new(
                    expr,
                    !optimizer.prop.disable_pruning,
                    self.subgroup_id,
                )) as Box<dyn Task<T, M>>);
            }
        }
        // optimize with default properties
        let default_props = optimizer
            .memo()
            .get_physical_property_builders()
            .default_many();
        let default_goal = optimizer.create_or_get_subgroup(self.group_id, default_props.into());
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if !typ.is_logical() {
                tasks.push(Box::new(OptimizeInputsTask::new(
                    expr,
                    !optimizer.prop.disable_pruning,
                    default_goal,
                )) as Box<dyn Task<T, M>>);
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %self.group_id, exprs_cnt = exprs_cnt);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("optimize_group {}", self.group_id)
    }
}

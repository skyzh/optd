// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use anyhow::Result;
use tracing::trace;

use super::Task;
use crate::cascades::optimizer::{CascadesOptimizer, GroupId};
use crate::cascades::tasks::OptimizeExpressionTask;
use crate::cascades::{Memo, SubGroupId};
use crate::nodes::NodeType;

pub struct ExploreGroupTask {
    group_id: GroupId,
    subgroup_id: SubGroupId,
}

impl ExploreGroupTask {
    pub fn new(group_id: GroupId, subgroup_id: SubGroupId) -> Self {
        Self {
            group_id,
            subgroup_id,
        }
    }
}

impl<T: NodeType, M: Memo<T>> Task<T, M> for ExploreGroupTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) -> Result<Vec<Box<dyn Task<T, M>>>> {
        trace!(event = "task_begin", task = "explore_group", group_id = %self.group_id);
        let mut tasks = vec![];
        if optimizer.is_group_explored(self.group_id, self.subgroup_id) {
            trace!(target: "task_finish", task = "explore_group", result = "already explored, skipping", group_id = %self.group_id);
            return Ok(vec![]);
        }
        let exprs = optimizer.get_all_exprs_in_group(self.group_id);
        let exprs_cnt = exprs.len();
        for expr in exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                tasks.push(
                    Box::new(OptimizeExpressionTask::new(expr, self.subgroup_id, true))
                        as Box<dyn Task<T, M>>,
                );
            }
        }
        optimizer.mark_group_explored(self.group_id, self.subgroup_id);
        trace!(
            event = "task_finish",
            task = "explore_group",
            result = "expand group",
            exprs_cnt = exprs_cnt
        );
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("explore_group {}", self.group_id)
    }
}

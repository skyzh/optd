// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use anyhow::Result;

use super::{CascadesOptimizer, Memo};
use crate::nodes::NodeType;

mod apply_rule;
mod explore_group;
mod optimize_expression;
mod optimize_group;
mod optimize_input_finalize;
mod optimize_inputs;

pub use apply_rule::ApplyRuleTask;
pub use explore_group::ExploreGroupTask;
pub use optimize_expression::OptimizeExpressionTask;
pub use optimize_group::OptimizeGroupTask;
pub use optimize_input_finalize::OptimizeInputFinalizeTask;
pub use optimize_inputs::OptimizeInputsTask;

pub trait Task<T: NodeType, M: Memo<T>>: 'static + Send + Sync {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) -> Result<Vec<Box<dyn Task<T, M>>>>;

    #[allow(dead_code)]
    fn describe(&self) -> String;
}

// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! The core cascades optimizer implementation.

mod memo;
mod optimizer;
pub(super) mod rule_match;
mod tasks2;

pub use memo::{Memo, NaiveMemo};
pub use optimizer::{
    CascadesOptimizer, ExprId, GroupId, OptimizerProperties, RelNodeContext, SubGoalId,
};

// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use super::macros::define_plan_node;
use super::predicates::ListPred;
use super::{ArcDfPlanNode, DfNodeType, DfPlanNode, DfReprPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalAgg(pub ArcDfPlanNode);

define_plan_node!(
    LogicalAgg : DfPlanNode,
    Agg, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, exprs: ListPred },
        { 1, groups: ListPred }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalHashAgg(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalHashAgg : DfPlanNode,
    PhysicalHashAgg, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, aggrs: ListPred },
        { 1, groups: ListPred }
    ]
);

/// Requires input to be sorted
#[derive(Clone, Debug)]
pub struct PhysicalStreamAgg(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalStreamAgg : DfPlanNode,
    PhysicalStreamAgg, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, aggrs: ListPred },
        { 1, groups: ListPred }
    ]
);

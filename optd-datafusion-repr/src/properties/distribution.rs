// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

// BEGIN Copyright 2024 RisingWave Labs

//!   "A -> B" represent A satisfies B
//!                                 x
//!  only as a required property    x  can used as both required
//!                                 x  and provided property
//!                                 x
//!            ┌───┐                x┌──────┐
//!            │Any◄─────────────────┤Single│
//!            └─▲─┘                x└──────┘
//!              │                  x
//!              │                  x
//!              │                  x
//!          ┌───┴────┐             x┌──────────┐
//!          │AnyShard◄──────────────┤SomeShard │
//!          └───▲────┘             x└──────────┘
//!              │                  x
//!          ┌───┴───────────┐      x┌──────────────┐ ┌──────────────┐
//!          │ KeyShard (a,b)◄───┬───┤HashShard(a,b)│ │HashShard(b,a)│
//!          └───▲──▲────────┘   │  x└──────────────┘ └┬─────────────┘
//!              │  │            │  x                  │
//!              │  │            └─────────────────────┘
//!              │  │               x
//!              │ ┌┴────────────┐  x┌────────────┐
//!              │ │ KeyShard (a)◄───┤HashShard(a)│
//!              │ └─────────────┘  x└────────────┘
//!              │                  x
//!             ┌┴────────────┐     x┌────────────┐
//!             │ KeyShard (b)◄──────┤HashShard(b)│
//!             └─────────────┘     x└────────────┘
//!                                 x
//!                                 x

// END Copyright 2024 RisingWave Labs

use std::{borrow::Borrow, collections::BTreeSet};

use itertools::Itertools;
use optd_core::{
    nodes::NodeType,
    physical_property::{PhysicalProperty, PhysicalPropertyBuilder},
};

use crate::plan_nodes::{ArcDfPredNode, ColumnRefPred, DfNodeType, DfReprPredNode, ListPred};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DistributionProp {
    // Used as required properties
    /// Any distribution
    Any,
    /// Any sharded distribution
    AnyShard,
    /// Any distribution sharded by key (order doesn't matter)
    KeyShard(BTreeSet<usize>),
    // Used as both required and derived properties
    /// Single shard
    Single,
    /// Sharded but not based on any hash
    SomeShard,
    /// Sharded by key
    HashShard(Vec<usize>),
}

impl std::fmt::Display for DistributionProp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl PhysicalProperty for DistributionProp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn to_boxed(&self) -> Box<dyn PhysicalProperty> {
        Box::new(self.clone())
    }
}

impl DistributionProp {
    fn key_shard(keys: impl IntoIterator<Item = usize>) -> Self {
        DistributionProp::KeyShard(keys.into_iter().collect())
    }
    /// Distribution property for projection mappings. In the future, it should take a ProjectionMapping.
    /// For now, we just assume we do not know how to manipulate the distribution.
    fn on_projection_passthrough(&self, predicates: &[ArcDfPredNode]) -> Self {
        let _ = predicates;
        match self {
            // Projections could change the distribution of shard keys, so we cannot passthrough them. TODO: we can generate a projection mapping
            // and use it to decide whether we can passthrough.
            DistributionProp::HashShard(_) => DistributionProp::AnyShard,
            DistributionProp::KeyShard(_) => DistributionProp::AnyShard,
            x => x.clone(),
        }
    }

    fn on_projection_derive(&self, prop: &DistributionProp, predicates: &[ArcDfPredNode]) -> Self {
        let _ = predicates;
        match prop {
            DistributionProp::HashShard(_) => DistributionProp::SomeShard,
            DistributionProp::KeyShard(_) => DistributionProp::SomeShard,
            x => x.clone(),
        }
    }
}

pub struct DistributionPropertyBuilder;

impl DistributionPropertyBuilder {
    pub fn new() -> Self {
        Self
    }
}

impl DistributionPropertyBuilder {
    fn enforce_hash_distribution(&self, columns: Vec<usize>) -> (DfNodeType, Vec<ArcDfPredNode>) {
        let predicate = ListPred::new(
            columns
                .iter()
                .map(|col| ColumnRefPred::new(*col).into_pred_node())
                .collect_vec(),
        )
        .into_pred_node();
        (DfNodeType::PhysicalHashShuffle, vec![predicate])
    }
}

impl PhysicalPropertyBuilder<DfNodeType> for DistributionPropertyBuilder {
    type Prop = DistributionProp;

    fn derive(
        &self,
        typ: DfNodeType,
        predicates: &[ArcDfPredNode],
        children: &[impl Borrow<Self::Prop>],
    ) -> Self::Prop {
        match typ {
            DfNodeType::PhysicalHashJoin(_) => {
                // use the left distribution? or the right? need func dep?
                let left_distribution = children[0].borrow().clone(); // TODO: ensure it satisfies current join key
                left_distribution
            }
            // TODO: add broadcast?
            DfNodeType::PhysicalNestedLoopJoin(_) => DistributionProp::Single,
            DfNodeType::PhysicalProjection => children[0]
                .borrow()
                .on_projection_derive(children[0].borrow(), predicates),
            DfNodeType::PhysicalScan => DistributionProp::SomeShard,
            DfNodeType::PhysicalEmptyRelation => DistributionProp::Single,
            DfNodeType::PhysicalGather => DistributionProp::Single,
            // Limit can only be done on a single node
            DfNodeType::PhysicalLimit => DistributionProp::Single,
            DfNodeType::PhysicalHashShuffle => {
                let pred = ListPred::from_pred_node(predicates[0].clone()).unwrap();
                let columns = pred
                    .to_vec()
                    .iter()
                    .map(|x| ColumnRefPred::from_pred_node(x.clone()).unwrap().index())
                    .collect_vec();
                DistributionProp::HashShard(columns)
            }
            _ if typ.is_logical() => unreachable!("logical node should not be called"),
            // Aggregations passthroughs child distribution
            _ if children.len() == 1 => children[0].borrow().clone(),
            other => unimplemented!("derive distribution prop for {other}"),
        }
    }

    fn passthrough(
        &self,
        typ: DfNodeType,
        predicates: &[ArcDfPredNode],
        required: &Self::Prop,
    ) -> Vec<Self::Prop> {
        match typ {
            // Let's only do single now. We can do distributed later.
            DfNodeType::PhysicalHashJoin(_) => {
                let left_keys = ListPred::from_pred_node(predicates[0].clone()).unwrap();
                let right_keys = ListPred::from_pred_node(predicates[1].clone()).unwrap();
                let left_columns = left_keys.to_column_indexes();
                let right_columns = right_keys.to_column_indexes();
                // Passthrough: if the required property is a subset of the join keys, we can passthrough
                if let DistributionProp::KeyShard(required) = required {
                    let positions = required
                        .iter()
                        .map(|x| left_columns.iter().position(|y| y == x))
                        .collect_vec();
                    if positions.iter().all(|x| x.is_some()) {
                        let left_columns = positions
                            .iter()
                            .map(|x| left_columns[x.unwrap()])
                            .collect_vec();
                        let right_columns = positions
                            .iter()
                            .map(|x| right_columns[x.unwrap()])
                            .collect_vec();
                        return vec![
                            DistributionProp::HashShard(left_columns.iter().copied().collect()),
                            DistributionProp::HashShard(right_columns.iter().copied().collect()),
                        ];
                    }
                }

                // Otherwise, use the join keys as the distribution property.
                // TODO: the executor needs to ensure left/right uses the same hash function (the hashes should be aligned)
                // We enforce hash shard here instead of key shard, considering self join hash partition alignment.
                vec![
                    DistributionProp::HashShard(left_columns),
                    DistributionProp::HashShard(right_columns),
                ]
            }
            DfNodeType::PhysicalNestedLoopJoin(_) => {
                vec![DistributionProp::Single, DistributionProp::Single]
            }
            DfNodeType::PhysicalStreamAgg => {
                let group_keys = ListPred::from_pred_node(predicates[1].clone()).unwrap();
                let group_columns = group_keys.to_column_indexes();
                vec![DistributionProp::key_shard(group_columns)]
            }
            DfNodeType::PhysicalHashAgg => {
                let group_keys = ListPred::from_pred_node(predicates[1].clone()).unwrap();
                let group_columns = group_keys.to_column_indexes();
                vec![DistributionProp::key_shard(group_columns)]
            }
            DfNodeType::PhysicalProjection => vec![required.on_projection_passthrough(predicates)],
            DfNodeType::PhysicalScan => vec![],
            DfNodeType::PhysicalEmptyRelation => vec![],
            // Limit can only be done on one node
            DfNodeType::PhysicalLimit => vec![DistributionProp::Single],
            DfNodeType::PhysicalFilter => vec![required.clone()],
            DfNodeType::PhysicalSort => vec![DistributionProp::Single],
            _ if typ.is_logical() => unreachable!("logical node should not be called"),
            other => unimplemented!("passthrough distribution prop for {other}"),
        }
    }

    fn satisfies(&self, prop: &DistributionProp, required: &DistributionProp) -> bool {
        match (prop, required) {
            (_, DistributionProp::Any) => true,
            (DistributionProp::Single, DistributionProp::Single) => true,
            (_, DistributionProp::Single) => false,
            (DistributionProp::Any | DistributionProp::Single, DistributionProp::AnyShard) => false,
            (_, DistributionProp::AnyShard) => true,
            (DistributionProp::SomeShard, DistributionProp::SomeShard) => true,
            (_, DistributionProp::SomeShard) => false,
            (DistributionProp::KeyShard(x), DistributionProp::KeyShard(y)) => {
                // KeyShard(1) satisfies KeyShard(1, 2, 3).
                // KeyShard(1, 2, 3) does not satisfy KeyShard(1) because if two rows have the same key on column 1
                // they don't necessarily get into the same shard as other columns values matter.
                for item in x {
                    if !y.contains(item) {
                        return false;
                    }
                }
                true
            }
            (DistributionProp::HashShard(x), DistributionProp::KeyShard(y)) => {
                for item in x {
                    if !y.contains(item) {
                        return false;
                    }
                }
                true
            }
            (DistributionProp::HashShard(x), DistributionProp::HashShard(y)) => x == y,
            _ => false,
        }
    }

    fn default(&self) -> Self::Prop {
        DistributionProp::Any
    }

    fn enforce(&self, prop: &DistributionProp) -> (DfNodeType, Vec<ArcDfPredNode>) {
        match prop {
            DistributionProp::HashShard(x) => self.enforce_hash_distribution(x.to_vec()),
            DistributionProp::KeyShard(x) => {
                self.enforce_hash_distribution(x.iter().copied().collect())
            }
            DistributionProp::Single => (DfNodeType::PhysicalGather, Vec::new()),
            _ => unreachable!(),
        }
    }

    fn property_name(&self) -> &'static str {
        "distribution"
    }
}

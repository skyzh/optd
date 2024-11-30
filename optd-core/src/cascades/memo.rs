// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use itertools::Itertools;
use tracing::trace;

use super::optimizer::{ExprId, GroupId, PredId, SubGoalId};
use crate::cost::{Cost, Statistics};
use crate::logical_property::{LogicalProperty, LogicalPropertyBuilderAny};
use crate::nodes::{ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeOrGroup};
use crate::physical_property::{PhysicalProperty, PhysicalPropertyBuilders};

pub type ArcMemoPlanNode<T> = Arc<MemoPlanNode<T>>;

/// The RelNode representation in the memo table. Store children as group IDs. Equivalent to MExpr
/// in Columbia/Cascades.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemoPlanNode<T: NodeType> {
    pub typ: T,
    pub children: Vec<GroupId>,
    pub predicates: Vec<PredId>,
}

impl<T: NodeType> std::fmt::Display for MemoPlanNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        for pred in &self.predicates {
            write!(f, " {}", pred)?;
        }
        write!(f, ")")
    }
}

#[derive(Debug, Clone)]
pub enum WinnerExpr {
    /// The winner is a physical expression, and the expression can passthrough all the required physical properties,
    /// or does not require any physical properties.
    Expr {
        /// The expression ID of the winner.
        expr_id: ExprId,
        // We don't need to store the passthrough-ed children properties because we can deterministically obtain it by
        // providing required output properties and the plan node type to the physical property builder. Of course,
        // we can cache it throughout the computation process, so that we don't need to compute it multiple times.
    },
    /// The winner is an enforcer of some group.
    Enforcer {
        /// The enforcer expression ID. Note that the expression might not belong to any of the group.
        expr_id: ExprId,
        /// The enforcer should have a single child (which is the current group) and we need to store the subgoal
        /// ID within the winner info.
        child_goal_id: SubGoalId,
    },
    /// The winner is another group with a required set of physical properties.
    /// For example, if a group X contains a logical sort on group Y, it will trigger a cascades search
    /// on group Y with a required sort property corresponding to the logical sort node. If any of the
    /// expression in group Y satisfies the required sort property, the "group Y with required property"
    /// will be the winner of group X.
    Propagate {
        /// The group ID of the winner.
        group_id: GroupId,
        /// The subgoal ID of the winner.
        subgoal_id: SubGoalId,
    },
}

#[derive(Clone)]
pub struct WinnerInfo {
    /// The winner of the group, it could be either an expression, or an enforcer.
    pub expr_id: WinnerExpr,
    /// The total weighted cost of the group.
    pub total_weighted_cost: f64,
    /// The weighted cost of the operation.
    pub operation_weighted_cost: f64,
    /// The cost of the group.
    pub total_cost: Cost,
    /// The cost of the operation.
    pub operation_cost: Cost,
    /// The statistics of the group.
    pub statistics: Arc<Statistics>,
    /// The derived physical properties of this winner.
    pub derived_physical_properties: Arc<[Box<dyn PhysicalProperty>]>,
}

#[derive(Clone)]
pub enum Winner {
    /// The winner is unknown, or it's impossible (due to no physical transformation).
    Unknown,
    /// The winner is a full winner.
    Full(WinnerInfo),
}

impl Winner {
    pub fn has_full_winner(&self) -> bool {
        matches!(self, Self::Full { .. })
    }

    pub fn as_full_winner(&self) -> Option<&WinnerInfo> {
        match self {
            Self::Full(info) => Some(info),
            _ => None,
        }
    }
}

impl Default for Winner {
    fn default() -> Self {
        Self::Unknown
    }
}

pub type RequiredPhysicalProperties = Arc<[Box<dyn PhysicalProperty>]>;

pub struct Group {
    /// Logical and physical expressions of the group.
    pub(crate) group_exprs: HashSet<ExprId>,
    /// Winners of each of the required set of physical properties within the group.
    /// Note that the winner does not necessarily have a physical property exactly equal
    /// to the required property. It could be a stronger property that satisfies the
    /// required property.
    pub(crate) winners: HashMap<SubGoalId, (RequiredPhysicalProperties, Winner)>,
    /// Logical properties of the group. This is inferred from the first expression added to the
    /// group.
    pub(crate) logical_properties: Arc<[Box<dyn LogicalProperty>]>,
}

/// Trait for memo table implementations.
pub trait Memo<T: NodeType>: 'static + Send + Sync {
    /// Add an expression to the memo table. If the expression already exists, it will return the
    /// existing group id and expr id. Otherwise, a new group and expr will be created.
    fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId);

    /// Add a new expression to an existing gruop. If the expression is a group, it will merge the
    /// two groups. Otherwise, it will add the expression to the group. Returns the expr id if
    /// the expression is not a group.
    fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId>;

    /// Add a new predicate into the memo table.
    fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId;

    /// Get the group id of an expression.
    /// The group id is volatile, depending on whether the groups are merged.
    fn get_group_id(&self, expr_id: ExprId) -> GroupId;

    /// Get the memoized representation of a node.
    fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T>;

    /// Get all groups IDs in the memo table.
    fn get_all_group_ids(&self) -> Vec<GroupId>;

    /// Get a group by ID
    fn get_group(&self, group_id: GroupId) -> &Group;

    /// Get a predicate by ID
    fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T>;

    /// Update the winner.
    fn update_winner(&mut self, group_id: GroupId, subgoal_id: SubGoalId, winner: Winner);

    /// Add a sub-group to the group, this creates a new search goal for a winner that satisfies
    /// the required physical properties.
    fn create_or_get_subgroup(
        &mut self,
        group_id: GroupId,
        required_phys_props: RequiredPhysicalProperties,
    ) -> SubGoalId;

    /// Get a subgroup based on required properties.
    fn get_subgroup(
        &self,
        group_id: GroupId,
        required_phys_props: RequiredPhysicalProperties,
    ) -> SubGoalId;

    /// Get all subgroups of a group.
    fn get_all_subgoal_ids(&self, group_id: GroupId) -> Vec<SubGoalId>;

    /// Get the goal of a subgroup.
    fn get_subgroup_goal(
        &self,
        group_id: GroupId,
        subgoal_id: SubGoalId,
    ) -> RequiredPhysicalProperties;

    /// Estimated plan space for the memo table, only useful when plan exploration budget is
    /// enabled. Returns number of expressions in the memo table.
    fn estimated_plan_space(&self) -> usize;

    // The below functions can be overwritten by the memo table implementation if there
    // are more efficient way to retrieve the information.

    /// Get all expressions in the group.
    fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let group = self.get_group(group_id);
        let mut exprs = group.group_exprs.iter().copied().collect_vec();
        // Sort so that we can get a stable processing order for the expressions, therefore making regression test
        // yield a stable result across different platforms.
        exprs.sort();
        exprs
    }

    /// Get winner of a group and a subgroup.
    fn get_group_winner(&self, group_id: GroupId, subgoal_id: SubGoalId) -> &Winner {
        &self.get_group(group_id).winners[&subgoal_id].1
    }

    /// Get the best group binding based on the cost
    fn get_best_group_binding(
        &self,
        group_id: GroupId,
        subgoal_id: SubGoalId,
        mut post_process: impl FnMut(ArcPlanNode<T>, GroupId, SubGoalId, &WinnerInfo),
    ) -> Result<ArcPlanNode<T>> {
        let mut visited = HashSet::new();
        get_best_group_binding_inner(self, group_id, subgoal_id, &mut post_process, &mut visited)
    }

    /// Get the physical property builders.
    fn get_physical_property_builders(&self) -> &PhysicalPropertyBuilders<T>;
}

fn get_best_group_binding_inner<M: Memo<T> + ?Sized, T: NodeType>(
    this: &M,
    group_id: GroupId,
    subgoal_id: SubGoalId,
    post_process: &mut impl FnMut(ArcPlanNode<T>, GroupId, SubGoalId, &WinnerInfo),
    visited: &mut HashSet<(GroupId, SubGoalId)>,
) -> Result<ArcPlanNode<T>> {
    let winner = this.get_group_winner(group_id, subgoal_id);

    if visited.contains(&(group_id, subgoal_id)) {
        bail!(
            "cycle detected in the winner result: group_id={}, subgoal_id={}",
            group_id,
            subgoal_id
        );
    }
    visited.insert((group_id, subgoal_id));

    if let Winner::Full(
        info @ WinnerInfo {
            expr_id,
            derived_physical_properties,
            ..
        },
    ) = winner
    {
        let required_phys_prop = this.get_subgroup_goal(group_id, subgoal_id);
        assert!(
            this.get_physical_property_builders()
                .satisfies_many(&derived_physical_properties, &required_phys_prop),
            "derived physical properties do not satisfy the required physical properties"
        );
        match expr_id {
            WinnerExpr::Expr { expr_id } => {
                let expr = this.get_expr_memoed(*expr_id);
                let predicates = expr
                    .predicates
                    .iter()
                    .map(|x| this.get_pred(*x))
                    .collect_vec();
                let required_child_phys_prop = this
                    .get_physical_property_builders()
                    .passthrough_many(
                        expr.typ.clone(),
                        &predicates,
                        &required_phys_prop,
                        expr.children.len(),
                    )
                    .into_iter()
                    .map(Arc::<[_]>::from)
                    .collect_vec();
                let mut children = Vec::with_capacity(expr.children.len());
                for (child_idx, &child) in expr.children.iter().enumerate() {
                    let child_required_phys_prop =
                        this.get_subgroup(child, required_child_phys_prop[child_idx].clone());
                    children.push(PlanNodeOrGroup::PlanNode(
                        get_best_group_binding_inner(
                            this,
                            child,
                            child_required_phys_prop,
                            post_process,
                            visited,
                        )
                        .with_context(|| format!("when processing expr {}", expr_id))?,
                    ));
                }
                let node: Arc<PlanNode<T>> = Arc::new(PlanNode {
                    typ: expr.typ.clone(),
                    children,
                    predicates,
                });
                // TODO: verify the node satisfies all required phys props
                post_process(node.clone(), group_id, subgoal_id, info);
                visited.remove(&(group_id, subgoal_id));
                return Ok(node);
            }
            WinnerExpr::Enforcer {
                expr_id,
                child_goal_id,
            } => {
                let expr = this.get_expr_memoed(*expr_id);
                assert_eq!(expr.children.len(), 1);
                let child_group_id = expr.children[0];
                let child_node = get_best_group_binding_inner(
                    this,
                    child_group_id,
                    *child_goal_id,
                    post_process,
                    visited,
                )?;
                let node = Arc::new(PlanNode {
                    typ: expr.typ.clone(),
                    children: vec![PlanNodeOrGroup::PlanNode(child_node)],
                    predicates: expr
                        .predicates
                        .iter()
                        .map(|x| this.get_pred(*x))
                        .collect_vec(),
                });
                post_process(node.clone(), group_id, subgoal_id, info);
                visited.remove(&(group_id, subgoal_id));
                return Ok(node);
            }
            WinnerExpr::Propagate {
                group_id: child_group_id,
                subgoal_id: child_subgoal_id,
            } => {
                let node = get_best_group_binding_inner(
                    this,
                    *child_group_id,
                    *child_subgoal_id,
                    post_process,
                    visited,
                )?;
                post_process(node.clone(), group_id, subgoal_id, info);
                visited.remove(&(group_id, subgoal_id));
                return Ok(node);
            }
        }
    }
    bail!("no best group binding for group {}", group_id)
}

/// A naive, simple, and unoptimized memo table implementation.
pub struct NaiveMemo<T: NodeType> {
    // Source of truth.
    groups: HashMap<GroupId, Group>,
    expr_id_to_expr_node: HashMap<ExprId, ArcMemoPlanNode<T>>,

    // Predicate stuff.
    pred_id_to_pred_node: HashMap<PredId, ArcPredNode<T>>,
    pred_node_to_pred_id: HashMap<ArcPredNode<T>, PredId>,

    // Internal states.
    group_expr_counter: usize,
    logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
    #[allow(dead_code)]
    physical_property_builders: PhysicalPropertyBuilders<T>,

    // Indexes.
    expr_node_to_expr_id: HashMap<MemoPlanNode<T>, ExprId>,
    expr_id_to_group_id: HashMap<ExprId, GroupId>,

    // We update all group IDs in the memo table upon group merging, but
    // there might be edge cases that some tasks still hold the old group ID.
    // In this case, we need this mapping to redirect to the merged group ID.
    merged_group_mapping: HashMap<GroupId, GroupId>,
    dup_expr_mapping: HashMap<ExprId, ExprId>,
}

impl<T: NodeType> Memo<T> for NaiveMemo<T> {
    fn create_or_get_subgroup(
        &mut self,
        group_id: GroupId,
        required_phys_props: RequiredPhysicalProperties,
    ) -> SubGoalId {
        assert_eq!(
            required_phys_props.len(),
            self.physical_property_builders.len()
        );
        let group_id = self.reduce_group(group_id);
        let group = self.groups.get_mut(&group_id).unwrap();
        for (subgoal_id, (props, _)) in group.winners.iter() {
            if self
                .physical_property_builders
                .exactly_eq(props, &required_phys_props)
            {
                return *subgoal_id;
            }
        }
        let subgoal_id = self.next_subgoal_id();
        let group = self.groups.get_mut(&group_id).unwrap();
        group
            .winners
            .insert(subgoal_id, (required_phys_props, Winner::Unknown));
        subgoal_id
    }

    fn get_subgroup(
        &self,
        group_id: GroupId,
        required_phys_props: RequiredPhysicalProperties,
    ) -> SubGoalId {
        let group_id = self.reduce_group(group_id);
        let group = self.groups.get(&group_id).unwrap();
        for (subgoal_id, (props, _)) in group.winners.iter() {
            if self
                .physical_property_builders
                .exactly_eq(props, &required_phys_props)
            {
                return *subgoal_id;
            }
        }
        unreachable!("subgroup not found")
    }

    fn get_subgroup_goal(
        &self,
        group_id: GroupId,
        subgoal_id: SubGoalId,
    ) -> RequiredPhysicalProperties {
        let group_id = self.reduce_group(group_id);
        self.groups[&group_id].winners[&subgoal_id].0.clone()
    }

    fn get_all_subgoal_ids(&self, group_id: GroupId) -> Vec<SubGoalId> {
        let group_id = self.reduce_group(group_id);
        let mut subgroups = self.groups[&group_id].winners.keys().copied().collect_vec();
        subgroups.sort();
        subgroups
    }

    fn get_physical_property_builders(&self) -> &PhysicalPropertyBuilders<T> {
        &self.physical_property_builders
    }

    fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let (group_id, expr_id) = self
            .add_new_group_expr_inner(rel_node, None)
            .expect("should not trigger merge group");
        self.verify_integrity();
        (group_id, expr_id)
    }

    fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        match rel_node {
            PlanNodeOrGroup::Group(input_group) => {
                let input_group = self.reduce_group(input_group);
                let group_id = self.reduce_group(group_id);
                self.merge_group_inner(input_group, group_id);
                None
            }
            PlanNodeOrGroup::PlanNode(rel_node) => {
                let reduced_group_id = self.reduce_group(group_id);
                let (returned_group_id, expr_id) = self
                    .add_new_group_expr_inner(rel_node, Some(reduced_group_id))
                    .unwrap();
                assert_eq!(returned_group_id, reduced_group_id);
                self.verify_integrity();
                Some(expr_id)
            }
        }
    }

    fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId {
        let pred_id = self.next_pred_id();
        if let Some(id) = self.pred_node_to_pred_id.get(&pred_node) {
            return *id;
        }
        self.pred_node_to_pred_id.insert(pred_node.clone(), pred_id);
        self.pred_id_to_pred_node.insert(pred_id, pred_node);
        pred_id
    }

    fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T> {
        self.pred_id_to_pred_node[&pred_id].clone()
    }

    fn get_group_id(&self, mut expr_id: ExprId) -> GroupId {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        *self
            .expr_id_to_group_id
            .get(&expr_id)
            .expect("expr not found in group mapping")
    }

    fn get_expr_memoed(&self, mut expr_id: ExprId) -> ArcMemoPlanNode<T> {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        self.expr_id_to_expr_node
            .get(&expr_id)
            .expect("expr not found in expr mapping")
            .clone()
    }

    fn get_all_group_ids(&self) -> Vec<GroupId> {
        let mut ids = self.groups.keys().copied().collect_vec();
        ids.sort();
        ids
    }

    fn get_group(&self, group_id: GroupId) -> &Group {
        let group_id = self.reduce_group(group_id);
        self.groups.get(&group_id).as_ref().unwrap()
    }

    fn update_winner(&mut self, group_id: GroupId, subgoal_id: SubGoalId, winner: Winner) {
        let group_id = self.reduce_group(group_id);
        let group = self.groups.get_mut(&group_id).unwrap();
        if let Winner::Full(WinnerInfo {
            total_weighted_cost,
            expr_id,
            ..
        }) = &winner
        {
            if let WinnerExpr::Expr { expr_id } = expr_id {
                assert!(
                    *total_weighted_cost != 0.0,
                    "{}",
                    self.expr_id_to_expr_node[expr_id]
                );
            } else {
                assert!(*total_weighted_cost != 0.0);
            }
        }
        let (winner_prop, current_winner) = group.winners.get_mut(&subgoal_id).unwrap();
        *current_winner = winner.clone();
        let winner_prop = winner_prop.clone();
        for (prop, other_winner) in group.winners.values_mut() {
            if self
                .physical_property_builders
                .exactly_eq(prop, &winner_prop)
            {
                *other_winner = winner.clone();
            }
        }
    }

    fn estimated_plan_space(&self) -> usize {
        self.expr_id_to_expr_node.len()
    }
}

impl<T: NodeType> NaiveMemo<T> {
    pub fn new(
        logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
        physical_property_builders: PhysicalPropertyBuilders<T>,
    ) -> Self {
        Self {
            expr_id_to_group_id: HashMap::new(),
            expr_id_to_expr_node: HashMap::new(),
            expr_node_to_expr_id: HashMap::new(),
            pred_id_to_pred_node: HashMap::new(),
            pred_node_to_pred_id: HashMap::new(),
            groups: HashMap::new(),
            group_expr_counter: 0,
            merged_group_mapping: HashMap::new(),
            logical_property_builders,
            physical_property_builders,
            dup_expr_mapping: HashMap::new(),
        }
    }

    /// Get the next group id. Group id and expr id shares the same counter, so as to make it easier
    /// to debug...
    fn next_group_id(&mut self) -> GroupId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        GroupId(id)
    }

    /// Get the next subgroup id. Group id and expr id shares the same counter, so as to make it easier
    fn next_subgoal_id(&mut self) -> SubGoalId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        SubGoalId(id)
    }

    /// Get the next expr id. Group id and expr id shares the same counter, so as to make it easier
    /// to debug...
    fn next_expr_id(&mut self) -> ExprId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ExprId(id)
    }

    /// Get the next pred id. Group id and expr id shares the same counter, so as to make it easier
    /// to debug...
    fn next_pred_id(&mut self) -> PredId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        PredId(id)
    }

    fn verify_integrity(&self) {
        if cfg!(debug_assertions) {
            let num_of_exprs = self.expr_id_to_expr_node.len();
            assert_eq!(num_of_exprs, self.expr_node_to_expr_id.len());
            assert_eq!(num_of_exprs, self.expr_id_to_group_id.len());

            let mut valid_groups = HashSet::new();
            for to in self.merged_group_mapping.values() {
                assert_eq!(self.merged_group_mapping[to], *to);
                valid_groups.insert(*to);
            }
            assert_eq!(valid_groups.len(), self.groups.len());

            for (id, node) in self.expr_id_to_expr_node.iter() {
                assert_eq!(self.expr_node_to_expr_id[node], *id);
                for child in &node.children {
                    assert!(
                        valid_groups.contains(child),
                        "invalid group used in expression {}, where {} does not exist any more",
                        node,
                        child
                    );
                }
            }

            let mut cnt = 0;
            for (group_id, group) in &self.groups {
                assert!(valid_groups.contains(group_id));
                cnt += group.group_exprs.len();
                assert!(!group.group_exprs.is_empty());
                for expr in &group.group_exprs {
                    assert_eq!(self.expr_id_to_group_id[expr], *group_id);
                }
            }
            assert_eq!(cnt, num_of_exprs);
        }
    }

    fn reduce_group(&self, group_id: GroupId) -> GroupId {
        self.merged_group_mapping[&group_id]
    }

    fn merge_group_inner(&mut self, merge_into: GroupId, merge_from: GroupId) {
        if merge_into == merge_from {
            return;
        }
        trace!(event = "merge_group", merge_into = %merge_into, merge_from = %merge_from);
        let group_merge_from = self.groups.remove(&merge_from).unwrap();
        let group_merge_into = self.groups.get_mut(&merge_into).unwrap();
        // TODO: update winner, cost and properties
        for from_expr in group_merge_from.group_exprs {
            let ret = self.expr_id_to_group_id.insert(from_expr, merge_into);
            assert!(ret.is_some());
            group_merge_into.group_exprs.insert(from_expr);
        }
        group_merge_into.winners.extend(group_merge_from.winners);
        let mut new_winners = HashMap::new();
        fn better_winner<'a>(a: &'a Winner, b: &'a Winner) -> &'a Winner {
            match (a, b) {
                (left @ Winner::Full(a), right @ Winner::Full(b)) => {
                    if a.total_weighted_cost < b.total_weighted_cost {
                        left
                    } else {
                        right
                    }
                }
                (left @ Winner::Full(_), _) => left,
                (_, right @ Winner::Full(_)) => right,
                (_, other) => other,
            }
        }
        struct HashableGoal<T: NodeType> {
            props: RequiredPhysicalProperties,
            builder: PhysicalPropertyBuilders<T>,
        }
        impl<T: NodeType> PartialEq for HashableGoal<T> {
            fn eq(&self, other: &Self) -> bool {
                self.builder.exactly_eq(&self.props, &other.props)
            }
        }
        impl<T: NodeType> Eq for HashableGoal<T> {}
        impl<T: NodeType> std::hash::Hash for HashableGoal<T> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.builder.hash_any(&self.props, state);
            }
        }

        // Update the winners based on the cost
        for (required_prop, winner) in group_merge_into.winners.values() {
            let entry = new_winners
                .entry(HashableGoal {
                    props: required_prop.clone(),
                    builder: self.physical_property_builders.clone(),
                })
                .or_insert_with(|| winner.clone());
            *entry = better_winner(entry, winner).clone();
        }
        for (required_prop, winner) in group_merge_into.winners.values_mut() {
            let entry = new_winners
                .get(&HashableGoal {
                    props: required_prop.clone(),
                    builder: self.physical_property_builders.clone(),
                })
                .unwrap();
            *winner = entry.clone();
        }

        self.merged_group_mapping.insert(merge_from, merge_into);

        // Update all indexes and other data structures
        // 1. update merged group mapping -- could be optimized with union find
        for (_, mapped_to) in self.merged_group_mapping.iter_mut() {
            if *mapped_to == merge_from {
                *mapped_to = merge_into;
            }
        }

        let mut pending_recursive_merge = Vec::new();
        // 2. update all group expressions and indexes
        for (group_id, group) in self.groups.iter_mut() {
            let mut new_expr_list = HashSet::new();
            for expr_id in group.group_exprs.iter() {
                let expr = self.expr_id_to_expr_node[expr_id].clone();
                if expr.children.contains(&merge_from) {
                    // Create the new expr node
                    let old_expr = expr.as_ref().clone();
                    let mut new_expr = expr.as_ref().clone();
                    new_expr.children.iter_mut().for_each(|x| {
                        if *x == merge_from {
                            *x = merge_into;
                        }
                    });
                    // Update all existing entries and indexes
                    self.expr_id_to_expr_node
                        .insert(*expr_id, Arc::new(new_expr.clone()));
                    self.expr_node_to_expr_id.remove(&old_expr);
                    if let Some(dup_expr) = self.expr_node_to_expr_id.get(&new_expr) {
                        // If new_expr == some_other_old_expr in the memo table, unless they belong
                        // to the same group, we should merge the two
                        // groups. This should not happen. We should simply drop this expression.
                        let dup_group_id = self.expr_id_to_group_id[dup_expr];
                        if dup_group_id != *group_id {
                            pending_recursive_merge.push((dup_group_id, *group_id));
                        }
                        self.expr_id_to_expr_node.remove(expr_id);
                        self.expr_id_to_group_id.remove(expr_id);
                        self.dup_expr_mapping.insert(*expr_id, *dup_expr);
                        new_expr_list.insert(*dup_expr); // adding this temporarily -- should be
                                                         // removed once recursive merge finishes
                    } else {
                        self.expr_node_to_expr_id.insert(new_expr, *expr_id);
                        new_expr_list.insert(*expr_id);
                    }
                } else {
                    new_expr_list.insert(*expr_id);
                }
            }
            assert!(!new_expr_list.is_empty());
            group.group_exprs = new_expr_list;
        }
        for (merge_from, merge_into) in pending_recursive_merge {
            // We need to reduce because each merge would probably invalidate some groups in the
            // last loop iteration.
            let merge_from = self.reduce_group(merge_from);
            let merge_into = self.reduce_group(merge_into);
            self.merge_group_inner(merge_into, merge_from);
        }
    }

    fn add_new_group_expr_inner(
        &mut self,
        rel_node: ArcPlanNode<T>,
        add_to_group_id: Option<GroupId>,
    ) -> anyhow::Result<(GroupId, ExprId)> {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                match child {
                    // TODO: can I remove reduce?
                    PlanNodeOrGroup::Group(group) => self.reduce_group(*group),
                    PlanNodeOrGroup::PlanNode(child) => {
                        // No merge / modification to the memo should occur for the following
                        // operation
                        let (group, _) = self
                            .add_new_group_expr_inner(child.clone(), None)
                            .expect("should not trigger merge group");
                        self.reduce_group(group) // TODO: can I remove?
                    }
                }
            })
            .collect::<Vec<_>>();
        let memo_node = MemoPlanNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            predicates: rel_node
                .predicates
                .iter()
                .map(|x| self.add_new_pred(x.clone()))
                .collect(),
        };
        if let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) {
            let group_id = self.expr_id_to_group_id[&expr_id];
            if let Some(add_to_group_id) = add_to_group_id {
                let add_to_group_id = self.reduce_group(add_to_group_id);
                self.merge_group_inner(add_to_group_id, group_id);
                return Ok((add_to_group_id, expr_id));
            }
            return Ok((group_id, expr_id));
        }
        let expr_id = self.next_expr_id();
        let group_id = if let Some(group_id) = add_to_group_id {
            group_id
        } else {
            self.next_group_id()
        };
        self.expr_id_to_expr_node
            .insert(expr_id, memo_node.clone().into());
        self.expr_id_to_group_id.insert(expr_id, group_id);
        self.expr_node_to_expr_id.insert(memo_node.clone(), expr_id);
        self.append_expr_to_group(expr_id, group_id, memo_node);
        Ok((group_id, expr_id))
    }

    /// This is inefficient: usually the optimizer should have a MemoRef instead of passing the full
    /// rel node. Should be only used for debugging purpose.
    #[cfg(test)]
    pub(crate) fn get_expr_info(&self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| match child {
                PlanNodeOrGroup::Group(group) => *group,
                PlanNodeOrGroup::PlanNode(child) => self.get_expr_info(child.clone()).0,
            })
            .collect::<Vec<_>>();
        let memo_node = MemoPlanNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            predicates: rel_node
                .predicates
                .iter()
                .map(|x| self.pred_node_to_pred_id[x])
                .collect(),
        };
        let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) else {
            unreachable!("not found {}", memo_node)
        };
        let group_id = self.expr_id_to_group_id[&expr_id];
        (group_id, expr_id)
    }

    fn infer_properties(&self, memo_node: MemoPlanNode<T>) -> Vec<Box<dyn LogicalProperty>> {
        let child_properties = memo_node
            .children
            .iter()
            .map(|child| self.groups[child].logical_properties.clone())
            .collect_vec();
        let mut props = Vec::with_capacity(self.logical_property_builders.len());
        for (id, builder) in self.logical_property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref())
                .collect::<Vec<_>>();
            let child_predicates = memo_node
                .predicates
                .iter()
                .map(|x| self.pred_id_to_pred_node[x].clone())
                .collect_vec();
            let prop = builder.derive_any(
                memo_node.typ.clone(),
                &child_predicates,
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        props
    }

    /// If group_id exists, it adds expr_id to the existing group
    /// Otherwise, it creates a new group of that group_id and insert expr_id into the new group
    fn append_expr_to_group(
        &mut self,
        expr_id: ExprId,
        group_id: GroupId,
        memo_node: MemoPlanNode<T>,
    ) {
        trace!(event = "add_expr_to_group", group_id = %group_id, expr_id = %expr_id, memo_node = %memo_node);
        if let Entry::Occupied(mut entry) = self.groups.entry(group_id) {
            let group = entry.get_mut();
            group.group_exprs.insert(expr_id);
            return;
        }
        // Create group and infer properties (only upon initializing a group).
        let mut group = Group {
            group_exprs: HashSet::new(),
            winners: Default::default(),
            logical_properties: self.infer_properties(memo_node).into(),
        };
        group.group_exprs.insert(expr_id);
        self.groups.insert(group_id, group);
        self.merged_group_mapping.insert(group_id, group_id);
    }

    pub fn clear_winner(&mut self) {
        for group in self.groups.values_mut() {
            for (_, (_, winner)) in group.winners.iter_mut() {
                *winner = Winner::Unknown;
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        nodes::Value,
        tests::common::{
            expr, group, join, list, project, scan, MemoTestRelTyp, TestProp, TestPropertyBuilder,
        },
    };

    #[test]
    fn add_predicate() {
        let mut memo = NaiveMemo::<MemoTestRelTyp>::new(
            Arc::new([]),
            PhysicalPropertyBuilders::new_empty_for_test(),
        );
        let pred_node = list(vec![expr(Value::Int32(233))]);
        let p1 = memo.add_new_pred(pred_node.clone());
        let p2 = memo.add_new_pred(pred_node.clone());
        assert_eq!(p1, p2);
    }

    #[test]
    fn group_merge_1() {
        let mut memo = NaiveMemo::new(Arc::new([]), PhysicalPropertyBuilders::new_empty_for_test());
        let (group_id, _) =
            memo.add_new_expr(join(scan("t1"), scan("t2"), expr(Value::Bool(true))));
        memo.add_expr_to_group(
            join(scan("t2"), scan("t1"), expr(Value::Bool(true))).into(),
            group_id,
        );
        assert_eq!(memo.get_group(group_id).group_exprs.len(), 2);
    }

    #[test]
    fn group_merge_2() {
        let mut memo = NaiveMemo::new(Arc::new([]), PhysicalPropertyBuilders::new_empty_for_test());
        let (group_id_1, _) = memo.add_new_expr(project(
            join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
            list(vec![expr(Value::Int64(1))]),
        ));
        let (group_id_2, _) = memo.add_new_expr(project(
            join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
            list(vec![expr(Value::Int64(1))]),
        ));
        assert_eq!(group_id_1, group_id_2);
    }

    #[test]
    fn group_merge_3() {
        let mut memo = NaiveMemo::new(Arc::new([]), PhysicalPropertyBuilders::new_empty_for_test());
        let expr1 = project(scan("t1"), list(vec![expr(Value::Int64(1))]));
        let expr2 = project(scan("t1-alias"), list(vec![expr(Value::Int64(1))]));
        memo.add_new_expr(expr1.clone());
        memo.add_new_expr(expr2.clone());
        // merging two child groups causes parent to merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1"));
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
        let (group_1, _) = memo.get_expr_info(expr1);
        let (group_2, _) = memo.get_expr_info(expr2);
        assert_eq!(group_1, group_2);
    }

    #[test]
    fn group_merge_4() {
        let mut memo = NaiveMemo::new(Arc::new([]), PhysicalPropertyBuilders::new_empty_for_test());
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        memo.add_new_expr(expr1.clone());
        memo.add_new_expr(expr2.clone());
        // merge two child groups, cascading merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1"));
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
        let (group_1, _) = memo.get_expr_info(expr1.clone());
        let (group_2, _) = memo.get_expr_info(expr2.clone());
        assert_eq!(group_1, group_2);
        let (group_1, _) = memo.get_expr_info(expr1.child_rel(0));
        let (group_2, _) = memo.get_expr_info(expr2.child_rel(0));
        assert_eq!(group_1, group_2);
    }

    #[test]
    fn group_merge_5() {
        let mut memo = NaiveMemo::new(Arc::new([]), PhysicalPropertyBuilders::new_empty_for_test());
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let (_, expr1_id) = memo.add_new_expr(expr1.clone());
        let (_, expr2_id) = memo.add_new_expr(expr2.clone());

        // experimenting with group id in expr (i.e., when apply rules)
        let (scan_t1, _) = memo.get_expr_info(scan("t1"));
        let pred = list(vec![expr(Value::Int64(1))]);
        let proj_binding = project(group(scan_t1), pred);
        let middle_proj_2 = memo.get_expr_memoed(expr2_id).children[0];

        memo.add_expr_to_group(proj_binding.into(), middle_proj_2);

        assert_eq!(
            memo.get_expr_memoed(expr1_id),
            memo.get_expr_memoed(expr2_id)
        ); // these two expressions are merged
        assert_eq!(memo.get_expr_info(expr1), memo.get_expr_info(expr2));
    }

    #[test]
    fn derive_logical_property() {
        let mut memo = NaiveMemo::new(
            Arc::new([Box::new(TestPropertyBuilder)]),
            PhysicalPropertyBuilders::new_empty_for_test(),
        );
        let (group_id, _) = memo.add_new_expr(join(
            scan("t1"),
            project(
                scan("t2"),
                list(vec![expr(Value::Int64(1)), expr(Value::Int64(2))]),
            ),
            expr(Value::Bool(true)),
        ));
        let group = memo.get_group(group_id);
        assert_eq!(group.logical_properties.len(), 1);
        assert_eq!(
            group.logical_properties[0]
                .as_any()
                .downcast_ref::<TestProp>()
                .unwrap()
                .0,
            vec!["scan_col", "1", "2"]
        );
    }
}

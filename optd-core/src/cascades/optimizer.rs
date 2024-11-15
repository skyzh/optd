// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::sync::Arc;

use anyhow::Result;
use itertools::Itertools;
use tracing::trace;

use super::memo::{ArcMemoPlanNode, Memo, RequiredPhysicalProperties};
use super::tasks::OptimizeGroupTask;
use super::{NaiveMemo, Task};
use crate::cascades::memo::{Winner, WinnerExpr};
use crate::cost::CostModel;
use crate::logical_property::{LogicalPropertyBuilder, LogicalPropertyBuilderAny};
use crate::nodes::{
    ArcPlanNode, ArcPredNode, NodeType, PlanNodeMeta, PlanNodeMetaMap, PlanNodeOrGroup,
};
use crate::optimizer::Optimizer;
use crate::physical_property::{
    PhysicalProperty, PhysicalPropertyBuilderAny, PhysicalPropertyBuilders,
};
use crate::rules::Rule;

pub type RuleId = usize;

#[derive(Default, Clone, Debug)]
pub struct OptimizerContext {
    pub budget_used: bool,
    pub rules_applied: usize,
}

#[derive(Default, Clone, Debug)]
pub struct OptimizerProperties {
    pub panic_on_budget: bool,
    /// If the number of rules applied exceeds this number, we stop applying logical rules.
    pub partial_explore_iter: Option<usize>,
    /// Plan space can be expanded by this number of times before we stop applying logical rules.
    pub partial_explore_space: Option<usize>,
    /// Disable pruning during optimization.
    pub disable_pruning: bool,
}

pub struct CascadesOptimizer<T: NodeType, M: Memo<T> = NaiveMemo<T>> {
    memo: M,
    pub(super) tasks: VecDeque<Box<dyn Task<T, M>>>,
    explored_group: HashSet<(GroupId, SubGroupId)>,
    explored_expr: HashSet<(ExprId, SubGroupId)>,
    fired_rules: HashMap<ExprId, HashSet<RuleId>>,
    rules: Arc<[Arc<dyn Rule<T, Self>>]>,
    disabled_rules: HashSet<usize>,
    cost: Arc<dyn CostModel<T, M>>,
    logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
    physical_property_builders: PhysicalPropertyBuilders<T>,
    pub ctx: OptimizerContext,
    pub prop: OptimizerProperties,
}

/// `RelNode` only contains the representation of the plan nodes. Sometimes, we need more context,
/// i.e., group id and expr id, during the optimization phase. All these information are collected
/// in this struct.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct RelNodeContext {
    pub group_id: GroupId,
    pub expr_id: ExprId,
    pub children_group_ids: Vec<GroupId>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct GroupId(pub(super) usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct SubGroupId(pub(super) usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct ExprId(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct PredId(pub usize);

impl Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "!{}", self.0)
    }
}

impl Display for SubGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, ".{}", self.0)
    }
}

impl Display for ExprId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for PredId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "P{}", self.0)
    }
}

impl<T: NodeType> CascadesOptimizer<T, NaiveMemo<T>> {
    pub fn new(
        rules: Vec<Arc<dyn Rule<T, Self>>>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
        physical_property_builders: Arc<[Box<dyn PhysicalPropertyBuilderAny<T>>]>,
    ) -> Self {
        Self::new_with_options(
            rules,
            cost,
            logical_property_builders,
            physical_property_builders,
            Default::default(),
        )
    }

    pub fn new_with_options(
        rules: Vec<Arc<dyn Rule<T, Self>>>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
        physical_property_builders: Arc<[Box<dyn PhysicalPropertyBuilderAny<T>>]>,
        prop: OptimizerProperties,
    ) -> Self {
        let tasks = VecDeque::new();
        let physical_property_builders = PhysicalPropertyBuilders(physical_property_builders);
        let memo = NaiveMemo::new(
            logical_property_builders.clone(),
            physical_property_builders.clone(),
        );
        Self {
            memo,
            tasks,
            explored_group: HashSet::new(),
            explored_expr: HashSet::new(),
            fired_rules: HashMap::new(),
            rules: rules.into(),
            cost: cost.into(),
            ctx: OptimizerContext::default(),
            logical_property_builders,
            physical_property_builders,
            prop,
            disabled_rules: HashSet::new(),
        }
    }

    /// Clear the memo table and all optimizer states.
    pub fn step_clear(&mut self) {
        self.memo = NaiveMemo::new(
            self.logical_property_builders.clone(),
            self.physical_property_builders.clone(),
        );
        self.fired_rules.clear();
        self.explored_group.clear();
        self.explored_expr.clear();
    }

    /// Clear the winner so that the optimizer can continue to explore the group.
    pub fn step_clear_winner(&mut self) {
        self.memo.clear_winner();
        self.explored_expr.clear();
    }
}

impl<T: NodeType, M: Memo<T>> CascadesOptimizer<T, M> {
    pub fn panic_on_explore_limit(&mut self, enabled: bool) {
        self.prop.panic_on_budget = enabled;
    }

    pub fn disable_pruning(&mut self, enabled: bool) {
        self.prop.disable_pruning = enabled;
    }

    pub fn cost(&self) -> Arc<dyn CostModel<T, M>> {
        self.cost.clone()
    }

    pub fn rules(&self) -> Arc<[Arc<dyn Rule<T, Self>>]> {
        self.rules.clone()
    }

    pub fn disable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.insert(rule_id);
    }

    pub fn enable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.remove(&rule_id);
    }

    pub fn is_rule_disabled(&self, rule_id: usize) -> bool {
        self.disabled_rules.contains(&rule_id)
    }

    pub fn dump(&self, mut buf: impl std::fmt::Write) -> std::fmt::Result {
        for group_id in self.memo.get_all_group_ids() {
            for subgroup_id in self.memo.get_all_subgroup_ids(group_id) {
                let winner_str = match &self.memo.get_group_winner(group_id, subgroup_id) {
                    Winner::Impossible => "winner=<impossible>".to_string(),
                    Winner::Unknown => "winner=<unknown>".to_string(),
                    Winner::Full(winner) => {
                        let (winner_expr, winner_str) = match &winner.expr_id {
                            WinnerExpr::Expr { expr_id } => {
                                let expr = self.memo.get_expr_memoed(*expr_id);
                                (format!("{}", expr_id), format!("{}", expr))
                            }
                            WinnerExpr::Propagate { group_id } => {
                                (format!("{}", group_id), format!("{}", group_id))
                            }
                            WinnerExpr::Enforcer {} => ("enforcer".to_string(), "".to_string()),
                        };
                        format!(
                            "winner={} weighted_cost={} | {}\n  cost={}\n  stat={}",
                            winner_expr,
                            winner.total_weighted_cost,
                            winner_str,
                            self.cost.explain_cost(&winner.total_cost),
                            self.cost.explain_statistics(&winner.statistics),
                        )
                    }
                };
                writeln!(
                    buf,
                    "group_id={} subgroup_id={} {}",
                    group_id, subgroup_id, winner_str
                )?;
                let goal = self.memo.get_subgroup_goal(group_id, subgroup_id);
                for (id, property) in self
                    .memo
                    .get_physical_property_builders()
                    .0
                    .iter()
                    .enumerate()
                {
                    writeln!(buf, "  {}={}", property.property_name(), goal[id])?;
                }
                let group = self.memo.get_group(group_id);
                for (id, property) in self.logical_property_builders.iter().enumerate() {
                    writeln!(
                        buf,
                        "  {}={}",
                        property.property_name(),
                        group.logical_properties[id]
                    )?;
                }
                let mut all_predicates = BTreeSet::new();
                for expr_id in self.memo.get_all_exprs_in_group(group_id) {
                    let memo_node = self.memo.get_expr_memoed(expr_id);
                    for pred in &memo_node.predicates {
                        all_predicates.insert(*pred);
                    }
                    writeln!(buf, "  expr_id={} | {}", expr_id, memo_node)?;
                }
                for pred in all_predicates {
                    writeln!(buf, "  {}={}", pred, self.memo.get_pred(pred))?;
                }
            }
        }
        Ok(())
    }

    /// Optimize a `RelNode`.
    pub fn step_optimize_rel(
        &mut self,
        root_rel: ArcPlanNode<T>,
        required_props: &[&dyn PhysicalProperty],
    ) -> Result<(GroupId, SubGroupId)> {
        let (group_id, _) = self.add_new_expr(root_rel);
        let required_props = required_props.iter().map(|x| x.to_boxed()).collect_vec();
        let subgroup_id = self
            .memo
            .create_or_get_subgroup(group_id, required_props.into());
        self.fire_optimize_tasks(group_id, subgroup_id)?;
        Ok((group_id, subgroup_id))
    }

    /// Gets the group binding.
    pub fn step_get_optimize_rel(
        &self,
        group_id: GroupId,
        subgroup_id: SubGroupId,
        meta: &mut Option<PlanNodeMetaMap>,
    ) -> Result<ArcPlanNode<T>> {
        let res = self.memo.get_best_group_binding(
            group_id,
            subgroup_id,
            |node, group_id, subgroup_id, info| {
                if let Some(meta) = meta {
                    let node = node.as_ref() as *const _ as usize;
                    let node_meta = PlanNodeMeta::new(
                        group_id,
                        subgroup_id,
                        info.total_weighted_cost,
                        info.total_cost.clone(),
                        info.statistics.clone(),
                        self.cost.explain_cost(&info.total_cost),
                        self.cost.explain_statistics(&info.statistics),
                    );
                    meta.insert(node, node_meta);
                }
            },
        );
        if res.is_err() && cfg!(debug_assertions) {
            let mut buf = String::new();
            self.dump(&mut buf).unwrap();
            eprintln!("{}", buf);
        }
        res
    }

    fn fire_optimize_tasks(&mut self, group_id: GroupId, subgroup_id: SubGroupId) -> Result<()> {
        trace!(event = "fire_optimize_tasks", root_group_id = %group_id, root_subgroup_id = %subgroup_id);
        self.tasks
            .push_back(Box::new(OptimizeGroupTask::new(group_id, subgroup_id)));
        // get the task from the stack
        self.ctx.budget_used = false;
        let plan_space_begin = self.memo.estimated_plan_space();
        let mut iter = 0;
        while let Some(task) = self.tasks.pop_back() {
            let new_tasks = task.execute(self)?;
            self.tasks.extend(new_tasks);
            iter += 1;
            if !self.ctx.budget_used {
                let plan_space = self.memo.estimated_plan_space();
                if let Some(partial_explore_space) = self.prop.partial_explore_space {
                    if plan_space - plan_space_begin > partial_explore_space {
                        println!(
                            "plan space size budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.ctx.budget_used = true;
                        if self.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                } else if let Some(partial_explore_iter) = self.prop.partial_explore_iter {
                    if iter >= partial_explore_iter {
                        println!(
                            "plan explore iter budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.ctx.budget_used = true;
                        if self.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn optimize_inner(
        &mut self,
        root_rel: ArcPlanNode<T>,
        required_props: &[&dyn PhysicalProperty],
    ) -> Result<ArcPlanNode<T>> {
        let (group_id, _) = self.add_new_expr(root_rel);
        let required_props = required_props.iter().map(|x| x.to_boxed()).collect_vec();
        let subgroup_id = self
            .memo
            .create_or_get_subgroup(group_id, required_props.into());
        self.fire_optimize_tasks(group_id, subgroup_id)?;
        self.memo
            .get_best_group_binding(group_id, subgroup_id, |_, _, _, _| {})
    }

    pub fn resolve_group_id(&self, root_rel: PlanNodeOrGroup<T>) -> GroupId {
        root_rel.unwrap_group()
    }

    pub(super) fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        self.memo.get_all_exprs_in_group(group_id)
    }

    pub fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        self.memo.add_new_expr(rel_node)
    }

    pub fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        self.memo.add_expr_to_group(rel_node, group_id)
    }

    pub(super) fn get_group_winner(&self, group_id: GroupId, subgroup_id: SubGroupId) -> &Winner {
        self.memo.get_group_winner(group_id, subgroup_id)
    }

    pub(super) fn update_group_winner(
        &mut self,
        group_id: GroupId,
        subgroup_id: SubGroupId,
        winner: Winner,
    ) {
        self.memo.update_winner(group_id, subgroup_id, winner)
    }

    /// Get the properties of a Cascades group
    /// P is the type of the property you expect
    /// idx is the idx of the property you want. The order of properties is defined
    ///   by the property_builders parameter in CascadesOptimizer::new()
    pub fn get_property_by_group<P: LogicalPropertyBuilder<T>>(
        &self,
        group_id: GroupId,
        idx: usize,
    ) -> P::Prop {
        self.memo.get_group(group_id).logical_properties[idx]
            .as_any()
            .downcast_ref::<P::Prop>()
            .unwrap()
            .clone()
    }

    pub(super) fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        self.memo.get_group_id(expr_id)
    }

    pub(super) fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T> {
        self.memo.get_expr_memoed(expr_id)
    }

    pub fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T> {
        self.memo.get_pred(pred_id)
    }

    pub(super) fn is_group_explored(&self, group_id: GroupId, subgroup_id: SubGroupId) -> bool {
        self.explored_group.contains(&(group_id, subgroup_id))
    }

    pub(super) fn mark_group_explored(&mut self, group_id: GroupId, subgroup_id: SubGroupId) {
        self.explored_group.insert((group_id, subgroup_id));
    }

    pub(super) fn is_expr_explored(&self, expr_id: ExprId, subgroup_id: SubGroupId) -> bool {
        self.explored_expr.contains(&(expr_id, subgroup_id))
    }

    pub(super) fn mark_expr_explored(&mut self, expr_id: ExprId, subgroup_id: SubGroupId) {
        self.explored_expr.insert((expr_id, subgroup_id));
    }

    pub(super) fn unmark_expr_explored(&mut self, expr_id: ExprId, subgroup_id: SubGroupId) {
        self.explored_expr.remove(&(expr_id, subgroup_id));
    }

    pub(super) fn is_rule_fired(&self, group_expr_id: ExprId, rule_id: RuleId) -> bool {
        self.fired_rules
            .get(&group_expr_id)
            .map(|rules| rules.contains(&rule_id))
            .unwrap_or(false)
    }

    pub(super) fn mark_rule_fired(&mut self, group_expr_id: ExprId, rule_id: RuleId) {
        self.fired_rules
            .entry(group_expr_id)
            .or_default()
            .insert(rule_id);
    }

    pub(crate) fn create_or_get_subgroup(
        &mut self,
        group_id: GroupId,
        required_phys_props: RequiredPhysicalProperties,
    ) -> SubGroupId {
        self.memo
            .create_or_get_subgroup(group_id, required_phys_props)
    }

    pub fn memo(&self) -> &M {
        &self.memo
    }
}

impl<T: NodeType, M: Memo<T>> Optimizer<T> for CascadesOptimizer<T, M> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        let phys_props = self.physical_property_builders.default_many();
        let phys_props_ref = phys_props.iter().map(|x| x.as_ref()).collect_vec();
        self.optimize_with_required_props(root_rel, &phys_props_ref)
    }

    fn optimize_with_required_props(
        &mut self,
        root_rel: ArcPlanNode<T>,
        required_props: &[&dyn PhysicalProperty],
    ) -> Result<ArcPlanNode<T>> {
        self.optimize_inner(root_rel, required_props)
    }

    fn get_logical_property<P: LogicalPropertyBuilder<T>>(
        &self,
        root_rel: PlanNodeOrGroup<T>,
        idx: usize,
    ) -> P::Prop {
        self.get_property_by_group::<P>(self.resolve_group_id(root_rel), idx)
    }
}

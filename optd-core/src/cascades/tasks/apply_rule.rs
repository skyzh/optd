use std::collections::HashMap;

use anyhow::Result;
use itertools::Itertools;
use tracing::trace;

use crate::{
    cascades::{
        memo::ArcMemoPlanNode,
        optimizer::{CascadesOptimizer, ExprId, RuleId},
        tasks::{OptimizeExpressionTask, OptimizeInputsTask},
        GroupId, Memo,
    },
    nodes::{ArcPredNode, NodeType, PlanNode, PlanNodeOrGroup},
    rules::{OptimizeType, RuleMatcher},
};

use super::Task;

pub struct ApplyRuleTask {
    rule_id: RuleId,
    expr_id: ExprId,
    exploring: bool,
}

impl ApplyRuleTask {
    pub fn new(rule_id: RuleId, expr_id: ExprId, exploring: bool) -> Self {
        Self {
            rule_id,
            expr_id,
            exploring,
        }
    }
}

// Pick/match logic, to get pieces of info to pass to the rule apply function
// TODO: I would like to see this moved elsewhere

fn match_node<T: NodeType, M: Memo<T>>(
    typ: &T,
    children: &[RuleMatcher<T>],
    predicates: &[RuleMatcher<T>],
    pick_to: Option<usize>,
    node: ArcMemoPlanNode<T>,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<(
    HashMap<usize, PlanNodeOrGroup<T>>,
    HashMap<usize, ArcPredNode<T>>,
)> {
    if let RuleMatcher::PickMany { .. } | RuleMatcher::IgnoreMany = children.last().unwrap() {
    } else {
        assert_eq!(
            children.len(),
            node.children.len(),
            "children size unmatched, please fix the rule: {}",
            node
        );
    }

    let mut should_end = false;
    let mut picks = vec![(HashMap::new(), HashMap::new())];
    for (idx, child) in children.iter().enumerate() {
        assert!(!should_end, "many matcher should be at the end");
        match child {
            RuleMatcher::IgnoreOne => {}
            RuleMatcher::IgnoreMany => {
                should_end = true;
            }
            RuleMatcher::PickOne { pick_to } => {
                let group_id = node.children[idx];
                let node = PlanNodeOrGroup::Group(group_id);
                for (pick, _) in &mut picks {
                    let res = pick.insert(*pick_to, node.clone());
                    assert!(res.is_none(), "dup pick");
                }
            }
            RuleMatcher::PickMany { .. } => {
                panic!("PickMany not supported currently");
            }
            _ => {
                let new_picks = match_and_pick_group(child, node.children[idx], optimizer);
                let mut merged_picks = vec![];
                for (old_pick, old_pred_pick) in &picks {
                    for (new_pick, new_pred_pick) in &new_picks {
                        let mut pick = old_pick.clone();
                        let mut pred_pick = old_pred_pick.clone();
                        pick.extend(new_pick.iter().map(|(k, v)| (*k, v.clone())));
                        pred_pick.extend(new_pred_pick.iter().map(|(k, v)| (*k, v.clone())));
                        merged_picks.push((pick, pred_pick));
                    }
                }
                picks = merged_picks;
            }
        }
    }
    for (idx, pred) in predicates.iter().enumerate() {
        match pred {
            RuleMatcher::PickPred { pick_to } => {
                for (_, pred_pick) in &mut picks {
                    let res = pred_pick
                        .insert(*pick_to, optimizer.get_pred(node.predicates[idx].clone()));
                    assert!(res.is_none(), "dup pred pick?");
                }
            }
            // TODO: sanity check
            RuleMatcher::IgnoreOne => {}
            RuleMatcher::IgnoreMany => {
                break;
            }
            _ => {
                panic!("only PickPred is supported for predicates");
            }
        }
    }

    if let Some(pick_to) = pick_to {
        for (pick, _) in &mut picks {
            let res: Option<PlanNodeOrGroup<T>> = pick.insert(
                pick_to,
                PlanNodeOrGroup::PlanNode(
                    PlanNode {
                        typ: typ.clone(),
                        children: node
                            .children
                            .iter()
                            .map(|x| PlanNodeOrGroup::Group(*x))
                            .collect_vec(),
                        predicates: node
                            .predicates
                            .iter()
                            .map(|x| optimizer.get_pred(*x))
                            .collect(),
                    }
                    .into(),
                ),
            );
            assert!(res.is_none(), "dup pick");
        }
    }
    picks
}

fn match_and_pick_expr<T: NodeType, M: Memo<T>>(
    matcher: &RuleMatcher<T>,
    expr_id: ExprId,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<(
    HashMap<usize, PlanNodeOrGroup<T>>,
    HashMap<usize, ArcPredNode<T>>,
)> {
    let node = optimizer.get_expr_memoed(expr_id);
    match_and_pick(matcher, node, optimizer)
}

fn match_and_pick_group<T: NodeType, M: Memo<T>>(
    matcher: &RuleMatcher<T>,
    group_id: GroupId,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<(
    HashMap<usize, PlanNodeOrGroup<T>>,
    HashMap<usize, ArcPredNode<T>>,
)> {
    let mut matches = vec![];
    for expr_id in optimizer.get_all_exprs_in_group(group_id) {
        let node = optimizer.get_expr_memoed(expr_id);
        matches.extend(match_and_pick(matcher, node, optimizer));
    }
    matches
}

fn match_and_pick<T: NodeType, M: Memo<T>>(
    matcher: &RuleMatcher<T>,
    node: ArcMemoPlanNode<T>,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<(
    HashMap<usize, PlanNodeOrGroup<T>>,
    HashMap<usize, ArcPredNode<T>>,
)> {
    match matcher {
        RuleMatcher::MatchAndPickNode {
            typ,
            children,
            predicates,
            pick_to,
        } => {
            if &node.typ != typ {
                return vec![];
            }
            match_node(typ, children, predicates, Some(*pick_to), node, optimizer)
        }
        RuleMatcher::MatchNode {
            typ,
            children,
            predicates,
        } => {
            if &node.typ != typ {
                return vec![];
            }
            match_node(typ, children, predicates, None, node, optimizer)
        }
        RuleMatcher::MatchDiscriminant {
            typ_discriminant,
            children,
            predicates,
        } => {
            if std::mem::discriminant(&node.typ) != *typ_discriminant {
                return vec![];
            }
            match_node(
                &node.typ.clone(),
                children,
                predicates,
                None,
                node,
                optimizer,
            )
        }
        RuleMatcher::MatchAndPickDiscriminant {
            typ_discriminant,
            children,
            predicates,
            pick_to,
        } => {
            if std::mem::discriminant(&node.typ) != *typ_discriminant {
                return vec![];
            }
            match_node(
                &node.typ.clone(),
                children,
                predicates,
                Some(*pick_to),
                node,
                optimizer,
            )
        }
        _ => panic!("top node should be match node"),
    }
}

impl<T: NodeType, M: Memo<T>> Task<T, M> for ApplyRuleTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) -> Result<Vec<Box<dyn Task<T, M>>>> {
        if optimizer.is_rule_fired(self.expr_id, self.rule_id) {
            return Ok(vec![]);
        }

        if optimizer.is_rule_disabled(self.rule_id) {
            optimizer.mark_rule_fired(self.expr_id, self.rule_id);
            return Ok(vec![]);
        }

        let rule_wrapper = optimizer.rules()[self.rule_id].clone();
        let rule = rule_wrapper.rule();

        trace!(event = "task_begin", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, rule = %rule.name(), optimize_type=%rule_wrapper.optimize_type());
        let group_id = optimizer.get_group_id(self.expr_id);
        let mut tasks = vec![];
        let binding_exprs = match_and_pick_expr(rule.matcher(), self.expr_id, optimizer);
        for (expr, preds) in binding_exprs {
            trace!(event = "before_apply_rule", task = "apply_rule");
            let applied = rule.apply(optimizer, expr, preds);

            if rule_wrapper.optimize_type() == OptimizeType::Heuristics {
                panic!("no more heuristics rule in cascades");
            }

            for expr in applied {
                trace!(event = "after_apply_rule", task = "apply_rule", binding=%expr);
                // TODO: remove clone in the below line
                if let Some(expr_id) = optimizer.add_expr_to_group(expr.clone(), group_id) {
                    let typ = expr.unwrap_typ();
                    if typ.is_logical() {
                        tasks.push(
                            Box::new(OptimizeExpressionTask::new(expr_id, self.exploring))
                                as Box<dyn Task<T, M>>,
                        );
                    } else {
                        tasks
                            .push(Box::new(OptimizeInputsTask::new(expr_id, true))
                                as Box<dyn Task<T, M>>);
                    }
                    optimizer.unmark_expr_explored(expr_id);
                    trace!(event = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, new_expr_id = %expr_id);
                } else {
                    trace!(event = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, "triggered group merge");
                }
            }
        }
        optimizer.mark_rule_fired(self.expr_id, self.rule_id);

        trace!(event = "task_end", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!(
            "apply_rule {{ rule_id: {}, expr_id: {}, exploring: {} }}",
            self.rule_id, self.expr_id, self.exploring
        )
    }
}

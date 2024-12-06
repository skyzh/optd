use std::borrow::Borrow;

use optd_core::{
    nodes::NodeType,
    physical_property::{PhysicalProperty, PhysicalPropertyBuilder},
};

use crate::plan_nodes::{
    ArcDfPredNode, ColumnRefPred, DfNodeType, DfReprPredNode, ListPred, SortOrderPred,
    SortOrderType,
};

pub struct SortPropertyBuilder;

impl SortPropertyBuilder {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum SortPropType {
    AnySorted, // Only used as required prop, Asc/Desc all satisfies this
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SortProp(pub Vec<(SortPropType, usize)>);

impl std::fmt::Display for SortProp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return write!(f, "<any>");
        } else {
            write!(f, "[")?;
            for (idx, (order, col)) in self.0.iter().enumerate() {
                if idx != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{:?}#{}", order, col)?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

impl PhysicalProperty for SortProp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_boxed(&self) -> Box<dyn PhysicalProperty> {
        Box::new(self.clone())
    }
}

impl SortProp {
    pub fn any_order() -> Self {
        SortProp(vec![])
    }

    pub fn satisfies(prop: &SortProp, required: &SortProp) -> bool {
        // required should be a prefix of the current property
        for i in 0..required.0.len() {
            if i >= prop.0.len() {
                return false;
            }
            if prop.0[i].1 != required.0[i].1 {
                return false;
            }
            match (prop.0[i].0, required.0[i].0) {
                (SortPropType::AnySorted, SortPropType::AnySorted)
                | (SortPropType::Asc, SortPropType::Asc)
                | (SortPropType::Desc, SortPropType::Desc)
                | (SortPropType::Asc, SortPropType::AnySorted)
                | (SortPropType::Desc, SortPropType::AnySorted) => {}
                (SortPropType::Asc, SortPropType::Desc)
                | (SortPropType::Desc, SortPropType::Asc)
                | (SortPropType::AnySorted, SortPropType::Asc)
                | (SortPropType::AnySorted, SortPropType::Desc) => return false,
            }
        }
        true
    }

    fn from_sort_order_predicates(preds: ListPred) -> Option<Self> {
        let mut columns = Vec::new();
        for pred in preds.to_vec() {
            let order = SortOrderPred::from_pred_node(pred).unwrap();
            // TODO: return None in case we sort by an expression
            let col_ref = ColumnRefPred::from_pred_node(order.child()).unwrap();
            let order = match order.order() {
                SortOrderType::Asc => SortPropType::Asc,
                SortOrderType::Desc => SortPropType::Desc,
            };
            columns.push((order, col_ref.index()));
        }
        Some(SortProp(columns))
    }

    fn from_list_predicates(preds: ListPred, required: &SortProp) -> Option<Self> {
        let mut columns = Vec::new();
        let mut take_from_required = true;
        for (idx, pred) in preds.to_vec().into_iter().enumerate() {
            // TODO: return None in case we sort by an expression
            let col_ref = ColumnRefPred::from_pred_node(pred).unwrap();
            if idx >= required.0.len() {
                take_from_required = false;
            }
            if take_from_required && col_ref.index() != required.0[idx].1 {
                take_from_required = false;
            }
            let order_req = if take_from_required {
                required.0[idx].0
            } else {
                SortPropType::AnySorted
            };
            columns.push((order_req, col_ref.index()));
        }
        Some(SortProp(columns))
    }
}

impl PhysicalPropertyBuilder<DfNodeType> for SortPropertyBuilder {
    type Prop = SortProp;

    fn derive(
        &self,
        typ: DfNodeType,
        predicates: &[ArcDfPredNode],
        children: &[impl Borrow<Self::Prop>],
    ) -> Self::Prop {
        match typ {
            DfNodeType::PhysicalSort => {
                match SortProp::from_sort_order_predicates(
                    ListPred::from_pred_node(predicates[0].clone()).unwrap(),
                ) {
                    Some(prop) => prop,
                    None => SortProp::any_order(),
                }
            }
            DfNodeType::PhysicalStreamAgg => {
                // child props should at least satisfy the group by columns as we passthroughed them
                let group_by_len = predicates[1].children.len();
                let child_sorts = &children[0].borrow().0;
                assert!(child_sorts.len() >= group_by_len);
                let mut sorts = Vec::new();
                // the output is always sorted by group by columns (0, 1, 2, ...)
                #[allow(clippy::needless_range_loop)]
                for i in 0..predicates[1].children.len() {
                    sorts.push((child_sorts[i].0, i));
                }
                SortProp(sorts)
            }
            DfNodeType::PhysicalFilter | DfNodeType::PhysicalLimit => children[0].borrow().clone(),
            DfNodeType::PhysicalHashJoin(_) => SortProp::any_order(),
            DfNodeType::PhysicalProjection => SortProp::any_order(),
            // Actually, the current task framework doesn't double-check whether enforcers change the properties...
            DfNodeType::PhysicalGather => SortProp::any_order(), // Gather doesn't preserve order, need to add MergeGather
            DfNodeType::PhysicalHashShuffle => children[0].borrow().clone(), // HashShuffle preserves order (in datafusion?)
            _ if typ.is_logical() => unreachable!("logical node should not be called"),
            _ => SortProp::any_order(),
        }
    }

    fn passthrough(
        &self,
        typ: DfNodeType,
        predicates: &[ArcDfPredNode],
        required: &Self::Prop,
    ) -> Vec<Self::Prop> {
        match typ {
            DfNodeType::PhysicalFilter | DfNodeType::PhysicalLimit => vec![required.clone()],
            DfNodeType::PhysicalHashAgg => vec![SortProp::any_order()],
            DfNodeType::PhysicalHashJoin(_) | DfNodeType::PhysicalNestedLoopJoin(_) => {
                vec![SortProp::any_order(), SortProp::any_order()]
            }
            DfNodeType::PhysicalScan | DfNodeType::PhysicalEmptyRelation => vec![],
            DfNodeType::PhysicalProjection => vec![SortProp::any_order()],
            DfNodeType::PhysicalSort => {
                let this_prop = SortProp::from_sort_order_predicates(
                    ListPred::from_pred_node(predicates[0].clone()).unwrap(),
                );
                match this_prop {
                    Some(this_prop) if self.satisfies(required, &this_prop) => {
                        vec![this_prop]
                    }
                    _ => vec![SortProp::any_order()],
                }
            }
            DfNodeType::PhysicalStreamAgg => {
                let group_by = SortProp::from_list_predicates(
                    ListPred::from_pred_node(predicates[1].clone()).unwrap(),
                    required,
                )
                .unwrap();
                vec![group_by]
            }
            _ if typ.is_logical() => unreachable!("logical node should not be called"),
            node => unimplemented!("passthrough for {:?}", node),
        }
    }

    fn satisfies(&self, prop: &SortProp, required: &SortProp) -> bool {
        SortProp::satisfies(prop, required)
    }

    fn default(&self) -> Self::Prop {
        SortProp::any_order()
    }

    fn search_goal(
        &self,
        typ: DfNodeType,
        predicates: &[ArcDfPredNode],
        required: &Self::Prop,
    ) -> Option<Self::Prop> {
        match typ {
            DfNodeType::Sort => {
                let prop = SortProp::from_sort_order_predicates(
                    ListPred::from_pred_node(predicates[0].clone()).unwrap(),
                );
                match prop {
                    Some(prop) if SortProp::satisfies(&prop, required) => Some(prop),
                    Some(prop) if SortProp::satisfies(required, &prop) => Some(required.clone()),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn enforce(&self, prop: &Self::Prop) -> (DfNodeType, Vec<ArcDfPredNode>) {
        let mut predicates = Vec::new();
        for (order, col_idx) in &prop.0 {
            let order = match order {
                SortPropType::Asc => SortOrderType::Asc,
                SortPropType::Desc => SortOrderType::Desc,
                SortPropType::AnySorted => SortOrderType::Asc,
            };
            predicates.push(
                SortOrderPred::new(order, ColumnRefPred::new(*col_idx).into_pred_node())
                    .into_pred_node(),
            );
        }
        (
            DfNodeType::PhysicalSort,
            vec![ListPred::new(predicates).into_pred_node()],
        )
    }

    fn property_name(&self) -> &'static str {
        "sort"
    }
}

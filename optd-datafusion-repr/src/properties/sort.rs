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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortProp(pub Vec<(SortOrderType, usize)>);

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
            if i >= prop.0.len() || prop.0[i] != required.0[i] {
                return false;
            }
        }
        true
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
                let mut columns = Vec::new();
                let preds = ListPred::from_pred_node(predicates[0].clone()).unwrap();
                for pred in preds.to_vec() {
                    let order = SortOrderPred::from_pred_node(pred).unwrap();
                    let col_ref = ColumnRefPred::from_pred_node(order.child()).unwrap();
                    columns.push((order.order(), col_ref.index()));
                }
                SortProp(columns)
            }
            DfNodeType::PhysicalFilter => children[0].borrow().clone(),
            DfNodeType::PhysicalHashJoin(_) => SortProp::any_order(),
            DfNodeType::PhysicalProjection => SortProp::any_order(),
            _ if typ.is_logical() => unreachable!("logical node should not be called"),
            _ => SortProp::any_order(),
        }
    }

    fn passthrough(
        &self,
        typ: DfNodeType,
        _: &[ArcDfPredNode],
        required: &Self::Prop,
    ) -> Vec<Self::Prop> {
        match typ {
            DfNodeType::PhysicalFilter => vec![required.clone()],
            DfNodeType::PhysicalHashAgg | DfNodeType::PhysicalLimit => vec![SortProp::any_order()],
            DfNodeType::PhysicalHashJoin(_) | DfNodeType::PhysicalNestedLoopJoin(_) => {
                vec![SortProp::any_order(), SortProp::any_order()]
            }
            DfNodeType::PhysicalScan => vec![],
            DfNodeType::PhysicalProjection => vec![SortProp::any_order()],
            DfNodeType::PhysicalSort => vec![SortProp::any_order()],
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

    fn search_goal(&self, typ: DfNodeType, predicates: &[ArcDfPredNode]) -> Option<Self::Prop> {
        match typ {
            DfNodeType::PhysicalSort => {
                let mut columns = Vec::new();
                let preds = ListPred::from_pred_node(predicates[0].clone()).unwrap();
                for pred in preds.to_vec() {
                    let order = SortOrderPred::from_pred_node(pred).unwrap();
                    let col_ref = ColumnRefPred::from_pred_node(order.child()).unwrap();
                    columns.push((order.order(), col_ref.index()));
                }
                Some(SortProp(columns))
            }
            _ => None,
        }
    }

    fn enforce(&self, prop: &Self::Prop) -> (DfNodeType, Vec<ArcDfPredNode>) {
        let mut predicates = Vec::new();
        for (order, col_idx) in &prop.0 {
            predicates.push(
                SortOrderPred::new(*order, ColumnRefPred::new(*col_idx).into_pred_node())
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

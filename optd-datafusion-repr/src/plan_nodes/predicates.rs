mod between_pred;
mod bin_op_pred;
mod cast_pred;
mod column_ref_pred;
mod constant_pred;
mod data_type_pred;
mod extern_column_ref_pred;
mod func_pred;
mod in_list_pred;
mod like_pred;
mod list_pred;
mod log_op_pred;
mod sort_order_pred;
mod un_op_pred;

use std::sync::Arc;

pub use between_pred::BetweenPred;
pub use bin_op_pred::{BinOpPred, BinOpType};
pub use cast_pred::CastPred;
pub use column_ref_pred::ColumnRefPred;
pub use constant_pred::{ConstantPred, ConstantType};
pub use data_type_pred::DataTypePred;
pub use extern_column_ref_pred::ExternColumnRefPred;
pub use func_pred::{FuncPred, FuncType};
pub use in_list_pred::InListPred;
pub use like_pred::LikePred;
pub use list_pred::ListPred;
pub use log_op_pred::{LogOpPred, LogOpType};
use optd_core::nodes::PredNode;
pub use sort_order_pred::{SortOrderPred, SortOrderType};
pub use un_op_pred::{UnOpPred, UnOpType};

use super::{ArcDfPredNode, DfReprPredNode};

/// Recursively rewrite all column references in the expression.using a provided
/// function that replaces a column index.
/// The provided function will, given a ColumnRefExpr's index,
/// return either Some(usize) or None.
/// - If it is Some, the column index can be rewritten with the value.
/// - If any of the columns is None, we will return None all the way up
/// the call stack, and no expression will be returned.
pub fn rewrite_column_refs(
    expr: ArcDfPredNode,
    rewrite_fn: &mut impl FnMut(usize) -> Option<usize>,
) -> Option<ArcDfPredNode> {
    if let Some(col_ref) = ColumnRefPred::from_pred_node(expr.clone()) {
        let rewritten = rewrite_fn(col_ref.index());
        return if let Some(rewritten_idx) = rewritten {
            let new_col_ref = ColumnRefPred::new(rewritten_idx);
            Some(new_col_ref.into_pred_node())
        } else {
            None
        };
    }
    let children = expr
        .children
        .iter()
        .map(|child| {
            if let Some(list) = ListPred::from_pred_node(child.clone()) {
                return Some(
                    ListPred::new(
                        list.to_vec()
                            .into_iter()
                            .map(|x| rewrite_column_refs(x, rewrite_fn).unwrap())
                            .collect(),
                    )
                    .into_pred_node(),
                );
            }
            rewrite_column_refs(child.clone(), rewrite_fn)
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Arc::new(PredNode {
        typ: expr.typ.clone(),
        children,
        data: expr.data.clone(),
    }))
}

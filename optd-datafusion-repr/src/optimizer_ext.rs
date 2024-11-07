use optd_core::{nodes::PlanNodeOrGroup, optimizer::Optimizer};

use crate::{
    plan_nodes::DfNodeType,
    properties::schema::{Schema, SchemaPropertyBuilder},
};

pub trait OptimizerExt {
    fn get_schema_of(&self, root_rel: PlanNodeOrGroup<DfNodeType>) -> Schema;
}

impl<O: Optimizer<DfNodeType>> OptimizerExt for O {
    fn get_schema_of(&self, root_rel: PlanNodeOrGroup<DfNodeType>) -> Schema {
        self.get_property::<SchemaPropertyBuilder>(root_rel, 0)
    }
}

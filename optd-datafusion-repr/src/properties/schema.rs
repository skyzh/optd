use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use optd_core::property::PropertyBuilder;

use crate::plan_nodes::{ConstantType, OptRelNodeTyp};
use datafusion::arrow::datatypes::{DataType, Field, Schema as DatafusionSchema};

#[derive(Clone, Debug)]
pub struct Schema(pub Vec<ConstantType>);

// TODO: add names, nullable to schema
impl Schema {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Into<DatafusionSchema> for Schema {
    // TODO: current DataType and ConstantType are not 1 to 1 mapping
    // optd schema stores constantType from data type in catalog.get
    // for decimal128, the precision is lost
    fn into(self) -> DatafusionSchema {
        let match_type = |typ: &ConstantType| match typ {
            ConstantType::Any => unimplemented!(),
            ConstantType::Bool => DataType::Boolean,
            ConstantType::Int => DataType::Int64,
            ConstantType::Date => DataType::Date32,
            ConstantType::Decimal => DataType::Float64,
            ConstantType::Utf8String => DataType::Utf8,
        };
        let fields : Vec<_> = self
            .0
            .iter()
            .enumerate()
            .map(|(i, typ)| Field::new(&format!("c{}", i), match_type(typ), false))
            .collect();
        DatafusionSchema::new(fields)
    }
}

pub trait Catalog: Send + Sync + 'static {
    fn get(&self, name: &str) -> Schema;
}

pub struct SchemaPropertyBuilder {
    catalog: Box<dyn Catalog>,
}

impl SchemaPropertyBuilder {
    pub fn new(catalog: Box<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

impl PropertyBuilder<OptRelNodeTyp> for SchemaPropertyBuilder {
    type Prop = Schema;

    fn derive(
        &self,
        typ: OptRelNodeTyp,
        data: Option<optd_core::rel_node::Value>,
        children: &[&Self::Prop],
    ) -> Self::Prop {
        match typ {
            OptRelNodeTyp::Scan => {
                let name = data.unwrap().as_str().to_string();
                self.catalog.get(&name)
            }
            OptRelNodeTyp::Projection => children[1].clone(),
            OptRelNodeTyp::Filter => children[0].clone(),
            OptRelNodeTyp::Join(_) => {
                let mut schema = children[0].clone();
                schema.0.extend(children[1].clone().0);
                schema
            }
            OptRelNodeTyp::List => Schema(vec![ConstantType::Any; children.len()]),
            _ => Schema(vec![]),
        }
    }

    fn property_name(&self) -> &'static str {
        "schema"
    }
}

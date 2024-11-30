// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::any::Any;
use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use itertools::Itertools;

use crate::nodes::{ArcPredNode, NodeType, PlanNode, PlanNodeOrGroup};

/// The trait enables we store any physical property in the memo table by erasing the concrete type.
/// In the future, we can implement `serialize`/`deserialize` on this trait so that we can serialize
/// the physical properties.
pub trait PhysicalProperty: 'static + Any + Send + Sync + Debug + Display {
    fn as_any(&self) -> &dyn Any;
    fn to_boxed(&self) -> Box<dyn PhysicalProperty>;
}

/// A wrapper around the `PhysicalPropertyBuilder` so that we can erase the concrete type and store
/// it safely in the memo table.
pub trait PhysicalPropertyBuilderAny<T: NodeType>: 'static + Send + Sync {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn PhysicalProperty],
    ) -> Box<dyn PhysicalProperty>;

    fn passthrough_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required_property: &dyn PhysicalProperty,
    ) -> Vec<Box<dyn PhysicalProperty>>;

    fn can_passthrough_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required_property: &dyn PhysicalProperty,
    ) -> bool;

    fn satisfies_any(&self, prop: &dyn PhysicalProperty, required: &dyn PhysicalProperty) -> bool;

    fn enforce_any(&self, prop: &dyn PhysicalProperty) -> (T, Vec<ArcPredNode<T>>);

    fn search_goal_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required_property: &dyn PhysicalProperty,
    ) -> Option<Box<dyn PhysicalProperty>>;

    fn default_any(&self) -> Box<dyn PhysicalProperty>;

    fn property_name(&self) -> &'static str;

    fn exactly_eq_any(&self, a: &dyn PhysicalProperty, b: &dyn PhysicalProperty) -> bool;

    fn hash_to_u64(&self, prop: &dyn PhysicalProperty) -> u64;
}

/// The trait for building physical properties for a plan node.
pub trait PhysicalPropertyBuilder<T: NodeType>: 'static + Send + Sync + Sized {
    type Prop: PhysicalProperty + Clone + Sized + PartialEq + Eq + Hash;

    /// Derive the output physical property based on the input physical properties and the current plan node information.
    fn derive(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[impl Borrow<Self::Prop>],
    ) -> Self::Prop;

    /// Passsthrough the `required` properties to the children if possible. Returns the derived properties for each child.
    /// If nothing can be passthroughed, simply return the default properties for each child.
    fn passthrough(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: &Self::Prop,
    ) -> Vec<Self::Prop>;

    /// Check if the input physical property can always be passed through to the output physical property. This is done
    /// by check if `satisfies(derive(passthrough(input)), input)`. The implementor can override this function to provide
    /// a more efficient implementation. If the plan node always satisfies a property (i.e., sort always satisfies sort
    /// property), then this function should also return true.
    fn can_passthrough(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: &Self::Prop,
    ) -> bool {
        let inputs = self.passthrough(typ.clone(), predicates, required);
        let derived = self.derive(typ, predicates, &inputs);
        self.satisfies(&derived, required)
    }

    /// Check if the input physical property satisfies the required output physical property.
    fn satisfies(&self, prop: &Self::Prop, required: &Self::Prop) -> bool;

    /// Enforce the required output physical property on the input plan node.
    fn enforce(&self, prop: &Self::Prop) -> (T, Vec<ArcPredNode<T>>);

    /// Convert a node back to a search goal (only used in Cascades).
    ///
    /// For example, sort <group> <orders> can be converted to a search goal of requiring <orders> over the <group>.
    fn search_goal(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required_property: &Self::Prop,
    ) -> Option<Self::Prop> {
        let _ = typ;
        let _ = predicates;
        let _ = required_property;
        // The default implementation indicates this physical property cannot be converted back to a search goal.
        None
    }

    /// Represents no requirement on a property.
    fn default(&self) -> Self::Prop;

    fn property_name(&self) -> &'static str;

    fn exactly_eq(&self, a: &Self::Prop, b: &Self::Prop) -> bool {
        a == b
    }
}

impl<T: NodeType, P: PhysicalPropertyBuilder<T>> PhysicalPropertyBuilderAny<T> for P {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn PhysicalProperty],
    ) -> Box<dyn PhysicalProperty> {
        let children: Vec<&P::Prop> = children
            .iter()
            .map(|child| {
                child
                    .as_any()
                    .downcast_ref::<P::Prop>()
                    .expect("Failed to downcast child")
            })
            .collect();
        Box::new(self.derive(typ, predicates, &children))
    }

    fn passthrough_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: &dyn PhysicalProperty,
    ) -> Vec<Box<dyn PhysicalProperty>> {
        let required = required
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.passthrough(typ, predicates, required)
            .into_iter()
            .map(|prop| Box::new(prop) as Box<dyn PhysicalProperty>)
            .collect()
    }

    fn can_passthrough_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required_property: &dyn PhysicalProperty,
    ) -> bool {
        let required = required_property
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.can_passthrough(typ, predicates, required)
    }

    fn satisfies_any(&self, prop: &dyn PhysicalProperty, required: &dyn PhysicalProperty) -> bool {
        let prop = prop
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        let required = required
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.satisfies(prop, required)
    }

    fn enforce_any(&self, prop: &dyn PhysicalProperty) -> (T, Vec<ArcPredNode<T>>) {
        let prop = prop
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        self.enforce(prop)
    }

    fn search_goal_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required_property: &dyn PhysicalProperty,
    ) -> Option<Box<dyn PhysicalProperty>> {
        let required_property = required_property
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.search_goal(typ, predicates, required_property)
            .map(|prop| Box::new(prop) as Box<dyn PhysicalProperty>)
    }

    fn default_any(&self) -> Box<dyn PhysicalProperty> {
        Box::new(self.default())
    }

    fn exactly_eq_any(&self, a: &dyn PhysicalProperty, b: &dyn PhysicalProperty) -> bool {
        let a = a
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property a");
        let b = b
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property b");
        self.exactly_eq(a, b)
    }

    fn property_name(&self) -> &'static str {
        PhysicalPropertyBuilder::property_name(self)
    }

    fn hash_to_u64(&self, prop: &dyn PhysicalProperty) -> u64 {
        let prop = prop
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        let mut hasher = DefaultHasher::new();
        prop.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Clone)]
pub struct PhysicalPropertyBuilders<T: NodeType>(pub Arc<[Box<dyn PhysicalPropertyBuilderAny<T>>]>);

/// Represents a set of physical properties for a specific plan node
pub type PhysicalPropertySet = Vec<Box<dyn PhysicalProperty>>;

impl<T: NodeType> PhysicalPropertyBuilders<T> {
    pub fn new_empty_for_test() -> Self {
        PhysicalPropertyBuilders(Arc::new([]))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Takes children_len x props_len (input properties for each child) and returns props_len derived properties
    pub fn derive_many<X, Y, Z>(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: Z,
        children_len: usize,
    ) -> PhysicalPropertySet
    where
        X: Borrow<dyn PhysicalProperty>,
        Y: AsRef<[X]>,
        Z: AsRef<[Y]>,
    {
        if let Some(first) = children.as_ref().first() {
            assert_eq!(first.as_ref().len(), self.0.len())
        }
        let children = children.as_ref();
        assert_eq!(children.len(), children_len);
        let mut derived_prop = Vec::with_capacity(self.0.len());
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let children = children
                .iter()
                .map(|child| child.as_ref()[i].borrow())
                .collect_vec();
            let prop = builder.derive_any(typ.clone(), predicates, &children);
            derived_prop.push(prop);
        }
        derived_prop
    }

    /// Returns children_len x props_len (required properties for each child)
    pub fn passthrough_many_no_required_property(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children_len: usize,
    ) -> Vec<PhysicalPropertySet> {
        let required_prop = self
            .0
            .iter()
            .map(|builder| builder.default_any())
            .collect_vec();
        self.passthrough_many(typ, predicates, required_prop, children_len)
    }

    /// Returns children_len x props_len (required properties for each child)
    pub fn passthrough_many<X, Y>(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: Y,
        children_len: usize,
    ) -> Vec<PhysicalPropertySet>
    where
        X: Borrow<dyn PhysicalProperty>,
        Y: AsRef<[X]>,
    {
        let required = required.as_ref();
        assert_eq!(self.0.len(), required.len());
        let mut required_prop = Vec::with_capacity(children_len);
        required_prop.resize_with(children_len, Vec::new);
        #[allow(clippy::needless_range_loop)]
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let required_1 = builder.passthrough_any(typ.clone(), predicates, required[i].borrow());
            assert_eq!(
                required_1.len(),
                children_len,
                "required properties length mismatch: passthrough {} != children_num {} for property {} and plan node typ {}",
                required_1.len(),
                children_len,
                builder.property_name(),
                typ
            );
            for (child_idx, child_prop) in required_1.into_iter().enumerate() {
                required_prop[child_idx].push(child_prop);
            }
        }
        required_prop
    }

    pub fn can_passthrough_any_many<X, Y>(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: Y,
    ) -> bool
    where
        X: Borrow<dyn PhysicalProperty>,
        Y: AsRef<[X]>,
    {
        let required = required.as_ref();
        assert_eq!(self.0.len(), required.len());
        #[allow(clippy::needless_range_loop)]
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            if !builder.can_passthrough_any(typ.clone(), predicates, required[i].borrow()) {
                return false;
            }
        }
        true
    }

    pub fn default_many(&self) -> PhysicalPropertySet {
        self.0.iter().map(|builder| builder.default_any()).collect()
    }

    pub fn satisfies_many<X1, Y1, X2, Y2>(&self, prop: Y1, required: Y2) -> bool
    where
        X1: Borrow<dyn PhysicalProperty>,
        Y1: AsRef<[X1]>,
        X2: Borrow<dyn PhysicalProperty>,
        Y2: AsRef<[X2]>,
    {
        let required = required.as_ref();
        let prop = prop.as_ref();
        assert_eq!(required.len(), self.0.len());
        assert_eq!(prop.len(), self.0.len());
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let required = required[i].borrow();
            let prop = prop[i].borrow();
            if !builder.satisfies_any(prop, required) {
                return false;
            }
        }
        true
    }

    /// Enforces the required properties on the input plan node if not satisfied.
    pub fn enforce_many_if_not_satisfied<X1, Y1, X2, Y2>(
        &self,
        child: PlanNodeOrGroup<T>,
        input_props: Y1,
        required_props: Y2,
    ) -> (PlanNodeOrGroup<T>, Vec<Box<dyn PhysicalProperty>>)
    where
        X1: Borrow<dyn PhysicalProperty>,
        Y1: AsRef<[X1]>,
        X2: Borrow<dyn PhysicalProperty>,
        Y2: AsRef<[X2]>,
    {
        let input_props = input_props.as_ref();
        let required_props = required_props.as_ref();
        let mut new_props = input_props
            .iter()
            .map(|x| x.borrow().to_boxed())
            .collect_vec();

        assert_eq!(self.0.len(), input_props.len());
        assert_eq!(self.0.len(), required_props.len());
        let mut child = child;
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let input_prop = input_props[i].borrow();
            let required_prop = required_props[i].borrow();
            if !builder.satisfies_any(input_prop, required_prop) {
                let (typ, predicates) = builder.enforce_any(required_prop);
                child = PlanNodeOrGroup::PlanNode(Arc::new(PlanNode {
                    typ,
                    predicates,
                    children: vec![child],
                }));
                // TODO: check if the new plan node really enforced the property by deriving in the very end;
                // enforcing new properties might cause the old properties to be lost. For example, order matters
                // when enforcing gather + sort.
                new_props[i] = required_prop.to_boxed();
            }
        }
        (child, new_props)
    }

    pub fn exactly_eq<X1, Y1, X2, Y2>(&self, a: Y1, b: Y2) -> bool
    where
        X1: Borrow<dyn PhysicalProperty>,
        Y1: AsRef<[X1]>,
        X2: Borrow<dyn PhysicalProperty>,
        Y2: AsRef<[X2]>,
    {
        let a = a.as_ref();
        let b = b.as_ref();
        assert_eq!(a.len(), self.0.len());
        assert_eq!(b.len(), self.0.len());
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let a = a[i].borrow();
            let b = b[i].borrow();
            if !builder.exactly_eq_any(a, b) {
                return false;
            }
        }
        true
    }

    pub fn hash_any<X: Borrow<dyn PhysicalProperty>, H: std::hash::Hasher>(
        &self,
        props: &[X],
        state: &mut H,
    ) {
        for (i, prop) in props.iter().enumerate() {
            let builder = &self.0[i];
            let prop = prop.borrow();
            state.write_u64(builder.hash_to_u64(prop));
        }
    }
}

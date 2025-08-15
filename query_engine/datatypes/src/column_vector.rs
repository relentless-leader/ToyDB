use crate::arrow_types::ArrowTypes;
use std::any::Any;

pub trait ColumnVector {
    fn get_type(&self) -> &ArrowTypes;
    fn get_value(&self, i: usize) -> Option<&dyn Any>;
    fn size(&self) -> usize;
}

pub struct LiteralValueVector {
    arrow_type: ArrowTypes,
    value: Option<Box<dyn Any>>,
    size: usize,
}

impl LiteralValueVector {
    pub fn new(arrow_type: ArrowTypes, value: Option<Box<dyn Any>>, size: usize) -> Self {
        Self {
            arrow_type,
            value,
            size,
        }
    }
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> &ArrowTypes {
        &self.arrow_type
    }

    fn get_value(&self, i: usize) -> Option<&dyn Any> {
        if i < 0 || i >= self.size {
            panic!("Index out of bound: {}", i);
        }
        self.value.as_deref()
    }

    fn size(&self) -> usize {
        self.size
    }
}

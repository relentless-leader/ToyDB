trait ColumnVector<T> {
    fn get_type(&self) -> ArrowTypes;
    fn get_value(&self, i: usize) -> T;
    fn size(&self) -> usize;
}

struct LiteralValueVector<T: Clone> {
    arrow_type: ArrowTypes,
    value: T,
    size: usize,
}

impl<T:Clone> LiteralValueVector<T> {
    pub fn new(arrow_type:ArrowTypes, value: T, size: usize) -> Self {
        Self {
            arrow_type,
            value,
            usize,
        }
    }
}

impl<T: Clone> ColumnVector<T> for LiteralValueVector<T> {
    fn get_type(&self) -> ArrowTypes {
        self.arrow_type.clone()
    }

    fn get_value(&self, i) -> T {
        if i >= self.size {
            panic!("Index out of bound: {}", i);
        }
        self.value.clone()
    }

    fn size(&self) {
        self.size
    }
}

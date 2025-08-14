pub enum ArrowTypes {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
}

impl From<&arrow_schema::DataType> for ArrowTypes {
    fn from(dt: &arrow_schema::DataType) -> Self {
        match dt {
            arrow_schema::DataType::Boolean => Self::Boolean,
            arrow_schema::DataType::Int8 => Self::Int8,
            arrow_schema::DataType::Int16 => Self::Int16,
            arrow_schema::DataType::Int32 => Self::Int32,
            arrow_schema::DataType::Int64 => Self::Int64,
            arrow_schema::DataType::UInt8 => Self::UInt8,
            arrow_schema::DataType::UInt16 => Self::UInt16,
            arrow_schema::DataType::UInt32 => Self::UInt32,
            arrow_schema::DataType::UInt64 => Self::UInt64,
            arrow_schema::DataType::Float32 => Self::Float32,
            arrow_schema::DataType::Float64 => Self::Float64,
            arrow_schema::DataType::Utf8 => Self::Utf8,
            _ => unimplemented!("Type not supported"),
        }
    }
}

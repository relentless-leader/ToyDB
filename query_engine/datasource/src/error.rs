use std::result;

pub type Result<T> = result::Result<T, ExecutionError>;

#[derive(Debug)]
pub enum ExecutionError {
    ExecutionError(String),
}

use std::fs::File;

use arrow::csv;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::string::String;
use std::sync::Arc;

use crate::datasource::{ScanResult, TableProvider};
use crate::error::Result;

pub struct ParquetTable {
    path: String,
    schema: Arc<Schema>,
}

impl ParquetTable {
    pub fn try_new(path: &str) -> Result<Self> {
        let parquet_exec = ParquetExec::try_new(path, None, 0).unwrap();
        let schema = parquet_exec.schema();
        Ok(Self {
            path: path.to_string(),
            schema,
        })
    }
}

impl TableProvider for ParquetTable {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> Result<Vec<ScanResult>> {
        let mut filenames: Vec<String> = vec![];
        panic!("To write the executor");
    }
}

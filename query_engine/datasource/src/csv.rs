use std::fs::File;

use arrow::csv;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::string::String;
use std::sync::Arc;

use crate::datasource::{ScanResult, TableProvider};
use crate::error::Result;

pub struct CsvFile {
    filename: String,
    schema: Arc<Schema>,
    has_header: bool,
}

impl CsvFile {
    pub fn new(filename: &str, schema: &Schema, has_header: bool) -> Self {
        Self {
            filename: String::from(filename),
            schema: Arc::new(schema.clone()),
            has_header,
        }
    }
}

impl TableProvider for CsvFile {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> Result<Vec<ScanResult>> {
        // Execute physical plan.
        panic!("Not implemented");
    }
}

pub struct CsvBatchIterator {
    schema: Arc<Schema>,
    reader: csv::Reader<File>,
}

impl CsvBatchIterator {
    pub fn try_new(
        filename: &str,
        schema: Arc<Schema>,
        has_header: bool,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(filename).unwrap();
        let reader = csv::ReaderBuilder::new(schema.clone())
            .with_header(has_header)
            .with_batch_size(batch_size)
            .with_projection(projection.clone().unwrap())
            .build(file)
            .unwrap();

        let projected_schema = match projection {
            Some(p) => {
                let projected_fields: Vec<Arc<Field>> =
                    p.iter().map(|i| schema.fields()[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => schema,
        };

        Ok(Self {
            schema: projected_schema,
            reader,
        })
    }
}

use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;

use arrow::datatypes::Schema;
use common::error::{ExecutionError, Result};
use common::record_batch_iterator::BatchIterator;
use common::record_batch_iterator::build_file_list;
use parquet::file::reader::SerializedFileReader;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub struct ParquetExec {
    filenames: Vec<String>,
    schema: Arc<Schema>,
    projection: Vec<usize>,
    batch_size: usize,
}

impl ParquetExec {
    pub fn try_new(path: &str, projection: Option<Vec<usize>>, batch_size: usize) -> Result<Self> {
        let mut filenames: Vec<String> = vec![];
        build_file_list(path, &mut filenames, ".parquet").unwrap();
        if filenames.is_empty() {
            Err(ExecutionError::ExecutionError(
                "No parquet files found".to_string(),
            ))
        } else {
            let file = File::open(&filenames[0]).expect("File not found!");
            //let file_reader = Rc::new(SerializedFileReader::new(file).unwrap());
            let arrow_reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let schema = arrow_reader_builder.schema().clone();

            let projection = match projection {
                Some(p) => p,
                None => (0..schema.fields().len()).collect(),
            };

            let projected_schema = Schema::new(
                projection
                    .iter()
                    .map(|i| schema.field(*i).clone())
                    .collect::<Vec<_>>(),
            );

            Ok(Self {
                filenames,
                schema: Arc::new(projected_schema),
                projection,
                batch_size,
            })
        }
    }
}

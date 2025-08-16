use crate::error::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::sync::Mutex;

pub struct RecordBatchIterator {
    schema: Arc<Schema>,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl RecordBatchIterator {
    pub fn new(schema: Arc<Schema>, batches: Vec<Arc<RecordBatch>>) -> Self {
        RecordBatchIterator {
            schema,
            batches,
            index: 0,
        }
    }
}

pub trait BatchIterator: Send + Sync {
    fn schema(&self) -> Arc<Schema>;

    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

impl BatchIterator for RecordBatchIterator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.index < self.batches.len() {
            let next_batch = self.batches[self.index].as_ref().clone();
            self.index += 1;
            Ok(Some(next_batch))
        } else {
            Ok(None)
        }
    }
}

pub type ScanResult = Arc<Mutex<dyn BatchIterator>>;

pub trait TableProvider {
    fn schema(&self) -> Arc<Schema>;

    fn scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> Result<Vec<ScanResult>>;
}

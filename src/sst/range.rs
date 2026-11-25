//! Packed range-deletion intervals. Splits share the sequence stripe array
//! and the unchanged endpoint across compaction jobs and output files.
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct RangeFragment {
    pub begin: Arc<[u8]>,
    pub end: Arc<[u8]>,
    stripes: Arc<[u64]>,
    first: usize,
}

impl RangeFragment {
    pub fn new(begin: &[u8], end: &[u8], stripes: Arc<[u64]>) -> Self {
        debug_assert!(!stripes.is_empty() && begin < end);
        Self {
            begin: Arc::from(begin),
            end: Arc::from(end),
            stripes,
            first: 0,
        }
    }

    pub fn sequences(&self) -> &[u64] {
        &self.stripes[self.first..]
    }

    pub fn keep_from(&mut self, minimum: u64) -> bool {
        self.first += self.sequences().partition_point(|s| *s < minimum);
        self.first < self.stripes.len()
    }
}

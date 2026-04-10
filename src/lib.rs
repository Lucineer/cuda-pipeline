/*!
# cuda-pipeline

Data pipeline and ETL processing.

Agents process data streams — sensor readings, messages, logs,
computed results. Pipelines chain transforms, filters, and aggregations
into reusable data flows.

- Pipeline stages (map, filter, reduce, flat_map)
- Pipeline composition
- Batch and streaming modes
- Error handling per stage
- Throughput tracking
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A data item in the pipeline
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataItem {
    pub id: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub timestamp: u64,
    pub source: String,
}

impl DataItem {
    pub fn new(source: &str, payload: &[u8]) -> Self { DataItem { id: format!("d_{}", now()), payload: payload.to_vec(), headers: HashMap::new(), timestamp: now(), source: source.to_string() } }
    pub fn with_header(mut self, k: &str, v: &str) -> Self { self.headers.insert(k.to_string(), v.to_string()); self }
}

/// Pipeline stage result
#[derive(Clone, Debug)]
pub enum StageResult { Pass(DataItem), Drop, Error(String), Emit(Vec<DataItem>) }

/// A pipeline stage
pub trait PipelineStage: Send + Sync {
    fn name(&self) -> &str;
    fn process(&self, item: DataItem) -> StageResult;
}

/// Map stage — transform each item
pub struct MapStage<F: Fn(DataItem) -> DataItem> {
    name_str: String,
    f: F,
}

impl<F: Fn(DataItem) -> DataItem> MapStage<F> {
    pub fn new(name: &str, f: F) -> Self { MapStage { name_str: name.to_string(), f } }
}

impl<F: Fn(DataItem) -> DataItem + Send + Sync> PipelineStage for MapStage<F> {
    fn name(&self) -> &str { &self.name_str }
    fn process(&self, item: DataItem) -> StageResult { StageResult::Pass((self.f)(item)) }
}

/// Filter stage — keep or drop items
pub struct FilterStage<F: Fn(&DataItem) -> bool> {
    name_str: String,
    f: F,
}

impl<F: Fn(&DataItem) -> bool + Send + Sync> PipelineStage for FilterStage<F> {
    fn name(&self) -> &str { &self.name_str }
    fn process(&self, item: DataItem) -> StageResult { if (self.f)(&item) { StageResult::Pass(item) } else { StageResult::Drop } }
}

impl<F: Fn(&DataItem) -> bool> FilterStage<F> {
    pub fn new(name: &str, f: F) -> Self { FilterStage { name_str: name.to_string(), f } }
}

/// FlatMap stage — one item → many items
pub struct FlatMapStage<F: Fn(DataItem) -> Vec<DataItem>> {
    name_str: String,
    f: F,
}

impl<F: Fn(DataItem) -> Vec<DataItem> + Send + Sync> PipelineStage for FlatMapStage<F> {
    fn name(&self) -> &str { &self.name_str }
    fn process(&self, item: DataItem) -> StageResult { StageResult::Emit((self.f)(item)) }
}

impl<F: Fn(DataItem) -> Vec<DataItem>> FlatMapStage<F> {
    pub fn new(name: &str, f: F) -> Self { FlatMapStage { name_str: name.to_string(), f } }
}

/// Pipeline statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PipelineStats {
    pub items_in: u64,
    pub items_out: u64,
    pub items_dropped: u64,
    pub items_errored: u64,
    pub bytes_processed: u64,
    pub duration_ms: u64,
}

impl PipelineStats {
    pub fn throughput(&self) -> f64 {
        if self.duration_ms == 0 { return 0.0; }
        self.items_in as f64 / (self.duration_ms as f64 / 1000.0)
    }
}

/// A pipeline
pub struct Pipeline {
    pub name: String,
    pub stages: Vec<Box<dyn PipelineStage>>,
    pub stats: PipelineStats,
}

impl Pipeline {
    pub fn new(name: &str) -> Self { Pipeline { name: name.to_string(), stages: vec![], stats: PipelineStats::default() } }

    /// Add a stage
    pub fn add_stage(&mut self, stage: Box<dyn PipelineStage>) { self.stages.push(stage); }

    /// Process a single item through all stages
    pub fn process(&mut self, item: DataItem) -> Vec<DataItem> {
        let start = now();
        self.stats.items_in += 1;
        self.stats.bytes_processed += item.payload.len() as u64;
        let mut items = vec![item];
        for stage in &self.stages {
            let mut next = vec![];
            for item in items {
                match stage.process(item) {
                    StageResult::Pass(passed) => next.push(passed),
                    StageResult::Drop => self.stats.items_dropped += 1,
                    StageResult::Error(e) => { self.stats.items_errored += 1; /* could log e */ }
                    StageResult::Emit(emitted) => next.extend(emitted),
                }
            }
            items = next;
            if items.is_empty() { break; }
        }
        self.stats.items_out += items.len() as u64;
        self.stats.duration_ms += now() - start;
        items
    }

    /// Process a batch
    pub fn process_batch(&mut self, items: Vec<DataItem>) -> Vec<DataItem> {
        items.into_iter().flat_map(|i| self.process(i)).collect()
    }

    /// Summary
    pub fn summary(&self) -> String {
        format!("Pipeline[{}]: {} stages, in={}, out={}, dropped={}, errors={}, throughput={:.1}/s",
            self.name, self.stages.len(), self.stats.items_in, self.stats.items_out,
            self.stats.items_dropped, self.stats.items_errored, self.stats.throughput())
    }
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_stage() {
        let item = DataItem::new("src", b"hello");
        let stage = MapStage::new("upper", |mut i: DataItem| { i.payload = i.payload.iter().map(|b| b.to_ascii_uppercase()).collect(); i });
        match stage.process(item) {
            StageResult::Pass(result) => assert_eq!(result.payload, b"HELLO".to_vec()),
            _ => panic!("expected pass"),
        }
    }

    #[test]
    fn test_filter_stage() {
        let stage = FilterStage::new("nonempty", |i: &DataItem| !i.payload.is_empty());
        let empty = DataItem::new("src", b"");
        let full = DataItem::new("src", b"data");
        assert!(matches!(stage.process(empty), StageResult::Drop));
        assert!(matches!(stage.process(full), StageResult::Pass(_)));
    }

    #[test]
    fn test_flatmap_stage() {
        let stage = FlatMapStage::new("split", |i: DataItem| {
            let s = String::from_utf8_lossy(&i.payload);
            s.split_whitespace().map(|w| DataItem::new("src", w.as_bytes())).collect()
        });
        let item = DataItem::new("src", b"hello world foo");
        match stage.process(item) {
            StageResult::Emit(items) => assert_eq!(items.len(), 3),
            _ => panic!("expected emit"),
        }
    }

    #[test]
    fn test_pipeline_single() {
        let mut pipe = Pipeline::new("test");
        pipe.add_stage(Box::new(MapStage::new("double", |mut i: DataItem| { i.payload = i.payload.repeat(2); i })));
        let result = pipe.process(DataItem::new("src", b"ab"));
        assert_eq!(result[0].payload, b"abab".to_vec());
    }

    #[test]
    fn test_pipeline_chain() {
        let mut pipe = Pipeline::new("chain");
        pipe.add_stage(Box::new(FilterStage::new("len_gt_2", |i: &DataItem| i.payload.len() > 2)));
        pipe.add_stage(Box::new(MapStage::new("upper", |mut i: DataItem| { i.payload = i.payload.iter().map(|b| b.to_ascii_uppercase()).collect(); i })));
        let results = pipe.process_batch(vec![
            DataItem::new("src", b"hi"),     // dropped (len=2)
            DataItem::new("src", b"hello"), // kept → HELLO
            DataItem::new("src", b"ab"),    // dropped
            DataItem::new("src", b"world"), // kept → WORLD
        ]);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_pipeline_stats() {
        let mut pipe = Pipeline::new("stats");
        pipe.add_stage(Box::new(FilterStage::new("keep_all", |_i: &DataItem| true)));
        pipe.process(DataItem::new("s", b"a"));
        pipe.process(DataItem::new("s", b"bb"));
        assert_eq!(pipe.stats.items_in, 2);
        assert_eq!(pipe.stats.items_out, 2);
    }

    #[test]
    fn test_pipeline_summary() {
        let pipe = Pipeline::new("empty");
        let s = pipe.summary();
        assert!(s.contains("0 stages"));
    }

    #[test]
    fn test_data_item_with_headers() {
        let item = DataItem::new("src", b"data").with_header("type", "sensor");
        assert_eq!(item.headers.get("type").unwrap(), "sensor");
    }

    #[test]
    fn test_empty_pipeline() {
        let mut pipe = Pipeline::new("passthrough");
        let result = pipe.process(DataItem::new("src", b"data"));
        assert_eq!(result.len(), 1);
    }
}

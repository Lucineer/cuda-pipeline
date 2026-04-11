//! cuda-pipeline: Multi-stage processing pipeline with backpressure.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageResult { pub stage_name: String, pub items_processed: usize, pub elapsed_ms: u64, pub errors: usize }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatus { pub stages: Vec<StageResult>, pub total_items: usize, pub total_errors: usize, pub throughput_per_sec: f64, pub is_healthy: bool }

pub struct Pipeline { stages: Vec<String>, max_backpressure: usize, current_backpressure: usize, results: Vec<StageResult> }
impl Pipeline {
    pub fn new(max_backpressure: usize) -> Self { Self { stages: Vec::new(), max_backpressure, current_backpressure: 0, results: Vec::new() } }
    pub fn add_stage(&mut self, name: &str) { self.stages.push(name.to_string()); }
    pub fn push(&mut self) -> bool {
        if self.current_backpressure >= self.max_backpressure { return false; }
        self.current_backpressure += 1; true
    }
    pub fn complete_stage(&mut self, name: &str, items: usize, elapsed_ms: u64, errors: usize) {
        self.current_backpressure = self.current_backpressure.saturating_sub(items);
        self.results.push(StageResult { stage_name: name.into(), items_processed: items, elapsed_ms, errors });
    }
    pub fn status(&self) -> PipelineStatus {
        let total_items: usize = self.results.iter().map(|r| r.items_processed).sum();
        let total_errors: usize = self.results.iter().map(|r| r.errors).sum();
        let total_ms: u64 = self.results.iter().map(|r| r.elapsed_ms).sum();
        let throughput = if total_ms > 0 { (total_items as f64 / total_ms as f64) * 1000.0 } else { 0.0 };
        let healthy = total_errors == 0 && self.current_backpressure < self.max_backpressure;
        PipelineStatus { stages: self.results.clone(), total_items, total_errors, throughput_per_sec: throughput, is_healthy: healthy }
    }
    pub fn stage_count(&self) -> usize { self.stages.len() }
    pub fn backpressure(&self) -> usize { self.current_backpressure }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn test_empty_pipeline() { let p = Pipeline::new(10); assert_eq!(p.stage_count(), 0); assert!(p.status().is_healthy); }
    #[test] fn test_push_backpressure() { let mut p = Pipeline::new(2); assert!(p.push()); assert!(p.push()); assert!(!p.push()); assert_eq!(p.backpressure(), 2); }
    #[test] fn test_stage_completion() { let mut p = Pipeline::new(10); p.add_stage("tokenize"); p.push(); p.complete_stage("tokenize", 1, 50, 0); assert_eq!(p.status().total_items, 1); }
    #[test] fn test_throughput() { let mut p = Pipeline::new(10); p.add_stage("parse"); p.push(); p.complete_stage("parse", 1000, 1000, 0); assert!((p.status().throughput_per_sec - 1000.0).abs() < 1.0); }
    #[test] fn test_unhealthy_on_errors() { let mut p = Pipeline::new(10); p.add_stage("lint"); p.push(); p.complete_stage("lint", 10, 100, 3); assert!(!p.status().is_healthy); }
}
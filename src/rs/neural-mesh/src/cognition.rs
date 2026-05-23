//! Cognition engine for distributed neural processing

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::{NeuralMeshError, Result};

/// Cognition engine that processes distributed thoughts
#[derive(Debug)]
pub struct CognitionEngine {
    config: CognitionConfig,
    thought_processors: Arc<RwLock<HashMap<String, Arc<dyn ThoughtProcessor>>>>,
    memory_store: Arc<RwLock<MemoryStore>>,
    stats: Arc<RwLock<CognitionStats>>,
    processing_tx: mpsc::UnboundedSender<ProcessingTask>,
}

impl CognitionEngine {
    /// Create a new cognition engine
    pub async fn new(config: CognitionConfig) -> Result<Self> {
        let (processing_tx, processing_rx) = mpsc::unbounded_channel();

        let engine = Self {
            config: config.clone(),
            thought_processors: Arc::new(RwLock::new(HashMap::new())),
            memory_store: Arc::new(RwLock::new(MemoryStore::new(config.memory_capacity))),
            stats: Arc::new(RwLock::new(CognitionStats::default())),
            processing_tx,
        };

        // Initialize default processors
        engine.init_default_processors().await?;

        // Spawn processing task
        let engine_clone = engine.clone();
        tokio::spawn(async move {
            engine_clone.process_tasks(processing_rx).await;
        });

        Ok(engine)
    }

    /// Start the cognition engine
    pub async fn start(&self) -> Result<()> {
        tracing::info!("Starting cognition engine");
        Ok(())
    }

    /// Stop the cognition engine
    pub async fn stop(&self) -> Result<()> {
        tracing::info!("Stopping cognition engine");
        Ok(())
    }

    /// Process a distributed cognition task
    pub async fn process_distributed(
        &self,
        task: CognitionTask,
        assignment: TaskAssignment,
    ) -> Result<CognitionResult> {
        let start_time = Instant::now();

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_tasks += 1;
        }

        // Process through assigned agents
        let mut partial_results = Vec::new();
        for agent_id in assignment.agent_ids {
            let partial = self.process_partial(task.clone(), agent_id).await?;
            partial_results.push(partial);
        }

        // Merge results
        let merged_result = self.merge_results(partial_results).await?;

        // Store in memory
        if task.store_in_memory {
            let mut memory = self.memory_store.write().await;
            memory.store(task.id, merged_result.clone()).await?;
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            let duration = start_time.elapsed();
            stats.update_response_time(duration.as_secs_f64());
        }

        Ok(merged_result)
    }

    /// Get statistics
    pub async fn get_stats(&self) -> CognitionStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Initialize default thought processors
    async fn init_default_processors(&self) -> Result<()> {
        let mut processors = self.thought_processors.write().await;

        // Pattern recognition processor
        processors.insert(
            "pattern_recognition".to_string(),
            Arc::new(PatternRecognitionProcessor::new()),
        );

        // Memory formation processor
        processors.insert(
            "memory_formation".to_string(),
            Arc::new(MemoryFormationProcessor::new()),
        );

        // Decision making processor
        processors.insert(
            "decision_making".to_string(),
            Arc::new(DecisionMakingProcessor::new()),
        );

        Ok(())
    }

    /// Process a partial task on a specific agent
    async fn process_partial(&self, task: CognitionTask, agent_id: Uuid) -> Result<PartialResult> {
        // This would communicate with the actual agent
        // For now, simulate processing
        let result = PartialResult {
            agent_id,
            output: ThoughtPattern::new(task.input.complexity),
            confidence: 0.85,
            processing_time: Duration::from_millis(50),
        };

        Ok(result)
    }

    /// Merge partial results from multiple agents
    async fn merge_results(&self, partials: Vec<PartialResult>) -> Result<CognitionResult> {
        if partials.is_empty() {
            return Err(NeuralMeshError::InvalidInput(
                "No partial results to merge".to_string(),
            ));
        }

        // Weight-average the results based on confidence
        let total_confidence: f64 = partials.iter().map(|p| p.confidence).sum();
        let num_partials = partials.len();

        // Create merged thought pattern
        let merged_pattern = ThoughtPattern::merge(
            partials.iter().map(|p| &p.output).collect(),
            partials
                .iter()
                .map(|p| p.confidence / total_confidence)
                .collect(),
        )?;

        Ok(CognitionResult {
            output: merged_pattern,
            agent_contributions: partials
                .into_iter()
                .map(|p| (p.agent_id, p.confidence))
                .collect(),
            total_confidence: total_confidence / num_partials as f64,
            consensus_level: 0.9, // Would calculate actual consensus
        })
    }

    /// Process tasks from the channel
    async fn process_tasks(&self, mut rx: mpsc::UnboundedReceiver<ProcessingTask>) {
        while let Some(task) = rx.recv().await {
            if let Err(e) = self.handle_processing_task(task).await {
                tracing::error!("Error processing task: {}", e);
            }
        }
    }

    /// Handle a single processing task
    async fn handle_processing_task(&self, task: ProcessingTask) -> Result<()> {
        match task {
            ProcessingTask::Learn(pattern) => {
                self.learn_pattern(pattern).await?;
            }
            ProcessingTask::Recall(key) => {
                self.recall_memory(key).await?;
            }
            ProcessingTask::Consolidate => {
                self.consolidate_memory().await?;
            }
        }
        Ok(())
    }

    /// Learn a new pattern
    async fn learn_pattern(&self, pattern: ThoughtPattern) -> Result<()> {
        let mut memory = self.memory_store.write().await;
        memory.learn(pattern).await
    }

    /// Recall a memory
    async fn recall_memory(&self, key: Uuid) -> Result<Option<CognitionResult>> {
        let memory = self.memory_store.read().await;
        memory.recall(key).await
    }

    /// Consolidate memory
    async fn consolidate_memory(&self) -> Result<()> {
        let mut memory = self.memory_store.write().await;
        memory.consolidate().await
    }
}

impl Clone for CognitionEngine {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            thought_processors: Arc::clone(&self.thought_processors),
            memory_store: Arc::clone(&self.memory_store),
            stats: Arc::clone(&self.stats),
            processing_tx: self.processing_tx.clone(),
        }
    }
}

/// Configuration for the cognition engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitionConfig {
    pub memory_capacity: usize,
    pub consolidation_interval: Duration,
    pub learning_rate: f64,
    pub pattern_threshold: f64,
}

impl Default for CognitionConfig {
    fn default() -> Self {
        Self {
            memory_capacity: 10000,
            consolidation_interval: Duration::from_secs(300),
            learning_rate: 0.1,
            pattern_threshold: 0.7,
        }
    }
}

/// A cognition task to be processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitionTask {
    pub id: Uuid,
    pub task_type: TaskType,
    pub input: ThoughtPattern,
    pub context: HashMap<String, serde_json::Value>,
    pub priority: Priority,
    pub store_in_memory: bool,
}

/// Types of cognition tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    PatternRecognition,
    MemoryFormation,
    DecisionMaking,
    ProblemSolving,
    CreativeThinking,
    AnalyticalReasoning,
}

/// Priority levels for tasks
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

/// Result of a cognition task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitionResult {
    pub output: ThoughtPattern,
    pub agent_contributions: Vec<(Uuid, f64)>,
    pub total_confidence: f64,
    pub consensus_level: f64,
}

/// A thought pattern that can be processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThoughtPattern {
    pub id: Uuid,
    pub complexity: f64,
    pub features: Vec<f64>,
    pub connections: Vec<Connection>,
    pub context: HashMap<String, serde_json::Value>,
    /// Unix timestamp in seconds (replaces Instant which is not serializable)
    pub timestamp: u64,
}

impl ThoughtPattern {
    /// Create a new thought pattern
    pub fn new(complexity: f64) -> Self {
        Self {
            id: Uuid::new_v4(),
            complexity,
            features: vec![0.0; 100], // Default feature vector
            connections: Vec::new(),
            context: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Convert to input vector for neural network
    pub fn to_input_vector(&self) -> Result<Vec<f32>> {
        Ok(self.features.iter().map(|&f| f as f32).collect())
    }

    /// Create from neural network output
    pub fn from_output_vector(
        output: Vec<f32>,
        context: HashMap<String, serde_json::Value>,
    ) -> Result<Self> {
        Ok(Self {
            id: Uuid::new_v4(),
            complexity: output.len() as f64,
            features: output.into_iter().map(|f| f as f64).collect(),
            connections: Vec::new(),
            context,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        })
    }

    /// Merge multiple thought patterns
    pub fn merge(patterns: Vec<&ThoughtPattern>, weights: Vec<f64>) -> Result<Self> {
        if patterns.is_empty() || patterns.len() != weights.len() {
            return Err(NeuralMeshError::InvalidInput(
                "Invalid merge parameters".to_string(),
            ));
        }

        let feature_len = patterns[0].features.len();
        let mut merged_features = vec![0.0; feature_len];

        for (pattern, weight) in patterns.iter().zip(weights.iter()) {
            for (i, &feature) in pattern.features.iter().enumerate() {
                merged_features[i] += feature * weight;
            }
        }

        Ok(Self {
            id: Uuid::new_v4(),
            complexity: patterns.iter().map(|p| p.complexity).sum::<f64>() / patterns.len() as f64,
            features: merged_features,
            connections: Vec::new(), // Would merge connections
            context: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        })
    }
}

/// Connection between thought patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub to: Uuid,
    pub strength: f64,
    pub connection_type: ConnectionType,
}

/// Types of connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionType {
    Association,
    Causation,
    Similarity,
    Contrast,
    Sequence,
}

/// Task assignment for distributed processing
#[derive(Debug, Clone)]
pub struct TaskAssignment {
    pub task_id: Uuid,
    pub agent_ids: Vec<Uuid>,
    pub strategy: AssignmentStrategy,
}

/// Strategy for assigning tasks
#[derive(Debug, Clone)]
pub enum AssignmentStrategy {
    RoundRobin,
    LoadBalanced,
    CapabilityBased,
    Random,
}

/// Partial result from an agent
struct PartialResult {
    agent_id: Uuid,
    output: ThoughtPattern,
    confidence: f64,
    processing_time: Duration,
}

/// Processing tasks for the engine
enum ProcessingTask {
    Learn(ThoughtPattern),
    Recall(Uuid),
    Consolidate,
}

/// Statistics for the cognition engine
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CognitionStats {
    pub total_tasks: u64,
    pub avg_response_time: f64,
    response_times: Vec<f64>,
}

impl CognitionStats {
    fn update_response_time(&mut self, time: f64) {
        self.response_times.push(time);
        if self.response_times.len() > 1000 {
            self.response_times.remove(0);
        }
        self.avg_response_time =
            self.response_times.iter().sum::<f64>() / self.response_times.len() as f64;
    }
}

/// Memory store for thought patterns
#[derive(Debug)]
struct MemoryStore {
    capacity: usize,
    memories: HashMap<Uuid, CognitionResult>,
    patterns: Vec<ThoughtPattern>,
}

impl MemoryStore {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            memories: HashMap::new(),
            patterns: Vec::new(),
        }
    }

    async fn store(&mut self, id: Uuid, result: CognitionResult) -> Result<()> {
        if self.memories.len() >= self.capacity {
            // Evict oldest memory
            if let Some(&oldest_id) = self.memories.keys().next() {
                self.memories.remove(&oldest_id);
            }
        }
        self.memories.insert(id, result);
        Ok(())
    }

    async fn recall(&self, key: Uuid) -> Result<Option<CognitionResult>> {
        Ok(self.memories.get(&key).cloned())
    }

    async fn learn(&mut self, pattern: ThoughtPattern) -> Result<()> {
        self.patterns.push(pattern);
        Ok(())
    }

    async fn consolidate(&mut self) -> Result<()> {
        // Consolidate patterns and memories
        // This would implement memory consolidation algorithms
        Ok(())
    }
}

/// Trait for thought processors
#[async_trait::async_trait]
trait ThoughtProcessor: Send + Sync + std::fmt::Debug {
    async fn process(&self, pattern: &ThoughtPattern) -> Result<ThoughtPattern>;
}

/// Pattern recognition processor
#[derive(Debug)]
struct PatternRecognitionProcessor;

impl PatternRecognitionProcessor {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl ThoughtProcessor for PatternRecognitionProcessor {
    async fn process(&self, pattern: &ThoughtPattern) -> Result<ThoughtPattern> {
        // Implement pattern recognition logic
        Ok(pattern.clone())
    }
}

/// Memory formation processor
#[derive(Debug)]
struct MemoryFormationProcessor;

impl MemoryFormationProcessor {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl ThoughtProcessor for MemoryFormationProcessor {
    async fn process(&self, pattern: &ThoughtPattern) -> Result<ThoughtPattern> {
        // Implement memory formation logic
        Ok(pattern.clone())
    }
}

/// Decision making processor
#[derive(Debug)]
struct DecisionMakingProcessor;

impl DecisionMakingProcessor {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl ThoughtProcessor for DecisionMakingProcessor {
    async fn process(&self, pattern: &ThoughtPattern) -> Result<ThoughtPattern> {
        // Implement decision making logic
        Ok(pattern.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cognition_engine_creation() {
        let config = CognitionConfig::default();
        let engine = CognitionEngine::new(config).await;
        assert!(engine.is_ok());
    }

    #[test]
    fn test_thought_pattern_creation() {
        let pattern = ThoughtPattern::new(0.5);
        assert_eq!(pattern.complexity, 0.5);
        assert_eq!(pattern.features.len(), 100);
    }

    #[test]
    fn test_thought_pattern_merge() {
        let pattern1 = ThoughtPattern::new(0.5);
        let pattern2 = ThoughtPattern::new(0.7);

        let patterns = vec![&pattern1, &pattern2];
        let weights = vec![0.6, 0.4];

        let merged = ThoughtPattern::merge(patterns, weights);
        assert!(merged.is_ok());
    }
}

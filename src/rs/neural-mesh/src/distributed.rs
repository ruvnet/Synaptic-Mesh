//! Distributed training for neural agents

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use uuid::Uuid;

use crate::{NeuralMeshError, Result};

/// Distributed training coordinator
#[derive(Debug)]
pub struct DistributedTraining {
    strategy: TrainingStrategy,
    active_sessions: Arc<RwLock<HashMap<Uuid, TrainingSession>>>,
    model_aggregator: Arc<ModelAggregator>,
    gradient_accumulator: Arc<RwLock<GradientAccumulator>>,
    broadcast_tx: broadcast::Sender<TrainingUpdate>,
    stats: Arc<RwLock<TrainingStats>>,
}

impl DistributedTraining {
    /// Create a new distributed training system
    pub async fn new(strategy: TrainingStrategy) -> Result<Self> {
        let (broadcast_tx, _) = broadcast::channel(1024);

        Ok(Self {
            strategy,
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
            model_aggregator: Arc::new(ModelAggregator::new()),
            gradient_accumulator: Arc::new(RwLock::new(GradientAccumulator::new())),
            broadcast_tx,
            stats: Arc::new(RwLock::new(TrainingStats::default())),
        })
    }

    /// Start a training session
    pub async fn start_session(&self, session_config: TrainingSessionConfig) -> Result<Uuid> {
        let session_id = Uuid::new_v4();

        let session = TrainingSession {
            id: session_id,
            config: session_config,
            participants: Vec::new(),
            current_epoch: 0,
            global_model: None,
            status: SessionStatus::Initializing,
        };

        {
            let mut sessions = self.active_sessions.write().await;
            sessions.insert(session_id, session);
        }

        // Broadcast session start
        let update = TrainingUpdate::SessionStarted { session_id };
        let _ = self.broadcast_tx.send(update);

        tracing::info!("Started distributed training session: {}", session_id);
        Ok(session_id)
    }

    /// Join a training session
    pub async fn join_session(
        &self,
        session_id: Uuid,
        agent_id: Uuid,
        initial_model: Vec<f32>,
    ) -> Result<broadcast::Receiver<TrainingUpdate>> {
        let mut sessions = self.active_sessions.write().await;

        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            NeuralMeshError::NotFound(format!("Session {} not found", session_id))
        })?;

        session.participants.push(agent_id);

        // Store initial model if this is the first participant
        if session.global_model.is_none() {
            session.global_model = Some(initial_model);
            session.status = SessionStatus::Active;
        }

        Ok(self.broadcast_tx.subscribe())
    }

    /// Submit gradients from an agent
    pub async fn submit_gradients(
        &self,
        session_id: Uuid,
        agent_id: Uuid,
        gradients: Vec<f32>,
        batch_size: usize,
    ) -> Result<()> {
        let sessions = self.active_sessions.read().await;
        let session = sessions.get(&session_id).ok_or_else(|| {
            NeuralMeshError::NotFound(format!("Session {} not found", session_id))
        })?;

        // Accumulate gradients based on strategy
        match &self.strategy {
            TrainingStrategy::FederatedAveraging { .. } => {
                let mut accumulator = self.gradient_accumulator.write().await;
                accumulator
                    .add_gradients(session_id, agent_id, gradients, batch_size)
                    .await?;

                // Check if we have enough gradients to aggregate
                if accumulator
                    .ready_to_aggregate(session_id, session.participants.len())
                    .await?
                {
                    self.aggregate_and_update(session_id).await?;
                }
            }
            TrainingStrategy::AsyncSGD { .. } => {
                // Apply gradients immediately
                self.apply_async_update(session_id, agent_id, gradients)
                    .await?;
            }
            TrainingStrategy::ModelAveraging { .. } => {
                // Store for later averaging
                let mut accumulator = self.gradient_accumulator.write().await;
                accumulator
                    .add_gradients(session_id, agent_id, gradients, batch_size)
                    .await?;
            }
            TrainingStrategy::GradientCompression { compression_ratio } => {
                // Compress gradients before accumulation
                let compressed = self.compress_gradients(gradients, *compression_ratio)?;
                let mut accumulator = self.gradient_accumulator.write().await;
                accumulator
                    .add_gradients(session_id, agent_id, compressed, batch_size)
                    .await?;
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_gradient_updates += 1;
        }

        Ok(())
    }

    /// Get current global model for a session
    pub async fn get_global_model(&self, session_id: Uuid) -> Result<Vec<f32>> {
        let sessions = self.active_sessions.read().await;
        let session = sessions.get(&session_id).ok_or_else(|| {
            NeuralMeshError::NotFound(format!("Session {} not found", session_id))
        })?;

        session
            .global_model
            .clone()
            .ok_or_else(|| NeuralMeshError::NotFound("Global model not initialized".to_string()))
    }

    /// End a training session
    pub async fn end_session(&self, session_id: Uuid) -> Result<TrainingResult> {
        let mut sessions = self.active_sessions.write().await;
        let session = sessions.remove(&session_id).ok_or_else(|| {
            NeuralMeshError::NotFound(format!("Session {} not found", session_id))
        })?;

        // Broadcast session end
        let update = TrainingUpdate::SessionEnded { session_id };
        let _ = self.broadcast_tx.send(update);

        Ok(TrainingResult {
            session_id,
            final_model: session.global_model.unwrap_or_default(),
            epochs_completed: session.current_epoch,
            participants: session.participants.len(),
        })
    }

    /// Aggregate gradients and update global model
    async fn aggregate_and_update(&self, session_id: Uuid) -> Result<()> {
        let mut accumulator = self.gradient_accumulator.write().await;
        let aggregated = accumulator.aggregate(session_id).await?;

        // Update global model
        let mut sessions = self.active_sessions.write().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            if let Some(global_model) = &mut session.global_model {
                // Apply aggregated gradients
                for (i, grad) in aggregated.iter().enumerate() {
                    if i < global_model.len() {
                        global_model[i] -= grad * 0.01; // Learning rate
                    }
                }

                session.current_epoch += 1;

                // Broadcast update
                let update = TrainingUpdate::ModelUpdated {
                    session_id,
                    epoch: session.current_epoch,
                    model: global_model.clone(),
                };
                let _ = self.broadcast_tx.send(update);
            }
        }

        // Clear accumulated gradients
        accumulator.clear(session_id).await;

        Ok(())
    }

    /// Apply asynchronous update
    async fn apply_async_update(
        &self,
        session_id: Uuid,
        agent_id: Uuid,
        gradients: Vec<f32>,
    ) -> Result<()> {
        let mut sessions = self.active_sessions.write().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            if let Some(global_model) = &mut session.global_model {
                // Apply gradients with momentum
                let momentum = 0.9;
                let learning_rate = 0.01;

                for (i, grad) in gradients.iter().enumerate() {
                    if i < global_model.len() {
                        global_model[i] = momentum * global_model[i] - learning_rate * grad;
                    }
                }

                // Broadcast update
                let update = TrainingUpdate::AsyncUpdate {
                    session_id,
                    agent_id,
                    timestamp: std::time::SystemTime::now(),
                };
                let _ = self.broadcast_tx.send(update);
            }
        }

        Ok(())
    }

    /// Compress gradients
    fn compress_gradients(&self, gradients: Vec<f32>, ratio: f32) -> Result<Vec<f32>> {
        // Simple top-k sparsification
        let k = (gradients.len() as f32 * ratio) as usize;
        let mut indexed_grads: Vec<(usize, f32)> = gradients
            .iter()
            .enumerate()
            .map(|(i, &g)| (i, g.abs()))
            .collect();

        indexed_grads.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        let mut compressed = vec![0.0; gradients.len()];
        for i in 0..k.min(gradients.len()) {
            let idx = indexed_grads[i].0;
            compressed[idx] = gradients[idx];
        }

        Ok(compressed)
    }
}

/// Training strategies for distributed learning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrainingStrategy {
    /// Federated averaging
    FederatedAveraging {
        rounds: usize,
        min_participants: usize,
    },

    /// Asynchronous SGD
    AsyncSGD { staleness_penalty: f64 },

    /// Model averaging
    ModelAveraging { averaging_frequency: usize },

    /// Gradient compression
    GradientCompression { compression_ratio: f32 },
}

/// Configuration for a training session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingSessionConfig {
    pub name: String,
    pub max_epochs: usize,
    pub batch_size: usize,
    pub learning_rate: f64,
    pub target_accuracy: f64,
}

/// Active training session
#[derive(Debug, Clone)]
struct TrainingSession {
    id: Uuid,
    config: TrainingSessionConfig,
    participants: Vec<Uuid>,
    current_epoch: usize,
    global_model: Option<Vec<f32>>,
    status: SessionStatus,
}

/// Session status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionStatus {
    Initializing,
    Active,
    Completed,
    Failed,
}

/// Model synchronization between agents
#[derive(Debug, Clone)]
pub struct ModelSync {
    sync_strategy: SyncStrategy,
    version_tracker: Arc<RwLock<HashMap<Uuid, ModelVersion>>>,
}

impl ModelSync {
    /// Create a new model synchronizer
    pub fn new(strategy: SyncStrategy) -> Self {
        Self {
            sync_strategy: strategy,
            version_tracker: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Synchronize models between agents
    pub async fn sync_models(
        &self,
        agent_models: HashMap<Uuid, Vec<f32>>,
    ) -> Result<HashMap<Uuid, Vec<f32>>> {
        match &self.sync_strategy {
            SyncStrategy::AllReduce => self.all_reduce_sync(agent_models).await,
            SyncStrategy::ParameterServer { server_id } => {
                self.parameter_server_sync(agent_models, *server_id).await
            }
            SyncStrategy::GossipProtocol { fanout } => {
                self.gossip_sync(agent_models, *fanout).await
            }
            SyncStrategy::RingAllReduce => self.ring_all_reduce_sync(agent_models).await,
        }
    }

    /// All-reduce synchronization
    async fn all_reduce_sync(
        &self,
        agent_models: HashMap<Uuid, Vec<f32>>,
    ) -> Result<HashMap<Uuid, Vec<f32>>> {
        if agent_models.is_empty() {
            return Ok(HashMap::new());
        }

        // Get model size
        let model_size = agent_models.values().next().unwrap().len();

        // Sum all models
        let mut sum_model = vec![0.0; model_size];
        for model in agent_models.values() {
            for (i, &param) in model.iter().enumerate() {
                sum_model[i] += param;
            }
        }

        // Average
        let count = agent_models.len() as f32;
        for param in &mut sum_model {
            *param /= count;
        }

        // Distribute averaged model to all agents
        let mut result = HashMap::new();
        for agent_id in agent_models.keys() {
            result.insert(*agent_id, sum_model.clone());
        }

        Ok(result)
    }

    /// Parameter server synchronization
    async fn parameter_server_sync(
        &self,
        agent_models: HashMap<Uuid, Vec<f32>>,
        server_id: Uuid,
    ) -> Result<HashMap<Uuid, Vec<f32>>> {
        // In a real implementation, this would communicate with a parameter server
        // For now, use the server's model as the global model
        let server_model = agent_models
            .get(&server_id)
            .ok_or_else(|| NeuralMeshError::NotFound("Parameter server not found".to_string()))?
            .clone();

        let mut result = HashMap::new();
        for agent_id in agent_models.keys() {
            result.insert(*agent_id, server_model.clone());
        }

        Ok(result)
    }

    /// Gossip protocol synchronization
    async fn gossip_sync(
        &self,
        agent_models: HashMap<Uuid, Vec<f32>>,
        fanout: usize,
    ) -> Result<HashMap<Uuid, Vec<f32>>> {
        // Simple gossip: each agent averages with 'fanout' random neighbors
        let mut result = agent_models.clone();
        let agent_ids: Vec<Uuid> = agent_models.keys().cloned().collect();

        for (agent_id, model) in agent_models.iter() {
            let mut averaged_model = model.clone();
            let mut count = 1;

            // Select random neighbors
            let mut rng = rand::thread_rng();
            let filtered: Vec<&Uuid> = agent_ids.iter().filter(|&id| id != agent_id).collect();
            let n = fanout.min(filtered.len());
            let neighbors: Vec<&Uuid> = {
                use rand::seq::SliceRandom;
                let mut shuffled = filtered;
                shuffled.shuffle(&mut rng);
                shuffled.into_iter().take(n).collect()
            };

            // Average with neighbors
            for neighbor_id in neighbors {
                if let Some(neighbor_model) = agent_models.get(neighbor_id) {
                    for (i, &param) in neighbor_model.iter().enumerate() {
                        averaged_model[i] += param;
                    }
                    count += 1;
                }
            }

            // Compute average
            for param in &mut averaged_model {
                *param /= count as f32;
            }

            result.insert(*agent_id, averaged_model);
        }

        Ok(result)
    }

    /// Ring all-reduce synchronization
    async fn ring_all_reduce_sync(
        &self,
        agent_models: HashMap<Uuid, Vec<f32>>,
    ) -> Result<HashMap<Uuid, Vec<f32>>> {
        if agent_models.is_empty() {
            return Ok(HashMap::new());
        }

        let agent_ids: Vec<Uuid> = agent_models.keys().cloned().collect();
        let n = agent_ids.len();
        let model_size = agent_models.values().next().unwrap().len();

        // Initialize result with input models
        let mut result = agent_models.clone();

        // Ring reduce-scatter
        for step in 0..n - 1 {
            for i in 0..n {
                let sender_idx = i;
                let receiver_idx = (i + 1) % n;

                let sender_id = &agent_ids[sender_idx];
                let receiver_id = &agent_ids[receiver_idx];

                // Simulate communication: receiver adds sender's chunk
                let chunk_size = model_size / n;
                let chunk_start = (sender_idx + step) % n * chunk_size;
                let chunk_end = chunk_start + chunk_size;

                let sender_model = result[sender_id].clone();
                let receiver_model = result.get_mut(receiver_id).unwrap();

                for j in chunk_start..chunk_end.min(model_size) {
                    receiver_model[j] += sender_model[j];
                }
            }
        }

        // Ring all-gather
        for step in 0..n - 1 {
            for i in 0..n {
                let sender_idx = i;
                let receiver_idx = (i + 1) % n;

                let sender_id = &agent_ids[sender_idx];
                let receiver_id = &agent_ids[receiver_idx];

                // Copy averaged chunk
                let chunk_size = model_size / n;
                let chunk_start = (sender_idx + n - step) % n * chunk_size;
                let chunk_end = chunk_start + chunk_size;

                let sender_model = result[sender_id].clone();
                let receiver_model = result.get_mut(receiver_id).unwrap();

                for j in chunk_start..chunk_end.min(model_size) {
                    receiver_model[j] = sender_model[j] / n as f32;
                }
            }
        }

        Ok(result)
    }
}

/// Synchronization strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncStrategy {
    /// All-reduce (average all models)
    AllReduce,

    /// Parameter server
    ParameterServer { server_id: Uuid },

    /// Gossip protocol
    GossipProtocol { fanout: usize },

    /// Ring all-reduce
    RingAllReduce,
}

/// Model aggregator
#[derive(Debug)]
struct ModelAggregator {
    aggregation_method: AggregationMethod,
}

impl ModelAggregator {
    fn new() -> Self {
        Self {
            aggregation_method: AggregationMethod::FederatedAveraging,
        }
    }
}

/// Aggregation methods
#[derive(Debug, Clone)]
enum AggregationMethod {
    FederatedAveraging,
    WeightedAveraging,
    MedianAggregation,
    TrimmedMean { trim_ratio: f32 },
}

/// Gradient accumulator
#[derive(Debug)]
struct GradientAccumulator {
    gradients: HashMap<Uuid, HashMap<Uuid, (Vec<f32>, usize)>>, // session -> agent -> (gradients, batch_size)
}

impl GradientAccumulator {
    fn new() -> Self {
        Self {
            gradients: HashMap::new(),
        }
    }

    async fn add_gradients(
        &mut self,
        session_id: Uuid,
        agent_id: Uuid,
        gradients: Vec<f32>,
        batch_size: usize,
    ) -> Result<()> {
        self.gradients
            .entry(session_id)
            .or_insert_with(HashMap::new)
            .insert(agent_id, (gradients, batch_size));
        Ok(())
    }

    async fn ready_to_aggregate(&self, session_id: Uuid, expected_agents: usize) -> Result<bool> {
        if let Some(session_grads) = self.gradients.get(&session_id) {
            Ok(session_grads.len() >= expected_agents)
        } else {
            Ok(false)
        }
    }

    async fn aggregate(&self, session_id: Uuid) -> Result<Vec<f32>> {
        let session_grads = self
            .gradients
            .get(&session_id)
            .ok_or_else(|| NeuralMeshError::NotFound("No gradients for session".to_string()))?;

        if session_grads.is_empty() {
            return Err(NeuralMeshError::InvalidInput(
                "No gradients to aggregate".to_string(),
            ));
        }

        // Get gradient size
        let grad_size = session_grads.values().next().unwrap().0.len();
        let mut aggregated = vec![0.0; grad_size];
        let mut total_samples = 0;

        // Weighted average by batch size
        for (gradients, batch_size) in session_grads.values() {
            for (i, &grad) in gradients.iter().enumerate() {
                aggregated[i] += grad * *batch_size as f32;
            }
            total_samples += batch_size;
        }

        // Normalize
        for grad in &mut aggregated {
            *grad /= total_samples as f32;
        }

        Ok(aggregated)
    }

    async fn clear(&mut self, session_id: Uuid) {
        self.gradients.remove(&session_id);
    }
}

/// Training update broadcast messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrainingUpdate {
    SessionStarted {
        session_id: Uuid,
    },
    SessionEnded {
        session_id: Uuid,
    },
    ModelUpdated {
        session_id: Uuid,
        epoch: usize,
        model: Vec<f32>,
    },
    AsyncUpdate {
        session_id: Uuid,
        agent_id: Uuid,
        timestamp: std::time::SystemTime,
    },
}

/// Result of a training session
#[derive(Debug, Clone)]
pub struct TrainingResult {
    pub session_id: Uuid,
    pub final_model: Vec<f32>,
    pub epochs_completed: usize,
    pub participants: usize,
}

/// Model version tracking
#[derive(Debug, Clone)]
struct ModelVersion {
    version: u64,
    timestamp: std::time::SystemTime,
    checksum: u64,
}

/// Training statistics
#[derive(Debug, Clone, Default)]
struct TrainingStats {
    total_sessions: u64,
    active_sessions: usize,
    total_gradient_updates: u64,
    total_model_syncs: u64,
}

use rand::seq::SliceRandom;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_distributed_training_creation() {
        let strategy = TrainingStrategy::FederatedAveraging {
            rounds: 10,
            min_participants: 3,
        };
        let training = DistributedTraining::new(strategy).await;
        assert!(training.is_ok());
    }

    #[tokio::test]
    async fn test_training_session() {
        let training = DistributedTraining::new(TrainingStrategy::FederatedAveraging {
            rounds: 5,
            min_participants: 2,
        })
        .await
        .unwrap();

        let config = TrainingSessionConfig {
            name: "test_session".to_string(),
            max_epochs: 10,
            batch_size: 32,
            learning_rate: 0.01,
            target_accuracy: 0.95,
        };

        let session_id = training.start_session(config).await.unwrap();
        assert!(!session_id.is_nil());
    }

    #[tokio::test]
    async fn test_model_sync_all_reduce() {
        let sync = ModelSync::new(SyncStrategy::AllReduce);

        let mut models = HashMap::new();
        models.insert(Uuid::new_v4(), vec![1.0, 2.0, 3.0]);
        models.insert(Uuid::new_v4(), vec![4.0, 5.0, 6.0]);

        let synced = sync.sync_models(models).await.unwrap();

        // Check that all models are averaged
        for model in synced.values() {
            assert_eq!(model, &vec![2.5, 3.5, 4.5]);
        }
    }
}

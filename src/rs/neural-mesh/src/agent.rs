//! Neural agents that form the basis of distributed cognition

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::{MeshNode, NeuralMesh, NeuralMeshError, Result, ThoughtPattern};

/// Configuration for a neural network inside an agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeuralNetworkConfig {
    /// Layer sizes: input, hidden..., output
    pub layer_sizes: Vec<usize>,
    pub learning_rate: f32,
}

impl Default for NeuralNetworkConfig {
    fn default() -> Self {
        Self {
            layer_sizes: vec![10, 16, 10],
            learning_rate: 0.01,
        }
    }
}

/// Neural agent that performs distributed cognition
#[derive(Debug, Clone)]
pub struct NeuralAgent {
    id: Uuid,
    config: AgentConfig,
    state: Arc<RwLock<AgentState>>,
    network: Arc<RwLock<ruv_fann::Network<f64>>>,
    mesh_node: Arc<RwLock<MeshNode>>,
    message_tx: mpsc::UnboundedSender<AgentMessage>,
    metrics: Arc<RwLock<AgentMetrics>>,
}

impl NeuralAgent {
    /// Create a new neural agent
    pub async fn new(config: AgentConfig, _mesh: Arc<NeuralMesh>) -> Result<Self> {
        let id = Uuid::new_v4();
        let layer_sizes: Vec<usize> = config
            .neural_config
            .layer_sizes
            .iter()
            .map(|&s| s)
            .collect();
        let network = ruv_fann::Network::<f64>::new(&layer_sizes);
        let mesh_node = MeshNode::new(id, config.capabilities.clone());

        let (message_tx, message_rx) = mpsc::unbounded_channel();

        let agent = Self {
            id,
            config: config.clone(),
            state: Arc::new(RwLock::new(AgentState::Idle)),
            network: Arc::new(RwLock::new(network)),
            mesh_node: Arc::new(RwLock::new(mesh_node)),
            message_tx,
            metrics: Arc::new(RwLock::new(AgentMetrics::new())),
        };

        // Spawn message processing task
        let agent_clone = agent.clone();
        tokio::spawn(async move {
            agent_clone.process_messages(message_rx).await;
        });

        Ok(agent)
    }

    /// Get the agent's unique ID
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Get the agent's capabilities
    pub fn capabilities(&self) -> Vec<String> {
        self.config.capabilities.clone()
    }

    /// Check if the agent is active
    pub fn is_active(&self) -> bool {
        true
    }

    /// Stop the agent
    pub async fn stop(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = AgentState::Stopped;
        Ok(())
    }

    /// Get the mesh node representation
    pub async fn get_node(&self) -> MeshNode {
        let node = self.mesh_node.read().await;
        node.clone()
    }

    /// Process a thought pattern
    pub async fn think(&self, pattern: ThoughtPattern) -> Result<ThoughtPattern> {
        let start_time = Instant::now();

        {
            let mut state = self.state.write().await;
            *state = AgentState::Thinking;
        }

        let result = {
            let mut network = self.network.write().await;
            let input: Vec<f64> = pattern
                .to_input_vector()?
                .iter()
                .map(|&x| x as f64)
                .collect();
            let output = network.run(&input);
            ThoughtPattern::from_output_vector(
                output.iter().map(|&x| x as f32).collect(),
                pattern.context.clone(),
            )?
        };

        {
            let mut metrics = self.metrics.write().await;
            metrics.thoughts_processed += 1;
            metrics.total_processing_time_ms += start_time.elapsed().as_millis() as u64;
            metrics.last_activity_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        {
            let mut state = self.state.write().await;
            *state = AgentState::Idle;
        }

        Ok(result)
    }

    /// Learn from a thought pattern
    pub async fn learn(&self, pattern: &ThoughtPattern, target: &ThoughtPattern) -> Result<()> {
        let mut network = self.network.write().await;
        let input: Vec<f64> = pattern
            .to_input_vector()?
            .iter()
            .map(|&x| x as f64)
            .collect();
        let expected_output: Vec<f64> = target
            .to_input_vector()?
            .iter()
            .map(|&x| x as f64)
            .collect();

        network
            .train(
                &[input],
                &[expected_output],
                self.config.neural_config.learning_rate,
                1,
            )
            .map_err(|e| NeuralMeshError::Training(e.to_string()))?;

        {
            let mut metrics = self.metrics.write().await;
            metrics.training_iterations += 1;
        }

        Ok(())
    }

    /// Send a message to another agent
    pub async fn send_message(&self, to: Uuid, content: MessageContent) -> Result<()> {
        let message = AgentMessage {
            from: self.id,
            to,
            content,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        self.message_tx
            .send(message)
            .map_err(|_| NeuralMeshError::Communication("Failed to send message".to_string()))?;

        Ok(())
    }

    /// Get agent metrics
    pub async fn get_metrics(&self) -> AgentMetrics {
        let metrics = self.metrics.read().await;
        metrics.clone()
    }

    /// Process incoming messages
    async fn process_messages(&self, mut message_rx: mpsc::UnboundedReceiver<AgentMessage>) {
        while let Some(message) = message_rx.recv().await {
            if let Err(e) = self.handle_message(message).await {
                tracing::error!("Error handling message: {}", e);
            }
        }
    }

    /// Handle a single message
    async fn handle_message(&self, message: AgentMessage) -> Result<()> {
        match message.content {
            MessageContent::ThoughtShare(pattern) => {
                self.learn_from_peer(&pattern).await?;
            }
            MessageContent::CollaborationRequest(_task) => {
                // no-op in simplified implementation
            }
            MessageContent::SyncRequest => {
                self.handle_sync_request(message.from).await?;
            }
            MessageContent::ModelUpdate(weights) => {
                self.apply_model_update(weights).await?;
            }
        }

        {
            let mut metrics = self.metrics.write().await;
            metrics.messages_received += 1;
        }

        Ok(())
    }

    /// Learn from a peer's thought pattern (unsupervised)
    async fn learn_from_peer(&self, pattern: &ThoughtPattern) -> Result<()> {
        let mut network = self.network.write().await;
        let input: Vec<f64> = pattern
            .to_input_vector()?
            .iter()
            .map(|&x| x as f64)
            .collect();

        network
            .train(
                &[input.clone()],
                &[input],
                self.config.neural_config.learning_rate,
                1,
            )
            .map_err(|e| NeuralMeshError::Training(e.to_string()))?;

        Ok(())
    }

    /// Handle sync request from another agent
    async fn handle_sync_request(&self, from: Uuid) -> Result<()> {
        let network = self.network.read().await;
        let weights: Vec<f32> = network.get_weights().iter().map(|&x| x as f32).collect();

        self.send_message(from, MessageContent::ModelUpdate(weights))
            .await?;
        Ok(())
    }

    /// Apply model update from another agent
    async fn apply_model_update(&self, weights: Vec<f32>) -> Result<()> {
        let mut network = self.network.write().await;

        let current_weights = network.get_weights();
        let averaged_weights: Vec<f64> = current_weights
            .iter()
            .zip(weights.iter())
            .map(|(&current, &new)| (current + new as f64) / 2.0)
            .collect();

        network
            .set_weights(&averaged_weights)
            .map_err(|e| NeuralMeshError::Training(e.to_string()))?;

        {
            let mut metrics = self.metrics.write().await;
            metrics.model_syncs += 1;
        }

        Ok(())
    }
}

/// Configuration for a neural agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub name: String,
    pub neural_config: NeuralNetworkConfig,
    pub capabilities: Vec<String>,
    pub max_connections: usize,
    pub learning_rate: f64,
}

/// Current state of a neural agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentState {
    Idle,
    Thinking,
    Learning,
    Communicating,
    Syncing,
    Stopped,
}

/// Metrics for monitoring agent performance.
/// Uses `last_activity_secs` (Unix timestamp) instead of `Instant` to stay Serialize-safe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMetrics {
    pub thoughts_processed: u64,
    pub training_iterations: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub model_syncs: u64,
    pub total_processing_time_ms: u64,
    pub last_activity_secs: u64,
    pub accuracy: f64,
}

impl AgentMetrics {
    fn new() -> Self {
        Self {
            thoughts_processed: 0,
            training_iterations: 0,
            messages_sent: 0,
            messages_received: 0,
            model_syncs: 0,
            total_processing_time_ms: 0,
            last_activity_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            accuracy: 0.0,
        }
    }

    pub fn average_processing_time(&self) -> Duration {
        if self.thoughts_processed > 0 {
            Duration::from_millis(self.total_processing_time_ms / self.thoughts_processed)
        } else {
            Duration::ZERO
        }
    }
}

/// Messages exchanged between agents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMessage {
    pub from: Uuid,
    pub to: Uuid,
    pub content: MessageContent,
    pub timestamp: u64,
}

/// Content of agent messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageContent {
    ThoughtShare(ThoughtPattern),
    CollaborationRequest(String),
    SyncRequest,
    ModelUpdate(Vec<f32>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_agent_config() {
        let config = AgentConfig {
            name: "test-agent".to_string(),
            neural_config: NeuralNetworkConfig::default(),
            capabilities: vec!["test".to_string()],
            max_connections: 5,
            learning_rate: 0.01,
        };

        assert_eq!(config.name, "test-agent");
        assert_eq!(config.capabilities, vec!["test"]);
    }

    #[tokio::test]
    async fn test_agent_metrics() {
        let metrics = AgentMetrics::new();
        assert_eq!(metrics.thoughts_processed, 0);
        assert_eq!(metrics.training_iterations, 0);
    }

    #[test]
    fn test_agent_message_serialization() {
        let message = AgentMessage {
            from: Uuid::new_v4(),
            to: Uuid::new_v4(),
            content: MessageContent::SyncRequest,
            timestamp: 1234567890,
        };

        let serialized = serde_json::to_string(&message).unwrap();
        let deserialized: AgentMessage = serde_json::from_str(&serialized).unwrap();

        assert_eq!(message.from, deserialized.from);
        assert_eq!(message.to, deserialized.to);
        assert_eq!(message.timestamp, deserialized.timestamp);
    }
}

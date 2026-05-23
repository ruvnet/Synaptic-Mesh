//! Neural Mesh - Distributed cognition layer for Synaptic Neural Mesh
//!
//! This crate provides the distributed neural cognition capabilities that connect
//! QuDAG networking with ruv-FANN neural networks and DAA swarm intelligence.

pub mod agent;
pub mod cognition;
pub mod coordinator;
pub mod distributed;
pub mod error;
pub mod mesh;

pub use agent::{AgentConfig, AgentMetrics, AgentState, NeuralAgent};
pub use cognition::{CognitionEngine, CognitionResult, CognitionTask, ThoughtPattern};
pub use coordinator::{CoordinationStrategy, MeshCoordinator, TaskDistribution};
pub use distributed::{DistributedTraining, ModelSync, TrainingStrategy};
pub use error::{NeuralMeshError, Result};
pub use mesh::{ConnectionStrength, MeshNode, MeshTopology, NeuralMesh};

use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Main neural mesh instance that coordinates distributed cognition
#[derive(Debug)]
pub struct SynapticNeuralMesh {
    mesh: Arc<NeuralMesh>,
    coordinator: Arc<MeshCoordinator>,
    agents: Arc<RwLock<std::collections::HashMap<Uuid, NeuralAgent>>>,
    cognition_engine: Arc<CognitionEngine>,
    config: MeshConfig,
}

impl SynapticNeuralMesh {
    /// Create a new Synaptic Neural Mesh
    pub async fn new(config: MeshConfig) -> Result<Self> {
        let mesh = Arc::new(NeuralMesh::new(config.mesh_topology.clone()).await?);
        let coordinator =
            Arc::new(MeshCoordinator::new(config.coordination_strategy.clone()).await?);
        let agents = Arc::new(RwLock::new(std::collections::HashMap::new()));
        let cognition_engine =
            Arc::new(CognitionEngine::new(config.cognition_config.clone()).await?);

        Ok(Self {
            mesh,
            coordinator,
            agents,
            cognition_engine,
            config,
        })
    }

    /// Start the neural mesh
    pub async fn start(&self) -> Result<()> {
        // Start mesh networking
        self.mesh.start().await?;

        // Start coordination
        self.coordinator.start().await?;

        // Start cognition engine
        self.cognition_engine.start().await?;

        // Spawn initial agents
        self.spawn_initial_agents().await?;

        tracing::info!("Synaptic Neural Mesh started successfully");
        Ok(())
    }

    /// Stop the neural mesh
    pub async fn stop(&self) -> Result<()> {
        // Stop all agents
        let mut agents = self.agents.write().await;
        for agent in agents.values_mut() {
            agent.stop().await?;
        }
        agents.clear();

        // Stop cognition engine
        self.cognition_engine.stop().await?;

        // Stop coordinator
        self.coordinator.stop().await?;

        // Stop mesh
        self.mesh.stop().await?;

        tracing::info!("Synaptic Neural Mesh stopped");
        Ok(())
    }

    /// Create a new neural agent
    pub async fn create_agent(&self, config: AgentConfig) -> Result<Uuid> {
        let agent = NeuralAgent::new(config, Arc::clone(&self.mesh)).await?;
        let agent_id = agent.id();

        // Add to mesh
        self.mesh
            .add_agent(agent_id, agent.get_node().await)
            .await?;

        // Register with coordinator
        self.coordinator
            .register_agent(agent_id, agent.capabilities())
            .await?;

        // Store agent
        {
            let mut agents = self.agents.write().await;
            agents.insert(agent_id, agent);
        }

        tracing::info!("Created neural agent: {}", agent_id);
        Ok(agent_id)
    }

    /// Remove a neural agent
    pub async fn remove_agent(&self, agent_id: Uuid) -> Result<bool> {
        let mut agents = self.agents.write().await;
        if let Some(mut agent) = agents.remove(&agent_id) {
            agent.stop().await?;
            self.mesh.remove_agent(agent_id).await?;
            self.coordinator.unregister_agent(agent_id).await?;
            tracing::info!("Removed neural agent: {}", agent_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Submit a cognition task to the mesh
    pub async fn think(&self, task: CognitionTask) -> Result<CognitionResult> {
        // Distribute task through coordinator
        let assignment = self.coordinator.assign_task(task.clone()).await?;

        // Execute distributed cognition
        let result = self
            .cognition_engine
            .process_distributed(task, assignment)
            .await?;

        // Update mesh based on results
        self.mesh.update_connections(&result).await?;

        Ok(result)
    }

    /// Get mesh statistics
    pub async fn get_stats(&self) -> MeshStats {
        let agents = self.agents.read().await;
        let mesh_stats = self.mesh.get_stats().await;
        let coordinator_stats = self.coordinator.get_stats().await;
        let cognition_stats = self.cognition_engine.get_stats().await;

        MeshStats {
            total_agents: agents.len(),
            active_agents: agents.values().filter(|a| a.is_active()).count(),
            mesh_connections: mesh_stats.connections,
            total_thoughts: cognition_stats.total_tasks,
            coordinator_load: coordinator_stats.load_factor,
            avg_response_time: cognition_stats.avg_response_time,
        }
    }

    /// Get agent by ID
    pub async fn get_agent(&self, agent_id: Uuid) -> Option<NeuralAgent> {
        let agents = self.agents.read().await;
        agents.get(&agent_id).cloned()
    }

    /// List all agents
    pub async fn list_agents(&self) -> Vec<Uuid> {
        let agents = self.agents.read().await;
        agents.keys().cloned().collect()
    }

    /// Spawn initial agents based on configuration
    async fn spawn_initial_agents(&self) -> Result<()> {
        for i in 0..self.config.initial_agent_count {
            let config = AgentConfig {
                name: format!("agent-{}", i),
                neural_config: self.config.default_neural_config.clone(),
                capabilities: vec![
                    "pattern_recognition".to_string(),
                    "memory_formation".to_string(),
                    "decision_making".to_string(),
                ],
                max_connections: 10,
                learning_rate: 0.01,
            };

            self.create_agent(config).await?;
        }

        Ok(())
    }
}

/// Configuration for the neural mesh
#[derive(Debug, Clone)]
pub struct MeshConfig {
    pub mesh_topology: MeshTopology,
    pub coordination_strategy: CoordinationStrategy,
    pub cognition_config: cognition::CognitionConfig,
    pub initial_agent_count: usize,
    pub default_neural_config: agent::NeuralNetworkConfig,
    pub sync_interval: std::time::Duration,
    pub max_agents: usize,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            mesh_topology: MeshTopology::SmallWorld { k: 6, p: 0.1 },
            coordination_strategy: CoordinationStrategy::Adaptive,
            cognition_config: cognition::CognitionConfig::default(),
            initial_agent_count: 5,
            default_neural_config: agent::NeuralNetworkConfig::default(),
            sync_interval: std::time::Duration::from_secs(30),
            max_agents: 100,
        }
    }
}

/// Statistics about the neural mesh
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MeshStats {
    pub total_agents: usize,
    pub active_agents: usize,
    pub mesh_connections: usize,
    pub total_thoughts: u64,
    pub coordinator_load: f64,
    pub avg_response_time: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mesh_creation() {
        let config = MeshConfig::default();
        let mesh = SynapticNeuralMesh::new(config).await;
        assert!(mesh.is_ok());
    }

    #[tokio::test]
    async fn test_mesh_lifecycle() {
        let config = MeshConfig::default();
        let mesh = SynapticNeuralMesh::new(config).await.unwrap();

        assert!(mesh.start().await.is_ok());
        assert!(mesh.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_agent_management() {
        let config = MeshConfig::default();
        let mesh = SynapticNeuralMesh::new(config).await.unwrap();

        let agent_config = AgentConfig {
            name: "test-agent".to_string(),
            neural_config: agent::NeuralNetworkConfig::default(),
            capabilities: vec!["test".to_string()],
            max_connections: 5,
            learning_rate: 0.01,
        };

        let agent_id = mesh.create_agent(agent_config).await.unwrap();
        assert!(mesh.get_agent(agent_id).await.is_some());

        assert!(mesh.remove_agent(agent_id).await.unwrap());
        assert!(mesh.get_agent(agent_id).await.is_none());
    }
}

//! DAA Swarm - Dynamic Agent Architecture for swarm intelligence
//!
//! This crate implements distributed swarm intelligence with autonomous agents
//! that can coordinate, learn, and adapt in real-time environments.

pub mod evolutionary_mesh;
pub mod self_organizing;
pub mod swarm_intelligence;

pub use evolutionary_mesh::{
    AdaptationEngine, EvolutionaryMesh, MeshConnection, MeshNode, MeshTopology,
};
pub use self_organizing::{
    EmergenceRule, NodeCluster, OrganizationPattern, SelfOrganizingSystem, Stigmergy,
};
pub use swarm_intelligence::{
    AgentGenome, EvolutionaryParams, FitnessMetrics, OptimizationStrategy, SwarmIntelligence,
};

use std::time::Duration;

/// Configuration for swarm intelligence features
#[derive(Debug, Clone)]
pub struct SwarmIntelligenceConfig {
    pub optimization_strategy: OptimizationStrategy,
    pub mesh_topology: MeshTopology,
    pub organization_pattern: OrganizationPattern,
    pub initial_population_size: usize,
    pub evolution_interval: Duration,
    pub organization_interval: Duration,
    pub evolutionary_params: EvolutionaryParams,
}

impl Default for SwarmIntelligenceConfig {
    fn default() -> Self {
        Self {
            optimization_strategy: OptimizationStrategy::HybridAdaptive,
            mesh_topology: MeshTopology::Adaptive,
            organization_pattern: OrganizationPattern::Dynamic,
            initial_population_size: 50,
            evolution_interval: Duration::from_secs(30),
            organization_interval: Duration::from_secs(60),
            evolutionary_params: EvolutionaryParams::default(),
        }
    }
}

/// Error type for swarm operations
#[derive(Debug, thiserror::Error)]
pub enum SwarmError {
    #[error("Coordination error: {0}")]
    CoordinationError(String),
    #[error("Agent error: {0}")]
    AgentError(String),
    #[error("Adaptation error: {0}")]
    AdaptationError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Result type for swarm operations
pub type Result<T> = std::result::Result<T, SwarmError>;

/// Agent type enumeration for DAA
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AgentType {
    Cognitive,
    Communication,
    Storage,
    Computation,
    Coordination,
    Custom(String),
}

/// Agent capabilities descriptor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentCapabilities {
    pub agent_type: AgentType,
    pub max_throughput: f64,
    pub supported_protocols: Vec<String>,
    pub neural_capacity: usize,
}

impl Default for AgentCapabilities {
    fn default() -> Self {
        Self {
            agent_type: AgentType::Cognitive,
            max_throughput: 1000.0,
            supported_protocols: vec!["neural-mesh".to_string()],
            neural_capacity: 100,
        }
    }
}

/// Configuration for the Dynamic Agent Architecture
#[derive(Debug, Clone)]
pub struct ArchitectureConfig {
    pub swarm_config: SwarmIntelligenceConfig,
    pub max_agents: usize,
    pub auto_scale: bool,
    pub fault_tolerance: bool,
}

impl Default for ArchitectureConfig {
    fn default() -> Self {
        Self {
            swarm_config: SwarmIntelligenceConfig::default(),
            max_agents: 100,
            auto_scale: true,
            fault_tolerance: true,
        }
    }
}

/// Dynamic Agent Architecture coordinator
pub struct DynamicAgentArchitecture {
    config: ArchitectureConfig,
    swarm: std::sync::Arc<SwarmIntelligence>,
    mesh: std::sync::Arc<EvolutionaryMesh>,
    system: std::sync::Arc<SelfOrganizingSystem>,
}

impl DynamicAgentArchitecture {
    pub async fn new(config: ArchitectureConfig) -> Result<Self> {
        let swarm = std::sync::Arc::new(SwarmIntelligence::new(
            config.swarm_config.optimization_strategy.clone(),
        ));
        let mesh = std::sync::Arc::new(EvolutionaryMesh::new(
            config.swarm_config.mesh_topology.clone(),
            config.swarm_config.optimization_strategy.clone(),
        ));
        let system = std::sync::Arc::new(SelfOrganizingSystem::new(
            config.swarm_config.organization_pattern.clone(),
        ));
        Ok(Self {
            config,
            swarm,
            mesh,
            system,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.swarm
            .initialize_population(self.config.swarm_config.initial_population_size)
            .await;
        self.mesh
            .initialize(self.config.swarm_config.initial_population_size)
            .await;
        self.system.initialize_rules().await;
        tracing::info!("DynamicAgentArchitecture started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        tracing::info!("DynamicAgentArchitecture stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = SwarmIntelligenceConfig::default();
        assert_eq!(config.initial_population_size, 50);
        assert!(matches!(
            config.optimization_strategy,
            OptimizationStrategy::HybridAdaptive
        ));
    }
}

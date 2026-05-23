use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use crate::swarm_intelligence::{
    AgentGenome, EvolutionaryParams, FitnessMetrics, OptimizationStrategy, SwarmContext,
    SwarmIntelligence,
};

/// Mesh topology for evolutionary optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MeshTopology {
    FullyConnected,
    Ring,
    Star,
    Grid,
    SmallWorld,
    ScaleFree,
    Adaptive,
}

/// Connection strength between mesh nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConnection {
    pub from_id: String,
    pub to_id: String,
    pub weight: f64,
    pub latency: f64,
    pub reliability: f64,
    pub bandwidth: f64,
}

/// Evolutionary mesh node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshNode {
    pub id: String,
    pub genome: AgentGenome,
    pub connections: Vec<MeshConnection>,
    pub local_memory: HashMap<String, Vec<f64>>,
    pub performance_history: VecDeque<FitnessMetrics>,
    pub adaptation_state: AdaptationState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptationState {
    pub learning_rate: f64,
    pub exploration_probability: f64,
    pub cooperation_score: f64,
    pub specialization: HashMap<String, f64>,
    pub last_mutation_generation: u64,
}

/// Evolutionary mesh coordinator
pub struct EvolutionaryMesh {
    nodes: Arc<RwLock<HashMap<String, MeshNode>>>,
    topology: MeshTopology,
    swarm_intelligence: Arc<SwarmIntelligence>,
    evolution_params: Arc<RwLock<EvolutionaryParams>>,
    mesh_metrics: Arc<RwLock<MeshMetrics>>,
    adaptation_engine: Arc<AdaptationEngine>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshMetrics {
    pub total_nodes: usize,
    pub active_connections: usize,
    pub average_fitness: f64,
    pub diversity_index: f64,
    pub convergence_rate: f64,
    pub adaptation_efficiency: f64,
    pub communication_overhead: f64,
}

/// Adaptation engine for mesh evolution
pub struct AdaptationEngine {
    strategies: HashMap<String, Box<dyn AdaptationStrategy>>,
    learning_history: Arc<RwLock<Vec<LearningEvent>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearningEvent {
    pub timestamp: u64,
    pub node_id: String,
    pub event_type: LearningEventType,
    pub fitness_delta: f64,
    pub context: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LearningEventType {
    Discovery,
    Optimization,
    Cooperation,
    Specialization,
    Failure,
    Recovery,
}

/// Trait for adaptation strategies
#[async_trait]
pub trait AdaptationStrategy: Send + Sync {
    async fn adapt(&self, node: &mut MeshNode, context: &MeshContext) -> Result<(), String>;
    fn applicable(&self, node: &MeshNode) -> bool;
    fn priority(&self) -> u32;
}

/// Context for mesh operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshContext {
    pub mesh_state: HashMap<String, f64>,
    pub network_conditions: NetworkConditions,
    pub task_requirements: TaskRequirements,
    pub environmental_factors: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConditions {
    pub average_latency: f64,
    pub packet_loss: f64,
    pub bandwidth_utilization: f64,
    pub node_churn_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequirements {
    pub computational_intensity: f64,
    pub communication_intensity: f64,
    pub reliability_requirement: f64,
    pub latency_sensitivity: f64,
}

impl EvolutionaryMesh {
    pub fn new(topology: MeshTopology, optimization_strategy: OptimizationStrategy) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            topology,
            swarm_intelligence: Arc::new(SwarmIntelligence::new(optimization_strategy)),
            evolution_params: Arc::new(RwLock::new(EvolutionaryParams::default())),
            mesh_metrics: Arc::new(RwLock::new(MeshMetrics {
                total_nodes: 0,
                active_connections: 0,
                average_fitness: 0.0,
                diversity_index: 1.0,
                convergence_rate: 0.0,
                adaptation_efficiency: 0.0,
                communication_overhead: 0.0,
            })),
            adaptation_engine: Arc::new(AdaptationEngine::new()),
        }
    }

    /// Initialize mesh with nodes
    pub async fn initialize(&self, num_nodes: usize) {
        // Initialize swarm population
        self.swarm_intelligence
            .initialize_population(num_nodes)
            .await;

        // Create mesh nodes
        let mut nodes = self.nodes.write().unwrap();
        nodes.clear();

        for i in 0..num_nodes {
            let node = MeshNode {
                id: format!("node_{}", i),
                genome: AgentGenome {
                    id: format!("genome_{}", i),
                    behavioral_traits: Self::initialize_traits(),
                    communication_weights: vec![0.5; 10],
                    decision_weights: vec![0.5; 15],
                    fitness: FitnessMetrics {
                        throughput: 0.0,
                        latency: 1000.0,
                        error_rate: 1.0,
                        resource_efficiency: 0.0,
                        adaptation_score: 0.0,
                        cooperation_index: 0.5,
                    },
                    generation: 0,
                },
                connections: Vec::new(),
                local_memory: HashMap::new(),
                performance_history: VecDeque::with_capacity(100),
                adaptation_state: AdaptationState {
                    learning_rate: 0.1,
                    exploration_probability: 0.3,
                    cooperation_score: 0.5,
                    specialization: HashMap::new(),
                    last_mutation_generation: 0,
                },
            };
            nodes.insert(node.id.clone(), node);
        }

        // Establish connections based on topology
        self.establish_topology().await;
    }

    /// Establish mesh topology connections
    pub async fn establish_topology(&self) {
        let mut nodes = self.nodes.write().unwrap();
        let node_ids: Vec<String> = nodes.keys().cloned().collect();

        match self.topology {
            MeshTopology::FullyConnected => {
                for i in 0..node_ids.len() {
                    for j in 0..node_ids.len() {
                        if i != j {
                            self.create_connection(&mut nodes, &node_ids[i], &node_ids[j]);
                        }
                    }
                }
            }
            MeshTopology::Ring => {
                for i in 0..node_ids.len() {
                    let next = (i + 1) % node_ids.len();
                    self.create_connection(&mut nodes, &node_ids[i], &node_ids[next]);
                    self.create_connection(&mut nodes, &node_ids[next], &node_ids[i]);
                }
            }
            MeshTopology::Star => {
                if !node_ids.is_empty() {
                    let hub = &node_ids[0];
                    for i in 1..node_ids.len() {
                        self.create_connection(&mut nodes, hub, &node_ids[i]);
                        self.create_connection(&mut nodes, &node_ids[i], hub);
                    }
                }
            }
            MeshTopology::Grid => {
                let size = (node_ids.len() as f64).sqrt() as usize;
                for i in 0..node_ids.len() {
                    let row = i / size;
                    let col = i % size;

                    // Connect to right neighbor
                    if col < size - 1 {
                        let right = i + 1;
                        self.create_connection(&mut nodes, &node_ids[i], &node_ids[right]);
                    }

                    // Connect to bottom neighbor
                    if row < size - 1 {
                        let bottom = i + size;
                        if bottom < node_ids.len() {
                            self.create_connection(&mut nodes, &node_ids[i], &node_ids[bottom]);
                        }
                    }
                }
            }
            MeshTopology::SmallWorld => {
                // Start with ring topology
                for i in 0..node_ids.len() {
                    let next = (i + 1) % node_ids.len();
                    self.create_connection(&mut nodes, &node_ids[i], &node_ids[next]);
                }

                // Add random shortcuts
                let num_shortcuts = node_ids.len() / 10;
                for _ in 0..num_shortcuts {
                    let i = rand::random::<usize>() % node_ids.len();
                    let j = rand::random::<usize>() % node_ids.len();
                    if i != j {
                        self.create_connection(&mut nodes, &node_ids[i], &node_ids[j]);
                    }
                }
            }
            MeshTopology::ScaleFree => {
                // Preferential attachment (Barabási-Albert model)
                if node_ids.len() > 2 {
                    // Start with a small fully connected core
                    for i in 0..3.min(node_ids.len()) {
                        for j in 0..3.min(node_ids.len()) {
                            if i != j {
                                self.create_connection(&mut nodes, &node_ids[i], &node_ids[j]);
                            }
                        }
                    }

                    // Add remaining nodes with preferential attachment
                    for i in 3..node_ids.len() {
                        let num_connections = 2.min(i);
                        for _ in 0..num_connections {
                            let target =
                                self.preferential_attachment_selection(&nodes, &node_ids[..i]);
                            self.create_connection(&mut nodes, &node_ids[i], &target);
                        }
                    }
                }
            }
            MeshTopology::Adaptive => {
                // Start with sparse random connections
                for i in 0..node_ids.len() {
                    let num_connections = 3.min(node_ids.len() - 1);
                    for _ in 0..num_connections {
                        let j = rand::random::<usize>() % node_ids.len();
                        if i != j {
                            self.create_connection(&mut nodes, &node_ids[i], &node_ids[j]);
                        }
                    }
                }
            }
        }
    }

    /// Evolve the mesh using swarm intelligence
    pub async fn evolve(&self, context: &MeshContext) {
        // Convert mesh context to swarm context
        let swarm_context = SwarmContext {
            agent_count: self.nodes.read().unwrap().len(),
            network_load: context.network_conditions.bandwidth_utilization,
            error_rate: context.network_conditions.packet_loss,
            resource_usage: context.mesh_state.clone(),
            environment_state: context.environmental_factors.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Evolve using swarm intelligence
        self.swarm_intelligence.evolve(&swarm_context).await;

        // Apply adaptations to mesh nodes
        self.apply_adaptations(context).await;

        // Update mesh metrics
        self.update_metrics().await;

        // Reorganize topology if adaptive
        if matches!(self.topology, MeshTopology::Adaptive) {
            self.adapt_topology(context).await;
        }
    }

    /// Apply adaptations to mesh nodes
    async fn apply_adaptations(&self, context: &MeshContext) {
        let mut nodes = self.nodes.write().unwrap();

        for node in nodes.values_mut() {
            // Apply adaptation strategies
            self.adaptation_engine.adapt_node(node, context).await;

            // Update performance history
            if node.performance_history.len() >= 100 {
                node.performance_history.pop_front();
            }
            node.performance_history
                .push_back(node.genome.fitness.clone());

            // Update adaptation state
            self.update_adaptation_state(node, context);
        }
    }

    /// Update adaptation state based on performance
    fn update_adaptation_state(&self, node: &mut MeshNode, context: &MeshContext) {
        // Calculate performance trend
        let recent_fitness = node
            .performance_history
            .iter()
            .rev()
            .take(10)
            .map(|f| self.calculate_fitness_score(f))
            .sum::<f64>()
            / (10.0_f64).min(node.performance_history.len() as f64);

        let older_fitness = node
            .performance_history
            .iter()
            .rev()
            .skip(10)
            .take(10)
            .map(|f| self.calculate_fitness_score(f))
            .sum::<f64>()
            / (10.0_f64).min(node.performance_history.len().saturating_sub(10) as f64);

        let fitness_trend = recent_fitness - older_fitness;

        // Adjust learning rate based on performance
        if fitness_trend > 0.0 {
            node.adaptation_state.learning_rate *= 0.95; // Decrease learning rate when improving
        } else {
            node.adaptation_state.learning_rate *= 1.05; // Increase learning rate when stagnating
        }
        node.adaptation_state.learning_rate = node.adaptation_state.learning_rate.clamp(0.01, 0.5);

        // Adjust exploration probability
        if fitness_trend < -0.1 {
            node.adaptation_state.exploration_probability *= 1.1; // Explore more when performance drops
        } else {
            node.adaptation_state.exploration_probability *= 0.95; // Exploit more when performing well
        }
        node.adaptation_state.exploration_probability = node
            .adaptation_state
            .exploration_probability
            .clamp(0.05, 0.5);

        // Update specialization based on task requirements
        if context.task_requirements.computational_intensity > 0.7 {
            *node
                .adaptation_state
                .specialization
                .entry("computation".to_string())
                .or_insert(0.0) += 0.1;
        }
        if context.task_requirements.communication_intensity > 0.7 {
            *node
                .adaptation_state
                .specialization
                .entry("communication".to_string())
                .or_insert(0.0) += 0.1;
        }
    }

    /// Adapt topology based on performance
    async fn adapt_topology(&self, context: &MeshContext) {
        let mut nodes = self.nodes.write().unwrap();
        let node_ids: Vec<String> = nodes.keys().cloned().collect();

        // Remove underperforming connections
        for node_id in &node_ids {
            if let Some(node) = nodes.get_mut(node_id) {
                node.connections.retain(|conn| {
                    conn.reliability > 0.5
                        && conn.latency < context.task_requirements.latency_sensitivity * 1000.0
                });
            }
        }

        // Add new connections between high-performing nodes
        let high_performers: Vec<String> = nodes
            .values()
            .filter(|n| self.calculate_fitness_score(&n.genome.fitness) > 0.8)
            .map(|n| n.id.clone())
            .collect();

        for i in 0..high_performers.len() {
            for j in i + 1..high_performers.len() {
                if !self.has_connection(&nodes, &high_performers[i], &high_performers[j]) {
                    self.create_connection(&mut nodes, &high_performers[i], &high_performers[j]);
                }
            }
        }
    }

    /// Update mesh metrics
    async fn update_metrics(&self) {
        let nodes = self.nodes.read().unwrap();
        let mut metrics = self.mesh_metrics.write().unwrap();

        metrics.total_nodes = nodes.len();
        metrics.active_connections = nodes.values().map(|n| n.connections.len()).sum::<usize>() / 2;

        // Calculate average fitness
        let total_fitness: f64 = nodes
            .values()
            .map(|n| self.calculate_fitness_score(&n.genome.fitness))
            .sum();
        metrics.average_fitness = total_fitness / nodes.len().max(1) as f64;

        // Calculate diversity index
        metrics.diversity_index = self.calculate_diversity(&nodes);

        // Calculate convergence rate (simplified)
        metrics.convergence_rate = 1.0 - metrics.diversity_index;

        // Calculate adaptation efficiency
        let adapted_nodes = nodes
            .values()
            .filter(|n| n.adaptation_state.learning_rate < 0.1)
            .count();
        metrics.adaptation_efficiency = adapted_nodes as f64 / nodes.len().max(1) as f64;

        // Calculate communication overhead
        let total_bandwidth: f64 = nodes
            .values()
            .flat_map(|n| &n.connections)
            .map(|c| c.bandwidth)
            .sum();
        metrics.communication_overhead =
            total_bandwidth / (nodes.len().max(1) * nodes.len().max(1)) as f64;
    }

    /// Helper functions
    fn create_connection(&self, nodes: &mut HashMap<String, MeshNode>, from: &str, to: &str) {
        if let Some(from_node) = nodes.get_mut(from) {
            let connection = MeshConnection {
                from_id: from.to_string(),
                to_id: to.to_string(),
                weight: 1.0,
                latency: 10.0 + rand::random::<f64>() * 90.0,
                reliability: 0.9 + rand::random::<f64>() * 0.1,
                bandwidth: 100.0 + rand::random::<f64>() * 900.0,
            };
            from_node.connections.push(connection);
        }
    }

    fn has_connection(&self, nodes: &HashMap<String, MeshNode>, from: &str, to: &str) -> bool {
        nodes
            .get(from)
            .map(|n| n.connections.iter().any(|c| c.to_id == to))
            .unwrap_or(false)
    }

    fn preferential_attachment_selection(
        &self,
        nodes: &HashMap<String, MeshNode>,
        candidates: &[String],
    ) -> String {
        let degrees: Vec<(String, usize)> = candidates
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    nodes.get(id).map(|n| n.connections.len()).unwrap_or(0),
                )
            })
            .collect();

        let total_degree: usize = degrees.iter().map(|(_, d)| d).sum();
        if total_degree == 0 {
            return candidates[0].clone();
        }

        let mut random_value = rand::random::<f64>() * total_degree as f64;
        for (id, degree) in degrees {
            random_value -= degree as f64;
            if random_value <= 0.0 {
                return id;
            }
        }

        candidates.last().unwrap().clone()
    }

    fn calculate_fitness_score(&self, fitness: &FitnessMetrics) -> f64 {
        fitness.throughput * 0.3
            + (1.0 / fitness.latency.max(1.0)) * 1000.0 * 0.2
            + (1.0 - fitness.error_rate) * 0.2
            + fitness.resource_efficiency * 0.1
            + fitness.adaptation_score * 0.1
            + fitness.cooperation_index * 0.1
    }

    fn calculate_diversity(&self, nodes: &HashMap<String, MeshNode>) -> f64 {
        if nodes.len() < 2 {
            return 1.0;
        }

        let mut trait_variances = HashMap::new();

        // Calculate mean for each trait
        for node in nodes.values() {
            for (trait_name, trait_value) in &node.genome.behavioral_traits {
                trait_variances
                    .entry(trait_name.clone())
                    .or_insert(Vec::new())
                    .push(*trait_value);
            }
        }

        // Calculate variance for each trait
        let mut total_variance = 0.0;
        for values in trait_variances.values() {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            total_variance += variance;
        }

        total_variance / trait_variances.len().max(1) as f64
    }

    fn initialize_traits() -> HashMap<String, f64> {
        let mut traits = HashMap::new();
        traits.insert("exploration_tendency".to_string(), rand::random());
        traits.insert("cooperation_level".to_string(), rand::random());
        traits.insert("risk_tolerance".to_string(), rand::random());
        traits.insert("learning_rate".to_string(), rand::random());
        traits.insert("communication_frequency".to_string(), rand::random());
        traits.insert("adaptability".to_string(), rand::random());
        traits.insert("resource_efficiency".to_string(), rand::random());
        traits
    }

    /// Get mesh statistics
    pub async fn get_stats(&self) -> MeshMetrics {
        self.mesh_metrics.read().unwrap().clone()
    }

    /// Get node by ID
    pub async fn get_node(&self, node_id: &str) -> Option<MeshNode> {
        self.nodes.read().unwrap().get(node_id).cloned()
    }

    /// Get all nodes
    pub async fn get_all_nodes(&self) -> Vec<MeshNode> {
        self.nodes.read().unwrap().values().cloned().collect()
    }
}

impl AdaptationEngine {
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
            learning_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn adapt_node(&self, node: &mut MeshNode, context: &MeshContext) {
        let mut applicable_strategies: Vec<_> = self
            .strategies
            .values()
            .filter(|s| s.applicable(node))
            .collect();

        applicable_strategies.sort_by_key(|s| std::cmp::Reverse(s.priority()));

        for strategy in applicable_strategies {
            if let Err(e) = strategy.adapt(node, context).await {
                tracing::error!("Adaptation strategy failed: {}", e);
            }
        }
    }

    pub fn register_strategy(&mut self, name: String, strategy: Box<dyn AdaptationStrategy>) {
        self.strategies.insert(name, strategy);
    }

    pub fn record_learning_event(&self, event: LearningEvent) {
        let mut history = self.learning_history.write().unwrap();
        history.push(event);

        // Keep only recent history
        if history.len() > 10000 {
            history.drain(0..1000);
        }
    }
}

// External dependencies placeholder
mod rand {
    pub fn random<T>() -> T
    where
        T: Default,
    {
        T::default()
    }
}

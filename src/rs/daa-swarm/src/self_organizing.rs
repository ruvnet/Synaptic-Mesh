use async_recursion::async_recursion;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Self-organizing patterns for autonomous mesh behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrganizationPattern {
    Hierarchical,
    Flat,
    Clustered,
    Dynamic,
    Emergent,
}

/// Emergence rules for self-organization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmergenceRule {
    pub id: String,
    pub condition: EmergenceCondition,
    pub action: EmergenceAction,
    pub priority: u32,
    pub activation_count: u64,
    pub success_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EmergenceCondition {
    ThresholdReached {
        metric: String,
        threshold: f64,
    },
    PatternDetected {
        pattern: String,
        confidence: f64,
    },
    TimeElapsed {
        duration_ms: u64,
    },
    EventOccurred {
        event_type: String,
        count: u32,
    },
    Composite {
        conditions: Vec<EmergenceCondition>,
        operator: LogicalOperator,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EmergenceAction {
    FormCluster {
        min_size: usize,
        max_size: usize,
    },
    SplitCluster {
        cluster_id: String,
    },
    ElectLeader {
        selection_criteria: LeaderSelectionCriteria,
    },
    MigrateNodes {
        from_cluster: String,
        to_cluster: String,
        count: usize,
    },
    AdaptTopology {
        new_pattern: OrganizationPattern,
    },
    TriggerEvolution {
        intensity: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderSelectionCriteria {
    pub fitness_weight: f64,
    pub experience_weight: f64,
    pub centrality_weight: f64,
    pub stability_weight: f64,
}

/// Cluster of self-organized nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCluster {
    pub id: String,
    pub members: HashSet<String>,
    pub leader: Option<String>,
    pub purpose: ClusterPurpose,
    pub formation_time: u64,
    pub performance_metrics: ClusterMetrics,
    pub internal_topology: OrganizationPattern,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterPurpose {
    Computation,
    Storage,
    Communication,
    Monitoring,
    Learning,
    Mixed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    pub cohesion: f64,
    pub efficiency: f64,
    pub redundancy: f64,
    pub fault_tolerance: f64,
    pub specialization: f64,
}

/// Self-organizing system coordinator
pub struct SelfOrganizingSystem {
    clusters: Arc<RwLock<HashMap<String, NodeCluster>>>,
    emergence_rules: Arc<RwLock<Vec<EmergenceRule>>>,
    organization_pattern: Arc<RwLock<OrganizationPattern>>,
    stigmergy: Arc<Stigmergy>,
    clustering_engine: Arc<ClusteringEngine>,
    metrics: Arc<RwLock<SelfOrganizationMetrics>>,
}

/// Stigmergy for indirect coordination
pub struct Stigmergy {
    pheromones: Arc<RwLock<HashMap<String, Pheromone>>>,
    evaporation_rate: f64,
    diffusion_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pheromone {
    pub type_id: String,
    pub intensity: f64,
    pub position: Vec<f64>,
    pub metadata: HashMap<String, String>,
    pub timestamp: u64,
}

/// Clustering engine for node grouping
pub struct ClusteringEngine {
    algorithms: HashMap<String, Box<dyn ClusteringAlgorithm>>,
    active_algorithm: String,
}

/// Trait for clustering algorithms
#[async_trait]
pub trait ClusteringAlgorithm: Send + Sync {
    async fn cluster(
        &self,
        nodes: &[NodeInfo],
        constraints: &ClusteringConstraints,
    ) -> Vec<NodeCluster>;
    fn name(&self) -> &str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub position: Vec<f64>,
    pub capabilities: HashMap<String, f64>,
    pub connections: Vec<String>,
    pub performance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusteringConstraints {
    pub min_cluster_size: usize,
    pub max_cluster_size: usize,
    pub max_clusters: Option<usize>,
    pub similarity_threshold: f64,
    pub geographic_constraint: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfOrganizationMetrics {
    pub total_clusters: usize,
    pub average_cluster_size: f64,
    pub organization_stability: f64,
    pub emergence_rate: f64,
    pub adaptation_frequency: f64,
    pub stigmergy_effectiveness: f64,
}

impl SelfOrganizingSystem {
    pub fn new(initial_pattern: OrganizationPattern) -> Self {
        Self {
            clusters: Arc::new(RwLock::new(HashMap::new())),
            emergence_rules: Arc::new(RwLock::new(Vec::new())),
            organization_pattern: Arc::new(RwLock::new(initial_pattern)),
            stigmergy: Arc::new(Stigmergy::new(0.1, 0.05)),
            clustering_engine: Arc::new(ClusteringEngine::new()),
            metrics: Arc::new(RwLock::new(SelfOrganizationMetrics {
                total_clusters: 0,
                average_cluster_size: 0.0,
                organization_stability: 1.0,
                emergence_rate: 0.0,
                adaptation_frequency: 0.0,
                stigmergy_effectiveness: 0.5,
            })),
        }
    }

    /// Initialize emergence rules
    pub async fn initialize_rules(&self) {
        let mut rules = self.emergence_rules.write().unwrap();

        // Rule: Form clusters when node density is high
        rules.push(EmergenceRule {
            id: "density_clustering".to_string(),
            condition: EmergenceCondition::ThresholdReached {
                metric: "node_density".to_string(),
                threshold: 0.7,
            },
            action: EmergenceAction::FormCluster {
                min_size: 5,
                max_size: 20,
            },
            priority: 10,
            activation_count: 0,
            success_rate: 0.0,
        });

        // Rule: Elect leader when cluster lacks one
        rules.push(EmergenceRule {
            id: "leader_election".to_string(),
            condition: EmergenceCondition::PatternDetected {
                pattern: "leaderless_cluster".to_string(),
                confidence: 0.9,
            },
            action: EmergenceAction::ElectLeader {
                selection_criteria: LeaderSelectionCriteria {
                    fitness_weight: 0.4,
                    experience_weight: 0.3,
                    centrality_weight: 0.2,
                    stability_weight: 0.1,
                },
            },
            priority: 8,
            activation_count: 0,
            success_rate: 0.0,
        });

        // Rule: Split large clusters
        rules.push(EmergenceRule {
            id: "cluster_fission".to_string(),
            condition: EmergenceCondition::ThresholdReached {
                metric: "cluster_size".to_string(),
                threshold: 50.0,
            },
            action: EmergenceAction::SplitCluster {
                cluster_id: "".to_string(), // Will be filled dynamically
            },
            priority: 7,
            activation_count: 0,
            success_rate: 0.0,
        });

        // Rule: Adapt topology based on performance
        rules.push(EmergenceRule {
            id: "topology_adaptation".to_string(),
            condition: EmergenceCondition::Composite {
                conditions: vec![
                    EmergenceCondition::ThresholdReached {
                        metric: "performance_degradation".to_string(),
                        threshold: 0.3,
                    },
                    EmergenceCondition::TimeElapsed {
                        duration_ms: 60000, // 1 minute
                    },
                ],
                operator: LogicalOperator::And,
            },
            action: EmergenceAction::AdaptTopology {
                new_pattern: OrganizationPattern::Dynamic,
            },
            priority: 6,
            activation_count: 0,
            success_rate: 0.0,
        });
    }

    /// Process self-organization cycle
    pub async fn organize(&self, nodes: Vec<NodeInfo>, context: &OrganizationContext) {
        // Update stigmergy
        self.stigmergy.update().await;

        // Check emergence rules
        let triggered_rules = self.check_emergence_rules(context).await;

        // Execute triggered rules
        for rule in triggered_rules {
            self.execute_emergence_action(&rule, &nodes, context).await;
        }

        // Perform clustering if needed
        if self.should_recluster(context) {
            self.perform_clustering(nodes, context).await;
        }

        // Update metrics
        self.update_metrics().await;
    }

    /// Check which emergence rules should trigger
    async fn check_emergence_rules(&self, context: &OrganizationContext) -> Vec<EmergenceRule> {
        let rules = self.emergence_rules.read().unwrap();
        let mut triggered = Vec::new();

        for rule in rules.iter() {
            if self.evaluate_condition(&rule.condition, context).await {
                triggered.push(rule.clone());
            }
        }

        // Sort by priority
        triggered.sort_by_key(|r| std::cmp::Reverse(r.priority));
        triggered
    }

    /// Evaluate emergence condition
    #[async_recursion]
    async fn evaluate_condition(
        &self,
        condition: &EmergenceCondition,
        context: &OrganizationContext,
    ) -> bool {
        match condition {
            EmergenceCondition::ThresholdReached { metric, threshold } => context
                .metrics
                .get(metric)
                .map_or(false, |&value| value >= *threshold),
            EmergenceCondition::PatternDetected {
                pattern,
                confidence,
            } => context
                .detected_patterns
                .get(pattern)
                .map_or(false, |&conf| conf >= *confidence),
            EmergenceCondition::TimeElapsed { duration_ms } => {
                let elapsed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    - context.last_organization_time;
                elapsed >= *duration_ms
            }
            EmergenceCondition::EventOccurred { event_type, count } => context
                .event_counts
                .get(event_type)
                .map_or(false, |&c| c >= *count),
            EmergenceCondition::Composite {
                conditions,
                operator,
            } => match operator {
                LogicalOperator::And => {
                    for cond in conditions {
                        if !self.evaluate_condition(cond, context).await {
                            return false;
                        }
                    }
                    true
                }
                LogicalOperator::Or => {
                    for cond in conditions {
                        if self.evaluate_condition(cond, context).await {
                            return true;
                        }
                    }
                    false
                }
                LogicalOperator::Not => {
                    conditions.len() == 1 && !self.evaluate_condition(&conditions[0], context).await
                }
            },
        }
    }

    /// Execute emergence action
    async fn execute_emergence_action(
        &self,
        rule: &EmergenceRule,
        nodes: &[NodeInfo],
        context: &OrganizationContext,
    ) {
        let mut rules = self.emergence_rules.write().unwrap();
        if let Some(rule_mut) = rules.iter_mut().find(|r| r.id == rule.id) {
            rule_mut.activation_count += 1;
        }

        match &rule.action {
            EmergenceAction::FormCluster { min_size, max_size } => {
                self.form_cluster(nodes, *min_size, *max_size).await;
            }
            EmergenceAction::SplitCluster { .. } => {
                // Find large clusters and split them; collect IDs first to avoid holding the lock
                let large_cluster_id: Option<String> = {
                    let clusters = self.clusters.read().unwrap();
                    clusters
                        .iter()
                        .find(|(_, cluster)| cluster.members.len() > 50)
                        .map(|(id, _)| id.clone())
                };
                if let Some(cluster_id) = large_cluster_id {
                    self.split_cluster(&cluster_id).await;
                }
            }
            EmergenceAction::ElectLeader { selection_criteria } => {
                self.elect_leaders(selection_criteria).await;
            }
            EmergenceAction::MigrateNodes {
                from_cluster,
                to_cluster,
                count,
            } => {
                self.migrate_nodes(from_cluster, to_cluster, *count).await;
            }
            EmergenceAction::AdaptTopology { new_pattern } => {
                *self.organization_pattern.write().unwrap() = new_pattern.clone();
            }
            EmergenceAction::TriggerEvolution { intensity } => {
                // Trigger evolutionary process with given intensity
                tracing::info!("Triggering evolution with intensity {}", intensity);
            }
        }
    }

    /// Form a new cluster
    async fn form_cluster(&self, nodes: &[NodeInfo], min_size: usize, max_size: usize) {
        let constraints = ClusteringConstraints {
            min_cluster_size: min_size,
            max_cluster_size: max_size,
            max_clusters: None,
            similarity_threshold: 0.6,
            geographic_constraint: false,
        };

        let new_clusters = self.clustering_engine.cluster(nodes, &constraints).await;

        let mut clusters = self.clusters.write().unwrap();
        for cluster in new_clusters {
            clusters.insert(cluster.id.clone(), cluster);
        }
    }

    /// Split a cluster into smaller ones
    async fn split_cluster(&self, cluster_id: &str) {
        let mut clusters = self.clusters.write().unwrap();

        if let Some(cluster) = clusters.remove(cluster_id) {
            let members: Vec<String> = cluster.members.into_iter().collect();
            let mid = members.len() / 2;

            // Create two new clusters
            let cluster1 = NodeCluster {
                id: format!("{}_1", cluster_id),
                members: members[..mid].iter().cloned().collect(),
                leader: None,
                purpose: cluster.purpose.clone(),
                formation_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                performance_metrics: ClusterMetrics::default(),
                internal_topology: OrganizationPattern::Flat,
            };

            let cluster2 = NodeCluster {
                id: format!("{}_2", cluster_id),
                members: members[mid..].iter().cloned().collect(),
                leader: None,
                purpose: cluster.purpose,
                formation_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                performance_metrics: ClusterMetrics::default(),
                internal_topology: OrganizationPattern::Flat,
            };

            clusters.insert(cluster1.id.clone(), cluster1);
            clusters.insert(cluster2.id.clone(), cluster2);
        }
    }

    /// Elect leaders for leaderless clusters
    async fn elect_leaders(&self, criteria: &LeaderSelectionCriteria) {
        let mut clusters = self.clusters.write().unwrap();

        for cluster in clusters.values_mut() {
            if cluster.leader.is_none() && !cluster.members.is_empty() {
                // Simple leader election based on node ID (in real implementation, use criteria)
                cluster.leader = cluster.members.iter().next().cloned();
            }
        }
    }

    /// Migrate nodes between clusters
    async fn migrate_nodes(&self, from_cluster: &str, to_cluster: &str, count: usize) {
        let mut clusters = self.clusters.write().unwrap();

        let nodes_to_migrate: Vec<String> = {
            if let Some(from) = clusters.get(from_cluster) {
                from.members.iter().take(count).cloned().collect()
            } else {
                return;
            }
        };

        // Remove from source cluster
        if let Some(from) = clusters.get_mut(from_cluster) {
            for node in &nodes_to_migrate {
                from.members.remove(node);
            }
        }

        // Add to destination cluster
        if let Some(to) = clusters.get_mut(to_cluster) {
            for node in nodes_to_migrate {
                to.members.insert(node);
            }
        }
    }

    /// Check if reclustering is needed
    fn should_recluster(&self, context: &OrganizationContext) -> bool {
        context
            .metrics
            .get("clustering_quality")
            .map_or(false, |&quality| quality < 0.5)
            || context.significant_change_detected
    }

    /// Perform clustering
    async fn perform_clustering(&self, nodes: Vec<NodeInfo>, context: &OrganizationContext) {
        let constraints = ClusteringConstraints {
            min_cluster_size: 3,
            max_cluster_size: 30,
            max_clusters: Some(context.desired_clusters),
            similarity_threshold: 0.7,
            geographic_constraint: context.use_geographic_clustering,
        };

        let new_clusters = self.clustering_engine.cluster(&nodes, &constraints).await;

        let mut clusters = self.clusters.write().unwrap();
        clusters.clear();
        for cluster in new_clusters {
            clusters.insert(cluster.id.clone(), cluster);
        }
    }

    /// Update self-organization metrics
    async fn update_metrics(&self) {
        let clusters = self.clusters.read().unwrap();
        let mut metrics = self.metrics.write().unwrap();

        metrics.total_clusters = clusters.len();

        if !clusters.is_empty() {
            let total_members: usize = clusters.values().map(|c| c.members.len()).sum();
            metrics.average_cluster_size = total_members as f64 / clusters.len() as f64;
        }

        // Calculate organization stability (simplified)
        metrics.organization_stability = 0.9; // Placeholder

        // Calculate emergence rate
        let rules = self.emergence_rules.read().unwrap();
        let total_activations: u64 = rules.iter().map(|r| r.activation_count).sum();
        metrics.emergence_rate = total_activations as f64 / rules.len().max(1) as f64;

        // Calculate stigmergy effectiveness
        metrics.stigmergy_effectiveness = self.stigmergy.get_effectiveness().await;
    }

    /// Get current organization pattern
    pub async fn get_pattern(&self) -> OrganizationPattern {
        self.organization_pattern.read().unwrap().clone()
    }

    /// Get clusters
    pub async fn get_clusters(&self) -> Vec<NodeCluster> {
        self.clusters.read().unwrap().values().cloned().collect()
    }

    /// Get metrics
    pub async fn get_metrics(&self) -> SelfOrganizationMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Deposit pheromone for stigmergic coordination
    pub async fn deposit_pheromone(&self, pheromone: Pheromone) {
        self.stigmergy.deposit(pheromone).await;
    }

    /// Sense pheromones at a position
    pub async fn sense_pheromones(&self, position: &[f64], radius: f64) -> Vec<Pheromone> {
        self.stigmergy.sense(position, radius).await
    }
}

/// Organization context for self-organizing decisions
#[derive(Debug, Clone)]
pub struct OrganizationContext {
    pub metrics: HashMap<String, f64>,
    pub detected_patterns: HashMap<String, f64>,
    pub event_counts: HashMap<String, u32>,
    pub last_organization_time: u64,
    pub significant_change_detected: bool,
    pub desired_clusters: usize,
    pub use_geographic_clustering: bool,
}

impl Stigmergy {
    pub fn new(evaporation_rate: f64, diffusion_rate: f64) -> Self {
        Self {
            pheromones: Arc::new(RwLock::new(HashMap::new())),
            evaporation_rate,
            diffusion_rate,
        }
    }

    pub async fn deposit(&self, pheromone: Pheromone) {
        let mut pheromones = self.pheromones.write().unwrap();
        pheromones.insert(pheromone.type_id.clone(), pheromone);
    }

    pub async fn sense(&self, position: &[f64], radius: f64) -> Vec<Pheromone> {
        let pheromones = self.pheromones.read().unwrap();

        pheromones
            .values()
            .filter(|p| {
                let distance = p
                    .position
                    .iter()
                    .zip(position.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>()
                    .sqrt();
                distance <= radius
            })
            .cloned()
            .collect()
    }

    pub async fn update(&self) {
        let mut pheromones = self.pheromones.write().unwrap();
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Evaporate pheromones
        pheromones.retain(|_, p| {
            let age = current_time - p.timestamp;
            let evaporation_factor = (-self.evaporation_rate * age as f64).exp();
            p.intensity * evaporation_factor > 0.01
        });

        // Update intensities
        for pheromone in pheromones.values_mut() {
            let age = current_time - pheromone.timestamp;
            let evaporation_factor = (-self.evaporation_rate * age as f64).exp();
            pheromone.intensity *= evaporation_factor;
        }
    }

    pub async fn get_effectiveness(&self) -> f64 {
        let pheromones = self.pheromones.read().unwrap();
        if pheromones.is_empty() {
            return 0.0;
        }

        let total_intensity: f64 = pheromones.values().map(|p| p.intensity).sum();
        (total_intensity / pheromones.len() as f64).min(1.0)
    }
}

impl ClusteringEngine {
    pub fn new() -> Self {
        let mut engine = Self {
            algorithms: HashMap::new(),
            active_algorithm: "k-means".to_string(),
        };

        // Register default algorithms
        engine
            .algorithms
            .insert("k-means".to_string(), Box::new(KMeansAlgorithm));
        engine
            .algorithms
            .insert("hierarchical".to_string(), Box::new(HierarchicalAlgorithm));

        engine
    }

    pub async fn cluster(
        &self,
        nodes: &[NodeInfo],
        constraints: &ClusteringConstraints,
    ) -> Vec<NodeCluster> {
        if let Some(algorithm) = self.algorithms.get(&self.active_algorithm) {
            algorithm.cluster(nodes, constraints).await
        } else {
            Vec::new()
        }
    }

    pub fn set_algorithm(&mut self, name: String) {
        if self.algorithms.contains_key(&name) {
            self.active_algorithm = name;
        }
    }

    pub fn register_algorithm(&mut self, name: String, algorithm: Box<dyn ClusteringAlgorithm>) {
        self.algorithms.insert(name, algorithm);
    }
}

/// Simple K-means clustering implementation
struct KMeansAlgorithm;

#[async_trait]
impl ClusteringAlgorithm for KMeansAlgorithm {
    async fn cluster(
        &self,
        nodes: &[NodeInfo],
        constraints: &ClusteringConstraints,
    ) -> Vec<NodeCluster> {
        // Simplified k-means implementation
        let k = constraints
            .max_clusters
            .unwrap_or(nodes.len() / constraints.min_cluster_size);
        let k = k.min(nodes.len()).max(1);

        let mut clusters = Vec::new();
        for i in 0..k {
            let cluster = NodeCluster {
                id: format!("cluster_{}", i),
                members: HashSet::new(),
                leader: None,
                purpose: ClusterPurpose::Mixed,
                formation_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                performance_metrics: ClusterMetrics::default(),
                internal_topology: OrganizationPattern::Flat,
            };
            clusters.push(cluster);
        }

        // Assign nodes to clusters (simplified)
        for (i, node) in nodes.iter().enumerate() {
            let cluster_idx = i % k;
            clusters[cluster_idx].members.insert(node.id.clone());
        }

        // Remove empty clusters
        clusters.retain(|c| c.members.len() >= constraints.min_cluster_size);

        clusters
    }

    fn name(&self) -> &str {
        "k-means"
    }
}

/// Hierarchical clustering implementation
struct HierarchicalAlgorithm;

#[async_trait]
impl ClusteringAlgorithm for HierarchicalAlgorithm {
    async fn cluster(
        &self,
        nodes: &[NodeInfo],
        constraints: &ClusteringConstraints,
    ) -> Vec<NodeCluster> {
        // Simplified hierarchical clustering
        let mut clusters = Vec::new();
        let mut current_cluster = NodeCluster {
            id: format!("hierarchical_0"),
            members: HashSet::new(),
            leader: None,
            purpose: ClusterPurpose::Mixed,
            formation_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            performance_metrics: ClusterMetrics::default(),
            internal_topology: OrganizationPattern::Hierarchical,
        };

        for node in nodes {
            current_cluster.members.insert(node.id.clone());

            if current_cluster.members.len() >= constraints.max_cluster_size {
                clusters.push(current_cluster.clone());
                current_cluster = NodeCluster {
                    id: format!("hierarchical_{}", clusters.len()),
                    members: HashSet::new(),
                    leader: None,
                    purpose: ClusterPurpose::Mixed,
                    formation_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    performance_metrics: ClusterMetrics::default(),
                    internal_topology: OrganizationPattern::Hierarchical,
                };
            }
        }

        if current_cluster.members.len() >= constraints.min_cluster_size {
            clusters.push(current_cluster);
        }

        clusters
    }

    fn name(&self) -> &str {
        "hierarchical"
    }
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        Self {
            cohesion: 0.5,
            efficiency: 0.5,
            redundancy: 0.3,
            fault_tolerance: 0.5,
            specialization: 0.0,
        }
    }
}

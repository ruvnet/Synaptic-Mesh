//! Neural mesh topology and connection management

use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{cognition::CognitionResult, NeuralMeshError, Result};

/// Neural mesh that manages agent connectivity
#[derive(Debug)]
pub struct NeuralMesh {
    topology: MeshTopology,
    nodes: Arc<RwLock<HashMap<Uuid, MeshNode>>>,
    connections: Arc<RwLock<HashMap<Uuid, HashSet<Connection>>>>,
    stats: Arc<RwLock<MeshStats>>,
}

impl NeuralMesh {
    /// Create a new neural mesh with specified topology
    pub async fn new(topology: MeshTopology) -> Result<Self> {
        Ok(Self {
            topology,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(MeshStats::default())),
        })
    }

    /// Start the mesh
    pub async fn start(&self) -> Result<()> {
        tracing::info!("Starting neural mesh with topology: {:?}", self.topology);
        Ok(())
    }

    /// Stop the mesh
    pub async fn stop(&self) -> Result<()> {
        tracing::info!("Stopping neural mesh");
        Ok(())
    }

    /// Add an agent node to the mesh
    pub async fn add_agent(&self, agent_id: Uuid, node: MeshNode) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let mut connections = self.connections.write().await;

        // Add node
        nodes.insert(agent_id, node);

        // Create connections based on topology
        let new_connections = self.create_connections(agent_id, &nodes).await?;
        connections.insert(agent_id, new_connections);

        // Update stats
        let mut stats = self.stats.write().await;
        stats.connections = connections.values().map(|c| c.len()).sum();

        tracing::info!("Added agent {} to mesh", agent_id);
        Ok(())
    }

    /// Remove an agent from the mesh
    pub async fn remove_agent(&self, agent_id: Uuid) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let mut connections = self.connections.write().await;

        // Remove node
        nodes.remove(&agent_id);

        // Remove connections
        connections.remove(&agent_id);

        // Remove references from other nodes
        for (_, node_connections) in connections.iter_mut() {
            node_connections.retain(|c| c.to != agent_id);
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.connections = connections.values().map(|c| c.len()).sum();

        tracing::info!("Removed agent {} from mesh", agent_id);
        Ok(())
    }

    /// Update connections based on cognition results
    pub async fn update_connections(&self, result: &CognitionResult) -> Result<()> {
        let mut connections = self.connections.write().await;

        // Strengthen connections between agents that contributed
        for (agent1, contrib1) in &result.agent_contributions {
            for (agent2, contrib2) in &result.agent_contributions {
                if agent1 != agent2 {
                    if let Some(agent_connections) = connections.get_mut(agent1) {
                        // HashSet doesn't support iter_mut; remove + reinsert to update strength
                        if let Some(existing) =
                            agent_connections.iter().find(|c| &c.to == agent2).cloned()
                        {
                            let strength_delta = contrib1 * contrib2 * 0.1;
                            let updated = Connection {
                                strength: (existing.strength + strength_delta).min(1.0),
                                ..existing
                            };
                            agent_connections.remove(&updated);
                            agent_connections.insert(updated);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Get mesh statistics
    pub async fn get_stats(&self) -> MeshStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Get neighbors of a node
    pub async fn get_neighbors(&self, agent_id: Uuid) -> Result<Vec<Uuid>> {
        let connections = self.connections.read().await;

        if let Some(node_connections) = connections.get(&agent_id) {
            Ok(node_connections.iter().map(|c| c.to).collect())
        } else {
            Err(NeuralMeshError::NotFound(format!(
                "Agent {} not found in mesh",
                agent_id
            )))
        }
    }

    /// Get shortest path between two nodes
    pub async fn shortest_path(&self, from: Uuid, to: Uuid) -> Result<Vec<Uuid>> {
        let connections = self.connections.read().await;

        // Dijkstra's algorithm
        let mut distances: HashMap<Uuid, f64> = HashMap::new();
        let mut previous: HashMap<Uuid, Option<Uuid>> = HashMap::new();
        let mut unvisited: HashSet<Uuid> = connections.keys().cloned().collect();

        // Initialize distances
        for &node in connections.keys() {
            distances.insert(node, f64::INFINITY);
            previous.insert(node, None);
        }
        distances.insert(from, 0.0);

        while !unvisited.is_empty() {
            // Find unvisited node with minimum distance
            let current = unvisited
                .iter()
                .min_by(|&&a, &&b| distances[&a].partial_cmp(&distances[&b]).unwrap())
                .cloned();

            if let Some(current) = current {
                if current == to {
                    break;
                }

                unvisited.remove(&current);

                // Update distances to neighbors
                if let Some(neighbors) = connections.get(&current) {
                    for connection in neighbors {
                        if unvisited.contains(&connection.to) {
                            let alt = distances[&current] + (1.0 / connection.strength);
                            if alt < distances[&connection.to] {
                                distances.insert(connection.to, alt);
                                previous.insert(connection.to, Some(current));
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        // Reconstruct path
        let mut path = Vec::new();
        let mut current = Some(to);

        while let Some(node) = current {
            path.push(node);
            current = previous.get(&node).and_then(|&p| p);
            if current == Some(from) {
                path.push(from);
                break;
            }
        }

        if path.last() == Some(&from) {
            path.reverse();
            Ok(path)
        } else {
            Err(NeuralMeshError::NotFound(format!(
                "No path from {} to {}",
                from, to
            )))
        }
    }

    /// Create connections for a new node based on topology
    async fn create_connections(
        &self,
        agent_id: Uuid,
        nodes: &HashMap<Uuid, MeshNode>,
    ) -> Result<HashSet<Connection>> {
        let mut connections = HashSet::new();
        let other_nodes: Vec<Uuid> = nodes
            .keys()
            .filter(|&&id| id != agent_id)
            .cloned()
            .collect();

        match &self.topology {
            MeshTopology::FullyConnected => {
                // Connect to all other nodes
                for &other_id in &other_nodes {
                    connections.insert(Connection {
                        to: other_id,
                        strength: ConnectionStrength::default_strength(),
                        connection_type: ConnectionType::Direct,
                    });
                }
            }
            MeshTopology::SmallWorld { k, p } => {
                // Small-world network (Watts-Strogatz model)
                let mut rng = thread_rng();
                let n = other_nodes.len();

                if n > 0 {
                    // Create ring lattice with k neighbors
                    let k_actual = (*k).min(n);
                    for i in 1..=k_actual / 2 {
                        let idx1 = i % n;
                        let idx2 = (n - i) % n;

                        connections.insert(Connection {
                            to: other_nodes[idx1],
                            strength: ConnectionStrength::default_strength(),
                            connection_type: ConnectionType::Direct,
                        });

                        if idx1 != idx2 {
                            connections.insert(Connection {
                                to: other_nodes[idx2],
                                strength: ConnectionStrength::default_strength(),
                                connection_type: ConnectionType::Direct,
                            });
                        }
                    }

                    // Rewire with probability p
                    let mut rewired_connections = Vec::new();
                    for connection in &connections {
                        if rng.gen::<f64>() < *p {
                            // Rewire to random node
                            let new_target = other_nodes.choose(&mut rng).cloned();
                            if let Some(new_to) = new_target {
                                if new_to != connection.to && new_to != agent_id {
                                    rewired_connections.push(Connection {
                                        to: new_to,
                                        strength: connection.strength,
                                        connection_type: ConnectionType::Rewired,
                                    });
                                }
                            }
                        }
                    }

                    // Apply rewiring
                    for rewired in rewired_connections {
                        connections.insert(rewired);
                    }
                }
            }
            MeshTopology::ScaleFree { m } => {
                // Scale-free network (Barabási-Albert model)
                let mut rng = thread_rng();

                if other_nodes.len() >= *m {
                    // Preferential attachment
                    let node_degrees: Vec<(Uuid, usize)> = other_nodes
                        .iter()
                        .map(|&id| {
                            let degree = nodes.get(&id).map(|n| n.connection_count).unwrap_or(1);
                            (id, degree)
                        })
                        .collect();

                    let total_degree: usize = node_degrees.iter().map(|(_, d)| d).sum();

                    // Connect to m nodes with probability proportional to degree
                    let mut connected = HashSet::new();
                    while connected.len() < *m {
                        let r = rng.gen_range(0..total_degree);
                        let mut cumulative = 0;

                        for (id, degree) in &node_degrees {
                            cumulative += degree;
                            if cumulative > r && !connected.contains(id) {
                                connected.insert(*id);
                                connections.insert(Connection {
                                    to: *id,
                                    strength: ConnectionStrength::default_strength(),
                                    connection_type: ConnectionType::Preferential,
                                });
                                break;
                            }
                        }
                    }
                }
            }
            MeshTopology::Hierarchical {
                levels,
                branching_factor,
            } => {
                // Hierarchical network
                if let Some(node) = nodes.get(&agent_id) {
                    let level = node.hierarchy_level.unwrap_or(0);

                    // Connect to parent (if not at top level)
                    if level > 0 {
                        // Find a node at level - 1
                        for (&other_id, other_node) in nodes.iter() {
                            if other_node.hierarchy_level == Some(level - 1) {
                                connections.insert(Connection {
                                    to: other_id,
                                    strength: ConnectionStrength::default_strength(),
                                    connection_type: ConnectionType::Hierarchical,
                                });
                                break;
                            }
                        }
                    }

                    // Connect to children (if not at bottom level)
                    if level < levels - 1 {
                        let mut children_count = 0;
                        for (&other_id, other_node) in nodes.iter() {
                            if other_node.hierarchy_level == Some(level + 1)
                                && children_count < *branching_factor
                            {
                                connections.insert(Connection {
                                    to: other_id,
                                    strength: ConnectionStrength::default_strength(),
                                    connection_type: ConnectionType::Hierarchical,
                                });
                                children_count += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(connections)
    }
}

/// Topology types for the neural mesh
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MeshTopology {
    /// All nodes connected to all other nodes
    FullyConnected,

    /// Small-world network (high clustering, low path length)
    SmallWorld {
        k: usize, // Average degree
        p: f64,   // Rewiring probability
    },

    /// Scale-free network (power-law degree distribution)
    ScaleFree {
        m: usize, // Number of connections for new nodes
    },

    /// Hierarchical structure
    Hierarchical {
        levels: usize,
        branching_factor: usize,
    },
}

/// Node in the neural mesh
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshNode {
    pub id: Uuid,
    pub capabilities: Vec<String>,
    pub capacity: f64,
    pub current_load: f64,
    pub connection_count: usize,
    pub hierarchy_level: Option<usize>,
}

impl MeshNode {
    /// Create a new mesh node
    pub fn new(id: Uuid, capabilities: Vec<String>) -> Self {
        Self {
            id,
            capabilities,
            capacity: 1.0,
            current_load: 0.0,
            connection_count: 0,
            hierarchy_level: None,
        }
    }
}

/// Connection between nodes in the mesh
#[derive(Debug, Clone, PartialEq)]
pub struct Connection {
    pub to: Uuid,
    pub strength: f64,
    pub connection_type: ConnectionType,
}

impl Eq for Connection {}

impl std::hash::Hash for Connection {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.to.hash(state);
    }
}

/// Types of connections
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionType {
    Direct,
    Rewired,
    Preferential,
    Hierarchical,
}

/// Connection strength utilities
#[derive(Debug, Clone, Copy)]
pub struct ConnectionStrength;

impl ConnectionStrength {
    pub fn default_strength() -> f64 {
        0.5
    }

    pub fn update(current: f64, delta: f64) -> f64 {
        (current + delta).clamp(0.0, 1.0)
    }
}

/// Statistics about the mesh
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MeshStats {
    pub connections: usize,
    pub average_degree: f64,
    pub clustering_coefficient: f64,
    pub average_path_length: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mesh_creation() {
        let topology = MeshTopology::SmallWorld { k: 4, p: 0.1 };
        let mesh = NeuralMesh::new(topology).await;
        assert!(mesh.is_ok());
    }

    #[tokio::test]
    async fn test_add_remove_agent() {
        let topology = MeshTopology::FullyConnected;
        let mesh = NeuralMesh::new(topology).await.unwrap();

        let agent_id = Uuid::new_v4();
        let node = MeshNode::new(agent_id, vec!["test".to_string()]);

        assert!(mesh.add_agent(agent_id, node).await.is_ok());
        assert!(mesh.remove_agent(agent_id).await.is_ok());
    }

    #[test]
    fn test_connection_strength() {
        let strength = ConnectionStrength::default_strength();
        assert_eq!(strength, 0.5);

        let updated = ConnectionStrength::update(0.7, 0.5);
        assert_eq!(updated, 1.0); // Clamped to max

        let decreased = ConnectionStrength::update(0.3, -0.5);
        assert_eq!(decreased, 0.0); // Clamped to min
    }
}

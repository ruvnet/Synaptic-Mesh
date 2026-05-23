//! DAG (Directed Acyclic Graph) data structures and operations
//!
//! Implements the core DAG functionality including nodes, edges, validation,
//! and network-wide DAG operations for the QuDAG system.

use blake3::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{PostQuantumSignature, QuDAGError, Result};

/// A node in the DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGNode {
    pub id: Uuid,
    pub data: Vec<u8>,
    pub parents: Vec<Uuid>,
    pub timestamp: u64,
    pub signature: Option<PostQuantumSignature>,
    pub hash: Vec<u8>,
    pub nonce: u64,
}

impl DAGNode {
    /// Create a new DAG node
    pub fn new(id: Uuid, data: Vec<u8>, parents: Vec<Uuid>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut node = Self {
            id,
            data,
            parents,
            timestamp,
            signature: None,
            hash: Vec::new(),
            nonce: 0,
        };

        node.hash = node.compute_hash();
        node
    }

    /// Create a genesis node (no parents)
    pub fn genesis(data: Vec<u8>) -> Self {
        Self::new(Uuid::new_v4(), data, Vec::new())
    }

    /// Compute the hash of this node
    pub fn compute_hash(&self) -> Vec<u8> {
        let mut hasher = Hasher::new();
        hasher.update(self.id.as_bytes());
        hasher.update(&self.data);
        hasher.update(&self.timestamp.to_le_bytes());
        hasher.update(&self.nonce.to_le_bytes());

        for parent in &self.parents {
            hasher.update(parent.as_bytes());
        }

        hasher.finalize().as_bytes().to_vec()
    }

    /// Verify the node's hash
    pub fn verify_hash(&self) -> bool {
        self.hash == self.compute_hash()
    }

    /// Add a signature to this node
    pub fn sign(&mut self, signature: PostQuantumSignature) {
        self.signature = Some(signature);
    }

    /// Check if this is a genesis node
    pub fn is_genesis(&self) -> bool {
        self.parents.is_empty()
    }

    /// Get the node's data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get the node's parents
    pub fn parents(&self) -> &[Uuid] {
        &self.parents
    }

    /// Get the node's signature
    pub fn signature(&self) -> Option<&PostQuantumSignature> {
        self.signature.as_ref()
    }

    /// Calculate work done (for PoW if needed)
    pub fn work_done(&self) -> u32 {
        self.hash
            .iter()
            .take(4)
            .fold(0u32, |acc, &byte| acc.wrapping_add(byte as u32))
    }
}

/// An edge in the DAG representing parent-child relationship
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGEdge {
    pub from: Uuid, // parent
    pub to: Uuid,   // child
    pub weight: f64,
}

impl DAGEdge {
    pub fn new(from: Uuid, to: Uuid) -> Self {
        Self {
            from,
            to,
            weight: 1.0,
        }
    }

    pub fn with_weight(from: Uuid, to: Uuid, weight: f64) -> Self {
        Self { from, to, weight }
    }
}

/// DAG network representation
#[derive(Debug)]
pub struct DAGNetwork {
    nodes: Arc<RwLock<HashMap<Uuid, DAGNode>>>,
    edges: Arc<RwLock<HashMap<Uuid, Vec<Uuid>>>>, // parent_id -> vec[child_ids]
    reverse_edges: Arc<RwLock<HashMap<Uuid, Vec<Uuid>>>>, // child_id -> vec[parent_ids]
    tips: Arc<RwLock<HashSet<Uuid>>>,             // nodes with no children
    validator: Arc<DAGValidation>,
}

impl DAGNetwork {
    /// Create a new empty DAG network
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
            reverse_edges: Arc::new(RwLock::new(HashMap::new())),
            tips: Arc::new(RwLock::new(HashSet::new())),
            validator: Arc::new(DAGValidation::new()),
        }
    }

    /// Add a node to the DAG
    pub async fn add_node(&self, node: DAGNode) -> Result<()> {
        // Validate the node
        self.validator.validate_node(&node, self).await?;

        let node_id = node.id;
        let parents = node.parents.clone();

        // Add node
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(node_id, node);
        }

        // Update edges
        {
            let mut edges = self.edges.write().await;
            let mut reverse_edges = self.reverse_edges.write().await;
            let mut tips = self.tips.write().await;

            for parent_id in &parents {
                // Add edge from parent to child
                edges
                    .entry(*parent_id)
                    .or_insert_with(Vec::new)
                    .push(node_id);
                reverse_edges
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(*parent_id);

                // Parent is no longer a tip
                tips.remove(parent_id);
            }

            // New node is a tip (unless it gets children later)
            tips.insert(node_id);
        }

        tracing::debug!(
            "Added node {} to DAG with {} parents",
            node_id,
            parents.len()
        );
        Ok(())
    }

    /// Get a node by ID
    pub async fn get_node(&self, id: Uuid) -> Option<DAGNode> {
        let nodes = self.nodes.read().await;
        nodes.get(&id).cloned()
    }

    /// Get all nodes
    pub async fn get_all_nodes(&self) -> Vec<DAGNode> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Get children of a node
    pub async fn get_children(&self, id: Uuid) -> Vec<Uuid> {
        let edges = self.edges.read().await;
        edges.get(&id).cloned().unwrap_or_default()
    }

    /// Get parents of a node
    pub async fn get_parents(&self, id: Uuid) -> Vec<Uuid> {
        let reverse_edges = self.reverse_edges.read().await;
        reverse_edges.get(&id).cloned().unwrap_or_default()
    }

    /// Get current tips (nodes with no children)
    pub async fn get_tips(&self) -> Vec<Uuid> {
        let tips = self.tips.read().await;
        tips.iter().cloned().collect()
    }

    /// Get nodes in topological order
    pub async fn get_topological_order(&self) -> Result<Vec<Uuid>> {
        let nodes = self.nodes.read().await;
        let reverse_edges = self.reverse_edges.read().await;

        let mut in_degree: HashMap<Uuid, usize> = HashMap::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        // Calculate in-degrees
        for node_id in nodes.keys() {
            let degree = reverse_edges
                .get(node_id)
                .map_or(0, |parents| parents.len());
            in_degree.insert(*node_id, degree);

            if degree == 0 {
                queue.push_back(*node_id);
            }
        }

        // Kahn's algorithm
        while let Some(node_id) = queue.pop_front() {
            result.push(node_id);

            let edges = self.edges.read().await;
            if let Some(children) = edges.get(&node_id) {
                for child_id in children {
                    if let Some(degree) = in_degree.get_mut(child_id) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(*child_id);
                        }
                    }
                }
            }
        }

        if result.len() != nodes.len() {
            return Err(QuDAGError::ValidationError(
                "DAG contains cycles".to_string(),
            ));
        }

        Ok(result)
    }

    /// Get ancestors of a node (all nodes reachable going backwards)
    pub async fn get_ancestors(&self, id: Uuid) -> Result<HashSet<Uuid>> {
        let mut ancestors = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(id);

        while let Some(current_id) = queue.pop_front() {
            if ancestors.contains(&current_id) {
                continue;
            }

            ancestors.insert(current_id);
            let parents = self.get_parents(current_id).await;
            for parent_id in parents {
                queue.push_back(parent_id);
            }
        }

        // Remove the starting node itself
        ancestors.remove(&id);
        Ok(ancestors)
    }

    /// Get descendants of a node (all nodes reachable going forwards)
    pub async fn get_descendants(&self, id: Uuid) -> Result<HashSet<Uuid>> {
        let mut descendants = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(id);

        while let Some(current_id) = queue.pop_front() {
            if descendants.contains(&current_id) {
                continue;
            }

            descendants.insert(current_id);
            let children = self.get_children(current_id).await;
            for child_id in children {
                queue.push_back(child_id);
            }
        }

        // Remove the starting node itself
        descendants.remove(&id);
        Ok(descendants)
    }

    /// Check if the DAG is valid (acyclic)
    pub async fn is_valid(&self) -> bool {
        self.get_topological_order().await.is_ok()
    }

    /// Get DAG statistics
    pub async fn get_stats(&self) -> DAGStats {
        let nodes = self.nodes.read().await;
        let tips = self.tips.read().await;

        DAGStats {
            node_count: nodes.len(),
            tip_count: tips.len(),
            genesis_count: nodes.values().filter(|n| n.is_genesis()).count(),
        }
    }
}

impl Default for DAGNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// DAG validation logic
#[derive(Debug)]
pub struct DAGValidation {
    max_parents: usize,
    min_timestamp_diff: u64,
}

impl DAGValidation {
    pub fn new() -> Self {
        Self {
            max_parents: 10,
            min_timestamp_diff: 1, // 1 second minimum between nodes
        }
    }

    /// Validate a DAG node
    pub async fn validate_node(&self, node: &DAGNode, dag: &DAGNetwork) -> Result<()> {
        // Basic validation
        if !node.verify_hash() {
            return Err(QuDAGError::ValidationError("Invalid node hash".to_string()));
        }

        // Check parent count
        if node.parents.len() > self.max_parents {
            return Err(QuDAGError::ValidationError(format!(
                "Too many parents: {} > {}",
                node.parents.len(),
                self.max_parents
            )));
        }

        // Validate parents exist
        for parent_id in &node.parents {
            if dag.get_node(*parent_id).await.is_none() {
                return Err(QuDAGError::ValidationError(format!(
                    "Parent {} does not exist",
                    parent_id
                )));
            }
        }

        // Check for timestamp ordering
        for parent_id in &node.parents {
            if let Some(parent) = dag.get_node(*parent_id).await {
                if node.timestamp <= parent.timestamp + self.min_timestamp_diff {
                    return Err(QuDAGError::ValidationError(
                        "Node timestamp must be after parent timestamps".to_string(),
                    ));
                }
            }
        }

        // Check for duplicate nodes
        if dag.get_node(node.id).await.is_some() {
            return Err(QuDAGError::ValidationError(format!(
                "Node {} already exists",
                node.id
            )));
        }

        Ok(())
    }

    /// Validate the entire DAG structure
    pub async fn validate_dag(&self, dag: &DAGNetwork) -> Result<()> {
        // Check for cycles
        if !dag.is_valid().await {
            return Err(QuDAGError::ValidationError(
                "DAG contains cycles".to_string(),
            ));
        }

        // Validate each node
        let nodes = dag.get_all_nodes().await;
        for node in &nodes {
            self.validate_node(node, dag).await?;
        }

        Ok(())
    }
}

impl Default for DAGValidation {
    fn default() -> Self {
        Self::new()
    }
}

/// DAG network statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGStats {
    pub node_count: usize,
    pub tip_count: usize,
    pub genesis_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dag_node_creation() {
        let data = b"test data".to_vec();
        let node = DAGNode::new(Uuid::new_v4(), data, Vec::new());

        assert!(node.verify_hash());
        assert!(node.is_genesis());
    }

    #[tokio::test]
    async fn test_dag_network() {
        let mut dag = DAGNetwork::new();

        // Add genesis node
        let genesis_data = b"genesis".to_vec();
        let genesis = DAGNode::genesis(genesis_data);
        let genesis_id = genesis.id;

        assert!(dag.add_node(genesis).await.is_ok());

        // Add child node
        let child_data = b"child".to_vec();
        let child = DAGNode::new(Uuid::new_v4(), child_data, vec![genesis_id]);
        let child_id = child.id;

        assert!(dag.add_node(child).await.is_ok());

        // Check relationships
        let children = dag.get_children(genesis_id).await;
        assert_eq!(children, vec![child_id]);

        let parents = dag.get_parents(child_id).await;
        assert_eq!(parents, vec![genesis_id]);

        // Check tips
        let tips = dag.get_tips().await;
        assert_eq!(tips, vec![child_id]);
    }

    #[tokio::test]
    async fn test_topological_ordering() {
        let mut dag = DAGNetwork::new();

        // Create a simple linear DAG: A -> B -> C
        let node_a = DAGNode::genesis(b"A".to_vec());
        let id_a = node_a.id;
        dag.add_node(node_a).await.unwrap();

        let node_b = DAGNode::new(Uuid::new_v4(), b"B".to_vec(), vec![id_a]);
        let id_b = node_b.id;
        dag.add_node(node_b).await.unwrap();

        let node_c = DAGNode::new(Uuid::new_v4(), b"C".to_vec(), vec![id_b]);
        let id_c = node_c.id;
        dag.add_node(node_c).await.unwrap();

        let topo_order = dag.get_topological_order().await.unwrap();

        // A should come before B, B before C
        let pos_a = topo_order.iter().position(|&x| x == id_a).unwrap();
        let pos_b = topo_order.iter().position(|&x| x == id_b).unwrap();
        let pos_c = topo_order.iter().position(|&x| x == id_c).unwrap();

        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[tokio::test]
    async fn test_dag_validation() {
        let dag = DAGNetwork::new();
        let validator = DAGValidation::new();

        // Test valid genesis node
        let genesis = DAGNode::genesis(b"genesis".to_vec());
        assert!(validator.validate_node(&genesis, &dag).await.is_ok());
    }

    #[test]
    fn test_node_hash_verification() {
        let mut node = DAGNode::genesis(b"test".to_vec());
        assert!(node.verify_hash());

        // Tamper with data
        node.data.push(42);
        assert!(!node.verify_hash());

        // Recompute hash
        node.hash = node.compute_hash();
        assert!(node.verify_hash());
    }
}

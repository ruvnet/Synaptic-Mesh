//! Storage layer for QuDAG DAG data and metadata

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{DAGNode, QuDAGError, Result};

/// Trait for DAG storage backends
#[async_trait]
pub trait DAGStorage: Send + Sync + std::fmt::Debug {
    /// Add a node to storage
    async fn add_node(&self, node: DAGNode) -> Result<()>;

    /// Get a node by ID
    async fn get_node(&self, id: Uuid) -> Result<Option<DAGNode>>;

    /// Remove a node from storage
    async fn remove_node(&self, id: Uuid) -> Result<bool>;

    /// Get all nodes
    async fn get_all_nodes(&self) -> Result<Vec<DAGNode>>;

    /// Get current tips (nodes with no children)
    async fn get_tips(&self) -> Result<Vec<Uuid>>;

    /// Get children of a node
    async fn get_children(&self, id: Uuid) -> Result<Vec<Uuid>>;

    /// Get parents of a node
    async fn get_parents(&self, id: Uuid) -> Result<Vec<Uuid>>;

    /// Check if a node exists
    async fn contains_node(&self, id: Uuid) -> Result<bool>;

    /// Get storage statistics
    async fn get_stats(&self) -> Result<StorageStats>;

    /// Compact storage (remove orphaned data)
    async fn compact(&self) -> Result<()>;

    /// Backup storage to a file
    async fn backup(&self, path: &std::path::Path) -> Result<()>;

    /// Restore storage from a file
    async fn restore(&self, path: &std::path::Path) -> Result<()>;
}

/// In-memory storage implementation
#[derive(Debug)]
pub struct MemoryStorage {
    nodes: Arc<RwLock<HashMap<Uuid, DAGNode>>>,
    edges: Arc<RwLock<HashMap<Uuid, Vec<Uuid>>>>, // parent -> children
    reverse_edges: Arc<RwLock<HashMap<Uuid, Vec<Uuid>>>>, // child -> parents
    tips: Arc<RwLock<HashSet<Uuid>>>,
}

impl MemoryStorage {
    /// Create a new memory storage
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
            reverse_edges: Arc::new(RwLock::new(HashMap::new())),
            tips: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Get node count
    pub async fn node_count(&self) -> usize {
        self.nodes.read().await.len()
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DAGStorage for MemoryStorage {
    async fn add_node(&self, node: DAGNode) -> Result<()> {
        let node_id = node.id;
        let parents = node.parents.clone();

        // Add node
        {
            let mut nodes = self.nodes.write().await;
            if nodes.contains_key(&node_id) {
                return Err(QuDAGError::StorageError(format!(
                    "Node {} already exists",
                    node_id
                )));
            }
            nodes.insert(node_id, node);
        }

        // Update edges and tips
        {
            let mut edges = self.edges.write().await;
            let mut reverse_edges = self.reverse_edges.write().await;
            let mut tips = self.tips.write().await;

            // Add reverse edges (child -> parents)
            if !parents.is_empty() {
                reverse_edges.insert(node_id, parents.clone());
            }

            // Add forward edges (parent -> children) and update tips
            for parent_id in parents {
                edges
                    .entry(parent_id)
                    .or_insert_with(Vec::new)
                    .push(node_id);
                tips.remove(&parent_id); // Parent is no longer a tip
            }

            // New node is a tip (unless it gets children later)
            tips.insert(node_id);
        }

        Ok(())
    }

    async fn get_node(&self, id: Uuid) -> Result<Option<DAGNode>> {
        let nodes = self.nodes.read().await;
        Ok(nodes.get(&id).cloned())
    }

    async fn remove_node(&self, id: Uuid) -> Result<bool> {
        let mut nodes = self.nodes.write().await;
        let mut edges = self.edges.write().await;
        let mut reverse_edges = self.reverse_edges.write().await;
        let mut tips = self.tips.write().await;

        if let Some(node) = nodes.remove(&id) {
            // Remove from edges
            edges.remove(&id);

            // Remove from reverse edges and update parent edges
            if let Some(parents) = reverse_edges.remove(&id) {
                for parent_id in parents {
                    if let Some(children) = edges.get_mut(&parent_id) {
                        children.retain(|&child_id| child_id != id);
                        // If parent has no more children, it becomes a tip
                        if children.is_empty() {
                            tips.insert(parent_id);
                        }
                    }
                }
            }

            // Remove from tips
            tips.remove(&id);

            // Update reverse edges for children
            for (child_id, parents) in reverse_edges.iter_mut() {
                parents.retain(|&parent_id| parent_id != id);
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn get_all_nodes(&self) -> Result<Vec<DAGNode>> {
        let nodes = self.nodes.read().await;
        Ok(nodes.values().cloned().collect())
    }

    async fn get_tips(&self) -> Result<Vec<Uuid>> {
        let tips = self.tips.read().await;
        Ok(tips.iter().cloned().collect())
    }

    async fn get_children(&self, id: Uuid) -> Result<Vec<Uuid>> {
        let edges = self.edges.read().await;
        Ok(edges.get(&id).cloned().unwrap_or_default())
    }

    async fn get_parents(&self, id: Uuid) -> Result<Vec<Uuid>> {
        let reverse_edges = self.reverse_edges.read().await;
        Ok(reverse_edges.get(&id).cloned().unwrap_or_default())
    }

    async fn contains_node(&self, id: Uuid) -> Result<bool> {
        let nodes = self.nodes.read().await;
        Ok(nodes.contains_key(&id))
    }

    async fn get_stats(&self) -> Result<StorageStats> {
        let nodes = self.nodes.read().await;
        let tips = self.tips.read().await;

        let total_size = nodes
            .values()
            .map(|node| node.data.len() + std::mem::size_of::<DAGNode>())
            .sum();

        Ok(StorageStats {
            node_count: nodes.len(),
            tip_count: tips.len(),
            total_size_bytes: total_size,
            storage_type: "memory".to_string(),
        })
    }

    async fn compact(&self) -> Result<()> {
        // Memory storage doesn't need compaction
        Ok(())
    }

    async fn backup(&self, path: &std::path::Path) -> Result<()> {
        let nodes = self.nodes.read().await;
        let backup_data = BackupData {
            nodes: nodes.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let serialized = bincode::serialize(&backup_data)
            .map_err(|e| QuDAGError::StorageError(format!("Backup serialization error: {}", e)))?;

        tokio::fs::write(path, serialized)
            .await
            .map_err(|e| QuDAGError::StorageError(format!("Backup write error: {}", e)))?;

        Ok(())
    }

    async fn restore(&self, path: &std::path::Path) -> Result<()> {
        let data = tokio::fs::read(path)
            .await
            .map_err(|e| QuDAGError::StorageError(format!("Restore read error: {}", e)))?;

        let backup_data: BackupData = bincode::deserialize(&data).map_err(|e| {
            QuDAGError::StorageError(format!("Restore deserialization error: {}", e))
        })?;

        // Clear current storage
        {
            let mut nodes = self.nodes.write().await;
            let mut edges = self.edges.write().await;
            let mut reverse_edges = self.reverse_edges.write().await;
            let mut tips = self.tips.write().await;

            nodes.clear();
            edges.clear();
            reverse_edges.clear();
            tips.clear();
        }

        // Restore nodes one by one to rebuild relationships
        for node in backup_data.nodes.into_values() {
            self.add_node(node).await?;
        }

        Ok(())
    }
}

/// Persistent storage implementation using files
#[derive(Debug)]
pub struct PersistentStorage {
    data_dir: std::path::PathBuf,
    memory_cache: MemoryStorage,
    write_buffer: Arc<RwLock<HashMap<Uuid, DAGNode>>>,
    flush_threshold: usize,
}

impl PersistentStorage {
    /// Create a new persistent storage
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        tokio::fs::create_dir_all(&data_dir).await.map_err(|e| {
            QuDAGError::StorageError(format!("Failed to create data directory: {}", e))
        })?;

        let storage = Self {
            data_dir,
            memory_cache: MemoryStorage::new(),
            write_buffer: Arc::new(RwLock::new(HashMap::new())),
            flush_threshold: 100,
        };

        // Load existing data
        storage.load_from_disk().await?;

        Ok(storage)
    }

    /// Load data from disk into memory cache
    async fn load_from_disk(&self) -> Result<()> {
        let nodes_file = self.data_dir.join("nodes.bin");
        if nodes_file.exists() {
            self.memory_cache.restore(nodes_file.as_path()).await?;
        }
        Ok(())
    }

    /// Flush write buffer to disk
    async fn flush_to_disk(&self) -> Result<()> {
        let buffer = {
            let mut write_buffer = self.write_buffer.write().await;
            let buffer = write_buffer.clone();
            write_buffer.clear();
            buffer
        };

        if buffer.is_empty() {
            return Ok(());
        }

        // Add buffered nodes to memory cache
        for node in buffer.into_values() {
            self.memory_cache.add_node(node).await?;
        }

        // Backup to disk
        let nodes_file = self.data_dir.join("nodes.bin");
        self.memory_cache.backup(nodes_file.as_path()).await?;

        Ok(())
    }
}

#[async_trait]
impl DAGStorage for PersistentStorage {
    async fn add_node(&self, node: DAGNode) -> Result<()> {
        let node_id = node.id;

        // Add to write buffer
        {
            let mut write_buffer = self.write_buffer.write().await;
            write_buffer.insert(node_id, node);

            // Flush if buffer is full
            if write_buffer.len() >= self.flush_threshold {
                drop(write_buffer);
                self.flush_to_disk().await?;
            }
        }

        Ok(())
    }

    async fn get_node(&self, id: Uuid) -> Result<Option<DAGNode>> {
        // Check write buffer first
        {
            let write_buffer = self.write_buffer.read().await;
            if let Some(node) = write_buffer.get(&id) {
                return Ok(Some(node.clone()));
            }
        }

        // Check memory cache
        self.memory_cache.get_node(id).await
    }

    async fn remove_node(&self, id: Uuid) -> Result<bool> {
        // Remove from write buffer
        {
            let mut write_buffer = self.write_buffer.write().await;
            write_buffer.remove(&id);
        }

        // Remove from memory cache
        self.memory_cache.remove_node(id).await
    }

    async fn get_all_nodes(&self) -> Result<Vec<DAGNode>> {
        let mut nodes = self.memory_cache.get_all_nodes().await?;

        // Add nodes from write buffer
        {
            let write_buffer = self.write_buffer.read().await;
            for node in write_buffer.values() {
                nodes.push(node.clone());
            }
        }

        Ok(nodes)
    }

    async fn get_tips(&self) -> Result<Vec<Uuid>> {
        // This is simplified - in reality we'd need to account for write buffer
        self.memory_cache.get_tips().await
    }

    async fn get_children(&self, id: Uuid) -> Result<Vec<Uuid>> {
        self.memory_cache.get_children(id).await
    }

    async fn get_parents(&self, id: Uuid) -> Result<Vec<Uuid>> {
        self.memory_cache.get_parents(id).await
    }

    async fn contains_node(&self, id: Uuid) -> Result<bool> {
        // Check write buffer first
        {
            let write_buffer = self.write_buffer.read().await;
            if write_buffer.contains_key(&id) {
                return Ok(true);
            }
        }

        self.memory_cache.contains_node(id).await
    }

    async fn get_stats(&self) -> Result<StorageStats> {
        let mut stats = self.memory_cache.get_stats().await?;

        // Add write buffer stats
        {
            let write_buffer = self.write_buffer.read().await;
            stats.node_count += write_buffer.len();
            stats.total_size_bytes += write_buffer
                .values()
                .map(|node| node.data.len() + std::mem::size_of::<DAGNode>())
                .sum::<usize>();
        }

        stats.storage_type = "persistent".to_string();
        Ok(stats)
    }

    async fn compact(&self) -> Result<()> {
        // Flush write buffer
        self.flush_to_disk().await?;

        // Compact memory cache
        self.memory_cache.compact().await?;

        Ok(())
    }

    async fn backup(&self, path: &std::path::Path) -> Result<()> {
        // Flush first
        self.flush_to_disk().await?;

        // Backup memory cache
        self.memory_cache.backup(path).await
    }

    async fn restore(&self, path: &std::path::Path) -> Result<()> {
        // Clear write buffer
        {
            let mut write_buffer = self.write_buffer.write().await;
            write_buffer.clear();
        }

        // Restore memory cache
        self.memory_cache.restore(path).await
    }
}

/// Storage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub node_count: usize,
    pub tip_count: usize,
    pub total_size_bytes: usize,
    pub storage_type: String,
}

/// Backup data structure
#[derive(Debug, Serialize, Deserialize)]
struct BackupData {
    nodes: HashMap<Uuid, DAGNode>,
    timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_memory_storage() {
        let storage = MemoryStorage::new();

        // Test adding a node
        let node = DAGNode::genesis(b"test".to_vec());
        let node_id = node.id;

        assert!(storage.add_node(node).await.is_ok());
        assert_eq!(storage.node_count().await, 1);

        // Test getting a node
        let retrieved = storage.get_node(node_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data, b"test");

        // Test tips
        let tips = storage.get_tips().await.unwrap();
        assert_eq!(tips, vec![node_id]);

        // Test removal
        assert!(storage.remove_node(node_id).await.unwrap());
        assert_eq!(storage.node_count().await, 0);
    }

    #[tokio::test]
    async fn test_dag_relationships() {
        let storage = MemoryStorage::new();

        // Create parent node
        let parent = DAGNode::genesis(b"parent".to_vec());
        let parent_id = parent.id;
        storage.add_node(parent).await.unwrap();

        // Create child node
        let child = DAGNode::new(Uuid::new_v4(), b"child".to_vec(), vec![parent_id]);
        let child_id = child.id;
        storage.add_node(child).await.unwrap();

        // Test relationships
        let children = storage.get_children(parent_id).await.unwrap();
        assert_eq!(children, vec![child_id]);

        let parents = storage.get_parents(child_id).await.unwrap();
        assert_eq!(parents, vec![parent_id]);

        // Test tips (only child should be a tip)
        let tips = storage.get_tips().await.unwrap();
        assert_eq!(tips, vec![child_id]);
    }

    #[tokio::test]
    async fn test_backup_restore() {
        let dir = tempdir().unwrap();
        let backup_path = dir.path().join("backup.bin");

        let storage = MemoryStorage::new();

        // Add some nodes
        let node1 = DAGNode::genesis(b"node1".to_vec());
        let node2 = DAGNode::genesis(b"node2".to_vec());

        storage.add_node(node1.clone()).await.unwrap();
        storage.add_node(node2.clone()).await.unwrap();

        // Backup
        storage.backup(backup_path.as_path()).await.unwrap();

        // Create new storage and restore
        let new_storage = MemoryStorage::new();
        new_storage.restore(backup_path.as_path()).await.unwrap();

        // Verify data
        assert_eq!(new_storage.node_count().await, 2);
        assert!(new_storage.get_node(node1.id).await.unwrap().is_some());
        assert!(new_storage.get_node(node2.id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_persistent_storage() {
        let dir = tempdir().unwrap();
        let storage = PersistentStorage::new(dir.path()).await.unwrap();

        // Add a node
        let node = DAGNode::genesis(b"persistent test".to_vec());
        let node_id = node.id;

        storage.add_node(node).await.unwrap();

        // Should be able to retrieve it
        let retrieved = storage.get_node(node_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data, b"persistent test");
    }

    #[tokio::test]
    async fn test_storage_stats() {
        let storage = MemoryStorage::new();

        let stats = storage.get_stats().await.unwrap();
        assert_eq!(stats.node_count, 0);
        assert_eq!(stats.tip_count, 0);

        // Add a node
        let node = DAGNode::genesis(b"stats test".to_vec());
        storage.add_node(node).await.unwrap();

        let stats = storage.get_stats().await.unwrap();
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.tip_count, 1);
        assert!(stats.total_size_bytes > 0);
    }
}

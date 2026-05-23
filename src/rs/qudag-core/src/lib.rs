//! QuDAG Core - Quantum-resistant DAG networking and consensus
//!
//! This crate provides the core networking and consensus functionality for QuDAG,
//! a quantum-resistant directed acyclic graph (DAG) system designed for peer-to-peer
//! mesh networking with post-quantum cryptography.

pub mod consensus;
pub mod crypto;
pub mod dag;
pub mod error;
pub mod metrics;
pub mod networking;
pub mod peer;
pub mod storage;

pub use consensus::{ConsensusEngine, ConsensusMessage, QRAvalanche};
pub use crypto::{PostQuantumSignature, QuantumFingerprint, QuantumResistantCrypto};
pub use dag::{DAGEdge, DAGNetwork, DAGNode, DAGValidation};
pub use error::{QuDAGError, Result};
pub use metrics::{ConsensusMetrics, NetworkMetrics};
pub use networking::{NetworkBehavior, NetworkEvent, QuDAGNetwork};
pub use peer::{PeerDiscovery, PeerInfo, PeerManager};
pub use storage::{DAGStorage, MemoryStorage, PersistentStorage};

/// Core QuDAG node that combines networking, consensus, and storage
#[derive(Debug)]
pub struct QuDAGNode {
    network: QuDAGNetwork,
    consensus: QRAvalanche,
    storage: Box<dyn DAGStorage + Send + Sync>,
    peer_manager: PeerManager,
    metrics: NetworkMetrics,
}

impl QuDAGNode {
    /// Create a new QuDAG node with the specified configuration
    pub async fn new(config: NodeConfig) -> Result<Self> {
        let storage = Box::new(MemoryStorage::new());
        let peer_manager = PeerManager::new(config.max_peers);
        let network = QuDAGNetwork::new(config.listen_addr, config.keypair).await?;
        let consensus = QRAvalanche::new(config.consensus_config);
        let metrics = NetworkMetrics::new();

        Ok(Self {
            network,
            consensus,
            storage,
            peer_manager,
            metrics,
        })
    }

    /// Start the QuDAG node and begin networking
    pub async fn start(&mut self) -> Result<()> {
        self.network.start().await?;
        self.consensus.start().await?;
        tracing::info!("QuDAG node started successfully");
        Ok(())
    }

    /// Stop the QuDAG node
    pub async fn stop(&mut self) -> Result<()> {
        self.consensus.stop().await?;
        self.network.stop().await?;
        tracing::info!("QuDAG node stopped");
        Ok(())
    }

    /// Submit a new transaction to the DAG
    pub async fn submit_transaction(&mut self, data: Vec<u8>) -> Result<uuid::Uuid> {
        let node_id = uuid::Uuid::new_v4();
        let dag_node = DAGNode::new(node_id, data, self.get_dag_tips().await?);

        // Validate and add to DAG
        self.consensus.validate_node(&dag_node).await?;
        self.storage.add_node(dag_node.clone()).await?;

        // Broadcast to network
        self.network.broadcast_dag_node(dag_node).await?;

        self.metrics.increment_transactions();
        Ok(node_id)
    }

    /// Get current DAG tips (nodes with no children)
    pub async fn get_dag_tips(&self) -> Result<Vec<uuid::Uuid>> {
        self.storage.get_tips().await
    }

    /// Get network peer count
    pub fn peer_count(&self) -> usize {
        self.peer_manager.peer_count()
    }

    /// Get node metrics
    pub fn metrics(&self) -> &NetworkMetrics {
        &self.metrics
    }
}

/// Configuration for a QuDAG node
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub listen_addr: multiaddr::Multiaddr,
    pub keypair: libp2p::identity::Keypair,
    pub max_peers: usize,
    pub consensus_config: consensus::ConsensusConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            listen_addr: "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
            keypair: libp2p::identity::Keypair::generate_ed25519(),
            max_peers: 50,
            consensus_config: consensus::ConsensusConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;

    #[tokio::test]
    async fn test_node_creation() {
        let config = NodeConfig::default();
        let node = QuDAGNode::new(config).await;
        assert!(node.is_ok());
    }

    #[tokio::test]
    async fn test_node_lifecycle() {
        let config = NodeConfig::default();
        let mut node = QuDAGNode::new(config).await.unwrap();

        assert!(node.start().await.is_ok());
        assert!(node.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_transaction_submission() {
        let config = NodeConfig::default();
        let mut node = QuDAGNode::new(config).await.unwrap();

        let data = b"test transaction".to_vec();
        let tx_id = node.submit_transaction(data).await;
        assert!(tx_id.is_ok());
    }
}

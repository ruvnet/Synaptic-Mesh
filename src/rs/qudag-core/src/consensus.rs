//! QR-Avalanche Consensus Algorithm
//!
//! Quantum-resistant adaptation of the Avalanche consensus protocol for DAG networks.
//! Uses post-quantum cryptography and probabilistic finality.

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::{DAGNode, QuDAGError, QuantumResistantCrypto, Result};

/// QR-Avalanche consensus engine
#[derive(Debug)]
pub struct QRAvalanche {
    config: ConsensusConfig,
    state: Arc<RwLock<ConsensusState>>,
    pending_votes: Arc<DashMap<Uuid, VoteSet>>,
    finalized_nodes: Arc<Mutex<HashSet<Uuid>>>,
    crypto: QuantumResistantCrypto,
    message_tx: Option<mpsc::UnboundedSender<ConsensusMessage>>,
    message_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<ConsensusMessage>>>>,
}

impl QRAvalanche {
    /// Create a new QR-Avalanche consensus engine
    pub fn new(config: ConsensusConfig) -> Self {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        Self {
            config,
            state: Arc::new(RwLock::new(ConsensusState::new())),
            pending_votes: Arc::new(DashMap::new()),
            finalized_nodes: Arc::new(Mutex::new(HashSet::new())),
            crypto: QuantumResistantCrypto::new(),
            message_tx: Some(message_tx),
            message_rx: Arc::new(Mutex::new(Some(message_rx))),
        }
    }

    /// Start the consensus engine
    pub async fn start(&mut self) -> Result<()> {
        let message_rx = self
            .message_rx
            .lock()
            .take()
            .ok_or(QuDAGError::ConsensusError("Already started".to_string()))?;

        let state = Arc::clone(&self.state);
        let pending_votes = Arc::clone(&self.pending_votes);
        let finalized_nodes = Arc::clone(&self.finalized_nodes);
        let config = self.config.clone();

        // Spawn consensus processing task
        tokio::spawn(async move {
            Self::process_consensus_messages(
                message_rx,
                state,
                pending_votes,
                finalized_nodes,
                config,
            )
            .await;
        });

        tracing::info!("QR-Avalanche consensus engine started");
        Ok(())
    }

    /// Stop the consensus engine
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(tx) = self.message_tx.take() {
            drop(tx); // Close the channel
        }
        tracing::info!("QR-Avalanche consensus engine stopped");
        Ok(())
    }

    /// Validate a DAG node according to consensus rules
    pub async fn validate_node(&self, node: &DAGNode) -> Result<()> {
        // Check node structure
        if node.parents().is_empty() && !self.is_genesis_allowed().await? {
            return Err(QuDAGError::ValidationError(
                "Non-genesis node must have parents".to_string(),
            ));
        }

        // Check parent validity
        for parent_id in node.parents() {
            if !self.is_node_finalized(*parent_id).await? {
                return Err(QuDAGError::ValidationError(format!(
                    "Parent {} not finalized",
                    parent_id
                )));
            }
        }

        // Verify quantum-resistant signature
        self.crypto.verify_signature(node)?;

        Ok(())
    }

    /// Submit a vote for a DAG node
    pub async fn submit_vote(&self, node_id: Uuid, vote: Vote) -> Result<()> {
        if let Some(tx) = &self.message_tx {
            let message = ConsensusMessage::Vote { node_id, vote };
            tx.send(message)
                .map_err(|_| QuDAGError::ConsensusError("Channel closed".to_string()))?;
        }
        Ok(())
    }

    /// Check if a node is finalized
    pub async fn is_node_finalized(&self, node_id: Uuid) -> Result<bool> {
        Ok(self.finalized_nodes.lock().contains(&node_id))
    }

    /// Check if genesis nodes are allowed
    async fn is_genesis_allowed(&self) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.finalized_count == 0)
    }

    /// Main consensus processing loop
    async fn process_consensus_messages(
        mut message_rx: mpsc::UnboundedReceiver<ConsensusMessage>,
        state: Arc<RwLock<ConsensusState>>,
        pending_votes: Arc<DashMap<Uuid, VoteSet>>,
        finalized_nodes: Arc<Mutex<HashSet<Uuid>>>,
        config: ConsensusConfig,
    ) {
        while let Some(message) = message_rx.recv().await {
            match message {
                ConsensusMessage::Vote { node_id, vote } => {
                    Self::process_vote(
                        node_id,
                        vote,
                        &state,
                        &pending_votes,
                        &finalized_nodes,
                        &config,
                    )
                    .await;
                }
                ConsensusMessage::Query { node_id, requester } => {
                    Self::process_query(node_id, requester, &state).await;
                }
            }
        }
    }

    /// Process a single vote
    async fn process_vote(
        node_id: Uuid,
        vote: Vote,
        state: &Arc<RwLock<ConsensusState>>,
        pending_votes: &Arc<DashMap<Uuid, VoteSet>>,
        finalized_nodes: &Arc<Mutex<HashSet<Uuid>>>,
        config: &ConsensusConfig,
    ) {
        // Add vote to pending votes
        let mut vote_set = pending_votes.entry(node_id).or_insert_with(VoteSet::new);
        vote_set.add_vote(vote);

        // Check if we have enough votes for finalization
        if vote_set.positive_votes() >= config.finalization_threshold {
            // Finalize the node
            finalized_nodes.lock().insert(node_id);
            pending_votes.remove(&node_id);

            let mut state_guard = state.write().await;
            state_guard.finalized_count += 1;

            tracing::debug!(
                "Node {} finalized with {} votes",
                node_id,
                vote_set.positive_votes()
            );
        }
    }

    /// Process a consensus query
    async fn process_query(
        node_id: Uuid,
        requester: libp2p::PeerId,
        state: &Arc<RwLock<ConsensusState>>,
    ) {
        // Implementation would depend on network layer for responding to queries
        tracing::debug!(
            "Received query for node {} from peer {}",
            node_id,
            requester
        );
    }
}

/// Consensus configuration
#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    pub finalization_threshold: usize,
    pub sample_size: usize,
    pub confidence_threshold: f64,
    pub beta1: f64,
    pub beta2: f64,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            finalization_threshold: 10,
            sample_size: 20,
            confidence_threshold: 0.8,
            beta1: 15.0,
            beta2: 20.0,
        }
    }
}

/// Internal consensus state
#[derive(Debug)]
struct ConsensusState {
    finalized_count: u64,
    last_finalized_round: u64,
}

impl ConsensusState {
    fn new() -> Self {
        Self {
            finalized_count: 0,
            last_finalized_round: 0,
        }
    }
}

/// Set of votes for a DAG node
#[derive(Debug, Default)]
struct VoteSet {
    positive: usize,
    negative: usize,
    voters: HashSet<String>,
}

impl VoteSet {
    fn new() -> Self {
        Self::default()
    }

    fn add_vote(&mut self, vote: Vote) {
        if !self.voters.contains(&vote.voter) {
            self.voters.insert(vote.voter.clone());
            if vote.preference {
                self.positive += 1;
            } else {
                self.negative += 1;
            }
        }
    }

    fn positive_votes(&self) -> usize {
        self.positive
    }

    fn total_votes(&self) -> usize {
        self.positive + self.negative
    }
}

/// A vote in the consensus process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vote {
    /// String representation of the voter PeerId for serialization compatibility
    pub voter: String,
    pub preference: bool,
    pub timestamp: u64,
    pub signature: Vec<u8>,
}

/// Consensus messages exchanged between nodes
#[derive(Debug, Clone)]
pub enum ConsensusMessage {
    Vote {
        node_id: Uuid,
        vote: Vote,
    },
    Query {
        node_id: Uuid,
        requester: libp2p::PeerId,
    },
}

/// Trait for consensus engines
pub trait ConsensusEngine: Send + Sync {
    fn validate_node(&self, node: &DAGNode)
        -> impl std::future::Future<Output = Result<()>> + Send;
    fn submit_vote(
        &self,
        node_id: Uuid,
        vote: Vote,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn is_finalized(&self, node_id: Uuid)
        -> impl std::future::Future<Output = Result<bool>> + Send;
}

impl ConsensusEngine for QRAvalanche {
    async fn validate_node(&self, node: &DAGNode) -> Result<()> {
        self.validate_node(node).await
    }

    async fn submit_vote(&self, node_id: Uuid, vote: Vote) -> Result<()> {
        self.submit_vote(node_id, vote).await
    }

    async fn is_finalized(&self, node_id: Uuid) -> Result<bool> {
        self.is_node_finalized(node_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_consensus_creation() {
        let config = ConsensusConfig::default();
        let consensus = QRAvalanche::new(config);
        assert!(consensus.pending_votes.is_empty());
    }

    #[tokio::test]
    async fn test_consensus_lifecycle() {
        let config = ConsensusConfig::default();
        let mut consensus = QRAvalanche::new(config);

        assert!(consensus.start().await.is_ok());
        assert!(consensus.stop().await.is_ok());
    }

    #[test]
    fn test_vote_set() {
        let mut vote_set = VoteSet::new();
        let peer_id = libp2p::PeerId::random();

        let vote = Vote {
            voter: peer_id.to_string(),
            preference: true,
            timestamp: 0,
            signature: vec![],
        };

        vote_set.add_vote(vote);
        assert_eq!(vote_set.positive_votes(), 1);
        assert_eq!(vote_set.total_votes(), 1);
    }
}

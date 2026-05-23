//! P2P networking layer for distributed market coordination
//!
//! This module provides peer-to-peer communication capabilities for the
//! Synaptic Market, enabling distributed order matching, reputation synchronization,
//! and coordination between market participants.

use crate::error::{MarketError, Result};
use crate::market::{Order, TaskAssignment};
use crate::reputation::ReputationScore;
use libp2p::{
    gossipsub::{self, Event as GossipsubEvent, IdentTopic, MessageAuthenticity, ValidationMode},
    identify::{self, Event as IdentifyEvent},
    kad::{self, Event as KademliaEvent, QueryResult, RecordKey},
    noise, tcp, yamux, Multiaddr, PeerId, SwarmBuilder,
    swarm::NetworkBehaviour,
    swarm::Swarm,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

/// P2P message types for market coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketMessage {
    /// New order announcement
    OrderAnnouncement {
        order: Order,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Order cancellation
    OrderCancellation {
        order_id: Uuid,
        trader: PeerId,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Trade execution confirmation
    TradeExecution {
        assignment: TaskAssignment,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Reputation update
    ReputationUpdate {
        peer_id: PeerId,
        score: ReputationScore,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Price discovery data
    PriceDiscovery {
        task_type: String,
        price_data: crate::market::PriceDiscovery,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Peer connection request
    ConnectionRequest {
        peer_id: PeerId,
        capabilities: Vec<String>,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Market heartbeat for network health
    Heartbeat {
        peer_id: PeerId,
        active_orders: u64,
        available_capacity: u64,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
}

/// P2P event types
#[derive(Debug)]
pub enum P2PEvent {
    /// New peer discovered
    PeerDiscovered {
        peer_id: PeerId,
        address: Multiaddr,
    },
    /// Peer disconnected
    PeerDisconnected {
        peer_id: PeerId,
    },
    /// Market message received
    MessageReceived {
        from: PeerId,
        message: MarketMessage,
    },
    /// DHT record stored
    RecordStored {
        key: RecordKey,
        peer_id: PeerId,
    },
    /// DHT record retrieved
    RecordRetrieved {
        key: RecordKey,
        record: kad::Record,
    },
}

/// Network behavior for the market
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "MarketBehaviourEvent")]
pub struct MarketBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub identify: identify::Behaviour,
}

#[derive(Debug)]
pub enum MarketBehaviourEvent {
    Gossipsub(GossipsubEvent),
    Kademlia(KademliaEvent),
    Identify(IdentifyEvent),
}

impl From<GossipsubEvent> for MarketBehaviourEvent {
    fn from(event: GossipsubEvent) -> Self {
        MarketBehaviourEvent::Gossipsub(event)
    }
}

impl From<KademliaEvent> for MarketBehaviourEvent {
    fn from(event: KademliaEvent) -> Self {
        MarketBehaviourEvent::Kademlia(event)
    }
}

impl From<IdentifyEvent> for MarketBehaviourEvent {
    fn from(event: IdentifyEvent) -> Self {
        MarketBehaviourEvent::Identify(event)
    }
}

/// P2P network configuration
#[derive(Debug, Clone)]
pub struct P2PConfig {
    /// Local peer ID
    pub local_peer_id: PeerId,
    /// Listen addresses
    pub listen_addresses: Vec<Multiaddr>,
    /// Bootstrap addresses for initial connection
    pub bootstrap_addresses: Vec<Multiaddr>,
    /// Maximum number of connections
    pub max_connections: usize,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// DHT record TTL
    pub record_ttl: Duration,
}

impl Default for P2PConfig {
    fn default() -> Self {
        Self {
            local_peer_id: PeerId::random(),
            listen_addresses: vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()],
            bootstrap_addresses: vec![],
            max_connections: 100,
            connection_timeout: Duration::from_secs(30),
            record_ttl: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// P2P network manager for market operations
pub struct P2PNetwork {
    swarm: Swarm<MarketBehaviour>,
    event_sender: mpsc::UnboundedSender<P2PEvent>,
    event_receiver: Arc<RwLock<mpsc::UnboundedReceiver<P2PEvent>>>,
    connected_peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    config: P2PConfig,
}

/// Information about connected peers
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Peer ID
    pub peer_id: PeerId,
    /// Network addresses
    pub addresses: Vec<Multiaddr>,
    /// Capabilities/features supported
    pub capabilities: Vec<String>,
    /// Last seen timestamp
    pub last_seen: chrono::DateTime<chrono::Utc>,
    /// Reputation score (if known)
    pub reputation: Option<f64>,
    /// Market statistics
    pub market_stats: MarketStats,
}

/// Market statistics for a peer
#[derive(Debug, Clone, Default)]
pub struct MarketStats {
    /// Number of active orders
    pub active_orders: u64,
    /// Available compute capacity
    pub available_capacity: u64,
    /// Total completed trades
    pub completed_trades: u64,
    /// Average response time (seconds)
    pub avg_response_time: Option<f64>,
}

impl P2PNetwork {
    /// Create a new P2P network instance
    pub async fn new(config: P2PConfig) -> Result<Self> {
        // Create a random key pair for the network identity
        let local_key = libp2p::identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());

        // Build the swarm using the SwarmBuilder API (libp2p 0.54+)
        let swarm = SwarmBuilder::with_existing_identity(local_key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|e| MarketError::Network(format!("TCP transport error: {}", e)))?
            .with_behaviour(|key| {
                // Create the Gossipsub behaviour
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10))
                    .validation_mode(ValidationMode::Strict)
                    .message_id_fn(|message| {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        message.data.hash(&mut hasher);
                        gossipsub::MessageId::from(hasher.finish().to_string())
                    })
                    .build()
                    .expect("valid gossipsub config");

                let gossipsub = gossipsub::Behaviour::new(
                    MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )
                .expect("valid gossipsub behaviour");

                // Create the Kademlia behaviour
                let mut kademlia = kad::Behaviour::new(
                    local_peer_id,
                    kad::store::MemoryStore::new(local_peer_id),
                );
                kademlia.set_mode(Some(kad::Mode::Server));

                // Create the Identify behaviour
                let identify = identify::Behaviour::new(identify::Config::new(
                    "/synaptic-market/1.0.0".to_string(),
                    key.public(),
                ));

                MarketBehaviour {
                    gossipsub,
                    kademlia,
                    identify,
                }
            })
            .map_err(|e| MarketError::Network(format!("Behaviour build error: {}", e)))?
            .with_swarm_config(|cfg| {
                cfg.with_idle_connection_timeout(Duration::from_secs(60))
            })
            .build();

        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(Self {
            swarm,
            event_sender,
            event_receiver: Arc::new(RwLock::new(event_receiver)),
            connected_peers: Arc::new(RwLock::new(HashMap::new())),
            config,
        })
    }

    /// Start the P2P network
    pub async fn start(&mut self) -> Result<()> {
        // Subscribe to market topics
        let topics = vec![
            "market/orders",
            "market/trades", 
            "market/reputation",
            "market/prices",
            "market/heartbeat",
        ];

        for topic_str in topics {
            let topic = IdentTopic::new(topic_str);
            self.swarm.behaviour_mut().gossipsub
                .subscribe(&topic)
                .map_err(|e| MarketError::Network(format!("Topic subscription error: {}", e)))?;
        }

        // Start listening on configured addresses
        for addr in &self.config.listen_addresses {
            self.swarm.listen_on(addr.clone())
                .map_err(|e| MarketError::Network(format!("Listen error: {}", e)))?;
        }

        // Connect to bootstrap peers
        for addr in &self.config.bootstrap_addresses {
            if let Err(e) = self.swarm.dial(addr.clone()) {
                tracing::warn!("Failed to connect to bootstrap peer {}: {}", addr, e);
            }
        }

        Ok(())
    }

    /// Publish a market message to the network
    pub async fn publish_message(&mut self, message: MarketMessage) -> Result<()> {
        let topic = match &message {
            MarketMessage::OrderAnnouncement { .. } | MarketMessage::OrderCancellation { .. } => "market/orders",
            MarketMessage::TradeExecution { .. } => "market/trades",
            MarketMessage::ReputationUpdate { .. } => "market/reputation", 
            MarketMessage::PriceDiscovery { .. } => "market/prices",
            MarketMessage::Heartbeat { .. } => "market/heartbeat",
            MarketMessage::ConnectionRequest { .. } => "market/connection",
        };

        let data = serde_json::to_vec(&message)
            .map_err(|e| MarketError::Serialization(e))?;

        let topic = IdentTopic::new(topic);
        self.swarm.behaviour_mut().gossipsub
            .publish(topic, data)
            .map_err(|e| MarketError::Network(format!("Publish error: {}", e)))?;

        Ok(())
    }

    /// Store data in the DHT
    pub async fn store_dht_record(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let record_key = RecordKey::new(&key);
        let record = kad::Record {
            key: record_key.clone(),
            value,
            publisher: Some(self.config.local_peer_id),
            expires: Some(std::time::Instant::now() + self.config.record_ttl),
        };

        self.swarm.behaviour_mut().kademlia
            .put_record(record, kad::Quorum::One)
            .map_err(|e| MarketError::Network(format!("DHT store error: {}", e)))?;

        Ok(())
    }

    /// Retrieve data from the DHT
    pub async fn get_dht_record(&mut self, key: String) -> Result<()> {
        let record_key = RecordKey::new(&key);
        self.swarm.behaviour_mut().kademlia
            .get_record(record_key);
        Ok(())
    }

    /// Get connected peers
    pub async fn get_connected_peers(&self) -> HashMap<PeerId, PeerInfo> {
        self.connected_peers.read().await.clone()
    }

    /// Process network events
    pub async fn handle_event(&mut self, event: MarketBehaviourEvent) {
        match event {
            MarketBehaviourEvent::Gossipsub(GossipsubEvent::Message {
                propagation_source,
                message_id: _,
                message,
            }) => {
                if let Ok(market_message) = serde_json::from_slice::<MarketMessage>(&message.data) {
                    let _ = self.event_sender.send(P2PEvent::MessageReceived {
                        from: propagation_source,
                        message: market_message,
                    });
                }
            }
            MarketBehaviourEvent::Kademlia(KademliaEvent::OutboundQueryProgressed {
                result: QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(record))),
                ..
            }) => {
                let _ = self.event_sender.send(P2PEvent::RecordRetrieved {
                    key: record.record.key.clone(),
                    record: record.record,
                });
            }
            MarketBehaviourEvent::Kademlia(KademliaEvent::OutboundQueryProgressed {
                result: QueryResult::PutRecord(Ok(kad::PutRecordOk { key })),
                ..
            }) => {
                let _ = self.event_sender.send(P2PEvent::RecordStored {
                    key,
                    peer_id: self.config.local_peer_id,
                });
            }
            MarketBehaviourEvent::Identify(IdentifyEvent::Received {
                peer_id,
                info,
            }) => {
                // Add peer to Kademlia
                for addr in &info.listen_addrs {
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                }

                // Update peer info
                let peer_info = PeerInfo {
                    peer_id,
                    addresses: info.listen_addrs.clone(),
                    capabilities: vec!["market".to_string()], // Could be extracted from protocol version
                    last_seen: chrono::Utc::now(),
                    reputation: None,
                    market_stats: MarketStats::default(),
                };

                self.connected_peers.write().await.insert(peer_id, peer_info);

                let _ = self.event_sender.send(P2PEvent::PeerDiscovered {
                    peer_id,
                    address: info.listen_addrs.first().cloned().unwrap_or_else(|| {
                        "/ip4/127.0.0.1/tcp/0".parse().unwrap()
                    }),
                });
            }
            _ => {}
        }
    }

    /// Get the next P2P event
    pub async fn next_event(&self) -> Option<P2PEvent> {
        self.event_receiver.write().await.recv().await
    }

    /// Broadcast order to the network
    pub async fn broadcast_order(&mut self, order: Order) -> Result<()> {
        let message = MarketMessage::OrderAnnouncement {
            order,
            timestamp: chrono::Utc::now(),
        };
        self.publish_message(message).await
    }

    /// Broadcast order cancellation
    pub async fn broadcast_cancellation(&mut self, order_id: Uuid, trader: PeerId) -> Result<()> {
        let message = MarketMessage::OrderCancellation {
            order_id,
            trader,
            timestamp: chrono::Utc::now(),
        };
        self.publish_message(message).await
    }

    /// Broadcast trade execution
    pub async fn broadcast_trade(&mut self, assignment: TaskAssignment) -> Result<()> {
        let message = MarketMessage::TradeExecution {
            assignment,
            timestamp: chrono::Utc::now(),
        };
        self.publish_message(message).await
    }

    /// Broadcast reputation update
    pub async fn broadcast_reputation(&mut self, peer_id: PeerId, score: ReputationScore) -> Result<()> {
        let message = MarketMessage::ReputationUpdate {
            peer_id,
            score,
            timestamp: chrono::Utc::now(),
        };
        self.publish_message(message).await
    }

    /// Send heartbeat to the network
    pub async fn send_heartbeat(&mut self, active_orders: u64, available_capacity: u64) -> Result<()> {
        let message = MarketMessage::Heartbeat {
            peer_id: self.config.local_peer_id,
            active_orders,
            available_capacity,
            timestamp: chrono::Utc::now(),
        };
        self.publish_message(message).await
    }

    /// Bootstrap network connectivity by connecting to known peers
    pub async fn bootstrap(&mut self) -> Result<()> {
        self.swarm.behaviour_mut().kademlia.bootstrap()
            .map_err(|e| MarketError::Network(format!("Bootstrap error: {}", e)))?;
        Ok(())
    }

    /// Update peer market statistics
    pub async fn update_peer_stats(&self, peer_id: PeerId, stats: MarketStats) {
        if let Some(peer_info) = self.connected_peers.write().await.get_mut(&peer_id) {
            peer_info.market_stats = stats;
            peer_info.last_seen = chrono::Utc::now();
        }
    }

    /// Find peers offering specific capabilities
    pub async fn find_peers_with_capability(&self, capability: &str) -> Vec<PeerInfo> {
        self.connected_peers
            .read()
            .await
            .values()
            .filter(|peer| peer.capabilities.contains(&capability.to_string()))
            .cloned()
            .collect()
    }

    /// Check network health
    pub async fn network_health(&self) -> NetworkHealth {
        let peers = self.connected_peers.read().await;
        let total_peers = peers.len();
        let active_peers = peers
            .values()
            .filter(|p| chrono::Utc::now().signed_duration_since(p.last_seen).num_seconds() < 300)
            .count();
        
        let avg_reputation = if !peers.is_empty() {
            peers
                .values()
                .filter_map(|p| p.reputation)
                .sum::<f64>()
                / peers.len() as f64
        } else {
            0.0
        };

        NetworkHealth {
            total_peers,
            active_peers,
            avg_reputation,
            network_load: 0.0, // Could be calculated based on message volume
        }
    }
}

/// Network health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkHealth {
    /// Total number of known peers
    pub total_peers: usize,
    /// Number of active peers (seen recently)
    pub active_peers: usize,
    /// Average reputation across all peers
    pub avg_reputation: f64,
    /// Network load (0.0 to 1.0)
    pub network_load: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_p2p_network_creation() {
        let config = P2PConfig::default();
        let network = P2PNetwork::new(config).await.unwrap();
        
        // Verify network was created successfully
        assert_eq!(network.connected_peers.read().await.len(), 0);
    }

    #[tokio::test]
    async fn test_message_serialization() {
        let order = Order {
            id: Uuid::new_v4(),
            order_type: OrderType::RequestCompute,
            trader: PeerId::random(),
            price_per_unit: 100,
            total_units: 50,
            filled_units: 0,
            status: crate::market::OrderStatus::Active,
            task_spec: crate::market::ComputeTaskSpec {
                task_type: "test".to_string(),
                compute_units: 50,
                max_duration_secs: 300,
                required_capabilities: vec!["test".to_string()],
                min_reputation: None,
                privacy_level: crate::market::PrivacyLevel::Public,
                encrypted_payload: None,
            },
            sla_spec: None,
            reputation_weight: 1.0,
            expires_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            signature: None,
        };

        let message = MarketMessage::OrderAnnouncement {
            order,
            timestamp: chrono::Utc::now(),
        };

        // Test serialization and deserialization
        let serialized = serde_json::to_vec(&message).unwrap();
        let deserialized: MarketMessage = serde_json::from_slice(&serialized).unwrap();
        
        match deserialized {
            MarketMessage::OrderAnnouncement { .. } => assert!(true),
            _ => panic!("Wrong message type after deserialization"),
        }
    }
}
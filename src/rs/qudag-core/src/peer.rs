//! Peer management and discovery for QuDAG network

use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{QuDAGError, QuantumFingerprint, Result};

/// Peer information in the QuDAG network
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub protocols: Vec<String>,
    pub agent_version: String,
    pub quantum_fingerprint: Option<QuantumFingerprint>,
    pub dark_domain: Option<String>,
    pub last_seen: u64,
    pub reputation: f64,
    pub capabilities: PeerCapabilities,
}

impl PeerInfo {
    /// Create new peer info
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            addresses: Vec::new(),
            protocols: Vec::new(),
            agent_version: "qudag/0.4.3".to_string(),
            quantum_fingerprint: None,
            dark_domain: None,
            last_seen: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            reputation: 1.0,
            capabilities: PeerCapabilities::default(),
        }
    }

    /// Update last seen timestamp
    pub fn update_last_seen(&mut self) {
        self.last_seen = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Check if peer is recently active
    pub fn is_active(&self, max_age_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.saturating_sub(self.last_seen) <= max_age_secs
    }

    /// Add an address to this peer
    pub fn add_address(&mut self, addr: Multiaddr) {
        if !self.addresses.contains(&addr) {
            self.addresses.push(addr);
        }
    }

    /// Set quantum fingerprint and derive .dark domain
    pub fn set_quantum_fingerprint(&mut self, fingerprint: QuantumFingerprint) {
        self.dark_domain = Some(fingerprint.to_dark_domain());
        self.quantum_fingerprint = Some(fingerprint);
    }

    /// Adjust reputation based on behavior
    pub fn adjust_reputation(&mut self, delta: f64) {
        self.reputation = (self.reputation + delta).clamp(0.0, 10.0);
    }

    /// Check if peer supports a capability
    pub fn supports_capability(&self, capability: &str) -> bool {
        match capability {
            "dag_sync" => self.capabilities.dag_sync,
            "consensus" => self.capabilities.consensus,
            "quantum_crypto" => self.capabilities.quantum_crypto,
            "relay" => self.capabilities.relay,
            "storage" => self.capabilities.storage,
            "compute" => self.capabilities.compute,
            _ => false,
        }
    }
}

/// Peer capabilities in the network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerCapabilities {
    pub dag_sync: bool,
    pub consensus: bool,
    pub quantum_crypto: bool,
    pub relay: bool,
    pub storage: bool,
    pub compute: bool,
    pub max_connections: u32,
    pub bandwidth_limit: Option<u64>, // bytes per second
}

impl Default for PeerCapabilities {
    fn default() -> Self {
        Self {
            dag_sync: true,
            consensus: true,
            quantum_crypto: true,
            relay: false,
            storage: false,
            compute: false,
            max_connections: 50,
            bandwidth_limit: None,
        }
    }
}

/// Peer manager for handling peer discovery and management
#[derive(Debug)]
pub struct PeerManager {
    peers: HashMap<PeerId, PeerInfo>,
    dark_domains: HashMap<String, PeerId>, // .dark domain -> peer_id mapping
    max_peers: usize,
    reputation_threshold: f64,
    cleanup_interval: Duration,
    last_cleanup: SystemTime,
}

impl PeerManager {
    /// Create a new peer manager
    pub fn new(max_peers: usize) -> Self {
        Self {
            peers: HashMap::new(),
            dark_domains: HashMap::new(),
            max_peers,
            reputation_threshold: 0.5,
            cleanup_interval: Duration::from_secs(300), // 5 minutes
            last_cleanup: SystemTime::now(),
        }
    }

    /// Add or update a peer
    pub fn add_peer(&mut self, mut peer_info: PeerInfo) {
        peer_info.update_last_seen();

        // Register .dark domain if available
        if let Some(ref dark_domain) = peer_info.dark_domain {
            self.dark_domains
                .insert(dark_domain.clone(), peer_info.peer_id);
        }

        self.peers.insert(peer_info.peer_id, peer_info);

        // Cleanup if necessary
        if self.peers.len() > self.max_peers {
            self.cleanup_peers();
        }
    }

    /// Remove a peer
    pub fn remove_peer(&mut self, peer_id: &PeerId) -> Option<PeerInfo> {
        if let Some(peer_info) = self.peers.remove(peer_id) {
            // Remove from dark domains
            if let Some(ref dark_domain) = peer_info.dark_domain {
                self.dark_domains.remove(dark_domain);
            }
            Some(peer_info)
        } else {
            None
        }
    }

    /// Get a peer by ID
    pub fn get_peer(&self, peer_id: &PeerId) -> Option<&PeerInfo> {
        self.peers.get(peer_id)
    }

    /// Get a peer by .dark domain
    pub fn get_peer_by_dark_domain(&self, dark_domain: &str) -> Option<&PeerInfo> {
        self.dark_domains
            .get(dark_domain)
            .and_then(|peer_id| self.peers.get(peer_id))
    }

    /// Get all connected peers
    pub fn connected_peers(&self) -> Vec<PeerId> {
        self.peers.keys().cloned().collect()
    }

    /// Get peers with specific capability
    pub fn peers_with_capability(&self, capability: &str) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter(|(_, peer)| peer.supports_capability(capability))
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Get high-reputation peers
    pub fn trusted_peers(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter(|(_, peer)| peer.reputation >= self.reputation_threshold * 2.0)
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Update peer reputation
    pub fn update_reputation(&mut self, peer_id: &PeerId, delta: f64) {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.adjust_reputation(delta);
        }
    }

    /// Get peer count
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Get network statistics
    pub fn get_stats(&self) -> PeerStats {
        let active_peers = self
            .peers
            .values()
            .filter(|p| p.is_active(300)) // Active in last 5 minutes
            .count();

        let avg_reputation = if self.peers.is_empty() {
            0.0
        } else {
            self.peers.values().map(|p| p.reputation).sum::<f64>() / self.peers.len() as f64
        };

        let capabilities_count: HashMap<String, usize> =
            self.peers.values().fold(HashMap::new(), |mut acc, peer| {
                if peer.capabilities.dag_sync {
                    *acc.entry("dag_sync".to_string()).or_insert(0) += 1;
                }
                if peer.capabilities.consensus {
                    *acc.entry("consensus".to_string()).or_insert(0) += 1;
                }
                if peer.capabilities.quantum_crypto {
                    *acc.entry("quantum_crypto".to_string()).or_insert(0) += 1;
                }
                if peer.capabilities.relay {
                    *acc.entry("relay".to_string()).or_insert(0) += 1;
                }
                if peer.capabilities.storage {
                    *acc.entry("storage".to_string()).or_insert(0) += 1;
                }
                if peer.capabilities.compute {
                    *acc.entry("compute".to_string()).or_insert(0) += 1;
                }
                acc
            });

        PeerStats {
            total_peers: self.peers.len(),
            active_peers,
            dark_domains: self.dark_domains.len(),
            average_reputation: avg_reputation,
            capabilities_count,
        }
    }

    /// Cleanup inactive peers
    pub fn cleanup_peers(&mut self) {
        let now = SystemTime::now();
        if now.duration_since(self.last_cleanup).unwrap() < self.cleanup_interval {
            return;
        }

        let max_age = 3600; // 1 hour
        let min_reputation = self.reputation_threshold;

        let to_remove: Vec<PeerId> = self
            .peers
            .iter()
            .filter(|(_, peer)| !peer.is_active(max_age) || peer.reputation < min_reputation)
            .map(|(peer_id, _)| *peer_id)
            .collect();

        for peer_id in to_remove {
            self.remove_peer(&peer_id);
        }

        self.last_cleanup = now;

        tracing::debug!(
            "Peer cleanup completed, {} peers remaining",
            self.peers.len()
        );
    }

    /// Find best peers for a specific task
    pub fn find_best_peers(&self, capability: &str, count: usize) -> Vec<PeerId> {
        let mut candidates: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.supports_capability(capability) && peer.is_active(300))
            .collect();

        // Sort by reputation (descending)
        candidates.sort_by(|a, b| b.1.reputation.partial_cmp(&a.1.reputation).unwrap());

        candidates
            .into_iter()
            .take(count)
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }
}

/// Peer discovery mechanism
#[derive(Debug)]
pub struct PeerDiscovery {
    bootstrap_peers: Vec<Multiaddr>,
    discovery_interval: Duration,
    max_discovery_peers: usize,
}

impl PeerDiscovery {
    /// Create new peer discovery
    pub fn new(bootstrap_peers: Vec<Multiaddr>) -> Self {
        Self {
            bootstrap_peers,
            discovery_interval: Duration::from_secs(30),
            max_discovery_peers: 20,
        }
    }

    /// Get bootstrap peers
    pub fn bootstrap_peers(&self) -> &[Multiaddr] {
        &self.bootstrap_peers
    }

    /// Add bootstrap peer
    pub fn add_bootstrap_peer(&mut self, addr: Multiaddr) {
        if !self.bootstrap_peers.contains(&addr) {
            self.bootstrap_peers.push(addr);
        }
    }

    /// Discover peers through DHT
    pub async fn discover_peers(&self) -> Result<Vec<PeerInfo>> {
        // This would implement actual peer discovery through Kademlia DHT
        // For now, return empty list
        Ok(Vec::new())
    }

    /// Validate peer through quantum fingerprint
    pub fn validate_peer(&self, peer_info: &PeerInfo, challenge: &[u8]) -> Result<bool> {
        if let Some(ref fingerprint) = peer_info.quantum_fingerprint {
            // In a real implementation, this would validate the quantum fingerprint
            // against a challenge to prove identity without revealing private keys
            Ok(fingerprint.algorithm == "BLAKE3-PQ")
        } else {
            Ok(false)
        }
    }
}

/// Peer statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerStats {
    pub total_peers: usize,
    pub active_peers: usize,
    pub dark_domains: usize,
    pub average_reputation: f64,
    pub capabilities_count: HashMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_info_creation() {
        let peer_id = PeerId::random();
        let peer_info = PeerInfo::new(peer_id);

        assert_eq!(peer_info.peer_id, peer_id);
        assert_eq!(peer_info.reputation, 1.0);
        assert!(peer_info.is_active(3600));
    }

    #[test]
    fn test_peer_manager() {
        let mut manager = PeerManager::new(10);
        let peer_id = PeerId::random();
        let peer_info = PeerInfo::new(peer_id);

        assert_eq!(manager.peer_count(), 0);

        manager.add_peer(peer_info);
        assert_eq!(manager.peer_count(), 1);

        assert!(manager.get_peer(&peer_id).is_some());

        manager.remove_peer(&peer_id);
        assert_eq!(manager.peer_count(), 0);
    }

    #[test]
    fn test_reputation_system() {
        let mut manager = PeerManager::new(10);
        let peer_id = PeerId::random();
        let peer_info = PeerInfo::new(peer_id);

        manager.add_peer(peer_info);

        // Test reputation adjustment
        manager.update_reputation(&peer_id, 1.0);
        assert_eq!(manager.get_peer(&peer_id).unwrap().reputation, 2.0);

        manager.update_reputation(&peer_id, -3.0);
        assert_eq!(manager.get_peer(&peer_id).unwrap().reputation, 0.0); // Clamped to 0
    }

    #[test]
    fn test_capabilities() {
        let peer_id = PeerId::random();
        let mut peer_info = PeerInfo::new(peer_id);

        assert!(peer_info.supports_capability("dag_sync"));
        assert!(peer_info.supports_capability("consensus"));
        assert!(!peer_info.supports_capability("unknown"));

        peer_info.capabilities.relay = true;
        assert!(peer_info.supports_capability("relay"));
    }

    #[test]
    fn test_peer_discovery() {
        let bootstrap_peers = vec![
            "/ip4/127.0.0.1/tcp/8000".parse().unwrap(),
            "/ip4/127.0.0.1/tcp/8001".parse().unwrap(),
        ];

        let discovery = PeerDiscovery::new(bootstrap_peers.clone());
        assert_eq!(discovery.bootstrap_peers(), &bootstrap_peers);
    }
}

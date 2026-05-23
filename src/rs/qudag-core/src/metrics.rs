//! Metrics collection and monitoring for QuDAG

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Network metrics collector
#[derive(Debug)]
pub struct NetworkMetrics {
    // Connection metrics
    connected_peers: AtomicUsize,
    total_connections: AtomicU64,
    failed_connections: AtomicU64,

    // Message metrics
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,

    // DAG metrics
    transactions_submitted: AtomicU64,
    nodes_validated: AtomicU64,
    validation_failures: AtomicU64,

    // Performance metrics
    start_time: Instant,
    last_update: AtomicU64,

    // Histogram data
    latency_histogram: Arc<RwLock<LatencyHistogram>>,
    throughput_window: Arc<RwLock<ThroughputWindow>>,
}

impl NetworkMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            connected_peers: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            failed_connections: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            transactions_submitted: AtomicU64::new(0),
            nodes_validated: AtomicU64::new(0),
            validation_failures: AtomicU64::new(0),
            start_time: Instant::now(),
            last_update: AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
            latency_histogram: Arc::new(RwLock::new(LatencyHistogram::new())),
            throughput_window: Arc::new(RwLock::new(ThroughputWindow::new())),
        }
    }

    /// Record a new peer connection
    pub fn peer_connected(&self) {
        self.connected_peers.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.update_timestamp();
    }

    /// Record a peer disconnection
    pub fn peer_disconnected(&self) {
        self.connected_peers.fetch_sub(1, Ordering::Relaxed);
        self.update_timestamp();
    }

    /// Record a failed connection attempt
    pub fn connection_failed(&self) {
        self.failed_connections.fetch_add(1, Ordering::Relaxed);
        self.update_timestamp();
    }

    /// Record a sent message
    pub fn message_sent(&self, bytes: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
        self.throughput_window.write().record_sent(bytes);
        self.update_timestamp();
    }

    /// Record a received message
    pub fn message_received(&self, bytes: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.throughput_window.write().record_received(bytes);
        self.update_timestamp();
    }

    /// Record a transaction submission
    pub fn increment_transactions(&self) {
        self.transactions_submitted.fetch_add(1, Ordering::Relaxed);
        self.update_timestamp();
    }

    /// Record a successful node validation
    pub fn node_validated(&self) {
        self.nodes_validated.fetch_add(1, Ordering::Relaxed);
        self.update_timestamp();
    }

    /// Record a validation failure
    pub fn validation_failed(&self) {
        self.validation_failures.fetch_add(1, Ordering::Relaxed);
        self.update_timestamp();
    }

    /// Record network latency
    pub fn record_latency(&self, latency: Duration) {
        self.latency_histogram.write().record(latency);
        self.update_timestamp();
    }

    /// Get current metrics snapshot
    pub fn get_snapshot(&self) -> MetricsSnapshot {
        let latency_stats = self.latency_histogram.read().get_stats();
        let throughput_stats = self.throughput_window.read().get_stats();

        MetricsSnapshot {
            // Connection metrics
            connected_peers: self.connected_peers.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            failed_connections: self.failed_connections.load(Ordering::Relaxed),

            // Message metrics
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),

            // DAG metrics
            transactions_submitted: self.transactions_submitted.load(Ordering::Relaxed),
            nodes_validated: self.nodes_validated.load(Ordering::Relaxed),
            validation_failures: self.validation_failures.load(Ordering::Relaxed),

            // Performance metrics
            uptime_seconds: self.start_time.elapsed().as_secs(),
            last_update: self.last_update.load(Ordering::Relaxed),

            // Derived metrics
            connection_success_rate: self.calculate_connection_success_rate(),
            validation_success_rate: self.calculate_validation_success_rate(),

            // Latency and throughput
            latency_stats,
            throughput_stats,
        }
    }

    /// Get metrics in Prometheus format
    pub fn to_prometheus(&self) -> String {
        let snapshot = self.get_snapshot();

        format!(
            "# HELP qudag_connected_peers Number of currently connected peers\n\
             # TYPE qudag_connected_peers gauge\n\
             qudag_connected_peers {}\n\
             \n\
             # HELP qudag_total_connections Total number of connections attempted\n\
             # TYPE qudag_total_connections counter\n\
             qudag_total_connections {}\n\
             \n\
             # HELP qudag_failed_connections Number of failed connection attempts\n\
             # TYPE qudag_failed_connections counter\n\
             qudag_failed_connections {}\n\
             \n\
             # HELP qudag_messages_sent Total number of messages sent\n\
             # TYPE qudag_messages_sent counter\n\
             qudag_messages_sent {}\n\
             \n\
             # HELP qudag_messages_received Total number of messages received\n\
             # TYPE qudag_messages_received counter\n\
             qudag_messages_received {}\n\
             \n\
             # HELP qudag_bytes_sent Total number of bytes sent\n\
             # TYPE qudag_bytes_sent counter\n\
             qudag_bytes_sent {}\n\
             \n\
             # HELP qudag_bytes_received Total number of bytes received\n\
             # TYPE qudag_bytes_received counter\n\
             qudag_bytes_received {}\n\
             \n\
             # HELP qudag_transactions_submitted Total number of transactions submitted\n\
             # TYPE qudag_transactions_submitted counter\n\
             qudag_transactions_submitted {}\n\
             \n\
             # HELP qudag_nodes_validated Total number of nodes validated\n\
             # TYPE qudag_nodes_validated counter\n\
             qudag_nodes_validated {}\n\
             \n\
             # HELP qudag_validation_failures Total number of validation failures\n\
             # TYPE qudag_validation_failures counter\n\
             qudag_validation_failures {}\n\
             \n\
             # HELP qudag_uptime_seconds Node uptime in seconds\n\
             # TYPE qudag_uptime_seconds gauge\n\
             qudag_uptime_seconds {}\n\
             \n\
             # HELP qudag_connection_success_rate Connection success rate\n\
             # TYPE qudag_connection_success_rate gauge\n\
             qudag_connection_success_rate {}\n\
             \n\
             # HELP qudag_validation_success_rate Validation success rate\n\
             # TYPE qudag_validation_success_rate gauge\n\
             qudag_validation_success_rate {}\n",
            snapshot.connected_peers,
            snapshot.total_connections,
            snapshot.failed_connections,
            snapshot.messages_sent,
            snapshot.messages_received,
            snapshot.bytes_sent,
            snapshot.bytes_received,
            snapshot.transactions_submitted,
            snapshot.nodes_validated,
            snapshot.validation_failures,
            snapshot.uptime_seconds,
            snapshot.connection_success_rate,
            snapshot.validation_success_rate,
        )
    }

    fn update_timestamp(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_update.store(now, Ordering::Relaxed);
    }

    fn calculate_connection_success_rate(&self) -> f64 {
        let total = self.total_connections.load(Ordering::Relaxed);
        let failed = self.failed_connections.load(Ordering::Relaxed);

        if total == 0 {
            1.0
        } else {
            (total - failed) as f64 / total as f64
        }
    }

    fn calculate_validation_success_rate(&self) -> f64 {
        let validated = self.nodes_validated.load(Ordering::Relaxed);
        let failed = self.validation_failures.load(Ordering::Relaxed);
        let total = validated + failed;

        if total == 0 {
            1.0
        } else {
            validated as f64 / total as f64
        }
    }
}

impl Default for NetworkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Consensus-specific metrics
#[derive(Debug)]
pub struct ConsensusMetrics {
    rounds_completed: AtomicU64,
    votes_cast: AtomicU64,
    finalizations: AtomicU64,
    consensus_failures: AtomicU64,
    average_round_time: AtomicU64, // in milliseconds
}

impl ConsensusMetrics {
    pub fn new() -> Self {
        Self {
            rounds_completed: AtomicU64::new(0),
            votes_cast: AtomicU64::new(0),
            finalizations: AtomicU64::new(0),
            consensus_failures: AtomicU64::new(0),
            average_round_time: AtomicU64::new(0),
        }
    }

    pub fn round_completed(&self, duration: Duration) {
        self.rounds_completed.fetch_add(1, Ordering::Relaxed);

        // Update average round time using exponential moving average
        let new_time = duration.as_millis() as u64;
        let current_avg = self.average_round_time.load(Ordering::Relaxed);
        let new_avg = if current_avg == 0 {
            new_time
        } else {
            (current_avg * 9 + new_time) / 10 // 90% weight to previous average
        };
        self.average_round_time.store(new_avg, Ordering::Relaxed);
    }

    pub fn vote_cast(&self) {
        self.votes_cast.fetch_add(1, Ordering::Relaxed);
    }

    pub fn node_finalized(&self) {
        self.finalizations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn consensus_failed(&self) {
        self.consensus_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> ConsensusStats {
        ConsensusStats {
            rounds_completed: self.rounds_completed.load(Ordering::Relaxed),
            votes_cast: self.votes_cast.load(Ordering::Relaxed),
            finalizations: self.finalizations.load(Ordering::Relaxed),
            consensus_failures: self.consensus_failures.load(Ordering::Relaxed),
            average_round_time_ms: self.average_round_time.load(Ordering::Relaxed),
        }
    }
}

impl Default for ConsensusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Latency histogram for tracking network latencies
#[derive(Debug)]
struct LatencyHistogram {
    buckets: HashMap<u64, u64>, // bucket_ms -> count
    total_samples: u64,
    sum_ms: u64,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            total_samples: 0,
            sum_ms: 0,
        }
    }

    fn record(&mut self, latency: Duration) {
        let latency_ms = latency.as_millis() as u64;

        // Determine bucket (powers of 2)
        let bucket = if latency_ms == 0 {
            1
        } else {
            latency_ms.next_power_of_two()
        };

        *self.buckets.entry(bucket).or_insert(0) += 1;
        self.total_samples += 1;
        self.sum_ms += latency_ms;
    }

    fn get_stats(&self) -> LatencyStats {
        if self.total_samples == 0 {
            return LatencyStats::default();
        }

        let average_ms = self.sum_ms as f64 / self.total_samples as f64;

        // Calculate percentiles
        let mut sorted_samples = Vec::new();
        for (&bucket, &count) in &self.buckets {
            for _ in 0..count {
                sorted_samples.push(bucket);
            }
        }
        sorted_samples.sort_unstable();

        let p50_idx = (self.total_samples as f64 * 0.5) as usize;
        let p95_idx = (self.total_samples as f64 * 0.95) as usize;
        let p99_idx = (self.total_samples as f64 * 0.99) as usize;

        LatencyStats {
            average_ms,
            p50_ms: sorted_samples.get(p50_idx).copied().unwrap_or(0) as f64,
            p95_ms: sorted_samples.get(p95_idx).copied().unwrap_or(0) as f64,
            p99_ms: sorted_samples.get(p99_idx).copied().unwrap_or(0) as f64,
            max_ms: sorted_samples.last().copied().unwrap_or(0) as f64,
            total_samples: self.total_samples,
        }
    }
}

/// Throughput window for tracking recent throughput
#[derive(Debug)]
struct ThroughputWindow {
    window_size: Duration,
    samples: Vec<ThroughputSample>,
}

#[derive(Debug, Clone)]
struct ThroughputSample {
    timestamp: Instant,
    bytes_sent: usize,
    bytes_received: usize,
}

impl ThroughputWindow {
    fn new() -> Self {
        Self {
            window_size: Duration::from_secs(60), // 1 minute window
            samples: Vec::new(),
        }
    }

    fn record_sent(&mut self, bytes: usize) {
        self.cleanup_old_samples();
        self.samples.push(ThroughputSample {
            timestamp: Instant::now(),
            bytes_sent: bytes,
            bytes_received: 0,
        });
    }

    fn record_received(&mut self, bytes: usize) {
        self.cleanup_old_samples();
        self.samples.push(ThroughputSample {
            timestamp: Instant::now(),
            bytes_sent: 0,
            bytes_received: bytes,
        });
    }

    fn cleanup_old_samples(&mut self) {
        let cutoff = Instant::now() - self.window_size;
        self.samples.retain(|sample| sample.timestamp > cutoff);
    }

    fn get_stats(&self) -> ThroughputStats {
        if self.samples.is_empty() {
            return ThroughputStats::default();
        }

        let total_sent: usize = self.samples.iter().map(|s| s.bytes_sent).sum();
        let total_received: usize = self.samples.iter().map(|s| s.bytes_received).sum();

        let window_secs = self.window_size.as_secs_f64();

        ThroughputStats {
            bytes_per_sec_sent: total_sent as f64 / window_secs,
            bytes_per_sec_received: total_received as f64 / window_secs,
            messages_per_sec: self.samples.len() as f64 / window_secs,
        }
    }
}

/// Metrics snapshot for external consumption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    // Connection metrics
    pub connected_peers: usize,
    pub total_connections: u64,
    pub failed_connections: u64,

    // Message metrics
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,

    // DAG metrics
    pub transactions_submitted: u64,
    pub nodes_validated: u64,
    pub validation_failures: u64,

    // Performance metrics
    pub uptime_seconds: u64,
    pub last_update: u64,

    // Derived metrics
    pub connection_success_rate: f64,
    pub validation_success_rate: f64,

    // Latency and throughput
    pub latency_stats: LatencyStats,
    pub throughput_stats: ThroughputStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub average_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub total_samples: u64,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            average_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
            total_samples: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputStats {
    pub bytes_per_sec_sent: f64,
    pub bytes_per_sec_received: f64,
    pub messages_per_sec: f64,
}

impl Default for ThroughputStats {
    fn default() -> Self {
        Self {
            bytes_per_sec_sent: 0.0,
            bytes_per_sec_received: 0.0,
            messages_per_sec: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusStats {
    pub rounds_completed: u64,
    pub votes_cast: u64,
    pub finalizations: u64,
    pub consensus_failures: u64,
    pub average_round_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_network_metrics() {
        let metrics = NetworkMetrics::new();

        metrics.peer_connected();
        metrics.peer_connected();
        metrics.connection_failed();

        let snapshot = metrics.get_snapshot();
        assert_eq!(snapshot.connected_peers, 2);
        assert_eq!(snapshot.total_connections, 2);
        assert_eq!(snapshot.failed_connections, 1);
        assert_eq!(snapshot.connection_success_rate, 0.5);
    }

    #[test]
    fn test_latency_histogram() {
        let mut histogram = LatencyHistogram::new();

        histogram.record(Duration::from_millis(10));
        histogram.record(Duration::from_millis(20));
        histogram.record(Duration::from_millis(30));

        let stats = histogram.get_stats();
        assert_eq!(stats.total_samples, 3);
        assert_eq!(stats.average_ms, 20.0);
    }

    #[test]
    fn test_consensus_metrics() {
        let metrics = ConsensusMetrics::new();

        metrics.vote_cast();
        metrics.vote_cast();
        metrics.node_finalized();

        let stats = metrics.get_stats();
        assert_eq!(stats.votes_cast, 2);
        assert_eq!(stats.finalizations, 1);
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = NetworkMetrics::new();
        metrics.peer_connected();

        let prometheus = metrics.to_prometheus();
        assert!(prometheus.contains("qudag_connected_peers 1"));
        assert!(prometheus.contains("# TYPE qudag_connected_peers gauge"));
    }

    #[test]
    fn test_throughput_window() {
        let mut window = ThroughputWindow::new();

        window.record_sent(1000);
        window.record_received(500);

        let stats = window.get_stats();
        assert!(stats.bytes_per_sec_sent > 0.0);
        assert!(stats.bytes_per_sec_received > 0.0);
        assert!(stats.messages_per_sec > 0.0);
    }
}

// Comprehensive test suite for Synaptic Neural Mesh CLI

// Unit tests
mod unit;

// Integration tests
mod integration;

// Test utilities and helpers
pub mod test_utils {
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    /// Test fixture for setting up test environments
    pub struct TestFixture {
        pub temp_dir: TempDir,
        pub node_id: String,
        pub config_path: String,
    }

    impl TestFixture {
        pub fn new() -> anyhow::Result<Self> {
            let temp_dir = TempDir::new()?;
            let node_id = Uuid::new_v4().to_string();
            let config_path = temp_dir
                .path()
                .join("config.toml")
                .to_string_lossy()
                .to_string();

            Ok(Self {
                temp_dir,
                node_id,
                config_path,
            })
        }

        pub fn create_test_config(&self) -> anyhow::Result<()> {
            let config = r#"
[node]
id = "test-node"
name = "Test Node"
port = 9090

[p2p]
listen_addresses = ["/ip4/127.0.0.1/tcp/0"]
bootstrap_peers = []

[storage]
path = "./test_data"

[neural]
max_agents = 10
memory_size = 1000
            "#;

            std::fs::write(&self.config_path, config)?;
            Ok(())
        }
    }

    /// Mock QuDAG node for testing
    pub struct MockQuDAGNode {
        pub id: String,
        pub data: Arc<RwLock<Vec<u8>>>,
    }

    impl MockQuDAGNode {
        pub fn new(id: String) -> Self {
            Self {
                id,
                data: Arc::new(RwLock::new(Vec::new())),
            }
        }
    }

    /// Mock Neural node for testing
    pub struct MockNeuralNode {
        pub id: String,
        pub memory: Arc<RwLock<Vec<f32>>>,
    }

    impl MockNeuralNode {
        pub fn new(id: String) -> Self {
            Self {
                id,
                memory: Arc::new(RwLock::new(vec![0.0; 1000])),
            }
        }
    }

    /// Mock Swarm node for testing
    pub struct MockSwarmNode {
        pub id: String,
        pub agents: Arc<RwLock<Vec<String>>>,
    }

    impl MockSwarmNode {
        pub fn new(id: String) -> Self {
            Self {
                id,
                agents: Arc::new(RwLock::new(Vec::new())),
            }
        }
    }

    /// Helper to create test P2P addresses
    pub fn create_test_multiaddr(port: u16) -> String {
        format!("/ip4/127.0.0.1/tcp/{}", port)
    }

    /// Helper to verify command output
    pub fn assert_output_contains(output: &str, expected: &[&str]) {
        for exp in expected {
            assert!(
                output.contains(exp),
                "Expected output to contain '{}', but got: {}",
                exp,
                output
            );
        }
    }

    /// Helper to create test neural network data
    pub fn create_test_neural_data(size: usize) -> Vec<f32> {
        (0..size).map(|i| (i as f32) * 0.1).collect()
    }
}

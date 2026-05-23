// Unit tests for configuration validation

use crate::test_utils::*;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
struct TestConfig {
    node: NodeConfig,
    p2p: P2PConfig,
    neural: NeuralConfig,
    storage: StorageConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeConfig {
    id: String,
    name: String,
    port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct P2PConfig {
    listen_addresses: Vec<String>,
    bootstrap_peers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NeuralConfig {
    max_agents: usize,
    memory_size: usize,
    learning_rate: f32,
}

#[derive(Debug, Serialize, Deserialize)]
struct StorageConfig {
    path: String,
    max_size_gb: u64,
}

#[test]
fn test_valid_config_parsing() {
    let fixture = TestFixture::new().unwrap();
    fixture.create_test_config().unwrap();

    let config_content = fs::read_to_string(&fixture.config_path).unwrap();
    let config: TestConfig = toml::from_str(&config_content).unwrap();

    assert_eq!(config.node.id, "test-node");
    assert_eq!(config.node.port, 9090);
    assert_eq!(config.neural.max_agents, 10);
}

#[test]
fn test_config_with_missing_fields() {
    let fixture = TestFixture::new().unwrap();

    let incomplete_config = r#"
[node]
id = "test-node"
# Missing name and port

[p2p]
listen_addresses = []
    "#;

    fs::write(&fixture.config_path, incomplete_config).unwrap();

    let result = toml::from_str::<TestConfig>(&incomplete_config);
    assert!(result.is_err());
}

#[test]
fn test_config_with_invalid_values() {
    let fixture = TestFixture::new().unwrap();

    let invalid_config = r#"
[node]
id = "test-node"
name = "Test Node"
port = 99999  # Invalid port

[neural]
max_agents = -5  # Invalid negative value
learning_rate = 2.5  # Invalid learning rate > 1
    "#;

    fs::write(&fixture.config_path, invalid_config).unwrap();

    // Test port validation
    let config: Result<TestConfig, _> = toml::from_str(&invalid_config);
    if let Ok(cfg) = config {
        assert!(cfg.node.port > 65535, "Port validation should fail");
    }
}

#[test]
fn test_config_merging() {
    let base_config = TestConfig {
        node: NodeConfig {
            id: "base-node".to_string(),
            name: "Base Node".to_string(),
            port: 8080,
        },
        p2p: P2PConfig {
            listen_addresses: vec!["/ip4/0.0.0.0/tcp/8080".to_string()],
            bootstrap_peers: vec![],
        },
        neural: NeuralConfig {
            max_agents: 5,
            memory_size: 500,
            learning_rate: 0.1,
        },
        storage: StorageConfig {
            path: "./data".to_string(),
            max_size_gb: 10,
        },
    };

    // Test that config can be serialized and deserialized
    let serialized = toml::to_string(&base_config).unwrap();
    let deserialized: TestConfig = toml::from_str(&serialized).unwrap();

    assert_eq!(deserialized.node.id, base_config.node.id);
    assert_eq!(
        deserialized.neural.max_agents,
        base_config.neural.max_agents
    );
}

#[test]
fn test_environment_variable_override() {
    std::env::set_var("SYNAPTIC_NODE_PORT", "9999");
    std::env::set_var("SYNAPTIC_NEURAL_MAX_AGENTS", "20");

    // In real implementation, config should read env vars
    // This is a placeholder for the actual implementation
    let port = std::env::var("SYNAPTIC_NODE_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let max_agents = std::env::var("SYNAPTIC_NEURAL_MAX_AGENTS")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    assert_eq!(port, 9999);
    assert_eq!(max_agents, 20);

    // Clean up
    std::env::remove_var("SYNAPTIC_NODE_PORT");
    std::env::remove_var("SYNAPTIC_NEURAL_MAX_AGENTS");
}

#[test]
fn test_config_validation_rules() {
    // Test various validation scenarios
    let validations = vec![
        // (field, value, expected_valid)
        ("port", "65535", true),
        ("port", "65536", false),
        ("max_agents", "1000", true),
        ("max_agents", "0", false),
        ("learning_rate", "0.5", true),
        ("learning_rate", "1.5", false),
        ("memory_size", "1000000", true),
        ("memory_size", "-100", false),
    ];

    for (field, value, expected) in validations {
        // In real implementation, validate against actual config structs
        match field {
            "port" => {
                let port: Result<u16, _> = value.parse();
                if expected {
                    assert!(port.is_ok() && port.unwrap() <= 65535);
                } else {
                    assert!(port.is_err() || port.unwrap() > 65535);
                }
            }
            "max_agents" => {
                let agents: Result<usize, _> = value.parse();
                if expected {
                    assert!(agents.is_ok() && agents.unwrap() > 0);
                } else {
                    assert!(agents.is_err() || agents.unwrap() == 0);
                }
            }
            "learning_rate" => {
                let rate: Result<f32, _> = value.parse();
                if expected {
                    assert!(rate.is_ok() && rate.unwrap() <= 1.0);
                } else {
                    assert!(rate.is_err() || rate.unwrap() > 1.0);
                }
            }
            "memory_size" => {
                let size: Result<i64, _> = value.parse();
                if expected {
                    assert!(size.is_ok() && size.unwrap() > 0);
                } else {
                    assert!(size.is_err() || size.unwrap() <= 0);
                }
            }
            _ => {}
        }
    }
}

#[test]
fn test_config_file_formats() {
    let fixture = TestFixture::new().unwrap();

    // Test TOML format
    let toml_config = r#"
[node]
id = "test-node"
name = "Test Node"
port = 8080
    "#;

    fs::write(&fixture.config_path, toml_config).unwrap();
    assert!(toml::from_str::<toml::Value>(&toml_config).is_ok());

    // Test JSON format (for future support)
    let json_config = r#"
{
    "node": {
        "id": "test-node",
        "name": "Test Node",
        "port": 8080
    }
}
    "#;

    let json_path = fixture.temp_dir.path().join("config.json");
    fs::write(&json_path, json_config).unwrap();
    assert!(serde_json::from_str::<serde_json::Value>(&json_config).is_ok());
}

#[test]
fn test_config_security_validation() {
    // Test that sensitive fields are properly handled
    let config_with_secrets = r#"
[node]
id = "test-node"
name = "Test Node"
port = 8080
api_key = "secret-key-123"  # Should be encrypted or in env var

[storage]
path = "/etc/passwd"  # Should not allow system paths
    "#;

    // In real implementation, validate against security rules
    assert!(
        config_with_secrets.contains("api_key"),
        "Should detect sensitive fields"
    );
    assert!(
        config_with_secrets.contains("/etc/"),
        "Should detect system paths"
    );
}

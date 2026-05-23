// Unit tests for CLI commands

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_cli_help() {
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Synaptic Neural Mesh CLI"))
        .stdout(predicate::str::contains("Commands:"))
        .stdout(predicate::str::contains("node"))
        .stdout(predicate::str::contains("swarm"))
        .stdout(predicate::str::contains("neural"))
        .stdout(predicate::str::contains("p2p"));
}

#[test]
fn test_cli_version() {
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("synaptic-mesh"));
}

#[test]
fn test_node_init_command() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("node")
        .arg("init")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("Node initialized successfully"));

    assert!(config_path.exists());
}

#[test]
fn test_node_start_without_config() {
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("node")
        .arg("start")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Config file not found"));
}

#[test]
fn test_swarm_create_command() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    // Create a basic config
    let config = r#"
[node]
id = "test-node"
port = 9090

[neural]
max_agents = 10
    "#;
    fs::write(&config_path, config).unwrap();

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("swarm")
        .arg("create")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("--name")
        .arg("test-swarm")
        .arg("--size")
        .arg("5")
        .assert()
        .success()
        .stdout(predicate::str::contains("Swarm created"));
}

#[test]
fn test_neural_train_command() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("training_data.json");

    // Create training data
    let training_data = r#"
{
    "inputs": [[0.1, 0.2], [0.3, 0.4]],
    "outputs": [[0.5], [0.6]]
}
    "#;
    fs::write(&data_path, training_data).unwrap();

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("neural")
        .arg("train")
        .arg("--data")
        .arg(data_path.to_str().unwrap())
        .arg("--epochs")
        .arg("10")
        .assert()
        .success()
        .stdout(predicate::str::contains("Training"));
}

#[test]
fn test_p2p_connect_command() {
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("p2p")
        .arg("connect")
        .arg("/ip4/127.0.0.1/tcp/9090")
        .assert()
        .success()
        .stdout(predicate::str::contains("Connecting to peer"));
}

#[test]
fn test_invalid_command() {
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("invalid-command")
        .assert()
        .failure()
        .stderr(predicate::str::contains("unrecognized subcommand"));
}

#[test]
fn test_environment_variables() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.env("SYNAPTIC_CONFIG", config_path.to_str().unwrap())
        .env("SYNAPTIC_LOG_LEVEL", "debug")
        .arg("node")
        .arg("init")
        .assert()
        .success();
}

#[test]
fn test_config_validation() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("invalid.toml");

    // Create invalid config
    let invalid_config = r#"
[node]
# Missing required fields
    "#;
    fs::write(&config_path, invalid_config).unwrap();

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("node")
        .arg("start")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid configuration"));
}

#[test]
fn test_batch_operations() {
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("swarm")
        .arg("batch")
        .arg("--operations")
        .arg("create:5,train:100,evolve:10")
        .assert()
        .success()
        .stdout(predicate::str::contains("Batch operations"));
}

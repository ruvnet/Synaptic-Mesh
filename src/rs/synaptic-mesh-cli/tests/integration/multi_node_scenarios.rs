// Integration tests for multi-node P2P scenarios

use assert_cmd::Command as AssertCommand;
use std::fs;
use std::process::{Child, Stdio};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

struct TestNode {
    id: String,
    dir: TempDir,
    process: Option<Child>,
    port: u16,
}

impl TestNode {
    fn new(id: &str, port: u16) -> anyhow::Result<Self> {
        let dir = TempDir::new()?;
        Ok(Self {
            id: id.to_string(),
            dir,
            process: None,
            port,
        })
    }

    fn config_path(&self) -> String {
        self.dir
            .path()
            .join("config.toml")
            .to_string_lossy()
            .to_string()
    }

    fn create_config(&self, bootstrap_peers: Vec<String>) -> anyhow::Result<()> {
        let config = format!(
            r#"
[node]
id = "{}"
name = "Test Node {}"
port = {}

[p2p]
listen_addresses = ["/ip4/127.0.0.1/tcp/{}"]
bootstrap_peers = [{}]

[storage]
path = "{}/data"

[neural]
max_agents = 10
memory_size = 1000
"#,
            self.id,
            self.id,
            self.port,
            self.port,
            bootstrap_peers
                .iter()
                .map(|p| format!("\"{}\"", p))
                .collect::<Vec<_>>()
                .join(", "),
            self.dir.path().to_string_lossy()
        );

        fs::write(self.config_path(), config)?;
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        let mut cmd = AssertCommand::cargo_bin("synaptic-mesh")?;
        let inner = cmd.get_program().to_owned();
        let child = std::process::Command::new(inner)
            .arg("node")
            .arg("start")
            .arg("--config")
            .arg(self.config_path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self.process = Some(child);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(mut process) = self.process.take() {
            process.kill()?;
            process.wait()?;
        }
        Ok(())
    }

    fn multiaddr(&self) -> String {
        format!("/ip4/127.0.0.1/tcp/{}", self.port)
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[tokio::test]
async fn test_two_node_connection() {
    // Create two nodes
    let mut node1 = TestNode::new("node-1", 9101).unwrap();
    let mut node2 = TestNode::new("node-2", 9102).unwrap();

    // Configure nodes
    node1.create_config(vec![]).unwrap();
    node2.create_config(vec![node1.multiaddr()]).unwrap();

    // Start nodes
    node1.start().unwrap();
    sleep(Duration::from_secs(1)).await; // Wait for node1 to start
    node2.start().unwrap();
    sleep(Duration::from_secs(2)).await; // Wait for connection

    // Verify connection via CLI
    let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("p2p")
        .arg("peers")
        .arg("--config")
        .arg(node2.config_path())
        .assert()
        .success()
        .stdout(predicates::str::contains("node-1"));
}

#[tokio::test]
async fn test_multi_node_mesh_formation() {
    let num_nodes = 5;
    let mut nodes: Vec<TestNode> = Vec::new();

    // Create nodes
    for i in 0..num_nodes {
        nodes.push(TestNode::new(&format!("mesh-node-{}", i), 9200 + i as u16).unwrap());
    }

    // Configure nodes in mesh topology
    for i in 0..num_nodes {
        let bootstrap_peers: Vec<String> = nodes
            .iter()
            .enumerate()
            .filter(|(j, _)| *j < i)
            .map(|(_, n)| n.multiaddr())
            .collect();

        nodes[i].create_config(bootstrap_peers).unwrap();
    }

    // Start all nodes
    for node in &mut nodes {
        node.start().unwrap();
        sleep(Duration::from_millis(500)).await;
    }

    // Wait for mesh formation
    sleep(Duration::from_secs(3)).await;

    // Verify mesh connectivity
    for node in &nodes {
        let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
        let output = cmd
            .arg("p2p")
            .arg("peers")
            .arg("--config")
            .arg(node.config_path())
            .output()
            .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Should be connected to at least 2 other nodes
        let peer_count = stdout
            .lines()
            .filter(|line| line.contains("mesh-node-"))
            .count();

        assert!(
            peer_count >= 2,
            "Node {} should have at least 2 peers",
            node.id
        );
    }
}

#[tokio::test]
async fn test_node_discovery_and_gossip() {
    let mut nodes: Vec<TestNode> = Vec::new();

    // Create 3 nodes
    for i in 0..3 {
        let mut node = TestNode::new(&format!("gossip-node-{}", i), 9300 + i as u16).unwrap();

        // Bootstrap from first node
        let bootstrap = if i > 0 {
            vec![nodes[0].multiaddr()]
        } else {
            vec![]
        };

        node.create_config(bootstrap).unwrap();
        node.start().unwrap();
        nodes.push(node);

        sleep(Duration::from_secs(1)).await;
    }

    // Publish message from node 0
    let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("p2p")
        .arg("publish")
        .arg("--config")
        .arg(nodes[0].config_path())
        .arg("--topic")
        .arg("test-topic")
        .arg("--message")
        .arg("Hello from node 0!")
        .assert()
        .success();

    // Wait for propagation
    sleep(Duration::from_secs(2)).await;

    // Check that other nodes received the message
    for i in 1..3 {
        let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
        cmd.arg("p2p")
            .arg("messages")
            .arg("--config")
            .arg(nodes[i].config_path())
            .arg("--topic")
            .arg("test-topic")
            .assert()
            .success()
            .stdout(predicates::str::contains("Hello from node 0!"));
    }
}

#[tokio::test]
async fn test_distributed_swarm_coordination() {
    let mut nodes: Vec<TestNode> = Vec::new();

    // Create 4 nodes for distributed swarm
    for i in 0..4 {
        let mut node = TestNode::new(&format!("swarm-node-{}", i), 9400 + i as u16).unwrap();

        let bootstrap = if i > 0 {
            vec![nodes[0].multiaddr()]
        } else {
            vec![]
        };

        node.create_config(bootstrap).unwrap();
        node.start().unwrap();
        nodes.push(node);

        sleep(Duration::from_millis(500)).await;
    }

    // Create distributed swarm from node 0
    let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("swarm")
        .arg("create")
        .arg("--config")
        .arg(nodes[0].config_path())
        .arg("--name")
        .arg("distributed-test")
        .arg("--size")
        .arg("10")
        .arg("--distributed")
        .assert()
        .success();

    sleep(Duration::from_secs(2)).await;

    // Verify swarm is visible from all nodes
    for node in &nodes {
        let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
        cmd.arg("swarm")
            .arg("list")
            .arg("--config")
            .arg(node.config_path())
            .assert()
            .success()
            .stdout(predicates::str::contains("distributed-test"));
    }
}

#[tokio::test]
async fn test_network_partition_recovery() {
    let mut nodes: Vec<TestNode> = Vec::new();

    // Create 3 nodes
    for i in 0..3 {
        let mut node = TestNode::new(&format!("partition-node-{}", i), 9500 + i as u16).unwrap();

        let bootstrap = match i {
            0 => vec![],
            1 => vec![nodes[0].multiaddr()],
            2 => vec![nodes[0].multiaddr(), nodes[1].multiaddr()],
            _ => vec![],
        };

        node.create_config(bootstrap).unwrap();
        node.start().unwrap();
        nodes.push(node);

        sleep(Duration::from_secs(1)).await;
    }

    // Verify initial connectivity
    let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
    let output = cmd
        .arg("p2p")
        .arg("peers")
        .arg("--config")
        .arg(nodes[2].config_path())
        .output()
        .unwrap();

    let initial_peers = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| l.contains("partition-node-"))
        .count();

    assert!(initial_peers >= 2);

    // Simulate partition by stopping node 1
    nodes[1].stop().unwrap();
    sleep(Duration::from_secs(3)).await;

    // Check that nodes 0 and 2 can still communicate
    let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("p2p")
        .arg("ping")
        .arg("--config")
        .arg(nodes[0].config_path())
        .arg("--peer")
        .arg(&nodes[2].multiaddr())
        .assert()
        .success();

    // Restart node 1
    nodes[1].start().unwrap();
    sleep(Duration::from_secs(3)).await;

    // Verify network healed
    let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
    let output = cmd
        .arg("p2p")
        .arg("peers")
        .arg("--config")
        .arg(nodes[2].config_path())
        .output()
        .unwrap();

    let healed_peers = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| l.contains("partition-node-"))
        .count();

    assert_eq!(healed_peers, initial_peers);
}

#[tokio::test]
async fn test_distributed_neural_training() {
    let mut nodes: Vec<TestNode> = Vec::new();

    // Create 3 nodes for distributed training
    for i in 0..3 {
        let mut node = TestNode::new(&format!("neural-node-{}", i), 9600 + i as u16).unwrap();

        let bootstrap = if i > 0 {
            vec![nodes[0].multiaddr()]
        } else {
            vec![]
        };

        node.create_config(bootstrap).unwrap();
        node.start().unwrap();
        nodes.push(node);

        sleep(Duration::from_secs(1)).await;
    }

    // Create training data
    let training_data = r#"
{
    "inputs": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
    "outputs": [[0.3], [0.7], [1.1]]
}
"#;

    let data_path = nodes[0].dir.path().join("training.json");
    fs::write(&data_path, training_data).unwrap();

    // Start distributed training from node 0
    let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("neural")
        .arg("train")
        .arg("--config")
        .arg(nodes[0].config_path())
        .arg("--data")
        .arg(data_path.to_string_lossy().to_string())
        .arg("--distributed")
        .arg("--epochs")
        .arg("10")
        .assert()
        .success();

    sleep(Duration::from_secs(3)).await;

    // Verify model is synchronized across nodes
    for node in &nodes {
        let mut cmd = AssertCommand::cargo_bin("synaptic-mesh").unwrap();
        cmd.arg("neural")
            .arg("status")
            .arg("--config")
            .arg(node.config_path())
            .assert()
            .success()
            .stdout(predicates::str::contains("Model synchronized"));
    }
}

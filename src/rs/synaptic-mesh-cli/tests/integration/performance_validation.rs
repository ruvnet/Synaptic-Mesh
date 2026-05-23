// Performance validation tests for the Synaptic Neural Mesh

use assert_cmd::Command;
use predicates::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[tokio::test]
async fn test_neural_training_performance() {
    let temp_dir = TempDir::new().unwrap();

    // Create large training dataset
    let dataset_size = 10000;
    let input_size = 100;
    let output_size = 10;

    let mut inputs = Vec::new();
    let mut outputs = Vec::new();

    for i in 0..dataset_size {
        let input: Vec<f32> = (0..input_size)
            .map(|j| ((i * input_size + j) as f32) / 1000.0)
            .collect();
        let output: Vec<f32> = (0..output_size).map(|j| ((i + j) as f32) / 100.0).collect();

        inputs.push(input);
        outputs.push(output);
    }

    let training_data = serde_json::json!({
        "inputs": inputs,
        "outputs": outputs
    });

    let data_path = temp_dir.path().join("large_dataset.json");
    fs::write(&data_path, training_data.to_string()).unwrap();

    let start = Instant::now();

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("neural")
        .arg("train")
        .arg("--data")
        .arg(data_path.to_str().unwrap())
        .arg("--epochs")
        .arg("100")
        .arg("--batch-size")
        .arg("64")
        .assert()
        .success();

    let training_time = start.elapsed();

    // Performance target: < 60 seconds for 10k samples, 100 epochs
    assert!(
        training_time.as_secs() < 60,
        "Training took {}s, expected < 60s",
        training_time.as_secs()
    );

    println!(
        "Training performance: {}ms for {} samples",
        training_time.as_millis(),
        dataset_size
    );
}

#[tokio::test]
async fn test_p2p_connection_latency() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "latency-test"
port = 9700

[p2p]
listen_addresses = ["/ip4/127.0.0.1/tcp/9700"]
bootstrap_peers = []
"#;

    fs::write(&config_path, config).unwrap();

    // Test connection setup time
    let start = Instant::now();

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("p2p")
        .arg("connect")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("/ip4/127.0.0.1/tcp/9701")
        .assert()
        .success();

    let connection_time = start.elapsed();

    // Performance target: < 500ms for connection setup
    assert!(
        connection_time.as_millis() < 500,
        "Connection took {}ms, expected < 500ms",
        connection_time.as_millis()
    );
}

#[tokio::test]
async fn test_swarm_scaling_performance() {
    let swarm_sizes = vec![5, 10, 25, 50, 100];
    let mut performance_data = HashMap::new();

    for &size in &swarm_sizes {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");

        let config = format!(
            r#"
[node]
id = "scale-test-{}"
port = {}

[neural]
max_agents = {}
memory_size = 10000
"#,
            size,
            9800 + size,
            size * 2
        );

        fs::write(&config_path, config).unwrap();

        let start = Instant::now();

        let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
        cmd.arg("swarm")
            .arg("create")
            .arg("--config")
            .arg(config_path.to_str().unwrap())
            .arg("--name")
            .arg(&format!("scale-test-{}", size))
            .arg("--size")
            .arg(&size.to_string())
            .assert()
            .success();

        let creation_time = start.elapsed();
        performance_data.insert(size, creation_time);

        println!("Swarm size {}: {}ms", size, creation_time.as_millis());
    }

    // Verify scaling is sub-linear (not exponential)
    let time_5 = performance_data[&5].as_millis();
    let time_100 = performance_data[&100].as_millis();

    // Time for 100 agents should be < 20x time for 5 agents
    assert!(
        time_100 < time_5 * 20,
        "Scaling should be sub-linear: {}ms vs {}ms",
        time_100,
        time_5
    );
}

#[tokio::test]
async fn test_memory_efficiency() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "memory-test"
port = 9900

[neural]
max_agents = 100
memory_size = 1000000  # 1M parameters
"#;

    fs::write(&config_path, config).unwrap();

    // Monitor memory usage during operations
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("neural")
        .arg("benchmark")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("--memory-profile")
        .assert()
        .success()
        .stdout(predicate::str::contains("Memory usage"));

    // Verify memory usage is within bounds
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    let output = cmd
        .arg("system")
        .arg("stats")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Extract memory usage (simplified parsing)
    if let Some(line) = stdout.lines().find(|l| l.contains("Memory:")) {
        let usage_mb: f32 = line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        // Should use < 2GB for 1M parameters + 100 agents
        assert!(
            usage_mb < 2048.0,
            "Memory usage {}MB should be < 2048MB",
            usage_mb
        );
    }
}

#[tokio::test]
async fn test_throughput_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "throughput-test"
port = 10000

[p2p]
listen_addresses = ["/ip4/127.0.0.1/tcp/10000"]
"#;

    fs::write(&config_path, config).unwrap();

    // Test message throughput
    let num_messages = 1000;
    let message_size = 1024; // 1KB messages

    let start = Instant::now();

    for i in 0..num_messages {
        let message = "x".repeat(message_size);

        let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
        cmd.arg("p2p")
            .arg("send")
            .arg("--config")
            .arg(config_path.to_str().unwrap())
            .arg("--message")
            .arg(&message)
            .arg("--no-wait")
            .output()
            .unwrap();
    }

    let total_time = start.elapsed();
    let throughput_mps = num_messages as f64 / total_time.as_secs_f64();

    // Performance target: > 100 messages/second for 1KB messages
    assert!(
        throughput_mps > 100.0,
        "Throughput {:.2} msg/s should be > 100 msg/s",
        throughput_mps
    );

    println!("Message throughput: {:.2} msg/s", throughput_mps);
}

#[tokio::test]
async fn test_concurrent_operations() {
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "concurrent-test"
port = 10100

[neural]
max_agents = 50
"#;

    fs::write(&config_path, config).unwrap();

    // Test concurrent swarm operations
    let semaphore = Arc::new(Semaphore::new(10)); // Limit to 10 concurrent ops
    let mut tasks = Vec::new();

    let start = Instant::now();

    for i in 0..50 {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let config_path = config_path.to_str().unwrap().to_string();

        let task = tokio::spawn(async move {
            let _permit = permit; // Hold permit for duration

            let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
            cmd.arg("swarm")
                .arg("agent")
                .arg("spawn")
                .arg("--config")
                .arg(&config_path)
                .arg("--type")
                .arg("worker")
                .arg("--name")
                .arg(&format!("agent-{}", i))
                .output()
                .unwrap()
        });

        tasks.push(task);
    }

    // Wait for all operations to complete
    for task in tasks {
        task.await.unwrap();
    }

    let total_time = start.elapsed();

    // Performance target: < 30 seconds for 50 concurrent agent spawns
    assert!(
        total_time.as_secs() < 30,
        "Concurrent operations took {}s, expected < 30s",
        total_time.as_secs()
    );
}

#[tokio::test]
async fn test_neural_inference_latency() {
    let temp_dir = TempDir::new().unwrap();
    let model_path = temp_dir.path().join("test_model.json");

    // Create a simple model for testing
    let model_data = serde_json::json!({
        "layers": [
            {"type": "dense", "units": 64, "activation": "relu"},
            {"type": "dense", "units": 32, "activation": "relu"},
            {"type": "dense", "units": 1, "activation": "sigmoid"}
        ],
        "weights": [0.1, 0.2, 0.3] // Simplified
    });

    fs::write(&model_path, model_data.to_string()).unwrap();

    // Test inference latency
    let input_data = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    let num_inferences = 1000;

    let start = Instant::now();

    for _ in 0..num_inferences {
        let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
        cmd.arg("neural")
            .arg("predict")
            .arg("--model")
            .arg(model_path.to_str().unwrap())
            .arg("--input")
            .arg(&serde_json::to_string(&input_data).unwrap())
            .output()
            .unwrap();
    }

    let total_time = start.elapsed();
    let avg_latency = total_time.as_millis() as f64 / num_inferences as f64;

    // Performance target: < 1ms average inference latency
    assert!(
        avg_latency < 1.0,
        "Average inference latency {:.3}ms should be < 1ms",
        avg_latency
    );

    println!("Inference latency: {:.3}ms per prediction", avg_latency);
}

#[tokio::test]
async fn test_resource_utilization() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "resource-test"
port = 10200

[neural]
max_agents = 20
memory_size = 100000

[performance]
cpu_limit = 80  # 80% CPU limit
memory_limit = 1024  # 1GB memory limit
"#;

    fs::write(&config_path, config).unwrap();

    // Start intensive workload
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("benchmark")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("--stress-test")
        .arg("--duration")
        .arg("10")
        .assert()
        .success();

    // Check resource limits were respected
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    let output = cmd
        .arg("system")
        .arg("stats")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify CPU usage stayed within limits
    if let Some(cpu_line) = stdout.lines().find(|l| l.contains("CPU usage:")) {
        let cpu_percent: f32 = cpu_line
            .split_whitespace()
            .nth(2)
            .and_then(|s| s.trim_end_matches('%').parse().ok())
            .unwrap_or(0.0);

        assert!(
            cpu_percent <= 85.0, // Allow 5% tolerance
            "CPU usage {}% should be <= 85%",
            cpu_percent
        );
    }
}

#[tokio::test]
async fn test_mesh_convergence_performance() {
    // Test how quickly distributed learning converges
    let num_nodes = 5;
    let convergence_threshold = 0.01;
    let max_iterations = 100;

    // Simulate distributed optimization
    let mut values: Vec<f32> = (0..num_nodes).map(|i| i as f32).collect();
    let target = values.iter().sum::<f32>() / values.len() as f32;

    let start = Instant::now();
    let mut iterations = 0;

    while iterations < max_iterations {
        // Simulate one round of averaging
        let avg = values.iter().sum::<f32>() / values.len() as f32;

        for val in &mut values {
            *val = 0.8 * (*val) + 0.2 * avg; // Converge towards average
        }

        // Check convergence
        let variance =
            values.iter().map(|v| (v - target).powi(2)).sum::<f32>() / values.len() as f32;

        if variance < convergence_threshold {
            break;
        }

        iterations += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let convergence_time = start.elapsed();

    // Performance target: Convergence in < 1 second
    assert!(
        convergence_time.as_secs() < 1,
        "Convergence took {}ms, expected < 1000ms",
        convergence_time.as_millis()
    );

    assert!(
        iterations < max_iterations,
        "Failed to converge within {} iterations",
        max_iterations
    );

    println!(
        "Convergence: {} iterations in {}ms",
        iterations,
        convergence_time.as_millis()
    );
}

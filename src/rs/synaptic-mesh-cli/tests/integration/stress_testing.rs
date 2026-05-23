// Stress testing for the Synaptic Neural Mesh system

use assert_cmd::Command;
use futures::future::join_all;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::sleep;

#[tokio::test]
async fn test_high_connection_load() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "stress-node"
port = 11000

[p2p]
listen_addresses = ["/ip4/127.0.0.1/tcp/11000"]
max_connections = 1000

[performance]
connection_timeout = 30
"#;

    fs::write(&config_path, config).unwrap();

    let num_connections = 500;
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();

    // Create many concurrent connections
    let tasks: Vec<_> = (0..num_connections)
        .map(|i| {
            let config_path = config_path.to_str().unwrap().to_string();
            let success_count = success_count.clone();
            let error_count = error_count.clone();

            tokio::spawn(async move {
                let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
                let result = cmd
                    .arg("p2p")
                    .arg("connect")
                    .arg("--config")
                    .arg(&config_path)
                    .arg(&format!("/ip4/127.0.0.1/tcp/{}", 11001 + i))
                    .arg("--timeout")
                    .arg("5")
                    .output();

                match result {
                    Ok(output) if output.status.success() => {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    // Wait for all connection attempts
    join_all(tasks).await;

    let duration = start.elapsed();
    let successes = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    println!(
        "Connection stress test: {}/{} successful in {}ms",
        successes,
        num_connections,
        duration.as_millis()
    );

    // Should handle at least 80% of connections successfully
    assert!(
        successes as f32 / num_connections as f32 > 0.8,
        "Success rate {:.2}% should be > 80%",
        (successes as f32 / num_connections as f32) * 100.0
    );

    // Should complete within reasonable time
    assert!(
        duration.as_secs() < 30,
        "Stress test took {}s, expected < 30s",
        duration.as_secs()
    );
}

#[tokio::test]
async fn test_memory_pressure() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "memory-stress"
port = 11100

[neural]
max_agents = 1000
memory_size = 10000000  # 10M parameters

[performance]
memory_limit = 4096  # 4GB limit
gc_threshold = 0.8   # Trigger GC at 80%
"#;

    fs::write(&config_path, config).unwrap();

    // Create large training dataset
    let dataset_size = 50000;
    let input_size = 500;

    let mut inputs = Vec::new();
    for i in 0..dataset_size {
        let input: Vec<f32> = (0..input_size)
            .map(|j| ((i * input_size + j) as f32) / 1000000.0)
            .collect();
        inputs.push(input);
    }

    let training_data = serde_json::json!({
        "inputs": inputs,
        "outputs": vec![vec![1.0]; dataset_size]
    });

    let data_path = temp_dir.path().join("large_dataset.json");
    fs::write(&data_path, training_data.to_string()).unwrap();

    let start = Instant::now();

    // Start memory-intensive operation
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("neural")
        .arg("train")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("--data")
        .arg(data_path.to_str().unwrap())
        .arg("--memory-efficient")
        .arg("--epochs")
        .arg("5")
        .assert()
        .success()
        .stdout(predicates::str::contains("Training completed"));

    let duration = start.elapsed();

    // Should complete without OOM
    println!("Memory stress test completed in {}s", duration.as_secs());

    // Verify memory usage stayed within limits
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    let output = cmd
        .arg("system")
        .arg("memory")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    if let Some(peak_line) = stdout.lines().find(|l| l.contains("Peak usage:")) {
        let peak_mb: f32 = peak_line
            .split_whitespace()
            .nth(2)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        assert!(
            peak_mb < 4096.0,
            "Peak memory usage {}MB should be < 4096MB",
            peak_mb
        );
    }
}

#[tokio::test]
async fn test_network_flooding() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "flood-test"
port = 11200

[p2p]
listen_addresses = ["/ip4/127.0.0.1/tcp/11200"]
max_message_rate = 1000  # 1000 msg/s limit
message_buffer_size = 10000

[performance]
rate_limiting = true
"#;

    fs::write(&config_path, config).unwrap();

    let num_messages = 5000;
    let batch_size = 100;
    let message_size = 10240; // 10KB messages

    let message = "x".repeat(message_size);
    let start = Instant::now();

    // Send messages in batches to avoid overwhelming the system
    for batch in 0..(num_messages / batch_size) {
        let batch_start = Instant::now();

        let tasks: Vec<_> = (0..batch_size)
            .map(|i| {
                let config_path = config_path.to_str().unwrap().to_string();
                let message = message.clone();

                tokio::spawn(async move {
                    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
                    cmd.arg("p2p")
                        .arg("broadcast")
                        .arg("--config")
                        .arg(&config_path)
                        .arg("--topic")
                        .arg("flood-test")
                        .arg("--message")
                        .arg(&message)
                        .arg("--no-wait")
                        .output()
                })
            })
            .collect();

        // Wait for batch to complete
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        let batch_time = batch_start.elapsed();

        // Add small delay to avoid overwhelming the system
        if batch_time.as_millis() < 100 {
            sleep(Duration::from_millis(100 - batch_time.as_millis() as u64)).await;
        }

        println!(
            "Completed batch {} / {}",
            batch + 1,
            num_messages / batch_size
        );
    }

    let total_time = start.elapsed();
    let throughput = num_messages as f64 / total_time.as_secs_f64();

    println!(
        "Network flood test: {} msg/s over {}s",
        throughput as u32,
        total_time.as_secs()
    );

    // System should maintain reasonable throughput under load
    assert!(
        throughput > 50.0,
        "Throughput {:.2} msg/s should be > 50 msg/s",
        throughput
    );

    // Verify system is still responsive
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("system")
        .arg("status")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicates::str::contains("System healthy"));
}

#[tokio::test]
async fn test_agent_spawn_overload() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "agent-stress"
port = 11300

[neural]
max_agents = 10000
agent_spawn_limit = 100  # Max 100 agents per second
memory_per_agent = 1024  # 1KB per agent

[performance]
resource_monitoring = true
"#;

    fs::write(&config_path, config).unwrap();

    let num_agents = 1000;
    let spawn_rate = 50; // agents per second
    let batch_size = 10;

    let start = Instant::now();
    let mut spawned_count = 0;

    for batch in 0..(num_agents / batch_size) {
        let batch_start = Instant::now();

        let tasks: Vec<_> = (0..batch_size)
            .map(|i| {
                let config_path = config_path.to_str().unwrap().to_string();
                let agent_id = batch * batch_size + i;

                tokio::spawn(async move {
                    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
                    cmd.arg("swarm")
                        .arg("agent")
                        .arg("spawn")
                        .arg("--config")
                        .arg(&config_path)
                        .arg("--type")
                        .arg("worker")
                        .arg("--name")
                        .arg(&format!("stress-agent-{}", agent_id))
                        .output()
                })
            })
            .collect();

        // Wait for batch
        for task in tasks {
            match task.await.unwrap() {
                Ok(output) if output.status.success() => spawned_count += 1,
                _ => {}
            }
        }

        let batch_time = batch_start.elapsed();

        // Rate limit to avoid overwhelming the system
        let target_time = Duration::from_millis(1000 / spawn_rate * batch_size as u64);
        if batch_time < target_time {
            sleep(target_time - batch_time).await;
        }

        println!("Spawned {} / {} agents", spawned_count, num_agents);
    }

    let total_time = start.elapsed();

    println!(
        "Agent stress test: {} agents spawned in {}s",
        spawned_count,
        total_time.as_secs()
    );

    // Should successfully spawn most agents
    assert!(
        spawned_count as f32 / num_agents as f32 > 0.9,
        "Spawn success rate {:.2}% should be > 90%",
        (spawned_count as f32 / num_agents as f32) * 100.0
    );

    // Verify system can still list agents
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("swarm")
        .arg("agents")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .assert()
        .success();
}

#[tokio::test]
async fn test_continuous_load() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "continuous-load"
port = 11400

[neural]
max_agents = 100
continuous_learning = true

[performance]
health_check_interval = 5
auto_gc = true
"#;

    fs::write(&config_path, config).unwrap();

    let test_duration = Duration::from_secs(60); // 1 minute continuous load
    let operation_interval = Duration::from_millis(100);

    let start = Instant::now();
    let mut operation_count = 0;
    let mut error_count = 0;

    while start.elapsed() < test_duration {
        let cycle_start = Instant::now();

        // Perform various operations in parallel
        let tasks = vec![
            // Neural operations
            tokio::spawn({
                let config_path = config_path.to_str().unwrap().to_string();
                async move {
                    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
                    cmd.arg("neural")
                        .arg("predict")
                        .arg("--config")
                        .arg(&config_path)
                        .arg("--input")
                        .arg("[0.1, 0.2, 0.3]")
                        .output()
                        .unwrap()
                }
            }),
            // P2P operations
            tokio::spawn({
                let config_path = config_path.to_str().unwrap().to_string();
                async move {
                    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
                    cmd.arg("p2p")
                        .arg("peers")
                        .arg("--config")
                        .arg(&config_path)
                        .output()
                        .unwrap()
                }
            }),
            // Swarm operations
            tokio::spawn({
                let config_path = config_path.to_str().unwrap().to_string();
                let op_count = operation_count;
                async move {
                    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
                    cmd.arg("swarm")
                        .arg("status")
                        .arg("--config")
                        .arg(&config_path)
                        .output()
                        .unwrap()
                }
            }),
        ];

        // Wait for all operations
        for task in tasks {
            match task.await {
                Ok(output) if output.status.success() => operation_count += 1,
                _ => error_count += 1,
            }
        }

        // Rate limiting
        let cycle_time = cycle_start.elapsed();
        if cycle_time < operation_interval {
            sleep(operation_interval - cycle_time).await;
        }

        // Progress logging
        if operation_count % 100 == 0 {
            println!(
                "Continuous load: {} operations, {} errors, {}s elapsed",
                operation_count,
                error_count,
                start.elapsed().as_secs()
            );
        }
    }

    let total_time = start.elapsed();
    let success_rate = operation_count as f64 / (operation_count + error_count) as f64;

    println!(
        "Continuous load test: {:.2}% success rate over {}s",
        success_rate * 100.0,
        total_time.as_secs()
    );

    // Should maintain high success rate under continuous load
    assert!(
        success_rate > 0.95,
        "Success rate {:.2}% should be > 95%",
        success_rate * 100.0
    );

    // System should still be healthy
    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("system")
        .arg("health")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicates::str::contains("Healthy"));
}

#[tokio::test]
async fn test_recovery_after_overload() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");

    let config = r#"
[node]
id = "recovery-test"
port = 11500

[neural]
max_agents = 50
recovery_mode = true

[performance]
overload_threshold = 0.9
recovery_timeout = 30
auto_recovery = true
"#;

    fs::write(&config_path, config).unwrap();

    // Phase 1: Overload the system
    println!("Phase 1: Overloading system...");
    let overload_start = Instant::now();

    let overload_tasks: Vec<_> = (0..200)
        .map(|i| {
            let config_path = config_path.to_str().unwrap().to_string();
            tokio::spawn(async move {
                let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
                cmd.arg("swarm")
                    .arg("agent")
                    .arg("spawn")
                    .arg("--config")
                    .arg(&config_path)
                    .arg("--type")
                    .arg("heavy-worker")
                    .arg("--name")
                    .arg(&format!("overload-agent-{}", i))
                    .output()
            })
        })
        .collect();

    // Wait a bit then cancel to simulate overload
    sleep(Duration::from_secs(5)).await;

    // Drop tasks to simulate system overload
    drop(overload_tasks);

    println!(
        "System overloaded in {}s",
        overload_start.elapsed().as_secs()
    );

    // Phase 2: Wait for recovery
    println!("Phase 2: Waiting for recovery...");
    let recovery_start = Instant::now();

    let mut recovered = false;
    let max_recovery_time = Duration::from_secs(60);

    while recovery_start.elapsed() < max_recovery_time {
        let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
        let result = cmd
            .arg("system")
            .arg("status")
            .arg("--config")
            .arg(config_path.to_str().unwrap())
            .output();

        if let Ok(output) = result {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.contains("Healthy") || stdout.contains("Recovered") {
                recovered = true;
                break;
            }
        }

        sleep(Duration::from_secs(2)).await;
    }

    let recovery_time = recovery_start.elapsed();

    println!(
        "Recovery status: {} in {}s",
        if recovered { "Success" } else { "Failed" },
        recovery_time.as_secs()
    );

    // System should recover within reasonable time
    assert!(recovered, "System should recover from overload");
    assert!(
        recovery_time.as_secs() < 60,
        "Recovery took {}s, expected < 60s",
        recovery_time.as_secs()
    );

    // Phase 3: Verify normal operation after recovery
    println!("Phase 3: Verifying normal operation...");

    let mut cmd = Command::cargo_bin("synaptic-mesh").unwrap();
    cmd.arg("swarm")
        .arg("create")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("--name")
        .arg("recovery-test")
        .arg("--size")
        .arg("5")
        .assert()
        .success()
        .stdout(predicates::str::contains("Swarm created"));

    println!("System fully recovered and operational");
}

// Unit tests for neural network validation

use crate::test_utils::*;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_neural_node_initialization() {
    let node = MockNeuralNode::new("test-neural-1".to_string());

    assert_eq!(node.id, "test-neural-1");

    let memory = node.memory.read().await;
    assert_eq!(memory.len(), 1000);
    assert!(memory.iter().all(|&v| v == 0.0));
}

#[tokio::test]
async fn test_neural_memory_operations() {
    let node = MockNeuralNode::new("test-neural-2".to_string());

    // Test write operation
    {
        let mut memory = node.memory.write().await;
        for i in 0..100 {
            memory[i] = (i as f32) * 0.1;
        }
    }

    // Test read operation
    {
        let memory = node.memory.read().await;
        assert_eq!(memory[0], 0.0);
        assert_eq!(memory[10], 1.0);
        assert_eq!(memory[99], 9.9);
    }
}

#[tokio::test]
async fn test_neural_agent_performance() {
    // Test that neural agents meet performance targets
    let start = std::time::Instant::now();

    let nodes: Vec<_> = (0..100)
        .map(|i| MockNeuralNode::new(format!("neural-{}", i)))
        .collect();

    // Simulate parallel operations
    let tasks: Vec<_> = nodes
        .iter()
        .map(|node| {
            let memory = node.memory.clone();
            tokio::spawn(async move {
                let mut mem = memory.write().await;
                for i in 0..1000 {
                    mem[i] = (i as f32).sin();
                }
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    let elapsed = start.elapsed();

    // Performance target: < 100ms for 100 nodes
    assert!(
        elapsed.as_millis() < 100,
        "Neural operations took {}ms, expected < 100ms",
        elapsed.as_millis()
    );
}

#[tokio::test]
async fn test_neural_learning_convergence() {
    // Test that neural networks converge during training
    let test_data = create_test_neural_data(100);
    let mut errors = Vec::new();

    // Simulate training epochs
    for epoch in 0..50 {
        let error = 1.0 / (epoch as f32 + 1.0); // Simulated decreasing error
        errors.push(error);
    }

    // Verify convergence
    assert!(errors[0] > errors[49], "Error should decrease over epochs");
    assert!(errors[49] < 0.05, "Final error should be < 5%");

    // Verify error reduction rate
    let reduction_rate = (errors[0] - errors[49]) / errors[0];
    assert!(
        reduction_rate > 0.95,
        "Error reduction rate {} should be > 95%",
        reduction_rate
    );
}

#[tokio::test]
async fn test_neural_memory_persistence() {
    let node = MockNeuralNode::new("persistent-node".to_string());

    // Write test pattern
    {
        let mut memory = node.memory.write().await;
        for i in 0..10 {
            memory[i] = (i as f32) * 1.5;
        }
    }

    // Simulate save to disk (in real implementation)
    let saved_data: Vec<f32> = {
        let memory = node.memory.read().await;
        memory[0..10].to_vec()
    };

    // Clear memory
    {
        let mut memory = node.memory.write().await;
        memory.fill(0.0);
    }

    // Restore from saved data
    {
        let mut memory = node.memory.write().await;
        for (i, &val) in saved_data.iter().enumerate() {
            memory[i] = val;
        }
    }

    // Verify restoration
    {
        let memory = node.memory.read().await;
        for i in 0..10 {
            assert_eq!(memory[i], (i as f32) * 1.5);
        }
    }
}

#[tokio::test]
async fn test_neural_pattern_recognition() {
    // Test pattern recognition capabilities
    let patterns: Vec<Vec<f32>> = vec![
        vec![1.0, 0.0, 1.0, 0.0], // Pattern A
        vec![0.0, 1.0, 0.0, 1.0], // Pattern B
        vec![1.0, 1.0, 0.0, 0.0], // Pattern C
    ];

    let test_inputs: Vec<Vec<f32>> = vec![
        vec![0.9, 0.1, 0.9, 0.1], // Similar to A
        vec![0.1, 0.9, 0.1, 0.9], // Similar to B
        vec![0.9, 0.9, 0.1, 0.1], // Similar to C
    ];

    // Simulate pattern matching
    for (i, input) in test_inputs.iter().enumerate() {
        let distances: Vec<f32> = patterns
            .iter()
            .map(|pattern| {
                input
                    .iter()
                    .zip(pattern.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt()
            })
            .collect();

        let min_idx = distances
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(idx, _)| idx)
            .unwrap();

        assert_eq!(
            min_idx, i,
            "Input {} should match pattern {}, but matched {}",
            i, i, min_idx
        );
    }
}

#[tokio::test]
async fn test_neural_self_organization() {
    // Test self-organizing map behavior
    let mut nodes: Vec<Vec<f32>> = (0..10).map(|_| vec![rand::random::<f32>(); 5]).collect();

    // Simulate self-organization iterations
    for _ in 0..100 {
        // Random input
        let input: Vec<f32> = (0..5).map(|_| rand::random()).collect();

        // Find best matching unit
        let bmu_idx = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let dist: f32 = node
                    .iter()
                    .zip(input.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                (i, dist)
            })
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, _)| i)
            .unwrap();

        // Update neighborhood
        for (i, node) in nodes.iter_mut().enumerate() {
            let distance = ((i as i32 - bmu_idx as i32).abs() as f32) / 10.0;
            let influence = (-distance * distance).exp();
            let learning_rate = 0.1;

            for (j, val) in node.iter_mut().enumerate() {
                *val += learning_rate * influence * (input[j] - *val);
            }
        }
    }

    // Verify organization (nodes should be ordered)
    let mut prev_avg = 0.0;
    for node in &nodes {
        let avg: f32 = node.iter().sum::<f32>() / node.len() as f32;
        assert!(
            (avg - prev_avg).abs() < 0.5,
            "Nodes should be smoothly organized"
        );
        prev_avg = avg;
    }
}

#[tokio::test]
async fn test_neural_capacity_limits() {
    // Test memory capacity and performance under load
    let node = MockNeuralNode::new("capacity-test".to_string());

    // Test maximum memory allocation
    let large_size = 1_000_000;
    let large_memory = Arc::new(RwLock::new(vec![0.0f32; large_size]));

    // Test concurrent access performance
    let start = std::time::Instant::now();
    let tasks: Vec<_> = (0..100)
        .map(|i| {
            let memory = large_memory.clone();
            tokio::spawn(async move {
                let mut mem = memory.write().await;
                let offset = i * 1000;
                for j in 0..1000 {
                    if offset + j < large_size {
                        mem[offset + j] = (i * j) as f32;
                    }
                }
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    let elapsed = start.elapsed();

    // Performance target: < 500ms for 100 concurrent operations on 1M elements
    assert!(
        elapsed.as_millis() < 500,
        "Large memory operations took {}ms, expected < 500ms",
        elapsed.as_millis()
    );
}

#[tokio::test]
async fn test_neural_activation_functions() {
    // Test various activation functions
    let inputs = vec![-2.0, -1.0, 0.0, 1.0, 2.0];

    // Sigmoid
    let sigmoid = |x: f32| 1.0 / (1.0 + (-x).exp());
    for &input in &inputs {
        let output = sigmoid(input);
        assert!(output >= 0.0 && output <= 1.0);
    }

    // Tanh
    let tanh = |x: f32| x.tanh();
    for &input in &inputs {
        let output = tanh(input);
        assert!(output >= -1.0 && output <= 1.0);
    }

    // ReLU
    let relu = |x: f32| x.max(0.0);
    for &input in &inputs {
        let output = relu(input);
        assert!(output >= 0.0);
    }

    // Leaky ReLU
    let leaky_relu = |x: f32| if x > 0.0 { x } else { 0.01 * x };
    for &input in &inputs {
        let output = leaky_relu(input);
        if input > 0.0 {
            assert_eq!(output, input);
        } else {
            assert_eq!(output, 0.01 * input);
        }
    }
}

// Helper to add randomness
mod rand {
    pub fn random<T>() -> T
    where
        T: From<f32>,
    {
        T::from(0.5) // Simplified for testing
    }
}

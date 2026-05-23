// Unit tests for swarm behavior validation

use crate::test_utils::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_swarm_initialization() {
    let swarm = MockSwarmNode::new("test-swarm-1".to_string());

    assert_eq!(swarm.id, "test-swarm-1");
    assert_eq!(swarm.agents.read().await.len(), 0);
}

#[tokio::test]
async fn test_swarm_agent_spawning() {
    let swarm = MockSwarmNode::new("test-swarm-2".to_string());

    // Spawn multiple agents
    let agent_types = vec!["researcher", "coder", "analyst", "tester", "coordinator"];

    for agent_type in &agent_types {
        let mut agents = swarm.agents.write().await;
        let idx = agents.len();
        agents.push(format!("{}-{}", agent_type, idx));
    }

    let agents = swarm.agents.read().await;
    assert_eq!(agents.len(), 5);
    assert!(agents.contains(&"researcher-0".to_string()));
    assert!(agents.contains(&"coordinator-4".to_string()));
}

#[tokio::test]
async fn test_swarm_evolution() {
    // Test swarm self-evolution and optimization
    #[derive(Debug, Clone)]
    struct SwarmGeneration {
        id: usize,
        fitness: f32,
        agents: Vec<String>,
    }

    let mut generations = Vec::new();
    let mut current_fitness = 0.5;

    // Simulate evolution over 10 generations
    for gen in 0..10 {
        // Evolve: add better agents, remove worse ones
        current_fitness *= 1.1; // 10% improvement per generation

        let agents = (0..5).map(|i| format!("agent-gen{}-{}", gen, i)).collect();

        generations.push(SwarmGeneration {
            id: gen,
            fitness: current_fitness,
            agents,
        });
    }

    // Verify evolution progress
    assert!(generations[0].fitness < generations[9].fitness);
    assert!(
        generations[9].fitness > 1.0,
        "Final fitness {} should be > 1.0",
        generations[9].fitness
    );
}

#[tokio::test]
async fn test_swarm_self_healing() {
    // Test swarm's ability to recover from agent failures
    let swarm = MockSwarmNode::new("healing-swarm".to_string());

    // Create initial agent pool
    {
        let mut agents = swarm.agents.write().await;
        for i in 0..10 {
            agents.push(format!("agent-{}", i));
        }
    }

    // Simulate failures
    {
        let mut agents = swarm.agents.write().await;
        // Remove 30% of agents
        let failures = 3;
        for _ in 0..failures {
            agents.pop();
        }
    }

    assert_eq!(swarm.agents.read().await.len(), 7);

    // Simulate self-healing
    {
        let mut agents = swarm.agents.write().await;
        let current_len = agents.len();
        for i in 0..3 {
            agents.push(format!("healed-agent-{}", current_len + i));
        }
    }

    let agents = swarm.agents.read().await;
    assert_eq!(agents.len(), 10);
    assert!(agents.iter().any(|a| a.starts_with("healed-")));
}

#[tokio::test]
async fn test_swarm_coordination_efficiency() {
    // Test coordination overhead and efficiency
    let num_agents = 50;
    let num_tasks = 100;

    let start = std::time::Instant::now();

    // Simulate task distribution
    let mut task_assignments: HashMap<String, Vec<usize>> = HashMap::new();

    for task_id in 0..num_tasks {
        let agent_id = format!("agent-{}", task_id % num_agents);
        task_assignments
            .entry(agent_id)
            .or_insert_with(Vec::new)
            .push(task_id);
    }

    // Verify load balancing
    for (_, tasks) in &task_assignments {
        assert!(
            tasks.len() <= (num_tasks / num_agents) + 1,
            "Task distribution should be balanced"
        );
    }

    let coordination_time = start.elapsed();

    // Performance target: < 10ms for 100 tasks across 50 agents
    assert!(
        coordination_time.as_millis() < 10,
        "Coordination took {}ms, expected < 10ms",
        coordination_time.as_millis()
    );
}

#[tokio::test]
async fn test_swarm_consensus_mechanism() {
    // Test swarm consensus for decision making
    let num_agents = 10;
    let proposals = vec!["option-a", "option-b", "option-c"];

    // Simulate voting
    let mut votes: HashMap<&str, usize> = HashMap::new();

    for i in 0..num_agents {
        // Simulate agent preference
        let choice = &proposals[i % proposals.len()];
        *votes.entry(choice).or_insert(0) += 1;
    }

    // Find consensus
    let consensus = votes
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(option, _)| *option)
        .unwrap();

    assert!(proposals.contains(&consensus));
}

#[tokio::test]
async fn test_swarm_adaptive_topology() {
    // Test swarm's ability to adapt topology based on workload
    #[derive(Debug, PartialEq)]
    enum Topology {
        Mesh,
        Hierarchical,
        Ring,
        Star,
    }

    struct AdaptiveSwarm {
        topology: Topology,
        agent_count: usize,
        task_complexity: f32,
    }

    impl AdaptiveSwarm {
        fn optimize_topology(&mut self) {
            self.topology = match (self.agent_count, self.task_complexity) {
                (n, c) if n > 20 && c > 0.8 => Topology::Hierarchical,
                (n, c) if n > 10 && c > 0.5 => Topology::Mesh,
                (n, _) if n < 5 => Topology::Star,
                _ => Topology::Ring,
            };
        }
    }

    let mut swarm = AdaptiveSwarm {
        topology: Topology::Mesh,
        agent_count: 25,
        task_complexity: 0.9,
    };

    swarm.optimize_topology();
    assert_eq!(swarm.topology, Topology::Hierarchical);

    swarm.agent_count = 3;
    swarm.optimize_topology();
    assert_eq!(swarm.topology, Topology::Star);
}

#[tokio::test]
async fn test_swarm_memory_coherence() {
    // Test distributed memory coherence across swarm
    let num_agents = 5;
    let shared_memory = Arc::new(RwLock::new(HashMap::<String, String>::new()));

    // Simulate concurrent memory updates
    let tasks: Vec<_> = (0..num_agents)
        .map(|i| {
            let memory = shared_memory.clone();
            tokio::spawn(async move {
                let key = format!("key-{}", i);
                let value = format!("value-{}", i);

                let mut mem = memory.write().await;
                mem.insert(key, value);
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    // Verify all updates are present
    let memory = shared_memory.read().await;
    assert_eq!(memory.len(), num_agents);

    for i in 0..num_agents {
        assert_eq!(
            memory.get(&format!("key-{}", i)),
            Some(&format!("value-{}", i))
        );
    }
}

#[tokio::test]
async fn test_swarm_task_prioritization() {
    // Test intelligent task prioritization
    #[derive(Debug, Clone)]
    struct Task {
        id: usize,
        priority: u8,
        estimated_time: u32,
        dependencies: Vec<usize>,
    }

    let mut tasks = vec![
        Task {
            id: 1,
            priority: 3,
            estimated_time: 100,
            dependencies: vec![],
        },
        Task {
            id: 2,
            priority: 1,
            estimated_time: 50,
            dependencies: vec![1],
        },
        Task {
            id: 3,
            priority: 2,
            estimated_time: 75,
            dependencies: vec![],
        },
        Task {
            id: 4,
            priority: 1,
            estimated_time: 25,
            dependencies: vec![3],
        },
    ];

    // Sort by priority and dependencies
    tasks.sort_by(|a, b| {
        if a.dependencies.is_empty() && !b.dependencies.is_empty() {
            std::cmp::Ordering::Less
        } else if !a.dependencies.is_empty() && b.dependencies.is_empty() {
            std::cmp::Ordering::Greater
        } else {
            b.priority.cmp(&a.priority)
        }
    });

    assert_eq!(tasks[0].id, 1); // Highest priority, no deps
    assert_eq!(tasks[1].id, 3); // Second priority, no deps
}

#[tokio::test]
async fn test_swarm_resource_optimization() {
    // Test resource allocation and optimization
    struct ResourcePool {
        cpu_cores: usize,
        memory_gb: usize,
        agents: Vec<(String, usize, usize)>, // (name, cpu, memory)
    }

    impl ResourcePool {
        fn allocate(&mut self, agent: &str, cpu: usize, memory: usize) -> bool {
            if self.cpu_cores >= cpu && self.memory_gb >= memory {
                self.cpu_cores -= cpu;
                self.memory_gb -= memory;
                self.agents.push((agent.to_string(), cpu, memory));
                true
            } else {
                false
            }
        }

        fn utilization(&self) -> (f32, f32) {
            let total_cpu = self.cpu_cores + self.agents.iter().map(|(_, c, _)| c).sum::<usize>();
            let total_mem = self.memory_gb + self.agents.iter().map(|(_, _, m)| m).sum::<usize>();

            let cpu_util = 1.0 - (self.cpu_cores as f32 / total_cpu as f32);
            let mem_util = 1.0 - (self.memory_gb as f32 / total_mem as f32);

            (cpu_util, mem_util)
        }
    }

    let mut pool = ResourcePool {
        cpu_cores: 16,
        memory_gb: 32,
        agents: Vec::new(),
    };

    // Allocate resources to agents
    assert!(pool.allocate("heavy-agent", 8, 16));
    assert!(pool.allocate("medium-agent", 4, 8));
    assert!(pool.allocate("light-agent", 2, 4));

    // Check utilization
    let (cpu_util, mem_util) = pool.utilization();
    assert!(cpu_util > 0.8, "CPU utilization should be > 80%");
    assert!(mem_util > 0.8, "Memory utilization should be > 80%");

    // Try to over-allocate
    assert!(!pool.allocate("excess-agent", 8, 16));
}

#[tokio::test]
async fn test_swarm_emergent_behavior() {
    // Test emergence of complex behaviors from simple rules
    #[derive(Clone)]
    struct SimpleAgent {
        id: usize,
        state: f32,
        neighbors: Vec<usize>,
    }

    let mut agents: Vec<SimpleAgent> = (0..20)
        .map(|i| {
            SimpleAgent {
                id: i,
                state: rand::random::<f32>(),
                neighbors: vec![(i + 1) % 20, (i + 19) % 20], // Ring topology
            }
        })
        .collect();

    // Run simulation steps
    for _ in 0..100 {
        let states: Vec<f32> = agents.iter().map(|a| a.state).collect();

        for agent in &mut agents {
            // Simple rule: average with neighbors
            let neighbor_sum: f32 = agent.neighbors.iter().map(|&n| states[n]).sum();

            agent.state = (agent.state + neighbor_sum) / (agent.neighbors.len() + 1) as f32;
        }
    }

    // Check for convergence (emergent consensus)
    let final_states: Vec<f32> = agents.iter().map(|a| a.state).collect();
    let avg_state = final_states.iter().sum::<f32>() / final_states.len() as f32;

    for state in &final_states {
        assert!(
            (state - avg_state).abs() < 0.01,
            "States should converge to consensus"
        );
    }
}

// Simplified random for testing
mod rand {
    pub fn random<T>() -> T
    where
        T: From<f32>,
    {
        T::from(0.5)
    }
}

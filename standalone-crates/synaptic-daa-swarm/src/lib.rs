//! Synaptic DAA Swarm - Distributed Autonomous Agent swarm intelligence
//!
//! This crate provides swarm intelligence capabilities for distributed
//! autonomous agents in the Synaptic Neural Mesh ecosystem.

use std::sync::Arc;
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use synaptic_neural_mesh::{Agent, NeuralMesh, Task, TaskRequirements};
use synaptic_qudag_core::QuDAGNode;

/// Swarm behaviors
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SwarmBehavior {
    Flocking,
    Foraging,
    Exploration,
    Consensus,
    Optimization,
}

/// Swarm intelligence coordinator
pub struct Swarm {
    id: Uuid,
    agents: Arc<DashMap<Uuid, SwarmAgent>>,
    behaviors: Arc<RwLock<Vec<SwarmBehavior>>>,
    mesh: Arc<NeuralMesh>,
    state: Arc<RwLock<SwarmState>>,
}

/// Individual swarm agent
#[derive(Debug, Clone)]
pub struct SwarmAgent {
    pub id: Uuid,
    pub position: Vector3,
    pub velocity: Vector3,
    pub fitness: f64,
    pub memory: Vec<f64>,
}

/// 3D vector for spatial positioning
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Vector3 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Vector3 {
    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }
    
    pub fn zero() -> Self {
        Self { x: 0.0, y: 0.0, z: 0.0 }
    }
    
    pub fn distance(&self, other: &Self) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }
    
    pub fn normalize(&mut self) {
        let mag = (self.x * self.x + self.y * self.y + self.z * self.z).sqrt();
        if mag > 0.0 {
            self.x /= mag;
            self.y /= mag;
            self.z /= mag;
        }
    }
}

/// Swarm state
#[derive(Debug, Clone)]
pub struct SwarmState {
    pub iteration: usize,
    pub global_best: Option<Solution>,
    pub convergence: f64,
}

/// Solution representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Solution {
    pub position: Vector3,
    pub fitness: f64,
    pub data: Vec<f64>,
}

impl Swarm {
    /// Create a new swarm
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            agents: Arc::new(DashMap::new()),
            behaviors: Arc::new(RwLock::new(Vec::new())),
            mesh: Arc::new(NeuralMesh::new()),
            state: Arc::new(RwLock::new(SwarmState {
                iteration: 0,
                global_best: None,
                convergence: 0.0,
            })),
        }
    }
    
    /// Add a behavior to the swarm
    pub fn add_behavior(&self, behavior: SwarmBehavior) {
        self.behaviors.write().push(behavior);
    }
    
    /// Initialize swarm with agents
    pub async fn initialize(&self, agent_count: usize) {
        let mut rng = rand::thread_rng();
        
        for _ in 0..agent_count {
            let agent = SwarmAgent {
                id: Uuid::new_v4(),
                position: Vector3::new(
                    rng.gen_range(-100.0..100.0),
                    rng.gen_range(-100.0..100.0),
                    rng.gen_range(-100.0..100.0),
                ),
                velocity: Vector3::zero(),
                fitness: 0.0,
                memory: vec![0.0; 10],
            };
            
            // Register with neural mesh
            let mesh_agent = Agent::new(format!("swarm-agent-{}", agent.id));
            self.mesh.add_agent(mesh_agent).await.ok();
            
            self.agents.insert(agent.id, agent);
        }
    }
    
    /// Run swarm simulation
    pub async fn run(&self) {
        loop {
            self.update_iteration().await;
            
            // Check convergence
            if self.state.read().convergence > 0.95 {
                break;
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
    
    /// Update one iteration of the swarm
    async fn update_iteration(&self) {
        let behaviors = self.behaviors.read().clone();
        
        for behavior in &behaviors {
            match behavior {
                SwarmBehavior::Flocking => self.apply_flocking().await,
                SwarmBehavior::Foraging => self.apply_foraging().await,
                SwarmBehavior::Exploration => self.apply_exploration().await,
                SwarmBehavior::Consensus => self.apply_consensus().await,
                SwarmBehavior::Optimization => self.apply_optimization().await,
            }
        }
        
        // Update global state
        self.update_global_state().await;
        
        // Increment iteration
        self.state.write().iteration += 1;
    }
    
    /// Apply flocking behavior
    async fn apply_flocking(&self) {
        let agents: Vec<SwarmAgent> = self.agents.iter().map(|a| a.clone()).collect();
        
        for mut agent_ref in self.agents.iter_mut() {
            let agent = agent_ref.value_mut();
            let mut separation = Vector3::zero();
            let mut alignment = Vector3::zero();
            let mut cohesion = Vector3::zero();
            let mut count = 0;
            
            for other in &agents {
                if other.id != agent.id {
                    let distance = agent.position.distance(&other.position);
                    
                    if distance < 50.0 {
                        // Separation
                        if distance < 10.0 {
                            let mut diff = Vector3 {
                                x: agent.position.x - other.position.x,
                                y: agent.position.y - other.position.y,
                                z: agent.position.z - other.position.z,
                            };
                            diff.normalize();
                            separation.x += diff.x;
                            separation.y += diff.y;
                            separation.z += diff.z;
                        }
                        
                        // Alignment
                        alignment.x += other.velocity.x;
                        alignment.y += other.velocity.y;
                        alignment.z += other.velocity.z;
                        
                        // Cohesion
                        cohesion.x += other.position.x;
                        cohesion.y += other.position.y;
                        cohesion.z += other.position.z;
                        
                        count += 1;
                    }
                }
            }
            
            if count > 0 {
                // Apply forces
                agent.velocity.x += separation.x * 0.1 + alignment.x * 0.05 + cohesion.x * 0.01;
                agent.velocity.y += separation.y * 0.1 + alignment.y * 0.05 + cohesion.y * 0.01;
                agent.velocity.z += separation.z * 0.1 + alignment.z * 0.05 + cohesion.z * 0.01;
                
                // Update position
                agent.position.x += agent.velocity.x;
                agent.position.y += agent.velocity.y;
                agent.position.z += agent.velocity.z;
            }
        }
    }
    
    /// Apply foraging behavior
    async fn apply_foraging(&self) {
        // Implement foraging logic
        for mut agent in self.agents.iter_mut() {
            // Simple random walk for now
            let mut rng = rand::thread_rng();
            agent.velocity.x += rng.gen_range(-1.0..1.0);
            agent.velocity.y += rng.gen_range(-1.0..1.0);
            agent.velocity.z += rng.gen_range(-1.0..1.0);
            
            agent.position.x += agent.velocity.x * 0.1;
            agent.position.y += agent.velocity.y * 0.1;
            agent.position.z += agent.velocity.z * 0.1;
        }
    }
    
    /// Apply exploration behavior
    async fn apply_exploration(&self) {
        // Implement exploration logic
        let mut rng = rand::thread_rng();
        
        for mut agent in self.agents.iter_mut() {
            // Levy flight pattern
            if rng.gen::<f64>() < 0.1 {
                agent.velocity.x = rng.gen_range(-10.0..10.0);
                agent.velocity.y = rng.gen_range(-10.0..10.0);
                agent.velocity.z = rng.gen_range(-10.0..10.0);
            }
        }
    }
    
    /// Apply consensus behavior
    async fn apply_consensus(&self) {
        // Calculate average position
        let agents: Vec<SwarmAgent> = self.agents.iter().map(|a| a.clone()).collect();
        let count = agents.len() as f64;
        
        if count > 0.0 {
            let mut avg_x = 0.0;
            let mut avg_y = 0.0;
            let mut avg_z = 0.0;
            
            for agent in &agents {
                avg_x += agent.position.x;
                avg_y += agent.position.y;
                avg_z += agent.position.z;
            }
            
            avg_x /= count;
            avg_y /= count;
            avg_z /= count;
            
            // Move towards consensus
            for mut agent in self.agents.iter_mut() {
                agent.velocity.x += (avg_x - agent.position.x) * 0.01;
                agent.velocity.y += (avg_y - agent.position.y) * 0.01;
                agent.velocity.z += (avg_z - agent.position.z) * 0.01;
            }
        }
    }
    
    /// Apply optimization behavior
    async fn apply_optimization(&self) {
        // Particle swarm optimization
        let global_best = self.state.read().global_best.clone();
        
        for mut agent in self.agents.iter_mut() {
            if let Some(ref best) = global_best {
                // Update velocity towards global best
                agent.velocity.x += (best.position.x - agent.position.x) * 0.1;
                agent.velocity.y += (best.position.y - agent.position.y) * 0.1;
                agent.velocity.z += (best.position.z - agent.position.z) * 0.1;
            }
            
            // Update fitness (example: distance from origin)
            agent.fitness = 1.0 / (1.0 + agent.position.distance(&Vector3::zero()));
        }
    }
    
    /// Update global swarm state
    async fn update_global_state(&self) {
        let agents: Vec<SwarmAgent> = self.agents.iter().map(|a| a.clone()).collect();
        
        // Find best solution
        if let Some(best_agent) = agents.iter().max_by(|a, b| a.fitness.total_cmp(&b.fitness)) {
            let solution = Solution {
                position: best_agent.position,
                fitness: best_agent.fitness,
                data: best_agent.memory.clone(),
            };
            
            let mut state = self.state.write();
            
            // Update global best
            if state.global_best.is_none() || solution.fitness > state.global_best.as_ref().unwrap().fitness {
                state.global_best = Some(solution);
            }
            
            // Calculate convergence
            let fitness_variance = agents.iter()
                .map(|a| (a.fitness - best_agent.fitness).powi(2))
                .sum::<f64>() / agents.len() as f64;
            
            state.convergence = 1.0 / (1.0 + fitness_variance);
        }
    }
    
    /// Get swarm statistics
    pub fn get_stats(&self) -> SwarmStats {
        let state = self.state.read();
        SwarmStats {
            agent_count: self.agents.len(),
            iteration: state.iteration,
            convergence: state.convergence,
            best_fitness: state.global_best.as_ref().map(|s| s.fitness).unwrap_or(0.0),
        }
    }
}

impl Default for Swarm {
    fn default() -> Self {
        Self::new()
    }
}

/// Swarm statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmStats {
    pub agent_count: usize,
    pub iteration: usize,
    pub convergence: f64,
    pub best_fitness: f64,
}

/// Evolutionary algorithm trait
#[async_trait]
pub trait EvolutionaryAlgorithm {
    /// Initialize population
    async fn initialize_population(&mut self, size: usize);
    
    /// Evaluate fitness
    async fn evaluate_fitness(&mut self);
    
    /// Select parents
    async fn selection(&mut self) -> Vec<Uuid>;
    
    /// Crossover operation
    async fn crossover(&mut self, parent1: Uuid, parent2: Uuid) -> SwarmAgent;
    
    /// Mutation operation
    async fn mutation(&mut self, agent: &mut SwarmAgent);
    
    /// Evolution step
    async fn evolve(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_swarm_creation() {
        let swarm = Swarm::new();
        swarm.initialize(10).await;
        
        let stats = swarm.get_stats();
        assert_eq!(stats.agent_count, 10);
        assert_eq!(stats.iteration, 0);
    }
    
    #[tokio::test]
    async fn test_vector_operations() {
        let mut v1 = Vector3::new(3.0, 4.0, 0.0);
        let v2 = Vector3::new(0.0, 0.0, 0.0);
        
        assert_eq!(v1.distance(&v2), 5.0);
        
        v1.normalize();
        assert!((v1.x - 0.6).abs() < 0.001);
        assert!((v1.y - 0.8).abs() < 0.001);
    }
    
    #[tokio::test]
    async fn test_swarm_behaviors() {
        let swarm = Swarm::new();
        swarm.add_behavior(SwarmBehavior::Flocking);
        swarm.add_behavior(SwarmBehavior::Optimization);
        
        swarm.initialize(5).await;
        swarm.update_iteration().await;
        
        let stats = swarm.get_stats();
        assert_eq!(stats.iteration, 1);
    }
}
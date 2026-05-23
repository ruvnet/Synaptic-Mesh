//! Coordinator for distributed task management

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::{
    cognition::{AssignmentStrategy, CognitionTask, Priority, TaskAssignment},
    NeuralMeshError, Result,
};

/// Mesh coordinator that manages task distribution
#[derive(Debug)]
pub struct MeshCoordinator {
    strategy: CoordinationStrategy,
    agents: Arc<RwLock<HashMap<Uuid, AgentInfo>>>,
    task_queue: Arc<RwLock<TaskQueue>>,
    assignments: Arc<RwLock<HashMap<Uuid, TaskAssignment>>>,
    stats: Arc<RwLock<CoordinatorStats>>,
    task_tx: mpsc::UnboundedSender<CoordinationTask>,
}

impl MeshCoordinator {
    /// Create a new coordinator
    pub async fn new(strategy: CoordinationStrategy) -> Result<Self> {
        let (task_tx, task_rx) = mpsc::unbounded_channel();

        let coordinator = Self {
            strategy,
            agents: Arc::new(RwLock::new(HashMap::new())),
            task_queue: Arc::new(RwLock::new(TaskQueue::new())),
            assignments: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(CoordinatorStats::default())),
            task_tx,
        };

        // Spawn task processing
        let coordinator_clone = coordinator.clone();
        tokio::spawn(async move {
            coordinator_clone.process_coordination_tasks(task_rx).await;
        });

        Ok(coordinator)
    }

    /// Start the coordinator
    pub async fn start(&self) -> Result<()> {
        tracing::info!(
            "Starting mesh coordinator with strategy: {:?}",
            self.strategy
        );
        Ok(())
    }

    /// Stop the coordinator
    pub async fn stop(&self) -> Result<()> {
        tracing::info!("Stopping mesh coordinator");
        Ok(())
    }

    /// Register an agent
    pub async fn register_agent(&self, agent_id: Uuid, capabilities: Vec<String>) -> Result<()> {
        let mut agents = self.agents.write().await;

        let info = AgentInfo {
            id: agent_id,
            capabilities,
            current_load: 0.0,
            max_load: 1.0,
            task_count: 0,
            performance_score: 1.0,
            status: AgentStatus::Available,
        };

        agents.insert(agent_id, info);

        tracing::info!("Registered agent {} with coordinator", agent_id);
        Ok(())
    }

    /// Unregister an agent
    pub async fn unregister_agent(&self, agent_id: Uuid) -> Result<()> {
        let mut agents = self.agents.write().await;
        agents.remove(&agent_id);

        // Reassign any tasks from this agent
        let mut assignments = self.assignments.write().await;
        let tasks_to_reassign: Vec<Uuid> = assignments
            .iter()
            .filter(|(_, assignment)| assignment.agent_ids.contains(&agent_id))
            .map(|(task_id, _)| *task_id)
            .collect();

        for task_id in tasks_to_reassign {
            self.task_tx
                .send(CoordinationTask::Reassign(task_id))
                .map_err(|_| {
                    NeuralMeshError::Communication("Failed to send reassignment".to_string())
                })?;
        }

        tracing::info!("Unregistered agent {} from coordinator", agent_id);
        Ok(())
    }

    /// Assign a task to agents
    pub async fn assign_task(&self, task: CognitionTask) -> Result<TaskAssignment> {
        let agents = self.agents.read().await;

        // Select agents based on strategy
        let selected_agents = match &self.strategy {
            CoordinationStrategy::Adaptive => self.select_agents_adaptive(&task, &agents).await?,
            CoordinationStrategy::RoundRobin => {
                self.select_agents_round_robin(&task, &agents).await?
            }
            CoordinationStrategy::LoadBalanced => {
                self.select_agents_load_balanced(&task, &agents).await?
            }
            CoordinationStrategy::CapabilityBased => {
                self.select_agents_capability_based(&task, &agents).await?
            }
            CoordinationStrategy::Consensus { min_agents } => {
                self.select_agents_consensus(&task, &agents, *min_agents)
                    .await?
            }
        };

        // Create assignment
        let assignment = TaskAssignment {
            task_id: task.id,
            agent_ids: selected_agents.clone(),
            strategy: self.get_assignment_strategy(),
        };

        // Store assignment
        {
            let mut assignments = self.assignments.write().await;
            assignments.insert(task.id, assignment.clone());
        }

        // Update agent loads
        {
            let mut agents_mut = self.agents.write().await;
            for agent_id in &selected_agents {
                if let Some(agent) = agents_mut.get_mut(agent_id) {
                    agent.current_load += 0.1; // Task weight
                    agent.task_count += 1;
                }
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_assignments += 1;
        }

        tracing::debug!(
            "Assigned task {} to {} agents",
            task.id,
            selected_agents.len()
        );
        Ok(assignment)
    }

    /// Get coordinator statistics
    pub async fn get_stats(&self) -> CoordinatorStats {
        let stats = self.stats.read().await;
        let agents = self.agents.read().await;

        let total_load: f64 = agents.values().map(|a| a.current_load).sum();
        let avg_load = if agents.is_empty() {
            0.0
        } else {
            total_load / agents.len() as f64
        };

        CoordinatorStats {
            load_factor: avg_load,
            total_assignments: stats.total_assignments,
            active_agents: agents
                .values()
                .filter(|a| a.status == AgentStatus::Available)
                .count(),
            queued_tasks: self.task_queue.blocking_read().len(),
        }
    }

    /// Select agents using adaptive strategy
    async fn select_agents_adaptive(
        &self,
        task: &CognitionTask,
        agents: &HashMap<Uuid, AgentInfo>,
    ) -> Result<Vec<Uuid>> {
        // Adaptive selection based on task type and agent performance
        let mut candidates: Vec<(&Uuid, f64)> = agents
            .iter()
            .filter(|(_, agent)| agent.status == AgentStatus::Available)
            .map(|(id, agent)| {
                let capability_score = agent
                    .capabilities
                    .iter()
                    .filter(|cap| task.context.contains_key(*cap))
                    .count() as f64;
                let load_score = 1.0 - agent.current_load;
                let performance_score = agent.performance_score;

                let total_score =
                    capability_score * 0.4 + load_score * 0.3 + performance_score * 0.3;
                (id, total_score)
            })
            .collect();

        // Sort by score
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // Select top agents based on task priority
        let num_agents = match task.priority {
            Priority::Critical => 5.min(candidates.len()),
            Priority::High => 3.min(candidates.len()),
            Priority::Medium => 2.min(candidates.len()),
            Priority::Low => 1.min(candidates.len()),
        };

        if num_agents == 0 {
            return Err(NeuralMeshError::NotFound("No available agents".to_string()));
        }

        Ok(candidates
            .into_iter()
            .take(num_agents)
            .map(|(id, _)| *id)
            .collect())
    }

    /// Select agents using round-robin strategy
    async fn select_agents_round_robin(
        &self,
        _task: &CognitionTask,
        agents: &HashMap<Uuid, AgentInfo>,
    ) -> Result<Vec<Uuid>> {
        let available_agents: Vec<Uuid> = agents
            .iter()
            .filter(|(_, agent)| agent.status == AgentStatus::Available)
            .map(|(id, _)| *id)
            .collect();

        if available_agents.is_empty() {
            return Err(NeuralMeshError::NotFound("No available agents".to_string()));
        }

        // Simple round-robin: take next agent
        Ok(vec![available_agents[0]])
    }

    /// Select agents using load-balanced strategy
    async fn select_agents_load_balanced(
        &self,
        task: &CognitionTask,
        agents: &HashMap<Uuid, AgentInfo>,
    ) -> Result<Vec<Uuid>> {
        let mut available_agents: Vec<(&Uuid, &AgentInfo)> = agents
            .iter()
            .filter(|(_, agent)| agent.status == AgentStatus::Available)
            .collect();

        if available_agents.is_empty() {
            return Err(NeuralMeshError::NotFound("No available agents".to_string()));
        }

        // Sort by load (ascending)
        available_agents.sort_by(|a, b| a.1.current_load.partial_cmp(&b.1.current_load).unwrap());

        // Select agents with lowest load
        let num_agents = match task.priority {
            Priority::Critical => 3.min(available_agents.len()),
            Priority::High => 2.min(available_agents.len()),
            _ => 1,
        };

        Ok(available_agents
            .into_iter()
            .take(num_agents)
            .map(|(id, _)| *id)
            .collect())
    }

    /// Select agents based on capabilities
    async fn select_agents_capability_based(
        &self,
        task: &CognitionTask,
        agents: &HashMap<Uuid, AgentInfo>,
    ) -> Result<Vec<Uuid>> {
        // Extract required capabilities from task context
        let required_capabilities: Vec<String> = task
            .context
            .keys()
            .filter(|k| k.starts_with("capability:"))
            .map(|k| k.strip_prefix("capability:").unwrap().to_string())
            .collect();

        let mut matching_agents: Vec<(&Uuid, usize)> = agents
            .iter()
            .filter(|(_, agent)| agent.status == AgentStatus::Available)
            .map(|(id, agent)| {
                let match_count = agent
                    .capabilities
                    .iter()
                    .filter(|cap| required_capabilities.contains(cap))
                    .count();
                (id, match_count)
            })
            .filter(|(_, count)| *count > 0)
            .collect();

        if matching_agents.is_empty() {
            // Fallback to any available agent
            let available: Vec<Uuid> = agents
                .iter()
                .filter(|(_, agent)| agent.status == AgentStatus::Available)
                .map(|(id, _)| *id)
                .collect();

            if available.is_empty() {
                return Err(NeuralMeshError::NotFound("No available agents".to_string()));
            }

            return Ok(vec![available[0]]);
        }

        // Sort by match count (descending)
        matching_agents.sort_by(|a, b| b.1.cmp(&a.1));

        // Select top matching agents
        let num_agents = 2.min(matching_agents.len());
        Ok(matching_agents
            .into_iter()
            .take(num_agents)
            .map(|(id, _)| *id)
            .collect())
    }

    /// Select agents for consensus
    async fn select_agents_consensus(
        &self,
        _task: &CognitionTask,
        agents: &HashMap<Uuid, AgentInfo>,
        min_agents: usize,
    ) -> Result<Vec<Uuid>> {
        let available_agents: Vec<Uuid> = agents
            .iter()
            .filter(|(_, agent)| agent.status == AgentStatus::Available)
            .map(|(id, _)| *id)
            .collect();

        if available_agents.len() < min_agents {
            return Err(NeuralMeshError::InvalidInput(format!(
                "Not enough agents for consensus. Need {}, have {}",
                min_agents,
                available_agents.len()
            )));
        }

        // Select all available agents up to min_agents
        Ok(available_agents.into_iter().take(min_agents).collect())
    }

    /// Get assignment strategy based on coordination strategy
    fn get_assignment_strategy(&self) -> AssignmentStrategy {
        match &self.strategy {
            CoordinationStrategy::RoundRobin => AssignmentStrategy::RoundRobin,
            CoordinationStrategy::LoadBalanced => AssignmentStrategy::LoadBalanced,
            CoordinationStrategy::CapabilityBased => AssignmentStrategy::CapabilityBased,
            _ => AssignmentStrategy::LoadBalanced,
        }
    }

    /// Process coordination tasks
    async fn process_coordination_tasks(&self, mut rx: mpsc::UnboundedReceiver<CoordinationTask>) {
        while let Some(task) = rx.recv().await {
            match task {
                CoordinationTask::Reassign(task_id) => {
                    if let Err(e) = self.reassign_task(task_id).await {
                        tracing::error!("Failed to reassign task {}: {}", task_id, e);
                    }
                }
                CoordinationTask::UpdateLoad(agent_id, load) => {
                    if let Err(e) = self.update_agent_load(agent_id, load).await {
                        tracing::error!("Failed to update agent {} load: {}", agent_id, e);
                    }
                }
                CoordinationTask::UpdatePerformance(agent_id, score) => {
                    if let Err(e) = self.update_agent_performance(agent_id, score).await {
                        tracing::error!("Failed to update agent {} performance: {}", agent_id, e);
                    }
                }
            }
        }
    }

    /// Reassign a task
    async fn reassign_task(&self, task_id: Uuid) -> Result<()> {
        // This would implement task reassignment logic
        Ok(())
    }

    /// Update agent load
    async fn update_agent_load(&self, agent_id: Uuid, load: f64) -> Result<()> {
        let mut agents = self.agents.write().await;
        if let Some(agent) = agents.get_mut(&agent_id) {
            agent.current_load = load;
        }
        Ok(())
    }

    /// Update agent performance
    async fn update_agent_performance(&self, agent_id: Uuid, score: f64) -> Result<()> {
        let mut agents = self.agents.write().await;
        if let Some(agent) = agents.get_mut(&agent_id) {
            agent.performance_score = score;
        }
        Ok(())
    }
}

impl Clone for MeshCoordinator {
    fn clone(&self) -> Self {
        Self {
            strategy: self.strategy.clone(),
            agents: Arc::clone(&self.agents),
            task_queue: Arc::clone(&self.task_queue),
            assignments: Arc::clone(&self.assignments),
            stats: Arc::clone(&self.stats),
            task_tx: self.task_tx.clone(),
        }
    }
}

/// Coordination strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinationStrategy {
    /// Adaptive strategy based on task type and agent performance
    Adaptive,

    /// Simple round-robin assignment
    RoundRobin,

    /// Load-balanced assignment
    LoadBalanced,

    /// Capability-based assignment
    CapabilityBased,

    /// Consensus-based processing
    Consensus { min_agents: usize },
}

/// Task distribution information
#[derive(Debug, Clone)]
pub struct TaskDistribution {
    pub task_id: Uuid,
    pub assigned_agents: Vec<Uuid>,
    pub distribution_time: std::time::Instant,
    pub expected_completion: std::time::Duration,
}

/// Information about an agent
#[derive(Debug, Clone)]
struct AgentInfo {
    id: Uuid,
    capabilities: Vec<String>,
    current_load: f64,
    max_load: f64,
    task_count: usize,
    performance_score: f64,
    status: AgentStatus,
}

/// Agent status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentStatus {
    Available,
    Busy,
    Offline,
}

/// Task queue for pending tasks
#[derive(Debug)]
struct TaskQueue {
    high_priority: VecDeque<CognitionTask>,
    medium_priority: VecDeque<CognitionTask>,
    low_priority: VecDeque<CognitionTask>,
}

impl TaskQueue {
    fn new() -> Self {
        Self {
            high_priority: VecDeque::new(),
            medium_priority: VecDeque::new(),
            low_priority: VecDeque::new(),
        }
    }

    fn push(&mut self, task: CognitionTask) {
        match task.priority {
            Priority::Critical | Priority::High => self.high_priority.push_back(task),
            Priority::Medium => self.medium_priority.push_back(task),
            Priority::Low => self.low_priority.push_back(task),
        }
    }

    fn pop(&mut self) -> Option<CognitionTask> {
        self.high_priority
            .pop_front()
            .or_else(|| self.medium_priority.pop_front())
            .or_else(|| self.low_priority.pop_front())
    }

    fn len(&self) -> usize {
        self.high_priority.len() + self.medium_priority.len() + self.low_priority.len()
    }
}

/// Coordination tasks for internal processing
enum CoordinationTask {
    Reassign(Uuid),
    UpdateLoad(Uuid, f64),
    UpdatePerformance(Uuid, f64),
}

/// Coordinator statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoordinatorStats {
    pub load_factor: f64,
    pub total_assignments: u64,
    pub active_agents: usize,
    pub queued_tasks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_creation() {
        let strategy = CoordinationStrategy::Adaptive;
        let coordinator = MeshCoordinator::new(strategy).await;
        assert!(coordinator.is_ok());
    }

    #[tokio::test]
    async fn test_agent_registration() {
        let coordinator = MeshCoordinator::new(CoordinationStrategy::RoundRobin)
            .await
            .unwrap();

        let agent_id = Uuid::new_v4();
        let capabilities = vec!["test".to_string()];

        assert!(coordinator
            .register_agent(agent_id, capabilities)
            .await
            .is_ok());
        assert!(coordinator.unregister_agent(agent_id).await.is_ok());
    }

    #[test]
    fn test_task_queue() {
        let mut queue = TaskQueue::new();

        let task = CognitionTask {
            id: Uuid::new_v4(),
            task_type: crate::cognition::TaskType::PatternRecognition,
            input: crate::cognition::ThoughtPattern::new(0.5),
            context: HashMap::new(),
            priority: Priority::High,
            store_in_memory: false,
        };

        queue.push(task.clone());
        assert_eq!(queue.len(), 1);

        let popped = queue.pop();
        assert!(popped.is_some());
        assert_eq!(queue.len(), 0);
    }
}

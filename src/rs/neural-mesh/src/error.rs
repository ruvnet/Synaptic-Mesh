//! Error types for the neural mesh

use thiserror::Error;

/// Result type for neural mesh operations
pub type Result<T> = std::result::Result<T, NeuralMeshError>;

/// Errors that can occur in the neural mesh
#[derive(Error, Debug)]
pub enum NeuralMeshError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("Neural network error: {0}")]
    Neural(#[from] ruv_fann::errors::RuvFannError),

    #[error("Communication error: {0}")]
    Communication(String),

    #[error("Synchronization error: {0}")]
    Synchronization(String),

    #[error("Agent error: {0}")]
    Agent(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Training error: {0}")]
    Training(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Capacity exceeded: {0}")]
    CapacityExceeded(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("WebGPU error: {0}")]
    WebGPU(String),

    #[error("WASM error: {0}")]
    Wasm(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<serde_json::Error> for NeuralMeshError {
    fn from(err: serde_json::Error) -> Self {
        NeuralMeshError::Serialization(err.to_string())
    }
}

impl From<bincode::Error> for NeuralMeshError {
    fn from(err: bincode::Error) -> Self {
        NeuralMeshError::Serialization(err.to_string())
    }
}

impl From<tokio::sync::mpsc::error::SendError<crate::agent::AgentMessage>> for NeuralMeshError {
    fn from(err: tokio::sync::mpsc::error::SendError<crate::agent::AgentMessage>) -> Self {
        NeuralMeshError::Communication(format!("Failed to send message: {}", err))
    }
}

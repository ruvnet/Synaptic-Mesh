//! Error types for QuDAG

use thiserror::Error;

/// Main error type for QuDAG operations
#[derive(Error, Debug)]
pub enum QuDAGError {
    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Consensus error: {0}")]
    ConsensusError(String),

    #[error("Cryptography error: {0}")]
    CryptoError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("LibP2P error: {0}")]
    LibP2PError(#[from] libp2p::core::transport::TransportError<std::io::Error>),

    #[error("UUID error: {0}")]
    UuidError(#[from] uuid::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Bincode error: {0}")]
    BincodeError(#[from] bincode::Error),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl QuDAGError {
    /// Create a network error
    pub fn network<S: Into<String>>(msg: S) -> Self {
        Self::NetworkError(msg.into())
    }

    /// Create a consensus error
    pub fn consensus<S: Into<String>>(msg: S) -> Self {
        Self::ConsensusError(msg.into())
    }

    /// Create a crypto error
    pub fn crypto<S: Into<String>>(msg: S) -> Self {
        Self::CryptoError(msg.into())
    }

    /// Create a validation error
    pub fn validation<S: Into<String>>(msg: S) -> Self {
        Self::ValidationError(msg.into())
    }

    /// Create a storage error
    pub fn storage<S: Into<String>>(msg: S) -> Self {
        Self::StorageError(msg.into())
    }

    /// Check if this is a network-related error
    pub fn is_network_error(&self) -> bool {
        matches!(self, Self::NetworkError(_) | Self::LibP2PError(_))
    }

    /// Check if this is a validation error
    pub fn is_validation_error(&self) -> bool {
        matches!(self, Self::ValidationError(_))
    }

    /// Check if this is a recoverable error
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::NetworkError(_) => true,
            Self::IoError(_) => true,
            Self::SerializationError(_) => false,
            Self::ValidationError(_) => false,
            Self::CryptoError(_) => false,
            Self::ConsensusError(_) => true,
            Self::StorageError(_) => true,
            Self::ConfigError(_) => false,
            Self::LibP2PError(_) => true,
            Self::UuidError(_) => false,
            Self::JsonError(_) => false,
            Self::BincodeError(_) => false,
            Self::Unknown(_) => false,
        }
    }
}

/// Result type alias for QuDAG operations
pub type Result<T> = std::result::Result<T, QuDAGError>;

/// Convert libp2p noise errors to QuDAGError
impl From<libp2p::noise::Error> for QuDAGError {
    fn from(err: libp2p::noise::Error) -> Self {
        Self::NetworkError(format!("Noise protocol error: {}", err))
    }
}

/// Convert libp2p gossipsub errors to QuDAGError
impl From<libp2p::gossipsub::PublishError> for QuDAGError {
    fn from(err: libp2p::gossipsub::PublishError) -> Self {
        Self::NetworkError(format!("Gossipsub publish error: {}", err))
    }
}

/// Convert libp2p gossipsub subscription errors to QuDAGError
impl From<libp2p::gossipsub::SubscriptionError> for QuDAGError {
    fn from(err: libp2p::gossipsub::SubscriptionError) -> Self {
        Self::NetworkError(format!("Gossipsub subscription error: {}", err))
    }
}

/// Convert libp2p dial errors to QuDAGError
impl From<libp2p::swarm::DialError> for QuDAGError {
    fn from(err: libp2p::swarm::DialError) -> Self {
        Self::NetworkError(format!("Dial error: {}", err))
    }
}

/// Convert multiaddr parse errors to QuDAGError
impl From<multiaddr::Error> for QuDAGError {
    fn from(err: multiaddr::Error) -> Self {
        Self::NetworkError(format!("Multiaddr error: {}", err))
    }
}

/// Convert hex decode errors to QuDAGError
impl From<hex::FromHexError> for QuDAGError {
    fn from(err: hex::FromHexError) -> Self {
        Self::CryptoError(format!("Hex decode error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = QuDAGError::network("test network error");
        assert!(err.is_network_error());
        assert!(err.is_recoverable());
    }

    #[test]
    fn test_error_types() {
        let validation_err = QuDAGError::validation("test");
        assert!(validation_err.is_validation_error());
        assert!(!validation_err.is_recoverable());

        let network_err = QuDAGError::network("test");
        assert!(network_err.is_network_error());
        assert!(network_err.is_recoverable());
    }

    #[test]
    fn test_error_display() {
        let err = QuDAGError::ConsensusError("test consensus error".to_string());
        assert_eq!(err.to_string(), "Consensus error: test consensus error");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let qudag_err = QuDAGError::from(io_err);

        match qudag_err {
            QuDAGError::IoError(_) => {}
            _ => panic!("Expected IoError"),
        }
    }

    #[test]
    fn test_result_type() {
        fn test_function() -> Result<String> {
            Ok("success".to_string())
        }

        let result = test_function();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }

    #[test]
    fn test_error_chain() {
        let json_err = serde_json::from_str::<i32>("invalid json").unwrap_err();
        let qudag_err = QuDAGError::from(json_err);

        match qudag_err {
            QuDAGError::JsonError(_) => {}
            _ => panic!("Expected JsonError"),
        }
    }
}

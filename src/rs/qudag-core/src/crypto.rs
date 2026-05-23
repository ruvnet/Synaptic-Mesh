//! Cryptography module for QuDAG
//!
//! Implements modern cryptographic algorithms including Ed25519 for signatures
//! and X25519 for key exchange, along with quantum-resistant fingerprinting.

use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use blake3::{Hash, Hasher};
use ed25519_dalek::{Signature as Ed25519Signature, SigningKey, Verifier, VerifyingKey};
use rand::{CryptoRng, RngCore};
use serde::{Deserialize, Serialize};
use x25519_dalek::{PublicKey as X25519PublicKey, SharedSecret, StaticSecret};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{DAGNode, QuDAGError, Result};

/// Main cryptography handler with modern algorithms
#[derive(Debug)]
pub struct QuDAGCrypto {
    signing_key: Option<SigningKey>,
    x25519_secret: Option<[u8; 32]>,
    known_keys: Arc<std::sync::RwLock<HashMap<libp2p::PeerId, VerifyingKey>>>,
}

impl QuDAGCrypto {
    /// Create a new crypto instance
    pub fn new() -> Self {
        Self {
            signing_key: None,
            x25519_secret: None,
            known_keys: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Generate Ed25519 keypair for signatures
    pub fn generate_signing_keypair<R: RngCore + CryptoRng>(&mut self, rng: &mut R) -> Result<()> {
        let mut secret_bytes = [0u8; 32];
        rng.fill_bytes(&mut secret_bytes);
        let signing_key = SigningKey::from_bytes(&secret_bytes);
        self.signing_key = Some(signing_key);
        Ok(())
    }

    /// Generate X25519 keypair for key exchange
    pub fn generate_exchange_keypair<R: RngCore + CryptoRng>(&mut self, rng: &mut R) -> Result<()> {
        let mut secret_bytes = [0u8; 32];
        rng.fill_bytes(&mut secret_bytes);
        self.x25519_secret = Some(secret_bytes);
        Ok(())
    }

    /// Sign data using Ed25519
    pub fn sign(&self, data: &[u8]) -> Result<PostQuantumSignature> {
        use ed25519_dalek::Signer;

        let signing_key = self.signing_key.as_ref().ok_or_else(|| {
            QuDAGError::CryptoError("Ed25519 keypair not initialized".to_string())
        })?;

        let mut hasher = Hasher::new();
        hasher.update(data);
        let hash = hasher.finalize();

        let signature: Ed25519Signature = signing_key.sign(hash.as_bytes());

        Ok(PostQuantumSignature {
            algorithm: SignatureAlgorithm::Ed25519,
            signature: signature.to_bytes().to_vec(),
            public_key: signing_key.verifying_key().to_bytes().to_vec(),
        })
    }

    /// Verify a signature using Ed25519
    pub fn verify_signature(&self, node: &DAGNode) -> Result<()> {
        let signature = node
            .signature()
            .ok_or_else(|| QuDAGError::ValidationError("No signature found".to_string()))?;

        if signature.algorithm != SignatureAlgorithm::Ed25519 {
            return Err(QuDAGError::CryptoError(
                "Unsupported signature algorithm".to_string(),
            ));
        }

        let public_key_bytes: &[u8; 32] = signature
            .public_key
            .as_slice()
            .try_into()
            .map_err(|_| QuDAGError::CryptoError("Invalid public key length".to_string()))?;
        let public_key = VerifyingKey::from_bytes(public_key_bytes)
            .map_err(|e| QuDAGError::CryptoError(format!("Invalid public key: {}", e)))?;

        let sig_bytes: &[u8; 64] = signature
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| QuDAGError::CryptoError("Invalid signature length".to_string()))?;
        let sig = Ed25519Signature::from_bytes(sig_bytes);

        let mut hasher = Hasher::new();
        hasher.update(node.data());
        let hash = hasher.finalize();

        public_key.verify(hash.as_bytes(), &sig).map_err(|e| {
            QuDAGError::CryptoError(format!("Signature verification failed: {}", e))
        })?;

        Ok(())
    }

    /// Create a shared secret using X25519
    pub fn create_shared_secret(&self, peer_public: &[u8]) -> Result<SharedSecret> {
        let secret_bytes = self
            .x25519_secret
            .as_ref()
            .ok_or_else(|| QuDAGError::CryptoError("X25519 secret not initialized".to_string()))?;

        let secret = StaticSecret::from(*secret_bytes);
        let peer_public_bytes: [u8; 32] = peer_public
            .try_into()
            .map_err(|_| QuDAGError::CryptoError("Invalid peer public key size".to_string()))?;
        let peer_public_key = X25519PublicKey::from(peer_public_bytes);

        Ok(secret.diffie_hellman(&peer_public_key))
    }

    /// Encrypt data using AES-256-GCM
    pub fn encrypt(&self, data: &[u8], shared_secret: &SharedSecret) -> Result<Vec<u8>> {
        let key = Key::<Aes256Gcm>::from_slice(shared_secret.as_bytes());
        let cipher = Aes256Gcm::new(key);

        let mut nonce_bytes = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, data)
            .map_err(|e| QuDAGError::CryptoError(format!("Encryption failed: {}", e)))?;

        // Prepend nonce to ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }

    /// Decrypt data using AES-256-GCM
    pub fn decrypt(&self, encrypted: &[u8], shared_secret: &SharedSecret) -> Result<Vec<u8>> {
        if encrypted.len() < 12 {
            return Err(QuDAGError::CryptoError(
                "Invalid encrypted data".to_string(),
            ));
        }

        let (nonce_bytes, ciphertext) = encrypted.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        let key = Key::<Aes256Gcm>::from_slice(shared_secret.as_bytes());
        let cipher = Aes256Gcm::new(key);

        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| QuDAGError::CryptoError(format!("Decryption failed: {}", e)))
    }

    /// Generate quantum-resistant fingerprint for .dark addressing
    pub fn generate_quantum_fingerprint(&self, input: &[u8]) -> QuantumFingerprint {
        let mut hasher = Hasher::new();
        hasher.update(b"qudag-quantum-fingerprint-v1");
        hasher.update(input);

        // Add post-quantum key material if available
        if let Some(signing_key) = &self.signing_key {
            hasher.update(&signing_key.verifying_key().to_bytes());
        }

        let hash = hasher.finalize();

        QuantumFingerprint {
            hash: hash.as_bytes().to_vec(),
            algorithm: "BLAKE3-QR".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Verify a quantum fingerprint
    pub fn verify_quantum_fingerprint(
        &self,
        fingerprint: &QuantumFingerprint,
        input: &[u8],
    ) -> Result<bool> {
        let expected = self.generate_quantum_fingerprint(input);
        Ok(fingerprint.hash == expected.hash)
    }

    /// Get Ed25519 public key
    pub fn signing_public_key(&self) -> Result<Vec<u8>> {
        let signing_key = self.signing_key.as_ref().ok_or_else(|| {
            QuDAGError::CryptoError("Ed25519 keypair not initialized".to_string())
        })?;
        Ok(signing_key.verifying_key().to_bytes().to_vec())
    }

    /// Get X25519 public key
    pub fn exchange_public_key(&self) -> Result<Vec<u8>> {
        let secret_bytes = self
            .x25519_secret
            .as_ref()
            .ok_or_else(|| QuDAGError::CryptoError("X25519 secret not initialized".to_string()))?;
        let secret = StaticSecret::from(*secret_bytes);
        let public = X25519PublicKey::from(&secret);
        Ok(public.to_bytes().to_vec())
    }

    /// Register a peer's verifying key
    pub fn register_peer_key(&self, peer_id: libp2p::PeerId, public_key: VerifyingKey) {
        let mut keys = self.known_keys.write().unwrap();
        keys.insert(peer_id, public_key);
    }

    /// Get a peer's verifying key
    pub fn get_peer_key(&self, peer_id: &libp2p::PeerId) -> Option<VerifyingKey> {
        let keys = self.known_keys.read().unwrap();
        keys.get(peer_id).cloned()
    }
}

impl Default for QuDAGCrypto {
    fn default() -> Self {
        Self::new()
    }
}

/// Post-quantum signature wrapper
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostQuantumSignature {
    pub algorithm: SignatureAlgorithm,
    pub signature: Vec<u8>,
    pub public_key: Vec<u8>,
}

/// Supported signature algorithms
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignatureAlgorithm {
    Ed25519,
    // Future quantum-resistant algorithms can be added here
    MLDSA, // For future upgrade
}

/// Quantum fingerprint for .dark addressing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantumFingerprint {
    pub hash: Vec<u8>,
    pub algorithm: String,
    pub timestamp: u64,
}

impl QuantumFingerprint {
    /// Convert to .dark domain format
    pub fn to_dark_domain(&self) -> String {
        let encoded = hex::encode(&self.hash[..16]); // Use first 16 bytes for domain
        format!("{}.dark", encoded)
    }

    /// Parse from .dark domain
    pub fn from_dark_domain(domain: &str) -> Result<Self> {
        if !domain.ends_with(".dark") {
            return Err(QuDAGError::CryptoError("Invalid .dark domain".to_string()));
        }

        let hex_part = domain.strip_suffix(".dark").unwrap();
        let hash_prefix = hex::decode(hex_part)
            .map_err(|e| QuDAGError::CryptoError(format!("Invalid hex in .dark domain: {}", e)))?;

        Ok(Self {
            hash: hash_prefix,
            algorithm: "BLAKE3-QR".to_string(),
            timestamp: 0,
        })
    }
}

/// Zeroizing wrapper for sensitive key material
#[derive(Debug, Clone, Zeroize, ZeroizeOnDrop)]
pub struct SecretKey {
    inner: Vec<u8>,
}

impl SecretKey {
    pub fn new(key: Vec<u8>) -> Self {
        Self { inner: key }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }
}

// Alias for compatibility
pub type QuantumResistantCrypto = QuDAGCrypto;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::thread_rng;

    #[test]
    fn test_ed25519_keygen() {
        let mut crypto = QuDAGCrypto::new();
        let mut rng = thread_rng();

        assert!(crypto.generate_signing_keypair(&mut rng).is_ok());
        assert!(crypto.signing_public_key().is_ok());
    }

    #[test]
    fn test_x25519_keygen() {
        let mut crypto = QuDAGCrypto::new();
        let mut rng = thread_rng();

        assert!(crypto.generate_exchange_keypair(&mut rng).is_ok());
        assert!(crypto.exchange_public_key().is_ok());
    }

    #[test]
    fn test_quantum_fingerprint() {
        let crypto = QuDAGCrypto::new();
        let data = b"test data";

        let fingerprint = crypto.generate_quantum_fingerprint(data);
        assert!(crypto
            .verify_quantum_fingerprint(&fingerprint, data)
            .unwrap());

        let wrong_data = b"wrong data";
        assert!(!crypto
            .verify_quantum_fingerprint(&fingerprint, wrong_data)
            .unwrap());
    }

    #[test]
    fn test_dark_domain() {
        let crypto = QuDAGCrypto::new();
        let data = b"test.example.com";

        let fingerprint = crypto.generate_quantum_fingerprint(data);
        let dark_domain = fingerprint.to_dark_domain();

        assert!(dark_domain.ends_with(".dark"));
        assert!(dark_domain.len() > 5);

        let parsed = QuantumFingerprint::from_dark_domain(&dark_domain);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_signature_algorithm_serialization() {
        let sig = PostQuantumSignature {
            algorithm: SignatureAlgorithm::Ed25519,
            signature: vec![1, 2, 3],
            public_key: vec![4, 5, 6],
        };

        let json = serde_json::to_string(&sig).unwrap();
        let deserialized: PostQuantumSignature = serde_json::from_str(&json).unwrap();

        assert_eq!(sig, deserialized);
    }

    #[test]
    fn test_encryption_decryption() {
        let mut crypto1 = QuDAGCrypto::new();
        let mut crypto2 = QuDAGCrypto::new();
        let mut rng = thread_rng();

        // Generate keypairs
        crypto1.generate_exchange_keypair(&mut rng).unwrap();
        crypto2.generate_exchange_keypair(&mut rng).unwrap();

        // Exchange public keys and create shared secret
        let pub1 = crypto1.exchange_public_key().unwrap();
        let pub2 = crypto2.exchange_public_key().unwrap();

        let shared1 = crypto1.create_shared_secret(&pub2).unwrap();
        let shared2 = crypto2.create_shared_secret(&pub1).unwrap();

        // Test encryption/decryption
        let plaintext = b"Hello, QuDAG!";
        let encrypted = crypto1.encrypt(plaintext, &shared1).unwrap();
        let decrypted = crypto2.decrypt(&encrypted, &shared2).unwrap();

        assert_eq!(plaintext, &decrypted[..]);
    }
}

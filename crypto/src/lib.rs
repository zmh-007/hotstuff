// Copyright(C) Facebook, Inc. and its affiliates.
use placeholder_project_name_placeholder_zk::field::goldilocks_field::GoldilocksField;
use placeholder_project_name_placeholder_zk::field::types::Field;
use placeholder_project_name_placeholder_zk::hash::poseidon::PoseidonHash;
use placeholder_project_name_placeholder_zk::plonk::config::{GenericHashOut, Hasher};
use placeholder_project_name_placeholder_zk::hash::hash_types::HashOut;
use rand::{RngCore};
use std::fmt;
use std::convert::TryInto;
use base64::{Engine as _, engine::general_purpose};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use std::cmp::Ordering;
use serde::{ser, de, Serialize, Serializer, Deserialize, Deserializer};
use blst::min_pk::{SecretKey};

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

/// Represents a hash digest (32 bytes).
#[derive(Copy, Hash, PartialEq, Default, Eq, Clone)]
pub struct Digest(pub HashOut<GoldilocksField>);

impl Digest {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    pub fn to_vec_field(&self) -> Vec<GoldilocksField> {
        self.0.elements.to_vec()
    }

    pub fn size(&self) -> usize {
        self.0.elements.len()
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(&self.0.to_bytes()))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(&self.0.to_bytes()))
    }
}

impl PartialOrd for Digest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Digest {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.to_bytes().cmp(&other.0.to_bytes())
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let base64_str = general_purpose::STANDARD.encode(&self.0.to_bytes());
        serializer.serialize_str(&base64_str)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let base64_str = String::deserialize(deserializer)?;
        let bytes = general_purpose::STANDARD.decode(&base64_str).unwrap();
        Ok(Digest(HashOut::from_bytes(&bytes)))
    }
}

/// This trait is implemented by all messages that can be hashed.
pub trait Hash {
    fn digest(&self) -> Digest;
}

/// Represents a public key (in bytes).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct PublicKey(pub [u8; 48]);

impl PublicKey {
    pub fn encode_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let array = bytes[..48]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }

    pub fn to_hash(&self) -> HashOut<GoldilocksField> {
        let mut fields = [GoldilocksField::ZERO; 6];
        for (i, chunk) in self.0.chunks_exact(8).enumerate() {
            let bytes: [u8; 8] = chunk.try_into().unwrap();
            fields[i] = GoldilocksField::from_canonical_u64(u64::from_le_bytes(bytes));
        }
        PoseidonHash::hash_no_pad(&fields)
    }
}

impl Default for PublicKey {
    fn default() -> Self {
        PublicKey([0u8; 48])
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64().get(0..16).unwrap())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct Signature(pub [u8; 96]);

impl Signature {
    pub fn encode_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let array = bytes[..96]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }
}

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Default for Signature {
    fn default() -> Self {
        Signature([0u8; 96])
    }
}

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    // gen rand sk
    let mut rng = rand::thread_rng();
    let mut ikm = [0u8; 32];
    rng.fill_bytes(&mut ikm);
    let sk = SecretKey::key_gen(&ikm, &[]).unwrap();

    // calculate pk
    let pk = sk.sk_to_pk();
    (PublicKey(pk.to_bytes()), sk)
}

/// This service holds the node's private key. It takes digests as input and returns a signature
/// over the digest (through a oneshot channel).
#[derive(Clone)]
pub struct SignatureService {
    channel: Sender<(Digest, oneshot::Sender<Signature>)>,
}

impl SignatureService {
    pub fn new(secret: SecretKey) -> Self {
        let (tx, mut rx): (Sender<(Digest, oneshot::Sender<Signature>)>, _) = channel(100);
        tokio::spawn(async move {
            while let Some((digest, sender)) = rx.recv().await {
                let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
                let signature = secret.sign(&digest.to_vec(), dst, &[]);
                let _ = sender.send(Signature(signature.to_bytes()));
            }
        });
        Self { channel: tx }
    }

    pub async fn request_signature(&mut self, digest: Digest) -> Signature {
        let (sender, receiver): (oneshot::Sender<_>, oneshot::Receiver<_>) = oneshot::channel();
        if let Err(e) = self.channel.send((digest, sender)).await {
            panic!("Failed to send message Signature Service: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive signature from Signature Service")
    }
}

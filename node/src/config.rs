use consensus::{Committee as ConsensusCommittee, Parameters as ConsensusParameters};
use crypto::{generate_production_keypair, PublicKey};
use mempool::{Committee as MempoolCommittee, Parameters as MempoolParameters};
use placeholder_project_name_placeholder_zk::hash::hash_types::HashOut;
use placeholder_project_name_placeholder_zk::field::goldilocks_field::GoldilocksField;
use placeholder_project_name_placeholder_zk::field::types::Sample;
use placeholder_project_name_placeholder_zk::placeholder_project_name_placeholder_patch::PlaceholderProjectNamePlaceholderVerifierOnlyCircuitData;
use placeholder_project_name_placeholder_zk::util::serialization::DefaultGateSerializer;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use thiserror::Error;
use std::convert::TryInto;
use base64::{Engine as _, engine::general_purpose};
use circuit::{Digest, SecretCircuit};
use blst::min_pk::SecretKey;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file '{file}': {message}")]
    ReadError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    WriteError { file: String, message: String },
}

pub trait Export: Serialize + DeserializeOwned {
    fn read(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ReadError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn write(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::WriteError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct Parameters {
    pub consensus: ConsensusParameters,
    pub mempool: MempoolParameters,
}

impl Export for Parameters {}

#[derive(Serialize, Deserialize)]
pub struct Secret {
    pub name: PublicKey,
    pub secret: SecretKey,
}

impl Secret {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Export for Secret {}

impl Default for Secret {
    fn default() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

#[derive(Serialize, Deserialize)]
pub struct PreImage {
    pub name: Digest,
    pub vd: String,
    pub secret: Digest,
}

impl PreImage {
    pub fn new() -> Self {
        let secret = HashOut::<GoldilocksField>::rand();
        let secret_circuit = SecretCircuit::new(secret);
        let vk = secret_circuit.vk();
        let vk_h: PlaceholderProjectNamePlaceholderVerifierOnlyCircuitData = vk.try_into().unwrap();
        let name = HashOut::from(vk_h);
        let vd = secret_circuit.vd();
        let vd_encoded = general_purpose::STANDARD.encode(&vd.to_bytes(&DefaultGateSerializer).unwrap());
  
        Self { name: Digest(name), vd: vd_encoded, secret: Digest(secret) }
    }
}

impl Export for PreImage {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    pub consensus: ConsensusCommittee,
    pub mempool: MempoolCommittee,
}

impl Export for Committee {}

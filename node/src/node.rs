use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use websocket::WebSocketServer;
use consensus::{Block, Consensus, UTXOCache};
use log::{error, info};
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use crypto::SignatureService;
use consensus::WebSocketEvent;
use tokio::sync::Mutex;
use std::sync::Arc;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    pub commit: Receiver<Block>,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(&filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Init mempool cache.
        // let mempool_cache = Arc::new(Mutex::new(HashSet::new()));

        // Run the proof service.
        let proof_service = SignatureService::new(secret_key);

        // load UTXO cache
        let utxo_cache = Self::load_utxo_cache(&mut store.clone()).await;
        let utxo_cache = Arc::new(Mutex::new(utxo_cache));

        // Make a new mempool.
        let tx_mempool_transactions = Mempool::spawn(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
        );

        // Start WebSocket server if address is provided
        let tx_websocket_event = {
            let (tx_event, rx_event) = channel::<WebSocketEvent>(100);
            let mut websocket_server = WebSocketServer::new(
                name,
                committee.websocket,
                store.clone(), 
                tx_mempool_transactions.clone(),
                rx_event,
            );
            tokio::spawn(async move {
                if let Err(e) = websocket_server.start().await {
                    log::error!("WebSocket server error: {}", e);
                }
            });
            tx_event
        };

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            proof_service,
            store.clone(),
            utxo_cache,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
            tx_websocket_event,
        );

        info!("Node {} successfully booted", name);
        Ok(Self {commit: rx_commit })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn start(&mut self) {
        loop {
            tokio::select! {
                Some(_) = self.commit.recv() => {
                    // Execute block
                }
            }
        }
    }

    async fn load_utxo_cache(store: &mut Store) -> UTXOCache {
        match store.get_utxo_cache().await {
            Ok(Some(v)) => {
                let utxo_cache: UTXOCache = bincode::deserialize(&v).unwrap();
                return utxo_cache;
            },
            Ok(None) => info!("No utxo cache exists, init a new one!"),
            Err(err) => error!("Failed to load chain state {err}"),
        }
        UTXOCache::default()
    }
}

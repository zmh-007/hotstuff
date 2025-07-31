use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use crate::websocket::WebSocketServer;
use consensus::{Block, Consensus};
use log::info;
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use crypto::SignatureService;
use consensus::WebSocketEvent;
use tokio::sync::mpsc;

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
        websocket_addr: Option<String>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // // build circuit
        // let secret_circuit = SecretCircuit::new(secret_encoded.0);

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(&filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Run the proof service.
        let proof_service = SignatureService::new(secret_key);

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
        let tx_websocket_event = if let Some(addr) = websocket_addr {
            let (tx_event, rx_event) = mpsc::channel::<WebSocketEvent>(100);
            let mut websocket_server = WebSocketServer::new(
                store.clone(), 
                tx_mempool_transactions.clone(),
                rx_event
            );
            let ws_addr = addr.clone();
            tokio::spawn(async move {
                if let Err(e) = websocket_server.start(&ws_addr).await {
                    log::error!("WebSocket server error: {}", e);
                }
            });
            info!("WebSocket server started on: {}", addr);
            Some(tx_event)
        } else {
            None
        };

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            proof_service,
            store,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
            tx_websocket_event,
        );

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(_block) = self.commit.recv().await {
            // This is where we can further process committed block.
        }
    }
}

use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use crate::l0::L0;
use crate::websocket::WebSocketServer;
use consensus::{Block, Consensus, FullBlock};
use log::info;
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use crypto::SignatureService;
use consensus::WebSocketEvent;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use std::sync::Arc;
use zk::{Fr, FrSerialization};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    l0: Arc<Mutex<L0>>,
    store: Store,
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

        // initialize L0
        let next0 = Fr::deserialize_be_compressed(&hex::decode("2092de7b23d178d6c8cf48debe44d6858554160e8eb95f5dba3aee5c3a564bd0").unwrap()[..]).unwrap();
        let price = Fr::deserialize_be_compressed([1u8; 32].as_ref()).unwrap();
        let l0 = L0::new(next0, price);
        let l0 = Arc::new(Mutex::new(l0));

        // Start WebSocket server if address is provided
        let tx_websocket_event = if let Some(addr) = websocket_addr {
            let (tx_event, rx_event) = mpsc::channel::<WebSocketEvent>(100);
            let mut websocket_server = WebSocketServer::new(
                store.clone(), 
                tx_mempool_transactions.clone(),
                rx_event,
                l0.clone(),
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
            store.clone(),
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
            tx_websocket_event,
        );


        info!("Node {} successfully booted", name);
        Ok(Self { l0, store, commit: rx_commit })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(block) = self.commit.recv().await {
            // This is where we can further process committed block.
            let mut txs = Vec::new();
            for tx_hash in block.payload {
                let tx_data = self.store.read_tx(tx_hash.to_vec()).await.expect(&format!("Failed to read transaction {:?} from store", tx_hash)).unwrap();
                txs.push(tx_data);
            }
            let full_block = FullBlock {
                qc: block.qc.clone(),
                tc: block.tc.clone(),
                author: block.author.clone(),
                round: block.round,
                payload: txs,
                txg: block.txg.clone(),
                next: block.next.clone(),
                signature: block.signature.clone(),
            };
            self.l0.lock().await.block(full_block).expect(&format!("Failed to process block {:?} in L0", block.qc.last_tail));
        }
    }
}

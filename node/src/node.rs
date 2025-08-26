use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use crate::websocket::WebSocketServer;
use consensus::{Block, Consensus, UTXOCache};
use l0::{Blk, Tx, Wp, L0};
use log::{error, info};
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use crypto::SignatureService;
use consensus::WebSocketEvent;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use std::convert::TryFrom;
use zk::{Fr, FrSerialization};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    cache: Arc<Mutex<HashSet<Fr>>>,
    l0: Arc<Mutex<L0>>,
    store: Store,
    pub commit: Receiver<Block>,
    pub rx_verify: Receiver<(Wp<Tx>, oneshot::Sender<bool>)>,
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
        let (tx_verify, rx_verify) = channel(CHANNEL_CAPACITY);

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

        // Init tx-in cache.
        let cache = Arc::new(Mutex::new(HashSet::new()));

        // Run the proof service.
        let proof_service = SignatureService::new(secret_key);

        // load L0
        let next0 = Fr::deserialize_be_compressed(&hex::decode("2092de7b23d178d6c8cf48debe44d6858554160e8eb95f5dba3aee5c3a564bd0").unwrap()[..]).unwrap();
        let price = Fr::deserialize_be_compressed([1u8; 32].as_ref()).unwrap();
        let l0 = Self::load_l0(store.clone(), next0, price).await;
        let l0 = Arc::new(Mutex::new(l0));

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
            tx_verify,
        );

        // Start WebSocket server if address is provided
        let tx_websocket_event = if let Some(addr) = websocket_addr {
            let (tx_event, rx_event) = channel::<WebSocketEvent>(100);
            let mut websocket_server = WebSocketServer::new(
                store.clone(), 
                tx_mempool_transactions.clone(),
                rx_event,
                cache.clone(),
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
            l0.clone(),
            utxo_cache.clone(),
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
            tx_websocket_event,
        );


        info!("Node {} successfully booted", name);
        Ok(Self { cache, l0, store, commit: rx_commit, rx_verify })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn start(&mut self) {
        loop {
            tokio::select! {
                Some(block) = self.commit.recv() => {
                    // This is where we can further process committed block.
                    let mut txs = Vec::new();
                    for tx_hash in block.payload {
                        let tx_data = self.store.read_tx(tx_hash.to_vec()).await.expect(&format!("Failed to read transaction {:?} from store", tx_hash)).unwrap();
                        let tx: Wp<Tx> = Wp::try_from(&tx_data[..]).expect("Failed to deserialize transaction from bytes");
                        txs.push(tx);
                    }
                    let verified_block = Blk {
                        last: Fr::deserialize_be_compressed(&block.qc.last_tail.0[..]).expect("Failed to deserialize last_tail to Fr"),
                        txs,
                        txg: Tx::try_from(&block.txg[..]).expect("Failed to convert txg to Tx"),
                        next: (Fr::deserialize_be_compressed(&block.next.0[..]).expect("Failed to deserialize next.0 to Fr"), Fr::deserialize_be_compressed(&block.next.1[..]).expect("Failed to deserialize next.1 to Fr")),
                    };
                    self.l0.lock().await.verified_block(verified_block).expect(&format!("Failed to process block {:?} in L0", block.qc.last_tail));
                    self.store.write_chain_state((&*self.l0.lock().await).into()).await;
                    let v = self.store.get_utxo_cache().await.expect("Failed to get UTXO cache from store").unwrap();
                    let mut utxo_cache = bincode::deserialize::<consensus::UTXOCache>(&v).expect("Failed to deserialize UTXO cache");
                    utxo_cache.cache.remove(&block.qc.hash);
                    self.store.write_utxo_cache(bincode::serialize(&utxo_cache).unwrap()).await;
                }
                Some((tx, response)) = self.rx_verify.recv() => {
                    // verify tx
                    let verify_result = self.l0.lock().await.verify(&tx);
                    if let Err(e) = verify_result {
                        error!("Failed to verify transaction: {}", e);
                        let _ = response.send(false);
                        continue;
                    }
                    // verify cache
                    let mut cache_guard = self.cache.lock().await;
                    if cache_guard.contains(&tx.val.ix) || cache_guard.contains(&tx.val.iy) {
                        error!("Transaction with ix={:?} or iy={:?} already exists in cache", tx.val.ix, tx.val.iy);
                        let _ = response.send(false);
                        continue;
                    }
                    let _ = response.send(true);
                    // add to cache
                    cache_guard.insert(tx.val.ix);
                    cache_guard.insert(tx.val.iy);
                }
            }
        }
    }

    async fn load_l0(mut store: Store, next: Fr, price: Fr) -> L0 {
        match store.get_chain_state().await {
            Ok(Some(v)) => {
                let l0 = L0::try_from(v.as_slice()).expect("Failed to convert chain state to L0");
                return l0
            },
            Ok(None) => info!("No chain state exists, will start from genesis!"),
            Err(err) => error!("Failed to load chain state {err}"),
        }
        L0::new(next, price)
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

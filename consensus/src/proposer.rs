use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC, UTXOCache};
use bytes::Bytes;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use l0::{Out, Tx, Wp};
use log::{debug, info, warn};
use network::{CancelHandler, ReliableSender};
use store::Store;
use tokio::sync::Mutex;
use zk::{AdditiveGroup, AsBytes, Fr};
use std::collections::HashSet;
use std::sync::Arc;
use crypto::{Digest, PublicKey, SignatureService};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    Cleanup(Vec<Digest>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    store: Store,
    utxo_cache: Arc<Mutex<UTXOCache>>,
    rx_mempool: Receiver<Digest>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    buffer: HashSet<Digest>,
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        utxo_cache: Arc<Mutex<UTXOCache>>,
        rx_mempool: Receiver<Digest>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                store,
                utxo_cache,
                rx_mempool,
                rx_message,
                tx_loopback,
                buffer: HashSet::new(),
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // if qc != QC::genesis() {
        //     tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        // }
        let account1 = Fr::dec(&mut hex::decode("43ddbcabd109d20df318b92b14b473912450b9192681f2af3ec348f917929cfd").unwrap().into_iter()).unwrap();
        let account2 = Fr::dec(&mut hex::decode("530e4cea319ed6244a8cd4c1d99c7ac7675e47ed8b9831b05b0c735e36410364").unwrap().into_iter()).unwrap();
        let txg = Tx {
            ix: Fr::from(10000000000000000u64),
            iy: Fr::from(10000000000000000u64),
            ox: Out {
                    amount: Fr::from(10000000000000000u64),
                    owner: account1,
                    data: Vec::new(),
                },
            oy: Out {
                    amount: Fr::from(10000000000000000u64),
                    owner: account2,
                    data: Vec::new(),
                },
        };  // TODO: Placeholder for txg
        let next0 = hex::decode("2092de7b23d178d6c8cf48debe44d6858554160e8eb95f5dba3aee5c3a564bd0").unwrap();
        let price = Fr::from(1u64).enc().collect();
        // Generate a new block.
        let mut payload = Vec::new();
        let mut tx_ins = HashSet::new();
        for digest in self.buffer.drain() {
            let tx_bytes = self.store.read_tx(digest.to_vec()).await.expect("Failed to get tx from store").expect("Digest in buffer but not in store");
            let tx = Wp::<Tx>::dec(&mut tx_bytes.into_iter()).expect("Failed to decode wp transaction");
            if tx.val.ix != Fr::ZERO && !tx_ins.insert(tx.val.ix) {
                warn!("Skipping double-spending transaction (ix conflict) {:?}", digest);
                continue;
            }
            if tx.val.iy != Fr::ZERO && !tx_ins.insert(tx.val.iy) {
                warn!("Skipping double-spending transaction (iy conflict) {:?}", digest);
                continue;
            }
            if !self.utxo_cache.lock().await.check_tx(&tx) {
                warn!("Skipping double-spending transaction {:?}", digest);
                continue;
            }
            payload.push(digest.clone());
        }
        
        let block = Block::new(
            qc,
            tc,
            self.name.clone(),
            round,
            payload,
            txg.enc().collect(),
            (next0, price), // TODO: Placeholder for next
            self.signature_service.clone(),
        )
        .await;

        if !block.payload.is_empty() {
            info!("Created block {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created block {} -> {:?}", block, x);
            }
        }
        debug!("Created block {:?}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let (names, addresses): (Vec<_>, _) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
            .expect("Failed to serialize block");
        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");

        // Control system: Wait for 2f+1 nodes to acknowledge our block before continuing.
        let mut wait_for_quorum: FuturesUnordered<_> = names
            .into_iter()
            .zip(handles.into_iter())
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        let mut total_stake = self.committee.stake(&self.name);
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                break;
            }
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(digest) = self.rx_mempool.recv() => {
                    //if self.buffer.len() < 155 {
                        debug!("Received tx digest: {:?}", digest);
                        self.buffer.insert(digest);
                    //}
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                    ProposerMessage::Cleanup(digests) => {
                        for x in &digests {
                            self.buffer.remove(x);
                        }
                    }
                }
            }
        }
    }
}

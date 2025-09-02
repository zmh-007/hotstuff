use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC, UTXOCache};
use bytes::Bytes;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use l0::{Out, Tx, Wp, L0};
use log::{debug, info, warn};
use network::{CancelHandler, ReliableSender};
use store::Store;
use tokio::sync::Mutex;
use zk::{Fr, Vk, ToHash};
use std::collections::HashSet;
use std::sync::Arc;
use crypto::{Digest, PublicKey, SignatureService};
use tokio::sync::mpsc::{Receiver, Sender};
use std::convert::TryInto;

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
    l0: Arc<Mutex<L0>>,
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
        l0: Arc<Mutex<L0>>,
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
                l0,
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
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        let vk: Vk = hex::decode("91e33e9340aa7e3eb785c21a2baea3066397ca7d3cd792d498dc10cc61a55c5d86d07e40b1b49a0a622297a312a2c90496556736ca9a7284431ea946c9b7f822dd6b05464add282f6a5358dda53fb65d956d531c1d83997fa66933d4740cfbbba48736b143fec6e419a41727d0f1d2b93a82029105864eee3ccc68ab2229491322b422bba12c90fb9357df63798593dd939d715247532fd95ee373020e69047a759c9786340e23ba430595235f87974414a83ce2843abc043918b67439d876e8c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b0cbe536a0026894debe231c3764f33d963d5f741410060da108cd1f46cdb11a6875b2419d836dfd3f0cb0f960523db40000000103").unwrap().as_slice().try_into().unwrap();
        let txg = &Tx {
            ix: Fr::from(10000000000000000u64),
            iy: Fr::from(10000000000000000u64),
            ox: Out {
                    amount: Fr::from(10000000000000000u64),
                    owner: vk.hash(),
                    data: Vec::new(),
                },
            oy: Out {
                    amount: Fr::from(10000000000000000u64),
                    owner: vk.hash(),
                    data: Vec::new(),
                },
        };  // TODO: Placeholder for txg
        let next0 = hex::decode("2092de7b23d178d6c8cf48debe44d6858554160e8eb95f5dba3aee5c3a564bd0").unwrap();
        // Generate a new block.
        let mut payload = Vec::new();
        let mut tx_ins = HashSet::new();
        for digest in self.buffer.drain() {
            let tx_bytes = self.store.read_tx(digest.to_vec()).await.expect("Failed to get tx from store").expect("Digest in buffer but not in store");
            let tx: Wp<Tx> = tx_bytes.as_slice().try_into().expect("Failed to convert tx bytes to Tx");
            if !tx_ins.insert(tx.val.ix) || !tx_ins.insert(tx.val.iy) || !self.utxo_cache.lock().await.check_tx(&tx) {
                warn!("Skipping double-spending transaction {:?}", digest);
                continue;
            }
            let verify_result = self.l0.lock().await.verify(&tx).await;
            if let Err(e) = verify_result {
                warn!("Skipping invalid transaction {:?}: {}", digest, e);
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
            txg.into(),
            (next0, [1u8; 32].to_vec()), // TODO: Placeholder for next
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

use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC};
use crate::synchronizer::Synchronizer;
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use tokio::sync::mpsc::{Receiver, Sender};
use rand::seq::IteratorRandom;
use store::Store;

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    Cleanup(Vec<Digest>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    store: Store,
    signature_service: SignatureService,
    synchronizer: Synchronizer,
    rx_mempool: Receiver<(Digest, Digest)>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    rx_loopback_propose: Receiver<Block>,
    buffer: HashMap<Digest, HashSet<Digest>>,
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        signature_service: SignatureService,
        synchronizer: Synchronizer,
        rx_mempool: Receiver<(Digest, Digest)>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
        rx_loopback_propose: Receiver<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                signature_service,
                synchronizer,
                rx_mempool,
                rx_message,
                tx_loopback,
                rx_loopback_propose,
                buffer: HashMap::new(),
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

    pub async fn get_last_committed_payload(&mut self) -> Vec<u8> {
        self.store.read(b"last_committed_payload".to_vec())
            .await
            .unwrap_or_else(|e| panic!("get store failed: {:?}", e))
            .unwrap_or_else(|| Digest::default().to_vec())
    }

    async fn make_block(&mut self, b: Block) {
        // TODO: empty the buffer
        let (b0, b1) = match self.synchronizer.get_ancestors(&b).await {
            Ok(Some(ancestors)) => ancestors,
            Ok(None) => {
                info!("Propose suspended: missing parent");
                return;
            }
            Err(e) => {
                panic!("Failed to get ancestors when propose block: {}", e);
            }
        };
        
        let prev_payload = if !b1.payload.is_empty() {
            &b1.payload[0]
        } else if !b0.payload.is_empty() {
            &b0.payload[0]
        } else {
            let last_committed_payload = self.get_last_committed_payload().await;
            &Digest::try_from(last_committed_payload.as_slice()).expect("Failed to convert to stored payload to Digest")
        };
        let payload = if let Some(digests) = self.buffer.get_mut(prev_payload) {
            digests.iter().choose(&mut rand::thread_rng()).cloned()
        } else {
            None
        };

        // Generate a new block.
        let block = Block::new(
            b.qc,
            b.tc,
            b.author,
            b.round,
            /* payload */ payload.into_iter().collect(),
            self.signature_service.clone(),
        )
        .await;

        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created {} -> {:?}", block, x);
            }
        }
        debug!("Created {:?}", block);

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
                Some((prev_digest, digest)) = self.rx_mempool.recv() => {
                    self.buffer.entry(prev_digest).or_default().insert(digest);
                },
                Some(block) = self.rx_loopback_propose.recv() => {
                    self.make_block(block).await;
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(Block{round, qc, tc, author: self.name, ..Default::default()}).await,
                    ProposerMessage::Cleanup(digests) => {
                        //TODO
                        for _ in &digests {
                            //self.buffer.remove(x);
                        }
                    }
                }
            }
        }
    }
}

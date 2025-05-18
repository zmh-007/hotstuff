use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC};
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
    rx_mempool: Receiver<(Digest, Digest)>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    buffer: HashMap<Digest, HashSet<Digest>>,
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        signature_service: SignatureService,
        rx_mempool: Receiver<(Digest, Digest)>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                signature_service,
                rx_mempool,
                rx_message,
                tx_loopback,
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

    pub async fn get_block(&mut self, key: Vec<u8>) -> Option<Vec<u8>> {
        match self.store.read(key).await {
            Ok(Some(data)) => Some(data),
            Ok(None) => {
                None
            },
            Err(e) => {
               panic!("get store failed: {:?}", e);
            }
        }
    }

    pub async fn get_last_committed_payload(&mut self) -> Vec<u8> {
        self.store.read(b"last_committed_payload".to_vec())
            .await
            .unwrap_or_else(|e| panic!("get store failed: {:?}", e))
            .unwrap_or_else(|| Digest::default().to_vec())
    }

    pub async fn find_prev_payload(&mut self, block_hash: Vec<u8>) -> Digest {
        // find this block
        if let Some(serialized) = self.get_block(block_hash).await {
            let block: Block = bincode::deserialize(&serialized).expect("Failed to deserialize block");
            if !block.payload.is_empty() {
                return block.payload[0].clone();
            }

            // find prev block
            if let Some(prev_serialized) = self.get_block(block.qc.hash.to_vec()).await {
                let prev_block: Block = bincode::deserialize(&prev_serialized)
                    .expect("Failed to deserialize previous block");
                if !prev_block.payload.is_empty() {
                    return prev_block.payload[0].clone();
                }
            }
            //TODO: process None, wait for sync?
        }
        //TODO: process None, wait for sync?

        // use last committed payload
        let last_committed_payload = self.get_last_committed_payload().await;
        Digest::try_from(last_committed_payload.as_slice()).expect("Failed to convert to stored payload to Digest")
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // TODO: empty the buffer
        let prev_payload = self.find_prev_payload(qc.hash.to_vec()).await;
        let payload = if let Some(digests) = self.buffer.get_mut(&prev_payload) {
            digests.iter().choose(&mut rand::thread_rng()).cloned()
        } else {
            None
        };
        // Generate a new block.
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
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

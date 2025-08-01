use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC};
use bytes::Bytes;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use l0::{Out, Tx};
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use zk::{Fr, Vk, ToHash};
use std::collections::HashSet;
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
        rx_mempool: Receiver<Digest>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
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
        let vk: Vk = hex::decode("8b6b3ab3c2ae37083056114669ceeabb60ce752f4e51f426fcc8bf949e4bc3923772394a558597aec6bb220cf706f13da6577be41cbc1a527c09f2535ff1e6072eb0f50e51ae976a886dbe28e2bfd286aac2001cf0e47562605aab843e8b851ea4a28a481e72db6a961c36362e559dd4dd265d26acc205d5d2dbc8870f1280017b432a6e639fe55162c3fdd6cf2ce487b755537e0feaa43c96d6870da2c9b1200f3d241a41d8c0c6407d0180e832b0807a167aeb5de10b4c7d0860cb340a2671b9041e54e98c25d9253e55217889f953a14e18d4e05c203d05efa889b2d82aa8e7c2fe6e3137f463208cd5aadf59f0fcb7ee753281ae21aa6ac7a50d956c7a3690354f9d403d2cd1296cace0684d44458f054d624dfca48c5669745577f18990a3c346697ad0e6c6a52fcdff8169fc74f3052bdcc2d16f6bcc99c87ffb9d2ec29983b15cca7888981bffa6b9e8a51a228c7cda7add9f88fc916faa8866ee3760ebe0aa25c6a5d52ecd52d85f1952f0cc522aa63f1a321fd0f2b7e2c9b8592e65907ace3a7e67f5b9653b6a3c4ceb77e0572b96fff7136974e2d5e5abc9caaba4f1f9ba195609cf1d1114107aa6313a9c0000000309").unwrap().as_slice().try_into().unwrap();
        let txg = Tx {
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
        // Generate a new block.
        let block = Block::new(
            qc,
            tc,
            self.name.clone(),
            round,
            /* payload */ self.buffer.drain().collect(),
            txg.into(),
            ([1u8; 32].to_vec(), [1u8; 32].to_vec()), // TODO: Placeholder for next
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

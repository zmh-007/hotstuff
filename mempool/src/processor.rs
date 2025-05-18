use crypto::Digest;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::payload_broadcaster::PayloadCommitment;
pub type PayloadMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_payload: Receiver<PayloadMessage>,
        // Output channel to send out payload' digests.
        tx_commitment: Sender<(Digest, Digest)>,
    ) {
        tokio::spawn(async move {
            while let Some(payload) = rx_payload.recv().await {
                // deserialize the payload.
                let payload_commitment: PayloadCommitment = bincode::deserialize(&payload).expect("Payload commitment deserialization failed");

                // Store the payload.
                let digest = payload_commitment.current_hash();
                store.write(digest.to_vec(), payload).await;

                tx_commitment.send((payload_commitment.prev_hash().clone(), digest.clone())).await.expect("Failed to send digest");
            }
        });
    }
}

use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Indicates a serialized `MempoolMessage::Batch` message.
pub type SerializedPayloadMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_payload: Receiver<SerializedPayloadMessage>,
        // Output channel to send out payload' digests.
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            while let Some(payload) = rx_payload.recv().await {
                // Hash the payload.
                let digest = Digest(Sha512::digest(&payload).as_slice()[..32].try_into().unwrap());

                // Store the payload.
                store.write(digest.to_vec(), payload).await;

                tx_digest.send(digest).await.expect("Failed to send digest");
            }
        });
    }
}

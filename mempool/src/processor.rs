use std::convert::TryInto;
use crypto::Digest;
use l0::Wp;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use zk::AsBytes;
use zk::ToHash;
use crate::mempool::{SerializedTransaction};
use l0::Tx;

pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_transaction: Receiver<SerializedTransaction>,
        // Output channel to send out tx' digests.
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            while let Some(tx_bytes) = rx_transaction.recv().await {
                let tx = Wp::<Tx>::dec(&mut tx_bytes.clone().into_iter()).expect("Failed to decode wp transaction");
                
                // Hash the transaction.
                let h: Vec<u8> = tx.val.hash().enc().collect();
                let hash = Digest(h.try_into().expect("Failed to convert transaction hash bytes to digest"));

                // Store the transaction.
                store.write_tx(hash.to_vec(), tx_bytes).await;

                tx_digest.send(hash).await.expect("Failed to send digest");
            }
        });
    }
}

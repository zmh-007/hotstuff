use std::convert::TryFrom;
use std::convert::TryInto;
use crypto::Digest;
use l0::Wp;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use zk::ToHash;
use zk::FrSerialization;
use crate::mempool::{SerializedTransaction};
use l0::Tx;
use log::error;

pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_transaction: Receiver<SerializedTransaction>,
        // Output channel to send out tx' digests.
        tx_digest: Sender<Digest>,
        // Channel to verify tx from node
        tx_verify: Sender<(Wp<Tx>, oneshot::Sender<bool>)>,
    ) {
        tokio::spawn(async move {
            while let Some(tx_bytes) = rx_transaction.recv().await {
                let tx: Wp<Tx> = Wp::try_from(&tx_bytes[..]).expect("Failed to deserialize transaction from bytes");
                
                // verify tx
                let (s, r) = oneshot::channel();
                tx_verify.send((tx.clone(), s)).await.expect("Failed to send tx to verify");
                let valid = r.await.unwrap();
                // Hash the transaction.
                let mut b = Vec::new();
                tx.val.hash().serialize_be_compressed(&mut b).expect("Failed to serialize transaction hash to bytes");
                let hash = Digest(b.try_into().expect("Failed to convert transaction hash bytes to digest"));
                if !valid {
                    error!("tx verify failed: {:?}", hash);
                    continue;
                }

                // Store the transaction.
                store.write_tx(hash.to_vec(), tx_bytes).await;

                tx_digest.send(hash).await.expect("Failed to send digest");
            }
        });
    }
}

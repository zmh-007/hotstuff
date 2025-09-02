use std::{array::from_fn, collections::HashMap, io::{Read, Write}};
use std::convert::TryFrom;
use anyhow::{Error, Result, anyhow};
use async_trait::async_trait;
use l0::Out;
use l0::StateStore;
use log::{debug, error, info, warn};
use zk::{AdditiveGroup, Fr, FrSerialization, ToHash};
use crate::store::Store;
use hex_str::HexString;

#[derive(Clone, Debug)]
pub enum StateNode {
    Branch((Fr, Fr)), // left and right
    Leaf(Out)
}


impl ToHash<true> for StateNode {
    fn hash(self) -> Fr {
        match self {
            Self::Branch(v) => v.hash(),
            Self::Leaf(out) => out.hash(),
        }
    }
}

impl From<&StateNode> for Vec<u8> {
    fn from(value: &StateNode) -> Self {
        let mut bytes = Vec::new();
        match value {
            StateNode::Branch((left, right)) => {
                bytes.write_all(&[0u8]).unwrap();
                left.serialize_be_compressed(&mut bytes).unwrap();
                right.serialize_be_compressed(&mut bytes).unwrap();
            }
            StateNode::Leaf(out) => {
                bytes.write_all(&[1u8]).unwrap();
                out.amount.serialize_be_compressed(&mut bytes).unwrap();
                out.owner.serialize_be_compressed(&mut bytes).unwrap();
                bytes.write_all(&u32::to_be_bytes(out.data.len() as u32)).unwrap();
                out.data.iter().for_each(|v| v.serialize_be_compressed(&mut bytes).unwrap());
            }
        }
        bytes
    }
}

fn fr_to_path(fr: Fr) -> [bool; 256] {
    let bytes = fr.to_hex();
    from_fn(|i| { bytes[i/8] & (1 << (i%8)) > 0 })
}

impl TryFrom<&[u8]> for StateNode {
    type Error = Error;
    fn try_from(mut value: &[u8]) -> Result<Self> {
        let mut node_type = [0u8; 1];
        value.read_exact(&mut node_type).map_err(|err| anyhow!("Failed read size: {}", err))?;
        match node_type[0] {
            0 => {
                let left = Fr::deserialize_be_compressed(&mut value)?;
                let right = Fr::deserialize_be_compressed(&mut value)?;
                Ok(StateNode::Branch((left, right)))
            },
            1 => {
                let out = {
                    let amount = Fr::deserialize_be_compressed(&mut value)?;
                    let owner= Fr::deserialize_be_compressed(&mut value)?;
                    let mut data_len_bytes = [0u8; 4];
                    value.read_exact(&mut data_len_bytes).map_err(|err| anyhow!("Failed read size: {}", err))?;
                    let data_len = u32::from_be_bytes(data_len_bytes);
                    let mut out = Out { amount, owner, data: Vec::with_capacity(data_len as usize) };
                    for _ in 0..data_len {
                        let item = Fr::deserialize_be_compressed(&mut value)?;
                        out.data.push(item);
                    }
                    out
                };
                Ok(StateNode::Leaf(out))
            },
            node_type @ _ => Err(anyhow!("invalid type {}", node_type))
        }
    }
}


pub struct L0State {
    root: Fr,
    store: Store,
    state: HashMap<Fr, StateNode>,
    dirty_updated: HashMap<Fr, StateNode>,
    dirty_removed: HashMap<Fr, Option<Fr>>,
}


impl L0State {
    pub fn new(store: Store) -> Self {
        let mut state = HashMap::new();
        // Fill with merkle nodes with zero leafs
        let root = (0..256).fold(Fr::ZERO, |left, _| {
            let hash = (left, left).hash();
            state.insert(hash, StateNode::Branch((left, left)));
            hash
        });

        Self {
            root,
            store,
            state,
            dirty_updated: HashMap::new(),
            dirty_removed: HashMap::new(),
        }
    }

    // Read key from state if cached, or from Store and cache it
    async fn read(&mut self, key: Fr) -> Result<Option<StateNode>> {
        if let Some(node) = self.state.get(&key) {
            return Ok(Some(node.clone()));
        }

        match self.store.read(key.to_hex().into()).await {
            Some(v) => {
                let node = StateNode::try_from(v.as_slice())?;
                self.state.insert(key, node.clone());
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    // Update the key in state and in dirty_updated
    fn update(&mut self, key: Fr, value: StateNode) {
        self.state.insert(key, value.clone());
        self.dirty_updated.insert(key, value);
    }

    // Delete the key from state and mark it in dirty_removed
    fn remove(&mut self, key: Fr) {
        if let Some(StateNode::Leaf(out)) = self.state.remove(&key) {
            self.dirty_removed.insert(key, Some(out.owner));
        } else {
            self.dirty_removed.insert(key, None);
        }
    }

    // read the leaf value from the state, with key as the merkle path
    // return the leaf value which is a hash and a merkle proof
    async fn get_leaf_with_root(&mut self, mut root: Fr, key: Fr) -> Result<(Fr, [Fr; 256])> {
        let mut proof = [Fr::ZERO; 256];
        let path = fr_to_path(key);

        for (i, bit) in path.iter().enumerate() {
            let node = self.read(root).await?;
            if let Some(StateNode::Branch((left, right))) = node {
                if *bit {
                    proof[i] = left;
                    root = right;
                } else {
                    proof[i] = right;
                    root = left;
                }
            } else {
                return Err(anyhow!("Invalid path"));
            }
        }
        Ok((root, proof))
    }

    async fn update_leaf_with_root(&mut self, root: Fr, key: Fr, value: Fr) -> Result<Fr> {
        let (_, proof) = self.get_leaf_with_root(root, key).await?;
        self.update_leaf_with_proof(key, proof, value).await
    }

    // update the leaf value in the state with the key as the merkle path
    // return the updated root
    async fn update_leaf_with_proof(&mut self, key: Fr, proof: [Fr; 256], value: Fr) -> Result<Fr> {
        let path = fr_to_path(key);
        let mut current_hash = value;

        for (i, bit) in path.iter().enumerate().rev() {
            let (left, right) = if *bit {
                (proof[i], current_hash)
            } else {
                (current_hash, proof[i])
            };
            current_hash = (left, right).hash();
            self.update(current_hash, StateNode::Branch((left, right)));
        }

        Ok(current_hash)
    }

    // root is a merkle tree root with depth 256
    // so the leaf is hash: either zero(means its empty) or non-zero(hash of Out)
    // non-leaf node is a hash of child nodes hash(left, right)
    // so this fn will try to read the leaf value from state, with key as the merkle path
    pub async fn get_utxo_with_root(&mut self, root: Fr, key: Fr) -> Result<Option<Out>> {
        let (leaf, _) = self.get_leaf_with_root(root, key).await?;
        if leaf == Fr::ZERO {
            Ok(None)
        } else {
            let out = self.read(leaf).await?;
            match out {
                Some(StateNode::Leaf(out)) => Ok(Some(out)),
                out @ _ => Err(anyhow!("unexpected read result {:?}", out))
            }
        }
    }

    // root is a merkle tree root with depth 256
    // so this fn will try check if leaf exist, with key as the merkle path
    pub async fn check_utxo_exist_with_root(&mut self, root: Fr, key: Fr) -> Result<bool> {
        Ok(self.get_leaf_with_root(root, key).await?.0 != Fr::ZERO)
    }

    pub async fn insert_utxo_with_root(&mut self, root: Fr, key: Fr, out: Out) -> Result<Fr> {
        let leaf = out.clone().hash();
        self.update(leaf, StateNode::Leaf(out.clone()));
        self.update_leaf_with_root(root, key, leaf).await
    }

    pub async fn delete_utxo_with_root(&mut self, root: Fr, key: Fr) -> Result<Fr> {
        let (leaf, _) = self.get_leaf_with_root(root, key).await?;
        if leaf != Fr::ZERO {
            self.remove(leaf);
        }
        self.update_leaf_with_root(root, key, Fr::ZERO).await
    }

    pub async fn save_account_utxo(&self, account: Fr, key: Fr) -> Result<()> {
        let mut store = self.store.clone();
        let state = store.get_account_state(account, account).await.unwrap_or(AccountStoreValue::LinkedListHead(Fr::ZERO));
        match state {
            AccountStoreValue::LinkedListHead(head) => {
                store.write_account_state(account, account, &AccountStoreValue::LinkedListHead(key)).await;
                store.write_account_state(account, key, &AccountStoreValue::LinkedListNode(Fr::ZERO, head)).await;

                if head != Fr::ZERO {
                    if let Some(AccountStoreValue::LinkedListNode(_, next)) = store.get_account_state(account, head).await {
                        store.write_account_state(account, head, &AccountStoreValue::LinkedListNode(key, next)).await;
                    } else {
                        warn!("unexpected account entry missing")
                    }                    
                }
            },
            _ => unreachable!(),
        }
        Ok(())
    }

    pub async fn delete_account_utxo(&self, account: Fr, key: Fr) -> Result<()> {
        let mut store = self.store.clone();
        let state = store.get_account_state(account, key).await;
        match state {
            Some(AccountStoreValue::LinkedListNode(prev, next)) => {
                store.delete_account_entry(account, key).await;
                if prev == Fr::ZERO {
                    if let Some(AccountStoreValue::LinkedListHead(head)) = store.get_account_state(account, account).await {
                        if head == key {
                            store.write_account_state(account, account, &AccountStoreValue::LinkedListHead(next)).await;
                        }
                    }
                } else {
                    if let Some(AccountStoreValue::LinkedListNode(prev_prev, _)) = store.get_account_state(account, prev).await {
                        store.write_account_state(account, prev, &AccountStoreValue::LinkedListNode(prev_prev, next)).await;
                    }
                }
                if next != Fr::ZERO {
                    if let Some(AccountStoreValue::LinkedListNode(_prev, following)) = store.get_account_state(account, next).await {
                        store.write_account_state(account, next, &AccountStoreValue::LinkedListNode(prev, following)).await;
                    }
                }
            },
            None => {},
            _ => unreachable!(),
        }
        Ok(())
    }
}

#[async_trait]
impl StateStore for L0State {
    async fn get(&mut self, key: &Fr) -> Option<Out> {
        self.get_utxo_with_root(self.root, *key).await.expect("unexpected state missing")
    }

    async fn insert(&mut self, key: Fr, value: Out) {
        let prev = self.root;
        self.root = self.insert_utxo_with_root(self.root, key, value).await.expect("unexpected failed insert");
        debug!("utxo root transition: {} -> {}", prev.to_hex(), self.root.to_hex())
    }

    async fn remove(&mut self, key: &Fr) {
        let prev = self.root;
        self.root = self.delete_utxo_with_root(self.root, *key).await.expect("unexpected failed store remove");
        debug!("utxo root transition: {} -> {}", prev.to_hex(), self.root.to_hex())
    }

    async fn flush(&mut self) {
        for (key, value) in self.dirty_updated.iter() {
            let bytes: Vec<u8> = value.into();
            self.store.write(key.to_hex().into(), bytes).await;
            if let StateNode::Leaf(out) = value {
                if let Err(err) = self.save_account_utxo(out.owner, *key).await {
                    error!("State save failure {err}");
                };
            }
        }
        self.dirty_updated.clear();

        for (key, owner) in self.dirty_removed.iter() {
            self.store.delete(key.to_hex().into()).await;
            if let Some(owner) = owner {
                if let Err(err) = self.delete_account_utxo(*owner, *key).await {
                    error!("State save failure {err}");
                }
            }
        }
        self.dirty_removed.clear();
    }

    async fn load(&mut self) -> Option<(Fr, Fr, Fr)> {
        let (root, tail, gov, price) = self.store.get_chain_state().await?;
        info!("Loaded from local state");
        info!(" - utxo root: {}", root.to_hex());
        info!(" - tx tail  : {}", tail.to_hex());
        info!(" - gov key  : {}", gov.to_hex());
        info!(" - gov price: {}", price.to_hex());

        self.root = root;
        Some((tail, gov, price))
    }
    async fn save(&mut self, tail: Fr, gov: Fr, price: Fr) {
        info!("Saving into local state");
        info!(" - utxo root: {}", self.root.to_hex());
        info!(" - tx tail  : {}", tail.to_hex());
        info!(" - gov key  : {}", gov.to_hex());
        info!(" - gov price: {}", price.to_hex());
        self.store.write_chain_state((self.root, tail, gov, price)).await;
    }

    async fn get_account_utxos(&mut self, account: &Fr) -> HashMap<Fr, Out> {
        // TODO: pagination
        let mut utxos = HashMap::new();
        if let Some(AccountStoreValue::LinkedListHead(mut head)) = self.store.get_account_state(*account, *account).await {
            while let Some(AccountStoreValue::LinkedListNode(_, next)) = self.store.get_account_state(*account, head).await {
                if !self.dirty_removed.contains_key(&head) {
                    if let Ok(Some(StateNode::Leaf(out))) = self.read(head).await {
                        utxos.insert(head, out);
                    }
                }
                head = next;
                if next == Fr::ZERO {
                    break
                }
            }
        }
        utxos
    }
}

pub enum AccountStoreValue {
    LinkedListHead(Fr),
    LinkedListNode(Fr, Fr),   // hash(account, utxo_hash) => prev, next
}

impl From<&AccountStoreValue> for Vec<u8> {
    fn from(value: &AccountStoreValue) -> Self {
        let mut bytes = Vec::new();
        match value {
            AccountStoreValue::LinkedListHead(head) => {
                bytes.write_all(&[0u8]).unwrap();
                head.serialize_be_compressed(&mut bytes).unwrap();
            }
            AccountStoreValue::LinkedListNode(prev, next) => {
                bytes.write_all(&[1u8]).unwrap();
                prev.serialize_be_compressed(&mut bytes).unwrap();
                next.serialize_be_compressed(&mut bytes).unwrap();
            }
        }
        bytes
    }
}

impl TryFrom<&[u8]> for AccountStoreValue {
    type Error = Error;
    fn try_from(mut value: &[u8]) -> Result<Self> {
        let mut node_type = [0u8; 1];
        value.read_exact(&mut node_type).map_err(|err| anyhow!("Failed read size: {}", err))?;
        match node_type[0] {
            0 => {
                let head = Fr::deserialize_be_compressed(&mut value)?;
                Ok(Self::LinkedListHead(head))
            },
            1 => {
                let prev = Fr::deserialize_be_compressed(&mut value)?;
                let next= Fr::deserialize_be_compressed(&mut value)?;
                Ok(Self::LinkedListNode(prev, next))
            },
            node_type @ _ => Err(anyhow!("invalid type {}", node_type))
        }
    }
}

pub trait HexConverter: Sized {
    fn from_hex(value: HexString) -> Result<Self>;
    fn to_hex(&self) -> HexString;
}

impl HexConverter for Fr {
    fn from_hex(value: HexString) -> Result<Self> {
        let reader = value.as_slice();
        Fr::deserialize_be_compressed(reader).map_err(|err| anyhow!("{}", err))
    }

    fn to_hex(&self) -> HexString {
        let mut bytes = Vec::new();
        self.serialize_be_compressed(&mut bytes).unwrap();
        HexString::new(bytes)
    }
}

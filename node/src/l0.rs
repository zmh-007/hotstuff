use anyhow::Context;
use anyhow::Error;
use anyhow::anyhow;
use anyhow::Result;
use anyhow::ensure;
use consensus::FullBlock;
use l0::Out;
use l0::Tx;
use l0::Wp;
use zk::FrSerialization;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use std::io::Write;
use std::convert::{TryInto, TryFrom};
use zk::AdditiveGroup;
use zk::Fr;
use zk::ToHash;
use zk::Zero;
#[derive(Debug)]
pub struct L0 {
    utxos: HashMap<Fr, Out>,
    tail: Fr,
    gov: Fr,
    price: Fr,
}
impl L0 {
    pub fn new(gov: Fr, price: Fr) -> Self { Self { utxos: HashMap::new(), tail: Fr::ZERO, gov, price } }
    pub fn exec(&mut self, tx: Tx) {
        self.tail = (self.tail, tx.clone().hash()).hash();
        [((Fr::ZERO, self.tail).hash(), tx.ox), ((self.tail, Fr::ZERO).hash(), tx.oy)].iter().filter(|(_, v)| !v.amount.is_zero()).for_each(|(k, v)| _ = self.utxos.insert(k.clone(), v.clone()));
        [tx.ix, tx.iy].iter().for_each(|v| _ = self.utxos.remove(&v));
    }
    pub fn verify(&mut self, tx: &Wp<Tx>) -> Result<()> {
        let owner = tx.vk.hash();
        let amti = [tx.val.ix, tx.val.iy].iter().filter(|v| !v.is_zero()).try_fold(Fr::ZERO, |val, v| self.utxos.get(&v).and_then(|v| if v.owner == owner { Some(v.amount + val) } else { None })).context("invalid in")?;
        let amto = [&tx.val.ox, &tx.val.oy].iter().try_fold(Fr::ZERO, |val, v| if v.amount >= self.price * Fr::from(v.data.len() as u64) { Some(v.amount + val) } else { None }).context("invalid out")?;
        ensure!(amti >= amto && amto >= tx.val.ox.amount && amto >= tx.val.oy.amount);
        tx.check()
    }
    pub fn block(&mut self, blk: FullBlock) -> Result<()> {
        let gov = Fr::deserialize_be_compressed(&blk.next.0[..]).expect("failed to deserialize gov");
        ensure!(gov == self.gov);
        let last_tail = Fr::deserialize_be_compressed(&blk.qc.last_tail.to_vec()[..]).expect("failed to deserialize last tail");
        ensure!( last_tail == self.tail);
        let r = blk.payload.iter().try_fold((HashSet::new(), Vec::new()), |(mut set, mut txs), v| {
            let tx: Wp<Tx> = v.as_slice().try_into().expect("Failed to convert payload to Tx");
            ensure!(set.insert(tx.val.ix));
            ensure!(set.insert(tx.val.iy));
            self.verify(&tx)?;
            txs.push(tx);
            Ok((set, txs))
        })?;
        r.1.into_iter().for_each(|v| self.exec(v.val));
        let txg: Tx = blk.txg.as_slice().try_into().expect("Failed to convert txg to Tx");
        self.exec(txg);
        self.gov = gov;
        self.price = Fr::deserialize_be_compressed(&blk.next.1[..]).expect("failed to deserialize price");
        Ok(())
    }
}

impl From<&L0> for Vec<u8> {
    fn from(value: &L0) -> Self {
        let mut bytes = Vec::new();
        value.tail.serialize_be_compressed(&mut bytes).unwrap();
        value.gov.serialize_be_compressed(&mut bytes).unwrap();
        value.price.serialize_be_compressed(&mut bytes).unwrap();
        bytes.write_all(&u32::to_be_bytes(value.utxos.len() as u32)).unwrap();
        for (k, v) in value.utxos.iter() {
            k.serialize_be_compressed(&mut bytes).unwrap();
            v.amount.serialize_be_compressed(&mut bytes).unwrap();
            v.owner.serialize_be_compressed(&mut bytes).unwrap();
            bytes.write_all(&u32::to_be_bytes(v.data.len() as u32)).unwrap();
            v.data.iter().for_each(|v| v.serialize_be_compressed(&mut bytes).unwrap());
        }
        bytes
    }
}

impl TryFrom<&[u8]> for L0 {
    type Error = Error;
    fn try_from(mut value: &[u8]) -> Result<Self> {
        let tail = Fr::deserialize_be_compressed(&mut value)?;
        let gov = Fr::deserialize_be_compressed(&mut value)?;
        let price = Fr::deserialize_be_compressed(&mut value)?;
        let mut len_bytes = [0u8; 4];
        value.read_exact(&mut len_bytes).map_err(|err| anyhow!("Failed read size: {}", err))?;
        let utxos_len = u32::from_be_bytes(len_bytes);
        let mut utxos = HashMap::with_capacity(utxos_len as usize);
        for _ in 0..utxos_len {
            let key = Fr::deserialize_be_compressed(&mut value)?;
            let amount = Fr::deserialize_be_compressed(&mut value)?;
            let owner = Fr::deserialize_be_compressed(&mut value)?;
            let mut len_bytes = [0u8; 4];
            value.read_exact(&mut len_bytes).map_err(|err| anyhow!("Failed read size: {}", err))?;
            let data_len = u32::from_be_bytes(len_bytes);
            let mut out = Out { amount, owner, data: Vec::with_capacity(data_len as usize) };
            for _ in 0..data_len {
                let item = Fr::deserialize_be_compressed(&mut value)?;
                out.data.push(item);
            }
            utxos.insert(key, out);
        }
        Ok(L0 { utxos, tail, gov, price })
    }
}

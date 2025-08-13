use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use consensus::FullBlock;
use l0::Out;
use l0::Tx;
use l0::Wp;
use zk::FrSerialization;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
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

use crate::config::Committee;
use crate::consensus::{Round, ToField};
use crate::error::{ConsensusError, ConsensusResult};
use blst::min_pk::AggregatePublicKey;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use l0::{Tx, Wp};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt;
use zk::{AdditiveGroup, AsBytes, Fr, ToHash};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct FullBlock {
    pub qc: QC,
    pub tc: Option<TC>,
    pub author: PublicKey,
    pub round: Round,
    pub payload: Vec<Vec<u8>>,
    pub txg: Vec<u8>,
    pub next: (Vec<u8>, Vec<u8>),
    pub signature: Signature,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub qc: QC,
    pub tc: Option<TC>,
    pub author: PublicKey,
    pub round: Round,
    pub payload: Vec<Digest>,
    pub txg: Vec<u8>,
    pub next: (Vec<u8>, Vec<u8>),
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        qc: QC,
        tc: Option<TC>,
        author: PublicKey,
        round: Round,
        payload: Vec<Digest>,
        txg: Vec<u8>,
        next: (Vec<u8>, Vec<u8>),
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            qc,
            tc,
            author,
            round,
            payload,
            txg,
            next,
            signature: Signature::default(),
        };
        let sig = signature_service.request_signature(block.digest()).await;
        Self { signature: sig, ..block }
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn parent(&self) -> &Digest {
        &self.qc.hash
    }

    pub fn tx_tail(&self) -> Digest {
        let txg = Tx::dec(&mut self.txg.clone().into_iter()).expect("Failed to decode txg");
        let fr_iter = std::iter::once(
        Fr::dec(&mut self.qc.last_tail.to_vec().into_iter())
            .expect("Failed to deserialize last_tail")
        )
        .chain(
            self.payload.iter().map(|v| {
                Fr::dec(&mut v.to_vec().into_iter())
                    .expect("Failed to deserialize tx hash to Fr")
            })
        )
        .chain(
            std::iter::once(
                txg.hash()
            )
        );
        let tx_tail = fr_iter.reduce(|a, b| (a, b).hash()).expect("Iterator cannot be empty");
        let b: Vec<u8> = tx_tail.enc().collect();
        Digest(b.try_into().expect("Failed to convert transaction tail hash bytes to digest"))
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the author proof.
        verify_signature(&self.digest(), &self.author, &self.signature)?;

        // Check the embedded QC.
        if self.qc != QC::genesis() {
            self.qc.verify(committee)?;
        }

        // Check the TC embedded in the block (if any).
        if let Some(ref tc) = self.tc {
            tc.verify(committee)?;
        }
        Ok(())
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let tx_tail = self.payload.iter().map(|v| {Fr::dec(&mut v.to_vec().into_iter()).expect("Failed to deserialize tx hash to Fr")}).hash();
        let txg = Tx::dec(&mut self.txg.clone().into_iter()).expect("Failed to decode txg");
        let elements = vec![
            self.author.to_hash(),
            self.round.to_field(),
            self.qc.hash.to_field(),
            self.qc.last_tail.to_field(),
            txg.hash(),
            Fr::dec(&mut self.next.0.clone().into_iter()).expect("Failed to deserialize next1 to Fr"),
            Fr::dec(&mut self.next.1.clone().into_iter()).expect("Failed to deserialize next2 to Fr"),
            tx_tail,
        ];

        let b: Vec<u8> = elements.hash().enc().collect();
        Digest(b.try_into().expect("Failed to convert block hash bytes to digest"))
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B({}, {}, {:?}, {})",
            self.digest(),
            self.author,
            self.round,
            self.qc,
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}", self.round)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub hash: Digest,
    pub round: Round,
    pub tx_tail: Digest,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        block: &Block,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let vote = Self {
            hash: block.digest(),
            round: block.round,
            tx_tail: block.tx_tail(),
            author,
            signature: Signature::default(),
        };
        let sig = signature_service.request_signature(vote.digest()).await;
        Self { signature: sig, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the proof.
        verify_signature(&self.digest(), &self.author, &self.signature)?;
        Ok(())
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let elements = vec![
            self.hash.to_field(),
            self.round.to_field(),
            self.tx_tail.to_field(),
        ];
        let b: Vec<u8> = elements.hash().enc().collect();
        Digest(b.try_into().expect("Failed to convert vote hash bytes to digest"))
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "V({}, {}, {}, {})", self.author, self.round, self.hash, self.tx_tail)
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub hash: Digest,
    pub round: Round,
    pub last_tail: Digest,
    pub votes: Vec<(PublicKey, Signature)>,
    pub aggregated_pk: PublicKey, 
    pub aggregated_signature: Signature,
}

impl QC {
    pub fn genesis() -> Self {
        QC::default()
    }

    pub fn timeout(&self) -> bool {
        self.hash == Digest::default() && self.round != 0
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::QCRequiresQuorum
        );

        // Check the signature.
        for (author, sig) in &self.votes {
            verify_signature(&self.digest(), author, sig)?;
        }
        // Check the aggregated pk.
        let mut public_keys = Vec::with_capacity(self.votes.len());
        for (pk, _) in self.votes.iter() {
            public_keys.push(blst::min_pk::PublicKey::from_bytes(&pk.0).expect("Invalid public key bytes"));
        }
        let pks: Vec<_> = public_keys.iter().collect();
        let aggregated_pk = AggregatePublicKey::aggregate(&pks, true).expect("failed to aggregate public keys");
        assert_eq!(self.aggregated_pk, PublicKey(aggregated_pk.to_public_key().to_bytes()), "Aggregated public key does not match the expected value");
        // Check the aggregated signature.
        verify_signature(&self.digest(), &self.aggregated_pk, &self.aggregated_signature)?;
        Ok(())
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let elements = vec![
            self.hash.to_field(),
            self.round.to_field(),
            self.last_tail.to_field(),
        ];
        let b: Vec<u8> = elements.hash().enc().collect();
        Digest(b.try_into().expect("Failed to convert qc hash bytes to digest"))
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "QC({}, {}, {})", self.hash, self.round, self.last_tail)
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.round == other.round
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Timeout {
    pub high_qc: QC,
    pub round: Round,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Timeout {
    pub async fn new(
        high_qc: QC,
        round: Round,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let timeout = Self {
            high_qc,
            round,
            author,
            signature: Signature::default(),
        };
        let sig = signature_service.request_signature(timeout.digest()).await;
        Self {signature: sig, ..timeout}
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the proof.
        verify_signature(&self.digest(), &self.author, &self.signature)?;

        // Check the embedded QC.
        if self.high_qc != QC::genesis() {
            self.high_qc.verify(committee)?;
        }
        Ok(())
    }
}

impl Hash for Timeout {
    fn digest(&self) -> Digest {
        let elements = vec![
            self.round.to_field(),
            self.high_qc.round.to_field(),
        ];
        let b: Vec<u8> = elements.hash().enc().collect();
        Digest(b.try_into().expect("Failed to convert timeout hash bytes to digest"))
    }
}

impl fmt::Debug for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TV({}, {}, {:?})", self.author, self.round, self.high_qc)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TC {
    pub round: Round,
    pub votes: Vec<(PublicKey, Signature, Round)>,
}

impl TC {
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _, _) in self.votes.iter() {
            ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::TCRequiresQuorum
        );

        // Check the proofs.
        for (author, sig, high_qc_round) in &self.votes {
            let elements = vec![
                self.round.to_field(),
                high_qc_round.to_field(),
            ];
            let b: Vec<u8> = elements.hash().enc().collect();
            let digest = Digest(b.try_into().expect("Failed to convert tc vote hash bytes to digest"));

            verify_signature(&digest, author, sig)?;
        }
        Ok(())
    }

    pub fn high_qc_rounds(&self) -> Vec<Round> {
        self.votes.iter().map(|(_, _, r)| r).cloned().collect()
    }
}

impl fmt::Debug for TC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TC({}, {:?})", self.round, self.high_qc_rounds())
    }
}

fn verify_signature(digest: &Digest, author: &PublicKey, sig: &Signature) -> ConsensusResult<()>{
    let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
    let signature = blst::min_pk::Signature::from_bytes(&sig.0).expect("Invalid signature bytes");
    let pk = blst::min_pk::PublicKey::from_bytes(&author.0).expect("Invalid public key bytes");
    let err = signature.verify(true, &digest.to_vec(), dst, &[], &pk, true);
    if err != blst::BLST_ERROR::BLST_SUCCESS {
        return Err(ConsensusError::InvalidSignature(author.to_string()));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub enum WebSocketEvent {
    BroadcastChainUpdate { hash: Vec<u8> },
}

#[derive(Serialize, Deserialize, Default)]
pub struct UTXOCache {
    pub cache: HashMap<Digest, HashSet<Digest>>,
}

impl UTXOCache {
    pub fn check_tx(&self, tx: &Wp<Tx>) -> bool {
        // Skip ix check if it's 0, otherwise serialize and compute digest
        let check_ix = tx.val.ix != Fr::ZERO;
        let digest_ix = if check_ix {
            let ix: Vec<u8> = tx.val.ix.enc().collect();
            Some(Digest(ix.try_into().expect("Failed to convert ix bytes to Digest")))
        } else {
            None
        };

        // Skip iy check if it's 0, otherwise serialize and compute digest
        let check_iy = tx.val.iy != Fr::ZERO;
        let digest_iy = if check_iy {
            let iy: Vec<u8> = tx.val.iy.enc().collect();
            Some(Digest(iy.try_into().expect("Failed to convert iy bytes to Digest")))
        } else {
            None
        };

        // Check cache for non-zero digests
        self.cache.values().all(|set| {
            (!check_ix || !set.contains(&digest_ix.as_ref().unwrap())) &&
            (!check_iy || !set.contains(&digest_iy.as_ref().unwrap()))
        })
    }
}

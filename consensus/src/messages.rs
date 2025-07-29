use crate::config::Committee;
use crate::consensus::{Round, ToHash};
use crate::error::{ConsensusError, ConsensusResult};
use blst::min_pk::AggregatePublicKey;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use placeholder_project_name_placeholder_zk::hash::poseidon::PoseidonHash;
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::fmt;
use placeholder_project_name_placeholder_zk::plonk::config::Hasher;
use placeholder_project_name_placeholder_zk::hash::hash_types::HashOut;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub qc: QC,
    pub tc: Option<TC>,
    pub author: PublicKey,
    pub round: Round,
    pub payload: Vec<Digest>,
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        qc: QC,
        tc: Option<TC>,
        author: PublicKey,
        round: Round,
        payload: Vec<Digest>,
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            qc,
            tc,
            author,
            round,
            payload,
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
        let tx_tail = self.payload.iter().fold(self.qc.last_tail.0, |x, y| PoseidonHash::two_to_one(x, y.0));
        Digest(tx_tail)
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the author proof.
        verify_signature(&self.digest(), &self.author, &self.signature);

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
        let h1= PoseidonHash::two_to_one(self.author.to_hash(), self.round.to_hash());
        let h2 = PoseidonHash::two_to_one(h1, HashOut::from_vec(self.qc.hash.to_vec_field()));
        let h3 = PoseidonHash::two_to_one(h2, HashOut::from_vec(self.qc.last_tail.to_vec_field()));
        let tx_tail = self.payload.iter().fold(self.qc.last_tail.0, |x, y| PoseidonHash::two_to_one(x, y.0));
        let h4 = PoseidonHash::two_to_one(h3, tx_tail);
        Digest(h4)
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
        verify_signature(&self.digest(), &self.author, &self.signature);
        Ok(())
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let h1= PoseidonHash::two_to_one(HashOut::from_vec(self.hash.to_vec_field()), self.round.to_hash());
        let h2= PoseidonHash::two_to_one(h1, HashOut::from_vec(self.tx_tail.to_vec_field()));
        Digest(h2)
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
            verify_signature(&self.digest(), author, sig);
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
        verify_signature(&self.digest(), &self.aggregated_pk, &self.aggregated_signature);
        Ok(())
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let h1= PoseidonHash::two_to_one(HashOut::from_vec(self.hash.to_vec_field()), self.round.to_hash());
        let h2= PoseidonHash::two_to_one(h1, HashOut::from_vec(self.last_tail.to_vec_field()));
        Digest(h2)
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
        verify_signature(&self.digest(), &self.author, &self.signature);

        // Check the embedded QC.
        if self.high_qc != QC::genesis() {
            self.high_qc.verify(committee)?;
        }
        Ok(())
    }
}

impl Hash for Timeout {
    fn digest(&self) -> Digest {
        let h1= PoseidonHash::two_to_one(self.round.to_hash(), self.high_qc.round.to_hash());
        Digest(h1)
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
            let h1= PoseidonHash::two_to_one(self.round.to_hash(), high_qc_round.to_hash());
            let digest = Digest(h1);
            verify_signature(&digest, author, sig);
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

fn verify_signature(digest: &Digest, author: &PublicKey, sig: &Signature) {
    let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
    let signature = blst::min_pk::Signature::from_bytes(&sig.0).expect("Invalid signature bytes");
    let pk = blst::min_pk::PublicKey::from_bytes(&author.0).expect("Invalid public key bytes");
    let err = signature.verify(true, &digest.to_vec(), dst, &[], &pk, true);
    assert_eq!(err, blst::BLST_ERROR::BLST_SUCCESS);
}

// fn verify_proof(digest: &Digest, author: &Digest, committee: &Committee, proof: Proof<GoldilocksField, PoseidonGoldilocksConfig, 2>) {
//     let vd_encoded = committee.authorities.get(author).map(|auth| auth.vd.clone()).unwrap();
//     let vd_decoded = general_purpose::STANDARD.decode(&vd_encoded).unwrap();
//     let vd = VerifierCircuitData::from_bytes(vd_decoded, &DefaultGateSerializer).unwrap();
//     vd.verify(ProofWithPublicInputs { proof: proof.into(), public_inputs: digest.to_vec_field() }).expect("proof verification failed");
// }
use crate::config::{Committee, Stake};
use crate::consensus::Round;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{Timeout, Vote, QC, TC};
use std::collections::{HashMap, HashSet};
use blst::min_pk::{AggregatePublicKey, AggregateSignature};
use crypto::{Digest, Hash, PublicKey, Signature};

pub struct Aggregator {
    committee: Committee,
    votes_aggregators: HashMap<Round, HashMap<Digest, Box<QCMaker>>>,
    timeouts_aggregators: HashMap<Round, Box<TCMaker>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            votes_aggregators: HashMap::new(),
            timeouts_aggregators: HashMap::new(),
        }
    }

    pub fn add_vote(&mut self, vote: Vote) -> ConsensusResult<Option<QC>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        self.votes_aggregators
            .entry(vote.round)
            .or_insert_with(HashMap::new)
            .entry(vote.digest())
            .or_insert_with(|| Box::new(QCMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn add_timeout(&mut self, timeout: Timeout) -> ConsensusResult<Option<TC>> {
        // TODO: A bad node may make us run out of memory by sending many timeouts
        // with different round numbers.

        // Add the new timeout to our aggregator and see if we have a TC.
        self.timeouts_aggregators
            .entry(timeout.round)
            .or_insert_with(|| Box::new(TCMaker::new()))
            .append(timeout, &self.committee)
    }

    pub fn cleanup(&mut self, round: &Round) {
        self.votes_aggregators.retain(|k, _| k >= round);
        self.timeouts_aggregators.retain(|k, _| k >= round);
    }
}

struct QCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl QCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<QC>> {
        let author = vote.author;

        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuse(author)
        );

        self.votes.push((author.clone(), vote.signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            let mut public_keys = Vec::with_capacity(self.votes.len());
            let mut signatures = Vec::with_capacity(self.votes.len());

            for (pk, sig) in self.votes.iter() {
                public_keys.push(blst::min_pk::PublicKey::from_bytes(&pk.0).expect("Invalid public key bytes"));
                signatures.push(blst::min_pk::Signature::from_bytes(&sig.0).expect("Invalid signature bytes"));
            }
            let pks: Vec<_> = public_keys.iter().collect();
            let aggregated_pk = AggregatePublicKey::aggregate(&pks, true).expect("failed to aggregate public keys");
            let sig_refs: Vec<_> = signatures.iter().collect();
            let aggregated_sig = AggregateSignature::aggregate(&sig_refs, true).expect("Failed to aggregate signatures");
            return Ok(Some(QC {
                hash: vote.hash.clone(),
                round: vote.round,
                last_tail: vote.tx_tail,
                votes: self.votes.clone(),
                aggregated_pk: PublicKey(aggregated_pk.to_public_key().to_bytes()),
                aggregated_signature: Signature(aggregated_sig.to_signature().to_bytes()),
            }));
        }
        Ok(None)
    }
}

struct TCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature, Round)>,
    used: HashSet<PublicKey>,
}

impl TCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        timeout: Timeout,
        committee: &Committee,
    ) -> ConsensusResult<Option<TC>> {
        let author = timeout.author;

        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuse(author)
        );

        // Add the timeout to the accumulator.
        self.votes
            .push((author.clone(), timeout.signature, timeout.high_qc.round));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures TC is only created once.
            return Ok(Some(TC {
                round: timeout.round,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

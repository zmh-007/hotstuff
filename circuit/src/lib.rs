use placeholder_project_name_placeholder_zk::field::goldilocks_field::GoldilocksField;
use placeholder_project_name_placeholder_zk::hash::hash_types::{HashOut, HashOutTarget};
use placeholder_project_name_placeholder_zk::hash::poseidon::PoseidonHash;
use placeholder_project_name_placeholder_zk::iop::generator::generate_partial_witness;
use placeholder_project_name_placeholder_zk::iop::target::Target;
use placeholder_project_name_placeholder_zk::plonk::prover::prove_with_partition_witness;
use placeholder_project_name_placeholder_zk::iop::witness::{PartialWitness, WitnessWrite};
use placeholder_project_name_placeholder_zk::plonk::circuit_data::{CircuitData, CircuitConfig};
use placeholder_project_name_placeholder_zk::plonk::config::{GenericConfig, Hasher, PoseidonGoldilocksConfig};
use placeholder_project_name_placeholder_zk::plonk::proof::Proof;
use placeholder_project_name_placeholder_zk::plonk::circuit_builder::CircuitBuilder;
use placeholder_project_name_placeholder_zk::plonk::circuit_data::{VerifierCircuitData, VerifierOnlyCircuitData};
use placeholder_project_name_placeholder_zk::plonk::config::GenericHashOut;
use placeholder_project_name_placeholder_zk::plonk::proof::{ProofWithPublicInputs, ProofWithPublicInputsTarget};
use placeholder_project_name_placeholder_zk::util::timing::TimingTree;
use base64::{Engine as _, engine::general_purpose};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use std::fmt;
use log::info;
use std::time::Instant;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use std::cmp::Ordering;

const D: usize = 2;
type C = PoseidonGoldilocksConfig;
type F = <C as GenericConfig<D>>::F;

pub struct SecretTargets {
    msg_hash: HashOutTarget,
    secret: HashOutTarget,
}

pub struct SecretCircuit {
    targets: SecretTargets,
    secret: HashOut<GoldilocksField>,
    cd: CircuitData<F, C, D>,
}

impl SecretCircuit {
    pub fn new(secret: HashOut<GoldilocksField>) -> Self {
        let mut builder = CircuitBuilder::<F, D>::new(CircuitConfig::standard_recursion_zk_config());
        let secret_hash = PoseidonHash::hash_no_pad(&secret.elements); // TODO: use [secret, secret]?
        let targets = Self::build(&mut builder, secret_hash);
        let cd = builder.build::<C>();
        Self {
            secret,
            cd,
            targets,
        }
    }

    fn build(builder: &mut CircuitBuilder<GoldilocksField, D>, secret_hash: HashOut<GoldilocksField>) -> SecretTargets {
        let secret = builder.add_virtual_hash();
        let msg_hash = builder.add_virtual_hash_public_input();
        let hash = builder.hash_n_to_hash_no_pad::<PoseidonHash>(secret.elements.to_vec()); // TODO: use [secret, secret]?
        let secret_hash = builder.constant_hash(secret_hash);
        builder.connect_hashes(hash, secret_hash);
        SecretTargets { msg_hash, secret }
    }

    pub fn prove(&self, msg_hash: HashOut<GoldilocksField>) -> Proof<GoldilocksField, C, 2> {
        let mut wi = PartialWitness::<GoldilocksField>::new();
        wi.set_hash_target(self.targets.msg_hash, msg_hash).unwrap();
        wi.set_hash_target(self.targets.secret, self.secret).unwrap();
        self.cd.prove(wi).unwrap().proof
    }

    pub fn vk(&self) -> VerifierOnlyCircuitData<C, D> {
        self.cd.verifier_only.clone()
    }

    pub fn vd(&self) -> VerifierCircuitData<F, C, D> {
        self.cd.verifier_data().clone()
    }
}

pub struct AggCircuit {
    ci: CircuitData<F, C, D>,
    proof_with_public_inputs: Vec<ProofWithPublicInputsTarget<D>>,
    author_target: HashOutTarget,
    round_target: HashOutTarget,
    pre_hash_target: HashOutTarget,
    last_tail_target: HashOutTarget,
    tx_tail_target: HashOutTarget,
}

impl AggCircuit {
    pub fn new(inners: Vec<VerifierCircuitData<F, C, D>>) -> Self {
        let mut bi = CircuitBuilder::<F, D>::new(CircuitConfig::standard_recursion_zk_config());
        let mut proof_with_public_inputs: Vec<ProofWithPublicInputsTarget<D>> = Vec::new();
        // private input
        let author_target = bi.add_virtual_hash();
        let round_target = bi.add_virtual_hash();
        let pre_hash_target = bi.add_virtual_hash();
        // public input
        let last_tail_target = bi.add_virtual_hash_public_input();
        let tx_tail_target = bi.add_virtual_hash_public_input();

        let h1 = bi.hash_n_to_hash_no_pad::<PoseidonHash>([author_target.elements, round_target.elements].concat());
        let h2 = bi.hash_n_to_hash_no_pad::<PoseidonHash>([h1.elements, pre_hash_target.elements].concat());
        let h3 = bi.hash_n_to_hash_no_pad::<PoseidonHash>([h2.elements, last_tail_target.elements].concat());
        let block_hash_target = bi.hash_n_to_hash_no_pad::<PoseidonHash>([h3.elements, tx_tail_target.elements].concat());

        let h4 = bi.hash_n_to_hash_no_pad::<PoseidonHash>([block_hash_target.elements, round_target.elements].concat());
        let sign_hash_target = bi.hash_n_to_hash_no_pad::<PoseidonHash>([h4.elements, tx_tail_target.elements].concat());

        for inner in inners.iter() {
            let proof_target = bi.add_virtual_proof_with_pis(&inner.common);
            bi.connect_array(sign_hash_target.elements, proof_target.public_inputs.clone().try_into().unwrap());
            proof_with_public_inputs.push(proof_target.clone());
            let vd_target = bi.constant_verifier_data(&inner.verifier_only);
            bi.verify_proof::<C>(&proof_target, &vd_target, &inner.common);
        }
        let ci = bi.build::<C>();
        Self {
            ci,
            proof_with_public_inputs,
            author_target,
            round_target,
            pre_hash_target,
            last_tail_target,
            tx_tail_target,
        }
    }

    pub fn prove(&self, proof_with_public_inputs: Vec<ProofWithPublicInputs<F, C, D>>, author: HashOut<GoldilocksField>, round: HashOut<GoldilocksField>, pre_hash: HashOut<GoldilocksField>, 
        last_tail: HashOut<GoldilocksField>, tx_tail: HashOut<GoldilocksField>) -> Proof<GoldilocksField, C, 2> {
        let mut wi = PartialWitness::<GoldilocksField>::new();
        for (i, p) in proof_with_public_inputs.iter().enumerate() {
            wi.set_proof_with_pis_target(&self.proof_with_public_inputs[i], &p).unwrap();
        }
        wi.set_hash_target(self.author_target, author).unwrap();
        wi.set_hash_target(self.round_target, round).unwrap();
        wi.set_hash_target(self.pre_hash_target, pre_hash).unwrap();
        wi.set_hash_target(self.last_tail_target, last_tail).unwrap();
        wi.set_hash_target(self.tx_tail_target, tx_tail).unwrap();
        self.ci.prove(wi).unwrap().proof
    }

    pub fn vk(&self) -> VerifierOnlyCircuitData<C, D> {
        self.ci.verifier_only.clone()
    }

    pub fn vd(&self) -> VerifierCircuitData<F, C, D> {
        self.ci.verifier_data().clone()
    }
}

pub struct TransCircuit {
    ci: CircuitData<F, C, D>,
    co: CircuitData<F, C, D>,
    proof_with_public_inputs: ProofWithPublicInputsTarget<D>,
    proof_with_pis_target: ProofWithPublicInputsTarget<D>,
    consensus_target: [Target; 68],
    meta_target: HashOutTarget,
}

impl TransCircuit {
    pub fn new(inner: VerifierCircuitData<F, C, D>) -> Self {
        let mut bi = CircuitBuilder::<F, D>::new(CircuitConfig::standard_recursion_zk_config());
        let proof_with_public_inputs = bi.add_virtual_proof_with_pis(&inner.common);
        bi.register_public_inputs(&proof_with_public_inputs.public_inputs);
        let consensus_target = bi.add_virtual_target_arr();
        bi.register_public_inputs(&consensus_target);
        let meta_target = bi.add_virtual_hash_public_input();
        let vd_target = bi.constant_verifier_data(&inner.verifier_only);
        bi.verify_proof::<C>(&proof_with_public_inputs, &vd_target, &inner.common);
        let ci = bi.build::<C>();
        let mut bo = CircuitBuilder::<F, D>::new(CircuitConfig::standard_recursion_config());
        let proof_with_pis_target = bo.add_virtual_proof_with_pis(&ci.common);
        let inner_verifier_data = bo.constant_verifier_data(&ci.verifier_only);
        bo.register_public_inputs(&proof_with_pis_target.public_inputs);
        bo.verify_proof::<C>(&proof_with_pis_target, &inner_verifier_data, &ci.common);
        let co = bo.build::<C>();
        Self {
            ci,
            co,
            proof_with_public_inputs,
            proof_with_pis_target,
            consensus_target,
            meta_target,
        }
    }

    pub fn prove(&self, proof_with_public_inputs: ProofWithPublicInputs<F, C, D>, consensus: [GoldilocksField; 68], meta: HashOut<GoldilocksField>) -> Proof<GoldilocksField, C, 2> {
        let mut wi = PartialWitness::<GoldilocksField>::new();
        wi.set_proof_with_pis_target(&self.proof_with_public_inputs, &proof_with_public_inputs).unwrap();
        wi.set_target_arr(&self.consensus_target, &consensus).unwrap();
        wi.set_hash_target(self.meta_target, meta).unwrap();
        let witnesses = generate_partial_witness(wi, &self.ci.prover_only, &self.ci.common).unwrap();
        let proof_with_pis = prove_with_partition_witness(&self.ci.prover_only, &self.ci.common, witnesses, &mut TimingTree::default()).unwrap();
        let mut wo = PartialWitness::new();
        wo.set_proof_with_pis_target(&self.proof_with_pis_target, &proof_with_pis).unwrap();
        self.co.prove(wo).unwrap().proof
    }

    pub fn vk(&self) -> VerifierOnlyCircuitData<C, D> {
        self.co.verifier_only.clone()
    }

    pub fn vd(&self) -> VerifierCircuitData<F, C, D> {
        self.co.verifier_data().clone()
    }
}

#[derive(Clone)]
pub struct ProofService {
    channel: Sender<(Digest, oneshot::Sender<Proof<GoldilocksField, C, 2>>)>,
}

impl ProofService {
    pub fn new(secret_circuit: SecretCircuit) -> Self {
        let (tx, mut rx) = channel::<(Digest, oneshot::Sender<Proof<GoldilocksField, C, 2>>)>(100);
        tokio::spawn(async move {
            while let Some((digest, sender)) = rx.recv().await {
                let start_time = Instant::now();
                let proof = secret_circuit.prove(digest.0);
                let elapsed = start_time.elapsed();
                info!("prove completed in {:?}", elapsed);
                let _ = sender.send(proof);
            }
        });
        Self { channel: tx }
    }

    pub async fn request_proof(&mut self, digest: Digest) -> Proof<GoldilocksField, C, 2> {
        let (sender, receiver): (oneshot::Sender<_>, oneshot::Receiver<_>) = oneshot::channel();
        if let Err(e) = self.channel.send((digest, sender)).await {
            panic!("Failed to send message Proof Service: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive proof from Proof Service")
    }
}

/// Represents a hash digest (32 bytes).
#[derive(Copy, Hash, PartialEq, Default, Eq, Clone)]
pub struct Digest(pub HashOut<GoldilocksField>);

impl Digest {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    pub fn to_vec_field(&self) -> Vec<GoldilocksField> {
        self.0.elements.to_vec()
    }

    pub fn size(&self) -> usize {
        self.0.elements.len()
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(&self.0.to_bytes()))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(&self.0.to_bytes()))
    }
}

impl PartialOrd for Digest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Digest {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.to_bytes().cmp(&other.0.to_bytes())
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let base64_str = general_purpose::STANDARD.encode(&self.0.to_bytes());
        serializer.serialize_str(&base64_str)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let base64_str = String::deserialize(deserializer)?;
        let bytes = general_purpose::STANDARD.decode(&base64_str).unwrap();
        Ok(Digest(HashOut::from_bytes(&bytes)))
    }
}

/// This trait is implemented by all messages that can be hashed.
pub trait Hash {
    fn digest(&self) -> Digest;
}

#[cfg(test)]
mod tests {
    use super::*;
    use placeholder_project_name_placeholder_zk::plonk::proof::ProofWithPublicInputs;
    use placeholder_project_name_placeholder_zk::field::types::Sample;
    use std::time::Instant;

    #[test]
    fn test_verify_block_circuit() {
        let secret = HashOut::<GoldilocksField>::rand();
        let msg_hash = HashOut::<GoldilocksField>::rand();
        
        let secret_circuit = SecretCircuit::new(secret);
        let prove_start = Instant::now();
        let proof = secret_circuit.prove(msg_hash);
        let prove_duration = prove_start.elapsed();
        println!("Prove time: {:?}", prove_duration);
        
        let verify_start = Instant::now();
        secret_circuit.cd.verify(ProofWithPublicInputs { 
            proof: proof, 
            public_inputs: [msg_hash.elements].concat()
        }).unwrap();
        let verify_duration = verify_start.elapsed();
        println!("Verify time: {:?}", verify_duration);
    }
}

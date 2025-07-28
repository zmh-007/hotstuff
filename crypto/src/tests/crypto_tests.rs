// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use blst::min_pk::{SecretKey, AggregateSignature, AggregatePublicKey};
use rand::Rng;

#[test]
fn verify_valid_signature() {
    // gen rand sk
    let mut rng = rand::thread_rng();
    let mut ikm = [0u8; 32];
    rng.fill_bytes(&mut ikm);
    let sk = SecretKey::key_gen(&ikm, &[]).unwrap();

    // calculate pk
    let pk = sk.sk_to_pk();

    // Make signature.
    let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
    let msg = b"Hello, blst!";
    let sig = sk.sign(msg, dst, &[]);

    // Verify the signature.
    let err = sig.verify(true, msg, dst, &[], &pk, true);
    assert_eq!(err, blst::BLST_ERROR::BLST_SUCCESS);
}

#[test]
fn aggregated_signature() {
    let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
    let num_signers = 3;

    // 1. gen sk & pk
    let mut rng = rand::thread_rng();
    let mut key_pairs = Vec::new();
    for _ in 0..num_signers {
        let mut sk_bytes = [0u8; 32];
        rng.fill(&mut sk_bytes);
        let sk = SecretKey::key_gen(&sk_bytes, &[]).unwrap();
        let pk = sk.sk_to_pk();
        key_pairs.push((sk, pk));
    }

    // 2. sign same message
    let message = b"Hello, BLS aggregation!";
    let mut signatures = Vec::new();
    for (sk, _) in &key_pairs {
        let sig = sk.sign(message, dst, &[]);
        signatures.push(sig);
    }

    // 3. aggregate signatures
    let sig_refs: Vec<_> = signatures.iter().collect();
    let aggregated_sig = AggregateSignature::aggregate(&sig_refs, true).expect("Failed to aggregate signatures");

    // 4. aggregate public keys
    let aggregated_pk = {
        let pks: Vec<_> = key_pairs.iter().map(|(_, pk)| pk).collect();
        AggregatePublicKey::aggregate(&pks, true).expect("failed to aggregate public keys")
    };

    // 5. verify the aggregated signature
    let final_sig = aggregated_sig.to_signature();
    let is_valid = final_sig.verify(
        true,
        message,
        dst,
        &[],
        &aggregated_pk.to_public_key(),
        true
    );

    assert_eq!(is_valid, blst::BLST_ERROR::BLST_SUCCESS, "Aggregate signature verification failed");
}

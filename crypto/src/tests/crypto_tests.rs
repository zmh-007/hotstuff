// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;

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

#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include <erl_nif.h>
#include <blst.h>

// BLS12-381 signatures over supranational/blst, in the "minimal-pubkey-size"
// variant: public keys in G1 (48 bytes compressed), signatures in G2 (96
// bytes compressed), messages hashed to G2.
//
// verify/3 is the RNG/timelock check and keeps its fixed NUL ciphersuite.
// keygen/1, sign/3, aggregate/1 and fast_aggregate_verify/4 serve callers that
// name their own domain separation tag (the PUSD committee certificate and its
// proofs of possession), so no signature made for one domain verifies in
// another.

#define BLS_PUBKEY_SIZE 48
#define BLS_SIGNATURE_SIZE 96
#define BLS_SECRET_SIZE 32
#define BLS_MAX_KEYS 64

static const char BLS_DST[] = "BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

static int blst_nif_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info) {
	return 0;
}

static ERL_NIF_TERM error_atom(ErlNifEnv* env, const char* reason) {
	return enif_make_tuple2(
		env,
		enif_make_atom(env, "error"),
		enif_make_atom(env, reason)
	);
}

static ERL_NIF_TERM binary_of(ErlNifEnv* env, const byte* data, size_t size) {
	ERL_NIF_TERM term;
	unsigned char* out = enif_make_new_binary(env, size, &term);
	memcpy(out, data, size);
	return term;
}

// A public key: valid encoding, on the curve, in G1, not the identity.
static const char* public_key(blst_p1_affine* pk, const ErlNifBinary* bin) {
	if (bin->size != BLS_PUBKEY_SIZE) return "bad_public_key_size";
	if (blst_p1_uncompress(pk, bin->data) != BLST_SUCCESS) return "bad_public_key";
	if (blst_p1_affine_is_inf(pk)) return "public_key_is_infinity";
	if (!blst_p1_affine_in_g1(pk)) return "public_key_not_in_group";
	return NULL;
}

// A signature: valid encoding, on the curve, in G2.
static const char* signature(blst_p2_affine* sig, const ErlNifBinary* bin) {
	if (bin->size != BLS_SIGNATURE_SIZE) return "bad_signature_size";
	if (blst_p2_uncompress(sig, bin->data) != BLST_SUCCESS) return "bad_signature";
	if (!blst_p2_affine_in_g2(sig)) return "signature_not_in_group";
	return NULL;
}

// verify(PublicKey :: <<_:384>>, Message :: binary(), Signature :: <<_:768>>)
//     -> true | false | {error, Reason}
static ERL_NIF_TERM verify_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary pk_bin, msg_bin, sig_bin;
	blst_p1_affine pk;
	blst_p2_affine sig;
	const char* why;
	BLST_ERROR err;
	if (argc != 3
			|| !enif_inspect_binary(env, argv[0], &pk_bin)
			|| !enif_inspect_binary(env, argv[1], &msg_bin)
			|| !enif_inspect_binary(env, argv[2], &sig_bin)) {
		return enif_make_badarg(env);
	}
	if ((why = public_key(&pk, &pk_bin))) return error_atom(env, why);
	if ((why = signature(&sig, &sig_bin))) return error_atom(env, why);
	err =
		blst_core_verify_pk_in_g1(
			&pk,
			&sig,
			true,
			msg_bin.data, msg_bin.size,
			(const byte*)BLS_DST, sizeof(BLS_DST) - 1,
			NULL, 0
		);
	return enif_make_atom(env, err == BLST_SUCCESS ? "true" : "false");
}

// keygen(IKM :: binary() of at least 32 bytes) -> {Secret :: <<_:256>>, PublicKey :: <<_:384>>}
static ERL_NIF_TERM keygen_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary ikm;
	blst_scalar sk;
	blst_p1 pk;
	byte sk_out[BLS_SECRET_SIZE], pk_out[BLS_PUBKEY_SIZE];
	ERL_NIF_TERM result;
	if (argc != 1 || !enif_inspect_binary(env, argv[0], &ikm)) return enif_make_badarg(env);
	if (ikm.size < 32) return error_atom(env, "short_key_material");
	blst_keygen(&sk, ikm.data, ikm.size, NULL, 0);
	if (!blst_sk_check(&sk)) return error_atom(env, "bad_secret");
	blst_sk_to_pk_in_g1(&pk, &sk);
	blst_bendian_from_scalar(sk_out, &sk);
	blst_p1_compress(pk_out, &pk);
	result = enif_make_tuple2(env, binary_of(env, sk_out, BLS_SECRET_SIZE),
		binary_of(env, pk_out, BLS_PUBKEY_SIZE));
	memset(&sk, 0, sizeof(sk));
	memset(sk_out, 0, sizeof(sk_out));
	return result;
}

// sign(Secret :: <<_:256>>, Message :: binary(), DST :: binary()) -> Signature :: <<_:768>>
static ERL_NIF_TERM sign_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
	ErlNifBinary sk_bin, msg_bin, dst_bin;
	blst_scalar sk;
	blst_p2 hash, sig;
	byte out[BLS_SIGNATURE_SIZE];
	if (argc != 3
			|| !enif_inspect_binary(env, argv[0], &sk_bin)
			|| !enif_inspect_binary(env, argv[1], &msg_bin)
			|| !enif_inspect_binary(env, argv[2], &dst_bin)) {
		return enif_make_badarg(env);
	}
	if (sk_bin.size != BLS_SECRET_SIZE) return error_atom(env, "bad_secret_size");
	if (dst_bin.size == 0 || dst_bin.size > 255) return error_atom(env, "bad_dst");
	blst_scalar_from_bendian(&sk, sk_bin.data);
	if (!blst_sk_check(&sk)) { memset(&sk, 0, sizeof(sk)); return error_atom(env, "bad_secret"); }
	blst_hash_to_g2(&hash, msg_bin.data, msg_bin.size, dst_bin.data, dst_bin.size, NULL, 0);
	blst_sign_pk_in_g1(&sig, &hash, &sk);
	memset(&sk, 0, sizeof(sk));
	blst_p2_compress(out, &sig);
	return binary_of(env, out, BLS_SIGNATURE_SIZE);
}

// aggregate(Signatures :: [<<_:768>>]) -> Aggregate :: <<_:768>> | {error, Reason}
static ERL_NIF_TERM aggregate_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
	ERL_NIF_TERM head, tail;
	ErlNifBinary bin;
	blst_p2_affine one;
	blst_p2 sum;
	byte out[BLS_SIGNATURE_SIZE];
	unsigned count = 0;
	const char* why;
	if (argc != 1 || !enif_is_list(env, argv[0])) return enif_make_badarg(env);
	tail = argv[0];
	while (enif_get_list_cell(env, tail, &head, &tail)) {
		if (!enif_inspect_binary(env, head, &bin)) return enif_make_badarg(env);
		if (++count > BLS_MAX_KEYS) return error_atom(env, "too_many");
		if ((why = signature(&one, &bin))) return error_atom(env, why);
		if (count == 1) blst_p2_from_affine(&sum, &one);
		else blst_p2_add_or_double_affine(&sum, &sum, &one);
	}
	if (count == 0) return error_atom(env, "empty");
	blst_p2_compress(out, &sum);
	return binary_of(env, out, BLS_SIGNATURE_SIZE);
}

// fast_aggregate_verify(PublicKeys :: [<<_:384>>], Message, Aggregate :: <<_:768>>, DST)
//     -> true | false | {error, Reason}
// Every key is checked as one public key (no identity, in G1) and the keys
// are summed; the caller must only pass keys whose possession was proven.
static ERL_NIF_TERM fast_aggregate_verify_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
	ERL_NIF_TERM head, tail;
	ErlNifBinary bin, msg_bin, sig_bin, dst_bin;
	blst_p1_affine one, agg_pk;
	blst_p2_affine sig;
	blst_p1 sum;
	unsigned count = 0;
	const char* why;
	BLST_ERROR err;
	if (argc != 4 || !enif_is_list(env, argv[0])
			|| !enif_inspect_binary(env, argv[1], &msg_bin)
			|| !enif_inspect_binary(env, argv[2], &sig_bin)
			|| !enif_inspect_binary(env, argv[3], &dst_bin)) {
		return enif_make_badarg(env);
	}
	if (dst_bin.size == 0 || dst_bin.size > 255) return error_atom(env, "bad_dst");
	tail = argv[0];
	while (enif_get_list_cell(env, tail, &head, &tail)) {
		if (!enif_inspect_binary(env, head, &bin)) return enif_make_badarg(env);
		if (++count > BLS_MAX_KEYS) return error_atom(env, "too_many");
		if ((why = public_key(&one, &bin))) return error_atom(env, why);
		if (count == 1) blst_p1_from_affine(&sum, &one);
		else blst_p1_add_or_double_affine(&sum, &sum, &one);
	}
	if (count == 0) return error_atom(env, "empty");
	if (blst_p1_is_inf(&sum)) return error_atom(env, "aggregate_key_is_infinity");
	blst_p1_to_affine(&agg_pk, &sum);
	if ((why = signature(&sig, &sig_bin))) return error_atom(env, why);
	err =
		blst_core_verify_pk_in_g1(
			&agg_pk, &sig, true,
			msg_bin.data, msg_bin.size,
			dst_bin.data, dst_bin.size,
			NULL, 0
		);
	return enif_make_atom(env, err == BLST_SUCCESS ? "true" : "false");
}

static ErlNifFunc nif_funcs[] = {
	{"verify", 3, verify_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"keygen", 1, keygen_nif, 0},
	{"sign", 3, sign_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"aggregate", 1, aggregate_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"fast_aggregate_verify", 4, fast_aggregate_verify_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(blst_nif, nif_funcs, blst_nif_load, NULL, NULL, NULL);

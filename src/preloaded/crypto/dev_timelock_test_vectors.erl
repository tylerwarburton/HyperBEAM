%%% @doc Interop vectors for `timelock@1.0'.
%%%
%%% BLS: produced by the beacon's own threshold-BLS code (Random-Process
%%% `v2/provider/bls.mjs', noble-curves 2.4.0 `bls12_381.longSignatures'): a
%%% 2-of-3 joint-Feldman DKG, a group signature combined from two partials, one
%%% member's own signature, and invalid cases. noble agreed with every `valid'
%%% flag when they were generated. The live round is round 1 of the beacon
%%% process `3K_0gcFJixOwe5vsewbHztrzTyfImWxjHRfHwmbtz6A', with the value it
%%% published.
%%%
%%% Time-lock: produced by the reference implementation (Random-Process
%%% `provider/timelock.mjs', generator `test/gen-timelock-vectors.mjs') with
%%% fresh 1024- and 2048-bit moduli and T = 1024 and 4096. The reference agreed
%%% with every `valid' flag when they were generated.
-module(dev_timelock_test_vectors).
-export([bls_vectors/0, live_round/0]).
-export([timelock_vectors/0, fixed_challenge_prime/0]).

%% @doc The generated vectors, as `{Name, Inputs, Valid}'.
bls_vectors() ->
    [
        {
            <<"group-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff0"
                    >>
            },
            true
        },
        {
            <<"group-sig-round-2">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2">>,
                <<"signature">> =>
                    <<
                        "97b006d98c3e7102e4b12010629256e2097159b3ea3553a12f7f19b22091fb1a"
                        "64553eadc62d6e9f4c4ec25d651b63ed0d231a94dac7d8b11005dd644844acf5"
                        "2a5f627a1b3874452280a920edd0d79a232bd0788a0a5085236a70043c0f7f88"
                    >>
            },
            true
        },
        {
            <<"single-key-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "b52db73744c3a6e8b1fef213082ec5a48448390e76c2ed42a35f55d15e9c5a87"
                        "889ac2f50c9bdccd864569d091096c30"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "a035b2e8130278dffd067fca21b95863c01bdfa5bc669ac0914e11f7f4343392"
                        "75f3104fe0a696f7275b21f2812598050eee8eee504fbe6a2197c63b0c654985"
                        "b27f6d71f18c43865663a5526693a3038d678ed2d73bf931c48ca1fa3cb2bf67"
                    >>
            },
            true
        },
        {
            <<"wrong-message">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff0"
                    >>
            },
            false
        },
        {
            <<"wrong-key">>,
            #{
                <<"public-key">> =>
                    <<
                        "8bd88ff3ba9b8689783ea765ac6c354fd75463311de265dd9c77602f88bde5f1"
                        "c61c806130d1467dd5b50977c1570d7e"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff0"
                    >>
            },
            false
        },
        {
            <<"partial-under-group-key">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "a035b2e8130278dffd067fca21b95863c01bdfa5bc669ac0914e11f7f4343392"
                        "75f3104fe0a696f7275b21f2812598050eee8eee504fbe6a2197c63b0c654985"
                        "b27f6d71f18c43865663a5526693a3038d678ed2d73bf931c48ca1fa3cb2bf67"
                    >>
            },
            false
        },
        {
            <<"corrupted-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff1"
                    >>
            },
            false
        },
        {
            <<"non-subgroup-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "a000000000000000000000000000000000000000000000000000000000000000"
                        "0000000000000000000000000000000100000000000000000000000000000000"
                        "0000000000000000000000000000000000000000000000000000000000000001"
                    >>
            },
            false
        },
        {
            <<"non-subgroup-key">>,
            #{
                <<"public-key">> =>
                    <<
                        "8000000000000000000000000000000000000000000000000000000000000000"
                        "00000000000000000000000000000004"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff0"
                    >>
            },
            false
        },
        {
            <<"infinity-key">>,
            #{
                <<"public-key">> =>
                    <<
                        "c000000000000000000000000000000000000000000000000000000000000000"
                        "00000000000000000000000000000000"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff0"
                    >>
            },
            false
        },
        {
            <<"infinity-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "c000000000000000000000000000000000000000000000000000000000000000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    >>
            },
            false
        },
        {
            <<"garbage-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    >>
            },
            false
        },
        {
            <<"zero-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "0000000000000000000000000000000000000000000000000000000000000000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    >>
            },
            false
        },
        {
            <<"short-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7f"
                    >>
            },
            false
        },
        {
            <<"short-key">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "abb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff0"
                    >>
            },
            false
        },
        {
            <<"non-hex-sig">>,
            #{
                <<"public-key">> =>
                    <<
                        "91a35f0fb83204dfd393d73d5f12f52b558a557f193bd992e012c520aff4d26a"
                        "ed07a6f9500e91d04279501928500a7f"
                    >>,
                <<"message">> => <<"Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1">>,
                <<"signature">> =>
                    <<
                        "zzb36051e68131a7621b302eb8d1d99083651546414546546dfa581966f9d6a8"
                        "57aee681217a298a81bb726801e4faaf166a988fe4a0e15edbfb1e714b4f3f90"
                        "a5ff01a596e40809de26b335f1c74a1bd9169daa79fc848c7f92e9332f0a7ff0"
                    >>
            },
            false
        }
    ].

%% @doc A live beacon round: the device's inputs and the round value.
live_round() ->
    {
        #{
            <<"public-key">> =>
                <<
                    "8085ed55cb08a9193f3ef23ee16b3212033aa5f228402508502667d381d0b4e6"
                    "65ef32948a248634faa796a37e3a4658"
                >>,
            <<"message">> => <<"3K_0gcFJixOwe5vsewbHztrzTyfImWxjHRfHwmbtz6A|1">>,
            <<"signature">> =>
                <<
                    "ae5cd6b763bcd4da954213976bab879b0643b8cb0ec6a19df4784c27cb1e8567"
                    "bd842af437905885ba1cfc2771883f4b067fb4895b7936ca0925d794079b0de7"
                    "c61489604d1e2a543ddea5eadb707c4ce312d7b2f0c839c5c6e53459f959fb94"
                >>
        },
        <<"28d9ea02796189a835d3ca071dc4fd0534062381268eb7d1153c9bba55e25529">>
    }.

%% @doc The generated time-lock vectors, as `{Name, Key, Inputs, Expected}'.
timelock_vectors() ->
    [
        {
            <<"1024-t1024-derive-x">>,
            <<"derive-x">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "473a4276e58884420f4f7b92590c253299c4893289db2b125fd45e2bd0b79255"
                    "f429ba3838e9467b1b48a47ad50bad7a420baa8e3e5ee4f695065290b0824b45"
                    "323fb87bbe6b92e0565f56678df1363682911abe9ba6ec7d4b3b3d51c7a84642"
                    "a20ef8ed4b8bc7ff3ae30f7afc3a563f0efae8e2358fc6b25de2e8a3efc5b03f"
                >>
            }
        },
        {
            <<"1024-t1024-derive-x-2">>,
            <<"derive-x">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|101|VVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVVVV"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "116d6feed2704f1f57d46592902ab47893d8fbd2085303b083bbfb146f3200dd"
                    "0e6ae5b764bb9246967b73a6d63a9c97560213fad7f96d7430d15835003ae81d"
                    "d5ea611a911e4ef45629b65d9ee8a0314f1284061ead0a5a2633a99d4be85b92"
                    "f5e077aee2ffea3053d208abe71ae0d80043a280ae8211065872906e4cef9a4b"
                >>
            }
        },
        {
            <<"1024-t1024-reveal-valid-p">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> =>
                <<
                    "cc70c4670abd23be15492963f34fc4e2a2fd10d9fc14c82fdffcb43080950f48"
                    "be19fafbcd0f28cd1f502c1768868e0866e12070ec5595124bff92a40bbae67d"
                >>
            },
            #{
                <<"valid">> => true
            }
        },
        {
            <<"1024-t1024-reveal-valid-q">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> =>
                <<
                    "d486a5c4a6fc25539bfdf86da20a7249e0155177976749607ff1c78d82928848"
                    "0707590b9ff6d730709a4096d5e9a1f50ca84209db968fba838641774148b23d"
                >>
            },
            #{
                <<"valid">> => true
            }
        },
        {
            <<"1024-t1024-reveal-wrong-y">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6c"
                >>,
                <<"p">> =>
                <<
                    "cc70c4670abd23be15492963f34fc4e2a2fd10d9fc14c82fdffcb43080950f48"
                    "be19fafbcd0f28cd1f502c1768868e0866e12070ec5595124bff92a40bbae67d"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-reveal-wrong-p">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> =>
                <<
                    "cc70c4670abd23be15492963f34fc4e2a2fd10d9fc14c82fdffcb43080950f48"
                    "be19fafbcd0f28cd1f502c1768868e0866e12070ec5595124bff92a40bbae67e"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-reveal-p-is-1">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> => <<"1">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-reveal-p-is-n">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-reveal-wrong-seed">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> =>
                <<
                    "cc70c4670abd23be15492963f34fc4e2a2fd10d9fc14c82fdffcb43080950f48"
                    "be19fafbcd0f28cd1f502c1768868e0866e12070ec5595124bff92a40bbae67d"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-reveal-wrong-t">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1025">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> =>
                <<
                    "cc70c4670abd23be15492963f34fc4e2a2fd10d9fc14c82fdffcb43080950f48"
                    "be19fafbcd0f28cd1f502c1768868e0866e12070ec5595124bff92a40bbae67d"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-reveal-garbage-p">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"p">> => <<"xyz">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-reveal-garbage-y">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> => <<"zz">>,
                <<"p">> =>
                <<
                    "cc70c4670abd23be15492963f34fc4e2a2fd10d9fc14c82fdffcb43080950f48"
                    "be19fafbcd0f28cd1f502c1768868e0866e12070ec5595124bff92a40bbae67d"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-valid">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"proof">> =>
                <<
                    "429778955fcd017de29100647b9bd387ea956fffb668712c63b6219e409f6527"
                    "e010be9b96c56316708ff61107110b3f090ba8478eb6b3a119a04fe6481b936b"
                    "bf82355df1ed2c71c05e15a3857169e0e50d34b0532cb3b775c96e92735c17c7"
                    "d8ab55b1c5614b728f8215e3eb48113feb88bc378e735815cacb641c82abc012"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "473a4276e58884420f4f7b92590c253299c4893289db2b125fd45e2bd0b79255"
                    "f429ba3838e9467b1b48a47ad50bad7a420baa8e3e5ee4f695065290b0824b45"
                    "323fb87bbe6b92e0565f56678df1363682911abe9ba6ec7d4b3b3d51c7a84642"
                    "a20ef8ed4b8bc7ff3ae30f7afc3a563f0efae8e2358fc6b25de2e8a3efc5b03f"
                >>,
                <<"l">> => <<"b6f79b14a8cd997cb08d741a8c5bdd77">>
            }
        },
        {
            <<"1024-t1024-crack-corrupt-proof">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"proof">> =>
                <<
                    "429778955fcd017de29100647b9bd387ea956fffb668712c63b6219e409f6527"
                    "e010be9b96c56316708ff61107110b3f090ba8478eb6b3a119a04fe6481b936b"
                    "bf82355df1ed2c71c05e15a3857169e0e50d34b0532cb3b775c96e92735c17c7"
                    "d8ab55b1c5614b728f8215e3eb48113feb88bc378e735815cacb641c82abc013"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-corrupt-y">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6c"
                >>,
                <<"proof">> =>
                <<
                    "429778955fcd017de29100647b9bd387ea956fffb668712c63b6219e409f6527"
                    "e010be9b96c56316708ff61107110b3f090ba8478eb6b3a119a04fe6481b936b"
                    "bf82355df1ed2c71c05e15a3857169e0e50d34b0532cb3b775c96e92735c17c7"
                    "d8ab55b1c5614b728f8215e3eb48113feb88bc378e735815cacb641c82abc012"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-wrong-seed">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"proof">> =>
                <<
                    "429778955fcd017de29100647b9bd387ea956fffb668712c63b6219e409f6527"
                    "e010be9b96c56316708ff61107110b3f090ba8478eb6b3a119a04fe6481b936b"
                    "bf82355df1ed2c71c05e15a3857169e0e50d34b0532cb3b775c96e92735c17c7"
                    "d8ab55b1c5614b728f8215e3eb48113feb88bc378e735815cacb641c82abc012"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-wrong-t">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1025">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"proof">> =>
                <<
                    "429778955fcd017de29100647b9bd387ea956fffb668712c63b6219e409f6527"
                    "e010be9b96c56316708ff61107110b3f090ba8478eb6b3a119a04fe6481b936b"
                    "bf82355df1ed2c71c05e15a3857169e0e50d34b0532cb3b775c96e92735c17c7"
                    "d8ab55b1c5614b728f8215e3eb48113feb88bc378e735815cacb641c82abc012"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-proof-is-0">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"proof">> => <<"0">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-y-is-n">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"proof">> =>
                <<
                    "429778955fcd017de29100647b9bd387ea956fffb668712c63b6219e409f6527"
                    "e010be9b96c56316708ff61107110b3f090ba8478eb6b3a119a04fe6481b936b"
                    "bf82355df1ed2c71c05e15a3857169e0e50d34b0532cb3b775c96e92735c17c7"
                    "d8ab55b1c5614b728f8215e3eb48113feb88bc378e735815cacb641c82abc012"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-garbage-y">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> => <<"zz">>,
                <<"proof">> =>
                <<
                    "429778955fcd017de29100647b9bd387ea956fffb668712c63b6219e409f6527"
                    "e010be9b96c56316708ff61107110b3f090ba8478eb6b3a119a04fe6481b936b"
                    "bf82355df1ed2c71c05e15a3857169e0e50d34b0532cb3b775c96e92735c17c7"
                    "d8ab55b1c5614b728f8215e3eb48113feb88bc378e735815cacb641c82abc012"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"1024-t1024-crack-garbage-proof">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a9b8ea0ddd1b859577e37de66eed4382c6bb310135fc240b7c3fac4bedd137f0"
                    "b2e4022b94c4344a63efef9bd14e8a3b0247bcb479fbf26d46fe0fb74510392f"
                    "9440c5bbe516e8ce1086642e0a30f83f6ae8f0e23a91986c808ba1b6366db6a1"
                    "b59a5ed548d393e67acab2b0d902d57cc6d93798392d652539cea3704ff3d5c9"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|1|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"1024">>,
                <<"y">> =>
                <<
                    "3a85e96b3a437e8a22ee372231047eaf907353db8e6d4cae5b0022af7cc6acf0"
                    "fe6f108892203df28070a4ed90398f26e04126dd59cce5726c9036375f1e88a2"
                    "7cff9f89406cb0f9bfcd4ad68c5f7bb431129a2c3943a12b4fb22d8bf6b9612a"
                    "e075dfb2c6363f9311cc157675f5aa1d576bb0af0b80ee35d7e633e55dd35a6b"
                >>,
                <<"proof">> => <<"not-hex">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-derive-x">>,
            <<"derive-x">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "22a25a40886ebfe6375a473b1e0c0458e47fca941ffae7e9779d06c56f006322"
                    "cfc49d3444026ae23ff55ff2df40de1b3c5ffb6c5f39f36fbf330fa44a06360f"
                    "bcec1ac094ad36998b5eb3c3a6ea93ffa1f24f64f79a4c97ce81829807eb4a3f"
                    "ab388b8f60a0d3b542b6830be97f72357e8548d2c46fa0a7566600deb2ade549"
                    "6fd7c906b0ee8390098ba58cd3c570f129b92cb2ef0eb27ebeeffeb0461e1ccf"
                    "eccd61f5733c76b2554ce49ecb25d322108d7f0fbb15603c2f5ed082a3bf102f"
                    "405e10a685caa1c3cb55daf0a0fb66f6bbafd04c7de6658327f8a3f4034cd0f2"
                    "328468b57e7e6b24238a7865b68ee2efcee6841c72119cf3488220b894ec0675"
                >>
            }
        },
        {
            <<"2048-t4096-derive-x-2">>,
            <<"derive-x">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|102|VVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVVVV"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "1cd2be573ea514dd755f222e75d4f45d7bdbf1ab00258311316c715732b74557"
                    "8ec70568e2753f1876ecb994b5884d27fc41a5932fe3527b658e903cc67169fa"
                    "a85f7c4f8b3892446dd158b31274c5fddada084d1918536831d30a288366f43d"
                    "bc027b48fafb70281220c6f9734e49fa603f02e083a5709a14e1656bcfddf9fe"
                    "0b3e8642403f469325dbc8d86670e19a071c684d75ba8efa4beea9c760a3bd69"
                    "2c04bd6f2394f94b35b5e1bff612ce9cd42b3daac174299e459146767be8ce2c"
                    "7440dcae153eeb7a74ceed72dd127aaad12579515a79bf97b25468c07d647838"
                    "772b5eb2c0d47f4b1aca591b488d81984990f5fc12c4b6ab916a91b8f29ebb02"
                >>
            }
        },
        {
            <<"2048-t4096-reveal-valid-p">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> =>
                <<
                    "cc37b0e8cc0bd118b44a7e06b456228bf2444f1ed63841b5a5d0d812ed1f0ff4"
                    "d479703223c230122d7b342c5756779198fd9448fa3140f86e465766a1ebd6dc"
                    "6a46ad6eca00a0e63c35e3b8c76ec692b5c124fa9431aca8f28df22d20c5c2fc"
                    "b4d243fc38be82d3adf3728deb9d7d3500ed06dd1fdd20277b5232df6dcc47bb"
                >>
            },
            #{
                <<"valid">> => true
            }
        },
        {
            <<"2048-t4096-reveal-valid-q">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> =>
                <<
                    "cd805f107c90fbbe8d01373f908a3e70532d2f3fa143a1771e0b7d42af7780d3"
                    "814e9571542cf72db168d89f3aeb321b9f2d374e4026f3e36972812a1a3372b6"
                    "24b47d1131ea44835d16f241dde0335685799b3eb777b5afae8bea7df075876b"
                    "46e038a0dc179d1c675d2859fe01772dce49abba3ccd9b2ac4463e96b104686d"
                >>
            },
            #{
                <<"valid">> => true
            }
        },
        {
            <<"2048-t4096-reveal-wrong-y">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a3"
                >>,
                <<"p">> =>
                <<
                    "cc37b0e8cc0bd118b44a7e06b456228bf2444f1ed63841b5a5d0d812ed1f0ff4"
                    "d479703223c230122d7b342c5756779198fd9448fa3140f86e465766a1ebd6dc"
                    "6a46ad6eca00a0e63c35e3b8c76ec692b5c124fa9431aca8f28df22d20c5c2fc"
                    "b4d243fc38be82d3adf3728deb9d7d3500ed06dd1fdd20277b5232df6dcc47bb"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-reveal-wrong-p">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> =>
                <<
                    "cc37b0e8cc0bd118b44a7e06b456228bf2444f1ed63841b5a5d0d812ed1f0ff4"
                    "d479703223c230122d7b342c5756779198fd9448fa3140f86e465766a1ebd6dc"
                    "6a46ad6eca00a0e63c35e3b8c76ec692b5c124fa9431aca8f28df22d20c5c2fc"
                    "b4d243fc38be82d3adf3728deb9d7d3500ed06dd1fdd20277b5232df6dcc47bc"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-reveal-p-is-1">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> => <<"1">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-reveal-p-is-n">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-reveal-wrong-seed">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|3|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> =>
                <<
                    "cc37b0e8cc0bd118b44a7e06b456228bf2444f1ed63841b5a5d0d812ed1f0ff4"
                    "d479703223c230122d7b342c5756779198fd9448fa3140f86e465766a1ebd6dc"
                    "6a46ad6eca00a0e63c35e3b8c76ec692b5c124fa9431aca8f28df22d20c5c2fc"
                    "b4d243fc38be82d3adf3728deb9d7d3500ed06dd1fdd20277b5232df6dcc47bb"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-reveal-wrong-t">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4097">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> =>
                <<
                    "cc37b0e8cc0bd118b44a7e06b456228bf2444f1ed63841b5a5d0d812ed1f0ff4"
                    "d479703223c230122d7b342c5756779198fd9448fa3140f86e465766a1ebd6dc"
                    "6a46ad6eca00a0e63c35e3b8c76ec692b5c124fa9431aca8f28df22d20c5c2fc"
                    "b4d243fc38be82d3adf3728deb9d7d3500ed06dd1fdd20277b5232df6dcc47bb"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-reveal-garbage-p">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"p">> => <<"xyz">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-reveal-garbage-y">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> => <<"zz">>,
                <<"p">> =>
                <<
                    "cc37b0e8cc0bd118b44a7e06b456228bf2444f1ed63841b5a5d0d812ed1f0ff4"
                    "d479703223c230122d7b342c5756779198fd9448fa3140f86e465766a1ebd6dc"
                    "6a46ad6eca00a0e63c35e3b8c76ec692b5c124fa9431aca8f28df22d20c5c2fc"
                    "b4d243fc38be82d3adf3728deb9d7d3500ed06dd1fdd20277b5232df6dcc47bb"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-valid">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"proof">> =>
                <<
                    "2ab47ed356dbd9e454244ca85ac5f36b9110278c9df42138f724ef92f140f21c"
                    "3d3e2c6a3bee6b6f41278a0f2a975f74e0e94ae45ecd51449995727a3df3cd0d"
                    "a312cf3a2e79fa6c367ffac20d3e7d057509b37ff59b4d2e555a824a0b4b11f9"
                    "b5684ade99df42f0ec37c3c796d2e10e2d6a28f04b4ea4854de8e43e6e029b81"
                    "afccf6dc7cef1e5f6a0504bd5adce8760c5f75298315385fe40be0b0b524e80f"
                    "8e7cdc08c17061e497c7d7339104dbc9b7362c5f1fdc579d860b9ccc65fca6e6"
                    "ecf00a5d5daa41bb2afe52658034baa6b9ee9c6fa4336c70a966ebd13e6486f8"
                    "c51df0f1e720d2380fcb9b8c32f71c4e2621d9b8df3f4b0640859388b213ba3c"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "22a25a40886ebfe6375a473b1e0c0458e47fca941ffae7e9779d06c56f006322"
                    "cfc49d3444026ae23ff55ff2df40de1b3c5ffb6c5f39f36fbf330fa44a06360f"
                    "bcec1ac094ad36998b5eb3c3a6ea93ffa1f24f64f79a4c97ce81829807eb4a3f"
                    "ab388b8f60a0d3b542b6830be97f72357e8548d2c46fa0a7566600deb2ade549"
                    "6fd7c906b0ee8390098ba58cd3c570f129b92cb2ef0eb27ebeeffeb0461e1ccf"
                    "eccd61f5733c76b2554ce49ecb25d322108d7f0fbb15603c2f5ed082a3bf102f"
                    "405e10a685caa1c3cb55daf0a0fb66f6bbafd04c7de6658327f8a3f4034cd0f2"
                    "328468b57e7e6b24238a7865b68ee2efcee6841c72119cf3488220b894ec0675"
                >>,
                <<"l">> => <<"c2a27f2ebf7a6a99024da4489f627877">>
            }
        },
        {
            <<"2048-t4096-crack-corrupt-proof">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"proof">> =>
                <<
                    "2ab47ed356dbd9e454244ca85ac5f36b9110278c9df42138f724ef92f140f21c"
                    "3d3e2c6a3bee6b6f41278a0f2a975f74e0e94ae45ecd51449995727a3df3cd0d"
                    "a312cf3a2e79fa6c367ffac20d3e7d057509b37ff59b4d2e555a824a0b4b11f9"
                    "b5684ade99df42f0ec37c3c796d2e10e2d6a28f04b4ea4854de8e43e6e029b81"
                    "afccf6dc7cef1e5f6a0504bd5adce8760c5f75298315385fe40be0b0b524e80f"
                    "8e7cdc08c17061e497c7d7339104dbc9b7362c5f1fdc579d860b9ccc65fca6e6"
                    "ecf00a5d5daa41bb2afe52658034baa6b9ee9c6fa4336c70a966ebd13e6486f8"
                    "c51df0f1e720d2380fcb9b8c32f71c4e2621d9b8df3f4b0640859388b213ba3d"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-corrupt-y">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a3"
                >>,
                <<"proof">> =>
                <<
                    "2ab47ed356dbd9e454244ca85ac5f36b9110278c9df42138f724ef92f140f21c"
                    "3d3e2c6a3bee6b6f41278a0f2a975f74e0e94ae45ecd51449995727a3df3cd0d"
                    "a312cf3a2e79fa6c367ffac20d3e7d057509b37ff59b4d2e555a824a0b4b11f9"
                    "b5684ade99df42f0ec37c3c796d2e10e2d6a28f04b4ea4854de8e43e6e029b81"
                    "afccf6dc7cef1e5f6a0504bd5adce8760c5f75298315385fe40be0b0b524e80f"
                    "8e7cdc08c17061e497c7d7339104dbc9b7362c5f1fdc579d860b9ccc65fca6e6"
                    "ecf00a5d5daa41bb2afe52658034baa6b9ee9c6fa4336c70a966ebd13e6486f8"
                    "c51df0f1e720d2380fcb9b8c32f71c4e2621d9b8df3f4b0640859388b213ba3c"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-wrong-seed">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|3|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"proof">> =>
                <<
                    "2ab47ed356dbd9e454244ca85ac5f36b9110278c9df42138f724ef92f140f21c"
                    "3d3e2c6a3bee6b6f41278a0f2a975f74e0e94ae45ecd51449995727a3df3cd0d"
                    "a312cf3a2e79fa6c367ffac20d3e7d057509b37ff59b4d2e555a824a0b4b11f9"
                    "b5684ade99df42f0ec37c3c796d2e10e2d6a28f04b4ea4854de8e43e6e029b81"
                    "afccf6dc7cef1e5f6a0504bd5adce8760c5f75298315385fe40be0b0b524e80f"
                    "8e7cdc08c17061e497c7d7339104dbc9b7362c5f1fdc579d860b9ccc65fca6e6"
                    "ecf00a5d5daa41bb2afe52658034baa6b9ee9c6fa4336c70a966ebd13e6486f8"
                    "c51df0f1e720d2380fcb9b8c32f71c4e2621d9b8df3f4b0640859388b213ba3c"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-wrong-t">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4097">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"proof">> =>
                <<
                    "2ab47ed356dbd9e454244ca85ac5f36b9110278c9df42138f724ef92f140f21c"
                    "3d3e2c6a3bee6b6f41278a0f2a975f74e0e94ae45ecd51449995727a3df3cd0d"
                    "a312cf3a2e79fa6c367ffac20d3e7d057509b37ff59b4d2e555a824a0b4b11f9"
                    "b5684ade99df42f0ec37c3c796d2e10e2d6a28f04b4ea4854de8e43e6e029b81"
                    "afccf6dc7cef1e5f6a0504bd5adce8760c5f75298315385fe40be0b0b524e80f"
                    "8e7cdc08c17061e497c7d7339104dbc9b7362c5f1fdc579d860b9ccc65fca6e6"
                    "ecf00a5d5daa41bb2afe52658034baa6b9ee9c6fa4336c70a966ebd13e6486f8"
                    "c51df0f1e720d2380fcb9b8c32f71c4e2621d9b8df3f4b0640859388b213ba3c"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-proof-is-0">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"proof">> => <<"0">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-y-is-n">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"proof">> =>
                <<
                    "2ab47ed356dbd9e454244ca85ac5f36b9110278c9df42138f724ef92f140f21c"
                    "3d3e2c6a3bee6b6f41278a0f2a975f74e0e94ae45ecd51449995727a3df3cd0d"
                    "a312cf3a2e79fa6c367ffac20d3e7d057509b37ff59b4d2e555a824a0b4b11f9"
                    "b5684ade99df42f0ec37c3c796d2e10e2d6a28f04b4ea4854de8e43e6e029b81"
                    "afccf6dc7cef1e5f6a0504bd5adce8760c5f75298315385fe40be0b0b524e80f"
                    "8e7cdc08c17061e497c7d7339104dbc9b7362c5f1fdc579d860b9ccc65fca6e6"
                    "ecf00a5d5daa41bb2afe52658034baa6b9ee9c6fa4336c70a966ebd13e6486f8"
                    "c51df0f1e720d2380fcb9b8c32f71c4e2621d9b8df3f4b0640859388b213ba3c"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-garbage-y">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> => <<"zz">>,
                <<"proof">> =>
                <<
                    "2ab47ed356dbd9e454244ca85ac5f36b9110278c9df42138f724ef92f140f21c"
                    "3d3e2c6a3bee6b6f41278a0f2a975f74e0e94ae45ecd51449995727a3df3cd0d"
                    "a312cf3a2e79fa6c367ffac20d3e7d057509b37ff59b4d2e555a824a0b4b11f9"
                    "b5684ade99df42f0ec37c3c796d2e10e2d6a28f04b4ea4854de8e43e6e029b81"
                    "afccf6dc7cef1e5f6a0504bd5adce8760c5f75298315385fe40be0b0b524e80f"
                    "8e7cdc08c17061e497c7d7339104dbc9b7362c5f1fdc579d860b9ccc65fca6e6"
                    "ecf00a5d5daa41bb2afe52658034baa6b9ee9c6fa4336c70a966ebd13e6486f8"
                    "c51df0f1e720d2380fcb9b8c32f71c4e2621d9b8df3f4b0640859388b213ba3c"
                >>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t4096-crack-garbage-proof">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "a3ef0058b14b90755a1cba1c7c80e0ce243636bf1eaadddb6f1deedb0787ae3e"
                    "413b31abde117435eceef0f3bc926da0ad454316f2eea82ea39b00581b2ae311"
                    "73ec9dfda862d40add3ddf0e13bbe00852f309a134e58e77f0e5c7cc555607a0"
                    "572f5720e4d53921c214c9335f61c15aed661a32fd1de861fc72d64ace07ad38"
                    "1d9493f2a0167b8719cf858411f71bf07478a35c19de2d5051226bec63e03b48"
                    "2169b7df87cabadcd2f9bf17cdc8eb9f6ed175ab98625f4546b8cbc55bd42adb"
                    "1c5309e5d4844bb918fd11fa92384f59b3b63ada0cd4be13213063115a7210fa"
                    "69970f5ea1911fe9d568656f05cff52b305ca69b4e6dd354c545b418270a829f"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|2|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"4096">>,
                <<"y">> =>
                <<
                    "77c3e8b66500492994427ac46a2c87affe8ea9fa7a8f220b8bcc55a9e60b1f6e"
                    "b0ded1abc9a2a55f1ef28bdc20d46018635e65d51c734ebafb3bb32bf15e8c3e"
                    "f7dd1c9ecbdb1d0775b710217f322478ee9aa1da91d404232006a330babbe3fb"
                    "c66d7ba40121231fa6423a6fb099ca31fab9057856fe48f2f2d382f8a6e55679"
                    "2bceea8d895bb4f48abc6d757c30735ba272461f6bf082eeb1be498f2be1e884"
                    "545363e2fc16d5f182a5b1dab880ad0dd15bc02dfa9566415b84b6a9bdabdad5"
                    "23cd046c4ceac6b3c99f1f70676eb98ddd4e31640ee6e25608e2385624cd79f5"
                    "9a7409a8af42bf4b95e13d94539a777113bb6d69114724011879693e564bd1a2"
                >>,
                <<"proof">> => <<"not-hex">>
            },
            #{
                <<"valid">> => false
            }
        },
        {
            <<"2048-t2^27-reveal-valid">>,
            <<"verify-reveal">>,
            #{
                <<"modulus">> =>
                <<
                    "ce0b44e8dd86f44a2e1da3147f86e9c5ec1143f0669ccd151cc37e3cf160f1eb"
                    "18d8cae45ca0f39fe14ba6c718cce326de8ae5ed0f5f62c93ec80592f727153c"
                    "be7f9bc3c6d5a6cd96b948a966ea8a96d1f13b75bf2729fb27cef9a715fa1489"
                    "358149889c28df4e1be8def4962d3ec5bc51f404d5e3c7e7e78bfe857bb4355e"
                    "2d9f2dce1ab7e76f87b650b9ab3d5ac8c1dbad50b42c1ab96f8a8faf4faa1953"
                    "980bacd671db39e48b5cc891fe709719ab65386b184bf26cae6d5e772000669a"
                    "807de14bf3c01de82709712af35004b60f76af3c1086ee5207d9d9d439cffd50"
                    "66e0a9126c102765cd7ed62486809978d2e4ce931e71433311995a512f940f83"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|3|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"134217728">>,
                <<"y">> =>
                <<
                    "445c2eea484091f86b820b68226df88b5c61427eadb554f22b2476845dfd15bc"
                    "2ec58c1bdeb6ad06881f3c9cb0a292539b44661d0d76bc6d493c07abdd947dd7"
                    "592c2bbb54ab0112ca22353adec454e17cc03d097f4943dec034fe728fc1319e"
                    "32fe86f65f65121a1ec53f2e7bc3daab2ca634a5ce5b8b1b3e9df8e16f25fe57"
                    "caedf937e5b96f23aa5e5873c163f12b7df55a8de646ed0a18f923517026b9e9"
                    "f2e6c337c0712186e5ee113cb91124768da7e82d93fa196308ff526083e3bac3"
                    "b3eece5085e124e1d2aa19b4a8b06ba8679e4dd7ff51a8979fe809d3bc322528"
                    "7770270b0faa0a3f4b4acf27e08a3632ef199f718ccd253a7598da517da315ec"
                >>,
                <<"p">> =>
                <<
                    "eb96e21e511c45aa170bf646fcac138329c421097e7f42c1e5c5774ca640b9a7"
                    "ebd60b424b330cb9d7b88816f04226caa1a2c23c73a0503b0b6552e9bf090db8"
                    "be2cbbc22bb249e49c893d3a4825dcfc32a642968d644bd957b4237e986b1e01"
                    "0e60d7f4de93422b2fbf6f22a1ac3b7dd55281aa86070b1d6f1ed4bbaddfb985"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "6e17ba4f3f35b749eec857ffb55f2266ccfd1a48de6000a7eb3afd2ad1de26a8"
                    "46d3075d694353fba3e38c8375356130d0220444867159a0001505c4b81286d2"
                    "6a8ccf0ee9e2d28eed7dc13e55637d7de4bee8fd8189b7dea106660c7c347bf2"
                    "8d3c5f5d3e417b0296ddd389ad0fd73417e9d3a95eb0fcb2db6fa01b1b0818dc"
                    "616fa37228ab053a368aeafff2f91ab313284715434e213cb0314d5feb28525f"
                    "66db5b1b6178eeb28a93865fce6270cc386156eb1d0164848aff4d5a040d5093"
                    "d6a75dcb07bdf3ce1c97d794091b2b5aeafef5dfe89eae13db55a9ec357226fb"
                    "c242b50d6fca739e7be93ffbddc8f4a9abf998a0622a2be0df1c2cc28582a405"
                >>
            }
        },
        {
            <<"2048-t2^27-crack-valid">>,
            <<"verify-crack">>,
            #{
                <<"modulus">> =>
                <<
                    "ce0b44e8dd86f44a2e1da3147f86e9c5ec1143f0669ccd151cc37e3cf160f1eb"
                    "18d8cae45ca0f39fe14ba6c718cce326de8ae5ed0f5f62c93ec80592f727153c"
                    "be7f9bc3c6d5a6cd96b948a966ea8a96d1f13b75bf2729fb27cef9a715fa1489"
                    "358149889c28df4e1be8def4962d3ec5bc51f404d5e3c7e7e78bfe857bb4355e"
                    "2d9f2dce1ab7e76f87b650b9ab3d5ac8c1dbad50b42c1ab96f8a8faf4faa1953"
                    "980bacd671db39e48b5cc891fe709719ab65386b184bf26cae6d5e772000669a"
                    "807de14bf3c01de82709712af35004b60f76af3c1086ee5207d9d9d439cffd50"
                    "66e0a9126c102765cd7ed62486809978d2e4ce931e71433311995a512f940f83"
                >>,
                <<"seed">> =>
                <<
                    "Xk3_9vQ2mZpL7aRtYw4Hn1cBfGdJs8uEoVq6iT0yK5A|3|VVVVVVVVVVVVVVVVVV"
                    "VVVVVVVVVVVVVVVVVVVVVVVVV"
                >>,
                <<"t">> => <<"134217728">>,
                <<"y">> =>
                <<
                    "445c2eea484091f86b820b68226df88b5c61427eadb554f22b2476845dfd15bc"
                    "2ec58c1bdeb6ad06881f3c9cb0a292539b44661d0d76bc6d493c07abdd947dd7"
                    "592c2bbb54ab0112ca22353adec454e17cc03d097f4943dec034fe728fc1319e"
                    "32fe86f65f65121a1ec53f2e7bc3daab2ca634a5ce5b8b1b3e9df8e16f25fe57"
                    "caedf937e5b96f23aa5e5873c163f12b7df55a8de646ed0a18f923517026b9e9"
                    "f2e6c337c0712186e5ee113cb91124768da7e82d93fa196308ff526083e3bac3"
                    "b3eece5085e124e1d2aa19b4a8b06ba8679e4dd7ff51a8979fe809d3bc322528"
                    "7770270b0faa0a3f4b4acf27e08a3632ef199f718ccd253a7598da517da315ec"
                >>,
                <<"proof">> =>
                <<
                    "9f9ce60ec551947e3de3ac11737fa6d8c9a57d937045625d1847d969c704405b"
                    "d3bc0c494b99d3d60e1a327b82fdb94f5a75d0cf50e45620cc1db1f6b26f7bda"
                    "3d819db0379b103f373f2eb5a293864ea400890ac85fabda73d91eb9c0c1c408"
                    "a040b70bf3799325a92b7f5e3da8b51a82dabb40076c166046bc26032f8bf107"
                    "2ee426f28b40c1bab5b86f461ae430d1ce708143660ede1ed2924b1aaa9756b6"
                    "ba2a028965a1b62a71a1f68276ac7c3750b3e9a2e22db38dc66e0ca479d9a03d"
                    "c3fb37c540e0a0ffaa827075bcf87f4f6edfef58035b001563b0038f44c66512"
                    "42d7f8b7bd624ca2d782cb7c46805aaf2dfc2ef4b4514380777b9d77e5c91272"
                >>
            },
            #{
                <<"valid">> => true,
                <<"x">> =>
                <<
                    "6e17ba4f3f35b749eec857ffb55f2266ccfd1a48de6000a7eb3afd2ad1de26a8"
                    "46d3075d694353fba3e38c8375356130d0220444867159a0001505c4b81286d2"
                    "6a8ccf0ee9e2d28eed7dc13e55637d7de4bee8fd8189b7dea106660c7c347bf2"
                    "8d3c5f5d3e417b0296ddd389ad0fd73417e9d3a95eb0fcb2db6fa01b1b0818dc"
                    "616fa37228ab053a368aeafff2f91ab313284715434e213cb0314d5feb28525f"
                    "66db5b1b6178eeb28a93865fce6270cc386156eb1d0164848aff4d5a040d5093"
                    "d6a75dcb07bdf3ce1c97d794091b2b5aeafef5dfe89eae13db55a9ec357226fb"
                    "c242b50d6fca739e7be93ffbddc8f4a9abf998a0622a2be0df1c2cc28582a405"
                >>,
                <<"l">> => <<"f58ca98ca94f9efecc545e86066e5a07">>
            }
        }
    ].

%% @doc The reference's challenge prime for N = ab, x = cd, y = ef, T = 10.
fixed_challenge_prime() ->
    {{<<"ab">>, <<"cd">>, <<"ef">>, 10}, <<"c3f59556d1dd1dc92c59746f158dcb5d">>}.

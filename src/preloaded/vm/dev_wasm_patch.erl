%%% @doc Request-in, patch-list-out execution of a WebAssembly module: the
%%% `lua-patch@1.0' protocol with the VM swapped for WAMR, AOT-compiled by the
%%% node (`hb_wasm_aot') or interpreted, called once per slot through a
%%% dirty-CPU NIF (`hb_wasm_nif'). The ABI is docs/WASM_PATCH_ABI.md in
%%% paralith-mono: `wp_alloc', `wp_init(definition)', `wp_handle(request)',
%%% binary request and result (`hb_wasm_patch_codec'). The request build and
%%% the result handling are `lua-patch@1.0''s own (`lib_patch_exec'), so a
%%% contract publishes the same terms on either device.
%%%
%%% Process fields: `image' (the `.wasm', an ID or a message with it as its
%%% body), `wasm-patch-abi' (1), optional `fuel-per-slot' and `fuel-init',
%%% and whatever the module reads from its definition (the Lua runtime:
%%% `module').
%%%
%%% At spawn the image goes through the pinned validation and instrumentation
%%% pass (`hb_wasm_aot:instrument/3'), which refuses what may not run and adds
%%% the fuel and call-depth counters; the instrumented module is what is
%%% compiled and run. A node may run modules as they are, without fuel, only
%%% with `wasm-patch-allow-uninstrumented' (tests).
%%%
%%% A process never halts. A slot that fails -- an error the module returns,
%%% a trap, out of fuel, a result that does not decode or apply -- leaves the
%%% state as it was before the slot: its patches are discarded, the slot's
%%% results are the error (as `lua-patch@1.0' gives them), and the instance is
%%% put back by restoring the last checkpoint and replaying the requests
%%% applied since. The same restore serves a state that is not the
%%% instance's newest (a stale base): the instance counts the calls applied
%%% to it (`gen'), and the NIF refuses a call naming another gen.
%%%
%%% Private state (`priv/state'):
%%% ```
%%% inst      the instance (NIF resource), shared by every state of one lineage
%%% gen       calls applied, counting `wp_init'
%%% ckpt      {Gen, Memory, Globals}: an in-memory checkpoint
%%% log       requests applied since the checkpoint, newest first
%%% '''
-module(dev_wasm_patch).
-implements(<<"wasm-patch@1.0">>).
-device_libraries([lib_patch_exec]).
-export([device_info/1, init/3, compute/4, snapshot/3, normalize/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEVICE, <<"wasm-patch@1.0">>).
-define(ABI, 1).
%% Fuel a slot (and `wp_init') may use when the process does not say
%% (`fuel-per-slot', `fuel-init'). Process constants: fuel decides results, so
%% it never comes from node options. Measured on pair.lua (1,500 accounts, 300
%% resting, 600 trades, then 1,000 stream messages): Order.Place, Cancel and a
%% one-fill take 1.1-2.1e8 at p50, at most 1.0e9; an Order.CancelAll of 50
%% orders 2.3e8; a taker sweeping every ask level 5.9e9; Admin.Checkpoint 6.4e9
%% (it grows with the state); loading pair.lua in wp_init 3.7e8. A slot gets
%% 100x the heaviest ordinary message; init 100x that.
-define(DEFAULT_FUEL, 100000000000).
-define(DEFAULT_INIT_FUEL, 10000000000000).
-define(STACK_BYTES, 8 * 1024 * 1024).
-define(CHECKPOINT_CALLS, 1000).
-define(CHECKPOINT_LOG_BYTES, 64 * 1024 * 1024).

device_info(_Base) ->
    #{
        default => fun compute/4,
        direct_message_keys => true,
        excludes =>
            [
                <<"id">>,
                <<"commitments">>,
                <<"committers">>,
                <<"keys">>,
                <<"path">>,
                <<"set">>,
                <<"remove">>,
                <<"verify">>,
                <<"encode">>,
                <<"decode">>
            ]
    }.

%%% Init

%% @doc Validate the image, get it compiled (or not), instantiate, and call
%% `wp_init' with the process definition. A state already holding an instance
%% made by this device is returned as it is.
init(Base, _Req, Opts) ->
    case hb_private:get(<<"state">>, Base, not_found, Opts) of
        not_found ->
            try boot(Base, Opts) of
                {ok, St} -> {ok, hb_private:set(Base, <<"state">>, St, Opts)};
                Error -> Error
            catch throw:{wasm_patch_init, Reason} -> init_error(Reason)
            end;
        #{ device := wasm_patch } -> {ok, Base};
        _ -> init_error(<<"the private state was not created by wasm-patch@1.0">>)
    end.

init_error(Reason) ->
    {error, #{
        <<"status">> => 422,
        <<"body">> => <<"wasm-patch@1.0 cannot start: ", (hb_util:bin(Reason))/binary>>
    }}.

boot(Base, Opts) ->
    {St0, Info} = instance(Base, Opts),
    Fuel = fuel(<<"fuel-per-slot">>, ?DEFAULT_FUEL, Base, Info, Opts),
    InitFuel = fuel(<<"fuel-init">>, ?DEFAULT_INIT_FUEL, Base, Info, Opts),
    Def = hb_wasm_patch_codec:encode(definition(Base, Opts)),
    #{ inst := Inst } = St0,
    case hb_wasm_nif:call(Inst, 0, 0, Def, InitFuel) of
        {ok, Reply, _} ->
            case hb_wasm_patch_codec:decode_init(Reply) of
                ok -> ok;
                {error, E} -> throw({wasm_patch_init, error_text(E)});
                {malformed, M} -> throw({wasm_patch_init, M})
            end,
            {ok, 1, Mem, Globals} = hb_wasm_nif:snapshot(Inst),
            {ok, St0#{
                gen => 1,
                fuel => Fuel,
                literal_links => lib_patch_exec:literal_input_links(Base, Opts),
                ckpt => {1, Mem, Globals},
                log => [],
                log_bytes => 0,
                slot => undefined
            }};
        {trap, Reason} -> throw({wasm_patch_init, <<"wp_init trapped: ", Reason/binary>>});
        {fault, Reason} -> throw({wasm_patch_init, <<"wp_init failed on this node: ", Reason/binary>>})
    end.

%% A loaded and instantiated module, before `wp_init' or a restore.
instance(Base, Opts) ->
    Image = image(Base, Opts),
    MaxPages = hb_util:int(hb_opts:get(<<"wasm-patch-max-pages">>, 65536, Opts)),
    {Wasm, Pass} =
        case allow_uninstrumented(Opts) of
            true -> {Image, <<"none">>};
            false ->
                case hb_wasm_aot:instrument(Image, MaxPages, Opts) of
                    {ok, Instrumented, Version} -> {Instrumented, Version};
                    {refused, Why} -> throw({wasm_patch_init, Why});
                    {error, Why} ->
                        throw({wasm_patch_init,
                            <<"no instrumentation pass on this node: ", (error_text(Why))/binary>>})
                end
        end,
    case lib_abi(Base, Opts) of
        ?ABI -> ok;
        Other -> throw({wasm_patch_init, <<"unsupported wasm-patch-abi ", (hb_util:bin(Other))/binary>>})
    end,
    case hb_wasm_nif:prepare(Wasm) of
        ok -> ok;
        {error, PrepErr} -> throw({wasm_patch_init, hb_util:bin(io_lib:format("~p", [PrepErr]))})
    end,
    % Loading the `.wasm' validates it (imports, exports, memory) whatever
    % mode it then runs in.
    {WasmMod, Info} =
        case hb_wasm_nif:load(Wasm) of
            {ok, M, I} -> {M, I};
            {error, LoadErr} -> throw({wasm_patch_init, LoadErr})
        end,
    case maps:get(max_pages, Info) of
        P when P > MaxPages -> throw({wasm_patch_init, <<"memory maximum above the node's cap">>});
        _ -> ok
    end,
    {Mod, AotKey} =
        case hb_wasm_aot:get(Wasm, Opts) of
            {aot, Aot, Key} ->
                case hb_wasm_nif:load(Aot) of
                    {ok, AotMod, _} -> {AotMod, Key};
                    {error, AotErr} ->
                        ?event(warning, {wasm_patch_aot_load_failed, AotErr}),
                        {WasmMod, interp}
                end;
            {interp, _} -> {WasmMod, interp}
        end,
    Stack = hb_util:int(hb_opts:get(<<"wasm-patch-stack-bytes">>, ?STACK_BYTES, Opts)),
    case hb_wasm_nif:instantiate(Mod, Stack, 0) of
        {ok, Inst, InstInfo} ->
            {#{
                device => wasm_patch,
                inst => Inst,
                wasm_sha256 => crypto:hash(sha256, Image),
                instrument => Pass,
                aot_key => AotKey
            }, InstInfo};
        {error, InstErr} -> throw({wasm_patch_init, InstErr})
    end.

lib_abi(Base, Opts) ->
    case find([<<"wasm-patch-abi">>], Base, Opts) of
        not_found -> ?ABI;
        Abi -> catch hb_util:int(Abi)
    end.

%% A fuel budget from the process (`Key'), else the default. A module without
%% the pass's fuel global (allowed only with `wasm-patch-allow-uninstrumented')
%% runs unmetered.
fuel(Key, Default, Base, #{ fuel := true }, Opts) ->
    case find([Key], Base, Opts) of
        not_found -> Default;
        F -> hb_util:int(F)
    end;
fuel(_Key, _Default, _Base, #{ fuel := false }, _Opts) ->
    -1.

allow_uninstrumented(Opts) ->
    case hb_opts:get(<<"wasm-patch-allow-uninstrumented">>, false, Opts) of
        true -> true;
        <<"true">> -> true;
        _ -> false
    end.

%% A key of the process definition: on the base, or under `process'.
find(Path, Base, Opts) ->
    case hb_util:deep_get(Path, Base, not_found, Opts) of
        not_found -> hb_util:deep_get([<<"process">> | Path], Base, not_found, Opts);
        Value -> Value
    end.

%% @doc The `.wasm' bytes. `image' is the bytes, a message whose `body' (or,
%% fetched from a gateway, `data') they are, or that message's ID; AOT bytes
%% are refused, as a node only runs AOT code it compiled itself.
%%
%% `image-sha256' pins the bytes: a process naming its image by ID must name
%% it, and any image whose hash differs from the one named is refused. With a
%% hash named, a copy the node holds as approved (`hb_wasm_aot:approved/2',
%% written by the image build's prewarm) is used without fetching the ID.
image(Base, Opts) ->
    Pinned =
        case find([<<"image-sha256">>], Base, Opts) of
            not_found -> not_found;
            Hex -> string:lowercase(hb_util:bin(Hex))
        end,
    Image =
        case Pinned =/= not_found andalso hb_wasm_aot:approved(Pinned, Opts) of
            {ok, Approved} -> Approved;
            _ -> image_bytes(find([<<"image">>], Base, Opts), Pinned, Opts)
        end,
    case Image of
        <<0, "asm", _/binary>> -> ok;
        _ -> throw({wasm_patch_init, <<"the image is not a .wasm module">>})
    end,
    case Pinned of
        not_found -> Image;
        _ ->
            case binary:encode_hex(crypto:hash(sha256, Image), lowercase) of
                Pinned -> Image;
                Got ->
                    throw({wasm_patch_init,
                        <<"the image's sha256 is ", Got/binary, ", not the ", Pinned/binary,
                            " the process names">>})
            end
    end.

image_bytes(not_found, _Pinned, _Opts) ->
    throw({wasm_patch_init, <<"no image">>});
image_bytes(<<0, "asm", _/binary>> = Bin, _Pinned, _Opts) -> Bin;
image_bytes(ID, not_found, _Opts) when ?IS_ID(ID) ->
    throw({wasm_patch_init, <<"an image named by ID needs image-sha256">>});
image_bytes(ID, _Pinned, Opts) when ?IS_ID(ID) ->
    case hb_cache:read(ID, Opts) of
        {ok, Msg} when is_map(Msg) -> body(Msg, Opts);
        {ok, Bin} when is_binary(Bin) -> Bin;
        _ -> throw({wasm_patch_init, <<"image ", ID/binary, " not found">>})
    end;
image_bytes(Msg, _Pinned, Opts) when is_map(Msg) -> body(Msg, Opts);
image_bytes(Link, Pinned, Opts) when ?IS_LINK(Link) ->
    image_bytes(hb_cache:ensure_all_loaded(Link, Opts), Pinned, Opts);
image_bytes(Other, _Pinned, _Opts) -> Other.

%% The payload of an image message: `body' as a message carries it, `data' as
%% an Arweave transaction decodes.
body(Msg, Opts) when is_map(Msg) ->
    case hb_maps:get(<<"body">>, Msg, not_found, Opts) of
        not_found -> hb_cache:ensure_all_loaded(hb_maps:get(<<"data">>, Msg, not_found, Opts), Opts);
        Body -> hb_cache:ensure_all_loaded(Body, Opts)
    end;
body(Bin, _Opts) -> Bin.

%% @doc The process definition `wp_init' receives: the process message
%% without its image, its private and per-slot keys, with its own `id' when
%% it runs under `process@1.0'; links loaded, and a `module' given by ID read
%% from the cache.
definition(Base, Opts) ->
    Proc =
        case hb_maps:get(<<"process">>, Base, not_found, Opts) of
            P when is_map(P) -> P;
            _ -> Base
        end,
    Def0 = maps:without(
        [<<"priv">>, <<"image">>, <<"snapshot">>, <<"results">>, <<"at-slot">>],
        hb_private:reset(hb_cache:ensure_all_loaded(Proc, Opts))
    ),
    % The process's own ID, as `process@1.0' names it (its signed ID), so the
    % runtime can serve `ao.resolve' of its own definition (`/<id>/<key>').
    Def =
        case Proc =/= Base of
            true -> Def0#{ <<"id">> => hb_message:id(Proc, signed, Opts) };
            false -> Def0
        end,
    lib_patch_exec:encode(module_by_id(Def, Opts), term, Opts).

module_by_id(Def = #{ <<"module">> := ID }, Opts) when ?IS_ID(ID) ->
    case hb_cache:read(ID, Opts) of
        {ok, Module} -> Def#{ <<"module">> => hb_cache:ensure_all_loaded(Module, Opts) };
        _ -> Def
    end;
module_by_id(Def, _Opts) -> Def.

%%% Compute

%% @doc One call of `wp_handle' with the slot's request, then `lua-patch@1.0''s
%% result handling. A failed slot puts the instance back first.
compute(Key, RawBase, RawReq, Opts) ->
    T0 = erlang:monotonic_time(microsecond),
    Req = request_commitments(RawBase, RawReq, Opts),
    case init(RawBase, Req, Opts) of
        {ok, Base} -> compute(Key, Base, Req, T0, Opts);
        Error -> Error
    end.

%% The request with every commitment the store holds for it, as
%% `lua-patch@1.0' reads it -- except in a process slot. There the request is
%% the assignment the scheduler just read from the store, with its path set to
%% `compute' (`dev_process:prepare_next_slot/4'), so it already carries every
%% stored commitment of that assignment; and the lookup is by the uncommitted
%% ID of the whole request, computed on every slot (0.5 ms of a 0.9 ms prep on
%% a signed message). Over both devices' suites (5,448 requests) it never
%% added one. The message's own commitments, the ones `signer()' reads, are
%% the body's and are not touched by it.
request_commitments(Base, Req, Opts) ->
    case lib_patch_exec:in_slot(hb_private:from_message(Base)) of
        true -> Req;
        false -> hb_cache:read_all_commitments(Req, Opts)
    end.

compute(_Key, Base, Req, T0, Opts) ->
    Priv = hb_private:from_message(Base),
    InSlot = lib_patch_exec:in_slot(Priv),
    % The module has one entry point, so no function is looked up; the input
    % link policy is the process's, read once (`literal_links/4').
    {Literal, St0} = literal_links(InSlot, maps:get(<<"state">>, Priv), Base, Opts),
    Params = lib_patch_exec:params(Req, InSlot, Literal, Opts),
    ReqBin =
        iolist_to_binary(
            lib_patch_exec:encode(
                case Params of
                    [One] -> One;
                    Many -> Many
                end,
                wire,
                Opts
            )
        ),
    T1 = erlang:monotonic_time(microsecond),
    hb_slot_log:note(wasm_patch_prep, T1 - T0),
    {St, Res} = call(St0, ReqBin, Opts),
    T2 = erlang:monotonic_time(microsecond),
    hb_slot_log:note(wasm_patch_call, T2 - T1),
    ?event(compute_short,
        {wasm_patch_timing, {prep_us, T1 - T0}, {call_us, T2 - T1}},
        Opts
    ),
    case Res of
        {ok, Reply} ->
            {DecodeMicros, Decoded} = timer:tc(fun() -> decode(Reply, Opts) end),
            case Decoded of
                {ok, Result} ->
                    Next = advance(St, ReqBin, lib_patch_exec:slot(Req, Opts), Opts),
                    Applied =
                        lib_patch_exec:result(
                            <<"ok">>,
                            Result,
                            DecodeMicros,
                            Base,
                            #{
                                device => ?DEVICE,
                                timing_event => wasm_patch_timing,
                                decode_phase => wasm_patch_decode,
                                apply_phase => wasm_patch_apply,
                                priv => Priv#{ <<"state">> => Next }
                            },
                            Opts
                        ),
                    case Applied of
                        {ok, _} -> Applied;
                        _ -> rollback(St, Opts), Applied
                    end;
                {error, Error} ->
                    hb_slot_log:note(wasm_patch_decode, DecodeMicros),
                    rollback(St, Opts),
                    lib_patch_exec:error_result(Error);
                {malformed, Reason} ->
                    hb_slot_log:note(wasm_patch_decode, DecodeMicros),
                    rollback(St, Opts),
                    {error, #{ <<"status">> => 422, <<"body">> => Reason }}
            end;
        {trap, Reason} ->
            rollback(St, Opts),
            {error, #{
                <<"status">> => 500,
                <<"body">> => <<"wasm-patch@1.0 trap: ", Reason/binary>>
            }};
        {fault, Reason} ->
            % Not a result of the computation (a runtime stack overflow before
            % the pass's depth limit): nothing of it is stored, and the slot is
            % computed again later.
            rollback(St, Opts),
            {failure, #{
                <<"status">> => 500,
                <<"body">> => <<"wasm-patch@1.0 node fault: ", Reason/binary>>
            }}
    end.

%% One walk of the binary into the published terms; a result carrying
%% `commitments' or private keys, and an error, take `lua-patch@1.0''s own
%% normalisation.
decode(Reply, Opts) ->
    case hb_wasm_patch_codec:decode_terms(Reply) of
        {ok, Patches, Results} -> {ok, #{ <<"patches">> => Patches, <<"results">> => Results }};
        fallback -> decode_general(Reply, Opts);
        Malformed -> Malformed
    end.

decode_general(Reply, Opts) ->
    case hb_wasm_patch_codec:decode_result(Reply) of
        {ok, Result} -> {ok, lib_patch_exec:decode(Result, Opts)};
        {error, Error} ->
            {error, try lib_patch_exec:decode(Error, Opts) catch _:_ -> Error end};
        Malformed -> Malformed
    end.

%% Whether the process consumes tag values as literal URIs
%% (`lua-input-links'): a property of its signed definition, so it is worked
%% out at init or restore and kept with the instance's state. The lookup can
%% resolve through the whole state, and cost 0.9 ms a slot.
literal_links(false, St, _Base, _Opts) -> {false, St};
literal_links(true, St = #{ literal_links := Literal }, _Base, _Opts) -> {Literal, St};
literal_links(true, St, Base, Opts) ->
    Literal = lib_patch_exec:literal_input_links(Base, Opts),
    {Literal, St#{ literal_links => Literal }}.

%% Call `wp_handle' on an instance at the state's gen. Another holder of the
%% instance may move it between the check and the call; the NIF refuses that
%% call and it is retried.
call(St, ReqBin, Opts) -> call(St, ReqBin, 3, Opts).
call(St0, ReqBin, Tries, Opts) ->
    St = #{ inst := Inst, gen := Gen, fuel := Fuel } = ensure_current(St0, Opts),
    case hb_wasm_nif:call(Inst, 1, Gen, ReqBin, Fuel) of
        {ok, Reply, _FuelLeft} -> {St, {ok, Reply}};
        {trap, Reason} -> {St, {trap, Reason}};
        {fault, Reason} -> {St, {fault, Reason}};
        {error, Busy} when (Busy == stale orelse Busy == dirty), Tries > 1 ->
            call(St, ReqBin, Tries - 1, Opts);
        {error, Busy} -> erlang:error({wasm_patch_instance_busy, Busy})
    end.

%% The state after a successful call: one more gen, the request logged, and a
%% new in-memory checkpoint when the log is long.
advance(St = #{ inst := Inst, gen := Gen, log := Log, log_bytes := Bytes }, ReqBin, Slot, Opts) ->
    Next = St#{
        gen => Gen + 1,
        log => [ReqBin | Log],
        log_bytes => Bytes + byte_size(ReqBin),
        slot => Slot
    },
    Calls = hb_util:int(hb_opts:get(<<"wasm-patch-checkpoint-calls">>, ?CHECKPOINT_CALLS, Opts)),
    case length(Log) + 1 >= Calls orelse Bytes > ?CHECKPOINT_LOG_BYTES of
        false -> Next;
        true ->
            case hb_wasm_nif:snapshot(Inst) of
                {ok, G, Mem, Globals} when G == Gen + 1 ->
                    Next#{ ckpt => {G, Mem, Globals}, log => [], log_bytes => 0 };
                _ -> Next
            end
    end.

%% Put the instance back at the state's gen after a failed slot.
rollback(St, Opts) ->
    _ = ensure_current(St, Opts),
    ok.

%% @doc The instance at the state's gen: as it is, or restored from the
%% checkpoint with the logged requests replayed.
ensure_current(St = #{ inst := Inst, gen := Gen }, Opts) ->
    case hb_wasm_nif:gen(Inst) of
        Gen -> St;
        _ -> replay(St, Opts)
    end.

replay(St = #{ inst := Inst, gen := Gen, ckpt := {CkGen, Mem, Globals}, log := Log, fuel := Fuel }, _Opts) ->
    {Micros, ok} =
        timer:tc(fun() ->
            ok = hb_wasm_nif:restore(Inst, CkGen, Mem, Globals),
            lists:foldl(
                fun(ReqBin, G) ->
                    case hb_wasm_nif:call(Inst, 1, G, ReqBin, Fuel) of
                        {ok, _, _} -> G + 1;
                        Other -> erlang:error({wasm_patch_replay_diverged, G, Other})
                    end
                end,
                CkGen,
                lists:reverse(Log)
            ),
            ok
        end),
    hb_slot_log:note(wasm_patch_restore, Micros),
    Gen = hb_wasm_nif:gen(Inst),
    St.

%%% Snapshot

%% @doc `{abi, wasm_sha256, aot_key, slot, gen, globals, memory}', the memory
%% compressed. Taken on `process@1.0''s snapshot schedule.
snapshot(Base, _Req, Opts) ->
    case hb_private:get(<<"state">>, Base, not_found, Opts) of
        St = #{ device := wasm_patch } ->
            #{ inst := Inst } = Cur = ensure_current(St, Opts),
            T0 = erlang:monotonic_time(microsecond),
            {ok, Gen, Mem, Globals} = hb_wasm_nif:snapshot(Inst),
            T1 = erlang:monotonic_time(microsecond),
            hb_slot_log:note(wasm_snapshot_copy, T1 - T0),
            hb_slot_log:note(wasm_snapshot_bytes, byte_size(Mem)),
            % A checkpoint written off the slot (`lib_process_cache') is
            % compressed whole there: the memory goes in as it is, and the
            % slot only copies it.
            Raw = hb_opts:get(<<"snapshot-deferred-compression">>, false, Opts) == true,
            {Encoding, Memory} =
                case Raw of
                    true -> {<<"raw">>, Mem};
                    false -> {<<"zlib">>, compress(Mem)}
                end,
            T2 = erlang:monotonic_time(microsecond),
            hb_slot_log:note(wasm_snapshot_zlib, T2 - T1),
            Snapshot = #{
                <<"abi">> => ?ABI,
                <<"wasm-sha256">> => maps:get(wasm_sha256, Cur),
                <<"instrument">> => maps:get(instrument, Cur),
                <<"aot-key">> => hb_util:bin(maps:get(aot_key, Cur)),
                <<"slot">> => maps:get(slot, Cur),
                <<"gen">> => Gen,
                <<"globals">> => [{N, atom_to_binary(K), V} || {N, K, V} <- Globals],
                <<"memory">> => Memory,
                <<"memory-encoding">> => Encoding
            },
            Body = term_to_binary(Snapshot, [deterministic]),
            hb_slot_log:note(wasm_snapshot_t2b, erlang:monotonic_time(microsecond) - T2),
            hb_slot_log:note(wasm_snapshot_compressed_bytes, byte_size(Body)),
            {ok, #{ <<"body">> => Body }};
        _ ->
            {error, <<"Cannot snapshot wasm-patch state: not initialized.">>}
    end.

%% The memory, zlib-compressed at level 1. `zlib:compress/1' (level 6) took
%% 1.8-4 s of a 64 MB instance, inside the slot that takes the snapshot, with
%% the process's next messages waiting behind it; level 1 is several times
%% faster for a slightly larger image, and `zlib:uncompress/1' reads either.
compress(Mem) ->
    Z = zlib:open(),
    try
        ok = zlib:deflateInit(Z, 1),
        Out = iolist_to_binary(zlib:deflate(Z, Mem, finish)),
        ok = zlib:deflateEnd(Z),
        Out
    after zlib:close(Z)
    end.

%% @doc Restore the instance from a snapshot when the state has none.
normalize(Base, _Req, RawOpts) ->
    Opts = RawOpts#{ <<"hashpath">> => ignore },
    case hb_private:get(<<"state">>, Base, not_found, Opts) of
        not_found ->
            DeviceKey =
                case hb_ao:get(<<"device-key">>, {as, <<"message@1.0">>, Base}, Opts) of
                    not_found -> [];
                    K -> [K]
                end,
            case hb_ao:get([<<"snapshot">>] ++ DeviceKey ++ [<<"body">>],
                    {as, <<"message@1.0">>, Base}, Opts) of
                not_found -> throw({error, no_wasm_patch_snapshot_found});
                Body ->
                    try restore(Base, Body, Opts) of
                        St -> {ok, hb_private:set(Base, <<"state">>, St, Opts)}
                    catch throw:{wasm_patch_init, Reason} -> init_error(Reason)
                    end
            end;
        _ -> {ok, Base}
    end.

restore(Base, Body, Opts) ->
    Snapshot =
        try binary_to_term(Body, [safe])
        catch _:_ -> throw({wasm_patch_init, <<"the snapshot does not decode">>})
        end,
    #{
        <<"abi">> := ?ABI,
        <<"wasm-sha256">> := Hash,
        <<"gen">> := Gen,
        <<"globals">> := Globals,
        <<"memory">> := Memory
    } = Snapshot,
    {St0 = #{ inst := Inst, wasm_sha256 := Hash1, instrument := Pass }, Info} =
        instance(Base, Opts),
    case Hash1 of
        Hash -> ok;
        _ -> throw({wasm_patch_init, <<"the snapshot is of another image">>})
    end,
    % Another pass counts fuel differently and exports other globals.
    case maps:get(<<"instrument">>, Snapshot, <<"none">>) of
        Pass -> ok;
        _ -> throw({wasm_patch_init, <<"the snapshot was taken under another instrumentation pass">>})
    end,
    Mem =
        case maps:get(<<"memory-encoding">>, Snapshot, <<"zlib">>) of
            <<"raw">> -> Memory;
            <<"zlib">> -> zlib:uncompress(Memory)
        end,
    Gl = [{N, Kind, V} || {N, Kind, V} <- Globals, is_binary(N), is_integer(V)],
    case hb_wasm_nif:restore(Inst, Gen, Mem, Gl) of
        ok -> ok;
        {error, Reason} -> throw({wasm_patch_init, Reason})
    end,
    St0#{
        gen => Gen,
        fuel => fuel(<<"fuel-per-slot">>, ?DEFAULT_FUEL, Base, Info, Opts),
        literal_links => lib_patch_exec:literal_input_links(Base, Opts),
        ckpt => {Gen, Mem, Gl},
        log => [],
        log_bytes => 0,
        slot => maps:get(<<"slot">>, Snapshot, undefined)
    }.

error_text(E) when is_binary(E) -> E;
error_text(E) -> hb_util:bin(io_lib:format("~p", [E])).

%%% Tests

test_opts() ->
    test_opts(#{}).
test_opts(Extra) ->
    maps:merge(
        #{
            <<"hashpath">> => ignore,
            <<"wasm-patch-allow-uninstrumented">> => true,
            <<"wasm-patch-aot">> => false
        },
        Extra
    ).

test_wasm() ->
    {ok, Wasm} = file:read_file("test/wasm-patch/wp-test.wasm"),
    Wasm.

test_base() ->
    #{
        <<"device">> => ?DEVICE,
        <<"image">> => test_wasm(),
        <<"start">> => 10
    }.

test_request(Action) -> test_request(Action, 1).
test_request(Action, Number) ->
    #{
        <<"path">> => <<"compute">>,
        <<"body">> => #{ <<"action">> => Action, <<"number">> => Number }
    }.

run(State, Actions, Opts) ->
    lists:foldl(
        fun(Action, Prev) ->
            {ok, Next} = hb_ao:resolve(Prev, test_request(Action), Opts),
            Next
        end,
        State,
        Actions
    ).

count(State, Opts) -> hb_ao:get(<<"count">>, State, Opts).

init_handle_test() ->
    hb:init(),
    Opts = test_opts(),
    {ok, S0} = hb_ao:resolve(test_base(), <<"init">>, Opts),
    S5 = run(S0, lists:duplicate(5, <<"Increment">>), Opts),
    ?assertEqual(<<"15">>, count(S5, Opts)),
    ?assertEqual(<<"Increment">>, hb_ao:get(<<"last-action">>, S5, Opts)),
    ?assertEqual(<<"15">>, hb_ao:get(<<"results/output/data">>, S5, Opts)),
    % The request reached the module as sent: the integer comes back as one.
    ?assertEqual(1, hb_ao:get(<<"echo-number">>, S5, Opts)),
    S6 = run(S5, [<<"Delete">>], Opts),
    % Not `hb_ao:get': a key the state lacks is a call into the module.
    ?assertNot(maps:is_key(<<"last-action">>, S6)),
    ?assertEqual(<<"15">>, maps:get(<<"count">>, S6)).

%% A returned error, a trap and a result that does not decode each fail the
%% slot with the state unchanged, and the next slot computes from it.
failed_slots_leave_state_test() ->
    hb:init(),
    Opts = test_opts(),
    {ok, S0} = hb_ao:resolve(test_base(), <<"init">>, Opts),
    S1 = run(S0, [<<"Increment">>], Opts),
    ?assertMatch(
        {error, #{ <<"status">> := 500, <<"body">> := <<"boom">> }},
        hb_ao:resolve(S1, test_request(<<"Fail">>), Opts)
    ),
    {error, #{ <<"status">> := 500, <<"body">> := Trap }} =
        hb_ao:resolve(S1, test_request(<<"Trap">>), Opts),
    ?assertMatch(<<"wasm-patch@1.0 trap: ", _/binary>>, Trap),
    ?assertMatch(
        {error, #{ <<"status">> := 422 }},
        hb_ao:resolve(S1, test_request(<<"Bad">>), Opts)
    ),
    % Not 112 or 1012: neither failure's write survived.
    S2 = run(S1, [<<"Increment">>], Opts),
    ?assertEqual(<<"12">>, count(S2, Opts)).

%% A state that is not the instance's newest computes from its own memory:
%% the instance is restored and replayed to it, then back again.
stale_base_replays_test() ->
    hb:init(),
    Opts = test_opts(),
    {ok, S0} = hb_ao:resolve(test_base(), <<"init">>, Opts),
    S1 = run(S0, [<<"Increment">>], Opts),
    S3 = run(S1, [<<"Increment">>, <<"Increment">>], Opts),
    ?assertEqual(<<"13">>, count(S3, Opts)),
    ?assertEqual(<<"12">>, count(run(S1, [<<"Increment">>], Opts), Opts)),
    ?assertEqual(<<"14">>, count(run(S3, [<<"Increment">>], Opts), Opts)),
    ?assertEqual(<<"11">>, count(run(S0, [<<"Increment">>], Opts), Opts)).

%% Restore crosses an in-memory checkpoint and memory that grew after it.
checkpoint_and_growth_test() ->
    hb:init(),
    Opts = test_opts(#{ <<"wasm-patch-checkpoint-calls">> => 2 }),
    {ok, S0} = hb_ao:resolve(test_base(), <<"init">>, Opts),
    S3 = run(S0, [<<"Increment">>, <<"Increment">>, <<"Increment">>], Opts),
    S4 = run(S3, [<<"Grow">>], Opts),
    ?assertEqual(<<"14">>, count(S4, Opts)),
    {error, _} = hb_ao:resolve(S4, test_request(<<"Trap">>), Opts),
    ?assertEqual(<<"14">>, count(run(S3, [<<"Increment">>], Opts), Opts)),
    ?assertEqual(<<"15">>, count(run(S4, [<<"Increment">>], Opts), Opts)).

%% A snapshot restores into a new instance that continues where it was.
snapshot_restore_test() ->
    hb:init(),
    Opts = test_opts(),
    {ok, S0} = hb_ao:resolve(test_base(), <<"init">>, Opts),
    S5 = run(S0, lists:duplicate(5, <<"Increment">>), Opts),
    {ok, Snapshot} = hb_ao:resolve(S5, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(S5), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    S6 = run(Restored, [<<"Increment">>], Opts),
    ?assertEqual(<<"16">>, count(S6, Opts)),
    % The two instances are independent.
    ?assertEqual(<<"16">>, count(run(S5, [<<"Increment">>], Opts), Opts)),
    % A snapshot left uncompressed for a checkpoint written off the slot
    % restores the same.
    {ok, RawSnapshot} = hb_ao:resolve(S5, <<"snapshot">>,
        Opts#{ <<"snapshot-deferred-compression">> => true }),
    ?assertMatch(#{ <<"memory-encoding">> := <<"raw">> },
        binary_to_term(hb_ao:get(<<"body">>, RawSnapshot, Opts))),
    {ok, RawRestored} = hb_ao:resolve(
        hb_ao:set(hb_private:reset(S5), <<"snapshot">>, RawSnapshot, Opts), <<"normalize">>, Opts),
    ?assertEqual(<<"16">>, count(run(RawRestored, [<<"Increment">>], Opts), Opts)),
    % A snapshot of another image is refused.
    Other = hb_ao:set(Cold, <<"image">>, <<0, "asm", 1:32/little>>, Opts),
    ?assertMatch({error, #{ <<"status">> := 422 }}, hb_ao:resolve(Other, <<"normalize">>, Opts)).

%% longjmp through `invoke_vii' and `throw_longjmp' returns to the caller.
longjmp_test() ->
    hb:init(),
    Opts = test_opts(),
    {ok, S0} = hb_ao:resolve(test_base(), <<"init">>, Opts),
    S1 = run(S0, [<<"Jump">>, <<"Increment">>], Opts),
    ?assertEqual(7, hb_ao:get(<<"jumped">>, S1, Opts)),
    ?assertEqual(<<"11">>, count(S1, Opts)).

%% The same patches from Lua on `lua-patch@1.0' and from the module give the
%% same delta, term for term.
same_terms_as_lua_patch_test() ->
    hb:init(),
    Opts = test_opts(),
    Lua = types_script(),
    LuaBase = #{
        <<"device">> => <<"lua-patch@1.0">>,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => Lua }
    },
    {ok, L0} = hb_ao:resolve(LuaBase, <<"init">>, Opts),
    {ok, L1} = hb_ao:resolve(L0, test_request(<<"Types">>), Opts),
    {ok, W0} = hb_ao:resolve(test_base(), <<"init">>, Opts),
    {ok, W1} = hb_ao:resolve(W0, test_request(<<"Types">>), Opts),
    Delta = fun(S) -> published(hb_private:get(<<"process-cache-delta">>, S, Opts), Opts) end,
    ?assertEqual(Delta(L1), Delta(W1)).

%% Terms as published: the unsigned commitments `lua-patch@1.0''s decode adds
%% to every nested message, which the process strips, removed.
without_commitments(Map) when is_map(Map) ->
    maps:map(fun(_, V) -> without_commitments(V) end, maps:remove(<<"commitments">>, Map));
without_commitments(List) when is_list(List) -> [without_commitments(V) || V <- List];
without_commitments(Tuple) when is_tuple(Tuple) ->
    list_to_tuple(without_commitments(tuple_to_list(Tuple)));
without_commitments(V) -> V.

%% A decoded result as a process publishes it: its patches and results
%% applied, the keys they changed normalised by `structured@1.0'
%% (`lib_process:normalize_changed/3'), and the unsigned commitments gone.
published(#{ <<"patches">> := Patches, <<"results">> := Results }, Opts) ->
    PatchList = case Patches of #{} -> []; _ -> Patches end,
    ResultMsg = case Results of L when is_list(L) -> hb_ao:normalize_keys(L, Opts); M -> M end,
    {ok, State} = hb_state_patch:apply(#{}, PatchList, ResultMsg, Opts),
    {ok, Decoded} = hb_ao:raw(<<"structured@1.0">>, <<"to">>, State, #{}, Opts),
    without_commitments(hb_ao:execution_input(Decoded, Opts)).

%% The one-walk decode publishes what `lua-patch@1.0''s decode publishes, on
%% generated values: nested tables, lists, empty tables, mixed-case,
%% integer- and string-numbered keys, every scalar.
decode_terms_matches_general_test() ->
    hb:init(),
    Opts = test_opts(),
    rand:seed(exsss, {7, 11, 13}),
    Paths = lists:map(
        fun(_) ->
            Patches = [{rand:uniform(2), <<"/k", (integer_to_binary(I))/binary>>, gen_value(3)}
                || I <- lists:seq(1, rand:uniform(4))],
            Results = maps:from_list([{gen_key(), gen_value(2)} || _ <- lists:seq(1, rand:uniform(3))]),
            Bin = iolist_to_binary([
                0, <<(length(Patches)):32/little>>,
                [case Op of
                    1 -> [1, hb_wasm_patch_codec:encode(P), hb_wasm_patch_codec:encode(V)];
                    2 -> [2, hb_wasm_patch_codec:encode(P)]
                 end || {Op, P, V} <- Patches],
                hb_wasm_patch_codec:encode(Results)
            ]),
            {ok, General} = decode_general(Bin, Opts),
            case hb_wasm_patch_codec:decode_terms(Bin) of
                fallback -> fallback;
                {ok, P1, R1} ->
                    ?assertEqual(
                        published(General, Opts),
                        published(#{ <<"patches">> => P1, <<"results">> => R1 }, Opts)
                    ),
                    fast
            end
        end,
        lists:seq(1, 300)
    ),
    % Most take the one walk; an empty table in a list takes the general path.
    ?assert(length([ x || fast <- Paths ]) > 100),
    ?assert(lists:member(fallback, Paths)),
    % `commitments' anywhere takes the general path.
    WithC = iolist_to_binary([0, <<0:32/little>>,
        hb_wasm_patch_codec:encode(#{ <<"commitments">> => #{} })]),
    ?assertEqual(fallback, hb_wasm_patch_codec:decode_terms(WithC)).

gen_value(0) -> gen_scalar();
gen_value(D) ->
    case rand:uniform(6) of
        1 -> [gen_value(D - 1) || _ <- lists:seq(1, rand:uniform(3))];
        2 -> #{};
        3 -> maps:from_list([{I, gen_value(D - 1)} || I <- lists:seq(1, rand:uniform(3))]);
        4 -> maps:from_list([{integer_to_binary(I), gen_value(D - 1)} || I <- lists:seq(1, rand:uniform(3))]);
        5 -> maps:from_list([{gen_key(), gen_value(D - 1)} || _ <- lists:seq(1, rand:uniform(4))]);
        6 -> gen_scalar()
    end.

gen_key() ->
    lists:nth(rand:uniform(6), [<<"a">>, <<"B">>, <<"x-y">>, <<"2">>, 3, <<"0">>]).

gen_scalar() ->
    lists:nth(rand:uniform(6), [<<"s">>, <<>>, 42, -7, 1.5, true]).

%% The `Types' action of the test module, in Lua.
types_script() ->
    <<
        "function compute(req)\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/t-int', value = 42 },\n"
        "      { path = '/t-float', value = 1.5 },\n"
        "      { path = '/t-true', value = true },\n"
        "      { path = '/t-false', value = false },\n"
        "      { path = '/t-map', value = { a = 'x', n = 7 } },\n"
        "      { path = '/t-list', value = { 'p', 'q' } },\n"
        "      { path = '/t-empty', value = {} },\n"
        "      { path = '/t-mixed', value = { B = { { true }, { x = 1 } }, [3] = 42, x = 'y' } }\n"
        "    },\n"
        "    results = {}\n"
        "  }\n"
        "end\n"
    >>.

%% The request written in one walk is the term encoding's bytes exactly.
wire_encoding_matches_term_encoding_test() ->
    hb:init(),
    Opts = test_opts(),
    rand:seed(exsss, {5, 6, 7}),
    Signed = hb_message:commit(#{ <<"action">> => <<"Order.Place">>, <<"price">> => 12,
        <<"assign-to">> => binary:copy(<<"T">>, 43), <<"tags">> => [<<"a">>, <<"b">>] },
        #{ <<"priv-wallet">> => ar_wallet:new() }),
    Samples = [#{ <<"slot">> => 3, <<"body">> => Signed, <<"path">> => <<"compute">> }, #{}, []]
        ++ [ gen_value(4) || _ <- lists:seq(1, 300) ],
    lists:foreach(
        fun(Value) ->
            ?assertEqual(
                hb_wasm_patch_codec:encode(lib_patch_exec:encode(Value, term, Opts)),
                iolist_to_binary(lib_patch_exec:encode(Value, wire, Opts))
            )
        end,
        Samples
    ).

%% An image named by ID is pinned by `image-sha256': unpinned or mismatched
%% is refused, `data' (a gateway-fetched transaction) serves as the payload,
%% and an approved copy from the prewarm boots without the ID resolving.
image_pinning_test() ->
    hb:init(),
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = test_opts(#{ <<"store">> => Store,
        <<"wasm-patch-aot-dir">> => "_build/test-wasm-approved-" ++ integer_to_list(erlang:unique_integer([positive])) }),
    Wasm = test_wasm(),
    Sha = binary:encode_hex(crypto:hash(sha256, Wasm), lowercase),
    {ok, ByBody} = hb_cache:write(#{ <<"body">> => Wasm }, Opts),
    {ok, ByData} = hb_cache:write(#{ <<"data">> => Wasm }, Opts),
    Base = fun(Image, Pin) ->
        maps:merge((test_base())#{ <<"image">> => Image },
            case Pin of none -> #{}; _ -> #{ <<"image-sha256">> => Pin } end)
    end,
    Init = fun(B) -> hb_ao:resolve(B, <<"init">>, Opts) end,
    ?assertMatch({error, #{ <<"status">> := 422 }}, Init(Base(ByBody, none))),
    ?assertMatch({error, #{ <<"status">> := 422 }}, Init(Base(ByBody, binary:copy(<<"0">>, 64)))),
    ?assertMatch({ok, _}, Init(Base(ByBody, Sha))),
    ?assertMatch({ok, _}, Init(Base(ByData, string:uppercase(Sha)))),
    % Inline bytes need no pin, but a wrong pin is still refused.
    ?assertMatch({ok, _}, Init(Base(Wasm, none))),
    ?assertMatch({error, #{ <<"status">> := 422 }}, Init(Base(Wasm, binary:copy(<<"a">>, 64)))),
    % An unknown ID boots from the approved copy of the pinned hash.
    Missing = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertMatch({error, _}, Init(Base(Missing, Sha))),
    Dir = hb_util:list(maps:get(<<"wasm-patch-aot-dir">>, Opts)),
    ok = filelib:ensure_dir(filename:join([Dir, "approved", "x"])),
    ok = file:write_file(filename:join([Dir, "approved", hb_util:list(Sha) ++ ".wasm"]), Wasm),
    {ok, S0} = Init(Base(Missing, Sha)),
    ?assertEqual(<<"11">>, count(run(S0, [<<"Increment">>], Opts), Opts)).

%% A module importing anything outside the allow-list never starts.
refuses_imports_test() ->
    hb:init(),
    Entries = iolist_to_binary([1, 3, <<"env">>, 5, <<"abort">>, 0, 0]),
    Type = <<1, 16#60, 0, 0>>,
    Wasm = iolist_to_binary([
        <<0, "asm", 1:32/little>>,
        1, byte_size(Type), Type,
        2, byte_size(Entries), Entries
    ]),
    {error, #{ <<"status">> := 422, <<"body">> := Body }} =
        hb_ao:resolve((test_base())#{ <<"image">> => Wasm }, <<"init">>, test_opts()),
    ?assertNotEqual(nomatch, binary:match(Body, <<"env.abort">>)).

%% The production path: the module goes through the instrumentation pass
%% and runs metered. Out of fuel is a failed slot like any other trap; the
%% next slot computes from the state before it.
instrumented_fuel_test() ->
    hb:init(),
    Opts = test_opts(#{ <<"wasm-patch-allow-uninstrumented">> => false }),
    {ok, S0} = hb_ao:resolve((test_base())#{ <<"fuel-per-slot">> => 100000 }, <<"init">>, Opts),
    #{ instrument := Pass } = hb_private:get(<<"state">>, S0, Opts),
    ?assertMatch(<<"wasm-patch-instrument/", _/binary>>, Pass),
    S2 = run(S0, [<<"Increment">>, <<"Jump">>], Opts),
    ?assertEqual(<<"11">>, count(S2, Opts)),
    ?assertEqual(7, hb_ao:get(<<"jumped">>, S2, Opts)),
    ?assertMatch(
        {error, #{ <<"status">> := 500, <<"body">> := <<"wasm-patch@1.0 trap: out of fuel">> }},
        hb_ao:resolve(S2, test_request(<<"Loop">>), Opts)
    ),
    ?assertEqual(<<"12">>, count(run(S2, [<<"Increment">>], Opts), Opts)),
    % A snapshot carries the pass version and restores under the same pass.
    {ok, Snapshot} = hb_ao:resolve(S2, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(S2), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    ?assertEqual(<<"12">>, count(run(Restored, [<<"Increment">>], Opts), Opts)),
    % ...and is refused under none.
    ?assertMatch({error, #{ <<"status">> := 422 }},
        hb_ao:resolve(Cold, <<"normalize">>, test_opts())),
    % The pass refuses what may not run: here, a memory above the cap.
    ?assertMatch({error, #{ <<"status">> := 422 }},
        hb_ao:resolve(test_base(), <<"init">>, Opts#{ <<"wasm-patch-max-pages">> => 8 })).

%% AOT and the interpreter give the same deltas. Needs `wamrc' 2.2.0
%% (`HB_WAMRC' or on the path); without one AOT falls back to the
%% interpreter and the comparison is trivially equal.
aot_matches_interp_test_() ->
    {timeout, 120, fun() ->
        hb:init(),
        Dir = "_build/test-wasm-aot-" ++ integer_to_list(erlang:unique_integer([positive])),
        Interp = test_opts(#{ <<"wasm-patch-allow-uninstrumented">> => false }),
        Aot = Interp#{ <<"wasm-patch-aot">> => true, <<"wasm-patch-aot-dir">> => Dir },
        Actions = [<<"Increment">>, <<"Types">>, <<"Jump">>, <<"Fail">>, <<"Grow">>,
            <<"Increment">>, <<"Trap">>, <<"Loop">>, <<"Delete">>, <<"Increment">>],
        Deltas =
            fun(Opts) ->
                {ok, S0} = hb_ao:resolve((test_base())#{ <<"fuel-per-slot">> => 1000000 },
                    <<"init">>, Opts),
                {_, Rev} =
                    lists:foldl(
                        fun(Action, {Prev, Acc}) ->
                            case hb_ao:resolve(Prev, test_request(Action), Opts) of
                                {ok, Next} ->
                                    Delta = hb_private:get(<<"process-cache-delta">>, Next, Opts),
                                    {Next, [Delta | Acc]};
                                {error, #{ <<"body">> := <<"wasm-patch@1.0 trap: ", _/binary>> }} ->
                                    % Only the runtime's wording differs.
                                    {Prev, [trap | Acc]};
                                Error -> {Prev, [Error | Acc]}
                            end
                        end,
                        {S0, []},
                        Actions
                    ),
                {maps:get(aot_key, hb_private:get(<<"state">>, S0, Opts)), lists:reverse(Rev)}
            end,
        {interp, InterpDeltas} = Deltas(Interp),
        {AotKey, AotDeltas} = Deltas(Aot),
        io:format(user, "wasm-patch aot key: ~p~n", [AotKey]),
        ?assertEqual(InterpDeltas, AotDeltas)
    end}.

%% The Lua runtime (paralith-mono backend/native/wasm-lua) against
%% `lua-patch@1.0' on the same script: same published values and the same
%% delta every slot, under the interpreter and AOT, instrumented. Runs when
%% `HB_WASM_LUA' names the runtime's `.wasm'.
lua_runtime_matches_lua_patch_test_() ->
    {timeout, 300, fun() ->
        case os:getenv("HB_WASM_LUA") of
            false -> ok;
            Path ->
                hb:init(),
                {ok, Runtime} = file:read_file(Path),
                Script = lua_equivalent_script(),
                Module = #{ <<"content-type">> => <<"application/lua">>, <<"body">> => Script },
                Interp = test_opts(#{ <<"wasm-patch-allow-uninstrumented">> => false }),
                Aot = Interp#{ <<"wasm-patch-aot">> => true,
                    <<"wasm-patch-aot-dir">> => "_build/test-wasm-aot-lua" },
                Run =
                    fun(Base, Opts) ->
                        {ok, S0} = hb_ao:resolve(Base, <<"init">>, Opts),
                        {_, Rev} =
                            lists:foldl(
                                fun(N, {Prev, Acc}) ->
                                    Action = case N rem 5 of 0 -> <<"Fail">>; _ -> <<"Increment">> end,
                                    case hb_ao:resolve(Prev, test_request(Action, N), Opts) of
                                        {ok, Next} ->
                                            {Next, [published(hb_private:get(
                                                    <<"process-cache-delta">>, Next, Opts), Opts)
                                                | Acc]};
                                        {error, #{ <<"status">> := Status }} ->
                                            {Prev, [{error, Status} | Acc]}
                                    end
                                end,
                                {S0, []},
                                lists:seq(1, 25)
                            ),
                        lists:reverse(Rev)
                    end,
                Lua = Run(#{ <<"device">> => <<"lua-patch@1.0">>, <<"module">> => Module }, Interp),
                WasmBase = #{ <<"device">> => ?DEVICE, <<"image">> => Runtime, <<"module">> => Module },
                ?assertEqual(Lua, Run(WasmBase, Interp)),
                ?assertEqual(Lua, Run(WasmBase, Aot))
        end
    end}.

lua_equivalent_script() ->
    <<
        "Count = Count or 0
"
        "function compute(req)
"
        "  local body = req.body or req
"
        "  local action = tostring(body.action or '')
"
        "  if action == 'Fail' then Count = Count + 100; error('refused') end
"
        "  Count = Count + 1
"
        "  local t = {}
"
        "  for i = 1, 50 do t[i] = { n = i, s = tostring(i * Count) } end
"
        "  return {
"
        "    patches = {
"
        "      { path = '/count', value = tostring(Count) },
"
        "      { path = '/last-action', value = action },
"
        "      { path = '/rows', value = { first = t[1].s, last = t[50].s } }
"
        "    },
"
        "    results = { output = { data = tostring(Count) } }
"
        "  }
"
        "end
"
    >>.

%% Through `process@1.0', the module's values publish exactly as the same
%% values from Lua on `lua-patch@1.0' do: both processes' states after the
%% slot, key by key, as computed and as `now' reads them.
process_publishes_like_lua_patch_test_() ->
    {timeout, 120, fun() ->
        hb:init(),
        Opts = test_opts(#{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false
        }),
        Address = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts))),
        Run =
            fun(Fields) ->
                Process = hb_message:commit(
                    Fields#{
                        <<"device">> => <<"process@1.0">>,
                        <<"type">> => <<"Process">>,
                        <<"scheduler-device">> => <<"scheduler@1.0">>,
                        <<"authority">> => [Address],
                        <<"scheduler-location">> => Address
                    },
                    Opts
                ),
                {ok, _} = hb_cache:write(Process, Opts),
                ProcID = hb_message:id(Process, all, Opts),
                {ok, _} = hb_ao:resolve(Process,
                    hb_message:commit(#{
                        <<"path">> => <<"schedule">>,
                        <<"method">> => <<"POST">>,
                        <<"body">> => hb_message:commit(#{ <<"target">> => ProcID,
                            <<"type">> => <<"Message">>, <<"action">> => <<"Types">> }, Opts)
                    }, Opts),
                    Opts),
                {ok, State} = hb_ao:resolve(Process,
                    #{ <<"path">> => <<"compute">>, <<"slot">> => 0 }, Opts),
                Keys = [ K || K <- maps:keys(State), is_binary(K),
                    binary:match(K, <<"t-">>) =:= {0, 2} ],
                {ok, Now} = hb_ao:resolve(Process, #{ <<"path">> => <<"now">> }, Opts),
                {
                    hb_cache:ensure_all_loaded(maps:with(Keys, State), Opts),
                    hb_cache:ensure_all_loaded(maps:with(Keys, Now), Opts)
                }
            end,
        {Lua, LuaNow} = Run(#{
            <<"execution-device">> => <<"lua-patch@1.0">>,
            <<"module">> => #{ <<"content-type">> => <<"application/lua">>,
                <<"body">> => types_script() }
        }),
        {Wasm, WasmNow} = Run(#{ <<"execution-device">> => ?DEVICE, <<"image">> => test_wasm() }),
        ?assertEqual(8, map_size(Lua)),
        ?assertEqual(Lua, Wasm),
        ?assertEqual(LuaNow, WasmNow)
    end}.

%% A `process@1.0' process on this device: slots stored as deltas, a failed
%% slot that does not halt it, and old slots read back.
process_test_() ->
    {timeout, 120, fun() ->
        hb:init(),
        Opts = test_opts(#{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 2
        }),
        Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
        Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
        Process = hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => ?DEVICE,
                <<"image">> => test_wasm(),
                <<"wasm-patch-abi">> => 1,
                <<"start">> => 10,
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address
            },
            Opts
        ),
        {ok, _} = hb_cache:write(Process, Opts),
        ProcID = hb_message:id(Process, all, Opts),
        Schedule =
            fun(Action) ->
                {ok, _} = hb_ao:resolve(Process,
                    hb_message:commit(
                        #{
                            <<"path">> => <<"schedule">>,
                            <<"method">> => <<"POST">>,
                            <<"body">> => hb_message:commit(
                                #{
                                    <<"target">> => ProcID,
                                    <<"type">> => <<"Message">>,
                                    <<"action">> => Action
                                },
                                Opts
                            )
                        },
                        Opts
                    ),
                    Opts)
            end,
        Compute =
            fun(Slot) ->
                hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts)
            end,
        % Slots count from 0.
        Schedule(<<"Increment">>),
        Schedule(<<"Fail">>),
        Schedule(<<"Increment">>),
        {ok, State0} = Compute(0),
        ?assertEqual(<<"11">>, hb_ao:get(<<"count">>, State0, Opts)),
        {ok, State1} = Compute(1),
        ?assertEqual(<<"11">>, hb_ao:get(<<"count">>, State1, Opts)),
        ?assertEqual(<<"error">>, hb_ao:get(<<"results/type">>, State1, Opts)),
        ?assertEqual(<<"boom">>, hb_ao:get(<<"results/error/body">>, State1, Opts)),
        {ok, State2} = Compute(2),
        ?assertEqual(<<"12">>, hb_ao:get(<<"count">>, State2, Opts)),
        Schedule(<<"Increment">>),
        {ok, State3} = Compute(3),
        ?assertEqual(<<"13">>, hb_ao:get(<<"count">>, State3, Opts)),
        {ok, Old} = Compute(0),
        ?assertEqual(<<"11">>, hb_ao:get(<<"count">>, Old, Opts))
    end}.

%% `bridge-root@1.0' through `ao.resolve': the Lua runtime's own
%% bridge_root.lua, over this NIF's verification natives (`wp_bls_fav',
%% `wp_rsa_pss_sha256'), against the Erlang device under `lua-patch@1.0'.
%% Every path PUSD uses (`possession', `verify'), valid and refused, gives
%% the same status and the same value as Lua sees it, under the interpreter
%% and AOT, instrumented. Runs when `HB_WASM_LUA' names the runtime's
%% `.wasm'; `HB_BRIDGE_ROOT_VECTORS' also writes the vectors and lua-patch's
%% answers as JSON, for the runtime's own host test (runtime.test.mjs).
bridge_root_matches_lua_patch_test_() ->
    {timeout, 900, fun() ->
        case os:getenv("HB_WASM_LUA") of
            false -> ok;
            Path ->
                hb:init(),
                {ok, Runtime} = file:read_file(Path),
                Vectors = br_vectors(),
                Module = #{ <<"content-type">> => <<"application/lua">>,
                    <<"body">> => br_script() },
                Interp = test_opts(#{ <<"wasm-patch-allow-uninstrumented">> => false }),
                Aot = Interp#{ <<"wasm-patch-aot">> => true,
                    <<"wasm-patch-aot-dir">> => "_build/test-wasm-aot-bridge-root" },
                Run =
                    fun(Base, Opts) ->
                        {ok, S0} = hb_ao:resolve(Base, <<"init">>, Opts),
                        lists:map(
                            fun({Name, _Expect, Call}) ->
                                {ok, S} = hb_ao:resolve(S0,
                                    #{ <<"path">> => <<"compute">>,
                                        <<"body">> => #{ <<"call">> => Call } }, Opts),
                                {Name, hb_ao:get(<<"r">>, S, Opts)}
                            end,
                            Vectors
                        )
                    end,
                Lua = Run(#{ <<"device">> => <<"lua-patch@1.0">>, <<"module">> => Module }, Interp),
                % Each vector says what the Erlang device answers, so a vector
                % that stopped testing what it names fails here.
                lists:foreach(
                    fun({{Name, Expect, _}, {Name, Out}}) ->
                        ?assertEqual({Name, Expect},
                            {Name, binary_to_atom(hd(binary:split(Out, <<"|">>)))})
                    end,
                    lists:zip(Vectors, Lua)
                ),
                case os:getenv("HB_BRIDGE_ROOT_VECTORS") of
                    false -> ok;
                    Out ->
                        ok = file:write_file(Out, hb_json:encode(#{
                            <<"script">> => br_script(),
                            <<"vectors">> => [
                                #{ <<"name">> => Name, <<"call">> => br_json(Call),
                                    <<"expect">> => Result }
                            || {{Name, _, Call}, {Name, Result}} <- lists:zip(Vectors, Lua)]
                        }))
                end,
                WasmBase = #{ <<"device">> => ?DEVICE, <<"image">> => Runtime, <<"module">> => Module },
                lists:foreach(
                    fun(Opts) ->
                        lists:foreach(
                            fun({Expected, Got}) -> ?assertEqual(Expected, Got) end,
                            lists:zip(Lua, Run(WasmBase, Opts))
                        )
                    end,
                    [Interp, Aot]
                ),
                % A native charges its fixed fuel (FUEL_BLS, 1e9) before it
                % runs: a slot that cannot pay traps as out of fuel, the same
                % under both.
                {_, _, PopValid} = hd(Vectors),
                lists:foreach(
                    fun(Opts) ->
                        {ok, Low} = hb_ao:resolve(WasmBase#{ <<"fuel-per-slot">> => 500000000 },
                            <<"init">>, Opts),
                        ?assertMatch(
                            {error, #{ <<"body">> := <<"wasm-patch@1.0 trap: out of fuel">> }},
                            hb_ao:resolve(Low, #{ <<"path">> => <<"compute">>,
                                <<"body">> => #{ <<"call">> => PopValid } }, Opts))
                    end,
                    [Interp, Aot]
                )
        end
    end}.

%% A value for the JSON fixture: every binary as base64, so any bytes survive.
br_json(Bin) when is_binary(Bin) -> #{ <<"b">> => base64:encode(Bin) };
br_json(Int) when is_integer(Int) -> #{ <<"i">> => Int };
br_json(List) when is_list(List) -> [br_json(V) || V <- List];
br_json(Map) when is_map(Map) -> #{ <<"m">> => maps:map(fun(_, V) -> br_json(V) end, Map) }.

%% Calls `ao.resolve' with the request in the slot's body and publishes the
%% answer serialised canonically: sorted keys, every string as hex.
br_script() ->
    <<
        "local HEX = '0123456789abcdef'\n"
        "local function hex(s)\n"
        "  local t = {}\n"
        "  for i = 1, #s do\n"
        "    local b = string.byte(s, i)\n"
        "    local hi, lo = b // 16 + 1, b % 16 + 1\n"
        "    t[i] = string.sub(HEX, hi, hi) .. string.sub(HEX, lo, lo)\n"
        "  end\n"
        "  return table.concat(t)\n"
        "end\n"
        "local function ser(v)\n"
        "  local t = type(v)\n"
        "  if t == 'string' then return 's' .. hex(v) end\n"
        "  if t ~= 'table' then return t .. ':' .. tostring(v) end\n"
        "  local keys = {}\n"
        "  for k in pairs(v) do keys[#keys + 1] = { ser(k), k } end\n"
        "  table.sort(keys, function(a, b) return a[1] < b[1] end)\n"
        "  local out = {}\n"
        "  for i, p in ipairs(keys) do out[i] = p[1] .. '=' .. ser(v[p[2]]) end\n"
        "  return '{' .. table.concat(out, ',') .. '}'\n"
        "end\n"
        "function compute(req)\n"
        "  local body = req.body or req\n"
        "  local status, value = ao.resolve({ device = 'bridge-root@1.0' }, body.call)\n"
        "  local out = tostring(status) .. '|' .. ser(value)\n"
        "  return { patches = { { path = '/r', value = out } },\n"
        "    results = { output = { data = out } } }\n"
        "end\n"
    >>.

-define(BR_DST, <<"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_PUSD_ROOT_">>).
-define(BR_POP_DST, <<"BLS_POP_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_PUSD_ROOT_">>).
-define(BR_P, 16#1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab).

br_member(I) ->
    {Secret, Key} = blst_nif:keygen(<<I:256>>),
    AO = hb_util:human_id(crypto:hash(sha256, <<"member", I:32>>)),
    {Secret, Key, AO}.

br_hex(Key) -> <<"0x", (binary:encode_hex(Key, lowercase))/binary>>.

br_pop(Secret, AO, Key, Dst) ->
    blst_nif:sign(Secret, <<"PUSD_BLS_POSSESSION_V1", (hb_util:decode(AO))/binary, Key/binary>>, Dst).

%% Points on the curves outside the prime-order subgroups (as in
%% dev_bridge_root's tests).
br_fp(X) -> ((X rem ?BR_P) + ?BR_P) rem ?BR_P.
br_fp_pow(B, E) -> binary:decode_unsigned(crypto:mod_pow(br_fp(B), E, ?BR_P)).
br_fp_square(A) -> br_fp_pow(A, (?BR_P - 1) div 2) =:= 1.
br_g1_off(X) ->
    case br_fp_square(X * X * X + 4) of
        true ->
            Y = br_fp_pow(X * X * X + 4, (?BR_P + 1) div 4),
            <<1:1, 0:1, (case Y > (?BR_P - 1) div 2 of true -> 1; false -> 0 end):1, X:381>>;
        false -> br_g1_off(X + 1)
    end.
br_g2_off(C1) ->
    {S0, S1} = {br_fp(1 - C1 * C1), br_fp(2 * C1)},
    {A0, A1} = {br_fp(S0 - S1 * C1 + 4), br_fp(S0 * C1 + S1 + 4)},
    case br_fp_square(A0 * A0 + A1 * A1) of
        true -> <<1:1, 0:1, 0:1, C1:381, 1:384>>;
        false -> br_g2_off(C1 + 1)
    end.

%% The author's signed item for an operation, as the device rebuilds it.
br_item(Wallet, Root, Operation) ->
    Author = hb_util:human_id(crypto:hash(sha256, ar_wallet:to_pubkey(Wallet))),
    Payload = Operation#{ <<"target">> => Root, <<"author">> => Author,
        <<"certificate-version">> => <<"pusd-root/v1">> },
    Anchor = case maps:get(<<"anchor">>, Payload, not_found) of
        not_found -> <<>>;
        A -> hb_util:decode(A)
    end,
    ar_bundles:sign_item(#tx{ format = ans104, target = hb_util:decode(Root), anchor = Anchor,
        tags = lists:sort(maps:to_list(maps:without([<<"target">>, <<"anchor">>], Payload))) },
        Wallet).

br_bitmap(N, Indices) ->
    Bytes = (N + 7) div 8,
    <<(lists:foldl(fun(I, Acc) -> Acc bor (1 bsl I) end, 0, Indices)):(Bytes * 8)/little>>.

br_signing_bytes(Root, Epoch, IntentID) ->
    <<"PUSD_ROOT_CERTIFICATE_V2", (hb_util:decode(Root))/binary, Epoch:64/big,
        (hb_util:decode(IntentID))/binary>>.

br_endorse(Members, Indices, Bytes) ->
    <<(br_bitmap(length(Members), Indices))/binary,
        (blst_nif:aggregate([blst_nif:sign(element(1, lists:nth(I + 1, Members)), Bytes, ?BR_DST)
            || I <- Indices]))/binary>>.

%% {Name, what the Erlang device answers, the request}.
br_vectors() ->
    Members = [br_member(I) || I <- lists:seq(1, 17)],
    [{S1, K1, AO1}, {_S2, K2, AO2} | _] = Members,
    Pop1 = br_pop(S1, AO1, K1, ?BR_POP_DST),
    Pop1Text = hb_util:encode(Pop1),
    Poss = fun(Fields) -> maps:merge(#{ <<"path">> => <<"possession">>, <<"ao">> => AO1,
        <<"bls">> => br_hex(K1), <<"pop">> => Pop1Text }, Fields) end,
    OffG1 = br_g1_off(1),
    Aliased = << <<(case C of $- -> $+; $_ -> $/; _ -> C end)>> || <<C>> <= Pop1Text >>,
    <<AOHead:42/binary, AOLast>> = AO1,
    Possession = [
        {<<"pop-valid">>, ok, Poss(#{})},
        {<<"pop-key-upper-hex">>, ok, Poss(#{ <<"bls">> => <<"0x", (binary:encode_hex(K1))/binary>> })},
        {<<"pop-key-raw">>, ok, Poss(#{ <<"bls">> => K1 })},
        {<<"pop-hex">>, ok, Poss(#{ <<"pop">> => br_hex(Pop1) })},
        {<<"pop-raw">>, ok, Poss(#{ <<"pop">> => Pop1 })},
        {<<"pop-aliased">>, ok, Poss(#{ <<"pop">> => Aliased })},
        {<<"pop-padded">>, ok, Poss(#{ <<"pop">> => <<Pop1Text/binary, "==">> })},
        {<<"pop-missing">>, error, maps:remove(<<"pop">>, Poss(#{}))},
        {<<"pop-empty">>, error, Poss(#{ <<"pop">> => <<>> })},
        {<<"pop-long">>, error, Poss(#{ <<"pop">> => <<Pop1Text/binary, "AA">> })},
        {<<"pop-bad-char">>, error, Poss(#{ <<"pop">> => <<(binary:part(Pop1Text, 0, 127))/binary, "!">> })},
        {<<"pop-other-key">>, error, Poss(#{ <<"bls">> => br_hex(K2) })},
        {<<"pop-other-ao">>, error, Poss(#{ <<"ao">> => AO2 })},
        {<<"pop-wrong-tag">>, error, Poss(#{ <<"pop">> => hb_util:encode(br_pop(S1, AO1, K1, ?BR_DST)) })},
        {<<"pop-garbage">>, error, Poss(#{ <<"pop">> => binary:copy(<<7>>, 96) })},
        {<<"pop-infinity">>, error, Poss(#{ <<"bls">> => br_hex(<<16#c0, 0:376>>),
            <<"pop">> => br_hex(<<16#c0, 0:760>>) })},
        {<<"pop-off-subgroup-key">>, error, Poss(#{ <<"bls">> => br_hex(OffG1) })},
        {<<"pop-off-subgroup-sig">>, error, Poss(#{ <<"pop">> => br_hex(br_g2_off(1)) })},
        {<<"pop-key-bad-hex">>, error, Poss(#{ <<"bls">> => <<"0x", (binary:part(br_hex(K1), 2, 95))/binary, "g">> })},
        {<<"pop-key-short">>, error, Poss(#{ <<"bls">> => binary:part(br_hex(K1), 0, 96) })},
        {<<"pop-ao-noncanonical">>, error, Poss(#{ <<"ao">> => <<AOHead/binary,
            (case AOLast of $A -> $B; _ -> $A end)>> })},
        {<<"pop-ao-short">>, error, Poss(#{ <<"ao">> => AOHead })}
    ],
    Wallet = ar_wallet:new(),
    Root = hb_util:human_id(crypto:hash(sha256, <<"root">>)),
    Committee = fun(N) -> [#{ <<"ao">> => AO, <<"bls">> => br_hex(K) } || {_, K, AO} <- lists:sublist(Members, N)] end,
    Base = #{ <<"action">> => <<"Transfer">>, <<"quantity">> => <<"50">>,
        <<"recipient">> => AO2, <<"attestor-epoch">> => <<"7">>, <<"root-sequence">> => <<"1">> },
    %% A certificate: the operation the author signs, the JSON the request
    %% carries (default: its encoding), the committee size and signers.
    Cert =
        fun(Opts) ->
            Op = maps:get(op, Opts, Base),
            Epoch = maps:get(epoch, Opts, 7),
            N = maps:get(n, Opts, 3),
            Item = br_item(Wallet, Root, Op),
            IntentID = hb_util:human_id(Item#tx.id),
            Bytes = br_signing_bytes(Root, Epoch, IntentID),
            #{ <<"path">> => <<"verify">>, <<"root">> => Root,
                <<"epoch">> => maps:get(epoch_value, Opts, Epoch),
                <<"intent-id">> => IntentID, <<"committee">> => Committee(N),
                <<"operation">> => maps:get(json, Opts, hb_json:encode(Op)),
                <<"authorization">> => <<(Item#tx.owner)/binary, (Item#tx.signature)/binary>>,
                <<"endorsements">> => br_endorse(lists:sublist(Members, N),
                    maps:get(signers, Opts, lists:seq(0, N div 2)), Bytes) }
        end,
    Valid = Cert(#{}),
    WithOp = fun(Extra) -> Cert(#{ op => maps:merge(Base, Extra) }) end,
    %% JSON the device reads as Base: the encoding with text spliced in after
    %% the opening brace.
    Spliced = fun(Prefix) ->
        <<"{", Rest/binary>> = hb_json:encode(Base),
        Cert(#{ json => <<"{", Prefix/binary, Rest/binary>> })
    end,
    BigEpoch = 18446744073709551615,
    Verify = [
        {<<"cert-valid">>, ok, Valid},
        {<<"cert-one-member">>, ok, Cert(#{ n => 1 })},
        {<<"cert-sixteen">>, ok, Cert(#{ n => 16 })},
        {<<"cert-other-majority">>, ok, Cert(#{ n => 5, signers => [1, 3, 4] })},
        {<<"cert-anchor">>, ok, WithOp(#{ <<"anchor">> => hb_util:human_id(crypto:hash(sha256, <<"a">>)) })},
        {<<"cert-epoch-text">>, ok, Cert(#{ epoch_value => <<"7">> })},
        {<<"cert-epoch-plus">>, ok, Cert(#{ epoch_value => <<"+07">> })},
        {<<"cert-epoch-max">>, ok, Cert(#{ epoch => BigEpoch, epoch_value => integer_to_binary(BigEpoch),
            op => Base#{ <<"attestor-epoch">> => integer_to_binary(BigEpoch) } })},
        {<<"cert-epoch-negative">>, error, Cert(#{ epoch_value => -7 })},
        {<<"cert-epoch-other">>, error, Cert(#{ epoch_value => 8 })},
        {<<"cert-minority">>, error, Cert(#{ signers => [0] })},
        {<<"cert-bit-past-committee">>, error, (fun() ->
            #{ <<"endorsements">> := <<Map, Agg/binary>> } = Valid,
            Valid#{ <<"endorsements">> => <<(Map bor 8), Agg/binary>> } end)()},
        {<<"cert-long-bitmap">>, error, (fun() ->
            #{ <<"endorsements">> := <<Map, Agg/binary>> } = Valid,
            Valid#{ <<"endorsements">> => <<Map, 0, Agg/binary>> } end)()},
        {<<"cert-identity">>, error, Valid#{ <<"endorsements">> => <<3, 16#c0, 0:760>> }},
        {<<"cert-off-subgroup">>, error, Valid#{ <<"endorsements">> => <<3, (br_g2_off(1))/binary>> }},
        {<<"cert-tampered">>, error, Valid#{ <<"operation">> =>
            hb_json:encode(Base#{ <<"quantity">> => <<"5000">> }) }},
        {<<"cert-authorization-zero">>, error, Valid#{ <<"authorization">> => <<0:8192>> }},
        {<<"cert-intent-other">>, error, Valid#{ <<"intent-id">> => Root }},
        {<<"cert-root-other">>, error, Valid#{ <<"root">> => AO1 }},
        {<<"cert-committee-17">>, error, Valid#{ <<"committee">> => Committee(17) }},
        {<<"cert-committee-empty">>, error, Valid#{ <<"committee">> => [] }},
        {<<"cert-committee-repeat-key">>, error, Valid#{ <<"committee">> =>
            (fun([M0, M1, M2]) -> [M0, M1#{ <<"bls">> => string:uppercase(maps:get(<<"bls">>, M0)) }, M2] end)(Committee(3)) }},
        {<<"cert-committee-raw-key">>, ok, Valid#{ <<"committee">> =>
            [M#{ <<"bls">> => K } || {M, {_, K, _}} <- lists:zip(Committee(3), lists:sublist(Members, 3))] }},
        {<<"cert-committee-loose-ao">>, ok, Valid#{ <<"committee">> =>
            [M#{ <<"ao">> => <<"member-", (integer_to_binary(I))/binary>> } || {I, M} <- lists:enumerate(Committee(3))] }},
        {<<"cert-json-first-wins">>, ok, Spliced(<<"\"quantity\":\"50\",\"quantity\":\"5000\",">>)},
        {<<"cert-json-later-number">>, ok, Spliced(<<"\"quantity\":\"50\",\"quantity\":-1.5e+3,">>)},
        {<<"cert-json-later-nested">>, ok, Spliced(<<"\"quantity\":\"50\",\"quantity\":{\"a\":[1,true,null,\"x\",{}]},">>)},
        {<<"cert-json-overflow">>, error, Spliced(<<"\"quantity\":\"50\",\"quantity\":1e400,">>)},
        {<<"cert-json-bad-utf8">>, error, Spliced(<<"\"quantity\":\"50\",\"quantity\":\"", 16#c0, 16#af, "\",">>)},
        {<<"cert-json-lone-surrogate">>, error, Spliced(<<"\"quantity\":\"50\",\"quantity\":\"\\ud800\",">>)},
        {<<"cert-json-number-value">>, error, Spliced(<<"\"zz\":1,">>)},
        {<<"cert-json-escaped-key">>, ok, (fun() ->
            <<"{", Rest/binary>> = hb_json:encode(maps:remove(<<"action">>, Base)),
            Valid#{ <<"operation">> => <<"{\"\\u0061ction\":\"Tr\\u0061nsfer\",", Rest/binary>> } end)()},
        {<<"cert-json-trailing">>, error, Valid#{ <<"operation">> =>
            <<(maps:get(<<"operation">>, Valid))/binary, "x">> }},
        {<<"cert-key-newline">>, ok, WithOp(#{ <<"note\n">> => <<"x">> })},
        {<<"cert-key-upper">>, error, WithOp(#{ <<"Note">> => <<"x">> })},
        {<<"cert-key-private">>, error, WithOp(#{ <<"private">> => <<"x">> })},
        {<<"cert-key-reserved">>, error, WithOp(#{ <<"body">> => <<"x">> })},
        {<<"cert-too-long">>, error, WithOp(#{ <<"pad">> => binary:copy(<<"x">>, 2048) })},
        {<<"cert-from-stamp">>, ok, WithOp(#{ <<"from-process">> => AO1 })},
        {<<"cert-data-key">>, ok, WithOp(#{ <<"ao-data-key">> => <<"x">> })},
        {<<"cert-data-key-existing">>, ok, WithOp(#{ <<"ao-data-key">> => <<"author">> })},
        {<<"cert-data-key-own">>, ok, WithOp(#{ <<"ao-data-key">> => <<"action">> })},
        {<<"cert-data-key-refused">>, error, WithOp(#{ <<"ao-data-key">> => <<"owner">> })},
        {<<"cert-data-key-private">>, error, WithOp(#{ <<"ao-data-key">> => <<"PRIVx">> })},
        {<<"cert-bundle-format">>, ok, WithOp(#{ <<"bundle-format">> => <<"binary">> })},
        {<<"cert-bundle-binary">>, ok, WithOp(#{ <<"bundle-format">> => <<"binary">>,
            <<"bundle-version">> => <<"2.0.0">> })},
        {<<"cert-bundle-map">>, ok, WithOp(#{ <<"bundle-format">> => <<"binary">>,
            <<"bundle-version">> => <<"2.0.0">>, <<"bundle-map">> => <<"x">> })},
        {<<"cert-bundle-version">>, ok, WithOp(#{ <<"bundle-version">> => <<"2.0.0">> })},
        {<<"cert-bundle-upper">>, ok, WithOp(#{ <<"bundle-format">> => <<"BINARY">>,
            <<"bundle-version">> => <<"2.0.0">> })},
        {<<"cert-bundle-map-empty">>, ok, WithOp(#{ <<"bundle-format">> => <<"binary">>,
            <<"bundle-version">> => <<"2.0.0">>, <<"bundle-map">> => <<>> })},
        {<<"cert-ao-type-binary">>, ok, WithOp(#{ <<"ao-type">> => <<"binary">> })},
        {<<"cert-ao-type-other">>, ok, WithOp(#{ <<"ao-type">> => <<"list">> })}
    ],
    Possession ++ Verify.

%% @doc Per-slot phase means, run only when asked:
%% `HB_WASM_PATCH_BENCH=2000 rebar3 device test -d dev_wasm_patch'
%% (with `HB_WAMRC' set for the AOT rows). A trivial direct call, and a
%% process slot carrying a signed order-shaped message.
bench_report_test_() ->
    {timeout, 600, fun() ->
        case os:getenv("HB_WASM_PATCH_BENCH") of
            false -> ok;
            Spec ->
                N = list_to_integer(Spec),
                Dir = "_build/bench-wasm-aot",
                Rows = [
                    {Mode, Kind, bench(Kind, N, test_opts(#{
                        <<"wasm-patch-allow-uninstrumented">> => false,
                        <<"wasm-patch-aot">> => Mode =:= aot,
                        <<"wasm-patch-aot-dir">> => Dir
                    }))}
                 || Mode <- [interp, aot], Kind <- [trivial, process]
                ],
                io:format(user, "WASM_PATCH_BENCH ~p~n", [Rows])
        end
    end}.

bench(trivial, N, Opts) ->
    hb:init(),
    {ok, S0} = hb_ao:resolve(bench_base(), <<"init">>, Opts),
    hb_phase_timer:reset(),
    {_, Times} =
        lists:foldl(
            fun(_, {Prev, Acc}) ->
                {Us, {ok, Next}} = timer:tc(fun() ->
                    hb_ao:resolve(Prev, test_request(<<"Increment">>), Opts) end),
                {Next, [Us | Acc]}
            end,
            {S0, []},
            lists:seq(1, N)
        ),
    bench_summary(Times);
bench(process, N, Opts0) ->
    hb:init(),
    Opts = Opts0#{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new(),
        <<"spawn-worker">> => false,
        <<"process-workers">> => false,
        <<"process-delta-cache">> => true,
        <<"process-snapshot-target">> => false
    },
    Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Process = hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => ?DEVICE,
            <<"authority">> => [Address],
            <<"scheduler-location">> => Address
        }#{ <<"image">> => maps:get(<<"image">>, bench_base()) }
            #{ <<"module">> => maps:get(<<"module">>, bench_base(), <<"none">>) },
        Opts
    ),
    {ok, _} = hb_cache:write(Process, Opts),
    ProcID = hb_message:id(Process, all, Opts),
    lists:foreach(
        fun(I) ->
            {ok, _} = hb_ao:resolve(Process,
                hb_message:commit(
                    #{
                        <<"path">> => <<"schedule">>,
                        <<"method">> => <<"POST">>,
                        <<"body">> => hb_message:commit(
                            #{
                                <<"target">> => ProcID,
                                <<"type">> => <<"Message">>,
                                <<"action">> => <<"Order.Place">>,
                                <<"pair">> => <<"RUNE-PUSD">>,
                                <<"side">> => <<"buy">>,
                                <<"price">> => <<"1250000">>,
                                <<"quantity">> => <<"40000000">>,
                                <<"client-order-id">> => hb_util:bin(I),
                                <<"time-in-force">> => <<"GTC">>,
                                <<"post-only">> => <<"false">>,
                                <<"reduce-only">> => <<"false">>,
                                <<"nonce">> => hb_util:bin(I),
                                <<"recipient">> => Address
                            },
                            Opts
                        )
                    },
                    Opts
                ),
                Opts)
        end,
        lists:seq(1, N)
    ),
    {ok, _} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => 1 }, Opts),
    hb_phase_timer:reset(),
    Times =
        [ element(1, timer:tc(fun() ->
            {ok, _} = hb_ao:resolve(Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts)
          end))
        || Slot <- lists:seq(2, N - 1) ],
    bench_summary(Times).

%% The test module, or the Lua runtime running `lua_equivalent_script/0'.
bench_base() ->
    case os:getenv("HB_WASM_LUA") of
        false -> test_base();
        Path ->
            {ok, Runtime} = file:read_file(Path),
            #{
                <<"device">> => ?DEVICE,
                <<"image">> => Runtime,
                <<"module">> => #{
                    <<"content-type">> => <<"application/lua">>,
                    <<"body">> => lua_equivalent_script()
                }
            }
    end.

bench_summary(Times) ->
    Sorted = lists:sort(Times),
    Len = length(Sorted),
    Phases =
        [ {P, Mean}
        || {P, _T, _C, Mean} <- hb_phase_timer:read(),
           lists:member(P, [wasm_patch_prep, wasm_patch_call, wasm_patch_decode,
               wasm_patch_apply, wasm_patch_restore, slot_execution, slot_store])
        ],
    #{
        n => Len,
        p50_us => lists:nth(max(1, Len div 2), Sorted),
        p95_us => lists:nth(max(1, (Len * 95) div 100), Sorted),
        phase_mean_us => Phases
    }.

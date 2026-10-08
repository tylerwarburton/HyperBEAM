#!/bin/sh
# One-time migration of the essentials already in the main store into the
# essentials store, on the RUNNING node (online: the main store is only read),
# then a byte-for-byte verification of every assignment and definition.
# usage: migrate.sh [container]   (default hyperbeam-prod)
# Prints "MIGRATE PASS ..." or "MIGRATE FAIL ..."; exit 0 only on PASS.
CT=${1:-hyperbeam-prod}
HB=/app/_build/rocksdb+genesis_wasm/rel/hb/bin/hb
OUT=$(docker exec "$CT" $HB eval '
begin
  W = hb:wallet(list_to_binary(os:getenv("HB_KEY"))),
  SID = hb_util:human_id(ar_wallet:to_address(W)),
  NodeOpts = hb_http_server:get_opts(#{ <<"http-server">> => SID }),
  [Main | _] = hb_opts:get(<<"store">>, [], NodeOpts),
  hb_store_lmdb = maps:get(<<"store-module">>, Main),
  [Ess] = hb_store_essentials:store(NodeOpts),
  Inner = maps:get(<<"inner">>, Ess, Ess),
  Src = NodeOpts#{ <<"store">> => [Main#{ <<"access">> => [<<"read">>] }] },
  Dst = NodeOpts#{ <<"store">> => [Ess] },
  {MigUs, Rep} = timer:tc(fun() -> hb_store_essentials:migrate(Src, Dst, #{ live_source => true }) end),
  #{ <<"db">> := EssDB } = hb_store:find(Inner),
  ok = elmdb:flush(EssDB),
  {VerUs, V} = timer:tc(fun() ->
      hb_store_essentials:verify(NodeOpts#{ <<"store">> => [Main] },
                                 NodeOpts#{ <<"store">> => [Inner] }) end),
  Summary = #{ assignment_slots => maps:get(assignment_slots, Rep, x),
               processes => maps:get(processes, Rep, x),
               misses => maps:get(misses, Rep, x),
               namespace_rows => maps:get(namespace_rows, Rep, x),
               migrate_ms => MigUs div 1000, verify_ms => VerUs div 1000 },
  case V of
      {ok, Counts} -> {pass, Counts, Summary};
      {error, Bad} -> {fail, lists:sublist(Bad, 20), Summary}
  end
end.' 2>&1)
case "$OUT" in
  "{pass,"*) echo "MIGRATE PASS $OUT"; exit 0 ;;
  *) echo "MIGRATE FAIL $OUT"; exit 1 ;;
esac

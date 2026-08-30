#!/usr/bin/env bash
# HyperBEAM storage maintenance: logs store sizes hourly (for growth analysis)
# and prunes the disposable gateway cache when it crosses a threshold.
# The gw-cache LMDB is capped (map_size) in node-config.json as the hard
# backstop; this prune keeps actual usage well below the cap so MDB_MAP_FULL
# never triggers, and refreshes stale cache. Only the DISPOSABLE gateway cache
# is ever touched — the authoritative primary LMDB is never pruned.
set -uo pipefail

HB=/root/HyperBEAM
REL=$HB/_build/rocksdb+genesis_wasm/rel/hb
BIN=$REL/bin/hb
GW=$REL/cache-mainnet/gw-cache
PRIMARY=$REL/cache-mainnet/lmdb
LOG=$HB/logs/storage-monitor.log

PRUNE_GIB=70          # reset gw-cache when it reaches this many GiB
DISK_MAX_PCT=88       # or when the root disk crosses this percent

ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
gw_kb=$(du -sk "$GW" 2>/dev/null | cut -f1); gw_kb=${gw_kb:-0}
pr_kb=$(du -sk "$PRIMARY" 2>/dev/null | cut -f1); pr_kb=${pr_kb:-0}
disk_pct=$(df --output=pcent / | tail -1 | tr -dc '0-9'); disk_pct=${disk_pct:-0}
gw_gib=$(( gw_kb / 1024 / 1024 ))
pr_gib=$(( pr_kb / 1024 / 1024 ))

pruned=no
if [ "$gw_gib" -ge "$PRUNE_GIB" ] || [ "$disk_pct" -ge "$DISK_MAX_PCT" ]; then
  "$BIN" eval 'GW = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => <<"cache-mainnet/gw-cache">>, <<"capacity">> => 128849018880 }, catch hb_store:reset(GW), ok.' >/dev/null 2>&1 || true
  pruned=yes
  logger -t hyperbeam-maintain "pruned gw-cache (was ${gw_gib}GiB, disk ${disk_pct}%)"
fi

echo "$ts primary_gib=$pr_gib gw_cache_gib=$gw_gib disk_pct=$disk_pct pruned=$pruned" >> "$LOG"

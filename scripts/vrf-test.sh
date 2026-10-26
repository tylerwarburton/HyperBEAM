#!/usr/bin/env bash
# Build this tree in a container (erlang:27, with the blst NIF) and run the
# `vrf@1.0' tests and the `lua@5.3a' tests next to them. Uncommitted changes
# are included. An argument replaces the default `--devices' list, e.g.
#   scripts/vrf-test.sh dev_vrf
set -euo pipefail
cd "$(dirname "$0")/.."
DEVICES="${1:-dev_vrf,dev_lua}"
SUBMODULES="native/lib/secp256k1 native/lib/blst"
git submodule update --init $SUBMODULES
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
# Snapshot the working tree through a copy of the index, so ignored files
# stay out, then append each submodule. Archives are written with LF line
# endings whatever the host's `core.autocrlf'.
cp "$(git rev-parse --git-path index)" "$tmp/index"
tree="$(GIT_INDEX_FILE="$tmp/index" sh -c 'git add -A && git write-tree')"
git -c core.autocrlf=false archive --format=tar -o "$tmp/src.tar" "$tree"
for sm in $SUBMODULES; do
    git -C "$sm" -c core.autocrlf=false archive --format=tar \
        --prefix="$sm/" -o "$tmp/sm.tar" HEAD
    tar -Af "$tmp/src.tar" "$tmp/sm.tar"
done
cp scripts/vrf-test.Dockerfile "$tmp/Dockerfile"
docker build -t hb-vrf-test "$tmp"
docker run --rm hb-vrf-test rebar3 device test --devices "$DEVICES"

# Redeploy pause sent toward the production venue (2026-11-24)

## What ran

The e2e for `integ/wasm-fast` was run against a local node (`http://localhost:10100`) from
a paralith-mono worktree of release/next (`/c/REPO/_wt/paralith-mono/e2e-rn`, ef63f76c):

```
NODE_URL=http://localhost:10100 HB_WALLET=.e2e-local/owner-wallet.json \
DAO_SPAWNER_KEY=.e2e-local/owner-wallet.json \
node backend/native/redeploy.mjs --no-market --skip-checks --skip-load-gate --allow-dirty
```

`HB_WALLET` was a throwaway key generated for the test; the real owner key was not used.

## What it tried

The worktree still held release/next's committed `backend/native/deployment-state.json`,
which records the production graph (node `https://hyperbeam.paralithmarkets.com`,
`externalVenue` `sV1s0tAnZej1sr3EDefCTbKhO1Eiqp9n5PUhNiPZP1Q`). `redeploy.mjs` reads that
file as the prior deployment. A blank deploy on `lua-patch@1.0` without `--rebaseline`
treats the prior venues as superseded. At stage 9/9 it sends each one an `Admin.Pause`
(Scope `all`) before it records the new graph (`guardContractMigration`, redeploy.mjs
lines 1350-1420).

The message was sent through the local node with the throwaway key. `hbclient.sendMessage`
follows redirects, and the local node is not that process's scheduler. So it may have been
forwarded to the production scheduler. The local node logs at the configured verbosity
cannot say whether it was. The deploy then waited in that stage for about 10 minutes,
until I killed it.

## Why it had no effect

- The venue's `Admin.Pause` handler starts with `requireOwner` (`venue.lua` 2073). A
  message signed by the throwaway key is refused even if it was scheduled.
- Read-only check of the live venue afterwards:
  `GET https://hyperbeam.paralithmarkets.com/sV1s0tAn…~process@1.0/now/paused` gave `0`.

Nothing else in the run touched a live node. Every spawn and every wiring message went to
the local node.

## What changed

- The deploy was rerun after deleting `deployment-state.json` from that worktree and
  adding `--rebaseline`. Every process was spawned and wired on the local node only, and
  e2e passed 63/63.
- The hazard remains in the tooling. `redeploy.mjs` run in any checkout that holds
  production state files will try to pause the production venues at a blank cutover,
  whatever `NODE_URL` says. It should only pause prior venues that live on the target
  node, or require an explicit flag to do so.

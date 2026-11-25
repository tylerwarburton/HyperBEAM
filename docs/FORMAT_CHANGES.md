# Changing a stored or wire format

A node must be able to roll back one release without losing data or failing to start.

**The rule:** a reader for a new format ships one release before anything writes that format.

1. **Release N** reads the old format and the new one. Writes stay on the old one.
2. **Release N+1** starts writing the new format. Rolling back to N still works, because N reads it.
3. **Release N+2 or later** can remove the old writer. The old reader stays for as long as any stored row may still hold the old format.

A reader that meets an encoding it does not know refuses it with a clear error (for a process: 422 on init) and does not crash. An unknown format must never become a `case_clause` inside a compute.

Formats this covers:
- Rows in the process cache (`pstate/...`: delta, full, snapshot markers, chunks).
- VM snapshot bodies (`memory-encoding`).
- Journal frames.
- Anything another node or an older build reads back: assignments, pushes, published keys.

## Why

2026-11-25: Paralith was rolled from 0675b5977 back to 1de95ca67. Every wasm-patch compute then failed with `case_clause: separate`:
- 8251cbbb1 writes VM snapshots with `memory-encoding => separate`, the memory stored next to the body.
- 1de95ca67 only knows `raw` and `zlib`.

Rolling forward again recovered it, and nothing was lost. `fix/snapshot-reader-compat` puts the `separate` reader on 1de95ca67 without the writer, so that line is a safe rollback target.

## Snapshot memory encodings

| encoding | written by | read by |
|---|---|---|
| `zlib` (no key) | every build before df9322fde; the in-slot path since | all |
| `raw` | df9322fde .. 8251cbbb1 (deferred, async checkpoints) | df9322fde and later |
| `separate` | 8251cbbb1 and later (deferred) | 8251cbbb1, `fix/snapshot-reader-compat`, and later |

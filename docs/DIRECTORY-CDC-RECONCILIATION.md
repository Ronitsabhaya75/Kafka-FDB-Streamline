# Directory CDC reconciliation (design only)

**Status: proposal, not implemented.** The shipped contract is a startup snapshot:
[`CDC-CONSUMER-SPEC.md`](CDC-CONSUMER-SPEC.md) §2.5–2.6 and
[`BRIDGE-PIPELINE-SPEC.md`](BRIDGE-PIPELINE-SPEC.md) "Directory mode". This document
covers how a directory stream could follow tree changes later. Nothing here (watchers,
rescans, stream replacement, cursor migration) exists in code.

## 1. Native constraints (verified at FoundationDB 8.0.0)

- **A stream's range set is immutable.** Re-registering the same name with a different
  union is rejected, so the only way to change ranges is a remove followed by a
  re-register (`api-c.rst` "CDC", `design/cdc.md` "Registration and removal semantics").
- **Re-registration mints a new `stream_id`.** Removal "relinquishes its unread
  history", so every persisted cursor for the old id becomes unusable (CDC-DEEP-DIVE §1).
- **All ranges share one cursor and one acknowledgement.** There is no per-range
  progress, so a new range cannot join an existing cursor position.
- **A stream captures mutations committed after its registration commit version `R`.**
  Earlier data in a newly watched directory is not in the stream.

Consequence: reconciliation is a **migration to a replacement stream**, never an
in-place edit.

## 2. What can change, and what it means

| Change | Logical paths | Physical prefixes | Stream must change? |
|--------|---------------|-------------------|---------------------|
| Child added | new path | new prefix | Yes: a new range |
| Child removed | path gone | prefix freed (`remove` clears its contents, which the old stream sees as a `CLEAR_RANGE`) | Optional: the dead range is harmless but may be reused by the allocator |
| Rename / move within the tree | path changes | **unchanged** (`move` rewrites metadata only) | No: ranges are identical; only the fingerprint's path mapping changed |
| Move out of / into the tree | path leaves / enters | unchanged prefix, leaves / enters scope | Yes |
| Remove + recreate at the same path | same path | **new** prefix | Yes; the old prefix may later be reallocated to an unrelated directory |
| Root replaced | root path same | new root prefix | Yes |

The v1 fingerprint hashes both paths and boundaries, so it cannot tell a rename from a
range change. Generation 2 should record the two separately, a **range digest** (raw
boundaries only) and a **path digest** (the path mapping), so that:

- a path-only change (same range digest) can be approved without touching the stream:
  the cursor stays valid and only the mapping consumers use changes;
- a range change always requires migration.

## 3. Detection

Options, in order of preference:

1. **A metadata CDC stream** over the Directory Layer node subspace (`\xfe…` for the
   default layer). Every create, remove or move commits there, with a commit version
   that orders it against data. This needs care: node keys for unrelated trees share
   the subspace, so the stream is a trigger to re-resolve, not a diff.
2. **Periodic re-resolution** in a separate read transaction, comparing range and path
   digests. Simple, but it has a detection lag and no version ordering against data.

Either way detection only proposes a plan; nothing changes the running stream on its own.

## 4. Migration to a replacement stream

Let the old stream be `S1` (cursor at `C1`). The plan is computed from a fresh
resolution `T2`.

1. **Plan.** Persist a migration record (states below) with `T2`'s fingerprint, the
   replacement name `S2` (a new name, e.g. with a generation suffix), and the diff.
   Require operator approval.
2. **Register `S2`** over `T2`'s ranges and read back its registration version `R`.
   `S1` keeps running and both retain history, so TLog retention cost roughly doubles
   for the shared ranges until cutover.
3. **Choose a handoff version `H ≥ R`.** Drain `S1` until its delivered version is
   `≥ H`, then stop acknowledging `S1` past `H`.
4. **Backfill** newly included directories with a snapshot read at version `H`
   (`set_read_version(H)`), published before `S2`'s first post-`H` version. The
   snapshot must finish within the transaction time limit, or be chunked with a
   documented consistency weakening. This is an open decision.
5. **Start `S2`** by resuming at cursor `(S2.id, H)`. Its retention began at `R ≤ H`,
   so `H` is still readable.
6. **Cut over** the checkpoint atomically to `{stream: S2, version: H, fingerprint: T2}`.
7. **Retire `S1`** only after the operator confirms `S2`; removal is terminal.

**Ordering and duplicates.** Kafka order is preserved per key if `S1` (≤ `H`) and `S2`
(> `H`) publish to the same topic and key in that order. Cross-stream dedup by
`(stream_name, fdb_version, sequence_no)` does **not** work: `sequence_no` is a position
within a version group *as filtered by that stream's ranges*, so the same mutation can
have different sequence numbers in `S1` and `S2`. The handoff must therefore be exact at
a version boundary. A crash between steps 3 and 6 replays whole versions, at-least-once
as today.

**Gaps.** A directory added after `R` and before cutover is not in `S2`; detection must
re-plan. A directory whose data predates `R` needs the step-4 backfill, or it is silently
incomplete.

## 5. Crash recovery and rollback

The migration record moves through `PLANNED → S2_REGISTERED(R) → HANDOFF_CHOSEN(H) →
BACKFILLED → CUTOVER → S1_REMOVED`, each step idempotent and written with the same
durable write sequence as the cursor. On restart the bridge resumes the recorded step.
Until `CUTOVER`, rollback means continuing `S1` and removing `S2`. After `CUTOVER` and
before `S1_REMOVED`, rollback is possible only while `S1` still retains versions `> H`,
because `S1` stopped acknowledging at `H`. After `S1_REMOVED` there is no rollback.

## 6. Directory partitions

Still rejected. Supporting one means resolving through the partition's own Directory
Layer, watching its children's prefixes, and never the partition's whole range, which
holds node metadata at `prefix + b"\xfe"`. Children of a partition share the partition's
prefix, so the overlap rules in CDC-CONSUMER-SPEC §2.6 still hold.

## 7. Open decisions

- Generation-2 checkpoint format (separate range and path digests, migration record
  location: the cursor file or a sibling).
- Replacement stream naming, and who owns removing `S1`.
- Backfill consistency for trees too large for one 5-second snapshot.
- Detection: a metadata stream versus a rescan cadence, and their retention cost.
- Whether path-only changes may be auto-approved.
- Behaviour when the allocator reuses a removed directory's prefix for an unrelated
  directory during a migration window.

## 8. Tests required before implementing

- Live: add, remove, move and recreate directories during a running stream; assert the
  plan diff for each.
- Live: full migration with writes concurrent to every step, asserting no gap and no
  duplicate across the `H` boundary per `(key, version)`.
- Crash injection after each migration state, then resume.
- Backfill at `H` matches a reference scan.
- Rollback before and after cutover.
- Path-only rename keeps the stream id and cursor and changes only the path digest.

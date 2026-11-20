# Active/passive pool failover

Contract for the lease library (`fdb_kafka_bridge.cdc.lease`), the pod supervisor, and
`Bridge` take/give-up. [Vocabulary](#vocabulary) defines the terms.
Related: [FDB checkpoints](FDB-CHECKPOINT-SPEC.md), [bridge state machine](BRIDGE-PIPELINE-SPEC.md),
[producer](KAFKA-PRODUCER-SPEC.md), [CDC listener](CDC-CONSUMER-SPEC.md).
Trello: [FoundationDB Commit Version Checkpointing](https://trello.com/c/QmqdoJy5).
Issue: #26.

## Scope

A pool of N ≥ 2 identical pods serves M configured streams. Each stream has at most one
leader, the only pod allowed to consume it. Every other pod watches the stream's lease
and claims it when the lease goes stale or is released. The target is p99 ≤ 2 s from
leader death to first consume on the new leader. See [Takeover target](#takeover-target).
The per-pod stream limit is provisional until the Streams-per-pod benchmark runs; see
[Unverified and provisional](#unverified-and-provisional).

Failover builds on the checkpoint backend and leaves these unchanged:

- the cursor key and value, sink identity and the save guard table
  ([storage and identity](FDB-CHECKPOINT-SPEC.md#storage-and-identity));
- the stream, range and retention checks
  ([startup guards](FDB-CHECKPOINT-SPEC.md#startup-guards)), which run after the claim.
  Registration runs at pod start, described in [Pool runtime](#pool-runtime);
- the poll → produce → flush → save → ack order and at-least-once delivery with
  reader-side version watermarks ([delivery boundary](FDB-CHECKPOINT-SPEC.md#delivery-boundary)).

It replaces these parts of the checkpoint spec:

| Checkpoint spec | Replaced by |
|---|---|
| Owner record v1 and claim-once-per-process ([storage and identity](FDB-CHECKPOINT-SPEC.md#storage-and-identity)) | [Lease state](#lease-state), [Lease transactions](#lease-transactions) |
| Claim before validation and failed-starter eviction ([startup guards](FDB-CHECKPOINT-SPEC.md#startup-guards)) | Claim, then check. A failed check blocks the stream ([Taking and giving up a stream](#taking-and-giving-up-a-stream)) |
| `replicas: 1` / `Recreate`, "no leases" ([scope and verification](FDB-CHECKPOINT-SPEC.md#scope-and-verification)) | [Deployment, exit codes and alerts](#deployment-exit-codes-and-alerts) |
| `--transactional-id` ([CLI](FDB-CHECKPOINT-SPEC.md#cli)) | `--transactional-id-prefix` ([Kafka fencing](#kafka-fencing)) |
| Exit 3 for stream-level refusals ([CLI](FDB-CHECKPOINT-SPEC.md#cli)) | Block the stream. Exit 3 only for pod-level checks ([Error mapping](#error-mapping)) |
| "Start one bridge process" ([operator recovery](FDB-CHECKPOINT-SPEC.md#operator-recovery-checkpoint-reset)) | Stop every pod of the pool before an offline reset |

## Vocabulary

| Term | Meaning | Avoid |
|---|---|---|
| Leader | The only pod allowed to consume a given stream. | owner (for the pod), master, primary |
| Lease | A stream's owner record plus its heartbeat: who leads the stream and whether that pod is alive. | lock |
| Owner record | The durable claim on a stream: epoch, the current tenure's owner ID (none when released), and the holder's pod name. | owner key, claim record |
| Owner ID | Random identity of one tenure. A pod that claims the same stream again gets a new one. | token, instance token |
| Epoch | Per-stream counter raised by every claim. Never decreases, including across release. | |
| Heartbeat | Per-stream counter the leader bumps to show it is alive. Watchers judge liveness by whether it changes. | hb (in prose), keepalive |
| Stale | A lease whose owner record and heartbeat a watcher has seen unchanged for the stale period. A stale lease may be claimed. | expired, dead |
| Tenure | One continuous period of a pod leading a stream, from claim to release or drop. | |
| Claim | Becoming leader of a stream whose owner record is released or whose lease is stale. | acquire, steal |
| Renew | The leader bumping its heartbeat while confirming its owner record is unchanged. | |
| Release | The leader giving up a stream whose tenure is still provably its own (shutdown, shed, runtime failure). Clears the owner ID, keeps the epoch. | |
| Drop | The leader ceasing to consume without writing anything, because it can no longer prove its tenure: fenced, lease lost, or renew deadline missed. | |
| Blocked stream | A stream no pod consumes until an operator fixes it: failed stream check, unreadable owner record, prefix collision, or foreign consumer at the proxy. Its leader keeps the lease and rechecks. An unreadable owner record is blocked by every pod without claiming. | |
| Prefix collision | Two deployments on one Kafka cluster using the same transactional ID prefix, fencing each other's producers for the same stream name. Detected when a leader is fenced in Kafka while its owner record still names its own tenure. | |
| Pod heartbeat | Per-process counter every running pod bumps, leading or not. The pool counts live pods by it. | membership key, pod lease |
| Instance ID | Random identity of one pod process, keying its pod heartbeat. Pod names are display-only and may be reused. | pod ID |
| Claim delay | Wait a pod adds before claiming, longer the more streams it already leads, so lightly loaded pods win. | |
| Shed | A release made only to rebalance: a pod leading more than its share gives up one stream. | evict, rebalance (for one stream) |

## Lease state

Lease keys live in `CheckpointStore`'s existing `subspace`, beside each stream's cursor
key. `cdc/lease.py` owns them and runs its own transaction loops, described in
[Lease transactions](#lease-transactions). Stream keys lead with the raw stream name as
bytes. `lease.py` and the store reject a non-bytes `stream_name`, which keeps stream keys
disjoint from the text-led pod keys.

| Key | Value | Written by |
|---|---|---|
| `(stream_name, "owner")` | tuple-packed `(2, epoch, owner_id, pod_name)` | claim, release |
| `(stream_name, "hb")` | 8-byte little-endian counter | renew, via `tr.add` |
| pod heartbeat, pod name | see [Pool runtime](#pool-runtime) | renew; name once at pod start |

- **Owner record v2.** `owner_id` is 16 random bytes per claim attempt, or `None` after a
  release. `pod_name` is display-only and never compared. Each claim raises the epoch by
  one. Release keeps it, and it never decreases.
- **Heartbeat.** A missing key reads as 0, and nothing clears it. Watchers compare
  `(owner record, heartbeat)` on their own monotonic clocks. The store holds no wall-clock
  time.
- **v1 records.** A single-process `(1, epoch, random ID)` record reads as held with no
  heartbeat. It goes stale after the stale period, and the claim rewrites it as v2.
  A v1 bridge never renews, so a pool pod would claim its stream while it still consumes.
  The move from v1 to the pool therefore needs one `Recreate` rollout; see
  [Deployment, exit codes and alerts](#deployment-exit-codes-and-alerts). A v1-only binary
  that reads v2 raises `CheckpointFormatError`, so an old pod cannot steal a stream
  mid-rollout.
- **Unreadable owner record**, from an unknown version or corruption. Every pod blocks the
  stream locally, never claims it, and logs `stream_blocked`. Pods keep serving their other
  streams.
- **Removed streams** keep their records. Cleanup is operator tooling.

A successful claim returns an immutable `Lease(stream_name, epoch, owner_id)`.
`CheckpointStore.save()`, `load()` and `verify_owner()` take it explicitly. There is no
per-process `claim()` or `_owners` cache. The fence compares `(epoch, owner_id)`
and nothing else. The save transaction neither reads nor bumps the heartbeat. `load()` is
always fenced, and there is no unfenced peek.

## Lease transactions

Renew, watch and claim run at `priority_system_immediate`. Release runs at default
priority: a throttled release degrades to a drop, and the lease goes stale 1 s later.
Pod-heartbeat GC runs at `priority_batch`, since it is idempotent and has no deadline.

**Claim.** A non-snapshot read of the owner record and heartbeat. The watcher proceeds
only if `owner_id` is `None` (released) or `(owner record, heartbeat)` still equals the
value it saw go stale. Otherwise the stream is not claimed. The claim writes
`(2, epoch + 1, new owner_id, pod_name)` and leaves the heartbeat alone. A retry that
reads its own `owner_id` reports success. The claim has a 1 s total timeout and a retry
limit of 5. Any failure, `commit_unknown_result` included, means not claimed, and the pod
keeps watching. A claim that committed without the claimer knowing goes stale and is
re-claimed under a new owner ID. The non-snapshot heartbeat read makes a claim that races
a live renew conflict and abort.

A successful claim counts as the tenure's first renew. The pre-GRV start of the attempt
that committed anchors the self-drop deadline. This is safe because no watcher sees the
new owner record before that commit. Claim therefore runs its own retry loop that records
each attempt's start, rather than `run_transaction()`.

**Renew.** One transaction per pod covers all its leases plus its pod heartbeat. It takes
a snapshot read of each owner record and calls `tr.add(hb, 1)` for each one that still
names our `(epoch, owner_id)`. An owner record that names anything else (another tenure,
released or missing) reports that lease lost. Renew has no read conflict ranges, so a
claim on one stream never aborts the shared renew. The cost is that a loss may surface
one tick late, and the save fence covers that tick.

Renew runs its own transaction loop, not `run_transaction()`. It retries without limit
and retries `commit_unknown_result`, since a double bump is harmless. Each attempt's
timeout runs to the current deadline, `last_ok_start` + self-drop. On success, the
attempt's pre-GRV start becomes the new `last_ok_start`. The next tick starts after the
attempt returns. A failed renew drops nothing by itself. The deadline does.

**Release.** Conditional on the owner record still naming our `(epoch, owner_id)`. It sets
`owner_id` to `None` and keeps the epoch, so it is idempotent. It has a 1 s total timeout
and a retry limit of 5. If it fails, the leader drops instead and the lease goes stale.

**Watch.** One read-only range read over the whole store subspace, filtered to configured
streams. Cursor, owner and heartbeat keys interleave per stream, and pod heartbeats come
back in the same read. The read has a 200 ms timeout and no retries. The watcher records
the monotonic time it first saw each `(owner record, heartbeat)` value. A lease is stale
once that value stays unchanged for the stale period. A failed read keeps every anchor,
and a claim needs a later successful read that still shows the same value. The
conditional claim makes this safe.

**Threads.** Renew and watch run on two pod-level threads that block in `wait()`, on
fixed-rate monotonic schedules measured start to start. An overrun tick is skipped, not
queued. Neither uses network-thread callbacks or asyncio. The bindings release the GIL in
every future wait.

## Timings

These are module constants, not config knobs. A deployment that needs other values edits
them.

| Constant | Value |
|---|---|
| Renew period | 200 ms |
| Renew attempt timeout | until `last_ok_start` + self-drop |
| Watch period / read timeout | 100 ms / 200 ms |
| Stale | 1000 ms |
| Self-drop | 800 ms from the last successful renew's pre-GRV start |
| Stop budget (stale − self-drop) | 200 ms |
| Claim / release | 1 s total timeout, retry limit 5 |
| Claim delay step / max / jitter | 50 / 200 / 0–50 ms |
| Over-cap backstop (stale + 2 × claim-delay max) | 1400 ms |
| Blocked-stream recheck | 10 s |
| Progress deadline | 10 s |
| Re-claim cooldown | 5 s doubling to 60 s; reset after a > 60 s tenure |

The invariant is that no watcher takes over earlier than stale after the leader's last
renew start. A leader that self-drops on time therefore has the stop budget to stop
consuming. A renew attempt times out by `last_ok_start` + self-drop, before any
watcher's earliest claim at `last_ok_start` + stale, so a renew cannot commit after a
racing claim in real time; the save fence is the backstop if that ever fails. The
deadline anchor sits before the GRV, never at commit acknowledgement. A slow commit
reply would otherwise stretch the leader's belief past a watcher's takeover.
[Pool runtime](#pool-runtime) covers pod-heartbeat join and leave, shedding and the
claim-delay formula.

[Takeover target](#takeover-target) covers FDB stalls.
[Unverified and provisional](#unverified-and-provisional) covers the GIL risk.

## Taking and giving up a stream

**Pod start.** Before it watches anything, the pod opens FDB, runs the overlap guard over
all configured streams, fetches and caches the sink `cluster_id` and registers streams
(described in [Pool runtime](#pool-runtime)). If FDB open or the overlap guard fails, the
pod exits 3. The other steps never exit:

| Step | On failure |
|---|---|
| Sink `cluster_id` fetch | Retry with the pipeline backoff. The pod does not watch until it succeeds, since it cannot lead without it |
| Registration | See [Registration](#pool-runtime) |
| Spare producer warm-up | Does not delay watching. The pod watches without a spare (a slower takeover) and keeps building one in the background |

The renew and watcher threads, and with them the pod heartbeat, start only once these
steps succeed, so a pod counts in N only when it can lead. SIGTERM during pod start exits
0 with nothing to release.

**Take.** The order is claim commits → `init_transactions()` (transactional mode only) →
stream checks → first consume → run loop. The stream checks are the
[startup guards](FDB-CHECKPOINT-SPEC.md#startup-guards): existence, `stream_id`, range,
multi-range, sink and retention. Nothing is checked before the claim. The first consume
retries 2000 for a budget at least as long as the server's
`CDC_PROXY_CONSUME_POLL_TIMEOUT`. The dead leader's parked consume normally clears in
milliseconds, and cancelling it client-side never ends the proxy hold early. A 2000 after
the budget means a foreign consumer, and the stream blocks.

**Blocked stream.** The leader keeps the lease and keeps renewing, with no listener or
producer. It re-runs the stream checks every 10 s and resumes when they pass. Holding the
lease means one pod alerts and the lease does not churn between pods. If that leader
dies, the next claimer blocks in turn.

**Release or drop.** The leader releases while the tenure is provably its own: on
SIGTERM, a shed, or a runtime failure. It drops when it cannot prove that: on
`CheckpointFencedError`, on `ProducerFenced` naming another tenure, when renew reports
loss, or when the self-drop deadline passes. A drop is irrevocable. A late renew success
does not revive the tenure, and leading again needs a fresh claim.

**Self-drop gate.** The renew thread checks `now − last_ok_start > self-drop` on each tick
and before each attempt. The run loop checks the same deadline before every produce,
flush, save and ack. A renew thread blocked in `wait()` therefore cannot push the drop
past stale.

**Drop mechanics.** The drop must finish within the stop budget. Each tenure has a `lost`
event, which the renew thread or a fence error sets. The thread that sets it calls
`listener.cancel_pending()` and `producer.purge()` (public on `MutationProducer`) without
taking `_step_lock`. The run loop
checks `lost` before produce, flush, save and ack, then tears down. It does not commit the
Kafka transaction, since the next leader's `init_transactions()` aborts it. It closes the
producer with `close(timeout=0)`, skips the ack and skips the release. An in-flight save
is left to its fence. A save or ack that lands after a self-drop is harmless.

**Release order.** Stop polling and cancel the parked consume → finish the polled reply
through ack (`_finish_outstanding_commit`) → close the producer → close the
listener → release. A budget overrun, or `lost` firing part way, switches to drop. Every
release, whether for SIGTERM, a shed or a runtime failure, runs under the
`--shutdown-timeout-s` budget. The lease keeps renewing throughout, so a long budget is
safe. SIGTERM releases all leases in parallel under one budget, then exits 0.

**In-tenure failures.** Transient errors retry in place with the pipeline backoff. A rebuild
stays inside the tenure and never claims. It builds a new listener from the checkpoint and
a new producer, runs `init_transactions()`, and re-runs the checks. A failing rebuild step
backs off and retries. A check that fails mid-tenure blocks the stream. If no step
succeeds for 10 s while errors repeat, that is a runtime failure and the leader releases.
An empty idle reply counts as a successful step.

**Re-claim cooldown.** After its own runtime failure or drop on a stream, a pod waits 5 s
before claiming that stream again. The wait doubles up to 60 s and resets after a tenure
longer than 60 s. With no standby, the pod keeps retrying. `stream_failing` (≥ 3 failed
tenures in 5 min) is the operator signal. Failing never blocks a stream automatically.

**Pre-warming.** The takeover target depends on it. The pod fetches the sink identity
(`cluster_id`) once at start and caches it. Each pod keeps one spare producer. At pod
start the spare fetches metadata for every configured topic, and it refreshes that
metadata in the background. This removes a ~500 ms first-commit stall seen in the
prototype. A claimer takes the spare, and the pod builds a replacement in the background.
In transactional mode the spare exists early, but `init_transactions()` runs only after
the claim commits. Pre-creating a CDC consumer does not help. Handles are client-only,
and a probing consume would take the proxy lease.

## Error mapping

How a tenure handles the conditions that are fatal to a single-process bridge:

| Condition | Action |
|---|---|
| `StartupError` (missing stream, `stream_id`, range, multi-range) | Block |
| `CheckpointSinkMismatchError`, `CheckpointStreamMismatchError`, `CheckpointFormatError`, `CheckpointRegressionError` | Block |
| `CDCInvalidCursorError`, 1007 | Block |
| Record larger than `message.max.bytes` | Block |
| 2000 past the first-consume budget | Block (`foreign_consumer`) |
| `ProducerFenced`, owner record still ours | Block (`prefix_collision`) |
| Unreadable owner record | Block on every pod, never claimed |
| `CheckpointFencedError`; `ProducerFenced`, owner record names another tenure | Drop |
| Renew reports loss; self-drop deadline passed | Drop |
| Fatal Kafka error (not fenced); `init_transactions()` failure; ack retries exhausted | Runtime failure → release |
| Progress deadline (10 s) passed | Runtime failure → release |
| `CheckpointWriteError`, 1211, retriable Kafka error | Retry within the tenure |
| `CheckpointOverlapError` | Pod exits 3 |

FDB open failure belongs to the pod, not a tenure. See
[Deployment, exit codes and alerts](#deployment-exit-codes-and-alerts).

## Kafka fencing

The lease fences checkpoint and ack. It does not fence Kafka. Transactions stay optional
and off by default. Both modes deliver at least once with duplicates, because the
checkpoint lives in FDB and the uncheckpointed reply replays either way. Readers apply the
version watermark ([delivery boundary](FDB-CHECKPOINT-SPEC.md#delivery-boundary)) in both
modes.

- **Transactions off.** A zombie normally stops within the stop budget, but records
  already queued in librdkafka may land later. That is why readers need the watermark.
  Records may repeat and arrive out of order.
- **Transactions on.** Records may repeat but stay ordered, and `read_committed` readers
  never see a partial reply.

The transactional ID is `{prefix}.{stream_name}`. It cannot use `stream_id`, which changes
on re-registration, or the epoch, which changes on every claim. Either would leave the
zombie unfenced. The config key is `kafka.transactional_id_prefix`
(`STREAMLINE_TRANSACTIONAL_ID_PREFIX`, `--transactional-id-prefix`), replacing
`kafka.transactional_id`. Transactional mode is on iff the prefix is set. It has no
default. The old `kafka.transactional_id` key is a config error naming the prefix key,
not a silent mapping, because the ID would change from `X` to `X.{stream}`.

> **The prefix must be unique per deployment per Kafka cluster. Reusing it makes
> deployments fence each other.**

The README configuration section and the `--transactional-id-prefix` help text carry the
same warning.

**Fence placement.** Claim commits in FDB → `init_transactions()` → stream checks → first
consume. The invariant is that a legitimate leader's claim commits before it fences
Kafka. A failure or timeout in `init_transactions()` is a runtime failure.

**`ProducerFenced` while leading.** The leader re-reads the owner record in a fresh
transaction. If the record names another tenure, the lease is lost and the leader drops.
If it still names our `(epoch, owner_id)`, a foreign deployment is using the same prefix.
That is a prefix collision, and the leader blocks the stream with reason
`prefix_collision`. Dropping would make the two deployments ping-pong. The placement
invariant rules out false positives.

Each tenure gets its own producer in both modes and never reuses it across tenures. See
[Pool runtime](#pool-runtime).

## Pool runtime

A pod serves M configured streams and leads a share of them. The pool wraps the
per-stream run loop (`Bridge`/`BridgePipeline`) without restructuring it. That loop keeps
the order poll → produce → flush → save → ack. In transactional mode, flush includes the
commit.

**Supervisor.** A pod-level `Supervisor` on the main thread owns the renew thread, the
watcher thread and one tenure thread per led stream. Each tenure thread runs a per-stream
`Bridge` through the whole take order in
[Taking and giving up a stream](#taking-and-giving-up-a-stream). The claim, with its 1 s
total timeout, runs on the tenure thread so it never holds up the 100 ms watch tick. Only
the supervisor decides whether to spawn a tenure thread. It applies the re-claim cooldown,
the shed hold-off, the caps, the claim delay and the backstop described below. There is no
asyncio. If staging shows GIL starvation, renew moves to a separate process. See
[Unverified and provisional](#unverified-and-provisional).

**Producers.** Each tenure gets one `MutationProducer` in both producer modes. The tenure
closes it at the end and never hands it on. Sharing a producer is ruled out even without
transactions. `purge()` and `flush_or_raise()` act on the whole producer, so one stream's
drop would purge or block other streams' in-flight records. The spare producer and the
cached sink identity are under pre-warming in
[Taking and giving up a stream](#taking-and-giving-up-a-stream).

**Mode.** The pool is the only runtime. There is no unleased single-process path. A
single pod is a pool with N = 1.

**Configuration.** A `streams:` list of `{name, topic}` entries sits beside the shared
`fdb`, `kafka` and pipeline sections. An entry holds only fields that differ per stream,
plus an optional registration range (`begin_key`/`end_key` or `subspace_prefix`). The
existing `cdc.stream_name` and `kafka.topic` keys still parse as M = 1 and log a
deprecation warning. Setting both those keys and `streams:` is a config error.
`streams:` comes only from the config file; the CLI flags stay single-stream. Every pod in
a deployment must carry the same list. Nothing detects drift.

`cdc.checkpoint_path` is required, because leases live in the checkpoint store.
`cdc.cursor_file` is a config error that names the migration, and running without a
checkpoint is no longer possible.

**Registration.** A pod registers missing streams only at pod start, never on the takeover
path. A freshly registered stream's first consume waits for the next commit, about 3 s in
the prototype, which would blow the takeover target. The pod lists streams and registers
each missing one that has a configured range. A missing stream without a range is left
alone and blocks after its claim.

- **Races.** FDB serializes registration, and a same-name, same-range register returns
  the existing `stream_id` without committing. Pods racing with the same `streams:` list
  therefore all succeed with one ID, and no coordination is needed. A live devcontainer
  test registers the same name and range twice and expects the same ID, pinning this on
  the build we run.
- **Failures.** Transient FDB errors retry with the pipeline backoff before watching. A
  non-retryable failure logs `stream_register_failed` and the pod carries on. The stream
  is still missing or mismatched after its claim, so it blocks while other streams run.
  The fix is to correct the config and restart.
- **Range conflict.** A same-name register with a different range fails with
  `client_invalid_operation` (2000). `FDBClient.register_cdc_stream` translates it to
  `CDCRangeConflictError`, which keeps it apart from the foreign-consumer 2000 at consume.
  It is non-retryable, and the post-claim range check blocks the stream.

A stream found missing after a claim blocks, as in [Error mapping](#error-mapping).

### Pod heartbeat and live N

N is the number of live pods. The pool observes it and never configures it. Each pod
process draws a random 16-byte instance ID at start, because pod names are display-only
and StatefulSets reuse them. It writes two keys in the checkpoint store's subspace:

| Key | Value |
|---|---|
| `subspace.pack(("pod", instance_id, "hb"))` | 8-byte little-endian counter, bumped with `tr.add` |
| `subspace.pack(("pod", instance_id, "name"))` | UTF-8 `pod_name`, display-only |

The leading element is the text string `"pod"`, and stream keys lead with the raw stream
name as bytes. The tuple layer gives them different type codes (`0x02` and `0x01`). No
stream name can collide with a pod key, and every pod key sorts after all stream keys. The
watcher's single range read over the subspace returns them without a second read. The
counter and the name live in separate keys because `tr.add` needs a bare little-endian
value.

- The pod writes the `name` key once at start. The renew transaction bumps the heartbeat
  on every tick, even when the pod leads nothing.
- A pod counts itself in N.
- A pod leaves N when its heartbeat stays unchanged for the stale period (1000 ms). It
  joins N after 10 s of observed changes. Join is slow on purpose, so a restarting or
  rolling pod does not trigger shedding the moment it appears.
- On SIGTERM, a pod clears its own pod keys after its releases.
- Every 60 s, any pod may run a blind-clear GC transaction. It clears both keys of every
  pod whose heartbeat has been stale for more than 5 min. The transaction is idempotent,
  so concurrent GCs from several pods do no harm.

### Caps, claim delay and backstop

A stream is claimable once its lease is released or stale (see
[Lease transactions](#lease-transactions)) and it is outside this pod's cooldown and shed
hold-off. `led` is the number of streams this pod leads, blocked streams included.

| Condition | Claim? |
|---|---|
| `led < ceil(M/N)` | Yes, after the claim delay |
| `ceil(M/N) ≤ led < ceil(M/(N−1))` and claimable longer than claim-delay max | Yes |
| `led ≥ ceil(M/(N−1))` and claimable longer than the backstop | Yes |
| Otherwise | No |

With N = 1, `ceil(M/(N−1))` is unbounded, so the third row never applies.
The second row lets survivors absorb a dead pod's streams before N has dropped. The
backstop (stale + 2 × claim-delay max = 1400 ms) means a wrong N can never leave a stream
unled. The claim delay is `min(led × step, max) + jitter`, with step 50 ms, max 200 ms
and jitter 0–50 ms. Lightly loaded pods wait less and so win the race. A pod may start
claims up to its cap in one watch tick, each on its own tenure thread. The backstop never
fired in the prototype's M = 6 / 3-pod mass takeover.

### Shedding

A pod that leads more than `ceil(M/N)` streams sheds. It releases its shortest-tenure
non-blocked stream, at most one every 5 s. The shed stream gets a 10 s hold-off on that
pod so the shedder does not claim it straight back. The hold-off is separate from the
failure cooldown and never doubles. Blocked streams are never shed. They count toward the
cap, and their leader keeps them until an operator fixes them.

### Thread failure

An exception that escapes a tenure thread is a runtime failure for that stream. The
leader releases, then the cooldown starts. An exception that escapes the renew, watcher
or supervisor thread ends the pod. It logs `pod_thread_died` naming the thread and exits 1
without attempting any release. Its leases go stale and the survivors claim them.

## Deployment, exit codes and alerts

This replaces the single-replica guidance in
[FDB checkpoints: scope and verification](FDB-CHECKPOINT-SPEC.md#scope-and-verification).
[FDB checkpoints: CLI](FDB-CHECKPOINT-SPEC.md#cli) defines the existing flags, exit
statuses and shutdown deadline. This section lists only what changes.

**Exit codes.** The values 0/1/2/3 are unchanged. Exit 3 covers only the pod-level checks,
FDB open and the overlap guard over all configured streams, which run once at pod start
before watching. Stream-level refusals that exit 3 on a single-stream process block the
stream instead; see [Error mapping](#error-mapping). A dying renew, watcher or supervisor thread
exits 1 with no release.

**Shutdown.** On SIGTERM the pod releases every led stream in parallel under the existing
`--shutdown-timeout-s` budget (default 20 s). It then clears its pod keys and exits 0. If
the budget runs out, the pod drops the remaining leases, which go stale, and still exits 0.
Set `terminationGracePeriodSeconds` to at least the shutdown timeout plus 5 s. The
Kubernetes default of 30 s fits.

**Rollout.**

| Setting | Value |
|---|---|
| Strategy | `RollingUpdate`, `maxSurge: 1`, `maxUnavailable: 0` |
| Replicas | ≥ 2 |
| `minReadySeconds` | 10, matching the pod-heartbeat join, so a new pod counts in N before an old one terminates |
| Node anti-affinity | Soft (preferred) |
| Probes | None |
| systemd / Compose | ≥ 2 instances on different hosts |

**Migrating from v1.** A single-process deployment moves to the pool with one `Recreate`
rollout, so no v1 bridge is still consuming when pool pods start. Later rollouts use the
settings above.

There are no probes because process exit is the liveness signal. A dead pod-level thread
exits the pod, and a hung tenure hits the 10 s progress deadline and releases. Full
manifests belong to the "Reference Deployment Configs (k8s, Compose, systemd)" card.

**Alerts.** Structured log events with stable names and fields are the contract. Alerting
reads logs. There is no metrics exporter.

| Severity | Signal |
|---|---|
| Page | `stream_blocked`, any reason, including `prefix_collision` and `foreign_consumer` |
| Page | `stream_failing`: ≥ 3 failed tenures of a stream in 5 min |
| Page | `pod_thread_died`; exit 3; crash loop |
| Page | `stream_unowned` for > 10 s. Every watcher logs it while a lease stays stale or released and nobody claims it |
| Warn | Drops and self-drops by reason above a pool-wide rate, e.g. > 3 per 10 min |
| Warn | Max renew lateness > 600 ms (self-drop − renew) |
| Info | Takeover phases `claim_start`, `claim_commit`, `checks_done`, `first_consume`. This spec fixes these names |

The drop-rate warning covers failovers that FDB stalls cause by design; see
[Takeover target](#takeover-target). Alerting on the stall itself is FDB monitoring's job.

## Takeover target

Takeover runs from leader death to the new leader's first consume, for one stream, with
FDB and Kafka steady. FDB recovery does not count.

| Mode | Target |
|---|---|
| Non-transactional | p99 ≤ 2 s |
| Transactional | p99 ≤ 2.5 s |

The transactional bar is looser because the dead holder's open Kafka transaction adds
about 510 ms to the new leader's `init_transactions()`. The measured phases sum to about
1.8 s at the chosen timings.

The devcontainer prototype (single-node FDB 8.0.0, one broker, amd64 emulation) ran 120
SIGKILLs per timing set, non-transactional. Times in ms:

| Set | Renew / poll / stale / self-drop | p50 | p99 | max |
|---|---|---|---|---|
| P0 | 250 / 200 / 1500 / 1250 | 1656 | 2272 | 3055 |
| **P1 (chosen)** | **200 / 100 / 1000 / 800** | **1059** | **1262** | **1357** |
| P2 | 150 / 100 / 800 / 650 | 860 | 1056 | 1655 |
| P3 | 100 / 50 / 600 / 500 | 662 | 764 | 772 |

Detection is nearly all of the takeover time. Claim delay, claim, checks, resume and first
consume together take about 80 ms at p50 and 150 ms at p99. No
`client_invalid_operation` (2000) occurred in 555 takeovers. A mass takeover (M = 6,
3 pods, P1) gave p99 1456 ms. P1 beat P2 and P3 because their self-drop periods (650 and
500 ms) fall inside the ~700 ms FDB stalls the prototype saw. P3 self-dropped live
leaders on those stalls.

**FDB stalls.** Under deliberate overload, with commit stalls of 1 to 2.7 s, every timing
set self-dropped live leaders: P0 14 to 160 per hour, P1 100 to 370 per hour. Moderate
load produced none. A stall longer than self-drop causes a failover at any timings. We
accept that. The cost is about 1.1 s of pause plus one replayed reply, mostly while
nothing could flow anyway.

## Tests and acceptance

**Layering.**

| Layer | Covers |
|---|---|
| Unit, fake clock + fake lease store (`tests/cdc/`, `tests/bridge/`) | All timing and decision logic: renew-to-deadline timeout, failed-watch anchors, run-loop self-drop gate and 200 ms stop budget, cooldown, progress deadline, caps, claim delay, backstop, shedding, pod-heartbeat join/leave, and the full block/drop/release/retry mapping. Spreading runs as a discrete simulation of N pods over the fake store. |
| Live devcontainer FDB | Transaction semantics only: conditional claim races, `tr.add` heartbeat, conflict-free snapshot renew, release, watch range read, same-range re-registration returning the same `stream_id`. |
| Devcontainer multi-process (CI) | Kill/term correctness, not latency. |
| Benchmark (manual) | Takeover p99. |
| Staging | GIL and self-drop rate. |

Renew-to-deadline and the stop budget are unit-only, because live FDB cannot be stalled
deterministically. Neither is verified live.

**Fake honesty.** One parametrized conformance suite of claim, renew, release and watch
outcomes, races included, runs against both the in-memory fake and live FDB
(`FDB_CDC_INTEGRATION=1`). If the fake drifts from FDB, this suite fails.

**Clock.** An injectable `Clock` (`monotonic()`, `sleep()`, event wait) runs through
`lease.py`, the renew and watcher threads and the run loop. Tests do not monkeypatch time
and do not run short real-time timings.

**Multi-process CI cases.** Subprocess pods against real FDB and Kafka, in a new
`test-failover` job.

| Case | Pass |
|---|---|
| SIGKILL leader | Standby takes over; no missing version groups in Kafka; ≤ 1 duplicated reply per takeover |
| SIGTERM leader | Release; standby claims before stale (< 1000 ms); 0 duplicates |
| SIGSTOP zombie, SIGCONT after takeover | Its save raises `CheckpointFencedError`; it never acks; it drops |
| Blocked stream | Leader keeps the lease; nobody else claims |
| Transactional zombie | `ProducerFenced` → drop |

CI asserts only that takeover completes within 5 s. Latency belongs to the benchmark.

**Takeover benchmark.** Manual, with results committed like the 0.1.0 benchmarks. It runs
120 SIGKILLs on one stream against the targets in [Takeover target](#takeover-target),
plus the M = 6 / 3-pod mass takeover, and reports the phase breakdown. The driver,
statistics, tally and writer scripts live in `scripts/takeover_bench/` and drive real CLI
processes. Phase timestamps come from the
info-level takeover events.

**Coverage.** `--cov-branch --cov-fail-under=100` on `cdc/lease.py`. Each pragma carries
a one-line reason. If 100 % would need non-deterministic tests, drop to 90 % and ask
rather than accept a flaky test. Runtime and supervisor coverage is reported, not gated.
Lease unit and conformance tests join the existing `test-bridge` job.

**Staging soak.** 24 h with real payloads at expected peak. The runtime logs renew
lateness (start-to-start jitter and max) and drop and self-drop counts by reason. The
soak passes with 0 self-drops at moderate load and max renew lateness under 600 ms. If it
fails, renew moves to a separate process.

## Unverified and provisional

- **Per-pod stream limit (provisional).** There is no supported max M per pod yet. The
  Streams-per-pod benchmark must measure per-tenure producer throughput and memory (one
  producer per tenure plus the spare) and renew and watch cost as M grows. It must
  produce a supported max M per pod and a sizing rule for N, recorded here. It reuses
  `scripts/takeover_bench/`.
- **Renew to deadline.** A renew attempt may run until the self-drop deadline. Only unit
  tests cover this.
- **Environments.** All measurements come from single-node FDB with one plaintext broker.
  Nothing covers multi-node clusters, TLS/SASL Kafka, death during a Kafka transaction
  commit, or correlated commit-path stalls in production.
- **GIL.** CDC waits release the GIL, and a same-process renew thread kept up in probes.
  Long GIL holds still stall renew and the FDB network thread alike. Sources include
  large reply decode, CPU-bound produce and C extensions that keep the GIL. The model
  gives 15 to 58 self-drops per hour on an idle cluster for 50 ms holds at 50 % duty. This
  spec sets no hard limit. The staging soak decides, and renew in a separate process is
  the fallback.

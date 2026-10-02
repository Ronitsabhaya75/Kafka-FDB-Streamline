# Checkpoint Spec

> **Draft.** Only §4 (Failover) is written. The other sections land when the
> design is finalized.

Contract for durably checkpointing the CDC cursor in FoundationDB and for the run-loop
ordering that makes restart safe.

Trello: [FoundationDB Commit Version Checkpointing](https://trello.com/c/QmqdoJy5).
Terms: [CDC consumer spec §1](CDC-CONSUMER-SPEC.md#1-scope--rules).
Related: [Daemon Entrypoint & CLI Configuration Runner](https://trello.com/c/5t718fA3),
Wire FDB Listener into Kafka Mutation Producer.

## 1. Guarantees

_Pending._ At-least-once delivery. The checkpoint lives in FDB, and consumers drop
duplicates by `(stream, commit version)`.

## 2. Storage

_Pending._ Covers the directory layer path, key and value encoding, and the overlap guard.

## 3. Write semantics

_Pending._ Covers the read-guard-set transaction, the no-op on an equal version, and
the `CheckpointError` hierarchy.

## 4. Failover

One daemon instance per stream, restarted by its platform supervisor (Docker, k8s,
systemd). Failover is a restart. There is no standby. The supervisor sees only the exit
status; everything else is decided by startup resolution reading FDB.

### 4.1 Restart cycle

```mermaid
flowchart LR
    SUP(["Supervisor starts daemon"]) --> START["Startup resolution<br/>(§4.2, read-only checks)"]
    START -- "check fails" --> X3(["exit 3"])
    START -- "cursor or fresh consumer" --> LOOP["Run loop<br/>(§4.3)"]
    LOOP -- "SIGTERM, reply finished" --> X0(["exit 0"])
    LOOP -- "flush / checkpoint / ack failed" --> X1(["exit 1"])
    X1 --> BACK["Supervisor restarts<br/>with backoff"]
    X3 --> BACK
    BACK --> SUP

    classDef fatal fill:#fde2e2,stroke:#c0392b,color:#000
    classDef ok fill:#e2f5e9,stroke:#27ae60,color:#000
    class X1,X3 fatal
    class X0 ok
```

### 4.2 Startup resolution

Every exit below is code 3.

```mermaid
flowchart TD
    OPEN["Open FDB client and CheckpointStore<br/>list_cdc_streams()"] --> OVL{"Checkpoint key or \xfe metadata<br/>inside a registered range?"}
    OVL -- yes --> E1(["exit 3"])
    OVL -- no --> HAS{"Checkpoint exists?"}

    HAS -- yes --> CEX{"Stream exists?"}
    CEX -- no --> E2(["exit 3"])
    CEX -- yes --> SID{"stream_id matches<br/>checkpoint?"}
    SID -- no --> E3(["exit 3"])
    SID -- yes --> RNG2{"Range matches prefix<br/>if one was given?"}
    RNG2 -- no --> E4(["exit 3"])
    RNG2 -- yes --> RES["resume_cdc_consumer(cursor)<br/>wait read version ≥ cursor, re-ack"]

    HAS -- no --> SEX{"Stream exists?"}
    SEX -- "no, no prefix" --> E5(["exit 3"])
    SEX -- "no, --subspace-prefix set" --> REG["register_cdc_stream"]
    SEX -- yes --> RNG1{"Range matches prefix<br/>if one was given?"}
    RNG1 -- no --> E6(["exit 3"])
    RNG1 -- yes --> WARN["WARN: no checkpoint, log min_version"]
    REG --> FRESH["create_cdc_consumer"]
    WARN --> FRESH

    RES --> LOOP(["Run loop"])
    FRESH --> LOOP

    classDef fatal fill:#fde2e2,stroke:#c0392b,color:#000
    class E1,E2,E3,E4,E5,E6 fatal
```

### 4.3 Run loop

```mermaid
flowchart TD
    STOP{"Stop requested?"} -- yes --> X0(["exit 0"])
    STOP -- no --> POLL["poll_records()"]
    POLL -- empty --> SLEEP["sleep poll_interval"] --> STOP
    POLL -- records --> PROD["produce"]
    PROD --> FLUSH{"flush until 0 remaining<br/>≤ flush_timeout_s"}
    FLUSH -- "timeout or KafkaException" --> X1a(["exit 1: no checkpoint, no ack"])
    FLUSH -- ok --> CKPT{"save(get_position())"}
    CKPT -- CheckpointError --> X1b(["exit 1: no ack"])
    CKPT -- ok --> ACK{"acknowledge()"}
    ACK -- CDCRetryableError --> RETRY["backoff, retry ≤ ~30 s"] --> ACK
    ACK -- "terminal or retries exhausted" --> X1c(["exit 1: checkpoint already covers reply"])
    ACK -- ok --> STOP

    classDef fatal fill:#fde2e2,stroke:#c0392b,color:#000
    classDef ok fill:#e2f5e9,stroke:#27ae60,color:#000
    class X1a,X1b,X1c fatal
    class X0 ok
```

Every startup check runs before any registration or write. A crash loop on exit 3 is
therefore read-only and leaves no side effects. SIGTERM sets the stop flag, which is
checked only before a poll. A reply that has already been polled runs through ack
before exit 0.

### 4.4 Crash points

What a restart sees when the process dies (crash, SIGKILL, or exit 1) at each step for
reply `R`:

| Dies during / after | Stored checkpoint | Restart resumes from | Effect for `R` |
|---|---|---|---|
| poll or produce | before `R` | before `R` | Re-polled and re-produced. Partial sends are duplicated. |
| flush | before `R` | before `R` | Delivered records are duplicated. |
| checkpoint commit (`commit_unknown_result`) | before or after `R` | whichever committed | Duplicated if it didn't commit, nothing if it did. |
| after checkpoint, before or during ack | after `R` | after `R` | None. Resume re-acks the cursor. |
| after ack | after `R` | after `R` | None. |

No row loses data. The worst case is redelivering one reply's records.

### 4.5 Exit codes

| Code | Meaning | Supervisor |
|---|---|---|
| 0 | Clean shutdown (SIGINT/SIGTERM) | Stays down if stopped intentionally |
| 1 | Runtime failure: flush, checkpoint, or ack | Restart with backoff |
| 2 | Config or usage error | systemd: no restart. Docker/k8s: crash loop |
| 3 | Operator action needed: stream missing, `stream_id` or range mismatch, overlap | systemd: no restart. Docker/k8s: crash loop, alert on the code |

### 4.6 Deployment requirements

- **Kubernetes:** `replicas: 1` and `strategy: Recreate`, or a StatefulSet. Rolling
  updates briefly run two daemons, and both would produce.
  `terminationGracePeriodSeconds ≥ 60` covers the flush deadline plus the ack retry.
- **Docker Compose:** `restart: unless-stopped`, one service, no `scale`.
- **systemd:** `Restart=on-failure`, `RestartPreventExitStatus=2 3`.

The write semantics' backwards-move guard is the only protection against two
concurrent writers. It does not stop duplicate production. Active/passive fencing is
not yet specified.

## 5. Store API

_Pending._ Covers `CheckpointStore` in `src/cdc/checkpoint.py` and `resolve_startup()`
in `src/bridge/`.

## 6. Tests

_Pending._

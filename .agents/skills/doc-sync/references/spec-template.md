# <Component> spec

Contract for `src/fdb_kafka_bridge/<pkg>/`: <one sentence on what the package does and guarantees>.

<!-- Include the next two lines only when known; ask the user rather than invent them. -->
Trello: [<card title>](<card url>).
Related: [`<research doc>`](<research doc>) (research; this spec is normative for this repo's behaviour).

**In scope:** <...>
**Out of scope:** <...>

## 1. Rules

- **<Rule name>:** <must/never statement in plain prose>.

## 2. Public API

### 2.1 <Area> (`src/fdb_kafka_bridge/<pkg>/<module>.py`)

`<signature>`: <behaviour, preconditions the caller must meet, what it guarantees>.

## 3. Errors

<a id="err-<pkg>-<name>"></a>

| Error | Meaning | Retry? |
|-------|---------|--------|
| `<ErrorClass>` | <...> | <yes/no> |

This section is the single source for these errors' handling; other specs link here.
Errors raised by other components link to their owner's anchor instead of restating it.

## 4. Testing

- **No infrastructure:** <what runs under `make test`>.
- **Opt-in:** <env var and what it enables>.

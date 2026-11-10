---
name: doc-sync
description: Sync normative docs with the current branch's diff. Maps each changed path to its owning doc via docs/INDEX.md, plans spec updates or a new spec, verifies doc claims touched by the diff against the implementation (including behaviour the docs omit or misstate), and flags code/spec contradictions for the user to decide. Writes .md files only. Run before opening a PR.
disable-model-invocation: true
argument-hint: "[base-ref, default main]"
---

# Sync specs with the branch diff

Hard rules:

- Write only `.md` files. Never edit code, tests, config or generated output.
- The diff sets the scope. Edit only doc text the diff touches or falsifies, but verify each touched claim in full: the whole sentence, table row or rule it sits in, against the code it describes.
- Docs, not code review: report a code problem only when it contradicts a normative doc, or when the doc omits or misstates what the code does.
- Never resolve a blocking contradiction yourself (step 5). Report it and ask the user.
- No invariant IDs, `# Pins` comments or "Enforced by" tracing. Rules are plain must/never prose.

## 1. Collect the diff

Base is `$ARGUMENTS`, else `main`. Diff from the merge-base with `origin/<base>`.

```sh
base=main  # or the ref passed as the argument
git fetch origin "$base" --quiet
git rev-list --count "HEAD..origin/$base"   # > 0: branch is behind
git diff --name-status "origin/$base...HEAD"
git diff "origin/$base...HEAD"
git log --oneline "origin/$base..HEAD"
```

If the branch is behind `origin/<base>`, say by how many commits and recommend pulling or rebasing first; continue unless the user stops you. If the fetch fails, say so and use the local `origin/<base>` ref. Make sure you read the complete diff; if your shell truncates output, read it file by file.

Ignore `protobuf/gen/`, lockfiles and formatting-only hunks. Cite lines on the new side (`file:line` at HEAD).

## 2. Map changes to owning docs

In scope for edits: the normative docs in `docs/INDEX.md`. Research and historical docs are checked but edited only as described below.

- `src/fdb_kafka_bridge/<pkg>/` and `tests/<pkg>/` → the "Component specs" row for that package. Top-level `tests/*.py` → the spec of the package they import.
- `protobuf/proto/` and `protobuf/tests/` → `SERIALIZATION-SPEC.md`.
- `demo/` → `demo/README.md`.
- `.github/workflows/`, `Makefile`, `requirements-dev.txt`, `.pre-commit-config.yaml`, `pyproject.toml`, `Dockerfile`, `.devcontainer/` → `docs/conventions.md` (CI table, local setup, code style).
- New subpackage, or a new or removed top-level path (incl. `scripts/`, `.agents/`, `.claude/`, `.cursor/`) → the `docs/conventions.md` layout table.
- `.agents/skills/*`, `AGENTS.md` → themselves; check their `docs/INDEX.md` row.
- Research doc edited by the diff → verify its changed claims (step 4) and its status banner. Research doc not in the diff → edit only an *As built* note or banner the diff falsifies; otherwise leave it.

Anything still unmapped is skipped; list it. If a mapping is a guess, say so.

## 3. Classify each change

- **Feature:** new module or subpackage, `__all__` entry, public kwarg, `BridgeConfig` key, or error class.
- **Fix / change:** bodies, constants or docstrings change; signatures don't.
- **Removal:** deleted public symbol, config key, error or behaviour.
- **Doc edit:** the diff changes a doc. On a docs-only diff every change is a doc edit; step 4's verification is then the whole job, not a formality.
- **No-op:** private-only refactor that changes no behaviour any spec describes.

A change in any class that falsifies existing doc text is also a contradiction (step 5).

## 4. Plan the doc edits and verify claims

For each owning doc, list planned edits as ADDED / MODIFIED / REMOVED, each tied to the diff hunk that justifies it.

- Owning spec exists: modify it. Keep untouched sections verbatim and match its line width.
- New subpackage: create a spec from [`references/spec-template.md`](references/spec-template.md), named `docs/<COMPONENT>-SPEC.md` after what the package does (cf. `CDC-CONSUMER-SPEC.md`, `KAFKA-PRODUCER-SPEC.md`); propose the name and let the user confirm. Add a `docs/INDEX.md` row and update the `conventions.md` layout table. The new spec owns the package's own contract; the spec of the package that wires it in owns the wiring and links to the new spec.
- Changed scope of an existing doc: update its INDEX row.
- Removal: find text, in every in-scope doc, that still claims the removed behaviour and plan its removal.
- Never invent a rule. A new rule must be demonstrated by code, a test or a code comment at HEAD (changed or not), or, for third-party behaviour, by a check under "External facts" below. If intent is unclear, ask.

### Verify against the implementation

For every doc claim the diff adds, changes or falsifies, read the implementation, not only its docstring or the diff hunk. Docstrings and the doc may agree and both be wrong. Check:

- **What a value means:** for returned positions, versions, cursors and counters, which event it reflects (polled/consumed vs produced vs delivered vs checkpointed vs acknowledged). A doc that lets a caller persist a position past what is durable is a data-loss hazard: blocking.
- **Every path:** each raise site and its error class, each early return, each mode or config branch. A rule stated unconditionally must hold in all of them; "always", "never", "only" and "every" need every path checked.
- **Order:** call, iteration and read order the doc states or implies, incl. in loops and retries.
- **Precision:** counts, bounds, units, defaults, inclusive vs exclusive ranges.
- **Omissions:** behaviour of the touched API the doc leaves out that a caller must know to use it safely (failure states that stick, state that a retry depends on, cleanup that can mask an error).
- **Deviations sections:** a *Deviations* or known-limitations entry records a departure; it does not approve or exempt one. Verify it describes the code correctly, and verify that the rule it departs from still exists.

### External facts

A claim about a third-party library, broker or FDB itself (e.g. librdkafka config aliasing, wire overhead) is not unverifiable just because repo code cannot show it. In order:

1. Cite a primary source: the library's docs or source at the pinned version, or a research doc in `docs/` that cites one.
2. Check it empirically: a short script in a scratch directory outside the repo, against installed packages. Report the command and output.
3. Neither possible: report the claim as **unverified (external)** with what you tried; non-blocking unless the claim carries a delivery or durability guarantee.

## 5. Check for contradictions

Check the diff against every in-scope doc, not only the owner:

- Changed constants and defaults: grep old and new values across `docs/`, including the old literal when it moved behind a named constant.
- Must/never statements anywhere in a spec vs the changed behaviour.
- Each new raise site: the error is covered by the raising function's `Raises:`, its callers' docstrings, and the owning spec's error table.
- Ordering in `bridge/pipeline.py`: poll → produce → flush/commit → checkpoint → ack.
- Error handling restated outside the raising component's spec: replace with a link to its `err-*` anchor (single owner per error). Anchors sit per row in the error table's first cell (see `CDC-CONSUMER-SPEC.md` §2.4). If the owning spec lacks the anchor, plan adding it there.
- Changed tests asserting behaviour a spec forbids. Deleted tests: report a spec rule left with no test (don't edit).
- Code comments that point to a doc section that doesn't exist.
- A docstring that newly documents existing behaviour the spec contradicts: likely a stale spec.

For each contradiction give the doc line, the code line, a verdict and why:

- **code** (doc is stale) and the change is deliberate (tested or explained in the diff): include the doc edit in the plan, marked as a flagged contradiction.
- **code** but not shown to be deliberate: treat as **unclear**.
- **spec** (code is wrong) or **unclear**: blocking. Do not edit that text; the user decides. If any part of a statement is unclear (e.g. deliberate in one mode, not another), the whole statement is blocking.
- **docstring** (spec matches the code, a code docstring or comment is stale): report with the code line; never edit it (`.md` only).
- **external**: per "External facts" above; verified claims are not contradictions.
- Accurately recorded in a *Deviations* or known-limitations section: report as known, no verdict. An inaccurate entry is an ordinary contradiction.

Never quietly rewrite a doc to match code that may be buggy.

## 6. Get approval, apply, report

Present the plan and contradictions together, numbered, and wait for approval; the user may approve all or individual items. If the plan is empty, say so and stop. After approval, write the approved edits and report:

- Docs changed or created, one-line reason each.
- Contradictions and their resolution, or "awaiting decision".
- Docstring and external findings, with evidence.
- Skipped paths and anything not checked, with the reason. Not checked is not a pass.

Doc prose follows the AGENTS.md comment rules: no history narration ("moved from X"), no caller-location notes. No AI attribution anywhere.

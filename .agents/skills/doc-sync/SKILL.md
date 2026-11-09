---
name: doc-sync
description: Sync normative docs with the current branch's diff. Maps each changed path to its owning spec via docs/INDEX.md, plans spec updates or a new spec, checks spec edits in the diff against code, and flags code/spec contradictions for the user to decide. Diff-scoped; writes .md files only. Run before opening a PR.
disable-model-invocation: true
argument-hint: "[base-ref, default main]"
---

# Sync specs with the branch diff

Hard rules:

- Write only `.md` files. Never edit code, tests, config or generated output.
- Work only from the diff. Spec text the diff does not touch stays as is.
- Docs, not code review: report a code problem only when it contradicts a normative doc.
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

In scope: the normative docs in `docs/INDEX.md`. Research and historical docs are out of scope.

- `src/fdb_kafka_bridge/<pkg>/` and `tests/<pkg>/` → the "Component specs" row for that package. Top-level `tests/*.py` → the spec of the package they import.
- New subpackage → also the `docs/conventions.md` layout table.
- `demo/` → `demo/README.md`.
- Everything else (`scripts/`, `protobuf/`, CI, packaging) has no owning spec; list it as skipped.

If a mapping is a guess, say so.

## 3. Classify each change

- **Feature:** new module or subpackage, `__all__` entry, public kwarg, `BridgeConfig` key, or error class.
- **Fix / change:** bodies, constants or docstrings change; signatures don't.
- **Removal:** deleted public symbol, config key, error or behaviour.
- **Spec edit:** the diff itself changes a normative doc.
- **No-op:** private-only refactor that changes no behaviour any spec describes.

A change in any class that falsifies existing spec text is also a contradiction (step 5).

## 4. Plan the doc edits

For each owning doc, list planned edits as ADDED / MODIFIED / REMOVED, each tied to the diff hunk that justifies it.

- Owning spec exists: modify it. Keep untouched sections verbatim and match its line width.
- New subpackage: create `docs/<UPPER-PKG>-SPEC.md` from [`references/spec-template.md`](references/spec-template.md), add a `docs/INDEX.md` row, and update the `conventions.md` layout table. The new spec owns the package's own contract; the spec of the package that wires it in owns the wiring and links to the new spec.
- Changed scope of an existing spec: update its INDEX row.
- Removal: find text, in every in-scope doc, that still claims the removed behaviour and plan its removal.
- Spec edit in the diff: verify every added or changed claim against the code at HEAD. A wrong claim is a contradiction (step 5).
- Verify each claim you write against the code, not only its docstring.
- A new rule must be demonstrated by code, a test, or a code comment at HEAD (changed or not). Never invent one. If intent is unclear, ask.

## 5. Check for contradictions

Check the diff against every in-scope doc, not only the owner:

- Changed constants and defaults: grep old and new values across `docs/`, including the old literal when it moved behind a named constant.
- Must/never statements anywhere in a spec vs the changed behaviour.
- Each new raise site: the error is covered by the raising function's `Raises:`, its callers' docstrings, and the owning spec's error section.
- Ordering in `bridge/pipeline.py`: poll → produce → flush/commit → checkpoint → ack.
- Error handling restated outside the raising component's spec: replace with a link to its `err-*` anchor (single owner per error).
- Changed tests asserting behaviour a spec forbids. Deleted tests: report a spec rule left with no test (don't edit).
- Code comments that point to a spec section that doesn't exist.
- A docstring that newly documents existing behaviour the spec contradicts: likely a stale spec.

For each contradiction give the spec line, the code line, a verdict and why:

- **code** (spec is stale) and the change is deliberate — tested or explained in the diff: include the spec edit in the plan, marked as a flagged contradiction.
- **code** but not shown to be deliberate: treat as **unclear**.
- **spec** (code is wrong) or **unclear**: blocking. Do not edit that text; the user decides. If any part of a statement is unclear (e.g. deliberate in one mode, not another), the whole statement is blocking.
- Already acknowledged in a spec (e.g. a "Deviations" or known-limitations section): report as known, no verdict.

Never quietly rewrite a spec to match code that may be buggy.

## 6. Get approval, apply, report

Present the plan and contradictions together, numbered, and wait for approval; the user may approve all or individual items. If the plan is empty, say so and stop. After approval, write the approved edits and report:

- Docs changed or created, one-line reason each.
- Contradictions and their resolution, or "awaiting decision".
- Skipped paths and anything not checked, with the reason. Not checked is not a pass.

Doc prose follows the AGENTS.md comment rules: no history narration ("moved from X"), no caller-location notes. No AI attribution anywhere.

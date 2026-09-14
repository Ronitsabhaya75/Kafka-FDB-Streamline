# AGENTS.md

Guidance for coding agents in this repo. Layout, branching, code style and PR rules live in [`docs/conventions.md`](docs/conventions.md) — follow them; this file only adds agent-specific guidance. Project context: [`Proposal.md`](Proposal.md).

## Before You Finish

- Run `pre-commit run --all-files` (ruff + black). Fix lint rather than adding `# noqa`; if a suppression is truly needed, scope it to one rule on one line and say why.
- Every function signature carries type hints; every public module, class and function carries a Google-style docstring. Ruff enforces both.
- Do not commit `.env`, `.mcp.json`, `.cursor/mcp.json`, or anything holding tokens.

## Code Comments

General guidance: comments should maximize clarity. They should be clear without being verbose. They should be concise without being cryptic.

Scope: the guidance here mainly governs inline and implementation comments. Docstrings on public APIs are contract statements and must be clear, precise, and complete.

Audience: assume readers are senior engineers fluent in Python, asyncio, distributed systems, Kafka, and FoundationDB's transaction and CDC model (see [`docs/CDC-DEEP-DIVE.md`](docs/CDC-DEEP-DIVE.md)). Calibrate "non-obvious" to that reader — do not explain the language, the standard library, asyncio idioms, or established patterns they already know. Reserve comments for what such a reader could not quickly infer from the code itself.

Implementation comments should focus on important, non-obvious fundamental rationale, motivation, and explanation of extremely subtle phenomena. Writing down important invariants — and the preconditions a piece of code requires of its caller (e.g. a CDC version that must already be acknowledged, or a Kafka produce that must be flushed before the ack) — is also a good idea.

Do not write comments about mundane day-to-day development refactorings — junk like `# This used to be part of xyz in foo.py but we moved it here so bar could also call it`.

Do not write caller-location / "happening elsewhere" comments — narration of who invokes this code or where it is used ("called from X", "used by Y"). They duplicate what a search answers instantly and rot when callers change; describe what the code requires and guarantees on its own terms. Stating a real invariant is fine — "the ack path is the only writer of minVersion" — naming the callers that happen to exist today is not.

After writing or changing a bunch of code, make a pass over it one last time to review the comments. Ask yourself: am I rambling? Am I repeating what the code is showing? Am I just telling stories here? Is the information I'm writing going to be relevant and important a week from now, a month from now, a year from now? Is it genuinely essential and **non-obvious from the code itself**?

## Code Review

Unless you have specific instructions to the contrary, when asked to review code (named files or a diff), address all of these explicitly:

- What is it trying to accomplish?
- Is it correct?
- Are there bugs?
- Are there omissions?
- Are there things that could be done better?
- Should it be LGTM'd? (clear yes / no / not-yet)

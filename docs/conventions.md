# Conventions

How the team lays out the repo, branches, writes Python, and merges. Tooling enforces what it can (see [Enforcement](#enforcement)); review covers the rest.

## Repo layout

| Path | Purpose |
|------|---------|
| `src/fdb_kafka_bridge/` | Import package (distribution `fdb-kafka-bridge`): `bridge/`, `cdc/`, `kafka/`, `serialization/` |
| `protobuf/` | `proto/` schema, `gen/` committed `buf generate` output, `tests/` schema and pipeline-simulation tests |
| `tests/` | Unit and integration tests; subdirectories mirror the `src` packages, plus `demo/` |
| `demo/` | Live FDB → Kafka demo ([`demo/README.md`](../demo/README.md)) |
| `docs/` | Specs, conventions and research; map in [`INDEX.md`](INDEX.md) |
| `scripts/` | CI and dev helper scripts |
| `config/` | Placeholder (`.gitkeep`); nothing reads it yet |
| `.github/` | CI workflows, PR template, issue templates |
| `.devcontainer/` | Dev container + `docker-compose.yml` (FDB + Kafka stack) |
| `.agents/`, `.cursor/` | Agent skills and Cursor rules |
| `Dockerfile` | FDB + native CDC runtime image; `FDB_VERSION` is the canonical FDB version |
| `buf.yaml`, `buf.gen.yaml` | buf module, lint/breaking config, codegen → `protobuf/gen` |
| `pyproject.toml`, `requirements-dev.txt`, `.pre-commit-config.yaml`, `Makefile` | Project metadata and tool config, pinned dev tools, hooks, dev commands |

- Empty directories hold a `.gitkeep` until real files land.
- Generated code (`buf generate` → `protobuf/gen/`) is committed but excluded from ruff and black. It ships in the wheel as the separate top-level package `fdbkafka`. Changing a `.proto` means committing the regenerated output; CI fails on drift.
- Running from a checkout needs the repo root, `src` and `protobuf/gen` on the import path (`PYTHONPATH=.:src:protobuf/gen`); pytest configures this itself.

## Branching

| Branch | Purpose |
|--------|---------|
| `main` | Stable. Every merge arrives by PR. |
| `<type>/<name>` | Work branches, cut from and merged back into `main`. `<name>` is short kebab-case. |

Work branch types: `feature/` (product code), `fix/`, `dev/` (tooling, CI, repo infra), `research/` (spikes, write-ups), `docs/`, `chore/` (reverts, housekeeping), `release/`. E.g. `feature/cdc-consumer`, `dev/scaffolding`.

A `develop` integration branch is planned but does not exist yet; until it does, PRs target `main`. It is `develop`, not `dev`, because git cannot hold `dev` alongside `dev/<name>` branches.

- Nothing is pushed directly to `main`; every merge goes through a PR (see [Pull requests](#pull-requests)). This is team practice: branch protection is not yet enabled (see [Enforcement](#enforcement)).
- All commits must be **signed and verified** (see [Signed commits](#signed-commits)).

## Specs and invariants

Docs are either **Normative** (component specs, this file) or **Research / Historical** (deep dives, [`Proposal.md`](../Proposal.md), schema validation). Code must match normative docs; research docs carry a status banner naming the normative doc that supersedes them. [`INDEX.md`](INDEX.md) records each doc's status.

- **Same-PR rule.** A behaviour change updates the owning spec, including its `## Invariants` section, in the same PR.
- **Single owner per error rule.** Each error class's handling rule lives in the spec of the component that raises it; other specs link to that anchor instead of restating it.
- **Invariant entries.** Component specs carry a `## Invariants` section. `<COMP>` is one of `BRIDGE`, `CDC`, `KAFKA`, `SER`. Name symbols, never line numbers. IDs are never reused or renumbered; a retired invariant stays listed as retired.

  ```markdown
  - <a id="inv-cdc-3"></a>**INV-CDC-3.** <statement>. Enforced by: `<module>.<Symbol>`. Pinned by: `tests/cdc/test_x.py::test_name`.
  ```

  Use `Pinned by: **Unpinned.**` when no test pins it yet.
- **Pinning tests cite the ID.** A test that pins an invariant carries `# Pins INV-<COMP>-N.` on the line directly above its `def`. A test must not assert behaviour a normative spec forbids.

## Code style

Python 3.12. All rules are configured in [`pyproject.toml`](../pyproject.toml).

| Rule | Tool |
|------|------|
| Formatting, line length 88 | `black` |
| pycodestyle, pyflakes, import order, pyupgrade, bugbear | `ruff` (`E`, `W`, `F`, `I`, `UP`, `B`) |
| PEP 8 naming: `snake_case` functions/variables/modules, `PascalCase` classes, `UPPER_CASE` constants | `ruff` (`N`) |
| Type hints on every parameter and return | `ruff` (`ANN`) |
| Google-style docstrings on public modules, classes and functions | `ruff` (`D`, convention `google`) |

- `Any` is allowed (the FDB bindings are untyped), but prefer a real type when one exists.
- `tests/` and `protobuf/tests/` are exempt from docstring rules; type hints still apply.
- `mypy` is optional and not enforced in CI.

Docstring shape:

```python
def encode_mutation(version: int, mutation: Mutation) -> bytes:
    """Serialize one CDC mutation into a Kafka record value.

    Args:
        version: Commit version the mutation belongs to.
        mutation: Raw `(type, param1, param2)` mutation from `consume()`.

    Returns:
        Protobuf-encoded record value.

    Raises:
        ValueError: If the mutation type is not replay-safe.
    """
```

## Local setup

```bash
make setup                    # .venv + dev tools + pre-commit hook (needs python3.12)
source .venv/bin/activate
make lint                     # ruff + black, check-only
make format                   # apply fixes
make test                     # pytest: tests/ + protobuf/tests/ (no FDB or Kafka needed)
make test-packaging           # build the wheel, install into a fresh venv, check imports (network)
```

`make setup` installs a pre-commit hook that runs ruff + black on every commit; `pre-commit run --all-files` runs it by hand. Tool versions are pinned in `requirements-dev.txt`; keep them in sync with the `rev`s in `.pre-commit-config.yaml`.

**VS Code / Cursor:** install `ms-python.black-formatter` and `charliermarsh.ruff`, pick `.venv` as the interpreter, and add to your user settings:

```json
{
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter",
    "editor.formatOnSave": true
  },
  "black-formatter.importStrategy": "fromEnvironment",
  "ruff.importStrategy": "fromEnvironment"
}
```

**Neovim:** activate `.venv` before launching `nvim` (or install `black`/`ruff` via Mason) so both are on `PATH`; each picks up `pyproject.toml` automatically. With [conform.nvim](https://github.com/stevearc/conform.nvim) and [nvim-lint](https://github.com/mfussenegger/nvim-lint):

```lua
require("conform").setup({
  formatters_by_ft = { python = { "black" } },
  format_on_save = { timeout_ms = 2000 },
})

require("lint").linters_by_ft = { python = { "ruff" } }
vim.api.nvim_create_autocmd("BufWritePost", {
  pattern = "*.py",
  callback = function() require("lint").try_lint() end,
})
```

### Trello MCP

Optional: lets Claude Code or Cursor read and update the team Trello board. Both run `bunx @delorenj/mcp-server-trello` (needs [Bun](https://bun.sh)) with `TRELLO_API_KEY` and `TRELLO_TOKEN` exported. Copy the template to its gitignored local file:

- Claude Code: `.mcp.json.example` → `.mcp.json`
- Cursor: `.cursor/mcp.json.example` → `.cursor/mcp.json`

### Signed commits

SSH signing with the key you already push with:

```bash
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
```

Then add the same public key on GitHub under Settings → SSH and GPG keys → New SSH key, with **Key type: Signing Key**. Commits show as "Verified" once both are done. Re-sign existing branch commits with `git rebase --exec 'git commit --amend --no-edit -S' <base>`.

## Pull requests

- Open from a work branch against `main`. The [PR template](../.github/pull_request_template.md) fills in automatically.
- For anything beyond a trivial fix, open a GitHub issue first and link it; large PRs with no justifying issue are closed.
- Link the Trello card in the description.
- At least 1 approval.
- All [CI](#ci) checks must pass.

## CI

[`main-ci.yml`](../.github/workflows/main-ci.yml) (`CI`) is the only workflow triggered by PRs (and by pushes to `main`). Every other workflow is a reusable `workflow_call` job nested under it (also runnable by hand via `workflow_dispatch`), and all of them wait on `signed-commits`. Check names below are as GitHub shows them.

| Check | Workflow | What it does |
|-------|----------|--------------|
| `signed-commits` | `main-ci.yml` | Fails if any PR commit is not GitHub-verified |
| `lint / lint` | [`lint.yml`](../.github/workflows/lint.yml) | `ruff check` and `black --check` at the pinned versions; check-only, never edits or pushes |
| `packaging / packaging` | [`packaging.yml`](../.github/workflows/packaging.yml) | `tests/test_packaging.py` with `PACKAGING_INTEGRATION=1`: builds the wheel and installs it into a fresh venv (no `libfdb_c`); fails if any test skips or none run |
| `buf / buf` | [`buf-ci.yaml`](../.github/workflows/buf-ci.yaml) | `buf lint`, `buf breaking` against the PR base, `buf build`, and a `buf generate` drift check on `protobuf/gen` |
| `container-setup / build-and-verify` | [`Container-setup.yml`](../.github/workflows/Container-setup.yml) | Builds the `Dockerfile` image, checks FDB binaries and bindings, waits for the cluster, runs a read/write check |
| `container-setup / test-protobuf`, `test-serialization`, `test-cdc-mutations`, `test-kafka`, `test-bridge` | `Container-setup.yml` | `pytest` per directory inside that image: `protobuf/tests`, `tests/serialization`, `tests/cdc` (live FDB), `tests/kafka` and `tests/bridge` + `tests/demo` (unit, then live Kafka integration), plus the Kafka smoke script |

There is no single `pytest` job: a new test directory needs its own CI job or it never runs in CI. `tests/test_cdc_e2e.py` is currently run by no job.

## Enforcement

| Convention | Enforced by |
|------------|-------------|
| Formatting, lint, naming, type hints, docstrings | pre-commit locally; `lint` check on PRs |
| Tests pass | `make test` locally; `container-setup` test jobs on PRs |
| Wheel builds, installs and imports | `make test-packaging` locally; `packaging` check on PRs |
| Proto lint, compatibility, committed codegen | `buf` check on PRs |
| Signed commits | `signed-commits` check on PRs |
| `main` protected: PR required, 1 approval, CI green | Intended, **not enabled**: `main` has no branch protection. A repo admin enables it under Settings → Branches; until then this is team practice |
| Branch naming, linked issue and Trello card, spec + invariants updated with behaviour changes | PR template + review |

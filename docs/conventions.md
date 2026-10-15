# Conventions

How the team lays out the repo, branches, writes Python, and merges. Tooling enforces what it can (see [Enforcement](#enforcement)); review covers the rest.

## Repo layout

```
.
├── src/
│   ├── bridge/           # Core bridge pipeline (CDC → Kafka)
│   ├── cdc/              # FDB CDC consumer wrapper
│   ├── kafka/            # Kafka producer module
│   └── serialization/    # Protobuf serialization
├── protobuf/
│   ├── proto/            # .proto schema definitions
│   ├── gen/              # buf generate output (committed)
│   └── tests/            # Schema and serialization tests
├── tests/                # Unit and integration tests
├── config/               # Configuration files
├── docs/                 # Documentation: INDEX.md (doc map), this file, research write-ups
├── scripts/              # Helper scripts
├── .devcontainer/        # devcontainer.json + docker-compose.yml (FDB + Kafka stack)
├── .github/              # PR template, CI workflows
├── Dockerfile            # FDB + CDC runtime image
├── buf.yaml              # buf module, lint and breaking-change config
├── buf.gen.yaml          # buf codegen → protobuf/gen
├── Makefile              # setup / lint / format / test
├── pyproject.toml        # Project metadata + black/ruff/pytest config
└── README.md
```

- Empty directories hold a `.gitkeep` until real files land.
- `.devcontainer/` and `Dockerfile` land with PR #2; `protobuf/` and `buf*.yaml` with PR #6.
- Generated code (`buf generate` → `protobuf/gen/`) is committed but excluded from ruff and black.

## Branching

| Branch | Purpose |
|--------|---------|
| `main` | Stable, protected. Only receives PRs from `develop` (or hotfixes). |
| `develop` | Integration branch. Work branches are cut from and merged back into it. |
| `<type>/<name>` | Work branches. `<name>` is short kebab-case. |

Work branch types: `feature/` (product code), `fix/`, `dev/` (tooling, CI, repo infra), `research/` (spikes, write-ups), `docs/`. E.g. `feature/cdc-consumer`, `dev/scaffolding`.

The integration branch is `develop`, not `dev`: git cannot hold a branch named `dev` alongside `dev/<name>` branches.

- Nothing is pushed directly to `main` or `develop`; every merge goes through a PR (see [Pull requests](#pull-requests)).
- All commits must be **signed and verified** (see [Signed commits](#signed-commits)).

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

- Open from a work branch against `develop` (against `main` until `develop` exists). The [PR template](../.github/pull_request_template.md) fills in automatically.
- Link the Trello card in the description.
- At least 1 approval required.
- CI must pass: the [`Lint`](../.github/workflows/lint.yml) workflow runs `ruff check` and `black --check` on every PR. It is check-only — it never edits files or pushes commits.
- CI must pass: the [`Tests`](../.github/workflows/tests.yml) workflow runs `pytest` on every PR.

## Enforcement

| Convention | Enforced by |
|------------|-------------|
| Formatting, lint, naming, type hints, docstrings | pre-commit locally; `Lint` workflow on PRs |
| Tests pass | `make test` locally; `Tests` workflow on PRs |
| `main`/`develop` protected, PR required, 1 approval, CI green, signed commits | GitHub branch protection — a repo admin enables it under Settings → Branches |
| Branch naming, Trello link | PR template + review |

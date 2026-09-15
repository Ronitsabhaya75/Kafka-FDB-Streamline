---
name: capstone-trello
description: >-
  Capstone board Kafka-FDB-Streamline workflows via Trello MCP. Use when Meet
  asks about sprint status, backlog, To-DO, In Progress, review, testing, Done,
  or syncing FDB-Kafka work to Trello.
---

# Capstone Trello (Kafka-FDB-Streamline)

Also follow personal skill `managing-trello` for MCP mechanics.

## Board

| Field | Value |
|-------|--------|
| Name | Kafka-FDB-Streamline |
| Short link | `PsScijLy` |
| URL | https://trello.com/b/PsScijLy/kafka-fdb-streamline |
| Board ID | `6a9dc33ce42c951c10dabe7b` |

At session start for board work: `set_active_board` with that ID (or confirm with `get_active_board_info`).

MCP namespace in this workspace: `project-0-GUI_Kafka-trello`.

## Lists (column flow)

Left → right:

| List | ID | Meaning |
|------|-----|---------|
| Sprints | `6a9dc62603c1778aff4d5bfd` | Sprint planning / meta |
| Backlog | `6a9dc33de42c951c10dabedd` | Not scheduled |
| To-DO | `6a9dc4212f11b53fe173c183` | Ready this sprint |
| In Progress | `6a9dc33de42c951c10dabede` | Active work |
| In Review | `6a9dc33de42c951c10dabedf` | PR / peer review |
| Testing | `6a9dc40ce4cb7736f18cce28` | Verification |
| Done | `6a9dc4123a979184e7016cfe` | Finished |

Default new work → **Backlog**. Ready for this week → **To-DO**. Starting implementation → **In Progress**.

## Labels (use IDs)

| Name | ID | When |
|------|-----|------|
| research | `6a9ed1d02e76515582698aea` | Docs / investigation |
| high priority | `6a9ed1e01177461fa0dc5604` | Blocking |
| Medium Priority | `6a9ed339695a7e5d252339fb` | Normal |
| Infra | `6a9ed3b7da46ca1e9d5b81aa` | Docker, CI, env |
| syncs | `6a9ed536c0b3a2b2ac817a61` | Team syncs / meetings |

## Common workflows

### Standup / status

1. `get_cards_by_list_id` on In Progress, In Review, Testing (compact fields).
2. Optionally `get_my_cards` for Meet (`meetkumarsaspara`).
3. Summarize blockers; do not move cards unless asked.

### Add a task

1. Confirm list (Backlog vs To-DO).
2. `add_card_to_list` with clear name; description = goal + acceptance notes.
3. Apply labels by ID; assign with `assign_member_to_card` only if Meet names someone.

### Advance work

`move_card` along To-DO → In Progress → In Review → Testing → Done. Comment when moving to Review/Testing with PR link or test notes (`add_comment`).

### Research / infra cards

- CDC / docs research → label `research`
- Docker, GHCR, Actions → label `Infra`
- Keep card descriptions short; link to repo paths (`FDB-CDC-Deep-Dive.md`, Actions URL) instead of pasting secrets.

## Members (assign by ID)

| Name | Member ID |
|------|-----------|
| Meetkumar Saspara | `6aa01fa9372c7560ae55c4b1` |
| Ronit Sabhaya | `6a9dc3396ca9a049113284af` |
| Robbie McLaughlin | `6a9dc65a0cac80297b0a0fe7` |
| Jay Findoliya | `6aa046bf7cf09bcdd16d7c0d` |
| John Greig | `6a9df9240557dc679d78b253` |
| david keathly | `5a7c86c11c6ccbb76d7d4c38` |
| tclinkenbeard | `6a9e1d405f5a4fa3f9200093` |

## Do not

- Create a second capstone board
- Archive Sprints / pipeline lists
- Put Trello API keys, GHCR tokens, or cluster secrets on cards
- Mass-move or mass-archive without an explicit ask

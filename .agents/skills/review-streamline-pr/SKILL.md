---
name: review-streamline-pr
description: Review a pull request in Ronitsabhaya75/Kafka-FDB-Streamline. Trace the changed behavior, check delivery and recovery guarantees when relevant, and draft a source-linked review with an LGTM decision.
---

# Review a Kafka-FDB-Streamline PR

Use the PR, its public checks, and the repository source to draft feedback the requester can paste into GitHub. Keep the review read-only. Inspect code and tests without executing the proposed changes or posting a review.

## Establish what changed

Confirm the target repository and capture the PR's base and head commit IDs. These GitHub CLI commands collect the review context, diff and inline comments.

```sh
repo=Ronitsabhaya75/Kafka-FDB-Streamline
pr=22  # Replace with the requested PR number.
gh pr view "$pr" -R "$repo" --json url,title,body,state,isDraft,baseRefOid,headRefOid,changedFiles,comments,reviews,statusCheckRollup
gh pr diff "$pr" -R "$repo"
gh api --paginate "repos/$repo/pulls/$pr/comments"
```

The GitHub web interface or REST API also works. If a diff is truncated, inspect the missing files at the captured commits. With both commits available locally, `git diff "$base...$head"` gives the PR changes from their merge base. Read affected callers and existing tests at the reviewed head. Verify leads from earlier comments against that code.

Read `AGENTS.md` and use `docs/INDEX.md` to find the contract for the changed component. Use `Proposal.md` for goals. Judge the PR against the behavior it changes and the guarantees it claims. A proposed future feature is only relevant when the current change depends on it.

## Trace one concrete case

Follow an input through the changed path to an observable result. For a failure-handling change, repeat the trace at the point where it can fail. Choose the questions that apply to the diff.

- **CDC and serialization.** Can a reader reconstruct one complete FoundationDB commit version from the records for its stream? Check native mutation order across split batches, raw key/value bytes, range clears, sequence numbers and the final `VersionEnd` count. Distinguish a reply carrying an idle watermark from a poll that delivered nothing. If serialization fails partway, can the listener still advance past undelivered mutations?
- **Kafka delivery and recovery.** Trace one reply through publication, confirmed delivery, durable cursor storage and FoundationDB acknowledgement. Enqueueing a record does not confirm broker delivery. Check delivery callbacks and flush results in idempotent mode, and the commit outcome in transactional mode. Are batches and their end marker routed to the same partition? If the process dies between steps, identify what is durable, what will replay, and whether a record can be lost or duplicated. Check whether an acknowledgement retry republishes the batch or shutdown aborts a commit whose outcome is still unknown.
- **Compatibility and tests.** For schema or cursor changes, trace an actual reader consuming older data. Compare Protobuf definitions with generated bindings and existing compatibility tests where relevant. For each requested test, name the failure it would expose and why an existing test would miss it.
- **Packaging, setup and docs.** Walk the advertised workflow as a new contributor would. For example, check whether an import works from the installed package outside the source tree, or whether a readiness check can succeed before its dependency is usable. Keep findings within the scope of that workflow.

## Draft the review

Open with a short, plain-language explanation of the problem and a concrete before-and-after example. Then walk through the solution in execution order, linking the few code blocks that explain the result. For docs or tooling, walk through the contributor's actions instead.

Answer the six questions from the repository's review instructions explicitly.

1. **What is it trying to accomplish?** State the intended behavior and scope.
2. **Is it correct?** Explain the strongest evidence for the judgment and any material uncertainty.
3. **Are there bugs?** Put actionable findings in descending order of impact. Give each a verified line, triggering condition, consequence and smallest reasonable fix. Write each finding so it can stand alone as a GitHub comment. If there are none, say so.
4. **Are there omissions?** Identify a specific uncovered failure, compatibility gap or missing instruction. Keep open questions distinct from demonstrated bugs.
5. **Are there things that could be done better?** Include an alternative only when it has a concrete benefit. Mark optional improvements as such.
6. **Should it be LGTM'd?** Give a clear yes, no or not-yet and the reason. For a blocker, say what would change the decision. On a merged PR, frame actionable findings as follow-ups.

Report the reviewed base and head, the scope of the available CI evidence, and that the review was by inspection. For a failed check, inspect its run and tested revision. Classify it as related, unrelated or inconclusive based on the failing step and its connection to the change. Flag a confirmed regression in the verdict. An unexplained red check alone does not establish one.

Use public permalinks of the form `https://github.com/OWNER/REPO/blob/SHA/path#L10-L15` with verified lines. Link new code at the head and removed code at the revision containing the old lines. Before handing back the draft, re-read both commit IDs. Refresh the review if either moved. If source or checks remain unavailable, state exactly what you could verify.

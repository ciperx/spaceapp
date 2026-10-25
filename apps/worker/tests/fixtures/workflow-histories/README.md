# Workflow history fixtures

This directory holds recorded Temporal event histories used by the replay
determinism gate (`apps/worker/tests/replay-gate.test.ts`).

- `dot-goal.json` — event history for `dotGoalWorkflow`, recorded once against
  the time-skipping test server and replayed in CI. If a workflow change makes
  the recorded history replay to a different command sequence, the gate fails
  and the change cannot land silently.

## How to re-record a fixture

The replay test only records when the fixture is missing: it checks the file
with `existsSync` and skips the record step when the file already exists, so
you have to delete the fixture to regenerate it.

```sh
# 1. Remove the fixture you want to re-record
rm apps/worker/tests/fixtures/workflow-histories/dot-goal.json

# 2. Run the worker suite; the gate records the missing fixture first and then
#    replays it, so a green run means the fresh history is replay-safe
npm test -w @spaceapp/worker
```

The test writes `{ "workflowId": ..., "historyBase64": ... }` with two-space
JSON indentation and a trailing newline. Keep that shape: the file is read back
verbatim, and a hand-edited or re-serialised history no longer proves the same
execution.

## What to commit

1. Inspect `git diff --stat apps/worker/tests/fixtures/workflow-histories/`.
   A changed history means real workflow behaviour changed — read the diff, do
   not commit it blindly.
2. Commit the regenerated fixture **together with** the workflow change that
   required it, in the same pull request, so reviewers can see the behaviour
   change next to the new history.
3. Never commit a fixture recorded with mocked activities or a shortened
   timeout: the gate must replay the same command sequence the production
   worker would produce. If a change is intentional and cannot be replayed,
   guard it with `patched()` and record a new fixture that covers the new
   branch.

## Adding a fixture for a new workflow

Copy the pattern in `replay-gate.test.ts`: start a time-skipping
`TestWorkflowEnvironment`, run the workflow against test activities, capture
`env.getWorkflowHistory(...)` / `handle.fetchHistory()`, and store the base64
history under this directory. Add the replay assertion for the new file to the
same test so the gate covers it.

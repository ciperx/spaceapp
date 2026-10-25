import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import protoPkg from "@temporalio/proto";
import fs from "node:fs";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { DeterminismViolationError } from "@temporalio/workflow";

const temporal = (protoPkg as unknown as { temporal?: typeof protoPkg }).temporal ?? protoPkg;

const MAIN = path.join(__dirname, "..", "dist", "workflows.js");
const MUTATION_BUNDLE = path.join(__dirname, "..", "dist", "memory-mutation-workflows.js");
const FIXTURE = path.join(__dirname, "fixtures", "workflow-histories", "dot-goal.json");
// Test-only bundle that exports dotGoalWorkflow with a divergent command sequence. Used as the
// negative control: the gate must reject it, otherwise a determinism break would pass silently.
const NEGATIVE_BUNDLE = path.join(__dirname, "fixtures", "replay-negative-workflows.js");
const QUEUE = "replay-gate";

let env: TestWorkflowEnvironment;
beforeAll(async () => { env = await TestWorkflowEnvironment.createTimeSkipping(); }, 240_000);
afterAll(async () => { await env?.teardown(); });

type Recorded = { workflowId: string; historyBase64: string };

let recordedStepCalls = 0;
const dotGoalActivities = {
  async dotGoalStep() {
    recordedStepCalls += 1;
    return recordedStepCalls === 1
      ? { state: "WAITING", runId: null, goalStatus: "RUNNING" }
      : { state: "DONE", runId: null, goalStatus: "COMPLETED" };
  },
  async dotGoalObserve() { return { run: "SUCCEEDED", goalStatus: "COMPLETED" }; },
};

async function replayDotGoal(recorded: Recorded) {
  const history = temporal.api.history.v1.History.decode(Buffer.from(recorded.historyBase64, "base64"));
  const results: Array<{ error?: unknown }> = [];
  for await (const r of Worker.runReplayHistories(
    { connection: env.nativeConnection, taskQueue: QUEUE, workflowsPath: MAIN },
    [{ workflowId: recorded.workflowId, history }],
  )) {
    results.push({ error: r.error });
  }
  return results;
}

// The replay gate replays a committed history, not one recorded in the same run: replaying
// a freshly recorded history would use the changed code for both sides and pass even when
// an existing execution can no longer replay.
describe("replay gate", () => {
  test("replays the committed dotGoalWorkflow history against the current bundle", async () => {
    if (!fs.existsSync(FIXTURE)) {
      // One-off recording; the file is committed and replayed from then on.
      const worker = await Worker.create({
        connection: env.nativeConnection, taskQueue: QUEUE, workflowsPath: MAIN, activities: dotGoalActivities,
      });
      await worker.runUntil(async () => {
        const h = await env.client.workflow.start("dotGoalWorkflow", {
          taskQueue: QUEUE, workflowId: "recorded-dot-goal", args: [{ goalId: "cmmgoal00000000000000001" }],
        });
        await h.result();
      });
      const history = await env.client.workflow.getHandle("recorded-dot-goal").fetchHistory();
      const historyBase64 = Buffer.from(
        temporal.api.history.v1.History.encode(history).finish(),
      ).toString("base64");
      fs.mkdirSync(path.dirname(FIXTURE), { recursive: true });
      fs.writeFileSync(FIXTURE, JSON.stringify({ workflowId: "recorded-dot-goal", historyBase64 }, null, 2) + "\n");
    }

    const recorded = JSON.parse(fs.readFileSync(FIXTURE, "utf8")) as Recorded;
    expect(recorded.historyBase64.length).toBeGreaterThan(0);
    const results = await replayDotGoal(recorded);
    expect(results).toHaveLength(1);
    expect(results[0].error).toBeUndefined();
  }, 240_000);

  // Negative control for issue #60: the gate has to catch a determinism change, not just pass.
  test("rejects the committed dotGoalWorkflow history when the bundle diverges", async () => {
    const recorded = JSON.parse(fs.readFileSync(FIXTURE, "utf8")) as Recorded;
    const results = await replayDotGoalForBundle(NEGATIVE_BUNDLE, recorded, QUEUE);
    expect(results).toHaveLength(1);
    const error = results[0].error;
    expect(error).toBeDefined();
    expect(error instanceof DeterminismViolationError || (error as Error)?.name?.includes("Determinism")).toBe(true);
  }, 240_000);

  test("memoryMutationWorkflow replays its own recorded history", async () => {
    const activities = {
      async executePersistedMemoryChangeSet(input: { changeSetId: string }) {
        return { changeSetId: input.changeSetId, applied: 1 };
      },
    };
    const worker = await Worker.create({
      connection: env.nativeConnection, taskQueue: QUEUE + "-mut", workflowsPath: MUTATION_BUNDLE, activities,
    });
    await worker.runUntil(async () => {
      const h = await env.client.workflow.start("memoryMutationWorkflow", {
        taskQueue: QUEUE + "-mut", workflowId: "replay-mutation", args: [{ changeSetId: "cmmcs000000000000000001" }],
      });
      await h.result();
    });
    const history = await env.client.workflow.getHandle("replay-mutation").fetchHistory();
    const historyBase64 = Buffer.from(
      temporal.api.history.v1.History.encode(history).finish(),
    ).toString("base64");
    const results = await replayDotGoalForBundle(MUTATION_BUNDLE, { workflowId: "replay-mutation", historyBase64 });
    expect(results[0].error).toBeUndefined();
  }, 240_000);
});

async function replayDotGoalForBundle(bundle: string, recorded: Recorded, taskQueue = QUEUE + "-mut") {
  const history = temporal.api.history.v1.History.decode(Buffer.from(recorded.historyBase64, "base64"));
  const results: Array<{ error?: unknown }> = [];
  for await (const r of Worker.runReplayHistories(
    { connection: env.nativeConnection, taskQueue, workflowsPath: bundle },
    [{ workflowId: recorded.workflowId, history }],
  )) {
    results.push({ error: r.error });
  }
  return results;
}

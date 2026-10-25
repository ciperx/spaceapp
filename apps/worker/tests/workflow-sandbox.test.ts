import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import path from "node:path";

const WORKFLOWS = path.join(__dirname, "..", "dist", "workflows.js");
const DOT_GOAL_SIGNAL = "dot-goal-wake";

let env: TestWorkflowEnvironment;

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping();
}, 240_000);

afterAll(async () => {
  await env?.teardown();
});

type StepResult = { state: string; runId: string | null; goalStatus: string };

function makeActivities(script: StepResult[], seen: { stepMs: number[] } = { stepMs: [] }) {
  let i = 0;
  const activities = {
    async dotGoalStep() {
      const next = script[Math.min(i, script.length - 1)];
      i += 1;
      return next;
    },
    async dotGoalObserve() {
      return { run: "SUCCEEDED", goalStatus: "COMPLETED" };
    },
  };
  return { activities, calls: () => i };
}

describe("dotGoalWorkflow in the real workflow sandbox", () => {
  test("waits on the 30 second timer for a WAITING step, then finishes on DONE", async () => {
    const { activities, calls } = makeActivities([
      { state: "WAITING", runId: null, goalStatus: "RUNNING" },
      { state: "DONE", runId: null, goalStatus: "COMPLETED" },
    ]);
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: "dot-goal-sandbox",
      workflowsPath: WORKFLOWS,
      activities,
    });
    const startedAt = await env.currentTimeMs();
    const result = await worker.runUntil(async () => {
      const handle = await env.client.workflow.start("dotGoalWorkflow", {
        taskQueue: "dot-goal-sandbox",
        workflowId: "dot-goal-timeout",
        args: [{ goalId: "goal-1" }],
      });
      return handle.result();
    });
    expect(result).toEqual({ goalStatus: "COMPLETED" });
    // The WAITING step must actually burn the 30 second timer.
    expect((await env.currentTimeMs()) - startedAt).toBeGreaterThanOrEqual(30_000);
    expect(calls()).toBe(2);
  }, 240_000);

  test("a wake signal ends the wait early instead of sleeping the full timeout", async () => {
    const { activities } = makeActivities([
      { state: "WAITING", runId: null, goalStatus: "RUNNING" },
      { state: "WAITING", runId: null, goalStatus: "RUNNING" },
      { state: "DONE", runId: null, goalStatus: "COMPLETED" },
    ]);
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: "dot-goal-sandbox-wake",
      workflowsPath: WORKFLOWS,
      activities,
    });
    const result = await worker.runUntil(async () => {
      const handle = await env.client.workflow.start("dotGoalWorkflow", {
        taskQueue: "dot-goal-sandbox-wake",
        workflowId: "dot-goal-wake",
        args: [{ goalId: "goal-2" }],
      });
      await env.sleep(1000);
      await handle.signal(DOT_GOAL_SIGNAL);
      return handle.result();
    });
    expect(result).toEqual({ goalStatus: "COMPLETED" });
  }, 240_000);
});

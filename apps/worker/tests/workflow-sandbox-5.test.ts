import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import path from "node:path";

const MAINT = path.join(__dirname, "..", "dist", "memory-maintenance-workflows.js");
const MUTATION_BUNDLE = path.join(__dirname, "..", "dist", "memory-mutation-workflows.js");
// From @space/contracts/src/workflows.ts — plural "mutations", otherwise the
// mutation activity is scheduled on a queue no worker polls and the test hangs.
const MAINT_QUEUE = "space-memory-maintenance";
const MUTATION_QUEUE = "space-memory-mutations";

let env: TestWorkflowEnvironment;
beforeAll(async () => { env = await TestWorkflowEnvironment.createTimeSkipping(); }, 240_000);
afterAll(async () => { await env?.teardown(); });

describe("memoryConsolidationWorkflow", () => {
  test("returns the prepared run and skips the mutation when there is no change set", async () => {
    const seen: string[] = [];
    const activities = {
      async prepareMemoryConsolidation() { seen.push("prepare"); return { changeSetId: null, run: { id: "run-7" } }; },
      async executePersistedMemoryChangeSet() { seen.push("execute"); return {}; },
      async finalizeMemoryRepair() { seen.push("finalize"); return {}; },
    };
    const worker = await Worker.create({ connection: env.nativeConnection, taskQueue: MAINT_QUEUE, workflowsPath: MAINT, activities });
    const result = await worker.runUntil(async () => {
      const h = await env.client.workflow.start("memoryConsolidationWorkflow", {
        taskQueue: MAINT_QUEUE, workflowId: "mem-consol-empty", args: [{ runId: "run-7", traceId: "tr-7" }],
      });
      return h.result();
    });
    expect(result).toEqual({ id: "run-7" });
    expect(seen).toEqual(["prepare"]);
  }, 240_000);

  test("runs the change set on the mutation queue and finalizes the repair", async () => {
    const seen: string[] = [];
    let finishMutation: () => void = () => {};
    const mutationDone = new Promise<void>((r) => { finishMutation = r; });

    const maintActivities = {
      async prepareMemoryConsolidation() { seen.push("prepare"); return { changeSetId: "cs-9", run: { id: "run-9" } }; },
      async finalizeMemoryRepair(input: { runId: string; changeSetId: string }) { seen.push("finalize:" + input.changeSetId); return { id: input.runId, repaired: true }; },
      async executePersistedMemoryChangeSet() { seen.push("execute-on-wrong-worker"); return {}; },
    };
    const mutationActivities = {
      async executePersistedMemoryChangeSet(input: { changeSetId: string }) { seen.push("execute:" + input.changeSetId); return { applied: 1, failed: 0 }; },
    };
    const maintWorker = await Worker.create({ connection: env.nativeConnection, taskQueue: MAINT_QUEUE, workflowsPath: MAINT, activities: maintActivities });
    const mutationWorker = await Worker.create({ connection: env.nativeConnection, taskQueue: MUTATION_QUEUE, workflowsPath: MUTATION_BUNDLE, activities: mutationActivities });

    const mutationJob = mutationWorker.runUntil(async () => { await mutationDone; });
    let result: unknown;
    try {
      result = await maintWorker.runUntil(async () => {
        const h = await env.client.workflow.start("memoryConsolidationWorkflow", {
          taskQueue: MAINT_QUEUE, workflowId: "mem-consol-mut", args: [{ runId: "run-9", traceId: "tr-9" }],
        });
        return h.result();
      });
    } finally {
      // Release the mutation worker even when h.result() rejects, otherwise the test
      // reports a timeout instead of the workflow failure.
      finishMutation();
      await mutationJob;
    }

    expect(result).toEqual({ id: "run-9", repaired: true });
    expect(seen).toEqual(["prepare", "execute:cs-9", "finalize:cs-9"]);
  }, 240_000);
});

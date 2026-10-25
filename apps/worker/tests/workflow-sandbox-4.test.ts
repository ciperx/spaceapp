import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import path from "node:path";

const MAINT = path.join(__dirname, "..", "dist", "memory-maintenance-workflows.js");
// Must match MEMORY_MAINTENANCE_TASK_QUEUE in src/memory-maintenance-config.ts,
// otherwise the child consolidation workflow is started on a queue no worker polls.
const QUEUE = "space-memory-maintenance";

let env: TestWorkflowEnvironment;
beforeAll(async () => { env = await TestWorkflowEnvironment.createTimeSkipping(); }, 240_000);
afterAll(async () => { await env?.teardown(); });

describe("memoryMaintenanceWorkflow", () => {
  test("continues when refresh and reconciliation fail, then runs consolidation as a child", async () => {
    const seen: string[] = [];
    const activities = {
      async refreshMemoryGraphSnapshot() { seen.push("refresh"); return { sourceHash: "h0" }; },
      async refreshAllMonthsMemoryGraphSnapshot() { seen.push("refresh-all"); throw new Error("all-months down"); },
      async reconcileStaleSpaceAgentSessions() { seen.push("reconcile"); throw new Error("reconcile down"); },
      async createScheduledMemoryAudit() { seen.push("audit"); return { id: "run-1", workflowId: "wf-consolidation-1", sourceHash: "h" }; },
      async prepareMemoryConsolidation() { seen.push("child:prepare"); return { changeSetId: null, run: { id: "run-1" } }; },
      async executePersistedMemoryChangeSet() { seen.push("child:execute"); return {}; },
      async finalizeMemoryRepair() { seen.push("child:finalize"); return {}; },
    };
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: QUEUE,
      workflowsPath: MAINT,
      activities,
    });
    const result = await worker.runUntil(async () => {
      const h = await env.client.workflow.start("memoryMaintenanceWorkflow", {
        taskQueue: QUEUE,
        workflowId: "mem-maint-1",
        args: [{ traceId: "trace-1" }],
      });
      return h.result();
    });
    expect(seen[0]).toBe("refresh");
    expect(seen).toContain("audit");
    expect(seen.at(-1)).toBe("child:prepare");
    // each failing activity is retried 3 times (maximumAttempts) before the workflow swallows it
    expect(seen.filter((s) => s === "refresh-all")).toHaveLength(3);
    expect(seen.filter((s) => s === "reconcile")).toHaveLength(3);
    expect(result).toBeTruthy();
  }, 240_000);
});

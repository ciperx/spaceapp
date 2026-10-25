import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import path from "node:path";

const dist = (f: string) => path.join(__dirname, "..", "dist", f);
const MAIN = dist("workflows.js");
const MUTATION = dist("memory-mutation-workflows.js");

let env: TestWorkflowEnvironment;
beforeAll(async () => { env = await TestWorkflowEnvironment.createTimeSkipping(); }, 240_000);
afterAll(async () => { await env?.teardown(); });

describe("memoryMutationWorkflow", () => {
  test("proxies the change-set executor and returns its result", async () => {
    const seen: unknown[] = [];
    const activities = {
      async executePersistedMemoryChangeSet(input: unknown) {
        seen.push(input);
        return { applied: 2, failed: 0 };
      },
    };
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: "mem-mutation",
      workflowsPath: MUTATION,
      activities,
    });
    const result = await worker.runUntil(async () => {
      const h = await env.client.workflow.start("memoryMutationWorkflow", {
        taskQueue: "mem-mutation",
        workflowId: "mem-mutation-1",
        args: [{ changeSetId: "cs-1", traceId: "t-1" }],
      });
      return h.result();
    });
    expect(result).toEqual({ applied: 2, failed: 0 });
    expect(seen).toHaveLength(1);
  }, 240_000);
});

describe("roomAgentActionWorkflow failure path", () => {
  test("reports the failure through failRoomAgentBackgroundAction and rethrows", async () => {
    const reported: Record<string, unknown>[] = [];
    const activities = {
      async runRoomAgentBackgroundAction() { throw new Error("bridge exploded"); },
      async failRoomAgentBackgroundAction(input: Record<string, unknown>) {
        reported.push(input);
        return null;
      },
    };
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: "room-action",
      workflowsPath: MAIN,
      activities,
    });
    const error = await worker.runUntil(async () => {
      const h = await env.client.workflow.start("roomAgentActionWorkflow", {
        taskQueue: "room-action",
        workflowId: "space-room-action:act-9",
        args: [{ bridge: { roomId: "room-1", missionId: "mis-1" }, action: {} }],
      });
      try { await h.result(); return null; } catch (e) { return e as Error; }
    });
    expect(error).toBeInstanceOf(Error);
    expect(reported).toHaveLength(1);
    expect(reported[0].roomId).toBe("room-1");
    expect(reported[0].missionId).toBe("mis-1");
    expect(reported[0].actionId).toBe("act-9");
    expect(reported[0].cancelled).toBe(false);
  }, 240_000);
});

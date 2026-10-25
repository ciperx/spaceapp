import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import path from "node:path";

const dist = (f: string) => path.join(__dirname, "..", "dist", f);
const MAIN = dist("workflows.js");
const MAINT = dist("memory-maintenance-workflows.js");

let env: TestWorkflowEnvironment;
beforeAll(async () => { env = await TestWorkflowEnvironment.createTimeSkipping(); }, 240_000);
afterAll(async () => { await env?.teardown(); });

describe("dummyTurnWorkflow", () => {
  test("records start then completion through its two activities", async () => {
    const order: string[] = [];
    const activities = {
      async recordDummyTurnStarted(i: unknown) { order.push("started:" + JSON.stringify(i)); return null; },
      async recordDummyTurnCompleted(i: unknown) { order.push("completed"); return { status: "COMPLETED", turnId: "t-1" }; },
    };
    const worker = await Worker.create({ connection: env.nativeConnection, taskQueue: "dummy-turn", workflowsPath: MAIN, activities });
    const result = await worker.runUntil(async () => {
      const h = await env.client.workflow.start("dummyTurnWorkflow", {
        taskQueue: "dummy-turn", workflowId: "dummy-1",
        args: [{ roomId: "room-1", turnId: "t-1", providerId: "opencode" }],
      });
      return h.result();
    });
    expect(result).toEqual({ status: "COMPLETED", turnId: "t-1" });
    expect(order[0]?.startsWith("started:")).toBe(true);
    expect(order[1]).toBe("completed");
  }, 240_000);
});

describe("codexAppServerTurnWorkflow routing", () => {
  test("providerId opencode goes to the opencode activity only", async () => {
    const calls: string[] = [];
    const activities = {
      async runOpenCodeAgentTurn() { calls.push("opencode"); return { status: "COMPLETED", turnId: "t-2" }; },
      async runCliAgentTurn() { calls.push("cli"); return { status: "COMPLETED", turnId: "t-2" }; },
      async runCodexAppServerTurn() { calls.push("codex"); return { status: "COMPLETED", turnId: "t-2" }; },
    };
    const worker = await Worker.create({ connection: env.nativeConnection, taskQueue: "turn-route", workflowsPath: MAIN, activities });
    const result = await worker.runUntil(async () => {
      const h = await env.client.workflow.start("codexAppServerTurnWorkflow", {
        taskQueue: "turn-route", workflowId: "turn-route-1",
        args: [{ roomId: "room-1", turnId: "t-2", providerId: "opencode" }],
      });
      return h.result();
    });
    expect(result).toEqual({ status: "COMPLETED", turnId: "t-2" });
    expect(calls).toEqual(["opencode"]);
  }, 240_000);
});


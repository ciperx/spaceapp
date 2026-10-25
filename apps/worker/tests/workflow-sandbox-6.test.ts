import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import path from "node:path";

const MAIN = path.join(__dirname, "..", "dist", "workflows.js");
const QUEUE = "room-supervisor-sandbox";

let env: TestWorkflowEnvironment;
beforeAll(async () => { env = await TestWorkflowEnvironment.createTimeSkipping(); }, 240_000);
afterAll(async () => { await env?.teardown(); });

function stubs(seen: string[]) {
  const rec = (name: string, value: unknown = null) => async () => { seen.push(name); return value; };
  return {
    checkRoomAgentSupervisorTurn: rec("check", { action: "RUN", reason: null }),
    finishRoomAgentSupervisorTurn: async (input: { status?: string } | undefined) => {
      seen.push("finish:" + String(input?.status));
      return null;
    },
    discardRoomAgentSupervisorTurn: rec("discard"),
    prepareRoomAgentBackgroundContinuation: rec("prepareContinuation", { pending: [], monitoring: [] }),
    failRoomAgentBackgroundAction: rec("failAction"),
    runCodexAppServerTurn: rec("turn", {
      status: "COMPLETED",
      turnId: "turn-1",
      roomAgentOutcome: { status: "VERIFIED", statusReason: "verified" },
    }),
    runRoomAgentBackgroundAction: rec("backgroundAction"),
    settleRoomAgentBackgroundMission: rec("settle", true),
  };
}

// The supervisor returns Promise<never> and polls with a 1 second condition, so under
// time skipping it never ends on its own: terminate() is the only way out of a test.
describe("roomAgentSupervisorWorkflow (current branch)", () => {
  test("runs a pending mission through the supervisor activities, then can be terminated", async () => {
    const seen: string[] = [];
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: QUEUE,
      workflowsPath: MAIN,
      activities: stubs(seen) as never,
    });
    await worker.runUntil(async () => {
      const h = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
        taskQueue: QUEUE,
        workflowId: "supervisor-1",
        args: [{
          roomId: "cmroom0000000000000000001",
          pending: [{ missionId: "cmmission000000000000001", turn: { roomId: "cmroom0000000000000000001", paneId: "cmpane0000000000000000001", prompt: "do the thing", traceId: "trace-0000000001" } }],
          processedCount: 0,
        }],
      });
      await env.sleep(4_000);
      await h.terminate();
    });
    // The pending mission must reach the turn and be reported as completed, not just
    // pass through the admission check.
    expect(seen).toEqual(["check", "turn", "finish:COMPLETED"]);
  }, 120_000);
});

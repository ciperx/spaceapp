import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import path from "node:path";
import { roomAgentSupervisorQueueItemSchema } from "@space/contracts";

const MAIN = path.join(__dirname, "..", "dist", "workflows.js");
const QUEUE = "room-supervisor-signals";
const ENQUEUE = "enqueueRoomAgentMission";

let env: TestWorkflowEnvironment;
beforeAll(async () => { env = await TestWorkflowEnvironment.createTimeSkipping(); }, 240_000);
afterAll(async () => { await env?.teardown(); });

// continuingRoomAgentSupervisorWorkflow registers its own handler, and it drops an item
// whose turn.agentRunId matches the active mission or anything already pending. Two
// payloads without agentRunId both read as `undefined`, so the guard swallows the signal.
function stubs(seen: string[], gate?: Promise<void>) {
  const rec = (name: string, value: unknown = null) => async () => { seen.push(name); return value; };
  return {
    markRoomAgentMissionStarted: rec("started"),
    markRoomAgentMissionContinued: rec("continued"),
    markRoomAgentMissionFinished: rec("finished"),
    checkRoomAgentSupervisorTurn: rec("check", { action: "RUN", reason: null }),
    finishRoomAgentSupervisorTurn: async (input: { status?: string } | undefined) => { seen.push("finish:" + String(input?.status)); return null; },
    discardRoomAgentSupervisorTurn: rec("discard"),
    prepareRoomAgentBackgroundContinuation: rec("prepareContinuation", { pending: [], monitoring: [] }),
    failRoomAgentBackgroundAction: rec("failAction"),
    runCodexAppServerTurn: async () => {
      seen.push("turn");
      // Hold the turn open when a gate is supplied: a duplicate only counts as arriving
      // while the first mission is in flight if the first one is still running.
      if (gate) await gate;
      return {
        status: "COMPLETED",
        turnId: "turn-1",
        roomAgentOutcome: { status: "VERIFIED", statusReason: "verified" },
      };
    },
    runRoomAgentBackgroundAction: rec("backgroundAction"),
    settleRoomAgentBackgroundMission: rec("settle", true),
  };
}

const ROOM = "cmroom0000000000000000001";

const mission = (args: { missionId: string; runId: string; roomId?: string }) =>
  roomAgentSupervisorQueueItemSchema.parse({
    missionId: args.missionId,
    turn: {
      roomId: args.roomId ?? ROOM,
      paneId: "cmpane0000000000000000001",
      prompt: "do the thing",
      traceId: "trace-0000000001",
      agentRunId: args.runId,
    },
  });

describe("roomAgentSupervisorWorkflow signals", () => {
  test("an enqueued mission runs check, turn and finish in that order", async () => {
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
        workflowId: "supervisor-signal-1",
        args: [{ roomId: ROOM, pending: [], processedCount: 0 }],
      });
      await env.sleep(1_000);
      expect(seen).toHaveLength(0);
      await h.signal(ENQUEUE, mission({ missionId: "cmmision0000000000000001", runId: "cmrun0000000000000000001" }));
      await env.sleep(8_000);
      await h.terminate();
    });
    expect(seen).toEqual(["check", "turn", "finish:COMPLETED"]);
  }, 120_000);

  test("a duplicate agentRunId arriving while the first mission is in flight is dropped", async () => {
    const seen: string[] = [];
    let releaseTurn: () => void = () => {};
    const turnGate = new Promise<void>((resolve) => {
      releaseTurn = resolve;
    });
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: QUEUE + "-dup",
      workflowsPath: MAIN,
      activities: stubs(seen, turnGate) as never,
    });
    await worker.runUntil(async () => {
      const h = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
        taskQueue: QUEUE + "-dup",
        workflowId: "supervisor-signal-dup",
        args: [{ roomId: ROOM, pending: [], processedCount: 0 }],
      });
      await env.sleep(1_000);
      // Both arrive while the first mission is still in flight: the dedupe guard compares
      // against active and pending only, so a duplicate sent after it finishes is accepted.
      await h.signal(ENQUEUE, mission({ missionId: "cmmision0000000000000003", runId: "cmrun0000000000000000003" }));
      // Wait until the first mission actually owns the turn before sending the duplicate.
      // Poll the virtual clock instead of one fixed sleep: the child turn workflow
      // needs several task cycles before the activity is dispatched.
      for (let i = 0; i < 40 && !seen.includes("turn"); i += 1) await env.sleep(500);
      expect(seen).toContain("turn");
      await h.signal(ENQUEUE, mission({ missionId: "cmmision0000000000000004", runId: "cmrun0000000000000000003" }));
      await env.sleep(2_000);
      releaseTurn();
      await env.sleep(2_000);
      await h.terminate();
    });
    expect(seen.filter((s) => s === "check")).toHaveLength(1);
  }, 120_000);

  test("a mission enqueued for another room is ignored", async () => {
    const seen: string[] = [];
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue: QUEUE + "-x",
      workflowsPath: MAIN,
      activities: stubs(seen) as never,
    });
    await worker.runUntil(async () => {
      const h = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
        taskQueue: QUEUE + "-x",
        workflowId: "supervisor-signal-2",
        args: [{ roomId: ROOM, pending: [], processedCount: 0 }],
      });
      await env.sleep(1_000);
      await h.signal(ENQUEUE, mission({ missionId: "cmmision0000000000000002", runId: "cmrun0000000000000000002", roomId: "cmroom0000000000000000002" }));
      await env.sleep(5_000);
      await h.terminate();
    });
    expect(seen).toHaveLength(0);
  }, 120_000);
});

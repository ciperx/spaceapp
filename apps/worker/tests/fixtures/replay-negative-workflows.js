// Test-only workflow bundle for the replay gate's negative control.
//
// It exports the same workflow type as the recorded fixture (`dotGoalWorkflow`) but emits a
// different first command: a timer instead of an activity. Replaying a history recorded by the
// real bundle against this one must be rejected as nondeterministic - that is the property the
// gate is supposed to guarantee (issue #60).
import { proxyActivities, sleep } from "@temporalio/workflow";

const { dotGoalStep } = proxyActivities({ startToCloseTimeout: "1 minute" });

export async function dotGoalWorkflow(input) {
  // Divergence: the real workflow schedules an activity first; this variant starts a timer.
  await sleep("1 second");
  await dotGoalStep(input.goalId);
  return { goalStatus: "COMPLETED" };
}

import { assert, it } from "@effect/vitest";

import * as SnapshotPolicy from "./SnapshotPolicy.ts";

const cfg: SnapshotPolicy.PolicyConfig = {
  debounceMs: 10_000,
  maxIntervalMs: 120_000,
  idleIntervalMs: 600_000,
};
const T0 = 1_000_000;

const history = { headAdvanced: true, dataVersionChanged: true };
const other = { headAdvanced: false, dataVersionChanged: true };

/** A home that last pushed at T0 (taking 100 ms) with nothing dirty since. */
const clean = SnapshotPolicy.pushed(SnapshotPolicy.initial(false), T0, T0 + 100, "committed");

/** The first of `times` at which `decide` pushes. */
const firstPush = (state: (now: number) => SnapshotPolicy.PolicyState, times: Array<number>) =>
  times.find((now) => SnapshotPolicy.decide(state(now), now, cfg) === "push");

it("decide", () => {
  const rows: Array<{
    readonly name: string;
    readonly state: SnapshotPolicy.PolicyState;
    readonly now: number;
    readonly expected: "push" | "wait";
  }> = [
    {
      name: "never pushed pushes at once",
      state: SnapshotPolicy.initial(true),
      now: 0,
      expected: "push",
    },
    {
      name: "a history write waits for the debounce",
      state: SnapshotPolicy.observe(clean, T0 + 40_000, history),
      now: T0 + 49_999,
      expected: "wait",
    },
    {
      name: "a history write pushes at the debounce",
      state: SnapshotPolicy.observe(clean, T0 + 40_000, history),
      now: T0 + 50_000,
      expected: "push",
    },
    {
      name: "other writes do not push at the debounce",
      state: SnapshotPolicy.observe(clean, T0 + 40_000, other),
      now: T0 + 40_000 + cfg.debounceMs,
      expected: "wait",
    },
    {
      name: "other writes push at the idle interval",
      state: SnapshotPolicy.observe(clean, T0 + 40_000, other),
      now: T0 + 40_000 + 600_000,
      expected: "push",
    },
    {
      name: "nothing dirty waits until the idle sweep",
      state: clean,
      now: T0 + 599_999,
      expected: "wait",
    },
    {
      name: "the idle sweep fires at the idle interval",
      state: clean,
      now: T0 + 600_000,
      expected: "push",
    },
    {
      name: "spacing is at least 30 s after a fast push",
      state: SnapshotPolicy.observe(clean, T0 + 1, history),
      now: T0 + SnapshotPolicy.MIN_SPACING_MS - 1,
      expected: "wait",
    },
    {
      name: "spacing is 20 × a slow push's duration",
      state: SnapshotPolicy.observe(
        SnapshotPolicy.pushed(SnapshotPolicy.initial(false), T0, T0 + 5_000, "committed"),
        T0 + 5_000,
        history,
      ),
      now: T0 + 99_999,
      expected: "wait",
    },
    {
      name: "a slow push's spacing ends at 20 × its duration",
      state: SnapshotPolicy.observe(
        SnapshotPolicy.pushed(SnapshotPolicy.initial(false), T0, T0 + 5_000, "committed"),
        T0 + 5_000,
        history,
      ),
      now: T0 + 100_000,
      expected: "push",
    },
    {
      name: "spacing holds back even a home that never pushed",
      state: SnapshotPolicy.pushed(SnapshotPolicy.initial(true), T0, T0 + 100, "failed"),
      now: T0 + 29_999,
      expected: "wait",
    },
  ];
  for (const row of rows) {
    assert.strictEqual(SnapshotPolicy.decide(row.state, row.now, cfg), row.expected, row.name);
  }

  // Writes every debounce/2 never let the debounce elapse, so maxInterval forces the push.
  const writesFrom = T0 + 40_000;
  const steps = Array.from({ length: 30 }, (_, index) => writesFrom + index * (cfg.debounceMs / 2));
  const continuous = (now: number) =>
    steps
      .filter((at) => at <= now)
      .reduce((state, at) => SnapshotPolicy.observe(state, at, history), clean);
  assert.strictEqual(firstPush(continuous, steps), writesFrom + cfg.maxIntervalMs);

  // A failed push keeps what it was meant to save; a successful one clears it.
  const dirty = SnapshotPolicy.observe(
    SnapshotPolicy.observe(clean, T0 + 40_000, other),
    T0 + 40_000,
    history,
  );
  const failed = SnapshotPolicy.pushed(dirty, T0 + 60_000, T0 + 60_100, "failed");
  assert.strictEqual(failed.historyDirtySince, T0 + 40_000);
  assert.strictEqual(failed.otherDirtySince, T0 + 40_000);
  assert.strictEqual(SnapshotPolicy.decide(failed, T0 + 90_000, cfg), "push");
  // A push that hung until its transport gave up says nothing about how long a push takes.
  const stalled = SnapshotPolicy.pushed(dirty, T0 + 60_000, T0 + 360_000, "failed");
  assert.strictEqual(SnapshotPolicy.decide(stalled, T0 + 390_000, cfg), "push");
  const saved = SnapshotPolicy.pushed(dirty, T0 + 60_000, T0 + 60_100, "unchanged");
  assert.strictEqual(saved.historyDirtySince, null);
  assert.strictEqual(saved.otherDirtySince, null);
  assert.strictEqual(SnapshotPolicy.decide(saved, T0 + 90_000, cfg), "wait");
});

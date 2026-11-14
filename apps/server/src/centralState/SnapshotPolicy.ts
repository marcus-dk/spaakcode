export interface PolicyConfig {
  readonly debounceMs: number;
  readonly maxIntervalMs: number;
  readonly idleIntervalMs: number;
}

/** Floor on push spacing; with 20 × the last push's duration it keeps snapshots near 5% of a core. */
export const MIN_SPACING_MS = 30_000;
const SPACING_PER_PUSH_DURATION = 20;

export interface PolicyState {
  /** The committed manifest has no database. */
  readonly neverPushed: boolean;
  /** First event-head advance since the last successful push. */
  readonly historyDirtySince: number | null;
  readonly lastHistoryChangeAt: number | null;
  /** data_version changed with no event-head advance. */
  readonly otherDirtySince: number | null;
  readonly lastPushStartedAt: number | null;
  readonly lastPushDurationMs: number;
}

export const initial = (neverPushed: boolean): PolicyState => ({
  neverPushed,
  historyDirtySince: null,
  lastHistoryChangeAt: null,
  otherDirtySince: null,
  lastPushStartedAt: null,
  lastPushDurationMs: 0,
});

export const observe = (
  s: PolicyState,
  nowMs: number,
  o: { readonly headAdvanced: boolean; readonly dataVersionChanged: boolean },
): PolicyState => {
  if (o.headAdvanced) {
    return { ...s, historyDirtySince: s.historyDirtySince ?? nowMs, lastHistoryChangeAt: nowMs };
  }
  if (o.dataVersionChanged) return { ...s, otherDirtySince: s.otherDirtySince ?? nowMs };
  return s;
};

export const decide = (s: PolicyState, nowMs: number, cfg: PolicyConfig): "push" | "wait" => {
  if (
    s.lastPushStartedAt !== null &&
    nowMs - s.lastPushStartedAt <
      Math.max(MIN_SPACING_MS, SPACING_PER_PUSH_DURATION * s.lastPushDurationMs)
  ) {
    return "wait";
  }
  if (s.neverPushed) return "push";
  if (
    s.historyDirtySince !== null &&
    ((s.lastHistoryChangeAt !== null && nowMs - s.lastHistoryChangeAt >= cfg.debounceMs) ||
      nowMs - s.historyDirtySince >= cfg.maxIntervalMs)
  ) {
    return "push";
  }
  if (s.otherDirtySince !== null && nowMs - s.otherDirtySince >= cfg.idleIntervalMs) return "push";
  // The files-only sweep: settings, secrets and transcripts change without touching the database.
  if (nowMs - (s.lastPushStartedAt ?? 0) >= cfg.idleIntervalMs) return "push";
  return "wait";
};

/** A successful push captured everything observed up to its start; later changes stay dirty. */
export const pushed = (
  s: PolicyState,
  startedAt: number,
  finishedAt: number,
  outcome: "committed" | "unchanged" | "failed",
): PolicyState => {
  // A failure's duration is how long the store took to fail, not what a push costs, so the spacing
  // keeps the last successful push's duration.
  if (outcome === "failed") return { ...s, lastPushStartedAt: startedAt };
  const timing = { lastPushStartedAt: startedAt, lastPushDurationMs: finishedAt - startedAt };
  const historyStillDirty = s.lastHistoryChangeAt !== null && s.lastHistoryChangeAt > startedAt;
  return {
    ...timing,
    neverPushed: outcome === "committed" ? false : s.neverPushed,
    historyDirtySince: historyStillDirty ? s.lastHistoryChangeAt : null,
    lastHistoryChangeAt: historyStillDirty ? s.lastHistoryChangeAt : null,
    otherDirtySince:
      s.otherDirtySince !== null && s.otherDirtySince > startedAt ? s.otherDirtySince : null,
  };
};

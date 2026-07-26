import { rebuildDays, getUtcDay, applyRawRetention } from "./telemetry-rebuild";
import {
  buildUsageCounterKey,
  upstashPipeline,
  usagePeriodForDay,
  TELEMETRY_LAST_REBUILD_KEY,
  TELEMETRY_USAGE_TTL_SECONDS,
} from "./_telemetry-shared";

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, max-age=0");

  if (req.method !== "GET" && req.method !== "POST") {
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const cronSecret = String(process.env.CRON_SECRET || "").trim();
  if (!cronSecret) {
    return res.status(500).json({ error: "Missing CRON_SECRET" });
  }

  const auth = String(req.headers.authorization || "").trim();
  if (auth !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const scope = String(req.query.scope || "today").trim().toLowerCase();
  const apply = true;
  // The automatic run never resets. Reset scans and deletes every
  // `telemetry:day:<day>:*` key, and any family the rebuild does not rewrite is
  // simply destroyed -- which is what happened nightly to the product active,
  // new-user, and migrated sets. Repairing stale keys stays available through
  // the authenticated manual endpoint, where it is a deliberate act.
  const resetDayKeys = false;

  const days = scope === "yesterday" ? [getUtcDay(-1)] : [getUtcDay(0)];

  const retentionLookback = Math.max(
    parseInt(process.env.TELEMETRY_RAW_RETENTION_LOOKBACK_DAYS || "3", 10) || 3,
    1
  );
  const retentionDays = Array.from({ length: retentionLookback }, (_, index) => getUtcDay(-index));

  try {
    const result = await rebuildDays({
      days,
      apply,
      resetDayKeys,
      pageCount: 500,
    });

    // Ingestion no longer sets raw retention per event; it is applied here, at
    // one command per stream.
    const retention = await applyRawRetention(retentionDays);

    // Nothing watches this system's own cost, which is how the last overrun
    // reached 513K of a 500K allowance before anyone noticed -- and noticed
    // only because feedback, sharing the database, started failing. Upstash
    // cannot alert on it: its notifications are spend-based and a free plan
    // with a hard cap never accrues spend.
    const health = await recordRunHealth(result.summaries || []);

    return res.status(200).json({
      ok: true,
      scope,
      dry_run: false,
      reset_day_keys: resetDayKeys,
      raw_retention: retention,
      health,
      ...result,
    });
  } catch (error) {
    return res.status(500).json({ error: "Cron rebuild failed", detail: error.message || String(error) });
  }
}

// Ingestion costs exactly one command per accepted event, so the day's event
// count IS its ingestion command count -- no separate measurement is needed.
// Adding the rebuild's own writes gives the recurring cost of the whole
// telemetry system for that day, accumulated into one counter per billing
// period so the dashboard can read it in a single command.
async function recordRunHealth(summaries) {
  const summary = summaries[0];
  if (!summary || !summary.day) {
    return { recorded: false };
  }

  const events = Number(summary.raw_events) || 0;
  const writes =
    (Number(summary.writes) || 0) +
    (Number(summary.new_user_writes) || 0) +
    (Number(summary.product_new_user_writes) || 0);
  const estimated = events + writes;
  const period = usagePeriodForDay(summary.day);
  const counterKey = buildUsageCounterKey(period);

  await upstashPipeline([
    ["INCRBY", counterKey, estimated],
    ["EXPIRE", counterKey, TELEMETRY_USAGE_TTL_SECONDS],
    [
      "SET",
      TELEMETRY_LAST_REBUILD_KEY,
      new Date().toISOString(),
      "EX",
      TELEMETRY_USAGE_TTL_SECONDS,
    ],
  ]);

  return { recorded: true, period, estimated_commands: estimated, events, writes };
}

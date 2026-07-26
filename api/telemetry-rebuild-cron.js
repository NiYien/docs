import { rebuildDays, getUtcDay, applyRawRetention } from "./telemetry-rebuild";

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

    return res.status(200).json({
      ok: true,
      scope,
      dry_run: false,
      reset_day_keys: resetDayKeys,
      raw_retention: retention,
      ...result,
    });
  } catch (error) {
    return res.status(500).json({ error: "Cron rebuild failed", detail: error.message || String(error) });
  }
}
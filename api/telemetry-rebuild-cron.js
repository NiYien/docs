import { rebuildDays, getUtcDay } from "./telemetry-rebuild";

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
  const resetDayKeys = String(process.env.TELEMETRY_REBUILD_RESET_DAY_KEYS || "true").toLowerCase() !== "false";

  const days = scope === "yesterday" ? [getUtcDay(-1)] : [getUtcDay(0)];

  try {
    const result = await rebuildDays({
      days,
      apply,
      resetDayKeys,
      pageCount: 500,
    });

    return res.status(200).json({
      ok: true,
      scope,
      dry_run: false,
      reset_day_keys: resetDayKeys,
      ...result,
    });
  } catch (error) {
    return res.status(500).json({ error: "Cron rebuild failed", detail: error.message || String(error) });
  }
}
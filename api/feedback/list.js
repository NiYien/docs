// GET /api/feedback/list — admin-only listing endpoint.
//
// Auth: `Authorization: Bearer <FEEDBACK_ADMIN_TOKEN>` (env var). Reject 401
// if missing or wrong.
//
// Query params:
//   since — ISO 8601 datetime, default = 7 days ago
//   limit — integer, 1..500, default 100
//
// Returns the most recent confirmed feedback records first, walking daily
// indexes from `since` through today (UTC).

import { safeJsonParse, upstashPipeline } from "../_telemetry-shared";

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 500;
const DEFAULT_LOOKBACK_DAYS = 7;
const MAX_LOOKBACK_DAYS = 90;

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");

  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const adminToken = String(process.env.FEEDBACK_ADMIN_TOKEN || "").trim();
  if (!adminToken) {
    return res.status(503).json({ error: "FEEDBACK_ADMIN_TOKEN not configured" });
  }

  const auth = String(req.headers?.authorization || "").trim();
  const expected = `Bearer ${adminToken}`;
  if (!constantTimeEquals(auth, expected)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const limit = clampLimit(req.query?.limit);
  const since = parseSince(req.query?.since);
  const today = new Date();
  const days = enumerateDateRangeUTC(since, today);
  if (!days.length) {
    return res.status(200).json({ items: [], count: 0 });
  }

  const ids = [];
  // Walk newest day first so the response is naturally reverse-chronological.
  for (let i = days.length - 1; i >= 0; i -= 1) {
    if (ids.length >= limit) {
      break;
    }
    const remaining = limit - ids.length;
    let listResp;
    try {
      const responses = await upstashPipeline([
        ["LRANGE", `fb:index:${days[i]}`, "0", String(remaining - 1)],
      ]);
      listResp = responses?.[0]?.result;
    } catch (error) {
      return res.status(500).json({
        error: "KV read failed",
        detail: error.message || String(error),
      });
    }
    if (Array.isArray(listResp)) {
      for (const id of listResp) {
        if (typeof id === "string" && id) {
          ids.push(id);
          if (ids.length >= limit) break;
        }
      }
    }
  }

  if (!ids.length) {
    return res.status(200).json({ items: [], count: 0 });
  }

  // Batch MGET in chunks of 200 to avoid oversized pipeline payloads.
  const records = [];
  const CHUNK = 200;
  for (let i = 0; i < ids.length; i += CHUNK) {
    const chunk = ids.slice(i, i + CHUNK);
    let mgetResp;
    try {
      const responses = await upstashPipeline([
        ["MGET", ...chunk.map((id) => `fb:${id}`)],
      ]);
      mgetResp = responses?.[0]?.result;
    } catch (error) {
      return res.status(500).json({
        error: "KV mget failed",
        detail: error.message || String(error),
      });
    }
    if (!Array.isArray(mgetResp)) continue;
    for (const raw of mgetResp) {
      if (!raw) continue;
      const parsed = safeJsonParse(raw, null);
      if (parsed && typeof parsed === "object") {
        records.push(parsed);
      }
    }
  }

  return res.status(200).json({ items: records, count: records.length });
}

function clampLimit(raw) {
  const numeric = parseInt(String(raw || ""), 10);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return DEFAULT_LIMIT;
  }
  return Math.min(numeric, MAX_LIMIT);
}

function parseSince(raw) {
  const text = String(raw || "").trim();
  if (text) {
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
      // Don't allow looking back further than MAX_LOOKBACK_DAYS to bound
      // worst-case KV reads.
      const earliest = new Date(Date.now() - MAX_LOOKBACK_DAYS * 86400 * 1000);
      return parsed.getTime() < earliest.getTime() ? earliest : parsed;
    }
  }
  return new Date(Date.now() - DEFAULT_LOOKBACK_DAYS * 86400 * 1000);
}

function enumerateDateRangeUTC(start, end) {
  const out = [];
  const cur = new Date(
    Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate())
  );
  const last = new Date(
    Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate())
  );
  while (cur.getTime() <= last.getTime()) {
    const y = cur.getUTCFullYear();
    const m = String(cur.getUTCMonth() + 1).padStart(2, "0");
    const d = String(cur.getUTCDate()).padStart(2, "0");
    out.push(`${y}${m}${d}`);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

// Constant-time string compare to discourage timing oracle on the bearer.
function constantTimeEquals(a, b) {
  const sa = String(a);
  const sb = String(b);
  if (sa.length !== sb.length) {
    return false;
  }
  let diff = 0;
  for (let i = 0; i < sa.length; i += 1) {
    diff |= sa.charCodeAt(i) ^ sb.charCodeAt(i);
  }
  return diff === 0;
}

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, max-age=0");

  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const requiredToken = String(process.env.TELEMETRY_REBUILD_TOKEN || "").trim();
  if (!requiredToken) {
    return res.status(500).json({ error: "Missing TELEMETRY_REBUILD_TOKEN" });
  }

  const providedToken = String(req.headers["x-rebuild-token"] || "").trim();
  if (!providedToken || providedToken !== requiredToken) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : (req.body || {});
  if (!body || typeof body !== "object") {
    return res.status(400).json({ error: "Invalid JSON" });
  }

  const startDay = normalizeDay(String(body.start_day || body.day || "").trim());
  const endDay = normalizeDay(String(body.end_day || body.day || "").trim());
  if (!startDay || !endDay) {
    return res.status(400).json({ error: "start_day/end_day required (YYYY-MM-DD)" });
  }

  const days = buildDayRange(startDay, endDay);
  if (!days.length) {
    return res.status(400).json({ error: "Invalid day range" });
  }

  const dryRun = body.dry_run !== false;
  const apply = body.apply === true || !dryRun;
  const resetDayKeys = body.reset_day_keys === true;

  try {
    const result = await rebuildDays({
      days,
      apply,
      resetDayKeys,
      pageCount: 500,
    });

    return res.status(200).json({
      ok: true,
      dry_run: !apply,
      reset_day_keys: resetDayKeys,
      ...result,
    });
  } catch (error) {
    return res.status(500).json({ error: "Rebuild failed", detail: error.message || String(error) });
  }
}

export async function rebuildDays({ days, apply, resetDayKeys, pageCount = 500 }) {
  const countTtlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
  const countTtlSeconds = Math.max(countTtlDays, 1) * 86400;
  const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
  const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;

  const summaries = [];
  for (const day of days) {
    const streamKey = buildRawStreamKey(day);
    const events = await readRawEvents(streamKey, pageCount);
    const aggregates = buildDayAggregates(events, day);

    const summary = {
      day,
      stream_key: streamKey,
      raw_events: events.length,
      opens_keys: Object.keys(aggregates.opens).length,
      unique_keys: Object.keys(aggregates.unique).length,
      applied: false,
      deleted_day_keys: 0,
      writes: 0,
    };

    if (apply) {
      if (resetDayKeys) {
        const keys = await scanKeys(`telemetry:day:${day}:*`);
        summary.deleted_day_keys = keys.length;
        await deleteKeys(keys);
      }

      summary.writes = await writeDayAggregates(aggregates, countTtlSeconds, uniqueTtlSeconds);
      summary.applied = true;
    }

    summaries.push(summary);
  }

  return { days, summaries };
}

export function getUtcDay(offsetDays = 0) {
  const now = new Date();
  now.setUTCHours(0, 0, 0, 0);
  const d = new Date(now.getTime() + offsetDays * 86400000);
  return d.toISOString().slice(0, 10);
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch (error) {
    return null;
  }
}

function normalizeDay(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : "";
}

function buildDayRange(startDay, endDay) {
  const start = new Date(`${startDay}T00:00:00Z`);
  const end = new Date(`${endDay}T00:00:00Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start > end) {
    return [];
  }

  const list = [];
  for (let ts = start.getTime(); ts <= end.getTime(); ts += 86400000) {
    list.push(new Date(ts).toISOString().slice(0, 10));
  }

  return list;
}

function buildRawStreamKey(day) {
  return `telemetry:raw:day:${day}`;
}

async function readRawEvents(streamKey, count) {
  const rows = [];
  let start = "-";

  while (true) {
    const [response] = await upstashPipeline([
      ["XRANGE", streamKey, start, "+", "COUNT", count],
    ]);

    const result = response && response.result;
    if (!Array.isArray(result) || result.length === 0) {
      break;
    }

    for (const item of result) {
      const id = item && item[0];
      const fields = item && item[1];
      if (!id || !Array.isArray(fields)) {
        continue;
      }

      rows.push(streamFieldsToObject(fields));
      start = `(${id}`;
    }

    if (result.length < count) {
      break;
    }
  }

  return rows;
}

function streamFieldsToObject(fields) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[String(fields[i])] = fields[i + 1];
  }
  return obj;
}

function buildDayAggregates(events, targetDay) {
  const opens = {};
  const uniqueSets = {};

  for (const row of events) {
    const event = String(row.event || "").trim();
    if (event !== "open") {
      continue;
    }

    const ts = Number(row.ts || 0);
    if (!Number.isFinite(ts) || ts <= 0) {
      continue;
    }

    const d = new Date(ts * 1000);
    const iso = d.toISOString();
    const day = iso.slice(0, 10);
    if (day !== targetDay) {
      continue;
    }

    const hour = iso.slice(11, 13);
    const city = normalizeKeyPart(String(row.city || "Unknown"));
    const country = normalizeKeyPart(String(row.country || "Unknown"));
    const brand = normalizeKeyPart(String(row.camera_brand || "Other"));
    const model = normalizeKeyPart(String(row.camera_model || "Unknown"));
    const language = normalizeKeyPart(String(row.language || "Unknown"));
    const anonId = String(row.anon_id || "").trim();
    if (!anonId) {
      continue;
    }

    const openKeys = [
      `telemetry:day:${targetDay}:city:${city}:brand:${brand}:event:open`,
      `telemetry:day:${targetDay}:city:${city}:event:open`,
      `telemetry:day:${targetDay}:brand:${brand}:event:open`,
      `telemetry:day:${targetDay}:model:${model}:event:open`,
      `telemetry:day:${targetDay}:lang:${language}:event:open`,
      `telemetry:day:${targetDay}:country:${country}:event:open`,
      `telemetry:day:${targetDay}:event:open`,
      `telemetry:day:${targetDay}:hour:${hour}:event:open`,
    ];

    for (const key of openKeys) {
      opens[key] = (opens[key] || 0) + 1;
    }

    const uniqueKeys = [
      `telemetry:day:${targetDay}:unique:all`,
      `telemetry:day:${targetDay}:unique:city:${city}`,
      `telemetry:day:${targetDay}:unique:brand:${brand}`,
      `telemetry:day:${targetDay}:unique:model:${model}`,
      `telemetry:day:${targetDay}:unique:country:${country}`,
    ];

    for (const key of uniqueKeys) {
      if (!uniqueSets[key]) {
        uniqueSets[key] = new Set();
      }
      uniqueSets[key].add(anonId);
    }
  }

  const unique = {};
  for (const [key, value] of Object.entries(uniqueSets)) {
    unique[key] = Array.from(value);
  }

  return { opens, unique };
}

function normalizeKeyPart(value) {
  const trimmed = String(value || "").trim().slice(0, 64);
  if (!trimmed) {
    return "Unknown";
  }
  return encodeURIComponent(trimmed);
}

async function writeDayAggregates(aggregates, countTtlSeconds, uniqueTtlSeconds) {
  const commands = [];
  for (const [key, count] of Object.entries(aggregates.opens)) {
    commands.push(["SET", key, String(count)]);
    commands.push(["EXPIRE", key, countTtlSeconds]);
  }

  for (const [key, anonIds] of Object.entries(aggregates.unique)) {
    if (anonIds.length > 0) {
      commands.push(["SADD", key, ...anonIds]);
    }
    if (uniqueTtlSeconds > 0) {
      commands.push(["EXPIRE", key, uniqueTtlSeconds]);
    }
  }

  let writes = 0;
  for (let i = 0; i < commands.length; i += 200) {
    const chunk = commands.slice(i, i + 200);
    await upstashPipeline(chunk);
    writes += chunk.length;
  }
  return writes;
}

async function scanKeys(pattern) {
  const keys = [];
  let cursor = "0";

  for (let i = 0; i < 60; i += 1) {
    const [response] = await upstashPipeline([
      ["SCAN", cursor, "MATCH", pattern, "COUNT", 1000],
    ]);

    const data = response && response.result;
    if (!Array.isArray(data) || data.length < 2) {
      break;
    }

    cursor = data[0];
    const batch = Array.isArray(data[1]) ? data[1] : [];
    keys.push(...batch);

    if (cursor === "0") {
      break;
    }
  }

  return keys;
}

async function deleteKeys(keys) {
  if (!keys.length) {
    return;
  }

  const chunkSize = 200;
  for (let i = 0; i < keys.length; i += chunkSize) {
    const chunk = keys.slice(i, i + chunkSize);
    await upstashPipeline([["DEL", ...chunk]]);
  }
}

async function upstashPipeline(commands) {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!url || !token) {
    throw new Error("Missing Upstash config");
  }

  const response = await fetch(`${url}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
  });

  if (!response.ok) {
    throw new Error("Upstash pipeline error");
  }

  const data = await response.json();
  return Array.isArray(data) ? data : [];
}
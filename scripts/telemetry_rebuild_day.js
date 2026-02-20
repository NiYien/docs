/*
Usage:
  DAY=2026-02-20 node scripts/telemetry_rebuild_day.js
  DAY=2026-02-20 APPLY=true RESET_DAY_KEYS=true node scripts/telemetry_rebuild_day.js

Required env:
  KV_REST_API_URL (or UPSTASH_REDIS_REST_URL)
  KV_REST_API_TOKEN (or UPSTASH_REDIS_REST_TOKEN)

Optional env:
  APPLY=true|false                default false (dry-run)
  RESET_DAY_KEYS=true|false       default false
  TELEMETRY_TTL_DAYS=90
  TELEMETRY_UNIQUE_TTL_DAYS=0
  BATCH_SIZE=500
*/

const day = String(process.env.DAY || "").trim();
if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
  console.error("Invalid DAY. Use YYYY-MM-DD");
  process.exit(1);
}

const apply = String(process.env.APPLY || "false").toLowerCase() === "true";
const resetDayKeys = String(process.env.RESET_DAY_KEYS || "false").toLowerCase() === "true";
const ttlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
const ttlSeconds = Math.max(ttlDays, 1) * 86400;
const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;
const batchSize = Math.max(50, parseInt(process.env.BATCH_SIZE || "500", 10));

const streamKey = `telemetry:raw:day:${day}`;

async function main() {
  const events = await readRawEvents(streamKey, batchSize);
  const aggregates = buildAggregates(events, day);

  console.log(JSON.stringify({
    day,
    stream_key: streamKey,
    events: events.length,
    opens_keys: Object.keys(aggregates.opens).length,
    unique_keys: Object.keys(aggregates.unique).length,
    apply,
    reset_day_keys: resetDayKeys,
  }, null, 2));

  if (!apply) {
    console.log("Dry-run only. Set APPLY=true to write.");
    return;
  }

  if (resetDayKeys) {
    const keys = await scanKeys(`telemetry:day:${day}:*`);
    await deleteKeys(keys);
    console.log(`Deleted day keys: ${keys.length}`);
  }

  await writeAggregates(aggregates, ttlSeconds, uniqueTtlSeconds);
  console.log("Rebuild done.");
}

function buildAggregates(events, targetDay) {
  const opens = {};
  const uniqueSets = {};

  for (const entry of events) {
    const event = String(entry.event || "").trim();
    if (event !== "open") {
      continue;
    }

    const ts = Number(entry.ts || 0);
    if (!Number.isFinite(ts) || ts <= 0) {
      continue;
    }

    const d = new Date(ts * 1000);
    const iso = d.toISOString();
    const dayValue = iso.slice(0, 10);
    if (dayValue !== targetDay) {
      continue;
    }

    const hour = iso.slice(11, 13);
    const city = normalizeKeyPart(String(entry.city || "Unknown"));
    const country = normalizeKeyPart(String(entry.country || "Unknown"));
    const brand = normalizeKeyPart(String(entry.camera_brand || "Other"));
    const model = normalizeKeyPart(String(entry.camera_model || "Unknown"));
    const language = normalizeKeyPart(String(entry.language || "Unknown"));
    const anonId = String(entry.anon_id || "").trim();
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
  for (const [key, set] of Object.entries(uniqueSets)) {
    unique[key] = Array.from(set);
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

async function readRawEvents(key, count) {
  const rows = [];
  let start = "-";

  while (true) {
    const [resp] = await upstashPipeline([
      ["XRANGE", key, start, "+", "COUNT", count],
    ]);

    const result = (resp && resp.result) || [];
    if (!Array.isArray(result) || result.length === 0) {
      break;
    }

    for (const item of result) {
      const id = item && item[0];
      const fields = item && item[1];
      if (!id || !Array.isArray(fields)) {
        continue;
      }

      rows.push(flattenStreamFields(fields));
      start = `(${id}`;
    }

    if (result.length < count) {
      break;
    }
  }

  return rows;
}

function flattenStreamFields(fields) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[String(fields[i])] = fields[i + 1];
  }
  return obj;
}

async function writeAggregates(aggregates, countTtl, uniqueTtl) {
  const commands = [];

  for (const [key, value] of Object.entries(aggregates.opens)) {
    commands.push(["SET", key, String(value)]);
    commands.push(["EXPIRE", key, countTtl]);
  }

  for (const [key, anonIds] of Object.entries(aggregates.unique)) {
    if (anonIds.length > 0) {
      commands.push(["SADD", key, ...anonIds]);
    }
    if (uniqueTtl > 0) {
      commands.push(["EXPIRE", key, uniqueTtl]);
    }
  }

  await runChunked(commands, 200);
}

async function scanKeys(pattern) {
  let cursor = "0";
  const keys = [];

  for (let i = 0; i < 60; i += 1) {
    const [scanResult] = await upstashPipeline([
      ["SCAN", cursor, "MATCH", pattern, "COUNT", 1000],
    ]);

    const data = scanResult && scanResult.result;
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

async function runChunked(commands, chunkSize) {
  for (let i = 0; i < commands.length; i += chunkSize) {
    const chunk = commands.slice(i, i + chunkSize);
    await upstashPipeline(chunk);
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
    throw new Error(`Upstash pipeline error: ${response.status}`);
  }

  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});

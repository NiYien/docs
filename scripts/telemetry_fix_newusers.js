/*
Fix new:all sets only — minimal commands.
Reads raw streams to get anon_ids, checks first_seen, backfills new:all.

Usage:
  START_DAY=2026-02-11 END_DAY=2026-03-28 node scripts/telemetry_fix_newusers.js
  START_DAY=2026-02-11 END_DAY=2026-03-28 APPLY=true node scripts/telemetry_fix_newusers.js

Required env:
  KV_REST_API_URL (or UPSTASH_REDIS_REST_URL)
  KV_REST_API_TOKEN (or UPSTASH_REDIS_REST_TOKEN)
*/

const startDay = String(process.env.START_DAY || "").trim();
const endDay = String(process.env.END_DAY || "").trim();

if (!/^\d{4}-\d{2}-\d{2}$/.test(startDay) || !/^\d{4}-\d{2}-\d{2}$/.test(endDay)) {
  console.error("Invalid START_DAY / END_DAY. Use YYYY-MM-DD");
  process.exit(1);
}

const apply = String(process.env.APPLY || "false").toLowerCase() === "true";
const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;
const batchSize = Math.max(50, parseInt(process.env.BATCH_SIZE || "500", 10));

function buildDayRange(start, end) {
  const s = new Date(`${start}T00:00:00Z`);
  const e = new Date(`${end}T00:00:00Z`);
  if (Number.isNaN(s.getTime()) || Number.isNaN(e.getTime()) || s > e) return [];
  const list = [];
  for (let ts = s.getTime(); ts <= e.getTime(); ts += 86400000) {
    list.push(new Date(ts).toISOString().slice(0, 10));
  }
  return list;
}

function normalizeDay(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "")) ? String(value) : "";
}

const days = buildDayRange(startDay, endDay);
if (!days.length) { console.error("Invalid day range"); process.exit(1); }

async function main() {
  let readCmds = 0;

  // Step 1: Read raw streams, only collect anon_id + day
  const dayAnonIds = {};
  const allAnonIds = new Set();

  for (const day of days) {
    dayAnonIds[day] = new Set();
    const streamKey = `telemetry:raw:day:${day}`;
    let start = "-";

    while (true) {
      readCmds++;
      const [resp] = await upstashPipeline([["XRANGE", streamKey, start, "+", "COUNT", batchSize]]);
      const result = (resp && resp.result) || [];
      if (!Array.isArray(result) || result.length === 0) break;

      for (const item of result) {
        const id = item && item[0];
        const fields = item && item[1];
        if (!id || !Array.isArray(fields)) continue;

        let event = "", anonId = "", ts = 0;
        for (let i = 0; i < fields.length; i += 2) {
          const k = String(fields[i]);
          if (k === "event") event = String(fields[i + 1] || "");
          else if (k === "anon_id") anonId = String(fields[i + 1] || "").trim();
          else if (k === "ts") ts = Number(fields[i + 1] || 0);
        }

        if (event === "open" && anonId && ts > 0) {
          const eventDay = new Date(ts * 1000).toISOString().slice(0, 10);
          if (eventDay === day) {
            dayAnonIds[day].add(anonId);
            allAnonIds.add(anonId);
          }
        }
        start = `(${id}`;
      }

      if (result.length < batchSize) break;
    }
  }

  // Step 2: MGET all first_seen keys (deduped)
  const anonIdList = Array.from(allAnonIds);
  const firstSeenKeys = anonIdList.map((id) => {
    const n = encodeURIComponent(String(id).trim().slice(0, 128) || "unknown");
    return `telemetry:user:first_seen:${n}`;
  });

  const existingValues = [];
  let mgetCmds = 0;
  for (let i = 0; i < firstSeenKeys.length; i += 200) {
    const chunk = firstSeenKeys.slice(i, i + 200);
    mgetCmds++;
    const [r] = await upstashPipeline([["MGET", ...chunk]]);
    existingValues.push(...((r && r.result) || []));
  }

  // Build lookup: anonId -> first_seen day
  const firstSeenMap = {};
  for (let i = 0; i < anonIdList.length; i++) {
    firstSeenMap[anonIdList[i]] = normalizeDay(String(existingValues[i] || "").trim());
  }

  // Step 3: Find users to backfill per day
  const dayNewUsers = {};
  let totalBackfilled = 0;

  for (const day of days) {
    const newUsers = [];
    for (const anonId of dayAnonIds[day]) {
      if (firstSeenMap[anonId] === day) {
        newUsers.push(anonId);
      }
    }
    if (newUsers.length > 0) {
      dayNewUsers[day] = newUsers;
      totalBackfilled += newUsers.length;
    }
  }

  // Print summary
  const daySummaries = days
    .filter((d) => dayAnonIds[d].size > 0)
    .map((d) => ({
      day: d,
      users: dayAnonIds[d].size,
      new_to_backfill: dayNewUsers[d] ? dayNewUsers[d].length : 0,
    }));

  const writeCmds = Object.keys(dayNewUsers).length + (uniqueTtlSeconds > 0 ? Object.keys(dayNewUsers).length : 0);

  console.log(JSON.stringify({
    days_with_data: daySummaries.length,
    total_users: anonIdList.length,
    total_to_backfill: totalBackfilled,
    estimated_commands: { read: readCmds, mget: mgetCmds, write: writeCmds, total: readCmds + mgetCmds + writeCmds },
    apply,
    day_summaries: daySummaries,
  }, null, 2));

  if (!apply) {
    console.log("\nDry-run only. Set APPLY=true to write.");
    return;
  }

  // Step 4: Write SADD per day
  const commands = [];
  for (const [day, ids] of Object.entries(dayNewUsers)) {
    commands.push(["SADD", `telemetry:day:${day}:new:all`, ...ids]);
    if (uniqueTtlSeconds > 0) {
      commands.push(["EXPIRE", `telemetry:day:${day}:new:all`, uniqueTtlSeconds]);
    }
  }

  let actualWrite = 0;
  for (let i = 0; i < commands.length; i += 200) {
    const chunk = commands.slice(i, i + 200);
    await upstashPipeline(chunk);
    actualWrite += chunk.length;
  }

  console.log(JSON.stringify({
    result: "done",
    commands_used: { read: readCmds, mget: mgetCmds, write: actualWrite, total: readCmds + mgetCmds + actualWrite },
    backfilled: totalBackfilled,
  }, null, 2));
}

async function upstashPipeline(commands) {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) throw new Error("Missing Upstash config");

  const response = await fetch(`${url}/pipeline`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(commands),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`Upstash pipeline error: ${response.status} ${text}`);
  }

  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

main().catch((e) => { console.error(e.stack || String(e)); process.exit(1); });

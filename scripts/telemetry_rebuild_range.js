/*
Usage:
  START_DAY=2026-03-26 END_DAY=2026-03-31 node scripts/telemetry_rebuild_range.js
  START_DAY=2026-03-26 END_DAY=2026-03-31 APPLY=true node scripts/telemetry_rebuild_range.js

Required env:
  KV_REST_API_URL (or UPSTASH_REDIS_REST_URL)
  KV_REST_API_TOKEN (or UPSTASH_REDIS_REST_TOKEN)

Optional env:
  APPLY=true|false                default false (dry-run)
  TELEMETRY_TTL_DAYS=90
  TELEMETRY_UNIQUE_TTL_DAYS=0
  BATCH_SIZE=500

Optimized for minimal Upstash commands:
  - Uses SETEX instead of SET+EXPIRE (halves counter writes)
  - Skips reset_day_keys (no existing data to clear)
  - Cross-day dedup for new user MGET
  - Batched SADD for new users per day
*/

const startDay = String(process.env.START_DAY || "").trim();
const endDay = String(process.env.END_DAY || "").trim();

if (!/^\d{4}-\d{2}-\d{2}$/.test(startDay) || !/^\d{4}-\d{2}-\d{2}$/.test(endDay)) {
  console.error("Invalid START_DAY / END_DAY. Use YYYY-MM-DD");
  process.exit(1);
}

const apply = String(process.env.APPLY || "false").toLowerCase() === "true";
const ttlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
const ttlSeconds = Math.max(ttlDays, 1) * 86400;
const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;
const batchSize = Math.max(50, parseInt(process.env.BATCH_SIZE || "500", 10));

function buildDayRange(start, end) {
  const startDate = new Date(`${start}T00:00:00Z`);
  const endDate = new Date(`${end}T00:00:00Z`);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || startDate > endDate) {
    return [];
  }
  const list = [];
  for (let ts = startDate.getTime(); ts <= endDate.getTime(); ts += 86400000) {
    list.push(new Date(ts).toISOString().slice(0, 10));
  }
  return list;
}

const days = buildDayRange(startDay, endDay);
if (!days.length) {
  console.error("Invalid day range");
  process.exit(1);
}

function normalizeKeyPart(value) {
  const trimmed = String(value || "").trim().slice(0, 64);
  return trimmed ? encodeURIComponent(trimmed) : "Unknown";
}

function normalizeDay(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "")) ? String(value) : "";
}

async function main() {
  // Step 1: Read raw events for all days
  const allDayData = {};
  let totalEvents = 0;
  let readCommands = 0;

  for (const day of days) {
    const streamKey = `telemetry:raw:day:${day}`;
    const events = await readRawEvents(streamKey, batchSize, (cmds) => { readCommands += cmds; });
    allDayData[day] = events;
    totalEvents += events.length;
  }

  // Step 2: Aggregate in memory
  const allOpens = {};       // key -> count
  const allUnique = {};      // key -> Set<anonId>
  const dayAnonIds = {};     // day -> Set<anonId>
  let allAnonIds = new Set();

  for (const day of days) {
    dayAnonIds[day] = new Set();
    for (const row of allDayData[day]) {
      const event = String(row.event || "").trim();
      if (event !== "open") continue;

      const ts = Number(row.ts || 0);
      if (!Number.isFinite(ts) || ts <= 0) continue;

      const d = new Date(ts * 1000);
      const iso = d.toISOString();
      const dayValue = iso.slice(0, 10);
      if (dayValue !== day) continue;

      const hour = iso.slice(11, 13);
      const city = normalizeKeyPart(String(row.city || "Unknown"));
      const country = normalizeKeyPart(String(row.country || "Unknown"));
      const brand = normalizeKeyPart(String(row.camera_brand || "Other"));
      const model = normalizeKeyPart(String(row.camera_model || "Unknown"));
      const language = normalizeKeyPart(String(row.language || "Unknown"));
      const anonId = String(row.anon_id || "").trim();
      if (!anonId) continue;

      dayAnonIds[day].add(anonId);
      allAnonIds.add(anonId);

      const openKeys = [
        `telemetry:day:${day}:city:${city}:brand:${brand}:event:open`,
        `telemetry:day:${day}:city:${city}:event:open`,
        `telemetry:day:${day}:brand:${brand}:event:open`,
        `telemetry:day:${day}:model:${model}:event:open`,
        `telemetry:day:${day}:lang:${language}:event:open`,
        `telemetry:day:${day}:country:${country}:event:open`,
        `telemetry:day:${day}:event:open`,
        `telemetry:day:${day}:hour:${hour}:event:open`,
      ];

      for (const key of openKeys) {
        allOpens[key] = (allOpens[key] || 0) + 1;
      }

      const uniqueKeys = [
        `telemetry:day:${day}:unique:all`,
        `telemetry:day:${day}:unique:city:${city}`,
        `telemetry:day:${day}:unique:brand:${brand}`,
        `telemetry:day:${day}:unique:model:${model}`,
        `telemetry:day:${day}:unique:country:${country}`,
      ];

      for (const key of uniqueKeys) {
        if (!allUnique[key]) allUnique[key] = new Set();
        allUnique[key].add(anonId);
      }
    }
  }

  // Count commands for dry-run estimate
  const opensCount = Object.keys(allOpens).length;    // 1 SETEX each
  let uniqueCount = 0;
  for (const key of Object.keys(allUnique)) {
    uniqueCount += 1;  // 1 SADD each
    if (uniqueTtlSeconds > 0) uniqueCount += 1;  // 1 EXPIRE each
  }

  const allAnonIdList = Array.from(allAnonIds);
  const mgetCommands = allAnonIdList.length > 0 ? Math.ceil(allAnonIdList.length / 200) : 0;

  // Estimate new user writes (worst case: all are new)
  const newUserEstimate = allAnonIdList.length; // SET per new user
  let newUserSaddEstimate = 0;
  for (const day of days) {
    if (dayAnonIds[day].size > 0) newUserSaddEstimate += 1; // 1 SADD per day
    if (uniqueTtlSeconds > 0 && dayAnonIds[day].size > 0) newUserSaddEstimate += 1;
  }

  const totalEstimate = readCommands + opensCount + uniqueCount + mgetCommands + newUserEstimate + newUserSaddEstimate;

  // Print summary
  const daySummaries = days.map((day) => ({
    day,
    raw_events: allDayData[day].length,
    unique_users: dayAnonIds[day].size,
  }));

  console.log(JSON.stringify({
    days,
    day_summaries: daySummaries,
    total_events: totalEvents,
    total_unique_users: allAnonIdList.length,
    opens_keys: opensCount,
    unique_keys: Object.keys(allUnique).length,
    estimated_commands: {
      read: readCommands,
      opens_write: opensCount,
      unique_write: uniqueCount,
      new_user_mget: mgetCommands,
      new_user_write_max: newUserEstimate + newUserSaddEstimate,
      total_max: totalEstimate,
    },
    apply,
  }, null, 2));

  if (!apply) {
    console.log("\nDry-run only. Set APPLY=true to write.");
    return;
  }

  // Step 3: Write opens with SETEX
  let writeCommands = 0;
  const opensCommands = [];
  for (const [key, count] of Object.entries(allOpens)) {
    opensCommands.push(["SETEX", key, ttlSeconds, String(count)]);
  }
  writeCommands += await runChunked(opensCommands, 200);

  // Step 4: Write unique sets
  const uniqueCommands = [];
  for (const [key, members] of Object.entries(allUnique)) {
    const memberList = Array.from(members);
    if (memberList.length > 0) {
      uniqueCommands.push(["SADD", key, ...memberList]);
    }
    if (uniqueTtlSeconds > 0) {
      uniqueCommands.push(["EXPIRE", key, uniqueTtlSeconds]);
    }
  }
  writeCommands += await runChunked(uniqueCommands, 200);

  // Step 5: New users - cross-day deduped MGET
  let newUsersCreated = 0;
  let newUsersCorrected = 0;
  let newUsersBackfilled = 0;
  let newUserCommands = 0;

  if (allAnonIdList.length > 0) {
    const firstSeenKeys = allAnonIdList.map((id) => {
      const normalized = encodeURIComponent(String(id).trim().slice(0, 128) || "unknown");
      return `telemetry:user:first_seen:${normalized}`;
    });

    const existingValues = await getValues(firstSeenKeys);
    newUserCommands += Math.ceil(firstSeenKeys.length / 200); // MGET commands

    // Find earliest day each user appeared in our rebuild range
    const userEarliestDay = {};
    for (const day of days) {
      for (const anonId of dayAnonIds[day]) {
        if (!userEarliestDay[anonId] || day < userEarliestDay[anonId]) {
          userEarliestDay[anonId] = day;
        }
      }
    }

    // Collect new user writes
    const newUserSetCommands = [];
    const dayNewUsers = {};  // day -> [anonId, ...]

    for (let i = 0; i < allAnonIdList.length; i++) {
      const anonId = allAnonIdList[i];
      const existingDay = normalizeDay(String(existingValues[i] || "").trim());
      const earliestDay = userEarliestDay[anonId];
      const firstSeenKey = firstSeenKeys[i];

      if (!existingDay) {
        // New user: set first_seen to earliest day in our range
        newUserSetCommands.push(["SET", firstSeenKey, earliestDay]);
        if (!dayNewUsers[earliestDay]) dayNewUsers[earliestDay] = [];
        dayNewUsers[earliestDay].push(anonId);
        newUsersCreated++;
      } else if (earliestDay < existingDay) {
        // Correct first_seen to earlier day
        newUserSetCommands.push(["SET", firstSeenKey, earliestDay]);
        newUserSetCommands.push(["SREM", `telemetry:day:${existingDay}:new:all`, anonId]);
        if (!dayNewUsers[earliestDay]) dayNewUsers[earliestDay] = [];
        dayNewUsers[earliestDay].push(anonId);
        newUsersCorrected++;
      } else if (days.includes(existingDay)) {
        // User's first_seen falls in our rebuild range — new:all was likely not populated
        if (!dayNewUsers[existingDay]) dayNewUsers[existingDay] = [];
        dayNewUsers[existingDay].push(anonId);
        newUsersBackfilled++;
      }
    }

    // Batched SADD per day for new users
    for (const [day, ids] of Object.entries(dayNewUsers)) {
      if (ids.length > 0) {
        const dayNewUsersKey = `telemetry:day:${day}:new:all`;
        newUserSetCommands.push(["SADD", dayNewUsersKey, ...ids]);
        if (uniqueTtlSeconds > 0) {
          newUserSetCommands.push(["EXPIRE", dayNewUsersKey, uniqueTtlSeconds]);
        }
      }
    }

    newUserCommands += await runChunked(newUserSetCommands, 200);
  }

  console.log(JSON.stringify({
    result: "done",
    commands_used: {
      read: readCommands,
      opens_write: opensCommands.length,
      unique_write: uniqueCommands.length,
      new_user: newUserCommands,
      total: readCommands + writeCommands + newUserCommands,
    },
    new_users_created: newUsersCreated,
    new_users_corrected: newUsersCorrected,
    new_users_backfilled: newUsersBackfilled,
  }, null, 2));
}

async function readRawEvents(streamKey, count, onCommand) {
  const rows = [];
  let start = "-";

  while (true) {
    onCommand(1);
    const [resp] = await upstashPipeline([
      ["XRANGE", streamKey, start, "+", "COUNT", count],
    ]);

    const result = (resp && resp.result) || [];
    if (!Array.isArray(result) || result.length === 0) break;

    for (const item of result) {
      const id = item && item[0];
      const fields = item && item[1];
      if (!id || !Array.isArray(fields)) continue;

      const obj = {};
      for (let i = 0; i < fields.length; i += 2) {
        obj[String(fields[i])] = fields[i + 1];
      }
      rows.push(obj);
      start = `(${id}`;
    }

    if (result.length < count) break;
  }

  return rows;
}

async function getValues(keys) {
  if (!keys.length) return [];

  const result = [];
  const chunkSize = 200;

  for (let i = 0; i < keys.length; i += chunkSize) {
    const chunk = keys.slice(i, i + chunkSize);
    const [mgetResult] = await upstashPipeline([["MGET", ...chunk]]);
    const values = (mgetResult && mgetResult.result) || [];
    result.push(...values);
  }

  return result;
}

async function runChunked(commands, chunkSize) {
  let total = 0;
  for (let i = 0; i < commands.length; i += chunkSize) {
    const chunk = commands.slice(i, i + chunkSize);
    await upstashPipeline(chunk);
    total += chunk.length;
  }
  return total;
}

async function upstashPipeline(commands) {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!url || !token) {
    throw new Error("Missing Upstash config (KV_REST_API_URL / KV_REST_API_TOKEN)");
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
    const text = await response.text().catch(() => "");
    throw new Error(`Upstash pipeline error: ${response.status} ${text}`);
  }

  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});

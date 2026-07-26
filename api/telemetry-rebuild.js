import {
  buildEventAggregationPlan,
  buildRawStreamKey,
  deleteKeys,
  extractEventFields,
  getValues,
  normalizeDay,
  safeJsonParse,
  scanKeys,
  streamFieldsToObject,
  upstashPipeline,
  validateEventFields,
} from "./_telemetry-shared";

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

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : req.body || {};
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
    return res.status(500).json({
      error: "Rebuild failed",
      detail: error.message || String(error),
    });
  }
}

export async function rebuildDays({ days, apply, resetDayKeys, pageCount = 500 }) {
  const countTtlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
  const countTtlSeconds = Math.max(countTtlDays, 1) * 86400;
  const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "120", 10);
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
      count_keys: Object.keys(aggregates.counts).length,
      unique_keys: Object.keys(aggregates.unique).length,
      new_user_candidates: aggregates.newUserEntries.length,
      product_new_user_candidates: aggregates.productNewUserEntries.length,
      applied: false,
      deleted_day_keys: 0,
      writes: 0,
      new_user_writes: 0,
      new_users_created: 0,
      new_users_corrected: 0,
      product_new_user_writes: 0,
      product_new_users_created: 0,
      product_new_users_corrected: 0,
      product_first_seen_seeded: 0,
    };

    if (apply) {
      if (resetDayKeys) {
        const keys = await scanKeys(`telemetry:day:${day}:*`);
        summary.deleted_day_keys = keys.length;
        await deleteKeys(keys);
      }

      summary.writes = await writeDayAggregates(aggregates, countTtlSeconds, uniqueTtlSeconds);
      const newUserResult = await writeDayNewUsers(aggregates.newUserEntries, uniqueTtlSeconds);
      summary.new_user_writes = newUserResult.writes;
      summary.new_users_created = newUserResult.created;
      summary.new_users_corrected = newUserResult.corrected;

      const productResult = await writeProductNewUsers(
        aggregates.productNewUserEntries,
        uniqueTtlSeconds
      );
      summary.product_new_user_writes = productResult.writes;
      summary.product_new_users_created = productResult.created;
      summary.product_new_users_corrected = productResult.corrected;
      summary.product_first_seen_seeded = productResult.seeded;

      summary.applied = true;
    }

    summaries.push(summary);
  }

  return { days, summaries };
}

// Raw retention used to be re-applied by ingestion on every single event. It
// belongs on a schedule: one command per stream, independent of traffic.
//
// `TELEMETRY_RAW_TTL_DAYS=0` is a supported "retain forever" setting and must
// leave the streams alone rather than expiring them.
export async function applyRawRetention(days) {
  const rawTtlDays = parseInt(process.env.TELEMETRY_RAW_TTL_DAYS || "365", 10);
  const rawTtlSeconds = rawTtlDays > 0 ? rawTtlDays * 86400 : 0;

  if (!rawTtlSeconds || !days.length) {
    return { applied: 0, ttl_seconds: rawTtlSeconds };
  }

  // EXPIRE returns 0 for a key that does not exist, so days with no events
  // cost a command but change nothing.
  const responses = await upstashPipeline(
    days.map((day) => ["EXPIRE", buildRawStreamKey(day), rawTtlSeconds])
  );

  let applied = 0;
  for (const response of responses) {
    if (response && Number(response.result) === 1) {
      applied += 1;
    }
  }

  return { applied, ttl_seconds: rawTtlSeconds };
}

export function getUtcDay(offsetDays = 0) {
  const now = new Date();
  now.setUTCHours(0, 0, 0, 0);
  const d = new Date(now.getTime() + offsetDays * 86400000);
  return d.toISOString().slice(0, 10);
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

async function readRawEvents(streamKey, count) {
  const rows = [];
  let start = "-";

  while (true) {
    const [response] = await upstashPipeline([["XRANGE", streamKey, start, "+", "COUNT", count]]);
    const result = response && response.result;
    if (!Array.isArray(result) || !result.length) {
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

function buildDayAggregates(events, targetDay) {
  const counts = {};
  const uniqueSets = {};
  const newUserEntries = new Map();
  // Product-scoped newness is judged separately from event-scoped newness: a
  // long-standing user emitting a newly-introduced event is new to the event
  // but not to the product. Ingestion used to make this call inline; with
  // ingestion reduced to a raw append, the rebuild is the only place left that
  // can make it.
  const productNewUserEntries = new Map();

  for (const row of events) {
    const fields = extractEventFields(row, {});
    const error = validateEventFields(fields);
    if (error) {
      continue;
    }

    const plan = buildEventAggregationPlan(fields, {
      city: row.city || "Unknown",
      country: row.country || "Unknown",
    });
    if (plan.day !== targetDay) {
      continue;
    }

    for (const key of plan.countKeys) {
      counts[key] = (counts[key] || 0) + 1;
    }

    // The product-level ACTIVE set is what tells the stats API that
    // product-level tracking was live on a day. It is written for every event,
    // new user or not; without it the API silently falls back to a per-event
    // count that reports existing users as new.
    for (const key of [...plan.uniqueKeys, ...plan.productUniqueKeys, ...plan.migratedUserKeys]) {
      if (!uniqueSets[key]) {
        uniqueSets[key] = new Set();
      }
      uniqueSets[key].add(fields.anonId);
    }

    for (const context of plan.dayNewUserContexts) {
      if (!newUserEntries.has(context.firstSeenKey)) {
        newUserEntries.set(context.firstSeenKey, context);
      }
    }

    for (const context of plan.productNewUserContexts) {
      if (!productNewUserEntries.has(context.firstSeenKey)) {
        productNewUserEntries.set(context.firstSeenKey, context);
      }
    }
  }

  const unique = {};
  for (const [key, anonIds] of Object.entries(uniqueSets)) {
    unique[key] = Array.from(anonIds);
  }

  return {
    counts,
    unique,
    newUserEntries: Array.from(newUserEntries.values()),
    productNewUserEntries: Array.from(productNewUserEntries.values()),
  };
}

async function writeDayAggregates(aggregates, countTtlSeconds, uniqueTtlSeconds) {
  const commands = [];

  for (const [key, count] of Object.entries(aggregates.counts)) {
    commands.push(["SET", key, String(count)]);
    commands.push(["EXPIRE", key, countTtlSeconds]);
  }

  for (const [key, anonIds] of Object.entries(aggregates.unique)) {
    if (anonIds.length) {
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

async function writeDayNewUsers(entries, uniqueTtlSeconds) {
  if (!entries.length) {
    return { writes: 0, created: 0, corrected: 0 };
  }

  const firstSeenKeys = entries.map((entry) => entry.firstSeenKey);
  const existingValues = await getValues(firstSeenKeys);
  const commands = [];
  let created = 0;
  let corrected = 0;

  for (let i = 0; i < entries.length; i += 1) {
    const entry = entries[i];
    const day = extractDayFromTelemetryKey(entry.dayNewUsersKey);
    const existingDay = normalizeDay(String(existingValues[i] || "").trim());
    if (!day) {
      continue;
    }

    if (!existingDay) {
      commands.push(["SET", entry.firstSeenKey, day]);
      commands.push(["SADD", entry.dayNewUsersKey, entry.anonId]);
      if (uniqueTtlSeconds > 0) {
        commands.push(["EXPIRE", entry.dayNewUsersKey, uniqueTtlSeconds]);
      }
      created += 1;
      continue;
    }

    if (day < existingDay) {
      commands.push(["SET", entry.firstSeenKey, day]);
      commands.push(["SREM", replaceDayInTelemetryKey(entry.dayNewUsersKey, existingDay), entry.anonId]);
      commands.push(["SADD", entry.dayNewUsersKey, entry.anonId]);
      if (uniqueTtlSeconds > 0) {
        commands.push(["EXPIRE", entry.dayNewUsersKey, uniqueTtlSeconds]);
      }
      corrected += 1;
      continue;
    }

    if (existingDay === day) {
      commands.push(["SADD", entry.dayNewUsersKey, entry.anonId]);
      if (uniqueTtlSeconds > 0) {
        commands.push(["EXPIRE", entry.dayNewUsersKey, uniqueTtlSeconds]);
      }
      corrected += 1;
    }
  }

  let writes = 0;
  for (let i = 0; i < commands.length; i += 200) {
    const chunk = commands.slice(i, i + 200);
    await upstashPipeline(chunk);
    writes += chunk.length;
  }

  return { writes, created, corrected };
}

// Product-level newness, mirroring what ingestion used to do inline but batched:
// one MGET for the product records, one more for the seed records of whoever
// has none.
//
// Cold start is the hazard. An absent product record cannot by itself mean
// "new", because the key family is younger than the user base. Before claiming
// newness the user's earliest per-event sighting is consulted; a genuinely new
// user has none, so they still register correctly.
async function writeProductNewUsers(entries, uniqueTtlSeconds) {
  if (!entries.length) {
    return { writes: 0, created: 0, corrected: 0, seeded: 0 };
  }

  const existingValues = await getValues(entries.map((entry) => entry.firstSeenKey));

  const unseen = [];
  for (let i = 0; i < entries.length; i += 1) {
    if (!normalizeDay(String(existingValues[i] || "").trim())) {
      unseen.push(entries[i]);
    }
  }

  const seedKeys = [...new Set(unseen.flatMap((entry) => entry.seedFirstSeenKeys || []))];
  const seedValues = seedKeys.length ? await getValues(seedKeys) : [];
  const seedByKey = new Map(seedKeys.map((key, index) => [key, seedValues[index]]));

  const commands = [];
  let created = 0;
  let corrected = 0;
  let seeded = 0;

  for (let i = 0; i < entries.length; i += 1) {
    const entry = entries[i];
    const day = extractDayFromTelemetryKey(entry.dayNewUsersKey);
    if (!day) {
      continue;
    }

    const existingDay = normalizeDay(String(existingValues[i] || "").trim());

    if (!existingDay) {
      let earliest = "";
      for (const key of entry.seedFirstSeenKeys || []) {
        const candidate = normalizeDay(String(seedByKey.get(key) || "").trim());
        if (candidate && (!earliest || candidate < earliest)) {
          earliest = candidate;
        }
      }

      if (earliest && earliest < day) {
        // The user predates this key family. Record the truth and do not count
        // them as newly acquired.
        commands.push(["SET", entry.firstSeenKey, earliest]);
        seeded += 1;
        continue;
      }

      commands.push(["SET", entry.firstSeenKey, day]);
      commands.push(["SADD", entry.dayNewUsersKey, entry.anonId]);
      if (uniqueTtlSeconds > 0) {
        commands.push(["EXPIRE", entry.dayNewUsersKey, uniqueTtlSeconds]);
      }
      created += 1;
      continue;
    }

    if (day < existingDay) {
      commands.push(["SET", entry.firstSeenKey, day]);
      commands.push([
        "SREM",
        replaceDayInTelemetryKey(entry.dayNewUsersKey, existingDay),
        entry.anonId,
      ]);
      commands.push(["SADD", entry.dayNewUsersKey, entry.anonId]);
      if (uniqueTtlSeconds > 0) {
        commands.push(["EXPIRE", entry.dayNewUsersKey, uniqueTtlSeconds]);
      }
      corrected += 1;
      continue;
    }

    if (existingDay === day) {
      commands.push(["SADD", entry.dayNewUsersKey, entry.anonId]);
      if (uniqueTtlSeconds > 0) {
        commands.push(["EXPIRE", entry.dayNewUsersKey, uniqueTtlSeconds]);
      }
      corrected += 1;
    }
  }

  let writes = 0;
  for (let i = 0; i < commands.length; i += 200) {
    const chunk = commands.slice(i, i + 200);
    await upstashPipeline(chunk);
    writes += chunk.length;
  }

  return { writes, created, corrected, seeded };
}

function extractDayFromTelemetryKey(key) {
  const match = String(key || "").match(/telemetry:day:(\d{4}-\d{2}-\d{2}):/);
  return match ? match[1] : "";
}

function replaceDayInTelemetryKey(key, day) {
  return String(key || "").replace(/telemetry:day:\d{4}-\d{2}-\d{2}:/, `telemetry:day:${day}:`);
}

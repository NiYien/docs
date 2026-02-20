export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, max-age=0");

  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const requiredToken = String(process.env.TELEMETRY_MIGRATE_TOKEN || "").trim();
  if (!requiredToken) {
    return res.status(500).json({ error: "Missing TELEMETRY_MIGRATE_TOKEN" });
  }

  const providedToken = String(req.headers["x-migrate-token"] || "").trim();
  if (!providedToken || providedToken !== requiredToken) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : (req.body || {});
  if (!body || typeof body !== "object") {
    return res.status(400).json({ error: "Invalid JSON" });
  }

  const mode = String(body.mode || "legacy-snapshot-copy").trim();
  if (mode !== "legacy-snapshot-copy") {
    return res.status(400).json({ error: "Unsupported mode" });
  }

  const startDay = normalizeDay(String(body.start_day || body.day || "").trim());
  const endDay = normalizeDay(String(body.end_day || body.day || "").trim());
  if (!startDay || !endDay) {
    return res.status(400).json({ error: "start_day/end_day required (YYYY-MM-DD)" });
  }

  const dayList = buildDayRange(startDay, endDay);
  if (!dayList.length) {
    return res.status(400).json({ error: "Invalid day range" });
  }

  const dryRun = body.dry_run !== false;
  const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
  const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;

  try {
    const legacy = await collectLegacyUniqueKeys();

    if (dryRun) {
      return res.status(200).json({
        ok: true,
        dry_run: true,
        mode,
        warning: "此迁移为旧集合快照复制到指定日期，无法无损还原历史日分布。",
        days: dayList,
        legacy_key_counts: {
          all: legacy.all ? 1 : 0,
          city: legacy.city.length,
          brand: legacy.brand.length,
          model: legacy.model.length,
          country: legacy.country.length,
        },
        planned_write_sets: dayList.length * (1 + legacy.city.length + legacy.brand.length + legacy.model.length + legacy.country.length),
      });
    }

    const migrated = await migrateLegacySnapshotToDays(dayList, legacy, uniqueTtlSeconds);
    return res.status(200).json({
      ok: true,
      dry_run: false,
      mode,
      warning: "迁移采用旧集合快照复制，不代表历史真实日分布。",
      days: dayList,
      migrated,
    });
  } catch (error) {
    return res.status(500).json({ error: "Migration failed", detail: error.message || String(error) });
  }
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

async function collectLegacyUniqueKeys() {
  const city = await scanKeys("telemetry:unique:city:*");
  const brand = await scanKeys("telemetry:unique:brand:*");
  const model = await scanKeys("telemetry:unique:model:*");
  const country = await scanKeys("telemetry:unique:country:*");

  return {
    all: "telemetry:unique:all",
    city,
    brand,
    model,
    country,
  };
}

async function migrateLegacySnapshotToDays(dayList, legacy, uniqueTtlSeconds) {
  const scopes = ["city", "brand", "model", "country"];
  const chunkSize = 120;
  let writes = 0;

  for (const day of dayList) {
    const commands = [];
    commands.push(["SUNIONSTORE", `telemetry:day:${day}:unique:all`, legacy.all]);
    if (uniqueTtlSeconds > 0) {
      commands.push(["EXPIRE", `telemetry:day:${day}:unique:all`, uniqueTtlSeconds]);
    }

    for (const scope of scopes) {
      for (const legacyKey of legacy[scope]) {
        const suffix = legacyKey.slice(`telemetry:unique:${scope}:`.length);
        const targetKey = `telemetry:day:${day}:unique:${scope}:${suffix}`;
        commands.push(["SUNIONSTORE", targetKey, legacyKey]);
        if (uniqueTtlSeconds > 0) {
          commands.push(["EXPIRE", targetKey, uniqueTtlSeconds]);
        }
      }
    }

    for (let i = 0; i < commands.length; i += chunkSize) {
      const chunk = commands.slice(i, i + chunkSize);
      await upstashPipeline(chunk);
      writes += chunk.length;
    }
  }

  return {
    days: dayList.length,
    writes,
  };
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

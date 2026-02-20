export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : req.body;
  if (!body) {
    return res.status(400).json({ error: "Invalid JSON" });
  }

  const ip = getClientIp(req);
  const debugGeo = shouldLogGeo(req);
  if (debugGeo) {
    console.log("[telemetry] headers", {
      x_forwarded_for: req.headers["x-forwarded-for"],
      x_vercel_ip_city: req.headers["x-vercel-ip-city"],
      x_vercel_ip_country: req.headers["x-vercel-ip-country"],
      remote_address: req.socket?.remoteAddress || "",
      ip,
    });
  }
  const geo = await lookupGeo(req, ip);
  const city = geo.city || "Unknown";
  const country = geo.country || "Unknown";
  const ttlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
  const ttlSeconds = Math.max(ttlDays, 1) * 86400;
  const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
  const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;
  const weekTtlDays = parseInt(process.env.TELEMETRY_USER_TTL_DAYS || "120", 10);
  const weekTtlSeconds = Math.max(weekTtlDays, 1) * 86400;
  const dedupeTtlDays = parseInt(process.env.TELEMETRY_EVENT_ID_TTL_DAYS || "120", 10);
  const dedupeTtlSeconds = Math.max(dedupeTtlDays, 1) * 86400;
  const rawStreamEnabled =
    String(process.env.TELEMETRY_RAW_STREAM_ENABLED || "true").toLowerCase() !== "false";
  const rawTtlDays = parseInt(process.env.TELEMETRY_RAW_TTL_DAYS || "365", 10);
  const rawTtlSeconds = rawTtlDays > 0 ? rawTtlDays * 86400 : 0;

  const isBatch = Array.isArray(body.events);
  if (isBatch) {
    if (body.events.length === 0) {
      return res.status(400).json({ error: "Empty events" });
    }

    const fallbackAnonId = String(body.anon_id || "").trim();
    const items = body.events.map((item, index) => {
      const fields = extractEventFields(item, fallbackAnonId);
      const error = validateEventFields(fields);
      return { index, fields, error };
    });

    const invalid = items.find((item) => item.error);
    if (invalid) {
      return res.status(400).json({
        error: invalid.error,
        index: invalid.index,
      });
    }

    try {
      let processed = 0;
      let deduped = 0;
      for (const item of items) {
        const result = await processEvent(
          item.fields,
          {
            city,
            country,
            ttlSeconds,
            uniqueTtlSeconds,
            weekTtlSeconds,
            dedupeTtlSeconds,
            rawStreamEnabled,
            rawTtlSeconds,
          }
        );
        if (result.processed) {
          processed += 1;
        } else {
          deduped += 1;
        }
      }

      return res.status(200).json({
        ok: true,
        batch: true,
        processed,
        deduped,
        received: items.length,
        city,
        country,
      });
    } catch (error) {
      return res.status(500).json({
        error: "Storage error",
        detail: error.message || String(error),
        has_url: !!(process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL),
        has_token: !!(process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN),
      });
    }
  }

  const fields = extractEventFields(body, "");
  const error = validateEventFields(fields);
  if (error) {
    return res.status(400).json({ error });
  }

  try {
    const result = await processEvent(
      fields,
      {
        city,
        country,
        ttlSeconds,
        uniqueTtlSeconds,
        weekTtlSeconds,
        dedupeTtlSeconds,
        rawStreamEnabled,
        rawTtlSeconds,
      }
    );

    const eventDate = new Date(fields.eventTs * 1000);
    const iso = eventDate.toISOString();
    const day = iso.slice(0, 10);
    const hour = iso.slice(11, 13);

    return res.status(200).json({
      ok: true,
      day,
      hour,
      city,
      country,
      deduped: !result.processed,
      event: fields.event,
      app_version: fields.appVersion || undefined,
      os: fields.os || undefined,
    });
  } catch (error) {
    return res.status(500).json({
      error: "Storage error",
      detail: error.message || String(error),
      has_url: !!(process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL),
      has_token: !!(process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN),
    });
  }
}

function extractEventFields(payload, fallbackAnonId) {
  const event = String(payload?.event || "").trim();
  const appVersion = String(payload?.app_version || "").trim();
  const os = String(payload?.os || "").trim();
  const cameraBrand = String(payload?.camera_brand || "").trim() || "Other";
  const cameraModel = String(payload?.camera_model || "").trim() || "Unknown";
  const language = String(payload?.language || "").trim() || "Unknown";
  const anonId = String(payload?.anon_id || fallbackAnonId || "").trim();
  const eventTs = normalizeEventTimestamp(payload?.ts);
  const eventId = buildEventId(payload, {
    event,
    appVersion,
    os,
    cameraBrand,
    cameraModel,
    language,
    anonId,
    eventTs,
  });

  return {
    event,
    appVersion,
    os,
    cameraBrand,
    cameraModel,
    language,
    anonId,
    eventTs,
    eventId,
  };
}

function validateEventFields(fields) {
  if (!fields.event || fields.event !== "open") {
    return "Invalid event";
  }

  if (!fields.anonId || fields.anonId.length > 64) {
    return "Invalid anon_id";
  }

  if (!fields.eventId || fields.eventId.length > 128) {
    return "Invalid event_id";
  }

  if (!Number.isFinite(fields.eventTs) || fields.eventTs <= 0) {
    return "Invalid ts";
  }

  return "";
}

async function processEvent(fields, context) {
  const eventDate = new Date(fields.eventTs * 1000);
  const iso = eventDate.toISOString();
  const day = iso.slice(0, 10);
  const hour = iso.slice(11, 13);
  const weekKey = getIsoWeekKey(eventDate);
  const keyParts = {
    day,
    hour,
    city: normalizeKeyPart(context.city),
    country: normalizeKeyPart(context.country),
    brand: normalizeKeyPart(fields.cameraBrand),
    model: normalizeKeyPart(fields.cameraModel),
    language: normalizeKeyPart(fields.language),
    event: normalizeKeyPart(fields.event),
  };

  const keys = buildKeys(keyParts);
  const uniqueKeys = buildUniqueKeys({
    day: keyParts.day,
    city: keyParts.city,
    brand: keyParts.brand,
    model: keyParts.model,
    country: keyParts.country,
  });
  const weekUserKey = buildWeekUserKey(weekKey, fields.anonId);
  const dedupeKey = buildEventDedupeKey(day, fields.eventId);
  const rawStreamKey = buildRawStreamKey(day);
  const rawEvent = {
    event_id: fields.eventId,
    event: fields.event,
    anon_id: fields.anonId,
    app_version: fields.appVersion,
    os: fields.os,
    camera_brand: fields.cameraBrand,
    camera_model: fields.cameraModel,
    language: fields.language,
    city: context.city,
    country: context.country,
    ts: fields.eventTs,
    ingested_at: Math.floor(Date.now() / 1000),
  };

  return upstashWrite(
    keys,
    context.ttlSeconds,
    uniqueKeys,
    context.uniqueTtlSeconds,
    fields.anonId,
    weekUserKey,
    context.weekTtlSeconds,
    dedupeKey,
    context.dedupeTtlSeconds,
    rawStreamKey,
    rawEvent,
    context.rawStreamEnabled,
    context.rawTtlSeconds
  );
}

function normalizeEventTimestamp(tsValue) {
  if (tsValue === null || tsValue === undefined || tsValue === "") {
    return Math.floor(Date.now() / 1000);
  }

  const raw = Number(tsValue);
  if (!Number.isFinite(raw) || raw <= 0) {
    return Math.floor(Date.now() / 1000);
  }

  if (raw > 1e12) {
    return Math.floor(raw / 1000);
  }

  return Math.floor(raw);
}

function buildEventId(payload, fields) {
  const explicit = String(payload?.event_id || "").trim();
  if (explicit) {
    return explicit.slice(0, 128);
  }

  const raw = [
    fields.anonId,
    fields.event,
    fields.eventTs,
    fields.cameraBrand,
    fields.cameraModel,
    fields.appVersion,
    fields.os,
    fields.language,
  ].join("|");

  return stableHash(raw);
}

function stableHash(text) {
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }

  return `evt_${(hash >>> 0).toString(16).padStart(8, "0")}`;
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch (error) {
    return null;
  }
}

function getClientIp(req) {
  const cfConnectingIp = req.headers["cf-connecting-ip"];
  if (typeof cfConnectingIp === "string" && cfConnectingIp.length > 0) {
    return cfConnectingIp.trim();
  }

  const trueClientIp = req.headers["true-client-ip"];
  if (typeof trueClientIp === "string" && trueClientIp.length > 0) {
    return trueClientIp.trim();
  }

  const forwarded = req.headers["x-forwarded-for"]; 
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }

  return req.socket?.remoteAddress || "";
}

async function lookupGeo(req, ip) {
  if (shouldUseVercelGeo(req)) {
    const vercelGeo = getVercelGeo(req);
    if (vercelGeo.city || vercelGeo.country) {
      return vercelGeo;
    }
  }

  const token = process.env.IPINFO_TOKEN;
  if (!token || !ip) {
    return { city: "Unknown" };
  }

  const url = `https://ipinfo.io/${encodeURIComponent(ip)}?token=${encodeURIComponent(token)}`;
  const debugGeo = shouldLogGeo(req);

  try {
    const response = await fetch(url, { method: "GET" });
    if (!response.ok) {
      return { city: "Unknown" };
    }

    const data = await response.json();
    const city = typeof data.city === "string" ? data.city.trim() : "Unknown";
    const country = typeof data.country === "string" ? data.country.trim() : "Unknown";
    if (debugGeo) {
      console.log("[telemetry] ipinfo", { ip, city, country, url });
    }
    return {
      city: city || "Unknown",
      country: country || "Unknown",
    };
  } catch (error) {
    return { city: "Unknown", country: "Unknown" };
  }
}

function getVercelGeo(req) {
  let city = String(req.headers["x-vercel-ip-city"] || "").trim();
  let country = String(req.headers["x-vercel-ip-country"] || "").trim();

  try { city = decodeURIComponent(city); } catch (e) {}
  try { country = decodeURIComponent(country); } catch (e) {}

  const debugGeo = shouldLogGeo(req);
  if (debugGeo) {
    console.log("[telemetry] vercel-geo", { city, country });
  }

  return {
    city: city || "",
    country: country || "",
  };
}

function shouldUseVercelGeo(req) {
  const cfConnectingIp = req.headers["cf-connecting-ip"];
  if (typeof cfConnectingIp === "string" && cfConnectingIp.length > 0) {
    return false;
  }

  const trueClientIp = req.headers["true-client-ip"];
  if (typeof trueClientIp === "string" && trueClientIp.length > 0) {
    return false;
  }

  return true;
}

function shouldLogGeo(req) {
  const enabled = String(process.env.TELEMETRY_DEBUG_GEO || "").toLowerCase() === "true";
  if (!enabled) {
    return false;
  }

  return String(req.headers["x-telemetry-debug"] || "") === "1";
}

function normalizeKeyPart(value) {
  const trimmed = value.trim().slice(0, 64);
  if (!trimmed) {
    return "Unknown";
  }

  return encodeURIComponent(trimmed);
}

function buildKeys({ day, hour, city, country, brand, model, language, event }) {
  return [
    `telemetry:day:${day}:city:${city}:brand:${brand}:event:${event}`,
    `telemetry:day:${day}:city:${city}:event:${event}`,
    `telemetry:day:${day}:brand:${brand}:event:${event}`,
    `telemetry:day:${day}:model:${model}:event:${event}`,
    `telemetry:day:${day}:lang:${language}:event:${event}`,
    `telemetry:day:${day}:country:${country}:event:${event}`,
    `telemetry:day:${day}:event:${event}`,
    `telemetry:day:${day}:hour:${hour}:event:${event}`,
  ];
}

function buildUniqueKeys({ day, city, brand, model, country }) {
  return [
    "telemetry:unique:all",
    `telemetry:unique:city:${city}`,
    `telemetry:unique:brand:${brand}`,
    `telemetry:unique:model:${model}`,
    `telemetry:unique:country:${country}`,
    `telemetry:day:${day}:unique:all`,
    `telemetry:day:${day}:unique:city:${city}`,
    `telemetry:day:${day}:unique:brand:${brand}`,
    `telemetry:day:${day}:unique:model:${model}`,
    `telemetry:day:${day}:unique:country:${country}`,
  ];
}

function buildWeekUserKey(weekKey, anonId) {
  return `telemetry:week:${weekKey}:user:${anonId}`;
}

function buildEventDedupeKey(day, eventId) {
  const normalized = encodeURIComponent(String(eventId || "").trim().slice(0, 128) || "unknown");
  return `telemetry:event:processed:${day}:${normalized}`;
}

function buildRawStreamKey(day) {
  return `telemetry:raw:day:${day}`;
}

function getIsoWeekKey(date) {
  const temp = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = temp.getUTCDay() || 7;
  temp.setUTCDate(temp.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(temp.getUTCFullYear(), 0, 1));
  const week = Math.ceil(((temp - yearStart) / 86400000 + 1) / 7);
  const year = temp.getUTCFullYear();
  return `${year}-W${String(week).padStart(2, "0")}`;
}

async function upstashWrite(
  keys,
  ttlSeconds,
  uniqueKeys,
  uniqueTtlSeconds,
  anonId,
  weekUserKey,
  weekTtlSeconds,
  dedupeKey,
  dedupeTtlSeconds,
  rawStreamKey,
  rawEvent,
  rawStreamEnabled,
  rawTtlSeconds
) {
  const dedupeResponse = await upstashCommand(["SET", dedupeKey, "1", "EX", dedupeTtlSeconds, "NX"]);
  const dedupeApplied = dedupeResponse && dedupeResponse.result === "OK";
  if (!dedupeApplied) {
    return { processed: false };
  }

  const pipeline = [];
  for (const key of keys) {
    pipeline.push(["INCR", key]);
    pipeline.push(["EXPIRE", key, ttlSeconds]);
  }

  for (const key of uniqueKeys) {
    pipeline.push(["SADD", key, anonId]);
    if (uniqueTtlSeconds > 0) {
      pipeline.push(["EXPIRE", key, uniqueTtlSeconds]);
    }
  }

  pipeline.push(["INCR", weekUserKey]);
  pipeline.push(["EXPIRE", weekUserKey, weekTtlSeconds]);

  if (rawStreamEnabled) {
    const rawFieldPairs = [];
    for (const [field, value] of Object.entries(rawEvent)) {
      rawFieldPairs.push(field, value === undefined || value === null ? "" : String(value));
    }
    pipeline.push(["XADD", rawStreamKey, "*", ...rawFieldPairs]);
    if (rawTtlSeconds > 0) {
      pipeline.push(["EXPIRE", rawStreamKey, rawTtlSeconds]);
    }
  }

  await upstashPipeline(pipeline);

  return { processed: true };
}

async function upstashCommand(command) {
  const responses = await upstashPipeline([command]);
  return responses[0] || null;
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
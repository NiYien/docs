export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : req.body;
  if (!body) {
    return res.status(400).json({ error: "Invalid JSON" });
  }
  
  const event = String(body.event || "").trim();
  const appVersion = String(body.app_version || "").trim();
  const os = String(body.os || "").trim();
  const cameraBrand = String(body.camera_brand || "").trim() || "Other";
  const cameraModel = String(body.camera_model || "").trim() || "Unknown";
  const language = String(body.language || "").trim() || "Unknown";
  const anonId = String(body.anon_id || "").trim();

  if (!event || event !== "open") {
    return res.status(400).json({ error: "Invalid event" });
  }

  if (!anonId || anonId.length > 64) {
    return res.status(400).json({ error: "Invalid anon_id" });
  }

  const now = new Date();
  const iso = now.toISOString();
  const day = iso.slice(0, 10);
  const hour = iso.slice(11, 13);
  const ip = getClientIp(req);
  const geo = await lookupGeo(req, ip);
  const city = geo.city || "Unknown";
  const country = geo.country || "Unknown";
  const weekKey = getIsoWeekKey(now);

  const keyParts = {
    day,
    hour,
    city: normalizeKeyPart(city),
    country: normalizeKeyPart(country),
    brand: normalizeKeyPart(cameraBrand),
    model: normalizeKeyPart(cameraModel),
    language: normalizeKeyPart(language),
    event: normalizeKeyPart(event),
  };

  const keys = buildKeys(keyParts);
  const ttlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
  const ttlSeconds = Math.max(ttlDays, 1) * 86400;
  const uniqueKeys = buildUniqueKeys(keyParts.city);
  const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
  const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;
  const weekUserKey = buildWeekUserKey(weekKey, anonId);
  const weekTtlDays = parseInt(process.env.TELEMETRY_USER_TTL_DAYS || "120", 10);
  const weekTtlSeconds = Math.max(weekTtlDays, 1) * 86400;

  try {
    await upstashWrite(
      keys,
      ttlSeconds,
      uniqueKeys,
      uniqueTtlSeconds,
      anonId,
      weekUserKey,
      weekTtlSeconds
    );
  } catch (error) {
    return res.status(500).json({
      error: "Storage error",
      detail: error.message || String(error),
      has_url: !!(process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL),
      has_token: !!(process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN),
    });
  }

  return res.status(200).json({
    ok: true,
    day,
    hour,
    city,
    country,
    event,
    app_version: appVersion || undefined,
    os: os || undefined,
  });
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch (error) {
    return null;
  }
}

function getClientIp(req) {
  const forwarded = req.headers["x-forwarded-for"]; 
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }

  return req.socket?.remoteAddress || "";
}

async function lookupGeo(req, ip) {
  const vercelGeo = getVercelGeo(req);
  if (vercelGeo.city || vercelGeo.country) {
    return vercelGeo;
  }

  const token = process.env.IPINFO_TOKEN;
  if (!token || !ip) {
    return { city: "Unknown" };
  }

  const url = `https://ipinfo.io/${encodeURIComponent(ip)}?token=${encodeURIComponent(token)}`;

  try {
    const response = await fetch(url, { method: "GET" });
    if (!response.ok) {
      return { city: "Unknown" };
    }

    const data = await response.json();
    const city = typeof data.city === "string" ? data.city.trim() : "Unknown";
    const country = typeof data.country === "string" ? data.country.trim() : "Unknown";
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

  return {
    city: city || "",
    country: country || "",
  };
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

function buildUniqueKeys(city) {
  return [
    "telemetry:unique:all",
    `telemetry:unique:city:${city}`,
  ];
}

function buildWeekUserKey(weekKey, anonId) {
  return `telemetry:week:${weekKey}:user:${anonId}`;
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
  weekTtlSeconds
) {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!url || !token) {
    throw new Error("Missing Upstash config");
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

  const response = await fetch(`${url}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(pipeline),
  });

  if (!response.ok) {
    throw new Error("Upstash pipeline error");
  }
}

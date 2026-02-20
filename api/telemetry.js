export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : req.body;
  if (!body) {
    return res.status(400).json({ error: "Invalid JSON" });
  }
  
  const now = new Date();
  const iso = now.toISOString();
  const day = iso.slice(0, 10);
  const hour = iso.slice(11, 13);
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
  const weekKey = getIsoWeekKey(now);
  const ttlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
  const ttlSeconds = Math.max(ttlDays, 1) * 86400;
  const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "0", 10);
  const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;
  const weekTtlDays = parseInt(process.env.TELEMETRY_USER_TTL_DAYS || "120", 10);
  const weekTtlSeconds = Math.max(weekTtlDays, 1) * 86400;

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
      for (const item of items) {
        await processEvent(
          item.fields,
          {
            day,
            hour,
            city,
            country,
            weekKey,
            ttlSeconds,
            uniqueTtlSeconds,
            weekTtlSeconds,
          }
        );
      }
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
      batch: true,
      processed: items.length,
      day,
      hour,
      city,
      country,
    });
  }

  const fields = extractEventFields(body, "");
  const error = validateEventFields(fields);
  if (error) {
    return res.status(400).json({ error });
  }

  try {
    await processEvent(
      fields,
      {
        day,
        hour,
        city,
        country,
        weekKey,
        ttlSeconds,
        uniqueTtlSeconds,
        weekTtlSeconds,
      }
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
    event: fields.event,
    app_version: fields.appVersion || undefined,
    os: fields.os || undefined,
  });
}

function extractEventFields(payload, fallbackAnonId) {
  const event = String(payload?.event || "").trim();
  const appVersion = String(payload?.app_version || "").trim();
  const os = String(payload?.os || "").trim();
  const cameraBrand = String(payload?.camera_brand || "").trim() || "Other";
  const cameraModel = String(payload?.camera_model || "").trim() || "Unknown";
  const language = String(payload?.language || "").trim() || "Unknown";
  const anonId = String(payload?.anon_id || fallbackAnonId || "").trim();

  return {
    event,
    appVersion,
    os,
    cameraBrand,
    cameraModel,
    language,
    anonId,
  };
}

function validateEventFields(fields) {
  if (!fields.event || fields.event !== "open") {
    return "Invalid event";
  }

  if (!fields.anonId || fields.anonId.length > 64) {
    return "Invalid anon_id";
  }

  return "";
}

async function processEvent(fields, context) {
  const keyParts = {
    day: context.day,
    hour: context.hour,
    city: normalizeKeyPart(context.city),
    country: normalizeKeyPart(context.country),
    brand: normalizeKeyPart(fields.cameraBrand),
    model: normalizeKeyPart(fields.cameraModel),
    language: normalizeKeyPart(fields.language),
    event: normalizeKeyPart(fields.event),
  };

  const { dailyKeys, totalKeys } = buildKeys(keyParts);
  const uniqueKeys = buildUniqueKeys(keyParts.city);
  const weekUserKey = buildWeekUserKey(context.weekKey, fields.anonId);

  await upstashWrite(
    dailyKeys,
    totalKeys,
    context.ttlSeconds,
    uniqueKeys,
    context.uniqueTtlSeconds,
    fields.anonId,
    weekUserKey,
    context.weekTtlSeconds
  );
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
  // 1. ipinfo (Primary)
  const token = process.env.IPINFO_TOKEN;
  if (token && ip) {
    try {
      const url = `https://ipinfo.io/${encodeURIComponent(ip)}?token=${encodeURIComponent(token)}`;
      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        return {
          city: data.city || "Unknown",
          country: data.country || "Unknown",
        };
      }
      console.warn(`[telemetry] ipinfo failed: ${res.status}`);
    } catch (err) {
      console.error("[telemetry] ipinfo error", err);
    }
  }

  // 2. Vercel / Cloudflare Headers (Backup)
  if (shouldUseVercelGeo(req)) {
    const vercelGeo = getVercelGeo(req);
    if (vercelGeo.city || vercelGeo.country) {
      return vercelGeo;
    }
  }

  return { city: "Unknown", country: "Unknown" };
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
  const dailyKeys = [
    `telemetry:day:${day}:city:${city}:brand:${brand}:event:${event}`,
    `telemetry:day:${day}:city:${city}:model:${model}:event:${event}`,
    `telemetry:day:${day}:city:${city}:event:${event}`,
    `telemetry:day:${day}:brand:${brand}:event:${event}`,
    `telemetry:day:${day}:model:${model}:event:${event}`,
    `telemetry:day:${day}:lang:${language}:event:${event}`,
    `telemetry:day:${day}:country:${country}:event:${event}`,
    `telemetry:day:${day}:event:${event}`,
    `telemetry:day:${day}:hour:${hour}:event:${event}`,
  ];

  const totalKeys = [
    `telemetry:total:city:${city}:event:${event}`,
    `telemetry:total:brand:${brand}:event:${event}`,
    `telemetry:total:model:${model}:event:${event}`,
    `telemetry:total:city:${city}:model:${model}:event:${event}`,
    `telemetry:total:country:${country}:event:${event}`,
    `telemetry:total:lang:${language}:event:${event}`,
    `telemetry:total:event:${event}`,
  ];

  return { dailyKeys, totalKeys };
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
  dailyKeys,
  totalKeys,
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
  // Daily Keys (expire)
  for (const key of dailyKeys) {
    pipeline.push(["INCR", key]);
    pipeline.push(["EXPIRE", key, ttlSeconds]);
  }

  // Total Keys (no expire)
  if (totalKeys) {
    for (const key of totalKeys) {
      pipeline.push(["INCR", key]);
    }
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

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

  const keyParts = {
    day,
    hour,
    city: normalizeKeyPart(city),
    brand: normalizeKeyPart(cameraBrand),
    event: normalizeKeyPart(event),
  };

  const keys = buildKeys(keyParts);
  const ttlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
  const ttlSeconds = Math.max(ttlDays, 1) * 86400;

  try {
    await upstashIncr(keys, ttlSeconds);
  } catch (error) {
    return res.status(500).json({ error: "Storage error" });
  }

  return res.status(200).json({
    ok: true,
    day,
    hour,
    city,
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
  if (vercelGeo.city) {
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
    return { city: city || "Unknown" };
  } catch (error) {
    return { city: "Unknown" };
  }
}

function getVercelGeo(req) {
  const city = String(req.headers["x-vercel-ip-city"] || "").trim();
  const country = String(req.headers["x-vercel-ip-country"] || "").trim();

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

function buildKeys({ day, hour, city, brand, event }) {
  return [
    `telemetry:day:${day}:city:${city}:brand:${brand}:event:${event}`,
    `telemetry:day:${day}:city:${city}:event:${event}`,
    `telemetry:day:${day}:brand:${brand}:event:${event}`,
    `telemetry:day:${day}:event:${event}`,
    `telemetry:day:${day}:hour:${hour}:event:${event}`,
  ];
}

async function upstashIncr(keys, ttlSeconds) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!url || !token) {
    throw new Error("Missing Upstash config");
  }

  const pipeline = [];
  for (const key of keys) {
    pipeline.push(["INCR", key]);
    pipeline.push(["EXPIRE", key, ttlSeconds]);
  }

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

const UNKNOWN_CITY = "Unknown";
const UNKNOWN_COUNTRY = "Unknown";
const DEFAULT_COUNTRY = "US";
const MAX_CACHE_SIZE = 512;

const GEO_CACHE = globalThis.__NIYIEN_GEO_CACHE || new Map();
globalThis.__NIYIEN_GEO_CACHE = GEO_CACHE;
const GEO_PENDING = globalThis.__NIYIEN_GEO_PENDING || new Map();
globalThis.__NIYIEN_GEO_PENDING = GEO_PENDING;

export function shouldLogGeo(req) {
  const enabled = String(process.env.TELEMETRY_DEBUG_GEO || "").toLowerCase() === "true";
  if (!enabled) {
    return false;
  }
  return String(req?.headers?.["x-telemetry-debug"] || "") === "1";
}

export function getClientIp(req) {
  const cfConnectingIp = req?.headers?.["cf-connecting-ip"];
  if (typeof cfConnectingIp === "string" && cfConnectingIp.trim()) {
    return cfConnectingIp.trim();
  }

  const trueClientIp = req?.headers?.["true-client-ip"];
  if (typeof trueClientIp === "string" && trueClientIp.trim()) {
    return trueClientIp.trim();
  }

  const forwarded = req?.headers?.["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.trim()) {
    return forwarded.split(",")[0].trim();
  }

  return req?.socket?.remoteAddress || "";
}

export function getVercelGeo(req) {
  let city = String(req?.headers?.["x-vercel-ip-city"] || "").trim();
  let country = String(req?.headers?.["x-vercel-ip-country"] || "").trim();

  try {
    city = decodeURIComponent(city);
  } catch (_) {}
  try {
    country = decodeURIComponent(country);
  } catch (_) {}

  if (shouldLogGeo(req)) {
    console.log("[geo] vercel", { city, country });
  }

  return {
    city: city || "",
    country: country || "",
  };
}

export async function getCountry(req, fallbackCountry = DEFAULT_COUNTRY) {
  const geo = await getGeo(req, { fallbackCountry });
  return geo.country || fallbackCountry;
}

export async function getGeo(req, options = {}) {
  const fallbackCountry = normalizeCountry(options.fallbackCountry || DEFAULT_COUNTRY) || DEFAULT_COUNTRY;
  const queryCountry = normalizeCountry(req?.query?.country);
  if (queryCountry) {
    return { city: UNKNOWN_CITY, country: queryCountry, source: "query" };
  }

  const headerCountry = normalizeCountry(req?.headers?.["x-country-code"]);
  if (headerCountry) {
    return { city: UNKNOWN_CITY, country: headerCountry, source: "header" };
  }

  const ip = getClientIp(req);
  const ipInfoGeo = await lookupGeoByIpInfo(req, ip);
  if (ipInfoGeo.country || ipInfoGeo.city) {
    return {
      city: ipInfoGeo.city || UNKNOWN_CITY,
      country: ipInfoGeo.country || fallbackCountry,
      source: ipInfoGeo.source,
    };
  }

  const vercelGeo = getVercelGeo(req);
  if (vercelGeo.city || vercelGeo.country) {
    return {
      city: normalizeCity(vercelGeo.city) || UNKNOWN_CITY,
      country: normalizeCountry(vercelGeo.country) || fallbackCountry,
      source: "vercel",
    };
  }

  return {
    city: UNKNOWN_CITY,
    country: fallbackCountry || UNKNOWN_COUNTRY,
    source: "fallback",
  };
}

async function lookupGeoByIpInfo(req, ip) {
  const token = String(process.env.IPINFO_TOKEN || "").trim();
  if (!token || !ip) {
    return { city: "", country: "", source: "none" };
  }

  const cached = readGeoCache(ip);
  if (cached) {
    if (shouldLogGeo(req)) {
      console.log("[geo] ipinfo-cache", { ip, city: cached.city, country: cached.country });
    }
    return { ...cached, source: "ipinfo_cache" };
  }

  let pending = GEO_PENDING.get(ip);
  if (!pending) {
    pending = fetchGeoByIpInfo(ip, token)
      .then((result) => {
        if (result.city || result.country) {
          writeGeoCache(ip, result);
        }
        return result;
      })
      .finally(() => {
        GEO_PENDING.delete(ip);
      });
    GEO_PENDING.set(ip, pending);
  } else if (shouldLogGeo(req)) {
    console.log("[geo] ipinfo-pending", { ip });
  }

  const result = await pending;
  if (shouldLogGeo(req)) {
    console.log("[geo] ipinfo", {
      ip,
      city: result.city,
      country: result.country,
      url: result.url,
      source: result.source,
    });
  }
  return result;
}

async function fetchGeoByIpInfo(ip, token) {
  const url = `https://ipinfo.io/${encodeURIComponent(ip)}?token=${encodeURIComponent(token)}`;
  try {
    const response = await fetch(url, { method: "GET" });
    if (!response.ok) {
      return { city: "", country: "", source: "ipinfo_error", url };
    }

    const data = await response.json();
    return {
      city: normalizeCity(data?.city),
      country: normalizeCountry(data?.country),
      source: "ipinfo",
      url,
    };
  } catch (_) {
    return { city: "", country: "", source: "ipinfo_error", url };
  }
}

function normalizeCountry(value) {
  return String(value || "").trim().toUpperCase();
}

function normalizeCity(value) {
  return String(value || "").trim();
}

function geoCacheTtlMs() {
  const raw = parseInt(
    process.env.IPINFO_GEO_CACHE_TTL_SECONDS || process.env.GEO_IPINFO_CACHE_TTL_SECONDS || "21600",
    10
  );
  const seconds = Number.isFinite(raw) && raw > 0 ? raw : 21600;
  return seconds * 1000;
}

function readGeoCache(ip) {
  const key = String(ip || "").trim();
  if (!key) {
    return null;
  }
  const entry = GEO_CACHE.get(key);
  if (!entry) {
    return null;
  }
  if (entry.expiresAt <= Date.now()) {
    GEO_CACHE.delete(key);
    return null;
  }
  return entry.value;
}

function writeGeoCache(ip, value) {
  const key = String(ip || "").trim();
  if (!key) {
    return;
  }
  if (GEO_CACHE.size >= MAX_CACHE_SIZE) {
    const oldestKey = GEO_CACHE.keys().next().value;
    if (oldestKey) {
      GEO_CACHE.delete(oldestKey);
    }
  }
  GEO_CACHE.set(key, {
    value: {
      city: normalizeCity(value?.city),
      country: normalizeCountry(value?.country),
    },
    expiresAt: Date.now() + geoCacheTtlMs(),
  });
}

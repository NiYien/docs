export default async function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const requiredToken = process.env.TELEMETRY_STATS_TOKEN;
  if (requiredToken) {
    const provided = String(req.headers["x-stats-token"] || req.query.token || "").trim();
    if (!provided || provided !== requiredToken) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }

  const dayQuery = String(req.query.day || "").trim();
  const daysQuery = String(req.query.days || "7").trim();
  const days = clampNumber(parseInt(daysQuery, 10) || 7, 1, 30);

  const dayList = dayQuery ? [normalizeDay(dayQuery)] : buildDayList(days);
  if (dayList.some((d) => !d)) {
    return res.status(400).json({ error: "Invalid day" });
  }

  try {
    const results = await collectStats(dayList);
    return res.status(200).json({ ok: true, days: dayList, ...results });
  } catch (error) {
    return res.status(500).json({ error: "Stats error" });
  }
}

function clampNumber(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function normalizeDay(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return "";
  }

  return value;
}

function buildDayList(days) {
  const list = [];
  const now = new Date();
  now.setUTCHours(0, 0, 0, 0);

  for (let i = 0; i < days; i += 1) {
    const d = new Date(now.getTime() - i * 86400000);
    list.push(d.toISOString().slice(0, 10));
  }

  return list;
}

async function collectStats(dayList) {
  const cityTotals = {};
  const brandTotals = {};
  const cityBrandTotals = {};
  const hourTotals = Array.from({ length: 24 }, () => 0);

  for (const day of dayList) {
    const cityBrandPattern = `telemetry:day:${day}:city:*:brand:*:event:open`;
    const hourPattern = `telemetry:day:${day}:hour:*:event:open`;

    const cityBrandKeys = await scanKeys(cityBrandPattern);
    const hourKeys = await scanKeys(hourPattern);

    await accumulateCityBrand(cityBrandKeys, cityTotals, brandTotals, cityBrandTotals);
    await accumulateHours(hourKeys, hourTotals);
  }

  const uniqueTotals = await collectUniqueTotals(cityTotals);

  return {
    city_totals: cityTotals,
    brand_totals: brandTotals,
    city_brand_totals: cityBrandTotals,
    hour_totals: hourTotals,
    city_unique_totals: uniqueTotals.cityUniqueTotals,
    global_unique_total: uniqueTotals.globalUniqueTotal,
  };
}

async function accumulateCityBrand(keys, cityTotals, brandTotals, cityBrandTotals) {
  const values = await getValues(keys);

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const parsed = parseCityBrandKey(key);
    if (!parsed) {
      continue;
    }

    cityTotals[parsed.city] = (cityTotals[parsed.city] || 0) + count;
    brandTotals[parsed.brand] = (brandTotals[parsed.brand] || 0) + count;
    if (!cityBrandTotals[parsed.city]) {
      cityBrandTotals[parsed.city] = {};
    }
    cityBrandTotals[parsed.city][parsed.brand] =
      (cityBrandTotals[parsed.city][parsed.brand] || 0) + count;
  }
}

async function accumulateHours(keys, hourTotals) {
  const values = await getValues(keys);

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const hour = parseHourKey(key);
    if (hour === null) {
      continue;
    }

    hourTotals[hour] += count;
  }
}

function parseCityBrandKey(key) {
  const parts = key.split(":");
  const cityIndex = parts.indexOf("city");
  const brandIndex = parts.indexOf("brand");

  if (cityIndex < 0 || brandIndex < 0) {
    return null;
  }

  const city = decodeKeyPart(parts[cityIndex + 1]);
  const brand = decodeKeyPart(parts[brandIndex + 1]);

  if (!city || !brand) {
    return null;
  }

  return { city, brand };
}

function parseHourKey(key) {
  const parts = key.split(":");
  const hourIndex = parts.indexOf("hour");

  if (hourIndex < 0) {
    return null;
  }

  const hourValue = parts[hourIndex + 1];
  const hour = parseInt(hourValue, 10);

  if (Number.isNaN(hour) || hour < 0 || hour > 23) {
    return null;
  }

  return hour;
}

function decodeKeyPart(value) {
  try {
    return decodeURIComponent(value || "");
  } catch (error) {
    return "";
  }
}

function encodeKeyPart(value) {
  const text = String(value || "").trim().slice(0, 64);
  if (!text) {
    return "Unknown";
  }

  return encodeURIComponent(text);
}

async function collectUniqueTotals(cityTotals) {
  const cityNames = Object.keys(cityTotals);
  const cityUniqueTotals = await getCityUniqueTotals(cityNames);
  const globalUniqueTotal = await getGlobalUniqueTotal();

  return { cityUniqueTotals, globalUniqueTotal };
}

async function getGlobalUniqueTotal() {
  const [response] = await upstashPipeline([["SCARD", "telemetry:unique:all"]]);
  const total = response && response.result ? response.result : 0;
  return parseInt(total, 10) || 0;
}

async function getCityUniqueTotals(cityNames) {
  if (!cityNames.length) {
    return {};
  }

  const result = {};
  const chunkSize = 200;

  for (let i = 0; i < cityNames.length; i += chunkSize) {
    const chunk = cityNames.slice(i, i + chunkSize);
    const commands = chunk.map((city) => {
      const key = `telemetry:unique:city:${encodeKeyPart(city)}`;
      return ["SCARD", key];
    });

    const responses = await upstashPipeline(commands);
    for (let j = 0; j < chunk.length; j += 1) {
      const response = responses[j];
      const total = response && response.result ? response.result : 0;
      result[chunk[j]] = parseInt(total, 10) || 0;
    }
  }

  return result;
}

async function scanKeys(pattern) {
  let cursor = "0";
  const keys = [];

  for (let i = 0; i < 50; i += 1) {
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

async function getValues(keys) {
  if (!keys.length) {
    return [];
  }

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

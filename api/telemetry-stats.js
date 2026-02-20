export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, max-age=0");
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const requiredToken = process.env.TELEMETRY_STATS_TOKEN;
  const provided = String(req.headers["x-stats-token"] || "").trim();
  if (requiredToken) {
    if (!provided || provided !== requiredToken) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }

  const dayQuery = String(req.query.day || "").trim();
  const daysQuery = String(req.query.days || "7").trim();
  const weekQuery = String(req.query.week || "").trim();
  const days = clampNumber(parseInt(daysQuery, 10) || 7, 1, 30);

  const dayList = dayQuery ? [normalizeDay(dayQuery)] : buildDayList(days);
  if (dayList.some((d) => !d)) {
    return res.status(400).json({ error: "Invalid day" });
  }

  try {
    const results = await collectStats(dayList, weekQuery);
    const breakpoint = buildBreakpointMeta(dayList);
    return res.status(200).json({ ok: true, days: dayList, auth_required: !!requiredToken, breakpoint, ...results });
  } catch (error) {
    return res.status(500).json({ error: "Stats error" });
  }
}

function buildBreakpointMeta(dayList) {
  const day = normalizeDay(String(process.env.TELEMETRY_BREAKPOINT_DAY || "").trim());
  if (!day) {
    return null;
  }

  let hasBefore = false;
  let hasAfter = false;
  for (const item of dayList) {
    if (!item) {
      continue;
    }
    if (item < day) {
      hasBefore = true;
    } else {
      hasAfter = true;
    }
  }

  const crosses = hasBefore && hasAfter;
  const note = String(
    process.env.TELEMETRY_BREAKPOINT_NOTE || "断点日前后口径不同，建议分段查看，不做同比。"
  );

  return {
    day,
    crosses,
    all_before: hasBefore && !hasAfter,
    all_after: hasAfter && !hasBefore,
    note,
  };
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

async function collectStats(dayList, weekQuery) {
  const cityTotals = {};
  const brandTotals = {};
  const modelTotals = {};
  const languageTotals = {};
  const countryTotals = {};
  const cityBrandTotals = {};
  const hourTotals = Array.from({ length: 24 }, () => 0);

  for (const day of dayList) {
    const cityBrandPattern = `telemetry:day:${day}:city:*:brand:*:event:open`;
    const modelPattern = `telemetry:day:${day}:model:*:event:open`;
    const languagePattern = `telemetry:day:${day}:lang:*:event:open`;
    const countryPattern = `telemetry:day:${day}:country:*:event:open`;
    const hourPattern = `telemetry:day:${day}:hour:*:event:open`;

    const cityBrandKeys = await scanKeys(cityBrandPattern);
    const modelKeys = await scanKeys(modelPattern);
    const languageKeys = await scanKeys(languagePattern);
    const countryKeys = await scanKeys(countryPattern);
    const hourKeys = await scanKeys(hourPattern);

    await accumulateCityBrand(cityBrandKeys, cityTotals, brandTotals, cityBrandTotals);
    await accumulateSingleTotals(modelKeys, modelTotals, "model");
    await accumulateSingleTotals(languageKeys, languageTotals, "lang");
    await accumulateSingleTotals(countryKeys, countryTotals, "country");
    await accumulateHours(hourKeys, hourTotals);
  }

  const uniqueTotals = await collectUniqueTotals({
    dayList,
    cityTotals,
    brandTotals,
    modelTotals,
    countryTotals,
  });
  const weeklyUsage = await collectWeeklyUsage(weekQuery);

  return {
    city_totals: cityTotals,
    brand_totals: brandTotals,
    model_totals: modelTotals,
    language_totals: languageTotals,
    country_totals: countryTotals,
    city_brand_totals: cityBrandTotals,
    hour_totals: hourTotals,
    city_unique_totals: uniqueTotals.cityUniqueTotals,
    brand_unique_totals: uniqueTotals.brandUniqueTotals,
    model_unique_totals: uniqueTotals.modelUniqueTotals,
    country_unique_totals: uniqueTotals.countryUniqueTotals,
    global_unique_total: uniqueTotals.globalUniqueTotal,
    unique_source: uniqueTotals.source,
    missing_unique_data: uniqueTotals.missingUniqueData,
    unique_observed_only: true,
    weekly_usage: weeklyUsage,
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

async function accumulateSingleTotals(keys, totals, label) {
  if (!keys.length) {
    return;
  }

  const values = await getValues(keys);
  for (let i = 0; i < keys.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const item = parseSingleKey(keys[i], label);
    if (!item) {
      continue;
    }

    totals[item] = (totals[item] || 0) + count;
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

function parseSingleKey(key, label) {
  const parts = key.split(":");
  const index = parts.indexOf(label);

  if (index < 0) {
    return "";
  }

  return decodeKeyPart(parts[index + 1]);
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

async function collectUniqueTotals({ dayList, cityTotals, brandTotals, modelTotals, countryTotals }) {
  const cityNames = Object.keys(cityTotals);
  const brandNames = Object.keys(brandTotals);
  const modelNames = Object.keys(modelTotals);
  const countryNames = Object.keys(countryTotals);

  let cityUniqueTotals = await getScopedUniqueTotals(dayList, cityNames, "city");
  let brandUniqueTotals = await getScopedUniqueTotals(dayList, brandNames, "brand");
  let modelUniqueTotals = await getScopedUniqueTotals(dayList, modelNames, "model");
  let countryUniqueTotals = await getScopedUniqueTotals(dayList, countryNames, "country");
  let globalUniqueTotal = await getGlobalUniqueTotal(dayList);
  let source = "day-observed";

  const hasOpenTotals =
    hasAnyTotals(cityTotals) ||
    hasAnyTotals(brandTotals) ||
    hasAnyTotals(modelTotals) ||
    hasAnyTotals(countryTotals);
  const missingUniqueData = hasOpenTotals && globalUniqueTotal === 0;

  if (missingUniqueData) {
    source = "day-observed-missing";
  }

  return {
    cityUniqueTotals,
    brandUniqueTotals,
    modelUniqueTotals,
    countryUniqueTotals,
    globalUniqueTotal,
    source,
    missingUniqueData,
  };
}

function hasAnyTotals(totals) {
  return Object.values(totals || {}).some((value) => (parseInt(value || "0", 10) || 0) > 0);
}

async function getGlobalUniqueTotal(dayList) {
  const keys = dayList.map((day) => `telemetry:day:${day}:unique:all`);
  return getUnionCardinality(keys);
}

async function getScopedUniqueTotals(dayList, names, scope) {
  if (!names.length) {
    return {};
  }

  const result = {};
  for (const name of names) {
    const keys = dayList.map((day) => `telemetry:day:${day}:unique:${scope}:${encodeKeyPart(name)}`);
    result[name] = await getUnionCardinality(keys);
  }

  return result;
}

async function getUnionCardinality(keys) {
  const list = keys.filter((key) => !!key);
  if (!list.length) {
    return 0;
  }

  if (list.length === 1) {
    const [response] = await upstashPipeline([["SCARD", list[0]]]);
    const total = response && response.result ? response.result : 0;
    return parseInt(total, 10) || 0;
  }

  const tempKey = `telemetry:tmp:stats:union:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`;
  const responses = await upstashPipeline([
    ["SUNIONSTORE", tempKey, ...list],
    ["EXPIRE", tempKey, 30],
    ["SCARD", tempKey],
    ["DEL", tempKey],
  ]);

  const cardResponse = responses[2];
  const total = cardResponse && cardResponse.result ? cardResponse.result : 0;
  return parseInt(total, 10) || 0;
}

async function collectWeeklyUsage(weekQuery) {
  const weekKey = weekQuery && /^\d{4}-W\d{2}$/.test(weekQuery)
    ? weekQuery
    : getIsoWeekKey(new Date());

  const pattern = `telemetry:week:${weekKey}:user:*`;
  const keys = await scanKeys(pattern);
  const values = await getValues(keys);

  const buckets = {
    "1": 0,
    "2-3": 0,
    "4-6": 0,
    "7-9": 0,
    "10+": 0,
  };

  let totalUsers = 0;
  let heavyUsers = 0;
  const heavyThreshold = 6;

  for (let i = 0; i < values.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    totalUsers += 1;
    if (count >= heavyThreshold) {
      heavyUsers += 1;
    }

    if (count === 1) {
      buckets["1"] += 1;
    } else if (count <= 3) {
      buckets["2-3"] += 1;
    } else if (count <= 6) {
      buckets["4-6"] += 1;
    } else if (count <= 9) {
      buckets["7-9"] += 1;
    } else {
      buckets["10+"] += 1;
    }
  }

  const ratio = totalUsers ? Math.round((heavyUsers / totalUsers) * 1000) / 1000 : 0;

  return {
    week: weekKey,
    total_users: totalUsers,
    heavy_users: heavyUsers,
    heavy_ratio: ratio,
    heavy_threshold: heavyThreshold,
    buckets,
  };
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
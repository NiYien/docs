export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, max-age=0");
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const requiredToken = process.env.TELEMETRY_STATS_TOKEN;
  const provided = String(req.headers["x-stats-token"] || req.query.token || "").trim();
  if (requiredToken) {
    if (!provided || provided !== requiredToken) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }

  const dayQuery = String(req.query.day || "").trim();
  const daysQuery = String(req.query.days || "7").trim();
  const weekQuery = String(req.query.week || "").trim();
  
  const isAllTime = daysQuery === "all";
  const days = isAllTime ? 36500 : clampNumber(parseInt(daysQuery, 10) || 7, 1, 30); // 100 years for 'all' logic fallback

  // If not all time, build day list as before
  const dayList = (dayQuery || !isAllTime) ? (dayQuery ? [normalizeDay(dayQuery)] : buildDayList(days)) : [];
  
  if (!isAllTime && dayList.some((d) => !d)) {
    return res.status(400).json({ error: "Invalid day" });
  }

  try {
    let results;
    if (isAllTime) {
        results = await collectAllTimeStats(weekQuery);
    } else {
        results = await collectStats(dayList, weekQuery);
    }
    return res.status(200).json({ ok: true, days: isAllTime ? "all" : dayList, auth_required: !!requiredToken, ...results });
  } catch (error) {
    console.error(error);
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

async function collectStats(dayList, weekQuery) {
  const cityTotals = {};
  const brandTotals = {};
  const modelTotals = {};
  const languageTotals = {};
  const countryTotals = {};
  const cityBrandTotals = {};
  const modelCityTotals = {}; // New: City + Model
  const hourTotals = Array.from({ length: 24 }, () => 0);

  for (const day of dayList) {
    const cityBrandPattern = `telemetry:day:${day}:city:*:brand:*:event:open`;
    const modelCityPattern = `telemetry:day:${day}:city:*:model:*:event:open`;
    const modelPattern = `telemetry:day:${day}:model:*:event:open`;
    const languagePattern = `telemetry:day:${day}:lang:*:event:open`;
    const countryPattern = `telemetry:day:${day}:country:*:event:open`;
    const hourPattern = `telemetry:day:${day}:hour:*:event:open`;

    const cityBrandKeys = await scanKeys(cityBrandPattern);
    const modelCityKeys = await scanKeys(modelCityPattern); // New
    const modelKeys = await scanKeys(modelPattern);
    const languageKeys = await scanKeys(languagePattern);
    const countryKeys = await scanKeys(countryPattern);
    const hourKeys = await scanKeys(hourPattern);

    await accumulateCityBrand(cityBrandKeys, cityTotals, brandTotals, cityBrandTotals);
    await accumulateCityModel(modelCityKeys, modelCityTotals); // New: Reuse logic or create
    await accumulateSingleTotals(modelKeys, modelTotals, "model");
    await accumulateSingleTotals(languageKeys, languageTotals, "lang");
    await accumulateSingleTotals(countryKeys, countryTotals, "country");
    await accumulateHours(hourKeys, hourTotals);
  }

  const uniqueTotals = await collectUniqueTotals(cityTotals);
  const weeklyUsage = await collectWeeklyUsage(weekQuery);

  return {
    city_totals: cityTotals,
    brand_totals: brandTotals,
    model_totals: modelTotals,
    language_totals: languageTotals,
    country_totals: countryTotals,
    city_brand_totals: cityBrandTotals,
    model_city_totals: modelCityTotals,
    hour_totals: hourTotals,
    city_unique_totals: uniqueTotals.cityUniqueTotals,
    global_unique_total: uniqueTotals.globalUniqueTotal,
    weekly_usage: weeklyUsage,
  };
} 

async function collectAllTimeStats(weekQuery) {
  const cityTotals = {};
  const brandTotals = {};
  const modelTotals = {};
  const languageTotals = {};
  const countryTotals = {};
  const cityBrandTotals = {};
  const modelCityTotals = {};
  
  // Directly scan All Time keys
  const cityBrandPattern = `telemetry:total:city:*:brand:*:event:open`; // Note: total keys don't have day
  const modelCityPattern = `telemetry:total:city:*:model:*:event:open`;
  const modelPattern = `telemetry:total:model:*:event:open`;
  const languagePattern = `telemetry:total:lang:*:event:open`;
  const countryPattern = `telemetry:total:countr:*:event:open`; // careful with "country" vs "countr" typo in keys if any. Correct is country.
  // Note: Hour totals for all time is telemetry:total:hour:? No, I didn't add that key. Skipping hours for All Time or assuming distinct if needed.
  // Actually I did not add hour to total keys. So hour_totals will be empty.
  const hourTotals = []; 

  const cityBrandKeys = await scanKeys(cityBrandPattern);
  const modelCityKeys = await scanKeys(modelCityPattern);
  const modelKeys = await scanKeys(modelPattern);
  const languageKeys = await scanKeys(languagePattern);
  // Re-check country key name. In buildKeys: `telemetry:total:country:${country}:event:${event}`
  const countryKeys = await scanKeys(`telemetry:total:country:*:event:open`);

  // Accum functions expect keys to follow a pattern.
  // daily: telemetry:day:..:city:..
  // total: telemetry:total:city:..
  // existing accumulateCityBrand calls parseCityBrandKey which does: key.split(":") and indexOf("city").
  // It should transparently work for 'total' keys too!

  await accumulateCityBrand(cityBrandKeys, cityTotals, brandTotals, cityBrandTotals); 
  await accumulateCityModel(modelCityKeys, modelCityTotals);
  await accumulateSingleTotals(modelKeys, modelTotals, "model");
  await accumulateSingleTotals(languageKeys, languageTotals, "lang");
  await accumulateSingleTotals(countryKeys, countryTotals, "country");

  const uniqueTotals = await collectUniqueTotals(cityTotals);
  // Weekly usage is inherently time-based (last 7 days retention).
  // For 'All Time', weekly usage chart might show "Current Week" context or be empty. 
  // Let's show current week context. 
  const weeklyUsage = await collectWeeklyUsage(weekQuery);

  return {
    city_totals: cityTotals,
    brand_totals: brandTotals,
    model_totals: modelTotals,
    language_totals: languageTotals,
    country_totals: countryTotals,
    city_brand_totals: cityBrandTotals,
    model_city_totals: modelCityTotals,
    hour_totals: hourTotals,
    city_unique_totals: uniqueTotals.cityUniqueTotals,
    global_unique_total: uniqueTotals.globalUniqueTotal,
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

async function accumulateCityModel(keys, modelCityTotals) {
  const values = await getValues(keys);

  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const parsed = parseCityModelKey(key);
    if (!parsed) {
      continue;
    }

    if (!modelCityTotals[parsed.city]) {
      modelCityTotals[parsed.city] = {};
    }
    modelCityTotals[parsed.city][parsed.model] =
      (modelCityTotals[parsed.city][parsed.model] || 0) + count;
  }
}

function parseCityModelKey(key) {
  const parts = key.split(":");
  const cityIndex = parts.indexOf("city");
  const modelIndex = parts.indexOf("model");

  if (cityIndex < 0 || modelIndex < 0) {
    return null;
  }

  const city = decodeKeyPart(parts[cityIndex + 1]);
  const model = decodeKeyPart(parts[modelIndex + 1]);

  if (!city || !model) {
    return null;
  }

  return { city, model };
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

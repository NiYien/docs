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
  const modelCityTotals = {};
  const hourTotals = Array.from({ length: 24 }, () => 0);

  for (const day of dayList) {
    const cityBrandPattern = `telemetry:day:${day}:city:*:brand:*:event:open`;
    const cityPattern = `telemetry:day:${day}:city:*:event:open`;
    const brandPattern = `telemetry:day:${day}:brand:*:event:open`;
    const modelCityPattern = `telemetry:day:${day}:city:*:model:*:event:open`;
    const modelPattern = `telemetry:day:${day}:model:*:event:open`;
    const languagePattern = `telemetry:day:${day}:lang:*:event:open`;
    const countryPattern = `telemetry:day:${day}:country:*:event:open`;
    const hourPattern = `telemetry:day:${day}:hour:*:event:open`;

    const cityBrandKeys = await scanKeys(cityBrandPattern);
    const cityKeys = await scanKeys(cityPattern);
    const brandKeys = await scanKeys(brandPattern);
    const modelCityKeys = await scanKeys(modelCityPattern);
    const modelKeys = await scanKeys(modelPattern);
    const languageKeys = await scanKeys(languagePattern);
    const countryKeys = await scanKeys(countryPattern);
    const hourKeys = await scanKeys(hourPattern);

    const prevCityCount = Object.keys(cityTotals).length;
    const prevBrandCount = Object.keys(brandTotals).length;

    await accumulateCityBrand(cityBrandKeys, cityTotals, brandTotals, cityBrandTotals);
    
    // Fallback if cityBrandKeys was empty for this day
    if (Object.keys(cityTotals).length === prevCityCount) {
      await accumulateSingleTotals(cityKeys, cityTotals, "city");
    }
    if (Object.keys(brandTotals).length === prevBrandCount) {
      await accumulateSingleTotals(brandKeys, brandTotals, "brand");
    }

    await accumulateCityModel(modelCityKeys, modelCityTotals);
    await accumulateSingleTotals(modelKeys, modelTotals, "model");
    await accumulateSingleTotals(languageKeys, languageTotals, "lang");
    await accumulateSingleTotals(countryKeys, countryTotals, "country");
    await accumulateHours(hourKeys, hourTotals);
  }

  const uniqueTotals = await collectUniqueTotals(dayList, cityTotals, brandTotals, modelTotals);
  const weeklyUsage = await collectWeeklyUsage(weekQuery);
  const cityCoords = await collectCityCoords(cityTotals);
  const retention = await collectUserRetention();

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
    brand_unique_totals: uniqueTotals.brandUniqueTotals,
    model_unique_totals: uniqueTotals.modelUniqueTotals,
    global_unique_total: uniqueTotals.globalUniqueTotal,
    weekly_usage: weeklyUsage,
    city_coords: cityCoords,
    user_retention: retention,
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
  const hourTotals = []; 

  const cityBrandPattern = `telemetry:total:city:*:brand:*:event:open`;
  const cityPattern = `telemetry:total:city:*:event:open`;
  const brandPattern = `telemetry:total:brand:*:event:open`;
  const modelCityPattern = `telemetry:total:city:*:model:*:event:open`;
  const modelPattern = `telemetry:total:model:*:event:open`;
  const languagePattern = `telemetry:total:lang:*:event:open`;
  const countryPattern = `telemetry:total:country:*:event:open`;

  const cityBrandKeys = await scanKeys(cityBrandPattern);
  const cityKeys = await scanKeys(cityPattern);
  const brandKeys = await scanKeys(brandPattern);
  const modelCityKeys = await scanKeys(modelCityPattern);
  const modelKeys = await scanKeys(modelPattern);
  const languageKeys = await scanKeys(languagePattern);
  const countryKeys = await scanKeys(countryPattern);

  await accumulateCityBrand(cityBrandKeys, cityTotals, brandTotals, cityBrandTotals); 
  
  // Fallback for old data where cityBrandKeys might be missing
  if (Object.keys(cityTotals).length === 0) {
    await accumulateSingleTotals(cityKeys, cityTotals, "city");
  }
  if (Object.keys(brandTotals).length === 0) {
    await accumulateSingleTotals(brandKeys, brandTotals, "brand");
  }

  await accumulateCityModel(modelCityKeys, modelCityTotals);
  await accumulateSingleTotals(modelKeys, modelTotals, "model");
  await accumulateSingleTotals(languageKeys, languageTotals, "lang");
  await accumulateSingleTotals(countryKeys, countryTotals, "country");

  const uniqueTotals = await collectUniqueTotals("all", cityTotals, brandTotals, modelTotals);
  const weeklyUsage = await collectWeeklyUsage(weekQuery);
  const cityCoords = await collectCityCoords(cityTotals);
  const retention = await collectUserRetention();

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
    brand_unique_totals: uniqueTotals.brandUniqueTotals,
    model_unique_totals: uniqueTotals.modelUniqueTotals,
    global_unique_total: uniqueTotals.globalUniqueTotal,
    weekly_usage: weeklyUsage,
    city_coords: cityCoords,
    user_retention: retention,
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

async function collectUniqueTotals(dayList, cityTotals, brandTotals, modelTotals) {
  const cityNames = cityTotals ? Object.keys(cityTotals) : [];
  const brandNames = brandTotals ? Object.keys(brandTotals) : [];
  const modelNames = modelTotals ? Object.keys(modelTotals) : [];

  const cityUniqueTotals = await getUniqueCounts(dayList, "city", cityNames);
  const brandUniqueTotals = await getUniqueCounts(dayList, "brand", brandNames);
  const modelUniqueTotals = await getUniqueCounts(dayList, "model", modelNames);
  const globalUniqueTotal = await getGlobalUniqueTotal(dayList);

  return { cityUniqueTotals, brandUniqueTotals, modelUniqueTotals, globalUniqueTotal };
}

async function getGlobalUniqueTotal(dayList) {
  if (dayList === "all") {
    const [response] = await upstashPipeline([["PFCOUNT", "telemetry:total:unique:all"]]);
    return response && response.result ? parseInt(response.result, 10) || 0 : 0;
  }

  if (!dayList || !dayList.length) return 0;

  const keys = dayList.map(day => `telemetry:day:${day}:unique:all`);
  const [response] = await upstashPipeline([["PFCOUNT", ...keys]]);
  return response && response.result ? parseInt(response.result, 10) || 0 : 0;
}

async function getUniqueCounts(dayList, type, names) {
  if (!names.length) {
    return {};
  }

  const result = {};
  const chunkSize = 200;

  for (let i = 0; i < names.length; i += chunkSize) {
    const chunk = names.slice(i, i + chunkSize);
    const commands = chunk.map((name) => {
      if (dayList === "all") {
        return ["PFCOUNT", `telemetry:total:unique:${type}:${encodeKeyPart(name)}`];
      } else {
        const keys = dayList.map(day => `telemetry:day:${day}:unique:${type}:${encodeKeyPart(name)}`);
        return ["PFCOUNT", ...keys];
      }
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

async function collectCityCoords(cityTotals) {
  const cityNames = Object.keys(cityTotals);
  if (!cityNames.length) return {};
  
  const result = {};
  const chunkSize = 100;

  for (let i = 0; i < cityNames.length; i += chunkSize) {
    const chunk = cityNames.slice(i, i + chunkSize);
    const keys = chunk.map(city => `telemetry:city_coords:${encodeKeyPart(city)}`);
    const values = await getValues(keys);
    
    for (let j = 0; j < chunk.length; j += 1) {
      if (values[j]) {
        try {
          result[chunk[j]] = JSON.parse(values[j]);
        } catch (e) {
          // ignore
        }
      }
    }
  }
  return result;
}

async function collectUserRetention() {
  const now = Date.now();
  const thirtyDaysMs = 30 * 24 * 60 * 60 * 1000;
  const threshold = now - thirtyDaysMs;

  const [activeRes, inactiveRes] = await upstashPipeline([
    ["ZCOUNT", "telemetry:users:last_seen", threshold, "+inf"],  // Active in last 30 days
    ["ZCOUNT", "telemetry:users:last_seen", "-inf", `(${threshold}`] // Inactive (seen before 30 days ago)
  ]);

  const active = activeRes && activeRes.result ? parseInt(activeRes.result, 10) : 0;
  const inactive = inactiveRes && inactiveRes.result ? parseInt(inactiveRes.result, 10) : 0;

  return {
    active_30d: active,
    inactive_30d: inactive,
    total: active + inactive
  };
}

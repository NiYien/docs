import { DEFAULT_PRODUCT_ID, LEGACY_SOURCE_APP_ID } from "./_control-plane";
import {
  DEFAULT_TELEMETRY_EVENT,
  buildDayNewUsersKey,
  buildScopedUniqueKey,
  buildStatsBasePrefix,
  buildUniqueAllKey,
  buildWeeklyUsagePattern,
  decodeKeyPart,
  getIsoWeekKey,
  getUnionCardinality,
  getValues,
  hasAnyExistingKeys,
  normalizeTelemetryQueryToken,
  scanKeys,
} from "./_telemetry-shared";

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, max-age=0");

  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const requiredToken = process.env.TELEMETRY_STATS_TOKEN;
  const provided = String(req.headers["x-stats-token"] || "").trim();
  if (requiredToken && provided !== requiredToken) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const dayQuery = String(req.query.day || "").trim();
  const daysQuery = String(req.query.days || "7").trim();
  const weekQuery = String(req.query.week || "").trim();
  const productId =
    normalizeTelemetryQueryToken(req.query.product_id, DEFAULT_PRODUCT_ID) || DEFAULT_PRODUCT_ID;
  const sourceAppId = normalizeTelemetryQueryToken(req.query.source_app_id, "");
  const event =
    normalizeTelemetryQueryToken(req.query.event, DEFAULT_TELEMETRY_EVENT) ||
    DEFAULT_TELEMETRY_EVENT;

  const days = clampNumber(parseInt(daysQuery, 10) || 7, 1, 30);
  const dayList = dayQuery ? [normalizeDay(dayQuery)] : buildDayList(days);
  if (dayList.some((day) => !day)) {
    return res.status(400).json({ error: "Invalid day" });
  }

  try {
    const results = await collectStats(dayList, weekQuery, {
      productId,
      sourceAppId,
      event,
    });
    const breakpoint = buildBreakpointMeta(dayList);

    return res.status(200).json({
      ok: true,
      days: dayList,
      filters: {
        product_id: productId,
        source_app_id: sourceAppId || null,
        event,
      },
      auth_required: !!requiredToken,
      breakpoint,
      ...results,
    });
  } catch (error) {
    return res.status(500).json({
      error: "Stats error",
      detail: error.message || String(error),
    });
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
    if (item < day) {
      hasBefore = true;
    } else {
      hasAfter = true;
    }
  }

  const note = String(
    process.env.TELEMETRY_BREAKPOINT_NOTE || "断点日前后口径不同，建议分段查看，不做同比。"
  );

  return {
    day,
    crosses: hasBefore && hasAfter,
    all_before: hasBefore && !hasAfter,
    all_after: hasAfter && !hasBefore,
    note,
  };
}

function clampNumber(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function normalizeDay(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : "";
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

async function collectStats(dayList, weekQuery, filters) {
  const cityTotals = {};
  const brandTotals = {};
  const modelTotals = {};
  const languageTotals = {};
  const countryTotals = {};
  const cityBrandTotals = {};
  const sourceTotals = {};
  const platformTotals = {};
  const statusTotals = {};
  const artifactTotals = {};
  const selectedSourceTotals = {};
  const platformStatusTotals = {};
  const selectedSourceStatusTotals = {};
  const artifactStatusTotals = {};
  const hourTotals = Array.from({ length: 24 }, () => 0);
  const legacyDaysUsed = [];
  const legacyFallbackEnabled = shouldUseLegacyFallback(filters);

  for (const day of dayList) {
    const basePrefix = buildStatsBasePrefix(day, filters.productId, filters.event, filters.sourceAppId);
    const sourcePrefix = `telemetry:day:${day}:product:${filters.productId}:source:*:event:${filters.event}`;
    const platformPattern = `${basePrefix}:platform:*`;
    const statusPattern = `${basePrefix}:status:*`;
    const artifactPattern = `${basePrefix}:artifact:*`;
    const selectedSourcePattern = `${basePrefix}:selected_source:*`;
    const platformStatusPattern = `${basePrefix}:platform:*:status:*`;
    const sourceStatusPattern = `${basePrefix}:selected_source:*:status:*`;
    const artifactStatusPattern = `${basePrefix}:artifact:*:status:*`;
    const [
      cityBrandKeys,
      modelKeys,
      languageKeys,
      countryKeys,
      hourKeys,
      sourceKeys,
      platformKeys,
      statusKeys,
      artifactKeys,
      selectedSourceKeys,
      platformStatusKeys,
      sourceStatusKeys,
      artifactStatusKeys,
      filteredSourceValue,
    ] = await Promise.all([
      scanKeys(`${basePrefix}:city:*:brand:*`),
      scanKeys(`${basePrefix}:model:*`),
      scanKeys(`${basePrefix}:lang:*`),
      scanKeys(`${basePrefix}:country:*`),
      scanKeys(`${basePrefix}:hour:*`),
      filters.sourceAppId ? Promise.resolve([]) : scanKeys(sourcePrefix),
      scanKeys(platformPattern),
      scanKeys(statusPattern),
      scanKeys(artifactPattern),
      scanKeys(selectedSourcePattern),
      scanKeys(platformStatusPattern),
      scanKeys(sourceStatusPattern),
      scanKeys(artifactStatusPattern),
      filters.sourceAppId ? getValues([basePrefix]) : Promise.resolve([]),
    ]);

    await accumulateCityBrand(cityBrandKeys, cityTotals, brandTotals, cityBrandTotals);
    await accumulateSingleTotals(modelKeys, modelTotals, "model");
    await accumulateSingleTotals(languageKeys, languageTotals, "lang");
    await accumulateSingleTotals(countryKeys, countryTotals, "country");
    await accumulateSourceTotals(sourceKeys, sourceTotals);
    await accumulateSingleTrailingTotals(platformKeys, platformTotals, "platform");
    await accumulateSingleTrailingTotals(statusKeys, statusTotals, "status");
    await accumulateSingleTrailingTotals(artifactKeys, artifactTotals, "artifact");
    await accumulateSingleTrailingTotals(
      selectedSourceKeys,
      selectedSourceTotals,
      "selected_source"
    );
    await accumulateDoubleTrailingTotals(
      platformStatusKeys,
      platformStatusTotals,
      "platform",
      "status"
    );
    await accumulateDoubleTrailingTotals(
      sourceStatusKeys,
      selectedSourceStatusTotals,
      "selected_source",
      "status"
    );
    await accumulateDoubleTrailingTotals(
      artifactStatusKeys,
      artifactStatusTotals,
      "artifact",
      "status"
    );
    await accumulateHours(hourKeys, hourTotals);

    if (filters.sourceAppId) {
      const count = parseInt(filteredSourceValue[0] || "0", 10);
      if (count > 0) {
        sourceTotals[filters.sourceAppId] = (sourceTotals[filters.sourceAppId] || 0) + count;
      }
    }

    if (legacyFallbackEnabled) {
      const legacyUsed = await accumulateLegacyDayTotals(day, {
        cityTotals,
        brandTotals,
        modelTotals,
        languageTotals,
        countryTotals,
        cityBrandTotals,
        hourTotals,
        sourceTotals,
      });
      if (legacyUsed) {
        legacyDaysUsed.push(day);
      }
    }
  }

  const uniqueTotals = await collectUniqueTotals(dayList, filters, {
    cityTotals,
    brandTotals,
    modelTotals,
    countryTotals,
  });
  const newTotals = await collectNewTotals(dayList, filters, uniqueTotals);
  const weeklyUsage = await collectWeeklyUsage(weekQuery, filters);
  const sourceUniqueTotals = await collectSourceUniqueTotals(dayList, filters, sourceTotals);
  const availableSources = Array.from(new Set(Object.keys(sourceTotals))).sort();

  return {
    city_totals: cityTotals,
    brand_totals: brandTotals,
    model_totals: modelTotals,
    language_totals: languageTotals,
    country_totals: countryTotals,
    city_brand_totals: cityBrandTotals,
    source_totals: sourceTotals,
    source_unique_totals: sourceUniqueTotals,
    platform_totals: platformTotals,
    status_totals: statusTotals,
    artifact_totals: artifactTotals,
    selected_source_totals: selectedSourceTotals,
    platform_status_totals: platformStatusTotals,
    selected_source_status_totals: selectedSourceStatusTotals,
    artifact_status_totals: artifactStatusTotals,
    available_sources: availableSources,
    hour_totals: hourTotals,
    city_unique_totals: uniqueTotals.cityUniqueTotals,
    brand_unique_totals: uniqueTotals.brandUniqueTotals,
    model_unique_totals: uniqueTotals.modelUniqueTotals,
    country_unique_totals: uniqueTotals.countryUniqueTotals,
    global_unique_total: uniqueTotals.globalUniqueTotal,
    unique_source: uniqueTotals.source,
    missing_unique_data: uniqueTotals.missingUniqueData,
    unique_observed_only: true,
    global_new_total: newTotals.globalNewTotal,
    new_source: newTotals.source,
    missing_new_data: newTotals.missingNewData,
    weekly_usage: weeklyUsage,
    legacy_fallback_used: legacyDaysUsed.length > 0,
    legacy_days_used: legacyDaysUsed,
  };
}

async function accumulateCityBrand(keys, cityTotals, brandTotals, cityBrandTotals) {
  if (!keys.length) {
    return;
  }

  const values = await getValues(keys);
  for (let i = 0; i < keys.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const city = parseKeyValue(keys[i], "city");
    const brand = parseKeyValue(keys[i], "brand");
    if (!city || !brand) {
      continue;
    }

    cityTotals[city] = (cityTotals[city] || 0) + count;
    brandTotals[brand] = (brandTotals[brand] || 0) + count;
    if (!cityBrandTotals[city]) {
      cityBrandTotals[city] = {};
    }
    cityBrandTotals[city][brand] = (cityBrandTotals[city][brand] || 0) + count;
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

    const value = parseKeyValue(keys[i], label);
    if (!value) {
      continue;
    }

    totals[value] = (totals[value] || 0) + count;
  }
}

async function accumulateSingleTrailingTotals(keys, totals, label) {
  if (!keys.length) {
    return;
  }

  const values = await getValues(keys);
  for (let i = 0; i < keys.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const value = parseTrailingKeyValue(keys[i], label);
    if (!value) {
      continue;
    }

    totals[value] = (totals[value] || 0) + count;
  }
}

async function accumulateDoubleTrailingTotals(keys, totals, labelA, labelB) {
  if (!keys.length) {
    return;
  }

  const values = await getValues(keys);
  for (let i = 0; i < keys.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const pair = parseTrailingKeyPair(keys[i], labelA, labelB);
    if (!pair) {
      continue;
    }

    if (!totals[pair.a]) {
      totals[pair.a] = {};
    }
    totals[pair.a][pair.b] = (totals[pair.a][pair.b] || 0) + count;
  }
}

async function accumulateSourceTotals(keys, totals) {
  if (!keys.length) {
    return;
  }

  const values = await getValues(keys);
  for (let i = 0; i < keys.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const source = parseSourceEventKey(keys[i]);
    if (!source) {
      continue;
    }

    totals[source] = (totals[source] || 0) + count;
  }
}

async function accumulateHours(keys, hourTotals) {
  if (!keys.length) {
    return;
  }

  const values = await getValues(keys);
  for (let i = 0; i < keys.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const hour = parseInt(parseKeyValue(keys[i], "hour"), 10);
    if (!Number.isFinite(hour) || hour < 0 || hour > 23) {
      continue;
    }

    hourTotals[hour] += count;
  }
}

async function collectUniqueTotals(dayList, filters, totals) {
  const cityNames = Object.keys(totals.cityTotals || {});
  const brandNames = Object.keys(totals.brandTotals || {});
  const modelNames = Object.keys(totals.modelTotals || {});
  const countryNames = Object.keys(totals.countryTotals || {});

  const cityUniqueTotals = await getScopedUniqueTotals(dayList, cityNames, "city", filters);
  const brandUniqueTotals = await getScopedUniqueTotals(dayList, brandNames, "brand", filters);
  const modelUniqueTotals = await getScopedUniqueTotals(dayList, modelNames, "model", filters);
  const countryUniqueTotals = await getScopedUniqueTotals(dayList, countryNames, "country", filters);
  const globalUniqueTotal = await getGlobalUniqueTotal(dayList, filters);
  const missingUniqueData = hasAnyTotals(totals) && globalUniqueTotal === 0;

  return {
    cityUniqueTotals,
    brandUniqueTotals,
    modelUniqueTotals,
    countryUniqueTotals,
    globalUniqueTotal,
    source: missingUniqueData ? "day-observed-missing" : "day-observed",
    missingUniqueData,
  };
}

async function collectNewTotals(dayList, filters, uniqueTotals) {
  const keys = dayList.map((day) =>
    buildDayNewUsersKey(day, filters.productId, filters.event, filters.sourceAppId)
  );
  if (shouldUseLegacyFallback(filters)) {
    keys.push(...dayList.map((day) => `telemetry:day:${day}:new:all`));
  }
  const globalNewTotal = await getUnionCardinality(keys);
  const hasStoredNewData = await hasAnyExistingKeys(keys);
  const missingNewData =
    !uniqueTotals.missingUniqueData &&
    uniqueTotals.globalUniqueTotal > 0 &&
    globalNewTotal === 0 &&
    !hasStoredNewData;

  return {
    globalNewTotal,
    source: missingNewData ? "day-first-seen-missing" : "day-first-seen",
    missingNewData,
  };
}

async function collectSourceUniqueTotals(dayList, filters, sourceTotals) {
  const result = {};
  const sourceNames = Array.from(
    new Set([
      ...Object.keys(sourceTotals || {}),
      ...(filters.sourceAppId ? [filters.sourceAppId] : []),
      ...(shouldUseLegacyFallback(filters) ? [LEGACY_SOURCE_APP_ID] : []),
    ])
  ).filter(Boolean);

  for (const source of sourceNames) {
    const keys = dayList.map((day) =>
      buildUniqueAllKey(day, filters.productId, filters.event, source)
    );
    if (shouldUseLegacyFallback(filters) && source === LEGACY_SOURCE_APP_ID) {
      keys.push(...dayList.map((day) => `telemetry:day:${day}:unique:all`));
    }
    const total = await getUnionCardinality(keys);
    if (total > 0 || (!filters.sourceAppId && sourceTotals[source])) {
      result[source] = total;
    }
  }

  return result;
}

function hasAnyTotals(totals) {
  return [
    totals.cityTotals,
    totals.brandTotals,
    totals.modelTotals,
    totals.countryTotals,
  ].some((bucket) =>
    Object.values(bucket || {}).some((value) => (parseInt(value || "0", 10) || 0) > 0)
  );
}

async function getGlobalUniqueTotal(dayList, filters) {
  const keys = dayList.map((day) =>
    buildUniqueAllKey(day, filters.productId, filters.event, filters.sourceAppId)
  );
  if (shouldUseLegacyFallback(filters)) {
    keys.push(...dayList.map((day) => `telemetry:day:${day}:unique:all`));
  }
  return getUnionCardinality(keys);
}

async function getScopedUniqueTotals(dayList, names, scope, filters) {
  if (!names.length) {
    return {};
  }

  const result = {};
  for (const name of names) {
    const keys = dayList.map((day) =>
      buildScopedUniqueKey(day, filters.productId, filters.event, scope, name, filters.sourceAppId)
    );
    if (shouldUseLegacyFallback(filters)) {
      const encodedName = encodeLegacyKeyPart(name);
      keys.push(...dayList.map((day) => `telemetry:day:${day}:unique:${scope}:${encodedName}`));
    }
    result[name] = await getUnionCardinality(keys);
  }

  return result;
}

async function collectWeeklyUsage(weekQuery, filters) {
  const weekKey =
    weekQuery && /^\d{4}-W\d{2}$/.test(weekQuery) ? weekQuery : getIsoWeekKey(new Date());
  const pattern = buildWeeklyUsagePattern(weekKey, filters.productId, filters.event, filters.sourceAppId);
  const patterns = [pattern];
  if (shouldUseLegacyFallback(filters)) {
    patterns.push(`telemetry:week:${weekKey}:user:*`);
  }

  const keyLists = await Promise.all(patterns.map((item) => scanKeys(item)));
  const userCounts = new Map();
  for (const keys of keyLists) {
    const values = await getValues(keys);
    mergeWeeklyUserCounts(keys, values, userCounts);
  }

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

  for (const count of userCounts.values()) {
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

  return {
    week: weekKey,
    total_users: totalUsers,
    heavy_users: heavyUsers,
    heavy_ratio: totalUsers ? Math.round((heavyUsers / totalUsers) * 1000) / 1000 : 0,
    heavy_threshold: heavyThreshold,
    buckets,
  };
}

function shouldUseLegacyFallback(filters) {
  return (
    filters.productId === DEFAULT_PRODUCT_ID &&
    filters.event === DEFAULT_TELEMETRY_EVENT &&
    (!filters.sourceAppId || filters.sourceAppId === LEGACY_SOURCE_APP_ID)
  );
}

async function accumulateLegacyDayTotals(day, totals) {
  const [
    cityBrandKeys,
    modelKeys,
    languageKeys,
    countryKeys,
    hourKeys,
    legacyEventValues,
  ] = await Promise.all([
    scanKeys(`telemetry:day:${day}:city:*:brand:*:event:open`),
    scanKeys(`telemetry:day:${day}:model:*:event:open`),
    scanKeys(`telemetry:day:${day}:lang:*:event:open`),
    scanKeys(`telemetry:day:${day}:country:*:event:open`),
    scanKeys(`telemetry:day:${day}:hour:*:event:open`),
    getValues([`telemetry:day:${day}:event:open`]),
  ]);

  await accumulateCityBrand(cityBrandKeys, totals.cityTotals, totals.brandTotals, totals.cityBrandTotals);
  await accumulateSingleTotals(modelKeys, totals.modelTotals, "model");
  await accumulateSingleTotals(languageKeys, totals.languageTotals, "lang");
  await accumulateSingleTotals(countryKeys, totals.countryTotals, "country");
  await accumulateHours(hourKeys, totals.hourTotals);

  const legacyEventCount = parseInt(legacyEventValues[0] || "0", 10);
  if (legacyEventCount > 0) {
    totals.sourceTotals[LEGACY_SOURCE_APP_ID] =
      (totals.sourceTotals[LEGACY_SOURCE_APP_ID] || 0) + legacyEventCount;
  }

  return (
    cityBrandKeys.length > 0 ||
    modelKeys.length > 0 ||
    languageKeys.length > 0 ||
    countryKeys.length > 0 ||
    hourKeys.length > 0 ||
    legacyEventCount > 0
  );
}

function parseKeyValue(key, label) {
  const parts = String(key || "").split(":");
  const index = parts.indexOf(label);
  if (index < 0) {
    return "";
  }
  return decodeKeyPart(parts[index + 1]);
}

function parseTrailingKeyValue(key, label) {
  const parts = String(key || "").split(":");
  if (parts.length < 2 || parts[parts.length - 2] !== label) {
    return "";
  }
  return decodeKeyPart(parts[parts.length - 1]);
}

function parseTrailingKeyPair(key, labelA, labelB) {
  const parts = String(key || "").split(":");
  if (
    parts.length < 4 ||
    parts[parts.length - 4] !== labelA ||
    parts[parts.length - 2] !== labelB
  ) {
    return null;
  }

  return {
    a: decodeKeyPart(parts[parts.length - 3]),
    b: decodeKeyPart(parts[parts.length - 1]),
  };
}

function parseSourceEventKey(key) {
  const match = String(key || "").match(/:source:([^:]+):event:[^:]+$/);
  return match ? decodeKeyPart(match[1]) : "";
}

function mergeWeeklyUserCounts(keys, values, totals) {
  for (let i = 0; i < keys.length; i += 1) {
    const count = parseInt(values[i] || "0", 10);
    if (!count) {
      continue;
    }

    const match = String(keys[i] || "").match(/:user:(.+)$/);
    if (!match) {
      continue;
    }

    const anonId = decodeKeyPart(match[1]);
    if (!anonId) {
      continue;
    }

    totals.set(anonId, (totals.get(anonId) || 0) + count);
  }
}

function encodeLegacyKeyPart(value) {
  const text = String(value || "").trim().slice(0, 96);
  return encodeURIComponent(text || "Unknown");
}

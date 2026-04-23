import {
  CURRENT_SOURCE_APP_ID,
  DEFAULT_PRODUCT_ID,
  LEGACY_SOURCE_APP_ID,
  safeJsonParse,
} from "./_control-plane";

export { safeJsonParse };

export const DEFAULT_TELEMETRY_EVENT = "open";

const SOURCE_ALIASES = new Map([
  ["niyien_tool", LEGACY_SOURCE_APP_ID],
  ["niyientool", LEGACY_SOURCE_APP_ID],
  ["niyien_tool_legacy", LEGACY_SOURCE_APP_ID],
  ["gyroflow", CURRENT_SOURCE_APP_ID],
  ["gyroflow_niyien", CURRENT_SOURCE_APP_ID],
  ["gyroflowniyien", CURRENT_SOURCE_APP_ID],
  ["gyroflow_niyien_app", CURRENT_SOURCE_APP_ID],
]);

const PRODUCT_ALIASES = new Map([
  ["gyroflow", DEFAULT_PRODUCT_ID],
  ["gyroflow_niyien", DEFAULT_PRODUCT_ID],
  ["gyroflowniyien", DEFAULT_PRODUCT_ID],
]);

export function createBatchFallbacks(payload) {
  return {
    anon_id: payload?.anon_id,
    source_app_id: payload?.source_app_id,
    product_id: payload?.product_id,
    app_version: payload?.app_version,
    os: payload?.os,
    camera_brand: payload?.camera_brand,
    camera_model: payload?.camera_model,
    language: payload?.language,
    platform: payload?.platform,
    artifact_type: payload?.artifact_type,
    artifact_version: payload?.artifact_version,
    selected_source: payload?.selected_source,
    status: payload?.status,
  };
}

export function extractEventFields(payload, fallbacks = {}) {
  const merged = { ...fallbacks, ...(payload || {}) };
  const event = normalizeToken(merged.event || DEFAULT_TELEMETRY_EVENT, DEFAULT_TELEMETRY_EVENT, 64);
  const { sourceAppId, productId } = inferIdentity(payload || {}, fallbacks);
  const appVersion = sanitizeText(merged.app_version, "", 96);
  const os = sanitizeText(merged.os, "", 64);
  const cameraBrand = sanitizeText(merged.camera_brand, "Other", 64);
  const cameraModel = sanitizeText(merged.camera_model, "Unknown", 96);
  const language = sanitizeText(merged.language, "Unknown", 32);
  const platform = normalizeToken(merged.platform, "", 32);
  const artifactType = normalizeToken(merged.artifact_type, "", 32);
  const artifactVersion = sanitizeText(merged.artifact_version, "", 96);
  const selectedSource = normalizeToken(merged.selected_source, "", 32);
  const status = normalizeToken(merged.status, "", 32);
  const anonId = sanitizeText(merged.anon_id, "", 128);
  const eventTs = normalizeEventTimestamp(merged.ts);
  const durationMs = normalizeInteger(merged.duration_ms);
  const bytes = normalizeInteger(merged.bytes);
  const eventId = buildEventId(payload || {}, {
    event,
    anonId,
    eventTs,
    appVersion,
    os,
    cameraBrand,
    cameraModel,
    language,
    platform,
    productId,
    sourceAppId,
    artifactType,
    artifactVersion,
    selectedSource,
    status,
  });

  return {
    event,
    eventId,
    eventTs,
    anonId,
    appVersion,
    os,
    cameraBrand,
    cameraModel,
    language,
    platform,
    productId,
    sourceAppId,
    artifactType,
    artifactVersion,
    selectedSource,
    status,
    durationMs,
    bytes,
  };
}

export function validateEventFields(fields) {
  if (!fields.event) {
    return "Invalid event";
  }

  if (!fields.anonId || fields.anonId.length > 128) {
    return "Invalid anon_id";
  }

  if (!fields.eventId || fields.eventId.length > 128) {
    return "Invalid event_id";
  }

  if (!Number.isFinite(fields.eventTs) || fields.eventTs <= 0) {
    return "Invalid ts";
  }

  if (!fields.productId || !fields.sourceAppId) {
    return "Invalid app identity";
  }

  return "";
}

export function buildEventAggregationPlan(fields, context) {
  const eventDate = new Date(fields.eventTs * 1000);
  const iso = eventDate.toISOString();
  const day = iso.slice(0, 10);
  const hour = iso.slice(11, 13);
  const weekKey = getIsoWeekKey(eventDate);
  const keyParts = {
    day,
    hour,
    event: fields.event,
    productId: fields.productId,
    sourceAppId: fields.sourceAppId,
    city: encodeDimensionPart(context?.city, "Unknown", 96),
    country: encodeDimensionPart(context?.country, "Unknown", 64),
    brand: encodeDimensionPart(fields.cameraBrand, "Other", 64),
    model: encodeDimensionPart(fields.cameraModel, "Unknown", 96),
    language: encodeDimensionPart(fields.language, "Unknown", 32),
    platform: fields.platform,
    artifactType: fields.artifactType,
    selectedSource: fields.selectedSource,
    status: fields.status,
  };

  return {
    day,
    hour,
    weekKey,
    countKeys: buildCountKeys(keyParts),
    uniqueKeys: buildUniqueKeys(keyParts),
    dayNewUserContexts: buildDayNewUserContexts(keyParts, fields.anonId),
    weekUserKeys: buildWeekUserKeys(weekKey, keyParts, fields.anonId),
    rawEvent: {
      event_id: fields.eventId,
      event: fields.event,
      anon_id: fields.anonId,
      app_version: fields.appVersion,
      os: fields.os,
      camera_brand: fields.cameraBrand,
      camera_model: fields.cameraModel,
      language: fields.language,
      product_id: fields.productId,
      source_app_id: fields.sourceAppId,
      platform: fields.platform,
      artifact_type: fields.artifactType,
      artifact_version: fields.artifactVersion,
      selected_source: fields.selectedSource,
      status: fields.status,
      duration_ms: fields.durationMs,
      bytes: fields.bytes,
      city: String(context?.city || "Unknown").trim() || "Unknown",
      country: String(context?.country || "Unknown").trim() || "Unknown",
      ts: fields.eventTs,
      ingested_at: Math.floor(Date.now() / 1000),
    },
  };
}

export function buildStatsBasePrefix(day, productId, event, sourceAppId = "") {
  if (sourceAppId) {
    return `telemetry:day:${day}:product:${productId}:source:${sourceAppId}:event:${event}`;
  }
  return `telemetry:day:${day}:product:${productId}:event:${event}`;
}

export function buildUniqueAllKey(day, productId, event, sourceAppId = "") {
  return `${buildStatsBasePrefix(day, productId, event, sourceAppId)}:unique:all`;
}

export function buildScopedUniqueKey(day, productId, event, scope, name, sourceAppId = "") {
  return `${buildStatsBasePrefix(day, productId, event, sourceAppId)}:unique:${scope}:${encodeDimensionPart(
    name,
    "Unknown",
    96
  )}`;
}

export function buildDayNewUsersKey(day, productId, event, sourceAppId = "") {
  return `${buildStatsBasePrefix(day, productId, event, sourceAppId)}:new:all`;
}

export function buildWeeklyUsagePattern(weekKey, productId, event, sourceAppId = "") {
  if (sourceAppId) {
    return `telemetry:week:${weekKey}:product:${productId}:source:${sourceAppId}:event:${event}:user:*`;
  }
  return `telemetry:week:${weekKey}:product:${productId}:event:${event}:user:*`;
}

export function normalizeTelemetryQueryToken(value, fallback = "") {
  return normalizeToken(value, fallback, 64);
}

export function normalizeDay(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "")) ? String(value) : "";
}

export function decodeKeyPart(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch (error) {
    return "";
  }
}

export function getIsoWeekKey(date) {
  const temp = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = temp.getUTCDay() || 7;
  temp.setUTCDate(temp.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(temp.getUTCFullYear(), 0, 1));
  const week = Math.ceil(((temp - yearStart) / 86400000 + 1) / 7);
  return `${temp.getUTCFullYear()}-W${String(week).padStart(2, "0")}`;
}

export function buildRawStreamKey(day) {
  return `telemetry:raw:day:${day}`;
}

export function buildEventDedupeKey(day, eventId) {
  const normalized = encodeIdentifier(eventId);
  return `telemetry:event:processed:${day}:${normalized}`;
}

export function streamFieldsToObject(fields) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[String(fields[i])] = fields[i + 1];
  }
  return obj;
}

export async function upstashCommand(command) {
  const responses = await upstashPipeline([command]);
  return responses[0] || null;
}

export async function upstashPipeline(commands) {
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
    throw new Error(`Upstash pipeline error (${response.status})`);
  }

  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

export async function scanKeys(pattern) {
  const keys = [];
  let cursor = "0";

  for (let i = 0; i < 60; i += 1) {
    const [response] = await upstashPipeline([["SCAN", cursor, "MATCH", pattern, "COUNT", 1000]]);
    const data = response && response.result;
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

export async function getValues(keys) {
  if (!keys.length) {
    return [];
  }

  const values = [];
  const chunkSize = 200;
  for (let i = 0; i < keys.length; i += chunkSize) {
    const chunk = keys.slice(i, i + chunkSize);
    const [response] = await upstashPipeline([["MGET", ...chunk]]);
    values.push(...((response && response.result) || []));
  }

  return values;
}

export async function deleteKeys(keys) {
  if (!keys.length) {
    return;
  }

  const chunkSize = 200;
  for (let i = 0; i < keys.length; i += chunkSize) {
    const chunk = keys.slice(i, i + chunkSize);
    await upstashPipeline([["DEL", ...chunk]]);
  }
}

export async function getUnionCardinality(keys) {
  const list = keys.filter(Boolean);
  if (!list.length) {
    return 0;
  }

  if (list.length === 1) {
    const [response] = await upstashPipeline([["SCARD", list[0]]]);
    return parseInt(response && response.result ? response.result : 0, 10) || 0;
  }

  const tempKey = `telemetry:tmp:stats:union:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`;
  const responses = await upstashPipeline([
    ["SUNIONSTORE", tempKey, ...list],
    ["EXPIRE", tempKey, 30],
    ["SCARD", tempKey],
    ["DEL", tempKey],
  ]);

  return parseInt(responses[2] && responses[2].result ? responses[2].result : 0, 10) || 0;
}

export async function hasAnyExistingKeys(keys) {
  const list = keys.filter(Boolean);
  if (!list.length) {
    return false;
  }

  const chunkSize = 200;
  for (let i = 0; i < list.length; i += chunkSize) {
    const chunk = list.slice(i, i + chunkSize);
    const responses = await upstashPipeline(chunk.map((key) => ["EXISTS", key]));
    if (responses.some((item) => parseInt(item && item.result ? item.result : 0, 10) > 0)) {
      return true;
    }
  }

  return false;
}

function inferIdentity(payload, fallbacks) {
  const rawSource = payload?.source_app_id ?? fallbacks.source_app_id;
  const rawProduct = payload?.product_id ?? fallbacks.product_id;
  let sourceAppId = normalizeSourceAppId(rawSource);
  let productId = normalizeProductId(rawProduct);

  if (!sourceAppId && !productId) {
    return {
      sourceAppId: LEGACY_SOURCE_APP_ID,
      productId: DEFAULT_PRODUCT_ID,
    };
  }

  if (!productId) {
    productId = DEFAULT_PRODUCT_ID;
  }

  if (!sourceAppId) {
    sourceAppId = productId === DEFAULT_PRODUCT_ID ? CURRENT_SOURCE_APP_ID : LEGACY_SOURCE_APP_ID;
  }

  if (sourceAppId === CURRENT_SOURCE_APP_ID) {
    productId = DEFAULT_PRODUCT_ID;
  }

  if (sourceAppId === LEGACY_SOURCE_APP_ID) {
    productId = DEFAULT_PRODUCT_ID;
  }

  return { sourceAppId, productId };
}

function normalizeSourceAppId(value) {
  const token = normalizeToken(value, "", 64);
  if (!token) {
    return "";
  }
  return SOURCE_ALIASES.get(token) || token;
}

function normalizeProductId(value) {
  const token = normalizeToken(value, "", 64);
  if (!token) {
    return "";
  }
  return PRODUCT_ALIASES.get(token) || token;
}

function normalizeEventTimestamp(value) {
  if (value === null || value === undefined || value === "") {
    return Math.floor(Date.now() / 1000);
  }

  const raw = Number(value);
  if (!Number.isFinite(raw) || raw <= 0) {
    return Math.floor(Date.now() / 1000);
  }

  if (raw > 1e12) {
    return Math.floor(raw / 1000);
  }

  return Math.floor(raw);
}

function normalizeInteger(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const raw = Number(value);
  if (!Number.isFinite(raw)) {
    return null;
  }

  return Math.max(0, Math.floor(raw));
}

function buildEventId(payload, fields) {
  const explicit = sanitizeText(payload?.event_id, "", 128);
  if (explicit) {
    return explicit;
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
    fields.platform,
    fields.productId,
    fields.sourceAppId,
    fields.artifactType,
    fields.artifactVersion,
    fields.selectedSource,
    fields.status,
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

function buildCountKeys(parts) {
  const aggregatePrefix = buildStatsBasePrefix(parts.day, parts.productId, parts.event);
  const sourcePrefix = buildStatsBasePrefix(parts.day, parts.productId, parts.event, parts.sourceAppId);

  return [
    ...buildDimensionCountKeys(aggregatePrefix, parts),
    ...buildDimensionCountKeys(sourcePrefix, parts),
  ];
}

function buildDimensionCountKeys(prefix, parts) {
  const keys = [
    `${prefix}:city:${parts.city}:brand:${parts.brand}`,
    `${prefix}:city:${parts.city}`,
    `${prefix}:brand:${parts.brand}`,
    `${prefix}:model:${parts.model}`,
    `${prefix}:lang:${parts.language}`,
    `${prefix}:country:${parts.country}`,
    `${prefix}`,
    `${prefix}:hour:${parts.hour}`,
  ];

  if (parts.platform) {
    keys.push(`${prefix}:platform:${parts.platform}`);
  }

  if (parts.artifactType) {
    keys.push(`${prefix}:artifact:${parts.artifactType}`);
  }

  if (parts.selectedSource) {
    keys.push(`${prefix}:selected_source:${parts.selectedSource}`);
  }

  if (parts.status) {
    keys.push(`${prefix}:status:${parts.status}`);
  }

  if (parts.platform && parts.status) {
    keys.push(`${prefix}:platform:${parts.platform}:status:${parts.status}`);
  }

  if (parts.selectedSource && parts.status) {
    keys.push(`${prefix}:selected_source:${parts.selectedSource}:status:${parts.status}`);
  }

  if (parts.artifactType && parts.status) {
    keys.push(`${prefix}:artifact:${parts.artifactType}:status:${parts.status}`);
  }

  if (parts.artifactType && parts.selectedSource && parts.status) {
    keys.push(
      `${prefix}:artifact:${parts.artifactType}:selected_source:${parts.selectedSource}:status:${parts.status}`
    );
  }

  return keys;
}

function buildUniqueKeys(parts) {
  const aggregatePrefix = buildStatsBasePrefix(parts.day, parts.productId, parts.event);
  const sourcePrefix = buildStatsBasePrefix(parts.day, parts.productId, parts.event, parts.sourceAppId);

  return [
    ...buildDimensionUniqueKeys(aggregatePrefix, parts),
    ...buildDimensionUniqueKeys(sourcePrefix, parts),
  ];
}

function buildDimensionUniqueKeys(prefix, parts) {
  const keys = [
    `${prefix}:unique:all`,
    `${prefix}:unique:city:${parts.city}`,
    `${prefix}:unique:brand:${parts.brand}`,
    `${prefix}:unique:model:${parts.model}`,
    `${prefix}:unique:country:${parts.country}`,
  ];

  if (parts.platform) {
    keys.push(`${prefix}:unique:platform:${parts.platform}`);
  }

  if (parts.artifactType) {
    keys.push(`${prefix}:unique:artifact:${parts.artifactType}`);
  }

  if (parts.selectedSource) {
    keys.push(`${prefix}:unique:selected_source:${parts.selectedSource}`);
  }

  if (parts.status) {
    keys.push(`${prefix}:unique:status:${parts.status}`);
  }

  return keys;
}

function buildDayNewUserContexts(parts, anonId) {
  return [
    {
      firstSeenKey: buildFirstSeenKey(parts.productId, parts.event, anonId),
      dayNewUsersKey: buildDayNewUsersKey(parts.day, parts.productId, parts.event),
      anonId,
    },
    {
      firstSeenKey: buildFirstSeenKey(parts.productId, parts.event, anonId, parts.sourceAppId),
      dayNewUsersKey: buildDayNewUsersKey(parts.day, parts.productId, parts.event, parts.sourceAppId),
      anonId,
    },
  ];
}

function buildFirstSeenKey(productId, event, anonId, sourceAppId = "") {
  const normalizedAnonId = encodeIdentifier(anonId);
  if (sourceAppId) {
    return `telemetry:user:first_seen:product:${productId}:source:${sourceAppId}:event:${event}:${normalizedAnonId}`;
  }
  return `telemetry:user:first_seen:product:${productId}:event:${event}:${normalizedAnonId}`;
}

function buildWeekUserKeys(weekKey, parts, anonId) {
  const normalizedAnonId = encodeIdentifier(anonId);
  return [
    `telemetry:week:${weekKey}:product:${parts.productId}:event:${parts.event}:user:${normalizedAnonId}`,
    `telemetry:week:${weekKey}:product:${parts.productId}:source:${parts.sourceAppId}:event:${parts.event}:user:${normalizedAnonId}`,
  ];
}

function encodeDimensionPart(value, fallback, maxLength) {
  const text = sanitizeText(value, fallback, maxLength);
  return encodeURIComponent(text);
}

function encodeIdentifier(value) {
  return encodeURIComponent(sanitizeText(value, "unknown", 128));
}

function sanitizeText(value, fallback = "", maxLength = 64) {
  const text = String(value ?? "").trim().slice(0, maxLength);
  return text || fallback;
}

function normalizeToken(value, fallback = "", maxLength = 64) {
  const text = String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_")
    .replace(/[^a-z0-9._]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, maxLength);
  return text || fallback;
}

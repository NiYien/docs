import {
  buildEventAggregationPlan,
  buildEventDedupeKey,
  buildRawStreamKey,
  createBatchFallbacks,
  extractEventFields,
  normalizeDay,
  safeJsonParse,
  upstashCommand,
  upstashPipeline,
  validateEventFields,
} from "./_telemetry-shared";
import { getClientIp, getGeo, shouldLogGeo } from "./_geo";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : req.body;
  if (!body) {
    return res.status(400).json({ error: "Invalid JSON" });
  }

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

  const geo = await getGeo(req, { fallbackCountry: "Unknown" });
  const city = geo.city || "Unknown";
  const country = geo.country || "Unknown";
  const ttlDays = parseInt(process.env.TELEMETRY_TTL_DAYS || "90", 10);
  const ttlSeconds = Math.max(ttlDays, 1) * 86400;
  const uniqueTtlDays = parseInt(process.env.TELEMETRY_UNIQUE_TTL_DAYS || "120", 10);
  const uniqueTtlSeconds = uniqueTtlDays > 0 ? uniqueTtlDays * 86400 : 0;
  const weekTtlDays = parseInt(process.env.TELEMETRY_USER_TTL_DAYS || "120", 10);
  const weekTtlSeconds = Math.max(weekTtlDays, 1) * 86400;
  const dedupeTtlDays = parseInt(process.env.TELEMETRY_EVENT_ID_TTL_DAYS || "120", 10);
  const dedupeTtlSeconds = Math.max(dedupeTtlDays, 1) * 86400;
  const rawStreamEnabled =
    String(process.env.TELEMETRY_RAW_STREAM_ENABLED || "true").toLowerCase() !== "false";
  const rawTtlDays = parseInt(process.env.TELEMETRY_RAW_TTL_DAYS || "365", 10);
  const rawTtlSeconds = rawTtlDays > 0 ? rawTtlDays * 86400 : 0;

  if (Array.isArray(body.events)) {
    if (!body.events.length) {
      return res.status(400).json({ error: "Empty events" });
    }

    const fallbacks = createBatchFallbacks(body);
    const items = body.events.map((item, index) => {
      const fields = extractEventFields(item, fallbacks);
      const error = validateEventFields(fields);
      return { index, fields, error };
    });
    const invalid = items.find((item) => item.error);
    if (invalid) {
      return res.status(400).json({ error: invalid.error, index: invalid.index });
    }

    try {
      let processed = 0;
      let deduped = 0;

      for (const item of items) {
        const result = await processEvent(item.fields, {
          city,
          country,
          ttlSeconds,
          uniqueTtlSeconds,
          weekTtlSeconds,
          dedupeTtlSeconds,
          rawStreamEnabled,
          rawTtlSeconds,
        });

        if (result.processed) {
          processed += 1;
        } else {
          deduped += 1;
        }
      }

      return res.status(200).json({
        ok: true,
        batch: true,
        processed,
        deduped,
        received: items.length,
        city,
        country,
        country_source: geo.source || "",
      });
    } catch (error) {
      return res.status(500).json({
        error: "Storage error",
        detail: error.message || String(error),
        has_url: !!(process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL),
        has_token: !!(process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN),
      });
    }
  }

  const fields = extractEventFields(body, {});
  const error = validateEventFields(fields);
  if (error) {
    return res.status(400).json({ error });
  }

  try {
    const result = await processEvent(fields, {
      city,
      country,
      ttlSeconds,
      uniqueTtlSeconds,
      weekTtlSeconds,
      dedupeTtlSeconds,
      rawStreamEnabled,
      rawTtlSeconds,
    });

    const eventDate = new Date(fields.eventTs * 1000);
    const iso = eventDate.toISOString();

    return res.status(200).json({
      ok: true,
      day: iso.slice(0, 10),
      hour: iso.slice(11, 13),
      city,
      country,
      country_source: geo.source || "",
      deduped: !result.processed,
      event: fields.event,
      product_id: fields.productId,
      source_app_id: fields.sourceAppId,
      app_version: fields.appVersion || undefined,
      os: fields.os || undefined,
    });
  } catch (storageError) {
    return res.status(500).json({
      error: "Storage error",
      detail: storageError.message || String(storageError),
      has_url: !!(process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL),
      has_token: !!(process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN),
    });
  }
}

async function processEvent(fields, options) {
  const plan = buildEventAggregationPlan(fields, {
    city: options.city,
    country: options.country,
  });
  const dedupeKey = buildEventDedupeKey(plan.day, fields.eventId);

  const dedupeResponse = await upstashCommand([
    "SET",
    dedupeKey,
    "1",
    "EX",
    options.dedupeTtlSeconds,
    "NX",
  ]);
  const dedupeApplied = dedupeResponse && dedupeResponse.result === "OK";
  if (!dedupeApplied) {
    return { processed: false };
  }

  const pipeline = [];
  for (const key of plan.countKeys) {
    pipeline.push(["INCR", key]);
    pipeline.push(["EXPIRE", key, options.ttlSeconds]);
  }

  for (const key of plan.uniqueKeys) {
    pipeline.push(["SADD", key, fields.anonId]);
    if (options.uniqueTtlSeconds > 0) {
      pipeline.push(["EXPIRE", key, options.uniqueTtlSeconds]);
    }
  }

  for (const weekUserKey of plan.weekUserKeys) {
    pipeline.push(["INCR", weekUserKey]);
    pipeline.push(["EXPIRE", weekUserKey, options.weekTtlSeconds]);
  }

  for (const context of plan.dayNewUserContexts) {
    const firstSeenResponse = await upstashCommand(["SET", context.firstSeenKey, plan.day, "NX"]);
    const isNewUser = firstSeenResponse && firstSeenResponse.result === "OK";

    if (isNewUser) {
      pipeline.push(["SADD", context.dayNewUsersKey, fields.anonId]);
      if (options.uniqueTtlSeconds > 0) {
        pipeline.push(["EXPIRE", context.dayNewUsersKey, options.uniqueTtlSeconds]);
      }
      continue;
    }

    const firstSeenValue = await upstashCommand(["GET", context.firstSeenKey]);
    const existingFirstSeenDay = normalizeDay(
      firstSeenValue && typeof firstSeenValue.result === "string" ? firstSeenValue.result : ""
    );

    if (existingFirstSeenDay && plan.day < existingFirstSeenDay) {
      pipeline.push(["SET", context.firstSeenKey, plan.day]);
      pipeline.push([
        "SREM",
        replaceDayInTelemetryKey(context.dayNewUsersKey, existingFirstSeenDay),
        fields.anonId,
      ]);
      pipeline.push(["SADD", context.dayNewUsersKey, fields.anonId]);
      if (options.uniqueTtlSeconds > 0) {
        pipeline.push(["EXPIRE", context.dayNewUsersKey, options.uniqueTtlSeconds]);
      }
      continue;
    }

    if (existingFirstSeenDay === plan.day) {
      pipeline.push(["SADD", context.dayNewUsersKey, fields.anonId]);
      if (options.uniqueTtlSeconds > 0) {
        pipeline.push(["EXPIRE", context.dayNewUsersKey, options.uniqueTtlSeconds]);
      }
    }
  }

  if (options.rawStreamEnabled) {
    const rawFieldPairs = [];
    for (const [field, value] of Object.entries(plan.rawEvent)) {
      rawFieldPairs.push(field, value === undefined || value === null ? "" : String(value));
    }
    pipeline.push(["XADD", buildRawStreamKey(plan.day), "*", ...rawFieldPairs]);
    if (options.rawTtlSeconds > 0) {
      pipeline.push(["EXPIRE", buildRawStreamKey(plan.day), options.rawTtlSeconds]);
    }
  }

  await upstashPipeline(pipeline);
  return { processed: true };
}

function replaceDayInTelemetryKey(key, day) {
  return String(key || "").replace(/telemetry:day:\d{4}-\d{2}-\d{2}:/, `telemetry:day:${day}:`);
}

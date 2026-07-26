import {
  buildEventAggregationPlan,
  buildRawStreamKey,
  createBatchFallbacks,
  extractEventFields,
  safeJsonParse,
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
      // One pipeline, one XADD per event: N valid events cost exactly N
      // billable Redis commands.
      await appendRawEvents(
        items.map((item) => item.fields),
        { city, country }
      );

      return res.status(200).json({
        ok: true,
        batch: true,
        processed: items.length,
        deduped: 0,
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
    await appendRawEvents([fields], { city, country });

    const eventDate = new Date(fields.eventTs * 1000);
    const iso = eventDate.toISOString();

    return res.status(200).json({
      ok: true,
      day: iso.slice(0, 10),
      hour: iso.slice(11, 13),
      city,
      country,
      country_source: geo.source || "",
      // Retained for response compatibility. Ingestion performs no Redis
      // dedupe, so an accepted event is never reported as deduplicated.
      deduped: false,
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

// Ingestion appends the raw event and nothing else. Every derived metric --
// counts, unique sets, first-seen records, weekly counters -- is reconstructed
// downstream from these streams, so a valid event costs exactly one billable
// Redis command.
//
// `buildEventAggregationPlan` is still the source of the raw record and the UTC
// day. It also computes aggregate key names that ingestion no longer writes;
// that is pure CPU with no I/O, and it keeps the written schema identical to
// what the downstream readers expect.
async function appendRawEvents(fieldsList, context) {
  if (!fieldsList.length) {
    return;
  }

  const commands = fieldsList.map((fields) => {
    const plan = buildEventAggregationPlan(fields, {
      city: context.city,
      country: context.country,
    });

    const pairs = [];
    for (const [field, value] of Object.entries(plan.rawEvent)) {
      pairs.push(field, value === undefined || value === null ? "" : String(value));
    }
    return ["XADD", buildRawStreamKey(plan.day), "*", ...pairs];
  });

  await upstashPipeline(commands);
}

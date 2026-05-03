// POST /api/feedback — entry point for client feedback submissions.
//
// Flow:
//   1. Validate body (size, sha256).
//   2. Determine region (vercel header → ipinfo fallback if uncertain).
//   3. Apply per-IP rate limit (10/day).
//   4. Generate id `<UTC_yyyymmdd>-<8 char uuid>`.
//   5. Sign upload credentials per region (R2 presigned PUT for global,
//      pan123 multipart init for cn).
//   6. Write `fb:pending:<id>` to KV with TTL 300s.
//   7. Return 200 with id + region + upload payload + expires_at.

import { safeJsonParse, upstashCommand, upstashPipeline } from "./_telemetry-shared";
import { getClientIp, getCountry } from "./_geo";
import { signR2PutUrl, getR2BucketName } from "./_r2";
import { signPan123UploadInit } from "./_pan123";

const MAX_SIZE_BYTES = 50_000_000;
const SHA256_HEX_RE = /^[a-f0-9]{64}$/i;
const SUMMARY_MAX_LEN = 200;
const EMAIL_MAX_LEN = 200;
const PENDING_TTL_SECONDS = 300;
const RATE_LIMIT_TTL_SECONDS = 86400;
const RATE_LIMIT_PER_DAY = 10;
const UPLOAD_TTL_SECONDS = 1800;

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");

  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const body = typeof req.body === "string" ? safeJsonParse(req.body) : req.body;
  if (!body || typeof body !== "object") {
    return res.status(400).json({ error: "Invalid JSON body" });
  }

  const validation = validateBody(body);
  if (validation.error) {
    return res.status(400).json({ error: validation.error });
  }

  const ip = getClientIp(req) || "unknown";
  const region = await detectRegion(req);
  const yyyymmdd = formatYyyymmddUTC(new Date());

  // Rate limit BEFORE generating id / signing token to keep work cheap on
  // throttled requests.
  const rateLimitResult = await applyRateLimit(ip, yyyymmdd);
  if (rateLimitResult.exceeded) {
    res.setHeader("Retry-After", String(rateLimitResult.retryAfterSeconds));
    return res.status(429).json({
      error: "Rate limit exceeded",
      limit: RATE_LIMIT_PER_DAY,
      window: "day",
    });
  }

  const id = `${yyyymmdd}-${randomShortId()}`;
  const filename = `${id}.zip`;
  const ts = new Date().toISOString();

  let upload;
  let bucketPath;
  try {
    if (region === "cn") {
      const init = await signPan123UploadInit(yyyymmdd, filename);
      bucketPath = init.bucketPath;
      upload = {
        method: "POST",
        kind: init.uploadKind,
        // No single PUT URL — client must walk the 123 multipart flow.
        // See feedback-schema.md "cn upload protocol" for details.
        open_api_base: init.openApiBase,
        access_token: init.accessToken,
        parent_file_id: init.parentFileId,
        parent_path: init.parentPath,
        filename: init.filename,
      };
    } else {
      const r2Key = `feedback/${yyyymmdd}/${filename}`;
      const signed = await signR2PutUrl(r2Key, UPLOAD_TTL_SECONDS);
      bucketPath = `${getR2BucketName()}/${r2Key}`;
      upload = {
        method: "PUT",
        kind: "r2_presigned_put",
        url: signed.url,
        headers: signed.headers,
      };
    }
  } catch (error) {
    return res.status(500).json({
      error: "Upload signing failed",
      detail: error.message || String(error),
    });
  }

  const expiresAt = new Date(Date.now() + UPLOAD_TTL_SECONDS * 1000).toISOString();
  const pendingRecord = {
    id,
    region,
    app_version: String(body.app_version || "").slice(0, 96),
    os: String(body.os || "").slice(0, 64),
    gpu: String(body.gpu || "").slice(0, 128),
    size: Number(body.size),
    sha256: String(body.sha256).toLowerCase(),
    summary: String(body.summary || "").slice(0, SUMMARY_MAX_LEN),
    email: String(body.email || "").slice(0, EMAIL_MAX_LEN),
    ts,
    bucket_path: bucketPath,
    ip_hash: hashIp(ip),
  };

  try {
    await upstashCommand([
      "SET",
      `fb:pending:${id}`,
      JSON.stringify(pendingRecord),
      "EX",
      String(PENDING_TTL_SECONDS),
    ]);
  } catch (error) {
    return res.status(500).json({
      error: "KV write failed",
      detail: error.message || String(error),
    });
  }

  return res.status(200).json({
    id,
    region,
    upload,
    expires_at: expiresAt,
  });
}

function validateBody(body) {
  const size = Number(body?.size);
  if (!Number.isFinite(size) || size <= 0) {
    return { error: "size must be a positive integer" };
  }
  if (size > MAX_SIZE_BYTES) {
    return { error: `size exceeds ${MAX_SIZE_BYTES} bytes` };
  }

  const sha256 = String(body?.sha256 || "").trim();
  if (!SHA256_HEX_RE.test(sha256)) {
    return { error: "sha256 must be 64 hex chars" };
  }

  const summary = String(body?.summary || "");
  if (summary.length > SUMMARY_MAX_LEN) {
    return { error: `summary too long (max ${SUMMARY_MAX_LEN})` };
  }

  return { error: "" };
}

// Region detection: defer to _geo.js::getCountry, which already implements
// vercel-header + ipinfo fallback with a 6h GEO_CACHE and per-IP pending-
// request dedup. Sharing that cache means the manifest endpoint and this
// endpoint don't double-bill ipinfo on the same client IP.
async function detectRegion(req) {
  const country = await getCountry(req, "US");
  return String(country || "").toUpperCase() === "CN" ? "cn" : "global";
}

async function applyRateLimit(ip, yyyymmdd) {
  const key = `rl:fb:${ip}:${yyyymmdd}`;
  // Pipeline INCR + EXPIRE so the TTL is set on first hit. EXPIRE is a no-op
  // on subsequent hits since the key already has a TTL.
  let responses;
  try {
    responses = await upstashPipeline([
      ["INCR", key],
      ["EXPIRE", key, String(RATE_LIMIT_TTL_SECONDS), "NX"],
    ]);
  } catch (error) {
    // KV outage — fail open (log and let through). Better than blocking all
    // submissions. Real protection still has cloudflare layer in front.
    console.warn("[feedback] rate-limit KV error", error?.message || error);
    return { exceeded: false, retryAfterSeconds: 0 };
  }

  const current = parseInt(responses?.[0]?.result || "0", 10) || 0;
  if (current > RATE_LIMIT_PER_DAY) {
    // Roughly seconds until next UTC midnight; client-friendly.
    const now = new Date();
    const tomorrow = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1)
    );
    return {
      exceeded: true,
      retryAfterSeconds: Math.max(60, Math.floor((tomorrow.getTime() - now.getTime()) / 1000)),
    };
  }
  return { exceeded: false, retryAfterSeconds: 0 };
}

function formatYyyymmddUTC(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function randomShortId() {
  // crypto.randomUUID() returns "xxxxxxxx-xxxx-..." — slice the first 8 hex.
  const uuid = globalThis.crypto.randomUUID();
  return uuid.replace(/-/g, "").slice(0, 8);
}

// Non-reversible IP marker for forensic correlation without storing the raw
// IP address. djb2 over a daily-rotated salt is plenty for "did the same IP
// submit twice" without enabling tracking.
function hashIp(ip) {
  const yyyymmdd = formatYyyymmddUTC(new Date());
  const text = `${yyyymmdd}|${ip}`;
  let hash = 5381;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) + hash + text.charCodeAt(i)) | 0;
  }
  return `h_${(hash >>> 0).toString(16)}`;
}

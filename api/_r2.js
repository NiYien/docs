// Cloudflare R2 helper: presigned PUT URLs via manual AWS SigV4 (no SDK).
//
// R2 speaks the S3 API, so the standard SigV4 query-string presign works
// against the R2 endpoint `https://<account_id>.r2.cloudflarestorage.com/<bucket>/<key>`.
// We implement SigV4 with the built-in Web Crypto API to avoid adding the
// 50MB+ `@aws-sdk/*` dependency tree to this otherwise dependency-free repo.
//
// Env vars consumed:
//   R2_ACCOUNT_ID        — Cloudflare account id (subdomain of r2.cloudflarestorage.com)
//   R2_ACCESS_KEY_ID     — R2 token's access key
//   R2_SECRET_ACCESS_KEY — R2 token's secret key
//   R2_BUCKET            — bucket name (default: gyroflow-feedback)
//   R2_REGION            — region code (default: auto, R2's accepted value)

const DEFAULT_REGION = "auto";
const DEFAULT_BUCKET = "gyroflow-feedback";
const SIGNED_HEADERS = "host";
const ALGORITHM = "AWS4-HMAC-SHA256";
const SERVICE = "s3";
// "UNSIGNED-PAYLOAD" lets the client PUT any body without re-signing the
// content; the URL signature still binds bucket/key/expiration.
const UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

export function getR2BucketName() {
  return String(process.env.R2_BUCKET || DEFAULT_BUCKET).trim() || DEFAULT_BUCKET;
}

export function getR2Region() {
  return String(process.env.R2_REGION || DEFAULT_REGION).trim() || DEFAULT_REGION;
}

export function getR2Endpoint() {
  const accountId = String(process.env.R2_ACCOUNT_ID || "").trim();
  if (!accountId) {
    throw new Error("Missing R2_ACCOUNT_ID");
  }
  return `https://${accountId}.r2.cloudflarestorage.com`;
}

// Sign a PUT URL valid for `expiresIn` seconds (max 7 days per SigV4 spec).
// Returns `{ url, headers, expiresAt }` where `headers` is the set the client
// MUST send with the PUT (only `Host` is signed; others are advisory).
export async function signR2PutUrl(key, expiresIn = 1800) {
  const accessKeyId = String(process.env.R2_ACCESS_KEY_ID || "").trim();
  const secretAccessKey = String(process.env.R2_SECRET_ACCESS_KEY || "").trim();
  if (!accessKeyId || !secretAccessKey) {
    throw new Error("Missing R2_ACCESS_KEY_ID or R2_SECRET_ACCESS_KEY");
  }

  const bucket = getR2BucketName();
  const region = getR2Region();
  const endpoint = getR2Endpoint();
  const host = new URL(endpoint).host;
  const expires = clampExpires(expiresIn);

  const now = new Date();
  const amzDate = formatAmzDate(now);
  const dateStamp = amzDate.slice(0, 8);
  const credentialScope = `${dateStamp}/${region}/${SERVICE}/aws4_request`;
  const credential = `${accessKeyId}/${credentialScope}`;

  // Path-style URL: /<bucket>/<encoded key>. Each path segment is URI-encoded
  // (reserved chars encoded), but slashes between segments are kept literal.
  const encodedKey = encodeS3Key(key);
  const canonicalUri = `/${bucket}/${encodedKey}`;

  const queryParams = [
    ["X-Amz-Algorithm", ALGORITHM],
    ["X-Amz-Credential", credential],
    ["X-Amz-Date", amzDate],
    ["X-Amz-Expires", String(expires)],
    ["X-Amz-SignedHeaders", SIGNED_HEADERS],
  ];
  // Sort query params lexicographically (SigV4 requires this).
  queryParams.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  const canonicalQuery = queryParams
    .map(([k, v]) => `${rfc3986Encode(k)}=${rfc3986Encode(v)}`)
    .join("&");

  const canonicalHeaders = `host:${host}\n`;
  const canonicalRequest = [
    "PUT",
    canonicalUri,
    canonicalQuery,
    canonicalHeaders,
    SIGNED_HEADERS,
    UNSIGNED_PAYLOAD,
  ].join("\n");

  const stringToSign = [
    ALGORITHM,
    amzDate,
    credentialScope,
    await sha256Hex(canonicalRequest),
  ].join("\n");

  const signingKey = await deriveSigningKey(secretAccessKey, dateStamp, region, SERVICE);
  const signature = await hmacHex(signingKey, stringToSign);

  const url = `${endpoint}${canonicalUri}?${canonicalQuery}&X-Amz-Signature=${signature}`;
  const expiresAt = new Date(now.getTime() + expires * 1000).toISOString();

  return {
    url,
    // Client must send no extra signed headers; advise empty headers.
    // R2 also requires no Content-MD5 / x-amz-content-sha256 since payload is unsigned.
    headers: {},
    expiresAt,
  };
}

function clampExpires(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 1800;
  }
  // Cap at 7 days = 604800s, AWS SigV4 hard limit.
  return Math.min(Math.trunc(numeric), 604800);
}

// "20260502T123456Z" — used as X-Amz-Date.
function formatAmzDate(date) {
  const iso = date.toISOString();
  return iso.replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}

// Encode each segment of the S3 key but preserve `/` between segments.
function encodeS3Key(key) {
  return String(key || "")
    .split("/")
    .map((segment) => rfc3986Encode(segment))
    .join("/");
}

// AWS SigV4 mandates RFC 3986 encoding (slash inside a value is %2F, etc.).
// The standard `encodeURIComponent` leaves `! * ' ( )` alone — those must
// also be percent-encoded for canonical-URI / canonical-query equivalence.
function rfc3986Encode(value) {
  return encodeURIComponent(String(value)).replace(
    /[!*'()]/g,
    (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

async function sha256Hex(text) {
  const enc = new TextEncoder();
  const digest = await globalThis.crypto.subtle.digest("SHA-256", enc.encode(String(text)));
  return bufferToHex(digest);
}

async function deriveSigningKey(secretKey, dateStamp, region, service) {
  const enc = new TextEncoder();
  const kDate = await hmacRaw(enc.encode(`AWS4${secretKey}`), dateStamp);
  const kRegion = await hmacRaw(kDate, region);
  const kService = await hmacRaw(kRegion, service);
  const kSigning = await hmacRaw(kService, "aws4_request");
  return kSigning;
}

async function hmacRaw(keyBytes, message) {
  const key = await globalThis.crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const enc = new TextEncoder();
  const sig = await globalThis.crypto.subtle.sign(
    "HMAC",
    key,
    typeof message === "string" ? enc.encode(message) : message
  );
  return new Uint8Array(sig);
}

async function hmacHex(keyBytes, message) {
  const sig = await hmacRaw(keyBytes, message);
  return bufferToHex(sig);
}

function bufferToHex(input) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  let hex = "";
  for (let i = 0; i < bytes.length; i += 1) {
    hex += bytes[i].toString(16).padStart(2, "0");
  }
  return hex;
}

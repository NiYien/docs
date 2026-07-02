import crypto from "node:crypto";

// Local 123 direct-link construction + optional hotlink-protection signing.
//
// The 123 direct-link space is pure deterministic path addressing:
//   <base>/<uid>/releases/<tag>/<file>
// (verified 2026-07-02: constructing the URL for a file that was never
// resolved through the open API returns HTTP 206), so the download endpoint
// can emit a redirect without any call to open-api.123pan.com — which was the
// source of intermittent FUNCTION_INVOCATION_TIMEOUT 504s (serial cross-border
// fetches with no timeout).
//
// Signing implements 123's hotlink protection (Aliyun CDN type-A auth):
//   auth_key = <timestamp>-<rand>-<uid>-<md5hash>
//   md5hash  = md5("<encodedPath>-<timestamp>-<rand>-<uid>-<key>")
// where <encodedPath> is the percent-encoded request path (no query) and
// <timestamp> is the expiry moment (unix seconds). The hash MUST be computed
// over the exact encoded path the CDN will receive — encoding mismatch means
// signature verification always fails.
//
// IMPORTANT: signing stays dormant until PAN123_DIRECT_LINK_AUTH_KEY is set.
// Enabling hotlink protection on the 123 side is a global switch that would
// break the bare direct links legacy NiYien_tool clients consume
// (…/Gyroflow_SDK/, …/Gyroflow_Plugins/); it must not be turned on until
// those consumers are migrated (deferred follow-up change).

const DEFAULT_EXPIRE_SECONDS = 600;
// Built-in defaults so the deploy needs zero env configuration. The uid and
// releases path are already visible in every public download URL, so encoding
// them here leaks nothing. Env vars exist only as overrides (e.g. if 123 ever
// changes the direct-link entry domain or the space is reorganized).
const DEFAULT_DIRECT_LINK_BASE = "https://vip.123pan.cn";
const DEFAULT_DIRECT_LINK_PREFIX = "/1846941948/releases";

export function buildDirectLinkUrl(tag, relativePath) {
  const base = stripTrailingSlash(process.env.PAN123_DIRECT_LINK_BASE || DEFAULT_DIRECT_LINK_BASE);
  const prefix = normalizePrefix(process.env.PAN123_DIRECT_LINK_PREFIX || DEFAULT_DIRECT_LINK_PREFIX);
  if (!base || !prefix) {
    throw new Error("Missing PAN123_DIRECT_LINK_BASE or PAN123_DIRECT_LINK_PREFIX");
  }

  // tag/relativePath arrive decoded (download.js normalizers run
  // decodeURIComponent), so every segment must be re-encoded here.
  const encodedTag = encodeURIComponent(String(tag || ""));
  const encodedPath = String(relativePath || "")
    .split("/")
    .map((item) => encodeURIComponent(item))
    .join("/");
  if (!encodedTag || !encodedPath) {
    throw new Error("Missing tag or relative path for direct link");
  }

  return `${base}${prefix}/${encodedTag}/${encodedPath}`;
}

export function signDirectLinkUrl(url) {
  const key = String(process.env.PAN123_DIRECT_LINK_AUTH_KEY || "").trim();
  if (!key) {
    return url;
  }

  const uid = resolveDirectLinkUid();
  if (!uid) {
    throw new Error("Missing PAN123_DIRECT_LINK_UID (or a uid-leading PAN123_DIRECT_LINK_PREFIX)");
  }

  const expireSeconds = parsePositiveInt(
    process.env.PAN123_DIRECT_LINK_EXPIRE_SECONDS,
    DEFAULT_EXPIRE_SECONDS
  );
  const timestamp = Math.floor(Date.now() / 1000) + expireSeconds;
  // rand must not contain '-' (it is the auth_key field separator).
  const rand = crypto.randomUUID().replace(/-/g, "");
  // Sign the percent-encoded path exactly as the CDN will receive it.
  const path = new URL(url).pathname;
  const md5hash = crypto
    .createHash("md5")
    .update(`${path}-${timestamp}-${rand}-${uid}-${key}`)
    .digest("hex");

  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}auth_key=${timestamp}-${rand}-${uid}-${md5hash}`;
}

// uid resolution: explicit PAN123_DIRECT_LINK_UID wins; otherwise fall back
// to the first path segment of PAN123_DIRECT_LINK_PREFIX (e.g.
// "/1846941948/releases" -> "1846941948"), which is the account uid in the
// 123 direct-link URL layout.
function resolveDirectLinkUid() {
  const explicit = String(process.env.PAN123_DIRECT_LINK_UID || "").trim();
  if (explicit) {
    return explicit;
  }
  const prefix = normalizePrefix(process.env.PAN123_DIRECT_LINK_PREFIX || "");
  const first = prefix.split("/").filter(Boolean)[0] || "";
  return /^\d+$/.test(first) ? first : "";
}

function parsePositiveInt(value, fallback) {
  const numeric = parseInt(String(value || ""), 10);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : fallback;
}

function stripTrailingSlash(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function normalizePrefix(value) {
  const trimmed = String(value || "").trim().replace(/\/+$/, "");
  if (!trimmed) {
    return "";
  }
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

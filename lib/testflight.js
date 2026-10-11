import { createPrivateKey, sign } from "node:crypto";

export const TESTFLIGHT_URL = "https://testflight.apple.com/join/MGh9AhgQ";
const APPLE_API_ORIGIN = "https://api.appstoreconnect.apple.com";
const CACHE_SECONDS = 3600;
const APPLE_TOKEN_SECONDS = 300;
const MAX_PAGES = 10;

class TestFlightError extends Error {
  constructor(code) {
    super("Unable to read the TestFlight version");
    this.code = code;
  }
}

export function createAppleToken(env, now) {
  const issuer = String(env.APPSTORE_ISSUER_ID || env.MACOS_ITCONNECT_ISSUER || "").trim();
  const keyId = String(env.APPSTORE_API_KEY_ID || env.MACOS_ITCONNECT_KEY_ID || "").trim();
  const rawKey = String(env.APPSTORE_API_PRIVATE_KEY || env.MACOS_ITCONNECT_KEY || "").trim();
  if (!issuer || !keyId || !rawKey) {
    throw new TestFlightError("missing_api_configuration");
  }

  let key;
  try {
    const pem = rawKey.includes("-----BEGIN")
      ? rawKey.replace(/\\n/g, "\n")
      : Buffer.from(rawKey, "base64").toString("utf8");
    key = createPrivateKey(pem);
    if (key.asymmetricKeyType !== "ec" || key.asymmetricKeyDetails?.namedCurve !== "prime256v1") {
      throw new Error("Unexpected key type");
    }
  } catch {
    throw new TestFlightError("invalid_api_private_key");
  }

  const issuedAt = Math.floor(now / 1000);
  const header = Buffer.from(JSON.stringify({ alg: "ES256", kid: keyId, typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(JSON.stringify({
    iss: issuer,
    iat: issuedAt,
    exp: issuedAt + APPLE_TOKEN_SECONDS,
    aud: "appstoreconnect-v1",
    scope: ["GET /v1/betaGroups", "GET /v1/builds"],
  })).toString("base64url");
  const content = `${header}.${payload}`;
  const signature = sign("sha256", Buffer.from(content), { key, dsaEncoding: "ieee-p1363" });
  return `${content}.${signature.toString("base64url")}`;
}

async function readCollection(path, token, fetchImpl, signal) {
  const data = [];
  const included = [];
  const visited = new Set();
  let next = new URL(path, APPLE_API_ORIGIN).href;
  while (next) {
    const url = new URL(next, APPLE_API_ORIGIN);
    if (url.origin !== APPLE_API_ORIGIN || !["/v1/betaGroups", "/v1/builds"].includes(url.pathname)) {
      throw new TestFlightError("invalid_pagination_url");
    }
    if (visited.has(url.href) || visited.size >= MAX_PAGES) {
      throw new TestFlightError("pagination_limit");
    }
    visited.add(url.href);
    const response = await fetchImpl(url.href, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
      signal,
      redirect: "error",
    });
    if (!response.ok) {
      throw new TestFlightError(`apple_http_${response.status}`);
    }
    const page = await response.json();
    if (!Array.isArray(page.data)) {
      throw new TestFlightError("invalid_apple_response");
    }
    data.push(...page.data);
    if (Array.isArray(page.included)) included.push(...page.included);
    next = page.links?.next || "";
  }
  return { data, included };
}

function compareNumbers(left, right) {
  const a = left.split(".").map(Number);
  const b = right.split(".").map(Number);
  for (let i = 0; i < Math.max(a.length, b.length); i += 1) {
    const difference = (a[i] || 0) - (b[i] || 0);
    if (difference) return difference;
  }
  return 0;
}

export function selectLatestBuild(collection, now) {
  const related = new Map(collection.included.map((item) => [`${item.type}:${item.id}`, item]));
  const candidates = [];
  for (const build of collection.data) {
    if (build.type !== "builds") continue;
    const attributes = build.attributes || {};
    const versionId = build.relationships?.preReleaseVersion?.data?.id;
    const detailId = build.relationships?.buildBetaDetail?.data?.id;
    const release = related.get(`preReleaseVersions:${versionId}`)?.attributes || {};
    const detail = related.get(`buildBetaDetails:${detailId}`)?.attributes || {};
    const expiration = Date.parse(attributes.expirationDate);
    const version = String(release.version || "").trim();
    const buildNumber = String(attributes.version || "").trim();
    if (release.platform !== "IOS" || attributes.processingState !== "VALID" ||
        attributes.expired !== false || !Number.isFinite(expiration) || expiration <= now ||
        detail.externalBuildState !== "IN_BETA_TESTING" ||
        !/^\d+(?:\.\d+){0,2}$/.test(version) || !/^\d+(?:\.\d+){0,2}$/.test(buildNumber)) {
      continue;
    }
    candidates.push({ version, buildNumber, expiration, uploadedAt: Date.parse(attributes.uploadedDate) || 0 });
  }
  candidates.sort((a, b) => compareNumbers(b.version, a.version) ||
    compareNumbers(b.buildNumber, a.buildNumber) || b.uploadedAt - a.uploadedAt);
  return candidates[0] || null;
}

async function lookupLatestBuild(env, fetchImpl, now) {
  const token = createAppleToken(env, now);
  const signal = AbortSignal.timeout(12000);
  const groupQuery = new URLSearchParams({
    "filter[publicLink]": TESTFLIGHT_URL,
    "filter[publicLinkEnabled]": "true",
    "filter[isInternalGroup]": "false",
    "fields[betaGroups]": "publicLink,publicLinkEnabled,isInternalGroup",
    limit: "200",
  });
  const groups = await readCollection(`/v1/betaGroups?${groupQuery}`, token, fetchImpl, signal);
  const group = groups.data.find((item) => item.type === "betaGroups" &&
    item.attributes?.publicLink === TESTFLIGHT_URL && item.attributes?.publicLinkEnabled === true &&
    item.attributes?.isInternalGroup === false);
  if (!group) return null;

  // The group relationship endpoint cannot include release versions or testing states.
  const buildQuery = new URLSearchParams({
    "filter[betaGroups]": group.id,
    "filter[expired]": "false",
    "filter[processingState]": "VALID",
    "filter[preReleaseVersion.platform]": "IOS",
    "fields[builds]": "version,uploadedDate,expirationDate,expired,processingState,preReleaseVersion,buildBetaDetail",
    "fields[preReleaseVersions]": "version,platform",
    "fields[buildBetaDetails]": "externalBuildState",
    include: "preReleaseVersion,buildBetaDetail",
    sort: "-uploadedDate",
    limit: "200",
  });
  const builds = await readCollection(`/v1/builds?${buildQuery}`, token, fetchImpl, signal);
  return selectLatestBuild(builds, now);
}

export function createTestFlightLookup({ env = process.env, fetchImpl = fetch, now = Date.now, warn = console.warn } = {}) {
  let cached = null;
  let pending = null;
  return async function readVersion() {
    if (!cached || cached.expiresAt <= now()) {
      if (!pending) {
        pending = (async () => {
          let build = null;
          let checkedAt;
          try {
            build = await lookupLatestBuild(env, fetchImpl, now());
            checkedAt = now();
            if (build && build.expiration <= checkedAt) {
              throw new TestFlightError("build_expired_during_lookup");
            }
          } catch (error) {
            build = null;
            warn("[testflight] Version lookup failed:", error instanceof TestFlightError ? error.code : "request_failed");
            checkedAt = now();
          }

          cached = {
            expiresAt: Math.min(checkedAt + CACHE_SECONDS * 1000, build?.expiration || Infinity),
            body: {
              status: build ? "available" : "unavailable",
              version: build?.version || "",
              build_number: build?.buildNumber || "",
              url: TESTFLIGHT_URL,
              checked_at: new Date(checkedAt).toISOString(),
            },
          };
        })().finally(() => { pending = null; });
      }
      await pending;
    }
    return { body: cached.body, cacheSeconds: Math.max(0, Math.floor((cached.expiresAt - now()) / 1000)) };
  };
}

const defaultLookup = createTestFlightLookup();

export function createTestFlightHandler(lookup = defaultLookup, warn = console.warn) {
  return async function handler(req, res) {
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Vercel-CDN-Cache-Control", "no-store");
    res.setHeader("CDN-Cache-Control", "no-store");
    if (req.method !== "GET") {
      res.setHeader("Allow", "GET");
      return res.status(405).json({ error: "Method Not Allowed" });
    }
    try {
      const result = await lookup();
      res.setHeader("Cache-Control", "public, max-age=0, must-revalidate");
      res.setHeader("Vercel-CDN-Cache-Control", `public, max-age=${result.cacheSeconds}`);
      res.setHeader("CDN-Cache-Control", `public, max-age=${result.cacheSeconds}`);
      return res.status(200).json(result.body);
    } catch (error) {
      warn("[testflight] Version lookup failed:", error instanceof TestFlightError ? error.code : "request_failed");
      // Vercel caches 200 responses, so availability is expressed in the JSON body.
      res.setHeader("Cache-Control", "public, max-age=0, must-revalidate");
      res.setHeader("Vercel-CDN-Cache-Control", `public, max-age=${CACHE_SECONDS}`);
      res.setHeader("CDN-Cache-Control", `public, max-age=${CACHE_SECONDS}`);
      return res.status(200).json({ status: "unavailable", version: "", url: TESTFLIGHT_URL });
    }
  };
}

export default createTestFlightHandler();

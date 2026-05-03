const DEFAULT_OPEN_API_BASE = "https://open-api.123pan.com";
const DEFAULT_PLATFORM = "open_platform";
const TOKEN_REFRESH_MARGIN_MS = 60 * 1000;
const LIST_CACHE_TTL_MS = 5 * 60 * 1000;
const PATH_CACHE_TTL_MS = 5 * 60 * 1000;

const tokenCache = {
  accessToken: "",
  expiresAt: 0,
  promise: null,
};

const listCache = new Map();
const pathCache = new Map();

export class Pan123NotFoundError extends Error {
  constructor(message) {
    super(message);
    this.name = "Pan123NotFoundError";
  }
}

export function isPan123NotFound(error) {
  return error instanceof Pan123NotFoundError;
}

// ---------------------------------------------------------------------------
// Upload signing for the feedback channel.
//
// 123 OpenAPI does NOT support a single presigned PUT URL like S3. The upload
// is multi-step: (1) POST /upload/v2/file/create to obtain a `preuploadID` +
// `sliceSize` + slice server URLs, (2) PUT each slice to the slice server,
// (3) poll /upload/v2/file/upload_complete. See `_scripts/publish_pan123_release.py`
// in the gyroflow repo for the canonical Python implementation.
//
// Therefore `signPan123UploadInit` returns the *initialization payload* (the
// access token + create response) and the client (Phase 4) is responsible for
// driving the slice uploads + completion poll. This is a documented deviation
// from the feedback-server-endpoints spec, which assumed a single PUT URL.
// ---------------------------------------------------------------------------

const FEEDBACK_DIR_CACHE = new Map();
const FEEDBACK_DIR_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

// Parse the configured feedback root path (e.g. "/feedback") and return the
// 123 fileID to use as the parent for daily subdirectories.
//
// Resolution order:
//   1. If PAN123_FEEDBACK_ROOT_ID is set as an integer, use it directly.
//   2. Else walk PAN123_FEEDBACK_ROOT path components from the user's root
//      (parent fileId 0 in 123) and locate or create the directory chain.
async function getFeedbackRootFileId() {
  const explicitId = parsePositiveInteger(process.env.PAN123_FEEDBACK_ROOT_ID);
  if (Number.isFinite(explicitId) && explicitId > 0) {
    return explicitId;
  }

  const rootPath = String(process.env.PAN123_FEEDBACK_ROOT || "/feedback").trim();
  const segments = rootPath
    .split("/")
    .map((item) => item.trim())
    .filter(Boolean);
  if (!segments.length) {
    throw new Error("Missing PAN123_FEEDBACK_ROOT or PAN123_FEEDBACK_ROOT_ID");
  }

  let parentId = 0; // 123 user root has id 0.
  for (const name of segments) {
    parentId = await ensureDirectory(parentId, name);
  }
  return parentId;
}

// Find or create a child directory under `parentFileId`. Caches lookups for
// 24h to avoid hammering the 123 list endpoint on every feedback call.
async function ensureDirectory(parentFileId, name) {
  const cacheKey = `${parentFileId}|${name}`;
  const cached = readCache(FEEDBACK_DIR_CACHE, cacheKey);
  if (cached) {
    return cached;
  }

  const existing = await findChildByName(parentFileId, name, 1);
  if (existing) {
    writeCache(FEEDBACK_DIR_CACHE, cacheKey, existing.fileId, FEEDBACK_DIR_CACHE_TTL_MS);
    return existing.fileId;
  }

  const data = await pan123Request("POST", "/upload/v1/file/mkdir", {
    auth: true,
    body: {
      parentID: Number(parentFileId),
      name: String(name),
    },
  });
  const newId = parsePositiveInteger(data.dirID);
  if (!Number.isFinite(newId) || newId <= 0) {
    throw new Error(`123 mkdir failed for ${cacheKey}`);
  }
  // Invalidate the listDirectory cache for this parent so the next read picks
  // up the new child.
  listCache.delete(String(parentFileId));
  writeCache(FEEDBACK_DIR_CACHE, cacheKey, newId, FEEDBACK_DIR_CACHE_TTL_MS);
  return newId;
}

// Sign initialization data for a 123-network upload. Returns:
//   {
//     uploadKind: "pan123_multipart",
//     accessToken,           // 30min OpenAPI bearer; reuse for all calls
//     openApiBase,           // e.g. https://open-api.123pan.com
//     parentFileId,          // numeric id of /<root>/feedback/<yyyymmdd>/
//     parentPath,            // human-readable path
//     filename,              // <id>.zip
//     bucketPath,            // "<root>/feedback/<yyyymmdd>/<id>.zip"
//     expiresAt,             // ISO 8601 of token expiry
//   }
//
// `dailyDir` is the yyyymmdd component (created on demand). `filename` is the
// `<id>.zip`. The client must POST /upload/v2/file/create with parentFileID
// and the file MD5 to obtain the preuploadID and slice servers.
export async function signPan123UploadInit(dailyDir, filename) {
  if (!/^\d{8}$/.test(String(dailyDir || ""))) {
    throw new Error(`Invalid pan123 daily directory: ${dailyDir}`);
  }
  if (!/^[A-Za-z0-9._-]+$/.test(String(filename || ""))) {
    throw new Error(`Invalid pan123 filename: ${filename}`);
  }

  const feedbackRootId = await getFeedbackRootFileId();
  const dailyId = await ensureDirectory(feedbackRootId, dailyDir);

  const accessToken = await getAccessToken();
  const expiresAt = new Date(tokenCache.expiresAt || Date.now() + 30 * 60 * 1000).toISOString();
  const openApiBase = stripTrailingSlash(
    process.env.PAN123_OPEN_API_BASE || DEFAULT_OPEN_API_BASE
  );
  const rootPath = String(process.env.PAN123_FEEDBACK_ROOT || "/feedback").trim();
  const bucketPath = `${stripTrailingSlash(rootPath)}/${dailyDir}/${filename}`;

  return {
    uploadKind: "pan123_multipart",
    accessToken,
    openApiBase,
    parentFileId: dailyId,
    parentPath: `${stripTrailingSlash(rootPath)}/${dailyDir}`,
    filename,
    bucketPath,
    expiresAt,
  };
}

export async function resolvePan123ReleaseDownloadUrl(tag, relativePath) {
  const fileId = await resolveReleasePathToFileId(tag, relativePath);

  try {
    const directUrl = await getDirectLinkUrl(fileId);
    if (directUrl) {
      return directUrl;
    }
  } catch (error) {}

  const fallbackUrl = await getDownloadInfoUrl(fileId);
  if (!fallbackUrl) {
    throw new Error("123 download API returned an empty URL");
  }

  return fallbackUrl;
}

async function resolveReleasePathToFileId(tag, relativePath) {
  const releasesRootId = parsePositiveInteger(process.env.PAN123_RELEASES_ROOT_ID);
  if (!releasesRootId) {
    throw new Error("Missing PAN123_RELEASES_ROOT_ID");
  }

  const normalizedTag = String(tag || "").trim();
  const normalizedPath = normalizeRelativePath(relativePath);
  if (!normalizedTag || !normalizedPath) {
    throw new Pan123NotFoundError("Missing tag or relative path");
  }

  const cacheKey = `${normalizedTag}|${normalizedPath}`;
  const cached = readCache(pathCache, cacheKey);
  if (cached) {
    return cached;
  }

  const segments = [normalizedTag, ...normalizedPath.split("/")];
  let parentFileId = releasesRootId;

  for (let index = 0; index < segments.length; index += 1) {
    const name = segments[index];
    const isLast = index === segments.length - 1;
    const expectedType = isLast ? 0 : 1;
    const item = await findChildByName(parentFileId, name, expectedType);
    if (!item) {
      throw new Pan123NotFoundError(`123 file not found for ${cacheKey}`);
    }
    parentFileId = item.fileId;
  }

  writeCache(pathCache, cacheKey, parentFileId, PATH_CACHE_TTL_MS);
  return parentFileId;
}

async function findChildByName(parentFileId, name, type) {
  const children = await listDirectory(parentFileId);
  return (
    children.find(
      (item) =>
        String(item.filename || "") === name &&
        Number(item.type) === Number(type) &&
        Number(item.trashed || 0) === 0
    ) || null
  );
}

async function listDirectory(parentFileId) {
  const normalizedParent = parsePositiveInteger(parentFileId, true);
  const cached = readCache(listCache, normalizedParent);
  if (cached) {
    return cached;
  }

  const fileList = [];
  let lastFileId = "";

  while (true) {
    const query = {
      parentFileId: normalizedParent,
      limit: 100,
    };
    if (lastFileId !== "" && lastFileId !== -1) {
      query.lastFileId = lastFileId;
    }

    const data = await pan123Request("GET", "/api/v2/file/list", {
      auth: true,
      query,
    });

    const pageFiles = Array.isArray(data.fileList) ? data.fileList : [];
    fileList.push(...pageFiles);

    const nextFileId =
      data.lastFileId === undefined || data.lastFileId === null
        ? -1
        : Number(data.lastFileId);
    if (!Number.isFinite(nextFileId) || nextFileId === -1) {
      break;
    }
    lastFileId = nextFileId;
  }

  writeCache(listCache, normalizedParent, fileList, LIST_CACHE_TTL_MS);
  return fileList;
}

async function getDirectLinkUrl(fileId) {
  const data = await pan123Request("GET", "/api/v1/direct-link/url", {
    auth: true,
    query: { fileID: parsePositiveInteger(fileId) },
  });
  return String(data.url || "").trim();
}

async function getDownloadInfoUrl(fileId) {
  const data = await pan123Request("GET", "/api/v1/file/download_info", {
    auth: true,
    query: { fileId: parsePositiveInteger(fileId) },
  });
  return String(data.downloadUrl || "").trim();
}

async function getAccessToken() {
  const now = Date.now();
  if (
    tokenCache.accessToken &&
    tokenCache.expiresAt - TOKEN_REFRESH_MARGIN_MS > now
  ) {
    return tokenCache.accessToken;
  }

  if (!tokenCache.promise) {
    tokenCache.promise = (async () => {
      const clientId = String(process.env.PAN123_CLIENT_ID || "").trim();
      const clientSecret = String(process.env.PAN123_CLIENT_SECRET || "").trim();
      if (!clientId || !clientSecret) {
        throw new Error("Missing PAN123_CLIENT_ID or PAN123_CLIENT_SECRET");
      }

      const data = await pan123Request("POST", "/api/v1/access_token", {
        auth: false,
        body: {
          clientID: clientId,
          clientSecret,
        },
      });

      const accessToken = String(data.accessToken || "").trim();
      if (!accessToken) {
        throw new Error("123 access token response is missing accessToken");
      }

      tokenCache.accessToken = accessToken;
      tokenCache.expiresAt = Date.parse(String(data.expiredAt || "")) || now + 5 * 60 * 1000;
      return accessToken;
    })().finally(() => {
      tokenCache.promise = null;
    });
  }

  return tokenCache.promise;
}

async function pan123Request(method, path, options = {}) {
  const baseUrl = stripTrailingSlash(process.env.PAN123_OPEN_API_BASE || DEFAULT_OPEN_API_BASE);
  const url = new URL(`${baseUrl}${path}`);
  for (const [key, value] of Object.entries(options.query || {})) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    url.searchParams.set(key, String(value));
  }

  const headers = {
    Platform: DEFAULT_PLATFORM,
    ...(options.headers || {}),
  };

  let body;
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(options.body);
  }

  if (options.auth !== false) {
    headers.Authorization = `Bearer ${await getAccessToken()}`;
  }

  const response = await fetch(url.toString(), {
    method,
    headers,
    body,
  });

  const text = await response.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch (error) {
    payload = null;
  }

  if (!response.ok) {
    throw new Error(
      payload?.message
        ? `123 API ${response.status}: ${payload.message}`
        : `123 API ${response.status}`
    );
  }

  if (!payload || typeof payload !== "object") {
    throw new Error("123 API returned an invalid response body");
  }

  if (Number(payload.code) !== 0) {
    throw new Error(`123 API error ${payload.code}: ${payload.message || "unknown error"}`);
  }

  return payload.data ?? {};
}

function normalizeRelativePath(value) {
  const parts = String(value || "")
    .split("/")
    .map((item) => decodePathPart(item))
    .map((item) => item.trim())
    .filter(Boolean);

  if (!parts.length || parts.some((item) => item === "." || item === "..")) {
    return "";
  }

  return parts.join("/");
}

function decodePathPart(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch (error) {
    return String(value || "");
  }
}

function parsePositiveInteger(value, allowZero = false) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return allowZero ? 0 : NaN;
  }
  const normalized = Math.trunc(numeric);
  if (allowZero && normalized === 0) {
    return 0;
  }
  return normalized > 0 ? normalized : NaN;
}

function stripTrailingSlash(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function readCache(cache, key) {
  const entry = cache.get(String(key));
  if (!entry) {
    return null;
  }
  if (entry.expiresAt <= Date.now()) {
    cache.delete(String(key));
    return null;
  }
  return entry.value;
}

function writeCache(cache, key, value, ttlMs) {
  cache.set(String(key), {
    value,
    expiresAt: Date.now() + ttlMs,
  });
}

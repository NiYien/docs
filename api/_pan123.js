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

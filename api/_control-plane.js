import { getGeo } from "./_geo";

export const DEFAULT_PRODUCT_ID = "gyroflow_niyien";
export const LEGACY_SOURCE_APP_ID = "niyien_tool";
export const CURRENT_SOURCE_APP_ID = "gyroflow_niyien";

const DEFAULT_GLOBAL_RELEASE_BASE = "https://github.com/NiYien/gyroflow/releases/download";
const DEFAULT_CN_RELEASE_BASE = "https://download.niyien.com/releases";
const DEFAULT_GLOBAL_SDK_BASE = "https://api.gyroflow.xyz/sdk";
const DEFAULT_GLOBAL_PLUGINS_BASE =
  "https://github.com/gyroflow/gyroflow-plugins/releases/latest/download";
const DEFAULT_DOWNLOAD_API_BASE = "https://www.niyien.com/api/download";
const DEFAULT_CN_COUNTRIES = ["CN"];
const DEFAULT_LENS_ASSET_NAME = "gyroflow-niyien-lens.cbor.gz";

export function safeJsonParse(value, fallback = null) {
  try {
    return JSON.parse(value);
  } catch (error) {
    return fallback;
  }
}

export function normalizePlatform(value) {
  const platform = String(value || "").trim().toLowerCase();
  if (platform === "macos" || platform === "linux" || platform === "android") {
    return platform;
  }
  return "windows";
}

export function getAppAssetName(platform) {
  switch (normalizePlatform(platform)) {
    case "macos":
      return "gyroflow-niyien-mac-universal.dmg";
    case "linux":
      return "gyroflow-niyien-linux64.AppImage";
    case "android":
      return "gyroflow-niyien.apk";
    case "windows":
    default:
      return "gyroflow-niyien-windows64.zip";
  }
}

export function buildAppUrl(sourceBase, tag, platform) {
  if (!sourceBase || !tag) {
    return "";
  }
  return `${stripTrailingSlash(sourceBase)}/${tag}/${getAppAssetName(platform)}`;
}

export function buildReleaseAssetUrl(sourceBase, tag, filename) {
  if (!sourceBase || !tag || !filename) {
    return "";
  }
  return `${stripTrailingSlash(sourceBase)}/${tag}/${filename}`;
}

export async function getCountry(req) {
  const geo = await getGeo(req, { fallbackCountry: "US" });
  return geo.country || "US";
}

export function getRoutingConfig() {
  const cnCountries =
    parseStringListEnv("NIYIEN_CN_COUNTRIES_JSON", true) ||
    parseStringListEnv("NIYIEN_CN_COUNTRIES", false) ||
    DEFAULT_CN_COUNTRIES;

  return {
    globalBase: stripTrailingSlash(
      process.env.NIYIEN_GLOBAL_RELEASE_BASE || DEFAULT_GLOBAL_RELEASE_BASE
    ),
    cnBase: stripTrailingSlash(process.env.NIYIEN_CN_RELEASE_BASE || DEFAULT_CN_RELEASE_BASE),
    cnCountries: new Set(cnCountries.map((item) => item.toUpperCase())),
  };
}

export function selectSourceForCountry(country) {
  const routing = getRoutingConfig();
  const normalizedCountry = String(country || "").trim().toUpperCase();
  const isCn = routing.cnCountries.has(normalizedCountry);

  return {
    country: normalizedCountry || "US",
    region: isCn ? "cn" : "global",
    selectedSource: isCn ? "cn" : "global",
    base: isCn ? routing.cnBase : routing.globalBase,
  };
}

export function loadReleasePolicy() {
  const fallbackVersion = String(
    process.env.NIYIEN_APP_VERSION || `${process.env.npm_package_version || "1.6.3"}-niyien.1`
  ).trim();
  const fallbackTag = String(process.env.NIYIEN_RELEASE_TAG || `v${fallbackVersion}`).trim();
  const fallback = {
    auto_version: fallbackVersion,
    versions: [
      {
        version: fallbackVersion,
        tag: fallbackTag,
        channels: ["auto", "manual"],
        changelog: String(process.env.NIYIEN_APP_CHANGELOG || "").trim(),
        recommended: true,
      },
    ],
  };

  const raw = String(process.env.NIYIEN_RELEASE_POLICY_JSON || "").trim();
  if (!raw) {
    return fallback;
  }

  const parsed = safeJsonParse(raw, null);
  if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.versions)) {
    return fallback;
  }

  const versions = parsed.versions.map(normalizePolicyEntry).filter(Boolean);
  if (!versions.length) {
    return fallback;
  }

  const autoVersion =
    typeof parsed.auto_version === "string" && parsed.auto_version.trim()
      ? parsed.auto_version.trim()
      : (versions.find((item) => item.channels.includes("auto")) || versions[0]).version;

  if (!versions.some((item) => item.version === autoVersion)) {
    return fallback;
  }

  return {
    auto_version: autoVersion,
    versions,
  };
}

export async function buildManifestPayload(req) {
  const geo = await getGeo(req, { fallbackCountry: "US" });
  const country = geo.country || "US";
  const platform = normalizePlatform(req?.query?.platform);
  const requestedAppVersion = String(req?.query?.app_version || "").trim();
  const source = selectSourceForCountry(country);
  const releasePolicy = loadReleasePolicy();
  const autoEntry =
    releasePolicy.versions.find((item) => item.version === releasePolicy.auto_version) ||
    releasePolicy.versions[0] ||
    null;
  const requestEntry =
    releasePolicy.versions.find((item) => item.version === requestedAppVersion) || null;
  const resolvedEntry = requestEntry || autoEntry;
  const resolvedContentTag = String(
    resolvedEntry?.content_tag || process.env.NIYIEN_CONTENT_RELEASE_TAG || autoEntry?.content_tag || ""
  ).trim();
  const resolvedPluginSourceMode = String(
    resolvedEntry?.plugins_source_mode || process.env.NIYIEN_PLUGINS_SOURCE_MODE || "release"
  )
    .trim()
    .toLowerCase();
  const resolvedPluginSourceRef = String(resolvedEntry?.plugins_source_ref || "").trim();
  const resolvedPluginSourceTag = String(resolvedEntry?.plugins_source_tag || "").trim();
  const resolvedLensVersion = coerceScalarValue(
    resolvedEntry?.lens_version ?? process.env.NIYIEN_LENS_VERSION ?? ""
  );
  const resolvedLensSha = String(
    resolvedEntry?.lens_sha256 || process.env.NIYIEN_LENS_SHA256 || ""
  ).trim();
  const manualVersions = releasePolicy.versions
    .filter((item) => item.channels.includes("manual"))
    .map((item) => ({
      version: item.version,
      url:
        source.region === "cn"
          ? buildDownloadApiUrl(req, "app", item.tag, getAppAssetName(platform))
          : resolveGlobalAppUrl(item, source.base, platform),
      changelog: item.changelog,
      recommended: item.recommended,
    }));

  let appUrl = "";
  let lensUrl = "";
  let sdkBase = "";
  let pluginsBase = "";

  if (source.region === "cn") {
    appUrl = autoEntry ? buildDownloadApiUrl(req, "app", autoEntry.tag, getAppAssetName(platform)) : "";
    lensUrl = resolvedContentTag
      ? buildDownloadApiUrl(req, "content", resolvedContentTag, getLensAssetName())
      : "";
    sdkBase = resolvedContentTag
      ? `${buildDownloadApiUrl(req, "content", resolvedContentTag, "sdk")}/`
      : "";
    pluginsBase = resolvedContentTag
      ? `${buildDownloadApiUrl(req, "content", resolvedContentTag, "plugins")}/`
      : "";
  } else {
    const resolvedAppSourceMode = String(resolvedEntry?.app_source_mode || "release").trim().toLowerCase();
    const resolvedLensTag = resolvedEntry?.tag || autoEntry?.tag || "";
    appUrl = autoEntry ? resolveGlobalAppUrl(autoEntry, source.base, platform) : "";
    if (resolvedAppSourceMode === "artifact" && resolvedContentTag) {
      lensUrl = buildDownloadApiUrl(req, "content", resolvedContentTag, getLensAssetName());
      sdkBase = `${buildDownloadApiUrl(req, "content", resolvedContentTag, "sdk")}/`;
    } else {
      lensUrl = resolvedLensTag
        ? buildReleaseAssetUrl(source.base, resolvedLensTag, getLensAssetName())
        : "";
      sdkBase = `${stripTrailingSlash(
        process.env.NIYIEN_GLOBAL_SDK_BASE || DEFAULT_GLOBAL_SDK_BASE
      )}/`;
    }
    pluginsBase =
      resolvedPluginSourceMode === "artifact" && resolvedContentTag
        ? `${buildDownloadApiUrl(req, "content", resolvedContentTag, "plugins")}/`
        : `${stripTrailingSlash(
            resolvedEntry?.global_plugins_base ||
              process.env.NIYIEN_GLOBAL_PLUGINS_BASE ||
              DEFAULT_GLOBAL_PLUGINS_BASE
          )}/`;
  }

  return {
    country: source.country,
    country_source: geo.source || "",
    city: geo.city || "Unknown",
    region: source.region,
    selected_source: source.selectedSource,
    product_id: DEFAULT_PRODUCT_ID,
    app: {
      version: autoEntry?.version || "",
      url: appUrl,
      changelog: autoEntry?.changelog || "",
      manual_versions: manualVersions,
    },
    lens: {
      version: resolvedLensVersion,
      url: lensUrl,
      sha256: resolvedLensSha,
    },
    sdk_base: sdkBase,
    plugins_base: pluginsBase,
    plugins_source_mode: resolvedPluginSourceMode,
    plugins_source_ref: resolvedPluginSourceRef,
    plugins_source_tag: resolvedPluginSourceTag,
  };
}

function normalizePolicyEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  const version = String(entry.version || "").trim();
  const tag = String(entry.tag || (version ? `v${version}` : "")).trim();
  if (!version || !tag) {
    return null;
  }

  return {
    version,
    tag,
    channels: normalizeChannels(entry.channels),
    changelog: typeof entry.changelog === "string" ? entry.changelog.trim() : "",
    recommended: Boolean(entry.recommended),
    app_source_mode:
      typeof entry.app_source_mode === "string" && entry.app_source_mode.trim()
        ? entry.app_source_mode.trim().toLowerCase()
        : "release",
    app_urls: normalizeAppUrls(entry.app_urls),
    content_tag: typeof entry.content_tag === "string" ? entry.content_tag.trim() : "",
    lens_version:
      entry.lens_version === undefined || entry.lens_version === null || entry.lens_version === ""
        ? ""
        : coerceScalarValue(entry.lens_version),
    lens_sha256: typeof entry.lens_sha256 === "string" ? entry.lens_sha256.trim() : "",
    plugins_source_mode:
      typeof entry.plugins_source_mode === "string" && entry.plugins_source_mode.trim()
        ? entry.plugins_source_mode.trim().toLowerCase()
        : "",
    plugins_source_ref:
      typeof entry.plugins_source_ref === "string" ? entry.plugins_source_ref.trim() : "",
    plugins_source_tag:
      typeof entry.plugins_source_tag === "string" ? entry.plugins_source_tag.trim() : "",
    global_plugins_base:
      typeof entry.global_plugins_base === "string" ? entry.global_plugins_base.trim() : "",
  };
}

function normalizeAppUrls(value) {
  if (!value || typeof value !== "object") {
    return {};
  }
  const result = {};
  for (const [platform, url] of Object.entries(value)) {
    const key = normalizePlatform(platform);
    const normalizedUrl = String(url || "").trim();
    if (normalizedUrl) {
      result[key] = normalizedUrl;
    }
  }
  return result;
}

function resolveGlobalAppUrl(entry, sourceBase, platform) {
  if (
    entry &&
    String(entry.app_source_mode || "").trim().toLowerCase() === "artifact" &&
    entry.app_urls &&
    typeof entry.app_urls === "object"
  ) {
    const artifactUrl = String(entry.app_urls[normalizePlatform(platform)] || "").trim();
    if (artifactUrl) {
      return artifactUrl;
    }
  }
  return buildAppUrl(sourceBase, entry?.tag || "", platform);
}

function normalizeChannels(channels) {
  if (!Array.isArray(channels) || !channels.length) {
    return ["manual"];
  }

  const values = Array.from(
    new Set(
      channels
        .map((item) => String(item || "").trim().toLowerCase())
        .filter((item) => item === "auto" || item === "manual")
    )
  );

  return values.length ? values : ["manual"];
}

function parseStringListEnv(name, expectJson) {
  const raw = String(process.env[name] || "").trim();
  if (!raw) {
    return null;
  }

  if (expectJson) {
    const parsed = safeJsonParse(raw, null);
    if (Array.isArray(parsed)) {
      return parsed
        .map((item) => String(item || "").trim())
        .filter((item) => item.length > 0);
    }
    return null;
  }

  return raw
    .split(/[\s,]+/)
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function stripTrailingSlash(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function getRequestOrigin(req) {
  const host = String(req?.headers?.host || "").trim();
  const protocol = String(req?.headers?.["x-forwarded-proto"] || "https")
    .split(",")[0]
    .trim();
  if (!host) {
    return "https://www.niyien.com";
  }
  return `${protocol || "https"}://${host}`;
}

function getDownloadApiBase(req) {
  const envBase = stripTrailingSlash(process.env.NIYIEN_DOWNLOAD_API_BASE || "");
  if (envBase) {
    return envBase;
  }
  return `${stripTrailingSlash(getRequestOrigin(req))}/api/download`;
}

function buildDownloadApiUrl(req, scope, tag, relativePath) {
  if (!scope || !tag || !relativePath) {
    return "";
  }
  const encodedTag = encodeURIComponent(String(tag).trim());
  const encodedPath = String(relativePath)
    .split("/")
    .map((item) => encodeURIComponent(String(item)))
    .join("/");
  return `${getDownloadApiBase(req)}/${scope}/${encodedTag}/${encodedPath}`;
}

function getLensAssetName() {
  return String(process.env.NIYIEN_LENS_ASSET_NAME || DEFAULT_LENS_ASSET_NAME).trim();
}

function coerceScalarValue(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (/^-?\d+$/.test(text)) {
    return parseInt(text, 10);
  }
  if (/^-?\d+\.\d+$/.test(text)) {
    return Number(text);
  }
  return text;
}

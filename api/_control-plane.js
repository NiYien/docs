import { getGeo } from "./_geo";

export const DEFAULT_PRODUCT_ID = "gyroflow_niyien";
export const LEGACY_SOURCE_APP_ID = "niyien_tool";
export const CURRENT_SOURCE_APP_ID = "gyroflow_niyien";

const DEFAULT_GLOBAL_RELEASE_BASE = "https://github.com/NiYien/gyroflow/releases/download";
const DEFAULT_CN_RELEASE_BASE = "https://download.niyien.com/releases";
const DEFAULT_GLOBAL_SDK_BASE = "https://api.gyroflow.xyz/sdk";
const DEFAULT_GLOBAL_PLUGINS_BASE =
  "https://github.com/NiYien/gyroflow-plugins/releases/latest/download";
const DEFAULT_DOWNLOAD_API_BASE = "https://www.niyien.com/api/download";
const DEFAULT_GLOBAL_NIGHTLY_BASE = "https://nightly.link/NiYien/gyroflow/actions/runs";
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
      return "gyroflow-niyien-windows64-setup.exe";
  }
}

export function getAppPackageAssetName(platform) {
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

export function getAppInstallerAssetName(platform) {
  return normalizePlatform(platform) === "windows"
    ? "gyroflow-niyien-windows64-setup.exe"
    : "";
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
  // gyroflow client deserializes lens.version as u64 — coerceScalarValue
  // returns "" when no source has a value, which breaks `serde_json` parse
  // ("expected u64, got string"). Coerce empty string to 0 so the client
  // can parse the manifest cleanly and fall through to its own defaults.
  const lensVersionRaw = coerceScalarValue(
    resolvedEntry?.lens_version ?? process.env.NIYIEN_LENS_VERSION ?? ""
  );
  const resolvedLensVersion = typeof lensVersionRaw === "number" ? lensVersionRaw : 0;
  const resolvedLensSha = String(
    resolvedEntry?.lens_sha256 || process.env.NIYIEN_LENS_SHA256 || ""
  ).trim();
  const manualVersions = releasePolicy.versions
    .filter((item) => item.channels.includes("manual"))
    .map((item) => {
      const manualPackage = withAbsolutePackageUrls(
        req,
        buildPlatformPackage(req, item, source, platform)
      );
      const manualPackages = Object.keys(manualPackage).length
        ? { [platform]: manualPackage }
        : {};
      return {
        version: item.version,
        url: manualPackage.installer_url || manualPackage.package_url || "",
        changelog: item.changelog,
        recommended: item.recommended,
        packages: manualPackages,
      };
    });
  const platformPackage = withAbsolutePackageUrls(
    req,
    buildPlatformPackage(req, autoEntry, source, platform)
  );
  const appPackages = Object.keys(platformPackage).length ? { [platform]: platformPackage } : {};
  let appUrl = platformPackage.installer_url || platformPackage.package_url || "";
  let lensUrl = "";
  let sdkBase = "";
  let pluginsBase = "";

  if (source.region === "cn") {
    lensUrl = resolvedContentTag
      ? buildDownloadApiUrl(req, "content", resolvedContentTag, getLensAssetName())
      : "";
    // SDK is shared across releases (publish_pan123_release.py uploads to
    // a flat `releases/sdk/` directory rather than per-release
    // `content-{hash}/sdk/`), so its base URL has no content_tag segment.
    // The download rewrite `/api/download/content/sdk/<file>` resolves to
    // RELEASES_ROOT/sdk/<file> via _pan123.js's segment-walk.
    sdkBase = `${getDownloadApiBase(req)}/content/sdk/`;
    pluginsBase = resolvedContentTag
      ? `${buildDownloadApiUrl(req, "content", resolvedContentTag, "plugins")}/`
      : "";
  } else {
    const resolvedAppSourceMode = String(resolvedEntry?.app_source_mode || "release").trim().toLowerCase();
    const resolvedLensTag = resolvedEntry?.tag || autoEntry?.tag || "";
    if (resolvedAppSourceMode === "artifact" && resolvedContentTag) {
      lensUrl = buildDownloadApiUrl(req, "content", resolvedContentTag, getLensAssetName());
      // Same flat shared-SDK layout as cn — see comment above.
      sdkBase = `${getDownloadApiBase(req)}/content/sdk/`;
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

  appUrl = toAbsoluteManifestUrl(req, appUrl);
  lensUrl = toAbsoluteManifestUrl(req, lensUrl);
  sdkBase = toAbsoluteManifestUrl(req, sdkBase);
  pluginsBase = toAbsoluteManifestUrl(req, pluginsBase);

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
      packages: appPackages,
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
    packages: normalizePackages(entry.packages),
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
  for (const [platform, rawValue] of Object.entries(value)) {
    const key = normalizePlatform(platform);
    if (typeof rawValue === "string") {
      const packageUrl = rawValue.trim();
      if (packageUrl) {
        result[key] = { package_url: packageUrl };
      }
      continue;
    }
    if (rawValue && typeof rawValue === "object") {
      const installerUrl = String(rawValue.installer_url || "").trim();
      const packageUrl = String(rawValue.package_url || rawValue.url || "").trim();
      if (installerUrl || packageUrl) {
        result[key] = {};
        if (installerUrl) {
          result[key].installer_url = installerUrl;
        }
        if (packageUrl) {
          result[key].package_url = packageUrl;
        }
      }
    }
  }
  return result;
}

function normalizePackages(value) {
  if (!value || typeof value !== "object") {
    return {};
  }
  const result = {};
  for (const [platform, rawValue] of Object.entries(value)) {
    const key = normalizePlatform(platform);
    if (!rawValue || typeof rawValue !== "object") {
      continue;
    }
    const normalized = {
      kind: String(rawValue.kind || defaultPackageKind(key)).trim(),
      installer_filename: String(rawValue.installer_filename || "").trim(),
      installer_sha256: String(rawValue.installer_sha256 || "").trim().toLowerCase(),
      installer_size: coercePositiveInteger(rawValue.installer_size),
      package_filename: String(rawValue.package_filename || "").trim(),
      package_sha256: String(rawValue.package_sha256 || "").trim().toLowerCase(),
      package_size: coercePositiveInteger(rawValue.package_size),
    };
    result[key] = normalized;
  }
  return result;
}

function buildPlatformPackage(req, entry, source, platform) {
  const key = normalizePlatform(platform);
  if (!entry) {
    return {};
  }

  const metadata = entry.packages?.[key] || {};
  const urls = resolvePlatformPackageUrls(req, entry, source, key, metadata);

  if (key === "windows") {
    return {
      kind: metadata.kind || "web_installer_zip",
      installer_url: urls.installer_url || "",
      installer_sha256: metadata.installer_sha256 || "",
      installer_size: metadata.installer_size || 0,
      package_url: urls.package_url || "",
      package_sha256: metadata.package_sha256 || "",
      package_size: metadata.package_size || 0,
    };
  }

  return {
    kind: metadata.kind || defaultPackageKind(key),
    package_url: urls.package_url || "",
    package_sha256: metadata.package_sha256 || "",
    package_size: metadata.package_size || 0,
  };
}

function withAbsolutePackageUrls(req, platformPackage) {
  if (!platformPackage || typeof platformPackage !== "object") {
    return {};
  }
  const result = { ...platformPackage };
  if ("installer_url" in result) {
    result.installer_url = toAbsoluteManifestUrl(req, result.installer_url || "");
  }
  if ("package_url" in result) {
    result.package_url = toAbsoluteManifestUrl(req, result.package_url || "");
  }
  return result;
}

function resolvePlatformPackageUrls(req, entry, source, platform, metadata) {
  if (!entry?.tag) {
    return {};
  }

  if (source.region === "cn") {
    return {
      installer_url: getAppInstallerAssetName(platform)
        ? buildDownloadApiUrl(req, "app", entry.tag, metadata.installer_filename || getAppInstallerAssetName(platform))
        : "",
      package_url: buildDownloadApiUrl(
        req,
        "app",
        entry.tag,
        metadata.package_filename || getAppPackageAssetName(platform)
      ),
    };
  }

  if (String(entry.app_source_mode || "").trim().toLowerCase() === "artifact") {
    const artifactUrls = entry.app_urls?.[platform] || {};
    if (artifactUrls.installer_url || artifactUrls.package_url) {
      return {
        installer_url: toAbsoluteManifestUrl(req, artifactUrls.installer_url || ""),
        package_url: toAbsoluteManifestUrl(req, artifactUrls.package_url || ""),
      };
    }

    // GLOBAL nightly: route to nightly.link proxy.
    // entry.tag is "actions-run-{run_id}" (set by publish_pan123_release.py
    // resolve_app_source when mode == artifact).
    const runIdMatch = String(entry.tag || "").match(/^(?:actions-run-|run-)(\d+)$/);
    if (runIdMatch) {
      const runId = runIdMatch[1];
      const nightlyBase = stripTrailingSlash(
        process.env.NIYIEN_GLOBAL_NIGHTLY_BASE || DEFAULT_GLOBAL_NIGHTLY_BASE
      );
      const installerName = metadata.installer_filename || getAppInstallerAssetName(platform);
      const packageName = metadata.package_filename || getAppPackageAssetName(platform);
      return {
        installer_url: installerName ? `${nightlyBase}/${runId}/${installerName}.zip` : "",
        package_url: `${nightlyBase}/${runId}/${packageName}.zip`,
      };
    }
    return { installer_url: "", package_url: "" };
  }

  return {
    installer_url: getAppInstallerAssetName(platform)
      ? buildReleaseAssetUrl(source.base, entry.tag, metadata.installer_filename || getAppInstallerAssetName(platform))
      : "",
    package_url: buildReleaseAssetUrl(
      source.base,
      entry.tag,
      metadata.package_filename || getAppPackageAssetName(platform)
    ),
  };
}

function defaultPackageKind(platform) {
  return normalizePlatform(platform) === "windows" ? "web_installer_zip" : "dmg";
}

function coercePositiveInteger(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 0;
  }
  return Math.trunc(numeric);
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
    if (/^[a-z][a-z0-9+.-]*:\/\//i.test(envBase)) {
      return envBase;
    }
    const origin = getManifestUrlOrigin(req);
    if (envBase.startsWith("/")) {
      return `${origin}${envBase}`;
    }
    return `${origin}/${envBase.replace(/^\/+/, "")}`;
  }
  return `${stripTrailingSlash(getRequestOrigin(req))}/api/download`;
}

function toAbsoluteManifestUrl(req, value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) {
    return raw;
  }

  const origin = getManifestUrlOrigin(req);
  if (raw.startsWith("/api/download/") || raw.startsWith("/")) {
    return `${origin}${raw}`;
  }
  return `${getDownloadApiBase(req)}/${raw.replace(/^\/+/, "")}`;
}

function getManifestUrlOrigin(req) {
  const envBase = stripTrailingSlash(process.env.NIYIEN_DOWNLOAD_API_BASE || "");
  if (envBase) {
    try {
      return new URL(envBase).origin;
    } catch (error) {}
  }
  return stripTrailingSlash(getRequestOrigin(req));
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

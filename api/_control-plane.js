import { getGeo } from "./_geo";

export const DEFAULT_PRODUCT_ID = "gyroflow_niyien";
export const LEGACY_SOURCE_APP_ID = "niyien_tool";
export const CURRENT_SOURCE_APP_ID = "gyroflow_niyien";

const DEFAULT_GLOBAL_RELEASE_BASE = "https://github.com/NiYien/gyroflow/releases/download";
const DEFAULT_CN_RELEASE_BASE = "https://download.niyien.com/releases";
const DEFAULT_GLOBAL_SDK_BASE = "https://www.niyien.com/api/sdk";
const DEFAULT_GLOBAL_PLUGINS_BASE =
  "https://github.com/NiYien/gyroflow-plugins/releases/latest/download";
// Lens data lives in a separate niyien-lens-data repo since 2026-04-21 (code+data
// split). Global lens.url always points here, regardless of app publish mode.
const DEFAULT_LENS_DATA_RELEASE_BASE =
  "https://github.com/NiYien/niyien-lens-data/releases/download";
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

export function getAppArchiveAssetName(platform) {
  return normalizePlatform(platform) === "linux"
    ? "gyroflow-niyien-linux64.tar.gz"
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

// Env-only fallback. We intentionally do not hardcode any default version
// here -- a hardcoded fallback resurrects deleted releases when ops clears
// NIYIEN_RELEASE_POLICY_JSON to retire a version. Returning an empty policy
// instead lets the manifest expose app.version="" so the client treats it
// as "no release available".
function buildEnvFallback() {
  const envVersion = String(process.env.NIYIEN_APP_VERSION || "").trim();
  if (!envVersion) {
    return { auto_version: "", versions: [], hidden_plugins: [] };
  }
  const envTag = String(process.env.NIYIEN_RELEASE_TAG || `v${envVersion}`).trim();
  return {
    auto_version: envVersion,
    versions: [
      {
        version: envVersion,
        tag: envTag,
        channels: ["auto", "manual"],
        changelog: String(process.env.NIYIEN_APP_CHANGELOG || "").trim(),
        recommended: true,
      },
    ],
    hidden_plugins: [],
  };
}

export function loadReleasePolicy() {
  const raw = String(process.env.NIYIEN_RELEASE_POLICY_JSON || "").trim();
  if (!raw) {
    return buildEnvFallback();
  }

  const parsed = safeJsonParse(raw, null);
  if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.versions)) {
    return buildEnvFallback();
  }

  const versions = parsed.versions.map(normalizePolicyEntry).filter(Boolean);
  if (!versions.length) {
    return buildEnvFallback();
  }

  const autoVersion =
    typeof parsed.auto_version === "string" && parsed.auto_version.trim()
      ? parsed.auto_version.trim()
      : (versions.find((item) => item.channels.includes("auto")) || versions[0]).version;

  if (!versions.some((item) => item.version === autoVersion)) {
    return buildEnvFallback();
  }

  // hidden_plugins[] is a top-level plugin blacklist. Each row is shaped
  // {kind: "release", ref: <tag>} or {kind: "artifact", run_id: <int>}.
  // Defaults to [] when missing from older policy values.
  const hiddenPlugins = Array.isArray(parsed.hidden_plugins)
    ? parsed.hidden_plugins.filter((row) => row && typeof row === "object")
    : [];

  return {
    auto_version: autoVersion,
    versions,
    hidden_plugins: hiddenPlugins,
  };
}

// Canonical plugin identity for a policy entry: {kind: "release", ref: <tag>}
// for release-mode entries, {kind: "artifact", run_id: <int>} for
// artifact-mode entries (parsed from `actions-run-<id>` in plugins_source_ref),
// or null when the entry has no plugin info. Mirrors the gyroflow backend
// helper `Api._canonical_plugin_key` so both sides agree on the matcher.
function canonicalPluginKey(entry) {
  if (!entry || typeof entry !== "object") return null;
  const mode = String(entry.plugins_source_mode || "").trim().toLowerCase();
  if (mode === "artifact") {
    const ref = String(entry.plugins_source_ref || "").trim();
    const prefix = "actions-run-";
    if (ref.startsWith(prefix)) {
      const n = parseInt(ref.slice(prefix.length), 10);
      if (Number.isFinite(n)) return { kind: "artifact", run_id: n };
    }
    return null;
  }
  const tag = String(entry.plugin_tag || "").trim();
  if (tag) return { kind: "release", ref: tag };
  return null;
}

function pluginKeysMatch(a, b) {
  if (!a || !b) return false;
  if (a.kind !== b.kind) return false;
  if (a.kind === "release") return String(a.ref || "") === String(b.ref || "");
  if (a.kind === "artifact") {
    const an = parseInt(a.run_id, 10);
    const bn = parseInt(b.run_id, 10);
    return Number.isFinite(an) && Number.isFinite(bn) && an === bn;
  }
  return false;
}

function isEntryPluginHidden(entry, hiddenPlugins) {
  const key = canonicalPluginKey(entry);
  if (!key || !Array.isArray(hiddenPlugins) || !hiddenPlugins.length) return false;
  return hiddenPlugins.some((row) => pluginKeysMatch(key, row));
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
  // Decoupled bundle layout: lens lives in `releases/lens-<sha12>/`, plugin in
  // `releases/plugin-<sha12>/`. Each is independent — a publish that updates
  // only one of them leaves the other untouched.
  const resolvedLensTagBundle = String(
    resolvedEntry?.lens_tag || process.env.NIYIEN_LENS_RELEASE_TAG || autoEntry?.lens_tag || ""
  ).trim();
  // Per-entry plugin blacklist check. When the resolved entry's plugin
  // identity (release tag or artifact run_id) is in policy.hidden_plugins[],
  // we skip the entry's plugin fields and fall back to env / autoEntry,
  // and also skip autoEntry's plugin if the autoEntry itself is the one
  // that's hidden — otherwise hiding the auto entry's plugin would have
  // no effect on most clients.
  const hiddenPlugins = releasePolicy.hidden_plugins || [];
  const resolvedEntryPluginHidden = isEntryPluginHidden(resolvedEntry, hiddenPlugins);
  const autoEntryPluginHidden = isEntryPluginHidden(autoEntry, hiddenPlugins);
  const pluginEntryFor = (preferResolved) => {
    // Choose which entry's plugin fields to read. When the requested
    // entry's plugin is hidden, fall through to autoEntry; if autoEntry
    // is also hidden, fall through to env / defaults below.
    if (preferResolved && resolvedEntry && !resolvedEntryPluginHidden) return resolvedEntry;
    if (autoEntry && !autoEntryPluginHidden) return autoEntry;
    return null;
  };
  const pluginEntry = pluginEntryFor(true);

  const resolvedPluginTagBundle = String(
    pluginEntry?.plugin_tag || process.env.NIYIEN_PLUGIN_RELEASE_TAG || ""
  ).trim();
  const resolvedLensReleaseTag = String(
    resolvedEntry?.lens_release_tag || autoEntry?.lens_release_tag || ""
  ).trim();
  const resolvedPluginSourceMode = String(
    pluginEntry?.plugins_source_mode || process.env.NIYIEN_PLUGINS_SOURCE_MODE || "release"
  )
    .trim()
    .toLowerCase();
  const resolvedPluginSourceRef = String(pluginEntry?.plugins_source_ref || "").trim();
  const resolvedPluginSourceTag = String(pluginEntry?.plugins_source_tag || "").trim();
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
  // Only versions strictly ABOVE the auto version belong here: the client
  // renders this list as the "manual test build" row next to the stable
  // one, and anything at or below the auto version would either restate
  // the stable row or offer a downgrade. Their notes are not lost — they
  // are pre-joined into app.changelog/app.changelogs below.
  const manualVersions = releasePolicy.versions
    .filter(
      (item) =>
        item.channels.includes("manual") &&
        (!autoEntry || compareAppVersions(item.version, autoEntry.version) > 0)
    )
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
        // release-notes-i18n: per-language release notes. Empty {} for
        // legacy entries written before this change; clients use
        // `pick_changelog` to choose by locale and fall back to legacy
        // `changelog` when the map is empty.
        changelogs: item.changelogs || {},
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

  // Build a directory base URL under /api/download/content/<tag>/. Each
  // segment is url-encoded; trailing slash is included so client-side
  // `pluginsBase + filename` and `lensBase + filename` produce valid URLs.
  const buildContentDirUrl = (tag) => {
    if (!tag) return "";
    const encoded = String(tag).split("/").map(encodeURIComponent).join("/");
    return `${getDownloadApiBase(req)}/content/${encoded}/`;
  };
  if (source.region === "cn") {
    // CN clients get URLs proxied through `/api/download/content/<tag>/<file>`,
    // which _pan123.js resolves by walking RELEASES_ROOT children. The bundle
    // layout (`lens-<sha12>/`, `plugin-<sha12>/`, `sdk/`) is just a top-level
    // dir name; the segment walker handles any shape.
    lensUrl = resolvedLensTagBundle
      ? buildDownloadApiUrl(req, "content", resolvedLensTagBundle, getLensAssetName())
      : "";
    // SDK is shared across releases (publish_pan123_release.py uploads to a
    // flat `releases/sdk/` directory), so its base URL has no tag segment.
    sdkBase = `${getDownloadApiBase(req)}/content/sdk/`;
    pluginsBase = buildContentDirUrl(resolvedPluginTagBundle);
  } else {
    // Global users always go through GitHub releases / nightly.link. Lens has
    // only one source (niyien-lens-data release); SDK is always the static
    // CDN; plugins follow plugins_source_mode (release latest vs nightly.link
    // for the specific run captured in entry.global_plugins_base).
    sdkBase = `${stripTrailingSlash(
      resolvedEntry?.global_sdk_base ||
        autoEntry?.global_sdk_base ||
        process.env.NIYIEN_GLOBAL_SDK_BASE ||
        DEFAULT_GLOBAL_SDK_BASE
    )}/`;
    lensUrl = resolvedLensReleaseTag
      ? buildReleaseAssetUrl(getLensDataReleaseBase(), resolvedLensReleaseTag, getLensAssetName())
      : "";
    pluginsBase = `${stripTrailingSlash(
      pluginEntry?.global_plugins_base ||
        process.env.NIYIEN_GLOBAL_PLUGINS_BASE ||
        DEFAULT_GLOBAL_PLUGINS_BASE
    )}/`;
  }

  // The stable row's aggregation window is empty now that manual_versions
  // starts above the auto version, so its sections are joined here and the
  // client's fallback path renders them as-is.
  const stableChangelogs = buildStableChangelogs(releasePolicy.versions, autoEntry);

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
      changelog: stableChangelogs.changelog,
      // release-notes-i18n: same fallback semantics as the manual versions —
      // clients pick by locale, fall back to `changelog` when empty.
      changelogs: stableChangelogs.changelogs,
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
    changelogs: normalizeChangelogs(entry.changelogs),
    recommended: Boolean(entry.recommended),
    app_source_mode:
      typeof entry.app_source_mode === "string" && entry.app_source_mode.trim()
        ? entry.app_source_mode.trim().toLowerCase()
        : "release",
    app_urls: normalizeAppUrls(entry.app_urls),
    packages: normalizePackages(entry.packages),
    content_tag: typeof entry.content_tag === "string" ? entry.content_tag.trim() : "",
    lens_tag: typeof entry.lens_tag === "string" ? entry.lens_tag.trim() : "",
    plugin_tag: typeof entry.plugin_tag === "string" ? entry.plugin_tag.trim() : "",
    lens_release_tag:
      typeof entry.lens_release_tag === "string" ? entry.lens_release_tag.trim() : "",
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
    global_sdk_base:
      typeof entry.global_sdk_base === "string" ? entry.global_sdk_base.trim() : "",
    global_plugins_base:
      typeof entry.global_plugins_base === "string" ? entry.global_plugins_base.trim() : "",
  };
}

// Multi-language release notes (release-notes-i18n). Input is a free-form
// map authored by the publisher's 9-language tabs; here we coerce it into
// a clean { lang_code: text } shape, dropping non-string entries and
// stripping whitespace-only values. The contract is: only languages the
// publisher actually filled survive into the manifest, so clients can
// rely on `key present` <=> `non-empty content`.
function normalizeChangelogs(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const result = {};
  for (const [code, text] of Object.entries(value)) {
    if (typeof code !== "string" || !code) {
      continue;
    }
    if (typeof text !== "string") {
      continue;
    }
    if (!text.trim()) {
      continue;
    }
    result[code] = text;
  }
  return result;
}

// ---- Update-dialog release notes (mirrors the gyroflow client) ----
//
// The client's update dialog shows at most two rows: the auto ("stable")
// channel and the newest manual one. It builds each row's notes by
// aggregating entries of `app.manual_versions[]` inside a window that
// starts at the RUNNING build and ends at that row's target. Both windows
// therefore start at the same place, and the manual one contains the
// stable one — so every section the stable row shows was repeated under
// the manual heading, including versions older than the stable target.
//
// Two coupled decisions keep the rows disjoint here, on the server, so
// already-installed clients get the fix without an app update:
//
//   1. `manual_versions[]` carries only versions strictly ABOVE the auto
//      version, leaving the manual row with exactly what the stable row
//      does not already cover.
//   2. That empties the stable row's own aggregation window, so its
//      sections are pre-joined into `app.changelog` / `app.changelogs`
//      here. The client finds no entries, takes its single-entry
//      fallback path, and renders the pre-joined text verbatim.
//
// Accepted consequence of (2): the stable row no longer trims to "the
// versions YOU skipped" — every client sees the same last-N sections
// regardless of which build it upgrades from.

// Mirrors AGGREGATED_CHANGELOG_MAX_VERSIONS in src/distribution.rs.
const AGGREGATED_CHANGELOG_MAX_VERSIONS = 5;

// Higher = newer at the same base. Mirrors `schema_priority` in the client.
const VERSION_SCHEMA_PRIORITY = { ni: 2, dev: 1 };

// Mirrors `parse_app_version`: strict `major.minor.patch` triple plus an
// optional `-<schema>.<sequence>` suffix. Returns null when unparseable.
function parseAppVersion(version) {
  const trimmed = String(version || "").trim().replace(/^v+/, "");
  if (!trimmed) {
    return null;
  }
  const dash = trimmed.indexOf("-");
  const baseStr = dash === -1 ? trimmed : trimmed.slice(0, dash);
  const suffixRaw = dash === -1 ? null : trimmed.slice(dash + 1);
  const parts = baseStr.split(".");
  if (parts.length !== 3 || parts.some((part) => !/^\d+$/.test(part))) {
    return null;
  }
  let suffix = null;
  if (suffixRaw !== null) {
    const dot = suffixRaw.indexOf(".");
    const schema = dot === -1 ? suffixRaw : suffixRaw.slice(0, dot);
    const seqStr = dot === -1 ? "" : suffixRaw.slice(dot + 1);
    suffix = {
      schema,
      sequence: /^\d+$/.test(seqStr) ? Number(seqStr) : null,
      raw: suffixRaw,
    };
  }
  return { base: parts.map(Number), suffix };
}

function cmpParsedVersions(a, b) {
  for (let i = 0; i < 3; i += 1) {
    if (a.base[i] !== b.base[i]) {
      return a.base[i] < b.base[i] ? -1 : 1;
    }
  }
  // Same base: the bare version is that base's FIRST release, so any
  // suffixed build of the same base is newer.
  if (!a.suffix && !b.suffix) return 0;
  if (!a.suffix) return -1;
  if (!b.suffix) return 1;
  const pa = VERSION_SCHEMA_PRIORITY[a.suffix.schema] || 0;
  const pb = VERSION_SCHEMA_PRIORITY[b.suffix.schema] || 0;
  if (pa !== pb) return pa < pb ? -1 : 1;
  if (a.suffix.sequence !== null && b.suffix.sequence !== null) {
    if (a.suffix.sequence === b.suffix.sequence) return 0;
    return a.suffix.sequence < b.suffix.sequence ? -1 : 1;
  }
  if (a.suffix.raw === b.suffix.raw) return 0;
  return a.suffix.raw < b.suffix.raw ? -1 : 1;
}

// Mirrors `compare_app_versions`: parseable beats unparseable, two
// unparseable ones fall back to plain string order.
function compareAppVersions(a, b) {
  const pa = parseAppVersion(a);
  const pb = parseAppVersion(b);
  if (pa && pb) return cmpParsedVersions(pa, pb);
  if (pa) return 1;
  if (pb) return -1;
  const ta = String(a || "").trim();
  const tb = String(b || "").trim();
  if (ta === tb) return 0;
  return ta < tb ? -1 : 1;
}

// Mirrors `base_lang_code`: first chunk before `_`/`-`, kept only when it
// has at least two characters. Deliberately does not lowercase — the
// client doesn't either.
function baseLangCode(locale) {
  const trimmed = String(locale || "").trim();
  if (!trimmed) {
    return "";
  }
  const idx = trimmed.search(/[_-]/);
  const base = idx === -1 ? trimmed : trimmed.slice(0, idx);
  return base.length >= 2 ? base : "";
}

// Mirrors `pick_changelog`: base language, then en, then zh, then the
// first key in sort order (the client's BTreeMap iterates sorted), then
// the legacy string. A non-empty map never falls through to legacy.
function pickChangelog(legacy, changelogs, locale) {
  const map = changelogs && typeof changelogs === "object" ? changelogs : {};
  const keys = Object.keys(map);
  if (keys.length) {
    const base = baseLangCode(locale);
    if (base && typeof map[base] === "string") {
      return map[base];
    }
    for (const fallback of ["en", "zh"]) {
      if (typeof map[fallback] === "string") {
        return map[fallback];
      }
    }
    const first = keys.slice().sort()[0];
    if (typeof map[first] === "string") {
      return map[first];
    }
  }
  return String(legacy || "");
}

// Mirrors the client's join: a lone section stays plain so single-version
// notes keep their current look; two or more get bold version headings
// (the dialog renders Markdown).
function joinChangelogSections(sections) {
  if (sections.length <= 1) {
    return sections.length ? sections[0].text : "";
  }
  return sections
    .map((section) => `**v${section.version.replace(/^v+/, "")}**\n\n${section.text}`)
    .join("\n\n");
}

// Pre-join the stable row's notes: policy versions from the auto version
// downwards, newest first, capped like the client does. `resolveText`
// decides how one entry's text is read (per-locale, or the legacy string
// alone). Entries that resolve to empty text don't consume a cap slot.
function stableChangelogSections(covered, resolveText) {
  const sections = [];
  for (const item of covered) {
    const text = resolveText(item).trim();
    if (!text) {
      continue;
    }
    sections.push({ version: String(item.version || "").trim(), text });
    if (sections.length >= AGGREGATED_CHANGELOG_MAX_VERSIONS) {
      break;
    }
  }
  return sections;
}

// Build the auto channel's `changelog` / `changelogs` pair, aggregated
// across every policy version at or below the auto version.
function buildStableChangelogs(versions, autoEntry) {
  if (!autoEntry) {
    return { changelog: "", changelogs: {} };
  }
  const covered = versions
    .filter((item) => compareAppVersions(item.version, autoEntry.version) <= 0)
    .sort((a, b) => compareAppVersions(b.version, a.version));
  // A policy that doesn't list its own auto version (env fallback, or a
  // hand-edited one) must still lead with the auto entry's own notes.
  if (!covered.some((item) => item.version === autoEntry.version)) {
    covered.unshift(autoEntry);
  }
  const locales = new Set();
  for (const item of covered) {
    for (const code of Object.keys(item.changelogs || {})) {
      locales.add(code);
    }
  }
  const changelogs = {};
  for (const locale of locales) {
    const text = joinChangelogSections(
      stableChangelogSections(covered, (item) =>
        pickChangelog(item.changelog, item.changelogs, locale)
      )
    );
    if (text) {
      changelogs[locale] = text;
    }
  }
  return {
    // Legacy field: aggregated from the legacy strings alone. Reading it
    // through `pickChangelog` would let an i18n map win and change what a
    // pre-i18n consumer sees; clients with a non-empty map never read it.
    changelog: joinChangelogSections(
      stableChangelogSections(covered, (item) => String(item.changelog || ""))
    ),
    changelogs,
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
      const archiveUrl = String(rawValue.archive_url || "").trim();
      if (installerUrl || packageUrl || archiveUrl) {
        result[key] = {};
        if (installerUrl) {
          result[key].installer_url = installerUrl;
        }
        if (packageUrl) {
          result[key].package_url = packageUrl;
        }
        if (archiveUrl) {
          result[key].archive_url = archiveUrl;
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
      archive_filename: String(rawValue.archive_filename || "").trim(),
      archive_sha256: String(rawValue.archive_sha256 || "").trim().toLowerCase(),
      archive_size: coercePositiveInteger(rawValue.archive_size),
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

  const platformPackage = {
    kind: metadata.kind || defaultPackageKind(key),
    package_url: urls.package_url || "",
    package_sha256: metadata.package_sha256 || "",
    package_size: metadata.package_size || 0,
  };
  if (key === "linux") {
    platformPackage.archive_url = urls.archive_url || "";
    platformPackage.archive_sha256 = metadata.archive_sha256 || "";
    platformPackage.archive_size = metadata.archive_size || 0;
  }
  return platformPackage;
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
  if ("archive_url" in result) {
    result.archive_url = toAbsoluteManifestUrl(req, result.archive_url || "");
  }
  return result;
}

function hasArchiveMetadata(metadata) {
  return Boolean(
    metadata?.archive_filename || metadata?.archive_sha256 || coercePositiveInteger(metadata?.archive_size)
  );
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
      archive_url: getAppArchiveAssetName(platform) && hasArchiveMetadata(metadata)
        ? buildDownloadApiUrl(
            req,
            "app",
            entry.tag,
            metadata.archive_filename || getAppArchiveAssetName(platform)
          )
        : "",
    };
  }

  if (String(entry.app_source_mode || "").trim().toLowerCase() === "artifact") {
    // The publish pipeline (`_scripts/publish_pan123_release.py
    // build_global_artifact_app_urls`) writes nightly.link URLs into
    // `entry.app_urls[platform]` whose artifact names match the V4 short-name
    // upload steps in `.github/workflows/release.yml`. There is no longer a
    // path that derives a nightly URL from `entry.tag` alone — if `app_urls`
    // is empty for a given platform, the deliverable was not published for
    // that platform and we return empty URLs.
    const artifactUrls = entry.app_urls?.[platform] || {};
    return {
      installer_url: toAbsoluteManifestUrl(req, artifactUrls.installer_url || ""),
      package_url: toAbsoluteManifestUrl(req, artifactUrls.package_url || ""),
      archive_url: toAbsoluteManifestUrl(req, artifactUrls.archive_url || ""),
    };
  }

  // Global GitHub Release path: ignore `metadata.*_filename` because the
  // publish pipeline writes those as the CN short-name zip wrapper (used to
  // dodge 123 disk's auto-`.bak` rename on .exe/.apk uploads). GH Release
  // hosts the raw deliverables under their default names — always use those
  // here so global users land on the bare .exe / .apk / .dmg / .AppImage / .tar.gz.
  const archiveAssetName = hasArchiveMetadata(metadata) ? getAppArchiveAssetName(platform) : "";
  return {
    installer_url: getAppInstallerAssetName(platform)
      ? buildReleaseAssetUrl(source.base, entry.tag, getAppInstallerAssetName(platform))
      : "",
    package_url: buildReleaseAssetUrl(
      source.base,
      entry.tag,
      getAppPackageAssetName(platform)
    ),
    archive_url: archiveAssetName
      ? buildReleaseAssetUrl(source.base, entry.tag, archiveAssetName)
      : "",
  };
}

function defaultPackageKind(platform) {
  switch (normalizePlatform(platform)) {
    case "windows":
      return "web_installer_zip";
    case "linux":
      return "appimage";
    case "android":
      return "apk";
    case "macos":
    default:
      return "dmg";
  }
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

function getLensDataReleaseBase() {
  return stripTrailingSlash(
    process.env.NIYIEN_LENS_DATA_RELEASE_BASE || DEFAULT_LENS_DATA_RELEASE_BASE
  );
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

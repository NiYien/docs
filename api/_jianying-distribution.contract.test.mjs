// SPDX-License-Identifier: GPL-3.0-or-later
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

function readApiFile(name) {
  return readFileSync(join(here, name), "utf8");
}

async function importControlPlane() {
  const source = readApiFile("_control-plane.js").replace(
    'import { getGeo } from "./_geo";',
    `async function getGeo(req, options = {}) {
      return {
        country: String(req?.query?.country || options.fallbackCountry || "US").toUpperCase(),
        source: "test",
        city: "Test City",
      };
    }`
  );
  return import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
}

async function importDownloadHandler() {
  const source = readApiFile("download.js")
    .replace(
      'import { Pan123NotFoundError, resolvePan123ReleaseDownloadUrl } from "./_pan123";',
      `class Pan123NotFoundError extends Error {}
    async function resolvePan123ReleaseDownloadUrl(tag, relativePath) {
      return "https://files.example.test/" + tag + "/" + relativePath;
    }`
    )
    // data: modules cannot resolve relative specifiers, so rewrite the
    // _direct-link import to an absolute file:// URL. This deliberately loads
    // the REAL module: the test then covers the actual direct-link
    // construction, including the built-in default base/prefix.
    .replace(
      'from "./_direct-link";',
      `from ${JSON.stringify(pathToFileURL(join(here, "_direct-link.js")).href)};`
    );
  return import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
}

async function withEnv(values, fn) {
  const original = { ...process.env };
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("NIYIEN_") || key.startsWith("PAN123_")) {
      delete process.env[key];
    }
  }
  Object.assign(process.env, values);
  try {
    return await fn();
  } finally {
    process.env = original;
  }
}

function makeReq(query = {}, headers = {}) {
  return {
    query,
    headers: {
      host: "preview.niyien.test",
      "x-forwarded-proto": "https",
      ...headers,
    },
  };
}

function makePolicy(versionEntry) {
  return JSON.stringify({
    auto_version: versionEntry.version,
    versions: [versionEntry],
  });
}

async function testJianyingPackagesFollowExactAppVersionAndPluginSource() {
  const controlPlane = await importControlPlane();
  const filename = "GyroflowNiyien-Jianying-windows.zip";
  const info = { sha256: "a".repeat(64), size: 12345 };
  const policy = {
    auto_version: "1.6.3-ni.200",
    versions: [
      { version: "1.6.3-ni.200", tag: "run-200", plugin_tag: "plugin-new",
        plugins_source_mode: "artifact", plugins_source_ref: "actions-run-200",
        global_plugins_base: "https://nightly.link/NiYien/gyroflow-plugins/actions/runs/200/",
        plugins_packages: { [filename]: info } },
      { version: "1.6.3-ni.199", tag: "run-199", plugin_tag: "plugin-old",
        plugins_source_mode: "release", plugins_source_ref: "v2.1.2" },
    ],
  };
  for (const country of ["CN", "US"]) {
    await withEnv({ NIYIEN_RELEASE_POLICY_JSON: JSON.stringify(policy) }, async () => {
      const current = await controlPlane.buildManifestPayload(makeReq({ platform: "windows", country, app_version: "1.6.3-ni.200" }));
      assert.deepEqual(current.plugins_packages, { [filename]: info });
      assert.equal(current.plugins_source_ref, "actions-run-200");
      assert.match(current.plugins_base, country === "CN" ? /content\/plugin-new\/$/ : /actions\/runs\/200\/$/);
      const old = await controlPlane.buildManifestPayload(makeReq({ platform: "windows", country, app_version: "1.6.3-ni.199" }));
      assert.deepEqual(old.plugins_packages, {});
      assert.equal(old.plugins_source_ref, "v2.1.2");
      const unknown = await controlPlane.buildManifestPayload(makeReq({ platform: "windows", country, app_version: "unknown" }));
      assert.deepEqual(unknown.plugins_packages, { [filename]: info });
    });
  }
  policy.hidden_plugins = [{ kind: "artifact", run_id: 200 }];
  await withEnv({ NIYIEN_RELEASE_POLICY_JSON: JSON.stringify(policy),
    NIYIEN_PLUGIN_RELEASE_TAG: "plugin-new",
    NIYIEN_PLUGINS_PACKAGES_JSON: JSON.stringify({ plugin_tag: "plugin-new", packages: { [filename]: info } }) }, async () => {
    const hidden = await controlPlane.buildManifestPayload(makeReq({ app_version: "1.6.3-ni.200" }));
    assert.deepEqual(hidden.plugins_packages, {});
  });
  policy.auto_version = "1.6.3-ni.199";
  await withEnv({ NIYIEN_RELEASE_POLICY_JSON: JSON.stringify(policy) }, async () => {
    const fallback = await controlPlane.buildManifestPayload(makeReq({ app_version: "1.6.3-ni.200" }));
    assert.equal(fallback.plugins_source_ref, "v2.1.2");
    assert.deepEqual(fallback.plugins_packages, {});
  });
}

async function testJianyingPackageMetadataRejectsMalformedEntries() {
  const controlPlane = await importControlPlane();
  const filename = "GyroflowNiyien-Jianying-windows.zip";
  for (const invalid of [{ sha256: "bad", size: 1 }, { sha256: "b".repeat(64), size: -1 }, { sha256: "b".repeat(64), size: "12" }]) {
    await withEnv({ NIYIEN_RELEASE_POLICY_JSON: makePolicy({ version: "1.0.0", tag: "v1.0.0", plugins_packages: { [filename]: invalid } }) }, async () => {
      const payload = await controlPlane.buildManifestPayload(makeReq());
      assert.deepEqual(payload.plugins_packages, {});
    });
  }
}

await testJianyingPackagesFollowExactAppVersionAndPluginSource();
await testJianyingPackageMetadataRejectsMalformedEntries();
async function testJianyingDownloadUsesExistingContentRoute() {
  const handler = (await importDownloadHandler()).default;
  const filename = "GyroflowNiyien-Jianying-windows.zip";
  for (const mode of ["direct", "api"]) {
    const response = {
      headers: new Map(),
      setHeader(key, value) { this.headers.set(key.toLowerCase(), value); },
      end() {},
    };
    await withEnv({ PAN123_DOWNLOAD_MODE: mode }, () => handler({ method: "GET", query: {
      scope: "content", tag: "plugin-123456abcdef", path: filename,
    } }, response));
    assert.equal(response.statusCode, 302);
    const location = response.headers.get("location");
    assert.equal(new URL(location).pathname.endsWith("/plugin-123456abcdef/" + filename), true);
    assert.equal(new URL(location).hostname, mode === "api" ? "files.example.test" : "1846941948.cdn.123clouddisk.com");
  }
}

await testJianyingDownloadUsesExistingContentRoute();
async function testEnvOnlyPackagesRequireTheExactBundleAndSource() {
  const controlPlane = await importControlPlane();
  const filename = "GyroflowNiyien-Jianying-windows.zip";
  const packages = { [filename]: { sha256: "c".repeat(64), size: 1234 } };
  for (const mode of ["release", "artifact"]) {
    const ref = mode === "artifact" ? "actions-run-123" : "v2.2.0";
    const base = mode === "artifact" ? "https://nightly.link/NiYien/gyroflow-plugins/actions/runs/123/"
      : "https://github.com/NiYien/gyroflow-plugins/releases/download/v2.2.0/";
    const record = { plugin_tag: "plugin-env", plugin_source_ref: ref, global_plugins_base: base, packages };
    const envs = { NIYIEN_APP_VERSION: "1.0.0", NIYIEN_PLUGIN_RELEASE_TAG: "plugin-env",
      NIYIEN_PLUGINS_SOURCE_MODE: mode, NIYIEN_PLUGINS_TAG: "v2.2.0", NIYIEN_PLUGINS_RUN_ID: "123",
      NIYIEN_GLOBAL_PLUGINS_BASE: "https://github.com/NiYien/gyroflow-plugins/releases/latest/download/",
      NIYIEN_PLUGINS_PACKAGES_JSON: JSON.stringify(record) };
    for (const country of ["CN", "US"]) {
      await withEnv(envs, async () => {
        const payload = await controlPlane.buildManifestPayload(makeReq({ country }));
        assert.deepEqual(payload.plugins_packages, packages);
        assert.equal(payload.plugins_source_ref, ref);
        assert.equal(payload.plugins_base, country === "CN" ? "https://preview.niyien.test/api/download/content/plugin-env/" : base);
      });
      for (const mismatch of [{ ...record, plugin_tag: "plugin-other" }, { ...record, plugin_source_ref: "v-old" }]) {
        await withEnv({ ...envs, NIYIEN_PLUGINS_PACKAGES_JSON: JSON.stringify(mismatch) }, async () => {
          const payload = await controlPlane.buildManifestPayload(makeReq({ country }));
          assert.deepEqual(payload.plugins_packages, {});
        });
      }
      await withEnv({ ...envs, NIYIEN_RELEASE_POLICY_JSON: makePolicy({ version: "1.0.0", tag: "v1.0.0",
        plugin_tag: "plugin-old", plugins_source_mode: "release", plugins_source_ref: "v2.1.2" }) }, async () => {
        const payload = await controlPlane.buildManifestPayload(makeReq({ country, app_version: "1.0.0" }));
        assert.deepEqual(payload.plugins_packages, {});
        assert.equal(payload.plugins_source_ref, "v2.1.2");
      });
    }
    const hidden = mode === "artifact" ? { kind: "artifact", run_id: 123 } : { kind: "release", ref: "plugin-env" };
    await withEnv({ ...envs, NIYIEN_RELEASE_POLICY_JSON: JSON.stringify({ auto_version: "1.0.0",
      versions: [{ version: "1.0.0", tag: "v1.0.0" }], hidden_plugins: [hidden] }) }, async () => {
      const payload = await controlPlane.buildManifestPayload(makeReq());
      assert.deepEqual(payload.plugins_packages, {});
    });
  }
}

async function testDisabledPluginsNeverExposePackagesOrDownloadBase() {
  const controlPlane = await importControlPlane();
  const packages = { "GyroflowNiyien-Jianying-windows.zip": { sha256: "d".repeat(64), size: 789 } };
  for (const country of ["CN", "US"]) {
    await withEnv({ NIYIEN_PLUGINS_DISABLED: "1", NIYIEN_RELEASE_POLICY_JSON: makePolicy({
      version: "1.0.0", tag: "v1.0.0", plugin_tag: "plugin-disabled", plugins_source_ref: "v2.2.0", plugins_packages: packages,
    }) }, async () => {
      const payload = await controlPlane.buildManifestPayload(makeReq({ country }));
      assert.deepEqual(payload.plugins_packages, {});
      assert.equal(payload.plugins_base, "");
    });
  }
}

await testEnvOnlyPackagesRequireTheExactBundleAndSource();
await testDisabledPluginsNeverExposePackagesOrDownloadBase();
console.log("Jianying distribution contract checks passed");

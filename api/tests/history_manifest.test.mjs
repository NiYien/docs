import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = (await readFile(new URL('../_control-plane.js', import.meta.url), 'utf8'))
  .replace('import { getGeo } from "./_geo";',
    'const getGeo = async (req) => ({ country: req.query.country || "US" });');
const { buildManifestPayload } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

function entry(version, platform = 'windows', extra = {}) {
  return {
    version, tag: `v${version}`, channels: ['manual'],
    packages: { [platform]: { package_sha256: 'a'.repeat(64) } },
    ...extra
  };
}

function request(platform = 'windows', country = 'US', includeHistory = true) {
  return { query: { platform, country, ...(includeHistory ? { include_history: '1' } : {}) },
    headers: { host: 'updates.example.test', 'x-forwarded-proto': 'https' } };
}

async function withPolicy(policy, callback) {
  const previous = { ...process.env };
  for (const key of Object.keys(process.env)) {
    if (key.startsWith('NIYIEN_')) delete process.env[key];
  }
  process.env.NIYIEN_RELEASE_POLICY_JSON = JSON.stringify(policy);
  try { await callback(); } finally { process.env = previous; }
}

test('history is opt-in and includes recent test builds newer than the main download', async () => {
  await withPolicy({ auto_version: '1.6.3-ni.144', versions: [
    entry('1.6.3-ni.99'), entry('1.6.3-ni.144'), entry('1.6.3-ni.145'),
    entry('1.6.3-ni.152'), entry('1.6.3-ni.145'), entry('1.6.3-ni.146', 'macos')
  ] }, async () => {
    const normal = await buildManifestPayload(request('windows', 'US', false));
    assert.equal('history_versions' in normal.app, false);
    const payload = await buildManifestPayload(request());
    assert.equal(payload.app.version, normal.app.version);
    assert.deepEqual(payload.app.manual_versions, normal.app.manual_versions);
    assert.deepEqual(payload.app.history_versions.map((row) => row.version), ['1.6.3-ni.152', '1.6.3-ni.145', '1.6.3-ni.99']);
    assert.equal(payload.app.history_versions[0].url,
      'https://github.com/NiYien/gyroflow/releases/download/v1.6.3-ni.152/gyroflow-niyien-windows64-setup.exe');
  });
});

test('history preserves CN proxy filenames for every downloadable platform', async () => {
  for (const [platform, filename] of [
    ['windows', 'old-setup.zip'], ['macos', 'old.dmg'], ['linux', 'old.AppImage'], ['android', 'old-apk.zip']
  ]) {
    const old = entry('1.0.0', platform);
    const current = entry('2.0.0', platform);
    Object.assign(old.packages[platform], { version: '1.0.0', version_code: 1,
      ...(platform === 'windows' ? { installer_filename: filename } : { package_filename: filename }) });
    Object.assign(current.packages[platform], { version: '2.0.0', version_code: 2 });
    await withPolicy({ auto_version: current.version, versions: [current, old] }, async () => {
      const row = (await buildManifestPayload(request(platform, 'CN'))).app.history_versions[0];
      assert.equal(row.url, `https://updates.example.test/api/download/app/v1.0.0/${filename}`);
      assert.equal(Object.keys(row.packages)[0], platform);
    });
  }
});

test('artifact history uses recorded URLs and skips unavailable global platforms', async () => {
  await withPolicy({ auto_version: '3.0.0', versions: [entry('3.0.0'),
    entry('2.0.0', 'windows', { app_source_mode: 'artifact', app_urls: {
      windows: { package_url: 'https://nightly.link/old/windows.zip' }
    } }),
    entry('1.0.0', 'windows', { app_source_mode: 'artifact' })
  ] }, async () => {
    const rows = (await buildManifestPayload(request())).app.history_versions;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].url, 'https://nightly.link/old/windows.zip');
  });
});

test('Android history follows independent package versions and skips missing version metadata', async () => {
  const android = (version, versionCode) => ({ android: { version, version_code: versionCode, package_sha256: 'a'.repeat(64) } });
  await withPolicy({ auto_version: '9.0.0', versions: [
    entry('9.0.0', 'android', { packages: android('1.0.2', 102) }),
    entry('8.0.0', 'android', { packages: android('1.0.1', 101) }),
    entry('7.0.0', 'android', { packages: android('1.0.1', 101) }),
    entry('6.0.0', 'android', { packages: android('1.0.2', 103) }),
    entry('5.0.0', 'android')
  ] }, async () => {
    const rows = (await buildManifestPayload(request('android'))).app.history_versions;
    assert.deepEqual(rows.map((row) => row.version), ['1.0.2-ni.103', '1.0.1-ni.101']);
    assert.equal(rows[0].packages.android.version, '1.0.2');
  });
});

test('history keeps only the ten newest downloadable versions', async () => {
  await withPolicy({ auto_version: '1.6.3-ni.144', versions: [
    entry('1.6.3-ni.144'),
    ...Array.from({ length: 15 }, (_, i) => entry(`1.6.3-ni.${130 + i}`)),
    entry('1.6.3-ni.152', 'macos')
  ] }, async () => {
    const rows = (await buildManifestPayload(request())).app.history_versions;
    assert.equal(rows.length, 10);
    assert.equal(rows[0].version, '1.6.3-ni.143');
    assert.equal(rows.at(-1).version, '1.6.3-ni.134');
  });
});

test('empty policy exposes no historical downloads', async () => {
  await withPolicy({ auto_version: '', versions: [] }, async () => {
    const payload = await buildManifestPayload(request());
    assert.deepEqual(payload.app.history_versions, []);
    assert.equal(payload.app.version, '');
  });
});

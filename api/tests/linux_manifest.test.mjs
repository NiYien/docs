import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';


const controlPlanePath = new URL('../_control-plane.js', import.meta.url);
const controlPlaneSource = (await readFile(controlPlanePath, 'utf8'))
  .replace(
    'import { getGeo } from "./_geo";',
    'const getGeo = async (req) => ({ country: req?.query?.country || "US", city: "Unknown", source: "test" });',
  )
  .replace(/^export /gm, '');
const moduleUrl = `data:text/javascript;base64,${Buffer.from(
  `${controlPlaneSource}\nexport { buildManifestPayload };`,
).toString('base64')}`;
const { buildManifestPayload } = await import(moduleUrl);


function request(country = 'US') {
  return {
    query: { country, platform: 'linux' },
    headers: { host: 'updates.example.test', 'x-forwarded-proto': 'https' },
    socket: {},
  };
}


function policy(overrides = {}) {
  return {
    auto_version: '9.9.9',
    versions: [{
      version: '9.9.9',
      tag: 'v9.9.9',
      channels: ['auto', 'manual'],
      packages: {
        linux: {
          kind: 'appimage',
          package_filename: 'gyroflow-niyien-linux64.AppImage',
          package_sha256: 'a'.repeat(64),
          package_size: 100,
          archive_filename: 'gyroflow-niyien-linux64.tar.gz',
          archive_sha256: 'b'.repeat(64),
          archive_size: 200,
        },
      },
      ...overrides,
    }],
  };
}


async function withPolicy(value, callback) {
  const previous = process.env.NIYIEN_RELEASE_POLICY_JSON;
  process.env.NIYIEN_RELEASE_POLICY_JSON = JSON.stringify(value);
  try {
    return await callback();
  } finally {
    if (previous === undefined) delete process.env.NIYIEN_RELEASE_POLICY_JSON;
    else process.env.NIYIEN_RELEASE_POLICY_JSON = previous;
  }
}


test('production release manifest exposes Linux AppImage and tar', async () => {
  await withPolicy(policy(), async () => {
    const manifest = await buildManifestPayload(request('US'));
    const linux = manifest.app.packages.linux;

    assert.equal(linux.kind, 'appimage');
    assert.ok(linux.package_url.endsWith('/v9.9.9/gyroflow-niyien-linux64.AppImage'));
    assert.ok(linux.archive_url.endsWith('/v9.9.9/gyroflow-niyien-linux64.tar.gz'));
    assert.equal(linux.archive_sha256, 'b'.repeat(64));
    assert.equal(linux.archive_size, 200);
    assert.equal(manifest.app.url, linux.package_url);
  });
});


test('production artifact manifest preserves independent absolute Linux URLs', async () => {
  await withPolicy(policy({
    tag: 'run-42',
    app_source_mode: 'artifact',
    app_urls: {
      linux: {
        package_url: '/api/download/app/run-42/gyroflow-niyien-linux64.AppImage',
        archive_url: '/api/download/app/run-42/gyroflow-niyien-linux64.tar.gz',
      },
    },
  }), async () => {
    const linux = (await buildManifestPayload(request('US'))).app.packages.linux;
    assert.equal(
      linux.package_url,
      'https://updates.example.test/api/download/app/run-42/gyroflow-niyien-linux64.AppImage',
    );
    assert.equal(
      linux.archive_url,
      'https://updates.example.test/api/download/app/run-42/gyroflow-niyien-linux64.tar.gz',
    );
  });
});


test('production CN manifest resolves both Linux download routes', async () => {
  await withPolicy(policy(), async () => {
    const linux = (await buildManifestPayload(request('CN'))).app.packages.linux;
    assert.ok(linux.package_url.endsWith('/api/download/app/v9.9.9/gyroflow-niyien-linux64.AppImage'));
    assert.ok(linux.archive_url.endsWith('/api/download/app/v9.9.9/gyroflow-niyien-linux64.tar.gz'));
  });
});


test('production legacy Linux policy defaults to AppImage without archive', async () => {
  const legacy = policy();
  delete legacy.versions[0].packages.linux.kind;
  delete legacy.versions[0].packages.linux.archive_filename;
  delete legacy.versions[0].packages.linux.archive_sha256;
  delete legacy.versions[0].packages.linux.archive_size;

  await withPolicy(legacy, async () => {
    const manifest = await buildManifestPayload(request('US'));
    const linux = manifest.app.packages.linux;
    assert.equal(linux.kind, 'appimage');
    assert.equal(linux.archive_url || '', '');
    assert.equal(manifest.app.url, linux.package_url);
  });
});

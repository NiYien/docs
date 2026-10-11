const assert = require('node:assert/strict');
const { generateKeyPairSync, verify } = require('node:crypto');
const test = require('node:test');

const api = import('../lib/testflight.js');
const NOW = Date.parse('2026-10-11T04:00:00Z');
const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
const pem = privateKey.export({ type: 'pkcs8', format: 'pem' });
const env = {
  APPSTORE_ISSUER_ID: 'test-issuer',
  APPSTORE_API_KEY_ID: 'TESTKEY001',
  APPSTORE_API_PRIVATE_KEY: pem,
};

function fixture(id, version, buildNumber, overrides = {}) {
  return {
    data: [{
      type: 'builds', id,
      attributes: {
        version: buildNumber, processingState: 'VALID', expired: false,
        expirationDate: new Date(NOW + 86400000).toISOString(),
        uploadedDate: new Date(NOW - 60000).toISOString(),
        ...overrides.build,
      },
      relationships: {
        preReleaseVersion: { data: { type: 'preReleaseVersions', id: `version-${id}` } },
        buildBetaDetail: { data: { type: 'buildBetaDetails', id: `detail-${id}` } },
      },
    }],
    included: [
      { type: 'preReleaseVersions', id: `version-${id}`, attributes: { version, platform: 'IOS', ...overrides.release } },
      { type: 'buildBetaDetails', id: `detail-${id}`, attributes: { externalBuildState: 'IN_BETA_TESTING', ...overrides.detail } },
    ],
  };
}

function combine(...collections) {
  return { data: collections.flatMap(c => c.data), included: collections.flatMap(c => c.included) };
}

async function group(attributes = {}) {
  const { TESTFLIGHT_URL } = await api;
  return { data: [{ type: 'betaGroups', id: 'public-group', attributes: {
    publicLink: TESTFLIGHT_URL, publicLinkEnabled: true, isInternalGroup: false, ...attributes,
  } }] };
}

function response(body, status = 200) {
  return { ok: status === 200, status, json: async () => body };
}

function fakeResponse() {
  return {
    headers: {}, code: 0, body: null,
    setHeader(name, value) { this.headers[name] = value; },
    status(code) { this.code = code; return this; },
    json(body) { this.body = body; return this; },
  };
}

test('Apple authentication uses a valid ES256 token and accepts existing private key formats', async () => {
  const { createAppleToken } = await api;
  for (const privateValue of [pem, pem.replace(/\n/g, '\\n'), Buffer.from(pem).toString('base64')]) {
    const token = createAppleToken({ ...env, APPSTORE_API_PRIVATE_KEY: privateValue }, NOW);
    const [header, payload, signature] = token.split('.');
    assert.deepEqual(JSON.parse(Buffer.from(header, 'base64url')), { alg: 'ES256', kid: 'TESTKEY001', typ: 'JWT' });
    const claims = JSON.parse(Buffer.from(payload, 'base64url'));
    assert.equal(claims.iss, env.APPSTORE_ISSUER_ID);
    assert.equal(claims.aud, 'appstoreconnect-v1');
    assert.equal(claims.exp - claims.iat, 300);
    assert.deepEqual(claims.scope, ['GET /v1/betaGroups', 'GET /v1/builds']);
    assert.equal(Buffer.from(signature, 'base64url').length, 64);
    assert.equal(verify('sha256', Buffer.from(`${header}.${payload}`), { key: publicKey, dsaEncoding: 'ieee-p1363' }, Buffer.from(signature, 'base64url')), true);
  }
  const legacyToken = createAppleToken({ MACOS_ITCONNECT_ISSUER: env.APPSTORE_ISSUER_ID, MACOS_ITCONNECT_KEY_ID: env.APPSTORE_API_KEY_ID, MACOS_ITCONNECT_KEY: pem }, NOW);
  assert.equal(legacyToken.split('.').length, 3);
});

test('TestFlight selection uses the release version and compares version and build numbers numerically', async () => {
  const { selectLatestBuild } = await api;
  const chosen = selectLatestBuild(combine(
    fixture('older', '1.0.9', '999', { build: { uploadedDate: new Date(NOW).toISOString() } }),
    fixture('same-release', '1.0.10', '5.2'),
    fixture('latest', '1.0.10', '5.10'),
  ), NOW);
  assert.equal(chosen.version, '1.0.10');
  assert.equal(chosen.buildNumber, '5.10');
});

test('Only processed, active external iOS builds that have not expired are displayed', async () => {
  const { selectLatestBuild } = await api;
  const excluded = [
    { build: { expired: true } },
    { build: { expirationDate: new Date(NOW).toISOString() } },
    { build: { expirationDate: 'invalid' } },
    { build: { processingState: 'PROCESSING' } },
    { detail: { externalBuildState: 'IN_BETA_REVIEW' } },
    { detail: { externalBuildState: 'BETA_APPROVED' } },
    { detail: { externalBuildState: 'READY_FOR_BETA_TESTING' } },
    { release: { platform: 'MAC_OS' } },
  ].map((overrides, index) => fixture(`excluded-${index}`, '9.0.0', '99', overrides));
  const chosen = selectLatestBuild(combine(fixture('available', '1.0.1', '7'), ...excluded), NOW);
  assert.equal(chosen.version, '1.0.1');
  assert.equal(selectLatestBuild({ ...fixture('missing', '2.0.0', '8'), included: [] }, NOW), null);
});

test('The public invitation group is resolved and all build pages are read with supported Apple filters', async () => {
  const { createTestFlightLookup, TESTFLIGHT_URL } = await api;
  const requests = [];
  const lookup = createTestFlightLookup({ env, now: () => NOW, fetchImpl: async (url, options) => {
    const parsed = new URL(url);
    requests.push(parsed);
    assert.match(options.headers.Authorization, /^Bearer /);
    assert.equal(options.redirect, 'error');
    if (parsed.pathname === '/v1/betaGroups') {
      assert.equal(parsed.searchParams.get('filter[publicLink]'), TESTFLIGHT_URL);
      return response(await group());
    }
    assert.equal(parsed.pathname, '/v1/builds');
    if (parsed.searchParams.has('cursor')) return response(fixture('page-two', '1.0.3', '10'));
    assert.equal(parsed.searchParams.get('filter[betaGroups]'), 'public-group');
    assert.equal(parsed.searchParams.get('filter[preReleaseVersion.platform]'), 'IOS');
    assert.equal(parsed.searchParams.get('include'), 'preReleaseVersion,buildBetaDetail');
    return response({ ...fixture('page-one', '1.0.1', '9'), links: { next: `${url}&cursor=next-page` } });
  } });
  const result = await lookup();
  assert.equal(result.body.version, '1.0.3');
  assert.equal(result.body.build_number, '10');
  assert.equal(result.cacheSeconds, 300);
  assert.equal(requests.length, 3);
});

test('Disabled, internal, or different invitation groups do not advertise a version', async () => {
  const { createTestFlightLookup } = await api;
  for (const attributes of [{ publicLinkEnabled: false }, { isInternalGroup: true }, { publicLink: 'https://testflight.apple.com/join/other' }]) {
    let requests = 0;
    const lookup = createTestFlightLookup({ env, now: () => NOW, fetchImpl: async () => { requests += 1; return response(await group(attributes)); } });
    assert.equal((await lookup()).body.version, '');
    assert.equal(requests, 1);
  }
});

test('Concurrent requests share a lookup and both server and CDN cache expiry stay within five minutes', async () => {
  const { createTestFlightLookup } = await api;
  let time = NOW;
  let requests = 0;
  let publishedVersion = '1.0.1';
  const lookup = createTestFlightLookup({ env, now: () => time, fetchImpl: async url => {
    requests += 1;
    await new Promise(resolve => setImmediate(resolve));
    return response(new URL(url).pathname === '/v1/betaGroups' ? await group() : fixture('current', publishedVersion, '9'));
  } });
  await Promise.all([lookup(), lookup()]);
  assert.equal(requests, 2);
  time += 10000;
  assert.equal((await lookup()).cacheSeconds, 290);
  assert.equal(requests, 2);
  time += 291000;
  publishedVersion = '1.0.2';
  assert.equal((await lookup()).body.version, '1.0.2');
  assert.equal(requests, 4);
});

test('Cached versions expire before the build expires', async () => {
  const { createTestFlightLookup } = await api;
  let time = NOW;
  const lookup = createTestFlightLookup({ env, now: () => time, fetchImpl: async url => response(
    new URL(url).pathname === '/v1/betaGroups' ? await group() : fixture('expiring', '1.0.1', '9', { build: { expirationDate: new Date(NOW + 30000).toISOString() } })
  ) });
  assert.equal((await lookup()).cacheSeconds, 30);
  time += 31000;
  assert.equal((await lookup()).body.status, 'unavailable');
});

test('Pagination never sends authentication to an unexpected host', async () => {
  const { createTestFlightLookup } = await api;
  let requests = 0;
  const lookup = createTestFlightLookup({ env, now: () => NOW, fetchImpl: async () => {
    requests += 1;
    return response({ ...(await group()), links: { next: 'https://unexpected.example.test/v1/builds' } });
  } });
  await assert.rejects(lookup(), { code: 'invalid_pagination_url' });
  assert.equal(requests, 1);
});

test('Authentication and upstream errors return an uncached unavailable response without private data', async () => {
  const { createTestFlightLookup, createTestFlightHandler } = await api;
  for (const configuration of [
    { env: {} },
    { env: { ...env, APPSTORE_API_PRIVATE_KEY: 'PRIVATE DATA MUST NOT BE RETURNED' } },
    { env, fetchImpl: async () => response({ secret: 'PRIVATE DATA MUST NOT BE RETURNED' }, 401) },
    { env, fetchImpl: async () => { throw new Error('PRIVATE DATA MUST NOT BE RETURNED'); } },
  ]) {
    const logs = [];
    const handler = createTestFlightHandler(createTestFlightLookup({ now: () => NOW, ...configuration }), (...args) => logs.push(args));
    const result = fakeResponse();
    await handler({ method: 'GET' }, result);
    assert.equal(result.code, 503);
    assert.equal(result.headers['Cache-Control'], 'no-store');
    assert.equal(result.headers['Vercel-CDN-Cache-Control'], 'no-store');
    assert.equal(result.headers['CDN-Cache-Control'], 'no-store');
    assert.equal(result.body.version, '');
    assert.doesNotMatch(JSON.stringify({ body: result.body, logs }), /PRIVATE DATA|BEGIN PRIVATE KEY|test-issuer/);
  }
});

test('The HTTP endpoint exposes only public release metadata and supports GET only', async () => {
  const { createTestFlightLookup, createTestFlightHandler, TESTFLIGHT_URL } = await api;
  const handler = createTestFlightHandler(createTestFlightLookup({ env, now: () => NOW, fetchImpl: async url => response(new URL(url).pathname === '/v1/betaGroups' ? await group() : fixture('current', '1.0.2', '5.1')) }));
  const result = fakeResponse();
  await handler({ method: 'GET' }, result);
  assert.equal(result.code, 200);
  assert.equal(result.headers['Vercel-CDN-Cache-Control'], 'public, max-age=300');
  assert.deepEqual(result.body, { status: 'available', version: '1.0.2', build_number: '5.1', url: TESTFLIGHT_URL, checked_at: new Date(NOW).toISOString() });

  const rejected = fakeResponse();
  await createTestFlightHandler(() => { throw new Error('Lookup must not run'); })({ method: 'POST' }, rejected);
  assert.equal(rejected.code, 405);
  assert.equal(rejected.headers.Allow, 'GET');
});

test('TestFlight shares the existing manifest function and preserves regional download cache behavior', async () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const vm = require('node:vm');
  const source = fs.readFileSync(path.join(__dirname, '../api/manifest.js'), 'utf8')
    .replace(/^import .*;\r?\n/gm, '')
    .replace('export default async function handler', 'async function handler');
  let manifestRequests = 0;
  let testflightRequests = 0;
  const handler = vm.runInNewContext(`${source}\nhandler;`, {
    buildManifestPayload: async () => { manifestRequests += 1; return { app: { version: '1.6.3-ni.140' } }; },
    testFlightHandler: async (req, res) => {
      testflightRequests += 1;
      res.setHeader('Cache-Control', 'public, max-age=0, must-revalidate');
      res.setHeader('Vercel-CDN-Cache-Control', 'public, max-age=300');
      return res.status(200).json({ version: '1.0.1' });
    },
  });
  const testflight = fakeResponse();
  await handler({ method: 'GET', query: { component: 'testflight' } }, testflight);
  assert.equal(testflight.body.version, '1.0.1');
  assert.equal(testflight.headers['Vercel-CDN-Cache-Control'], 'public, max-age=300');
  assert.equal(manifestRequests, 0);
  assert.equal(testflightRequests, 1);

  const manifest = fakeResponse();
  await handler({ method: 'GET', query: { platform: 'android' } }, manifest);
  assert.equal(manifest.body.app.version, '1.6.3-ni.140');
  assert.equal(manifest.headers['Cache-Control'], 'private, max-age=300');
  assert.equal(manifest.headers['Vercel-CDN-Cache-Control'], 'no-store');
  assert.equal(manifest.headers['CDN-Cache-Control'], 'no-store');
  assert.equal(manifestRequests, 1);
  assert.equal(testflightRequests, 1);
});

test('The public TestFlight URL is a rewrite and does not add a serverless entry point', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const config = JSON.parse(fs.readFileSync(path.join(__dirname, '../vercel.json'), 'utf8'));
  const route = config.rewrites.find(item => item.source === '/api/testflight');
  assert.equal(route.destination, '/api/manifest?component=testflight');
  assert.equal(config.functions['api/manifest.js'].maxDuration, 30);
  assert.equal(config.functions['api/testflight.js'], undefined);
  assert.equal(fs.existsSync(path.join(__dirname, '../api/testflight.js')), false);
  assert.equal(fs.existsSync(path.join(__dirname, '../lib/testflight.js')), true);
});

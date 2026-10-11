const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');


class FakeElement {
  constructor() {
    this.hidden = true;
    this.listeners = new Map();
    this.style = {};
    this.textContent = '';
  }

  addEventListener(type, listener) {
    this.listeners.set(type, listener);
  }

  focus() {}
}


function loadDownloadPage(language, androidPackage = { version: '1.0.2' }) {
  const html = fs.readFileSync(
    path.join(__dirname, '..', 'v2', language, 'download.html'),
    'utf8'
  );
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map((match) => match[1]);
  const elements = new Map(ids.map((id) => [id, new FakeElement()]));
  for (const match of html.matchAll(/<p[^>]+id="(ver[^"]+)">([\s\S]*?)<\/p>/g)) {
    elements.get(match[1]).textContent = match[2].replace(/<[^>]*>/g, '');
  }
  const requestedPlatforms = [];
  const inlineScripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)]
    .map((match) => match[1])
    .filter((source) => source.trim());

  assert.equal(inlineScripts.length, 1, `${language} page should have one inline script`);

  const document = {
    activeElement: null,
    body: { style: {} },
    addEventListener() {},
    getElementById(id) { return elements.get(id) || null; }
  };
  const window = { location: { href: '' } };
  const context = {
    alert() {},
    document,
    fetch(url) {
      const platform = new URL(url, 'https://example.test').searchParams.get('platform');
      requestedPlatforms.push(platform);
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          app: {
            version: '9.9.9',
            packages: {
              [platform]: { package_url: `https://downloads.example.test/${platform}`, ...(platform === 'android' ? androidPackage : {}) }
            }
          }
        })
      });
    },
    URL,
    window
  };

  vm.runInNewContext(inlineScripts[0], context);
  return { elements, html, requestedPlatforms, window };
}


async function flushPromises() {
  await new Promise((resolve) => setImmediate(resolve));
}


for (const language of ['zh', 'en']) {
  test(`${language} download page renders and wires every manifest platform`, async () => {
    const page = loadDownloadPage(language);
    const renderedPlatforms = [...page.html.matchAll(/\bdata-platform="([^"]+)"/g)]
      .map((match) => match[1]);

    assert.deepEqual(
      renderedPlatforms,
      ['windows', 'macos', 'linux', 'android'],
      'platform cards should match the supported manifest platforms'
    );

    await flushPromises();
    assert.deepEqual(
      page.requestedPlatforms,
      ['windows', 'macos', 'linux', 'android'],
      'version loading should request every manifest platform'
    );

    const linuxButton = page.elements.get('btnLinux');
    assert.ok(linuxButton, 'Linux download button should be rendered');
    assert.ok(linuxButton.listeners.has('click'), 'Linux download button should be wired');

    linuxButton.listeners.get('click')({ preventDefault() {} });
    await flushPromises();
    assert.equal(
      page.elements.get('downloadModal').hidden,
      false,
      'Linux download should open the confirmation modal'
    );
  });
}

for (const language of ['zh', 'en']) {
  test(`${language} mobile versions stay independent of the desktop manifest`, async () => {
    const page = loadDownloadPage(language);
    await flushPromises();

    assert.match(page.html, /id="verIos">(?:版本|Version) 1\.0\.0<br>TestFlight/);
    assert.match(page.elements.get('verIos').textContent, /1\.0\.0/);
    assert.equal(page.elements.get('verAndroid').textContent, language === 'zh' ? '版本 1.0.2' : 'Version 1.0.2');
    assert.match(page.elements.get('verWindows').textContent, /9\.9\.9/);
    assert.match(page.html, /href="https:\/\/testflight\.apple\.com\/join\/MGh9AhgQ" id="btnIos"/);
    assert.equal(page.elements.get('btnIos').listeners.has('click'), false);
    assert.equal(page.requestedPlatforms.includes('ios'), false);

    page.elements.get('btnAndroid').listeners.get('click')({ preventDefault() {} });
    await flushPromises();
    assert.equal(page.requestedPlatforms.at(-1), 'android');
    assert.equal(page.elements.get('downloadModal').hidden, false);
    page.elements.get('downloadConfirmBtn').listeners.get('click')();
    assert.equal(page.window.location.href, 'https://downloads.example.test/android');
    assert.equal(page.elements.get('verAndroid').textContent, language === 'zh' ? '版本 1.0.2' : 'Version 1.0.2');
  });
}

for (const language of ['zh', 'en']) {
  test(`${language} Android version follows published package metadata`, async () => {
    for (const version of ['1.0.1', '1.0.3']) {
      const page = loadDownloadPage(language, { version });
      await flushPromises();
      assert.equal(page.elements.get('verAndroid').textContent, `${language === 'zh' ? '版本' : 'Version'} ${version}`);
    }
  });

  test(`${language} Android version fallback identifies only the known legacy package`, async () => {
    const legacy = loadDownloadPage(language, { package_sha256: 'cce453c92e18ee4c4c2d932203b86b7eab1b794d812b94be4e8fef65d6a84f53' });
    await flushPromises();
    assert.equal(legacy.elements.get('verAndroid').textContent, language === 'zh' ? '版本 1.0.0' : 'Version 1.0.0');

    const unknown = loadDownloadPage(language, { package_sha256: 'unknown' });
    await flushPromises();
    assert.equal(unknown.elements.get('verAndroid').textContent, language === 'zh' ? '版本信息暂不可用' : 'Version unavailable');
  });
}

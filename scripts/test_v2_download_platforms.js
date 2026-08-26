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


function loadDownloadPage(language) {
  const html = fs.readFileSync(
    path.join(__dirname, '..', 'v2', language, 'download.html'),
    'utf8'
  );
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map((match) => match[1]);
  const elements = new Map(ids.map((id) => [id, new FakeElement()]));
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
              [platform]: { package_url: `https://downloads.example.test/${platform}` }
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
      'version loading should request every rendered platform'
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

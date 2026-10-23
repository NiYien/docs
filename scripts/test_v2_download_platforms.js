const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');


class FakeElement {
  constructor() {
    this.hidden = false;
    this.listeners = new Map();
    this.style = {};
    this.textContent = '';
    this.children = [];
  }

  addEventListener(type, listener) {
    this.listeners.set(type, listener);
  }

  focus() {}

  appendChild(child) { this.children.push(child); }
}


function loadDownloadPage(language, androidPackage = { version: '1.0.2' }, testflightVersion = '1.0.4', history = {}) {
  const html = fs.readFileSync(
    path.join(__dirname, '..', 'v2', language, 'download.html'),
    'utf8'
  );
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map((match) => match[1]);
  const elements = new Map(ids.map((id) => [id, new FakeElement()]));
  for (const match of html.matchAll(/<[^>]+\bid="([^"]+)"[^>]*>/g)) {
    elements.get(match[1]).hidden = /\bhidden\b/.test(match[0]);
  }
  let historyFailures = history.failures || 0;
  for (const match of html.matchAll(/<(?:p|span)[^>]+id="(ver[^"]+)">([\s\S]*?)<\/(?:p|span)>/g)) {
    elements.get(match[1]).textContent = match[2].replace(/<[^>]*>/g, '');
  }
  const requestedPlatforms = [];
  const requestedUrls = [];
  const inlineScripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)]
    .map((match) => match[1])
    .filter((source) => source.trim());

  assert.equal(inlineScripts.length, 1, `${language} page should have one inline script`);

  const document = {
    activeElement: null,
    body: { style: {} },
    addEventListener() {},
    createElement() { return new FakeElement(); },
    getElementById(id) { return elements.get(id) || null; }
  };
  const window = { location: { href: '' } };
  const context = {
    alert() {},
    document,
    fetch(url) {
      requestedUrls.push(url);
      if (url === '/api/testflight') {
        return testflightVersion === null
          ? Promise.reject(new Error('TestFlight API unavailable'))
          : Promise.resolve({ ok: true, json: async () => ({ version: testflightVersion }) });
      }
      const params = new URL(url, 'https://example.test').searchParams;
      const platform = params.get('platform');
      requestedPlatforms.push(platform);
      if (params.has('include_history') && historyFailures-- > 0) {
        return Promise.reject(new Error('History API unavailable'));
      }
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          app: {
            version: '9.9.9',
            ...(history.legacy
              ? { manual_versions: history.versions || [] }
              : { history_versions: history.versions || [] }),
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
  return { elements, html, requestedPlatforms, requestedUrls, window };
}


async function flushPromises() {
  await new Promise((resolve) => setImmediate(resolve));
}


for (const language of ['zh', 'en']) {
  test(`${language} recent test builds remain visible with the existing manifest API`, async () => {
    const page = loadDownloadPage(language, undefined, undefined, { legacy: true, versions: [
      { version: '1.6.3-ni.152', url: 'https://downloads.example.test/run-38869077226/setup.zip' },
      { version: '1.6.3-ni.145', url: 'https://downloads.example.test/run-38505112500/setup.zip' }
    ] });
    const details = page.elements.get('historyWindows');
    details.open = true;
    details.listeners.get('toggle')();
    await flushPromises();
    const links = page.elements.get('historyListWindows').children.map((item) => item.children[0]);
    assert.equal(links.length, 2);
    assert.match(links[0].textContent, /1\.6\.3-ni\.152/);
    assert.match(links[1].textContent, /1\.6\.3-ni\.145/);
    links[0].listeners.get('click')({ preventDefault() {} });
    page.elements.get('downloadConfirmBtn').listeners.get('click')();
    assert.equal(page.window.location.href, links[0].href);
  });

  test(`${language} history loads on demand and downloads the selected version`, async () => {
    const page = loadDownloadPage(language, undefined, undefined, { versions: [
      { version: '1.6.4-ni.9', url: 'https://downloads.example.test/old-9.exe' },
      { version: '1.6.4-ni.8', packages: { windows: { installer_url: 'https://downloads.example.test/old-8.exe' } } },
      { version: '1.6.4-ni.7', url: '' }
    ] });
    await flushPromises();
    assert.equal(page.requestedUrls.some((url) => url.includes('include_history')), false);
    const details = page.elements.get('historyWindows');
    details.open = true;
    details.listeners.get('toggle')();
    details.listeners.get('toggle')();
    await flushPromises();
    assert.equal(page.requestedUrls.filter((url) => url.includes('include_history')).length, 1);
    const list = page.elements.get('historyListWindows');
    assert.equal(list.hidden, false);
    assert.equal(page.elements.get('historyStatusWindows').hidden, true);
    assert.equal(list.children.length, 2);
    const link = list.children[1].children[0];
    assert.equal(link.href, 'https://downloads.example.test/old-8.exe');
    assert.match(link.textContent, /1\.6\.4-ni\.8/);
    link.listeners.get('click')({ preventDefault() {} });
    assert.equal(page.elements.get('downloadModal').hidden, false);
    page.elements.get('downloadConfirmBtn').listeners.get('click')();
    assert.equal(page.window.location.href, link.href);
    details.listeners.get('toggle')();
    await flushPromises();
    assert.equal(page.requestedUrls.filter((url) => url.includes('include_history')).length, 1);
  });

  test(`${language} history shows an empty state and retries failures independently`, async () => {
    const page = loadDownloadPage(language, undefined, undefined, { failures: 1 });
    const details = page.elements.get('historyLinux');
    details.open = true;
    details.listeners.get('toggle')();
    await flushPromises();
    assert.equal(page.elements.get('historyStatusLinux').textContent, language === 'zh'
      ? '历史版本暂不可用，请收起后重试。'
      : 'Previous versions unavailable. Close and reopen to retry.');
    assert.match(page.elements.get('verWindows').textContent, /9\.9\.9/);
    details.open = false;
    details.listeners.get('toggle')();
    details.open = true;
    details.listeners.get('toggle')();
    await flushPromises();
    assert.equal(page.elements.get('historyListLinux').hidden, true);
    assert.equal(page.elements.get('historyStatusLinux').hidden, false);
    assert.equal(page.elements.get('historyStatusLinux').textContent, language === 'zh'
      ? '暂无可下载的历史版本' : 'No previous versions available');
    assert.equal(page.requestedUrls.filter((url) => url.includes('include_history')).length, 2);
  });

  test(`${language} Android history uses its package version`, async () => {
    const page = loadDownloadPage(language, undefined, undefined, { versions: [{
      version: '1.0.1-ni.42', url: 'https://downloads.example.test/old.apk',
      packages: { android: { version: '1.0.1' } }
    }] });
    const details = page.elements.get('historyAndroid');
    details.open = true;
    details.listeners.get('toggle')();
    await flushPromises();
    const link = page.elements.get('historyListAndroid').children[0].children[0];
    assert.equal(link.textContent, language === 'zh' ? '版本 1.0.1' : 'Version 1.0.1');
    assert.equal(link.href, 'https://downloads.example.test/old.apk');
    assert.equal(page.elements.has('historyIos'), false);
  });
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

    assert.equal(page.elements.get('verIos').textContent, language === 'zh' ? '版本 1.0.4' : 'Version 1.0.4');
    assert.ok(page.requestedUrls.includes('/api/testflight'));
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

for (const language of ['zh', 'en']) {
  test(`${language} iOS follows the TestFlight version independently of Android`, async () => {
    for (const version of ['1.0.1', '1.0.3']) {
      const page = loadDownloadPage(language, { version: '1.0.2' }, version);
      await flushPromises();
      assert.equal(page.elements.get('verIos').textContent, `${language === 'zh' ? '版本' : 'Version'} ${version}`);
      assert.equal(page.elements.get('verAndroid').textContent, language === 'zh' ? '版本 1.0.2' : 'Version 1.0.2');
    }
  });

  test(`${language} unavailable TestFlight metadata does not interfere with other downloads`, async () => {
    for (const version of ['', null]) {
      const page = loadDownloadPage(language, { version: '1.0.2' }, version);
      await flushPromises();
      assert.equal(page.elements.get('verIos').textContent, language === 'zh' ? '版本信息暂不可用' : 'Version unavailable');
      assert.match(page.elements.get('verWindows').textContent, /9\.9\.9/);
      assert.equal(page.elements.get('verAndroid').textContent, language === 'zh' ? '版本 1.0.2' : 'Version 1.0.2');
      assert.match(page.html, /href="https:\/\/testflight\.apple\.com\/join\/MGh9AhgQ" id="btnIos"/);
    }
  });
}

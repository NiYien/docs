const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

class FakeClassList {
  add() {}
  remove() {}
  toggle() {}
  contains() { return false; }
}

class FakeElement {
  constructor(tagName) {
    this.tagName = tagName;
    this.attributes = new Map();
    this.children = [];
    this.classList = new FakeClassList();
    this.frame = null;
    this._innerHTML = '';
  }

  set innerHTML(value) {
    this._innerHTML = value;
    if (this.className === 'lightbox' && value.includes('lightbox-frame')) {
      this.frame = new FakeElement('div');
      this.frame.className = 'lightbox-frame';
    }
    if (this.className === 'lightbox-frame' && value === '') this.children = [];
  }

  get innerHTML() { return this._innerHTML; }
  setAttribute(name, value) { this.attributes.set(name, value); }
  getAttribute(name) { return this.attributes.has(name) ? this.attributes.get(name) : null; }
  appendChild(child) { this.children.push(child); }
  addEventListener() {}
  querySelector(selector) { return selector === '.lightbox-frame' ? this.frame : null; }
}

const siteScript = fs.readFileSync(
  path.join(__dirname, '..', 'v2', 'shared', 'site.js'),
  'utf8'
);
const body = new FakeElement('body');
const document = {
  documentElement: { lang: 'zh-CN' },
  body,
  querySelector() { return null; },
  getElementById() { return null; },
  createElement(tagName) { return new FakeElement(tagName); },
  addEventListener() {}
};
const context = {
  document,
  location: { pathname: '/v2/zh/tutorials.html' },
  URL,
  window: {}
};

vm.runInNewContext(siteScript, context);

function openFrame(url) {
  context.window.NY.openLightbox(url);
  return body.children[0].frame.children[0];
}

const bilibiliFrame = openFrame('https://player.bilibili.com/player.html?bvid=example');
assert.equal(bilibiliFrame.getAttribute('allowfullscreen'), 'true');
assert.equal(bilibiliFrame.getAttribute('allow'), null);

const youtubeFrame = openFrame('https://www.youtube-nocookie.com/embed/example?autoplay=1');
assert.equal(youtubeFrame.getAttribute('allowfullscreen'), 'true');
assert.equal(youtubeFrame.getAttribute('allow'), 'autoplay; fullscreen');

console.log('v2 video iframe contracts: PASS');

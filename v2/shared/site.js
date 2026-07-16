/* NiYien.com v2 — shared nav/footer injection, language switch, lightbox. No deps. */
(function () {
  'use strict';
  var lang = (document.documentElement.lang || 'zh-CN').toLowerCase().indexOf('zh') === 0 ? 'zh' : 'en';
  var page = document.body.getAttribute('data-page') || '';

  var LOGO_SVG = '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="6" fill="#111827"/><circle cx="12" cy="12" r="4.5" stroke="white" stroke-width="2"/><circle cx="12" cy="12" r="1.2" fill="#165DFF"/></svg>';

  var T = {
    zh: {
      nav: [['home','index.html','首页'],['tutorials','tutorials.html','教程'],['download','download.html','下载'],['specs','specs.html','技术参数'],['faq','faq.html','常见问题'],['cameras','cameras.html','支持的相机']],
      cta: '立即下载', langLabel: 'EN', langTarget: '../en/',
      fCols: [
        ['产品', [['下载','download.html'],['技术参数','specs.html'],['更新日志','/changelog/index.html?lang=zh']]],
        ['支持', [['教程','tutorials.html'],['常见问题','faq.html'],['高级用法','/advanced_usage/zh/Camera_Settings.html']]],
        ['社区', [['微信交流群','faq.html#community'],['联系我们','faq.html#contact']]]
      ],
      copyright: '© 2026 NiYien · 每一帧，都稳定'
    },
    en: {
      nav: [['home','index.html','Home'],['tutorials','tutorials.html','Tutorials'],['download','download.html','Download'],['specs','specs.html','Tech Specs'],['faq','faq.html','FAQ'],['cameras','cameras.html','Cameras']],
      cta: 'Download', langLabel: '中文', langTarget: '../zh/',
      fCols: [
        ['Product', [['Download','download.html'],['Tech Specs','specs.html'],['Changelog','/changelog/index.html?lang=en']]],
        ['Support', [['Tutorials','tutorials.html'],['FAQ','faq.html'],['Advanced Usage','/advanced_usage/en/Camera_Settings.html']]],
        ['Community', [['WeChat Group','faq.html#community'],['Contact','faq.html#contact']]]
      ],
      copyright: '© 2026 NiYien · Every frame, stable.'
    }
  }[lang];

  function currentFile() {
    var p = location.pathname.split('/').pop();
    return p === '' ? 'index.html' : p;
  }

  function buildNav() {
    var links = T.nav.map(function (n) {
      var active = (page === n[0]) ? ' class="active"' : '';
      return '<a href="' + n[1] + '"' + active + '>' + n[2] + '</a>';
    }).join('');
    return '<nav class="site-nav"><div class="nav-inner">' +
      '<a class="nav-logo" href="index.html">' + LOGO_SVG + '<span>NiYien</span></a>' +
      '<div class="nav-links" id="nyNavLinks">' + links + '</div>' +
      '<div class="nav-side">' +
      '<a class="lang-switch" href="' + T.langTarget + currentFile() + '">' + T.langLabel + '</a>' +
      '<a class="btn-accent nav-cta" href="download.html">' + T.cta + '</a>' +
      '<button class="nav-burger" id="nyBurger" aria-label="menu">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="#111827" stroke-width="2" stroke-linecap="round"><path d="M4 7h16M4 12h16M4 17h16"/></svg>' +
      '</button></div></div></nav>';
  }

  function buildFooter() {
    var cols = T.fCols.map(function (c) {
      var links = c[1].map(function (l) { return '<a href="' + l[1] + '">' + l[0] + '</a>'; }).join('');
      return '<div class="footer-col"><h4>' + c[0] + '</h4>' + links + '</div>';
    }).join('');
    return '<footer class="site-footer"><div class="footer-inner">' +
      '<div class="footer-brand"><span class="nav-logo">' + LOGO_SVG.replace('#111827', '#FFFFFF').replace('white', '#111827') + '<span style="color:#fff">NiYien</span></span>' +
      '<div class="small">' + T.copyright + '</div></div>' +
      '<div class="footer-cols">' + cols + '</div></div></footer>';
  }

  var navMount = document.querySelector('.nav-mount');
  if (navMount) navMount.outerHTML = buildNav();
  var footMount = document.querySelector('.footer-mount');
  if (footMount) footMount.outerHTML = buildFooter();

  var burger = document.getElementById('nyBurger');
  if (burger) burger.addEventListener('click', function () {
    document.getElementById('nyNavLinks').classList.toggle('nav-links-open');
  });

  /* ---------- lightbox ---------- */
  var lb = document.createElement('div');
  lb.className = 'lightbox';
  lb.innerHTML = '<button class="lightbox-close" aria-label="close">×</button><div class="lightbox-frame"></div>';
  document.body.appendChild(lb);
  function closeLightbox() {
    lb.classList.remove('lightbox-open');
    lb.querySelector('.lightbox-frame').innerHTML = '';
  }
  lb.addEventListener('click', function (e) {
    if (e.target === lb || e.target.classList.contains('lightbox-close')) closeLightbox();
  });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeLightbox(); });
  window.NY = {
    openLightbox: function (embedUrl) {
      lb.querySelector('.lightbox-frame').innerHTML =
        '<iframe src="' + embedUrl + '" allowfullscreen allow="autoplay; fullscreen"></iframe>';
      lb.classList.add('lightbox-open');
    },
    closeLightbox: closeLightbox
  };
})();

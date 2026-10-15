/* Play the translation demo once when it becomes visible. */
(function () {
  'use strict';
  var figure = document.querySelector('.translation-demo');
  if (!figure) return;
  var video = figure.querySelector('video');
  var button = figure.querySelector('button');
  var reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
  var observer;
  var isChinese = (document.documentElement.lang || '').toLowerCase().indexOf('zh') === 0;
  var labels = isChinese
    ? { play: '播放演示', pause: '暂停', resume: '继续播放', replay: '重新播放' }
    : { play: 'Play demo', pause: 'Pause', resume: 'Resume', replay: 'Replay' };

  function updateButton() {
    button.textContent = !video.paused ? labels.pause : video.ended ? labels.replay : video.currentTime > 0 ? labels.resume : labels.play;
  }

  function play() {
    if (video.ended) video.currentTime = 0;
    var request = video.play();
    if (request && request.catch) request.catch(updateButton);
  }

  video.muted = true;
  video.loop = false;
  video.controls = false;
  button.hidden = false;
  button.addEventListener('click', function () {
    if (video.paused) play();
    else video.pause();
  });
  video.addEventListener('play', function () {
    if (observer) observer.disconnect();
    updateButton();
  });
  video.addEventListener('pause', updateButton);
  video.addEventListener('ended', updateButton);

  if ('IntersectionObserver' in window && !reducedMotion.matches) {
    observer = new IntersectionObserver(function (entries) {
      if (entries[0].isIntersecting && entries[0].intersectionRatio >= 0.45 && !reducedMotion.matches) {
        observer.disconnect();
        play();
      }
    }, { threshold: 0.45 });
    observer.observe(video);
  }
})();

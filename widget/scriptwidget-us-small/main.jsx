// US Lower 48 rocket map - small
// Loads only from GitHub Pages; no fallback host.
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://lzq1206.github.io/notam-whisper/widget/us-widget-small.jpg?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://lzq1206.github.io/notam-whisper/">
    <image url={imageUrl} frame="max" ratio={1.0} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

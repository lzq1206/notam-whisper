// US Lower 48 rocket map - medium
// Loads only from GitHub Pages; no fallback host.
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://lzq1206.github.io/notam-whisper/widget/us-widget-medium.jpg?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://lzq1206.github.io/notam-whisper/">
    <image url={imageUrl} frame="max" ratio={2.1301775147928996} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

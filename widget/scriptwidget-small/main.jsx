// 中国火箭地图 - 小号 507×507（3×）
// 图片仅从 GitHub Pages 加载，不请求备用站。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `${base}china-widget-small.jpg?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://lzq1206.github.io/notam-whisper/">
    <image url={imageUrl} frame="max" ratio=1.0 mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

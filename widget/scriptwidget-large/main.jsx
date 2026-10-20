// 中国火箭地图 - 大号 1092×1146（364×382 pt，3×）
// 图片仅从 GitHub Pages 加载，不请求备用站。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `${base}china-widget-large.jpg?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://lzq1206.github.io/notam-whisper/">
    <image url={imageUrl} frame="max" ratio=364 / 382 mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

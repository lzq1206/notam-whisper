// 中国火箭地图 - 大号 1092×1146（364×382 pt，3×）
// 先检查 GitHub Pages 主源；不可用时回退到 rocket.rainywhisper.com。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const primaryBase = "https://lzq1206.github.io/notam-whisper/widget/";
const fallbackBase = "https://rocket.rainywhisper.com/widget/";
const imageFile = "china-widget-large.jpg";
let imageBase = fallbackBase;
try {
  const probe = await Promise.race([
    fetch(`${primaryBase}manifest.json?v=${cacheBucket}`),
    new Promise(resolve => setTimeout(() => resolve(""), 1500))
  ]);
  if (typeof probe === "string" && probe.includes("widget_images")) imageBase = primaryBase;
} catch (_error) {
  imageBase = fallbackBase;
}
const imageUrl = `${imageBase}${imageFile}?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    <image url={imageUrl} frame="max" ratio={364 / 382} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

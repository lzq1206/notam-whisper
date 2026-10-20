// 中国火箭地图 - large 组件
// 持久缓存最近一次成功的 JPEG；主源/备源都失败时继续显示缓存。
const primaryBase = "https://lzq1206.github.io/notam-whisper/widget/";
const fallbackBase = "https://rocket.rainywhisper.com/widget/";
const imageFile = "china-widget-large.jpg";
const cacheKey = "last-good-image-v1";
const cachedBase64 = $storage.getString(cacheKey);
let imageBase64 = "";

async function tryDownload(base) {
  try {
    const result = await Promise.race([
      fetch(`${base}${imageFile}?v=${Math.floor(Date.now() / (3 * 60 * 60 * 1000))}`, { responseType: "base64", timeoutInterval: 1.5 }),
      new Promise(resolve => setTimeout(() => resolve(""), 1800))
    ]);
    // JPEG magic bytes in base64 are /9j/; reject error pages and oversized values.
    if (typeof result === "string" && result.startsWith("/9j/") && result.length <= 250000) return result;
  } catch (_error) {}
  return "";
}

imageBase64 = await tryDownload(primaryBase);
if (!imageBase64) imageBase64 = await tryDownload(fallbackBase);
if (imageBase64) $storage.setString(cacheKey, imageBase64);
else imageBase64 = cachedBase64;

const imageUrl = imageBase64
  ? `data:image/jpeg;base64,${imageBase64}`
  : `${fallbackBase}${imageFile}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    <image url={imageUrl} frame="max" ratio={364 / 382} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

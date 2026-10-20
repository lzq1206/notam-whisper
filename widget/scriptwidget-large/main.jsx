// 中国火箭地图 - large 组件
// 显示持久缓存；远程更新最多每 3 小时尝试一次，不在 SWT 内嵌图片。
const primaryBase = "https://lzq1206.github.io/notam-whisper/widget/";
const fallbackBase = "https://rocket.rainywhisper.com/widget/";
const imageFile = "china-widget-large.jpg";
const cacheKey = "last-good-image-v1";
const attemptKey = "last-attempt-bucket-v1";
const bucket = String(Math.floor(Date.now() / (3 * 60 * 60 * 1000)));
let imageBase64 = $storage.getString(cacheKey);
const lastAttemptBucket = $storage.getString(attemptKey);

async function tryDownload(base) {
  try {
    const result = await Promise.race([
      fetch(`${base}${imageFile}?v=${bucket}`, { responseType: "base64", timeoutInterval: 1 }),
      new Promise(resolve => setTimeout(() => resolve(""), 1200))
    ]);
    // JPEG magic bytes in base64 are /9j/; reject error pages and oversized values.
    if (typeof result === "string" && result.startsWith("/9j/") && result.length <= 250000) return result;
  } catch (_error) {}
  return "";
}

if (lastAttemptBucket !== bucket) {
  // Persist the attempt before networking so repeated WidgetKit refreshes don't hammer hosts.
  $storage.setString(attemptKey, bucket);
  let freshImage = await tryDownload(primaryBase);
  if (!freshImage) freshImage = await tryDownload(fallbackBase);
  if (freshImage && $storage.setString(cacheKey, freshImage)) imageBase64 = freshImage;
}

const imageUrl = imageBase64
  ? `data:image/jpeg;base64,${imageBase64}`
  : "";
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    {imageUrl
      ? <image url={imageUrl} frame="max" ratio={364 / 382} mode="fill" accentedRenderingMode="fullColor" />
      : <text font="caption" color="#9fb3c4">地图暂不可用</text>}
  </zstack>
);

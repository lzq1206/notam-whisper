// 中国火箭地图 - medium 组件
// 先显示上次缓存；首次离线时显示包内地图。远程更新最多每 3 小时尝试一次。
const primaryBase = "https://lzq1206.github.io/notam-whisper/widget/";
const fallbackBase = "https://rocket.rainywhisper.com/widget/";
const imageFile = "china-widget-medium.jpg";
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

const imageElement = imageBase64
  ? <image url={`data:image/jpeg;base64,${imageBase64}`} frame="max" ratio={2.1301775147928996} mode="fill" accentedRenderingMode="fullColor" />
  : <image name={imageFile} frame="max" ratio={2.1301775147928996} mode="fill" accentedRenderingMode="fullColor" />;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    {imageElement}
  </zstack>
);

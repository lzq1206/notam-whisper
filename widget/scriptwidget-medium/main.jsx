// 中国火箭地图 - medium 组件
// 直接将 ScriptWidget 图片视图链接到线上 JPG；主源探测失败时使用备用源。
const primaryBase = "https://lzq1206.github.io/notam-whisper/widget/";
const fallbackBase = "https://rocket.rainywhisper.com/widget/";
const imageFile = "china-widget-medium.jpg";
const bucket = String(Math.floor(Date.now() / (3 * 60 * 60 * 1000)));
const hostKey = "last-good-image-host-v1";
const attemptKey = "last-attempt-bucket-v1";
let selectedBase = $storage.getString(hostKey) || primaryBase;
const lastAttemptBucket = $storage.getString(attemptKey);

async function sourceResponds(base) {
  try {
    const result = await Promise.race([
      fetch(`${base}${imageFile}?v=${bucket}`, { responseType: "base64", timeoutInterval: 1 }),
      new Promise(resolve => setTimeout(() => resolve(""), 1200))
    ]);
    return typeof result === "string" && result.startsWith("/9j/");
  } catch (_error) {
    return false;
  }
}

if (lastAttemptBucket !== bucket) {
  $storage.setString(attemptKey, bucket);
  if (await sourceResponds(primaryBase)) {
    selectedBase = primaryBase;
  } else if (await sourceResponds(fallbackBase)) {
    selectedBase = fallbackBase;
  }
  $storage.setString(hostKey, selectedBase);
}

const imageUrl = `${selectedBase}${imageFile}?v=${bucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    <image url={imageUrl} frame="max" ratio=2.13 mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

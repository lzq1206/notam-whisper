// 中国及周边火箭地图 - ScriptWidget
const size = String($getenv("widget-size") || "small").toLowerCase();
const isLarge = size.includes("large");
const imageName = isLarge ? "china-widget-large.png" : "china-widget-small.png";
// Refresh the cache key only when the server's 3-hour image cycle changes.
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://rocket.rainywhisper.com/widget/${imageName}?v=${cacheBucket}`;

$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/widget/">
    <image
      url={imageUrl}
      frame="max"
      ratio={isLarge ? 364 / 382 : 1}
      mode="fill"
      accentedRenderingMode="fullColor"
    />
  </zstack>
);

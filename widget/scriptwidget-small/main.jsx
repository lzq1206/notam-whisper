// 中国火箭地图 - 小号 507×507（3×）
// 服务端每 3 小时重绘；图片像素按 iPhone 逻辑尺寸 3x 输出。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://rocket.rainywhisper.com/widget/china-widget-small.jpg?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    <image url={imageUrl} frame="max" ratio={1.0} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

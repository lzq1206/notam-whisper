// 中国火箭地图 - 中号 1080×507（3×）
// 服务端每 3 小时重绘；图片像素按 iPhone 逻辑尺寸 3x 输出。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://rocket.rainywhisper.com/widget/china-widget-medium.jpg?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    <image url={imageUrl} frame="max" ratio={2.1301775147928996} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

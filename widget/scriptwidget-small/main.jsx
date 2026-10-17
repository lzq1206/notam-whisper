// 中国火箭地图 - 小号 338×338（169×169 pt @2x）
// 固定图片版本；服务端每 3 小时重绘，iOS 实际刷新时间由系统调度。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://rocket.rainywhisper.com/widget/china-widget-small.png?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/widget/">
    <image url={imageUrl} frame="max" ratio={1.0} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

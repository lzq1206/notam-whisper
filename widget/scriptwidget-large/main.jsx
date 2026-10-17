// 中国火箭地图 - 大号 720×752（360×376 pt @2x）
// 固定图片版本；服务端每 3 小时重绘，iOS 实际刷新时间由系统调度。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://rocket.rainywhisper.com/widget/china-widget-large.png?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/widget/">
    <image url={imageUrl} frame="max" ratio={0.9574468085106383} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

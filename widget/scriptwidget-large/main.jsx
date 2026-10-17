// 中国火箭地图 - 大号 1092×1260（横向不变，纵向扩展约10%）
// 服务端每 3 小时重绘；图片填满画布，不添加上下黑边。
const cacheBucket = Math.floor(Date.now() / (3 * 60 * 60 * 1000));
const imageUrl = `https://rocket.rainywhisper.com/widget/china-widget-large.jpg?v=${cacheBucket}`;
$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/">
    <image url={imageUrl} frame="max" ratio={364 / 382} mode="fill" accentedRenderingMode="fullColor" />
  </zstack>
);

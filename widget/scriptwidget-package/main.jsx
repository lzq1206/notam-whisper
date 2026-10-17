// 中国及周边火箭地图 - ScriptWidget
const cacheBucket = Math.floor(Date.now() / 10000);
const imageUrl = `https://rocket.rainywhisper.com/widget/china-square.png?v=${cacheBucket}`;

$render(
  <zstack frame="max" background="#07141e" linkurl="https://rocket.rainywhisper.com/widget/">
    <image
      url={imageUrl}
      frame="max"
      ratio={1}
      mode="fill"
      accentedRenderingMode="fullColor"
    />
  </zstack>
);

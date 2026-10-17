# iPhone 火箭地图小组件

- 网页预览：`https://rocket.rainywhisper.com/widget/`
- ScriptWidget 一键下载包：`https://rocket.rainywhisper.com/widget/china-rocket-map.swt`
- PNG 订阅源：`https://rocket.rainywhisper.com/widget/china-square.png`
- 地图范围：未来 7 天内有效的 NOTAM/MSI 和发射；颜色渐变仅按此窗口内记录计算。
- 定时：GitHub Actions 每 3 小时生成 1024×1024 PNG；失败时保留上一版。
- ScriptWidget 导入：iPhone 点击 `.swt` 下载链接，点“分享/打开方式”并选择 ScriptWidget 导入，然后添加 ScriptWidget 的小号组件。
- ScriptWidget 包源文件：`scriptwidget-package/widget.json`、`scriptwidget-package/main.jsx`。
- 旧版 Scriptable 用户仍可使用 `Scriptable-widget.js`。

iOS 控制小组件实际刷新时机，配置的 3 小时是目标最早刷新周期，并非准点承诺。

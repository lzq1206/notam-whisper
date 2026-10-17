# iPhone 火箭地图小组件

- 网页预览：`https://rocket.rainywhisper.com/widget/`
- ScriptWidget 组件包：`https://rocket.rainywhisper.com/widget/china-rocket-map.swt`
- 小号组件图片（169pt @2x）：`https://rocket.rainywhisper.com/widget/china-widget-small.png`（338×338）
- 大号组件图片（360×376pt @2x）：`https://rocket.rainywhisper.com/widget/china-widget-large.png`（720×752）
- 网页预览图：`https://rocket.rainywhisper.com/widget/china-square.png`（1024×1024）
- 地图范围：未来 7 天内有效的 NOTAM/MSI 和发射；颜色渐变仅按此窗口内记录计算。
- 定时：GitHub Actions 每 3 小时生成；小/大号图片按对应 iPhone 小组件像素尺寸导出，失败时保留上一版。
- ScriptWidget 包支持小号和大号，按 `$getenv("widget-size")` 自动选用对应图片。iPhone 点击 `.swt` 后通过系统“分享/打开方式”导入 ScriptWidget。
- ScriptWidget 包源文件：`scriptwidget-package/widget.json`、`scriptwidget-package/main.jsx`。
- 旧版 Scriptable 用户仍可使用 `Scriptable-widget.js`。

图片链接使用 3 小时缓存版本号；iOS 的实际刷新时间由 WidgetKit 调度，可能晚于服务端生成。

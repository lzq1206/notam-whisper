# iPhone 火箭地图小组件

- 网页子页/图片预览：`https://rocket.rainywhisper.com/widget/`
- 渲染模板（服务端截图用）：`widget/render.html`
- PNG 订阅源：`https://raw.githubusercontent.com/lzq1206/notam-whisper/main/widget/china-square.png`
- 定时：GitHub Actions 每 3 小时生成 1024×1024 PNG；失败时不提交空图，保留上一版。
- 缓存：Scriptable 请求按 10 秒时间片加查询参数并要求重新验证，减少 CDN/客户端拿到旧图的概率。
- 安装：将 `Scriptable-widget.js` 完整复制到 iPhone Scriptable，新建脚本运行一次，再添加 Scriptable 的小号方形组件并选择此脚本。

iOS 控制小组件实际刷新时机，`refreshAfterDate` 是最早刷新时间，并非准点承诺。

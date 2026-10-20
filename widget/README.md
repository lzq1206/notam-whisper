# 中国及周边火箭动态小组件

- 网页安装教程：`https://lzq1206.github.io/notam-whisper/widget/`
- 小组件点击目标：`https://rocket.rainywhisper.com/`
- 网页地图预览图：`https://lzq1206.github.io/notam-whisper/widget/china-square.png`
- 小号 ScriptWidget 包（169×169 pt，@3x，507×507 px）：`https://lzq1206.github.io/notam-whisper/widget/china-rocket-map-small.swt`
- 中号 ScriptWidget 包（360×169 pt，@3x，1080×507 px）：`https://lzq1206.github.io/notam-whisper/widget/china-rocket-map-medium.swt`
- 大号 ScriptWidget 包（显示框 364×382 pt；图片 1092×1146 px，3×）：`https://lzq1206.github.io/notam-whisper/widget/china-rocket-map-large.swt`
- 三个包分别固定支持 `systemSmall` / `systemMedium` / `systemLarge`，按 iPhone 上准备添加的尺寸导入。
- 组件图片优先从 `lzq1206.github.io/notam-whisper` 加载，连接失败时回退到 `rocket.rainywhisper.com`。
- 图片视图直接引用所选源的线上 JPG URL；优先 GitHub Pages，主源探测失败则切换到备用域名。组件包不内置地图图片，源站选择最多每 3 小时检查一次；设备无法访问图片地址时可能暂时无法显示。
- 导入时请允许组件使用 `network` 和 `storage` 权限。
- 中号图采用 360×169 pt 比例，东西向区域跨度与方图一致，北缘到乌鲁木齐约 43.8°N。
- 小组件图每 3 小时由服务端重绘；iOS 实际刷新时点由系统调度。
- 底图采用 NASA GIBS Blue Marble Shaded Relief and Bathymetry（MODIS）；不在图片内叠加时间或署名，来源在安装网页披露。

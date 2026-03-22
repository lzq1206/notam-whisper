# notam-whisper

Auto-updated Rocket / NOTAM / NavWarnings KML viewer  
https://lzq1206.github.io/notam-whisper/

## 简介
notam-whisper 是一组轻量 Python 脚本与静态页面，用于：

- 抓取航空/航海相关通告（NOTAM、MSI / maritime warnings 等）
- 过滤并解析位置与时间信息
- 导出 CSV 与 KML（用于在 Google Earth / 地图工具中可视化）
- 维护按周归档的历史记录（history/*）

项目目前包含对 notammap.org 的 NOTAM 抓取（fetch_notams.py）和 NGA MSI（Maritime Safety Information）抓取（fetch_msi.py）。仓库同时包含示例输出（.csv/.kml）和一个静态 index.html 用于展示结果（并托管于 GitHub Pages）。

## 主要功能 / 产品特色

- 自动并行抓取（fetch_notams 使用 ThreadPoolExecutor 并行获取每个国家的数据）
- 关键词 KEEP / DROP 过滤，减少噪声（可在 fetch_notams.py 中自定义）
- 智能解析坐标（多种 NOTAM/MSI 坐标格式）并生成 KML（点 / 圆 / 多边形）
- 周度归档（将每周数据合并写入 history/ 子目录）
- 对 NGA MSI 提供备用抓取方式（主 API、fallback 模板、以及文本备忘抓取）
- 生成的人类可读 CSV 方便后续处理、可视化与导出

## 仓库结构（摘要）
- fetch_notams.py       — 抓取并处理 NOTAM（notammap.org）；输出 notams.csv, notams.kml, history/notams/YYYY-WNN.*
- fetch_msi.py          — 抓取并处理 NGA MSI（海事通知）；输出 msi_raw.csv, msi.csv, msi.kml, history/msi/YYYY-WNN.*
- index.html            — 静态页面（演示 / GitHub Pages）
- *.csv / *.kml         — 示例输出（latest.csv, latest.kml, notams.kml 等）
- history/              — 周度归档数据
- msi_fetch_log.txt     — 抓取日志（由 fetch_msi.py 产生）
- tests / example files — 简单测试脚本（test_*.py / test.js）

（注：仓库以 Python 脚本为核心，前端演示用 HTML/JS）

## 环境与依赖
- 推荐 Python 3.8+
- 需要安装：
  - requests
  - urllib3（可选；脚本主要通过 Python 标准库 urllib.request 发送请求，无需单独安装 urllib3，但部分高级 TLS 配置可能会用到）

安装示例：

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install requests
```

（如果需要，我可以把确切依赖写入 requirements.txt）

## 快速开始（本地运行）

1. 克隆仓库

```bash
git clone https://github.com/lzq1206/notam-whisper.git
cd notam-whisper
```

2. 建议启用虚拟环境并安装依赖（见上）

3. 运行 NOTAM 抓取：

```bash
python3 fetch_notams.py
```

- 输出文件：notams.csv, notams.kml
- 会更新 history/notams/YYYY-WNN.csv 和对应 .kml

4. 运行 MSI（海事）抓取：

```bash
python3 fetch_msi.py
```

- 输出文件：msi_raw.csv, msi.csv, msi.kml
- 产生日志 msi_fetch_log.txt
- 更新 history/msi/YYYY-WNN.csv

5. 在 Google Earth/Maps 中打开生成的 .kml 查看可视化结果，或通过 index.html 在浏览器查看（若托管于 GitHub Pages，会自动显示最新资源）。

## 输出文件说明（CSV header）

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw']

字段含义简述：
- country: 数据来源 / 国家标识（若可用）
- id: 源记录 id（内部）
- notam_id: 组合 ID（series+number/year）或 MSI msgID
- fir: 飞行情报区或来源标识
- from_utc / to_utc: 有效起止时间（ISO 格式）
- lat / lon: 中心点（十进制）
- radius_nm: 半径（海里）
- qcode: NOTAM Q-code（若存在）
- raw: 原始文本（处理后的）

MSI 的 raw/coords 解析会先写入 msi_raw.csv（原始文本），再写入 msi.csv（包含经纬度中心点）

## 配置与可调参数
- fetch_notams.py:
  - KEEP / DROP 列表：用于关键词过滤（文件顶部），可按需修改以扩大或缩小匹配
  - 时间窗口：脚本默认过滤未来超过 5 天或已过期的 NOTAM（代码变量名：`five_days`，可按需调整）
- fetch_msi.py:
  - 环境变量 MSI_FALLBACK_URL_TEMPLATE：可设置备用的 MSI 数据源模板 URL（格式中含 {nav_area}）
  - 日志文件 LOG_FILE（默认 msi_fetch_log.txt）
- 如果想用不同的输出目录或文件名，可在脚本中修改对应常量或通过包装脚本/环境变量实现

## 自动化部署建议
- 使用 cron / systemd timer 定时运行（例：每天凌晨 02:00 更新）

Cron 示例（每天 02:00 抓取 NOTAM）：

```cron
0 2 * * * /usr/bin/python3 /path/to/notam-whisper/fetch_notams.py >> /var/log/notam-whisper.log 2>&1
```

- 将结果上载到对象存储或网页托管：
  - 将生成的 notams.kml / msi.kml 发布到静态服务器或 GitHub Pages（本仓库已托管页面：https://lzq1206.github.io/notam-whisper/）
  - 可用 rsync / rclone / GitHub Actions 自动推送到 Pages 或 S3

- Docker（轻示例）

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir requests
CMD ["python3", "fetch_notams.py"]
```

注意：为长期运行，建议把抓取频率交给容器编排或外部任务调度（不要在容器内循环 sleep）

## 日志与故障排查
- fetch_msi.py 会写 msi_fetch_log.txt（请求/解析错误记录）
- 常见问题：
  - 网络超时或证书问题：脚本中对部分请求禁用了证书校验以兼容某些源（谨慎使用）
  - 页面返回 HTML（站点维护或反爬）：检查日志与返回内容前缀
  - 坐标解析失败：NOTAM/MSI 原文格式多样，脚本做了多种正则解析，但仍可能错过某些格式 -> 可在 parse 函数中扩展正则

## 可改进点（建议）
- 将 KEEP / DROP 过滤改为可配置文件（JSON/YAML）以便运行时调整
- 输出加入唯一 ID 与哈希以便更好地去重与差异比对
- 增加 CLI 参数（输出目录、最大并发数、时间窗口等）
- 增加单元测试与集成测试，增加 requirements.txt 与 Docker Compose / systemd 单元文件
- 为 KML 增加更友好的 Description（包含时间、原始文本全文、来源链接等）

## 贡献与许可证
- 当前仓库不包含明确 LICENSE 文件（如果希望对外开放代码，请添加许可证，例如 MIT / Apache-2.0 等）
- 欢迎 PR / issue：请在提交前描述修改目的并包含可复现步骤

## 示例：把抓取加入 GitHub Actions（思路）
- 使用 schedule 触发器每天运行脚本，生成文件并 push 到 gh-pages 分支或上传到 S3
- 注意 GitHub Actions 运行时间限制与私密凭据的安全存放（使用 Secrets）

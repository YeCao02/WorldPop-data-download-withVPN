# WorldPop Data Download with VPN

面向中国大陆网络环境的 WorldPop 批量下载工具。脚本从指定目录中的一个或多个 `.txt` 文件读取下载链接，通过 Clash Verge Rev/Mihomo 外部控制器对当前配置中的代理节点进行真实文件吞吐测速，选择速度最优的直连或代理线路，并使用并行下载、分块、重试和断点续传完成大批量数据获取。

针对 `data.worldpop.org` 宣称支持 Range、实际却返回完整文件的问题，项目提供推荐的 `arcgis` 后端：从 WorldPop 官方 ArcGIS ImageServer 获取支持 HTTP Range 的无损分块，经 aria2 多连接下载后在本地拼接为 GeoTIFF。

## 主要功能

- 扫描输入目录中的所有 `.txt` 文件，逐行提取并去重 HTTP/HTTPS 链接。
- 自动识别 Clash Verge Rev 的混合代理端口、Controller 地址和本地配置。
- 通过 Clash Controller 切换当前手动选择组中的节点。
- 使用真实 WorldPop/ArcGIS 数据而非仅 Ping 延迟进行吞吐测速。
- 同时比较直连与代理线路，自动选择实测速度最高的路径。
- Rich 实时进度条、总下载量、瞬时速率、耗时和预计剩余时间。
- aria2 多文件并行、多连接分段、失败降级重试和断点续传。
- ArcGIS 全球栅格分块下载、本地无重采样拼接、GeoTIFF 完整性校验。
- 已完成文件自动跳过；临时分块仅在最终文件验证成功后清理。
- 保留失败日志与 aria2 任务清单，便于中断后继续。

## 下载后端

| 后端 | 用途 | 断点续传 | 说明 |
| --- | --- | --- | --- |
| `arcgis` | **推荐的 WorldPop 2000–2020 1 km 数据后端** | 支持 | 官方 ArcGIS ImageServer 分块，aria2 下载，本地拼接 GeoTIFF |
| `aria2c` | 直接下载 TXT 中的原始 URL | 取决于服务器 | `data.worldpop.org` 对部分文件不真正支持 Range，只能单连接续下或重新开始 |
| `python` | 无 aria2 时的兼容后端 | 取决于服务器 | aiohttp 下载，自适应文件并发，可运行时复测线路 |
| `auto` | 自动选择 | 取决于后端 | 安装 aria2 时优先直接使用 `aria2c`，否则使用 `python` |

`arcgis` 后端目前识别以下文件名：

- `ppp_YYYY_1km_Aggregated.tif`
- `global_m_60_YYYY_1km.tif` 至 `global_m_80_YYYY_1km.tif`
- `global_f_60_YYYY_1km.tif` 至 `global_f_80_YYYY_1km.tif`

对应 WorldPop 2000–2020 全球 1 km 总人口与男女 60 岁以上五岁年龄组数据。

## 环境要求

- Windows 10/11
- Python 3.11 或兼容版本
- Clash Verge Rev/Mihomo，可选但推荐
- [aria2](https://aria2.github.io/)
- 对 `arcgis` 后端，需要 GDAL/rasterio

推荐使用 Conda：

```powershell
conda create -n GEO python=3.11 -y
conda activate GEO
pip install -r requirements.txt
```

安装 aria2：

```powershell
winget install aria2.aria2
```

## Clash Verge Rev 配置

在 Clash Verge Rev 中启用：

1. **外部控制器**。
2. Controller 监听地址，例如 `127.0.0.1:9097`。
3. 设置一个强 API 访问密钥。
4. 确保混合代理端口可用，例如 `127.0.0.1:7897`。

脚本默认尝试从 Clash Verge Rev 配置目录读取端口和密钥，也可显式传入：

```powershell
python download_links.py --backend arcgis `
  --proxy http://127.0.0.1:7897 `
  --controller http://127.0.0.1:9097 `
  --secret "YOUR_CONTROLLER_SECRET"
```

不要将真实 Controller 密钥写入代码、README 或提交到 GitHub。

Controller 只能看到 **当前已加载 Clash 配置** 中的节点。Clash Verge 中未启用的其他订阅不会自动参与测速；如需同时测试多个订阅，应先在 Mihomo 配置中合并相应节点或代理提供者。

## 准备链接 TXT

创建一个输入目录，并在其中放置任意数量的 `.txt` 文件。每行填写一个完整下载链接：

```text
https://data.worldpop.org/GIS/Population/Global_2000_2020/2000/0_Mosaicked/ppp_2000_1km_Aggregated.tif
https://data.worldpop.org/GIS/Population/Global_2000_2020/2001/0_Mosaicked/ppp_2001_1km_Aggregated.tif
```

规则：

- 文件名不限，例如 `population.txt`、`age60plus.txt`。
- 脚本扫描输入目录下所有 `.txt` 文件。
- 空行和非 HTTP/HTTPS 行会被忽略。
- 重复 URL 会自动去重。
- 输出文件名默认取 URL 最后一段。

要更换数据列表，直接增加、删除或修改输入目录中的 TXT 文件，无需修改 Python 代码。

## 运行

推荐的 ArcGIS 后端：

```powershell
conda run --no-capture-output -n GEO python -u download_links.py `
  --backend arcgis `
  --input-folder "G:\Download-IDM" `
  --output-folder "K:\0_worldpop_Nie"
```

`--no-capture-output` 很重要，否则 `conda run` 可能缓存进度条输出，窗口看起来像没有响应。

仓库同时提供 Windows 启动器：

```powershell
run_arcgis_downloader.cmd `
  --input-folder "G:\Download-IDM" `
  --output-folder "K:\0_worldpop_Nie"
```

如果长期使用固定目录，也可以修改 `download_links.py` 顶部的：

```python
DEFAULT_INPUT_FOLDER = Path(r"G:\Download-IDM")
DEFAULT_OUTPUT_FOLDER = Path(r"K:\0_worldpop_Nie")
```

但更推荐使用命令行参数，避免提交个人路径。

## 常用参数

```text
--backend arcgis                 使用 ArcGIS 分块后端
--input-folder PATH              TXT 链接文件目录
--output-folder PATH             最终 GeoTIFF 输出目录
--proxy auto|direct|URL          自动识别、直连或显式代理
--controller auto|off|URL        Clash Controller 地址
--secret SECRET                  Controller 密钥
--benchmark-candidates 0         测试全部当前可用节点；正数表示最多测试 N 个
--benchmark-bytes-mb 24          每条线路最大测速样本
--arcgis-tile-size 4096          ArcGIS 分块边长，范围 256–4096
--arcgis-split 8                 每个分块的 aria2 连接数
--arcgis-concurrent-tiles 3      同时下载的分块数
--arcgis-keep-tiles              最终验证后仍保留临时分块
--retries 4                      下载与失败块重试次数
--limit N                        只处理前 N 条链接，便于测试
--dry-run                        仅检查配置，不正式下载
```

默认 ArcGIS 组合是 3 个并发分块 × 每块 8 个连接，最多 24 条连接。TLS 不稳定时，失败块会自动按 `8 → 4 → 2 → 1` 降低连接数重试。

## 节点测速逻辑

1. ArcGIS 后端先生成一个支持 Range 的真实数据样本。
2. 获取 Clash 手动选择组中的实际代理节点。
3. 逐个切换节点，并对同一数据样本进行限时吞吐测试。
4. 同时测试不经过系统代理的真正直连路径。
5. 选择 MB/s 最高的线路并固定用于本次 aria2 下载。

延迟低不等于大文件吞吐高，因此最终选择依据是实际下载速率，而不是节点名称、地区或单纯 Ping 值。测试所有节点需要数分钟，可用 `--benchmark-candidates` 缩短测试。

## ArcGIS 分块与本地拼接

ArcGIS ImageServer 单次导出最大约 `4100 × 4100` 像素。脚本默认使用对齐的 `4096 × 4096` 分块：

1. 按年份、性别和年龄变量生成 ArcGIS 导出任务。
2. 保存每个分块的临时 URL 与长度到 `_exports.json`。
3. aria2 使用真实 HTTP Range 下载，并保存 `.aria2` 续传状态。
4. 下载完成后，rasterio/GDAL 按原始行列窗口无重采样拼接。
5. 输出 float32、EPSG:4326、DEFLATE + Predictor 3、512 像素内部块的 GeoTIFF。
6. 校验尺寸、CRS、NoData 和末端数据块可读性。
7. 仅验证成功后删除对应 `_arcgis_tiles` 临时目录。

DEFLATE + 浮点预测器是无损压缩。生成文件的像元精度不会因该压缩降低，但压缩布局、文件大小和校验值不会与网站上的原始 TIFF 完全相同。

## 断点续传与故障恢复

- 直接重新运行相同命令即可继续。
- 已验证完成的 GeoTIFF 会跳过。
- 未完成分块保留 `.aria2` 控制文件。
- ArcGIS 临时 URL 未过期时会被复用。
- URL 过期时，为避免新旧导出字节混合，脚本会安全地重新开始该分块。
- 单块失败不会终止全部数据，脚本会降低连接数重试。
- 最终失败记录写入输出目录的 `download_failures.txt`。
- aria2 日志和清单位于当前 `_arcgis_tiles/<文件名>/` 目录。

不要手动删除 `_arcgis_tiles`，除非确定不再需要恢复未完成任务。

## 输出与磁盘空间

最终文件直接写入 `--output-folder`。中间目录示例：

```text
K:\0_worldpop_Nie\
├── ppp_2000_1km_Aggregated.tif
├── global_m_60_2000_1km.tif
└── _arcgis_tiles\
    └── 当前文件名\
        ├── _exports.json
        ├── _aria2_input.txt
        ├── _aria2.log
        └── r00000_c00000.tif
```

脚本按最终栅格逐个下载和拼接，成功后清理该栅格的分块，因此不需要同时保留完整数据集的双份副本。但拼接单个全球文件时仍需要额外临时空间。

## 注意事项

- 请遵守所在地法律法规、代理服务条款及 WorldPop 数据许可和引用要求。
- 大规模下载前建议先使用 `--limit 1` 验证目录、空间和网络。
- 不要同时运行多个脚本实例写入同一输出目录。
- 系统代理中的“DIRECT”规则不等于脚本的真正直连；`--proxy direct` 会明确绕过代理。
- WorldPop 官方数据与元数据请以 [WorldPop Data Portal](https://hub.worldpop.org/) 为准。
- ArcGIS 数据源：[Total Population 1 km](https://worldpop.arcgis.com/arcgis/rest/services/WorldPop_Total_Population_1km/ImageServer) 与 [Population Cohorts 1 km](https://worldpop.arcgis.com/arcgis/rest/services/WorldPop_Population_Cohorts_1km/ImageServer)。

## 项目文件

- `download_links.py`：入口、Clash 检测、节点测速、Python/aria2 后端和进度显示。
- `arcgis_backend.py`：WorldPop ArcGIS 映射、分块导出、恢复、拼接和校验。
- `run_arcgis_downloader.cmd`：Windows + Conda GEO 快捷启动器。
- `requirements.txt`：Python 依赖。


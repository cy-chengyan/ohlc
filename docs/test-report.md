# ohlc 首版测试报告

软件版本：`0.1.0-beta.1`\
产品源码基准：`2f4ff51`\
验证日期：2026-10-11\
格式：磁盘 6 / 协议 6 / C ABI 4

本报告记录首版源码的实际验证结果。产品契约、格式、算法、容量模型和验收要求见
[设计文档](design.md)。四库基准使用本次工作区的驱动和脚本；产品实现未修改，
测量开始前后的源码与二进制摘要随证据保存。

本轮四库均完成 5000 万条导入与校验。ohlc、ClickHouse、Aerospike 完成约定测项；
InfluxDB 在 32 GiB 限额下的 16 客户端阶段 OOM。结果与条件见[第 4 节](#4-五千万条四库基准)。

## 1. 功能验证环境

| 项目 | 实际环境 |
|---|---|
| 架构 | Linux x86-64 |
| 内核 | RHEL 8，`4.18.0-553.111.1.el8_10.x86_64` |
| 构建与测试用户态 | UBI/EL8 容器，使用宿主机内核 |
| C 编译器 | GCC 8.5.0 |
| Java | JDK 17，JAR 编译目标为 Java 17 |
| 构建类型 | Debug，项目诊断与警告检查开启 |
| 数据目录 | 测试创建的独立临时目录 |

EL8 日志对应 `3b199c7`，与上述产品源码的实现文件一致。
另在第 4 节的 EL9 测试机以 GCC 11.5.0、CMake 3.31.8、Release 构建运行 CTest。
这些结果验证源码构建及程序行为，不代表 RPM 安装验收。

## 2. 回归结果

EL8 Debug 构建的 CTest **11/11 通过**，总用时 7.02 秒，含 JDK 17/JNI。
EL9 Release 构建的 CTest **11/11 通过**，总用时 8.86 秒；该构建检测到系统默认 JDK 8，
明确跳过 Java/JNI，不能据此声称完成 EL9 Java 验证。以下 Java 覆盖来自 EL8 验证。
用时仅反映测试执行，不是产品性能指标。

| 测试 | 验证范围 | 结果 |
|---|---|---|
| `core` | 行编码、时间与块索引、快照、表隔离、WAL、检查点和恢复 | 通过 |
| `concurrency` | 并发访问与快照一致性 | 通过 |
| `pipeline` | 批量提交、队列及失败边界 | 通过 |
| `commit_sync_failure` | 同步失败注入与提交结果 | 通过 |
| `wal_reclaim_crash` | WAL 回收中断后的恢复 | 通过 |
| `embedded` | Python 与 Java/JNI 嵌入式访问 | 通过 |
| `shell_edit` | 终端编辑、历史搜索与终端恢复 | 通过 |
| `network` | C 网络库、Python、Java、shell 和协议边界 | 通过 |
| `transport` | TLS、连接与传输错误 | 通过 |
| `config` | 配置解析、检查及资源参数 | 通过 |
| `admin` | 离线检查、备份与恢复 | 通过 |

关键数据与恢复检查包括：

- 64 B 行、128 行组成的 8 KiB 行情块，以及独立的记录存在位。
- 八字段全部 256 种有效位组合；int64、uint64、int32 极值；NULL 与整数 0 的区别。
- 全 NULL 行仍返回记录；省略 reserved 写入 NULL；完整行覆盖与跨语言往返。
- 非零保留位、非零空字段载荷、越界值和非整数拒绝；失败写入不创建证券。
- WAL 重开与检查点重开、跨表隔离、范围端点、按实际时间排序和证券集合筛选。
- catalog 显式保存最大表号；两个检查点中的表号高水位非法时，即使 CRC 正确也拒绝打开。
- 未知文件或协议版本被拒绝；损坏元数据不会被自动推算或修复。
- shell 的 NULL 输入、可省略 reserved、CSV/TSV 导入和 JSON 无损整数输出。

## 3. 静态与辅助检查

| 检查 | 结果 |
|---|---|
| clang-format 21.1.8 | 通过 |
| 变更 Python 文件语法 | 通过 |
| `git diff --check` | 通过 |
| 四库原生驱动编译 | EL9、GNU C17、Aerospike C SDK 7.5.0，严格警告作为错误 |

### 3.1 RPM 发布验收

2026-10-11 对当前产品实现构建 `0.1.0~beta.1-10.el8/el9` x86-64 RPM。
UBI/EL8 与 UBI/EL9 容器共用 RHEL 8 内核 `4.18.0-553.111.1.el8_10.x86_64`；
使用 GCC 8.5.0 / 11.5.0、RelWithDebInfo、系统 OpenSSL 及 JDK 17。
两平台的 RPM `%check` 均通过全部 11 项 CTest，明确包含 Java/JNI。

| 安装后检查 | EL8 | EL9 |
|---|---|---|
| 八个功能包安装及 RPM 摘要校验 | 通过 | 通过 |
| 默认目录启动、自定义目录写入/查询/重启 | 通过 | 通过 |
| `ProtectSystem=strict` 失败复现、`full` 修复、服务账户权限约束 | 通过 | 通过 |
| 安装后的 C、Python、pkg-config、CMake 接口 | 通过 | 通过 |
| 离线检查、备份及恢复 | 通过 | 通过 |
| JDK 17 / JDK 21 编译、网络、TLS、JNI | 通过 | 通过 |
| 仅有 JDK 21 时 Java RPM 依赖满足及安装 | 通过 | 通过 |
| 全部 20 个 JAR 类为 Java 17 字节码、无预览特性 | 通过 | 通过 |
| 可执行文件及共享库无构建目录 RPATH/RUNPATH | 通过 | 通过 |

原始构建、服务、语言接口日志及已验收程序摘要位于
`build/acceptance/rpm-release10-20261011/`。最终制品收集时核对程序、systemd 单元及
JAR 内容与这些摘要一致，并核对随包文档与仓库一致。RPM 未签名，交付 SHA-256 清单。
安装测试使用隔离容器，不代表独立 EL9 内核、生产主机或 SELinux enforcing 验收。

## 4. 五千万条四库基准

### 4.1 环境与资源限制

| 项目 | 实际配置 |
|---|---|
| 主机 | `home01`，RHEL 9.4，内核 `5.14.0-729.el9.x86_64` |
| CPU | 2 × Intel Xeon Platinum 8260，48 物理核 / 96 逻辑 CPU |
| 物理内存 | 125 GiB；每次仅运行一个被测数据库 |
| 数据盘 | `/ssd02`，KIOXIA EXCERIA PLUS G2 NVMe，XFS，`noatime` |
| 页大小 | 4096 B |
| 服务限制 | cgroup v2：`memory.max=34359738368`，`memory.swap.max=0` |
| CPU 分配 | 服务 CPU 0–15；客户端 CPU 16–23；均位于 NUMA 0 |
| 访问方式 | 本机回环 TCP；C 客户端，完整接收及解码结果 |
| 编译 | 产品 C17 Release `-O3 -DNDEBUG`；驱动 GNU C17（SDK 需要 GNU 扩展） |

32 GiB 限额包含服务进程及计入该 cgroup 的文件缓存，客户端和输入文件缓存不计入。
CPU 集合限制不是独占核心预留；主机原有 MySQL 等非本次测试服务未被停止或删除。
原有 ohlc RPM、服务配置和 `/ssd01/ohlc` 数据已按要求删除，未做备份。
本次源码在独立目录编译，使用临时 systemd 单元运行。

| 数据库 | 版本、存储及索引 | 内部预算与关键设置 |
|---|---|---|
| ohlc | `0.1.0-beta.1`，64 B 行、8 KiB 块，不压缩 | 引擎 1024 MiB、块缓存 64 MiB；成功前同步 WAL |
| ClickHouse | `25.8.31.9`，MergeTree `(ticker,time_key)`，`(time_key,ticker)` 投影；默认压缩 | 服务 16 GiB、单查询 8 GiB、16 线程、mark cache 256 MiB；granularity=256；结果缓存关闭；同步插入与 part fsync |
| InfluxDB OSS | `2.9.1`，TSM/TSI；ticker 为 tag，七个整数 field | 写缓存 1 GiB，64 MiB 快照阈值；WAL fsync delay=0s，GOMAXPROCS=16 |
| Aerospike CE | `8.2.0.0`，C SDK `7.5.0`；组合整数键，`skey` 与 `time_key` 两个二级索引 | 索引预算 16 GiB；64 GiB 稀疏设备文件、direct I/O、flush-max-ms=1000；16 service threads，单副本 |

内部预算并不相同；共同硬上限是 32 GiB。本次是这些明确配置下的结果，未对各库进行全面调参。
Aerospike 测的是设备存储 namespace 与二级索引范围查询，连单行形状也使用二级索引；
不是纯内存配置，也不是主键 get/batch-get。其默认 long query 策略和客户端排序均保留。
Aerospike CE 的确认不保证每次写入都已落盘，且批次不是 ohlc 的整批原子事务；
不能把导入速度解释为同等持久性或事务保证下的排名。
[官方 commit-to-device 说明](https://aerospike.com/docs/database/reference/config/#commit-to-device)。

### 4.2 数据、查询与计时口径

- 每库 **50,000,000 条**：10,000 个证券 × 5,000 个分钟时间点；按时间导入，500 批，每批 100,000 条。
- 四个价格及 volume、amount、adjust_factor 均为整数。确定性生成器包含价格走势、伪随机涨跌和成交量，复权因子固定；合成时段每 390 点换日，每 5 日跳过周末。
- reserved 全部为 NULL：ohlc 实际读取其载荷与有效位，其余三库省略该字段，驱动规范化为 NULL。本次不覆盖非空 reserved、NULL 比例变化、价格极值或稀疏数据。
- ohlc/Aerospike 通过 5,000 次横截面逐值检查全部 5,000 万行；ClickHouse/InfluxDB 检查总行数和七个整数总和。每次计时查询另核验行数、键和全部字段值。
- 热查询每形状 3 轮，每轮 16 次预热、40 次计量，共 120 个延迟样本。分位数使用 nearest-rank，原始单位为 ns。
- 并发每档 3 × 15 秒，客户端交替读取单股 240 行和横截面 10,000 行；报告三轮 QPS 中位数及范围。所有延迟样本保留，p99 合并三轮样本计算。
- 延迟含发送、服务端处理、完整接收、解析以及 Aerospike 客户端排序；独立正确性核验不计入单次延迟，但占用并发吞吐测量窗口。HTTP、专用协议和原生 SDK 的编码成本不同。
- 重开首读原计划每形状 3 次；按用户要求，Aerospike 实际仅执行两次单股 240 行首读，其余首读形状取消。停本库服务后仅对本库文件执行 `POSIX_FADV_DONTNEED`，用 `mincore` 核验残留页，重新启动后发出一次查询。不执行全机 `drop_caches`，启动耗时另记。

数据可以容纳于本机内存，热查询不是超内存规模的随机磁盘性能测试。各库均使用同一套固定热查询及首读目标；并发为相同种子的持续负载，完成的查询数量随速度变化。
“热查询”指完成全量校验和预热后的测量，不保证各库行数据都已缓存；Aerospike 本配置仍使用 direct I/O 读取设备文件。

### 4.3 导入与正确性

四库均完成 5,000 万条导入、校验和热读。InfluxDB 在 16 客户端首轮触及 32 GiB 上限，cgroup 记录 `oom_kill=1`，systemd 确认 `oom-kill`；该档没有完整吞吐量，未继续重开首读。ohlc、ClickHouse、Aerospike 完成约定范围的全部测项，没有 OOM。数据生成耗时 51.286 秒，不计入导入。

| 数据库 | 客户端准备 s | 导入 s | 整理 s | 三项合计 s | 导入行/s |
|---|---|---|---|---|---|
| ohlc | 18.099 | 18.269 | 0.195 | 36.564 | 2,736,806 |
| ClickHouse | 0.000 | 70.908 | 35.066 | 105.975 | 705,134 |
| InfluxDB | 0.000 | 684.849 | 57.632 | 742.481 | 73,009 |
| Aerospike CE | 0.241 | 165.288 | 2.007 | 167.537 | 302,503 |

客户端准备包含 ohlc 建表和逐个证券注册、Aerospike 二级索引创建与等待；其他库的服务端建表及服务启动在此表之外。导入含客户端读输入文件与格式转换；整理分别为 ohlc 检查点、ClickHouse `OPTIMIZE FINAL`、InfluxDB 文件连续稳定 25 秒、Aerospike 等待 2 秒。整理语义不同，合计也不是同等持久性比较。

| 数据库 | 每批 10 万条确认 p50 / p99，ms |
|---|---|
| ohlc | 31.393 / 74.467 |
| ClickHouse | 126.233 / 489.639 |
| InfluxDB | 1347.713 / 1679.435 |
| Aerospike CE | 323.131 / 352.637 |

### 4.4 热查询延迟

单位 ms，每格为 **p50 / p99**。单股范围均返回全部字段，横截面返回 10,000 行。

| 查询 | ohlc | ClickHouse | InfluxDB | Aerospike CE |
|---|---|---|---|---|
| 单股 1 行 | 0.062 / 0.069 | 2.430 / 3.296 | 3.329 / 3.643 | 2.681 / 5.864 |
| 单股 240 行 | 0.396 / 0.454 | 2.587 / 3.259 | 5.265 / 6.893 | 6.791 / 8.660 |
| 单股 2048 行 | 3.027 / 3.241 | 3.971 / 5.187 | 22.104 / 23.824 | 47.198 / 59.501 |
| 单股 5000 行 | 6.137 / 6.470 | 5.903 / 6.501 | 48.212 / 53.831 | 102.468 / 104.757 |
| 横截面 10,000 行 | 8.173 / 8.678 | 54.614 / 59.107 | 429.014 / 658.550 | 205.538 / 208.542 |

### 4.5 并发混合查询

QPS 为每秒完成的查询数，不是记录数；每格为三轮中位数。

| 客户端数 | ohlc | ClickHouse | InfluxDB | Aerospike CE |
|---|---|---|---|---|
| 1 | 217.64 | 36.49 | 4.08 | 9.44 |
| 4 | 862.10 | 139.00 | 9.03 | 32.95 |
| 8 | 1,688.60 | 268.01 | 6.69 | 54.36 |
| 16 | 3,039.81 | 460.07 | 失败（OOM） | 90.39 |

| 16 客户端 | 三轮 QPS 范围 | 单股 240 行 p99，ms | 横截面 p99，ms |
|---|---|---|---|
| ohlc | 3033.60–3042.51 | 1.542 | 10.985 |
| ClickHouse | 453.58–460.52 | 21.393 | 79.576 |
| InfluxDB | 失败（OOM） | — | — |
| Aerospike CE | 90.01–90.94 | 17.129 | 382.296 |

InfluxDB 的 1/4/8 客户端均完成三轮；16 客户端第 1 轮在约 7 秒后失败，没有用该轮部分请求推算 QPS，也没有提高内存上限或降低负载后替换结果。

### 4.6 重开后的首次查询

单位 ms，每格为中位数 `[最小值, 最大值]`；ohlc/ClickHouse 每形状 3 次，Aerospike 按用户要求仅两次单股 240 行首读，其余形状取消。样本很少，不估计 p99。

| 数据库 | 单股 240 行 | 单股 5000 行 | 横截面 10,000 行 |
|---|---|---|---|
| ohlc | 1.290 [1.142, 1.329] | 19.168 [19.060, 20.565] | 61.012 [57.832, 63.373] |
| ClickHouse | 139.463 [132.565, 159.574] | 162.254 [130.515, 171.397] | 217.917 [155.239, 219.238] |
| InfluxDB | 未运行 | 未运行 | 未运行 |
| Aerospike CE | 8.247 [8.172, 8.322] | 未运行 | 未运行 |

| 数据库 | 各形状启动中位数范围，s | 驱逐后最多残留页数 |
|---|---|---|
| ohlc | 0.275–0.279 | 0 |
| ClickHouse | 0.707–0.710 | 1 |
| InfluxDB | 未运行 | — |
| Aerospike CE | 237.640（仅单股 240 行） | 0 |

Aerospike 两次启动分别为 237.714、237.566 秒，首读分别为 8.322、8.172 ms。

这是重开首读，启动时读取的元数据和建立的缓存会影响结果，不能视为所有层级均为空的物理冷盘。Aerospike CE 的主索引和二级索引位于易失进程内存，重开成本与其他库不同。[官方索引存储说明](https://aerospike.com/docs/database/learn/architecture/data-storage/secondary-index/)。

### 4.7 内存与磁盘

| 数据库 | 采样 RSS 峰值 GiB | cgroup 内核峰值 GiB | 文件逻辑 GiB | 磁盘实际分配 GiB |
|---|---|---|---|---|
| ohlc | 0.297 | 5.594 | 4.927 | 4.927 |
| ClickHouse | 0.948 | 12.258 | 11.770 | 11.856 |
| InfluxDB | 32.000 | 32.000 | 0.860 | 0.829 |
| Aerospike CE | 5.957 | 5.987 | 64.000 | 8.820 |

内存统计覆盖导入、整理、校验、热读和并发阶段；RSS 每 0.5 秒采样并对服务 cgroup 内进程求和，cgroup 峰值读取 `memory.peak`，包含文件缓存。重开阶段同样受 32 GiB 硬限额约束，但未纳入此峰值采样区间。磁盘量去除硬链接重复计数，不含输入数据、软件与测试日志；除 InfluxDB 外均在首读完成后统计。

磁盘数字需要按状态解读：

- ohlc 数据文件 4,879,364,096 B、WAL 388,809,280 B；总占用还包括元数据和索引，数据文件包含检查点保留镜像，不能视为净行情载荷。
- ClickHouse 当前有效 part（含时间投影）为 **2.442 GiB**，另有 596 个待清理的 inactive parts，共 **9.327 GiB**。目录实占包含这些文件及分配开销，不能作为压缩后有效数据大小。EXPLAIN 确认横截面使用 `by_time` 投影，读取 40/195313 个 granules。
- InfluxDB 的 **0.829 GiB** 是 OOM 停机后的实际分配量；通过独立文件清单补采，没有重启或验证 OOM 后的恢复，不能把这一磁盘快照视为后续测项通过。
- Aerospike 的 **64 GiB** 是稀疏文件逻辑长度，实际分配 **8.820 GiB**，不是 64 GiB 常驻内存。

### 4.8 本轮结论

在本次配置、稠密数据与完整结果返回的负载下，ohlc 的短范围、横截面和并发混合读取延迟/吞吐较优；
5000 行单股查询的 p50 则是 ClickHouse 略低（5.903 ms 对 6.137 ms）。
ClickHouse 有效压缩数据小于 ohlc 的文件实占；InfluxDB 占盘小，但本轮 16 客户端查询发生 OOM。
Aerospike 设备存储与二级索引模式完成 16 客户端负载，重开重建索引约需 238 秒。
这些结论不外推为其他存储模式、查询接口、数据分布或持久性保证下的产品排名。

## 5. 未完成的验收

ASan/UBSan、独立 EL9 内核上的 RPM 安装、生产 systemd 部署、Linux ARM64、macOS、
SELinux enforcing、FIPS、真实断电与设备故障均未在本次验证。

20 年目标规模、多表持续高压、并发读写、历史补录与修正、证券生命周期、跨主机 TLS 性能
需要独立测量。本报告的单机稠密初始导入与只读负载不能替代这些验收；
设计文档中的空间与内存公式仍属于计算模型。

## 6. 复现与证据

功能回归从仓库根目录执行；需要 C17、CMake、OpenSSL、Python 3.9+。
验证 Java 分支时明确选择 JDK 17 并确认 JNI 已注册，不能仅依据 CTest 项目数量。

```bash
cmake -S . -B build-check -DCMAKE_BUILD_TYPE=Release
cmake --build build-check -j8
ctest --test-dir build-check --output-on-failure -j2
python3 tools/check_format.py /path/to/clang-format-21.1.8
```

基准入口为 [C 驱动](../tests/baseline_driver.c)、[Aerospike 适配器](../tests/baseline_aerospike.h)、
[执行脚本](../tools/baseline_compare.py) 和 [原始结果校验器](../tools/summarize_baseline.py)。
在隔离的 `/ssd02` 目录准备源代码、数据库二进制和 SDK，编译驱动后执行：

```bash
python3 tools/baseline_compare.py --root /ssd02/ohlc-benchmark50m-20261011 \
    --phase main --stocks 10000 --times 5000
python3 tools/summarize_baseline.py /ssd02/ohlc-benchmark50m-20261011 \
    --output /ssd02/ohlc-benchmark50m-20261011/summary.json
```

执行脚本需要免交互 sudo 以创建带资源限制的临时服务；目录须为独立新目录，不能复用已有数据库。
驱动链接 libcurl、MariaDB 客户端库、OpenSSL、zlib、libyaml 和 Aerospike SDK；
完整编译命令保存在证据的 `build-driver.sh`。依赖准备、数据库配置和运行参数也随证据归档。
主测量期间仅结果汇总脚本更新，计时驱动、产品二进制及原执行脚本保持不变。
Aerospike 按用户要求由收尾脚本在第二次首读结果写入后结束；调整事件及收尾脚本均归档，
不将取消的首读样本补记为已测量。

实测时段为 **2026-10-11 17:22:12–18:21:10（Asia/Shanghai）**。
远端证据和新测试数据保留于 `/ssd02/ohlc-benchmark50m-20261011`，测试服务已全部停止。
本地归档已校验 SHA-256，原始样本在本地重新计算后的汇总与服务器逐项一致。

- [完整原始证据归档](../build/acceptance/benchmark50m-20261011/evidence.tar.gz) · [SHA-256](../build/acceptance/benchmark50m-20261011/evidence.tar.gz.sha256)
- [机器可读汇总](../build/acceptance/benchmark50m-20261011/evidence/summary.json) · [测量与计划调整事件](../build/acceptance/benchmark50m-20261011/evidence/main/results/events.jsonl)
- [EL9 构建与回归日志](../build/acceptance/benchmark50m-20261011/evidence/prepare.log) · [驱动编译命令](../build/acceptance/benchmark50m-20261011/evidence/build-driver.sh)
- [制品摘要验证](../build/acceptance/benchmark50m-20261011/evidence/binary-verification.txt) · [源码摘要](../build/acceptance/benchmark50m-20261011/evidence/final-source-sha256.json) · [测量期间源码变化记录](../build/acceptance/benchmark50m-20261011/evidence/source-changes-during-run.json)
- [旧版本与旧数据删除状态](../build/acceptance/benchmark50m-20261011/evidence/legacy-cleanup-verification.json)

证据归档 SHA-256：`3a3c10b42c9cc0a5f53d61cdfcf1cf193b2cad5aefcd61ca1ff11a3baa2dd30f`。

- [EL8 编译与完整回归日志](../build/acceptance/current-format-20261011/test-el8.log)
- [EL8 验证时源码 SHA-256 清单](../build/acceptance/current-format-20261011/source-sha256.json)

原始证据位于被 Git 忽略的工作区目录；上述本地链接适用于持有该工作区的环境，Git 仓库不包含数据库文件或登录凭据。

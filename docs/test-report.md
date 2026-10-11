# ohlc 首版测试报告

软件版本：`0.1.0-beta.1`\
源码基准：`3b199c7`\
验证日期：2026-10-11\
格式：磁盘 6 / 协议 6 / C ABI 4

本报告记录首版源码的实际验证结果。产品契约、格式、算法、容量模型和验收要求见
[设计文档](design.md)。本次文档整理不改变被测代码。

## 1. 验证环境

| 项目 | 实际环境 |
|---|---|
| 架构 | Linux x86-64 |
| 内核 | RHEL 8，`4.18.0-553.111.1.el8_10.x86_64` |
| 构建与测试用户态 | UBI/EL8 容器，使用宿主机内核 |
| C 编译器 | GCC 8.5.0 |
| Java | JDK 17，JAR 编译目标为 Java 17 |
| 构建类型 | Debug，项目诊断与警告检查开启 |
| 数据目录 | 测试创建的独立临时目录 |

此环境验证源码构建及程序行为，不代表 RPM 安装或 systemd 部署验收。

## 2. 回归结果

编译成功，CTest **11/11 通过**，总用时 7.02 秒。用时仅反映测试执行，不是产品性能指标。

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
| `baseline_driver.c` 严格 C 编译检查 | EL9 用户态通过，警告作为错误 |

辅助基准的编译通过不等于数据库对照运行通过；本次未启动 MySQL、ClickHouse 或 InfluxDB
执行性能对照。生命周期统计工具使用明确的 64 B 行、8192 B 块参数。

## 4. 未完成的验收

当前源码基准尚未完成 EL9 全量回归、JDK 21 完整运行验收、ASan/UBSan、RPM 构建与安装、
systemd 服务部署、Linux ARM64 或 macOS 运行验收。

性能、20 年目标规模、多表持续高压、跨主机 TLS 性能、SELinux enforcing、FIPS、
真实断电与设备故障均待独立验证。没有当前版本的吞吐量、延迟或实测容量结论。
设计文档中的空间与内存公式属于计算模型。

## 5. 复现与证据

从仓库根目录，在具备 C17 编译器、CMake、OpenSSL、Python 3.9+ 和 JDK 17 的 Linux 环境执行：

```bash
cmake -S . -B build-check -DCMAKE_BUILD_TYPE=Debug
cmake --build build-check -j4
ctest --test-dir build-check --output-on-failure -j2
python3 tools/check_format.py /path/to/clang-format-21.1.8
```

CMake 根据可用的 Python/JDK 注册跨语言测试；复现本报告须确认包含全部 11 项测试以及 JNI。
测试证据保存在独立目录，不纳入 Git；以下链接用于持有该工作区证据的环境：

- [EL8 编译与完整回归日志](../build/acceptance/current-format-20261011/test-el8.log)
- [验证时变更文件的 SHA-256 清单](../build/acceptance/current-format-20261011/source-sha256.json)

清单记录验证时的文件内容；被测实现对应上述源码基准，文档整理后的文字不参与运行时验证。

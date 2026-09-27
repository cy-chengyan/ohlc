# ohlc

[English](README.md) | 简体中文

ohlc 数据库只用于保存 OHLC 行情数据，它专注于两类查询：

- 读取单只股票在一段时间内的行情。
- 读取某个时间点所有股票的行情横截面。

ohlc 采用 **二维分块、计算寻址、定长行存、不压缩** 的存储方案。
既可以作为嵌入式库使用，也可以作为独立服务运行，提供 C、Python、Java 接口和交互式命令行客户端。

**当前版本：`0.1.0-beta.1` · 目标部署环境：Linux · 许可证：Apache-2.0**

[快速上手](#快速上手) · [客户端库](#客户端库) ·
[RHEL 安装包](#rhel-安装包) · [性能基准](#性能基准) ·
[设计文档](docs/design.md) · [测试报告](docs/test-report.md)

## 功能概览

- 支持创建 `1m`、`3m`、`1d`、`5d` 等周期的表，所有表使用相同的数据结构。
- 行情字段全部采用整数，证券代码精确匹配，支持日期或日期时间输入。
- 单表内的批量写入具备原子性，成功确认写入之前同步 WAL，查询具有快照一致性。
- 支持实时写入、批量导入，以及对已有键对应记录的整行替换。
- 支持 Unix 套接字、TCP、TLS 与身份验证、配置文件和 systemd 集成。
- 命令行客户端支持 `help`、`series`、`cross`、CSV/TSV 导入、导出和脚本执行。
- 提供统计信息、显式检查点，以及离线检查、备份和恢复工具。

ohlc 面向完整行情记录的读取，不提供 SQL、任意表结构、自动聚合或交易日历。
行情记录及其时间戳、单位、缩放比例和复权因子均由应用提供。

## 数据模型

逻辑键为 **`(table, ticker, timestamp)`**；日线表使用日历日期标签表示时间。
每条记录按以下顺序包含七个字段：

| 字段 | 类型 |
|---|---|
| `open`、`high`、`low`、`close` | 各为 `int32_t` |
| `volume` | `uint32_t` |
| `amount` | `uint64_t` |
| `adjust_factor` | `uint32_t` |

每条记录编码后的数据部分为 **32 字节**。索引、元数据、WAL、块内未使用的槽位以及保留的检查点镜像
需要额外空间。C 结构体在内存中可能包含填充字节，其布局不等于存储格式；请使用项目提供的编解码函数。

使用常规插入或写入 API 前，需要先注册一次证券代码。重复注册同一代码会返回已有 ID。
命令行客户端的批量导入功能可以自动注册尚不存在的代码。ID 保持稳定；股票退市后停止写入，
其历史数据仍然保留，也不会产生占位记录。已存储的零是有效值，不表示数据缺失。

范围查询采用 **`[start, end)`**，结果按实际时间排序，补录历史数据后也遵循此规则。
分钟表必须设置时区，输入时间戳必须为整分钟；时间戳中显式指定的 UTC 偏移量优先于表的时区设置。
日线表使用日历日期标签。`3m`、`5d` 等周期仅用于描述调用方提供的行情：
ohlc 不会对时间戳取整、填补缺口或计算聚合结果。

## 快速上手

### 从源码构建

依赖要求：

- 支持 C17 的编译器、CMake 3.20+、构建工具、POSIX 线程和系统时区数据。
- 默认的服务端和客户端构建需要 OpenSSL 1.1.1 或更高版本的开发头文件及库。
  请使用操作系统维护的版本。
- Python 接口及基于 Python 的测试需要 Python 3.9+。
- Java JAR 和 JNI 桥接库需要 JDK 17+；如果没有合适的 JDK，CMake 会跳过这些构建目标。

克隆仓库，然后在仓库根目录构建：

```sh
git clone https://github.com/cy-chengyan/ohlc.git
cd ohlc
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

如果只需要不依赖 OpenSSL 的 C/Python 嵌入式构建，请使用独立的构建目录，并在配置时添加
`-DOHLC_BUILD_NETWORK=OFF -DOHLC_BUILD_JAVA_CLIENT=OFF`。

### 启动服务

```sh
mkdir -p data
./build/ohlcd --data ./data/market --socket /tmp/ohlc-demo.sock
```

保持这个终端中的服务运行，在另一个终端进入仓库根目录并执行：

```sh
./build/ohlc --socket /tmp/ohlc-demo.sock
```

在新建的演示数据库中尝试以下命令：

```text
help;
create bars_1m --period 1m --timezone UTC;
register AAPL;
insert bars_1m AAPL "2026-09-01 09:30:00Z" 10000 10100 9950 10080 1200 12100000 1000000;
insert bars_1m AAPL "2026-09-01 09:31:00Z" 10080 10120 10000 10100 900 9100000 1000000;
series bars_1m AAPL from "2026-09-01 09:30:00Z" to "2026-09-01 09:32:00Z" --all;
cross bars_1m "2026-09-01 09:30:00Z" --all;
stats;
help import;
quit;
```

`series` 返回两条行情，`cross` 返回 09:30 的行情。`--all` 用于取消交互模式下的预览条数限制。
使用 `help examples;` 查看更多命令示例。在服务所在终端按 Ctrl+C 可停止服务；
`quit;` 只退出命令行客户端。如果示例中的套接字路径已被占用，请改用其他路径。

## 客户端库

两种运行模式使用相同的存储引擎和文件格式。**一个数据库目录只能由一个进程持有**：
多个应用需要共享数据库时，请通过服务端访问；不要同时以嵌入式方式打开该数据库的文件。

| 语言 | 连接服务端 | 嵌入式访问 |
|---|---|---|
| C | [`libohlc_client`](include/ohlc/client.h) | [`libohlc`](include/ohlc/ohlc.h) |
| Python | `ohlc.Connection`，通过 C 客户端库 | `ohlc.Database`，通过 C 存储引擎 |
| Java 17+ | `io.ohlc.Ohlc`，无需 JNI | `io.ohlc.Database`，通过 `libohlc_jni` |

### Python

在虚拟环境中从当前源码目录安装 Python 封装。封装会加载前面构建的本地库，不会自行构建或打包这些库。

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./clients/python
export OHLC_CLIENT_LIBRARY="$PWD/build/libohlc_client.so"
export OHLC_LIBRARY="$PWD/build/libohlc.so"
```

以上路径适用于 Linux。在 macOS 开发环境中，请将 `.so` 替换为 `.dylib`。
通过系统安装后，也可以通过系统的库搜索路径找到这些库。

读取快速上手中创建的服务和数据表：

```python
from ohlc import Connection

with Connection(socket="/tmp/ohlc-demo.sock") as client:
    table = client.table("bars_1m")
    with table.series("AAPL", "2026-09-01 09:30:00Z", "2026-09-01 09:32:00Z") as query:
        for chunk in query:
            for time_key, *values in chunk.rows():
                print(table.format_time(time_key), values)
```

也可以不启动服务，直接创建独立的嵌入式数据库：

```python
from ohlc import Database

with Database("./data/python-demo", create=True) as db:
    table = db.create("bars_1m", period="1m", timezone="UTC")
    db.register("AAPL")
    table.insert("AAPL", "2026-09-01 09:30:00Z",
                 (10000, 10100, 9950, 10080, 1200, 12100000, 1000000))
    with table.cross("2026-09-01 09:30:00Z") as query:
        for chunk in query:
            for row in chunk.rows():
                print(row)
    db.checkpoint()
```

创建示例请使用全新的目录。访问已有数据库时，打开时不要传入 `create=True`，并使用
`db.table(name)` 获取已有表。嵌入式应用需要显式安排检查点；关闭数据库不会自动执行检查点。
服务端会在后台调度检查点。两种模式下，成功的写入均已通过 WAL 持久化。

面向批量数据的应用应使用 `write()` 或 `write_encoded()`，并按结果块处理查询数据，
避免为每一行创建 Python 对象。参见[嵌入式示例](examples/embedded.py)和
[接口与所有权约定](docs/design.md)。

### C 和 Java

构建会生成一个 C 嵌入式示例：

```sh
./build/ohlc_example ./data/c-demo
```

[examples/embedded.c](examples/embedded.c) 展示了建表、注册、批量写入和两个方向的查询。
安装后的开发文件提供 `pkg-config` 包 `ohlc`、`ohlc-client`，以及 CMake 目标
`ohlc::ohlc`、`ohlc::client`。

如果构建时提供了 JDK 17+，可以运行 Java 嵌入式示例：

```sh
javac --release 17 -cp build/ohlc-client.jar -d build/examples examples/Embedded.java
java -Djava.library.path="$PWD/build" -cp build/ohlc-client.jar:build/examples \
  Embedded ./data/java-demo
```

每个示例都应使用各自独立的全新目录。Java 网络连接使用 `Ohlc.unix(path)` 或 `Ohlc.connect(...)`；
嵌入式访问使用 `Database.open(...)`。请使用 try-with-resources 管理数据库、连接和查询句柄。
源码参见 [Ohlc.java](clients/java/io/ohlc/Ohlc.java)、
[Database.java](clients/java/io/ohlc/Database.java) 和 [Embedded.java](examples/Embedded.java)。

## 配置与运维

[examples/ohlcd.conf](examples/ohlcd.conf) 说明了各项配置。
请根据部署需要修改数据目录、监听地址和资源预算，并在启动前检查配置：

```sh
./build/ohlcd --config /path/to/ohlcd.conf --check-config
./build/ohlcd --config /path/to/ohlcd.conf
```

命令行参数优先于配置文件；修改配置后需要重启。引擎内存、块缓存、网络缓冲区、连接数和查询超时
均可分别设置。引擎内存预算并不限制整个进程的 RSS 或操作系统的页缓存。

Unix 套接字默认只允许其所有者访问。监听非回环地址的 TCP 服务必须启用 TLS，
并配置只读或读写凭据。凭据从受访问权限保护的私有文件中读取；客户端会验证服务端证书和身份。
未启用身份验证的本地连接具有读写权限。

`ohlc-admin` 提供离线 `check`、`backup` 和 `restore` 命令。
使用这些命令前，必须先停止持有数据库的进程。完整的配置、权限、持久性和备份流程
见[设计文档中的运维说明](docs/design.md)。

## RHEL 安装包

仓库提供适用于 RHEL 8 和 9 的 [RPM 打包配置](packaging/ohlc.spec)。
从源码构建不会自动配置软件包仓库；以下命令假定你已取得与系统匹配的 EL8 或 EL9 RPM 文件及其校验清单。

| 软件包 | 内容 |
|---|---|
| `ohlc-libs` | C 嵌入式运行库 |
| `ohlc-client-libs` | C 网络客户端运行库 |
| `ohlc-server` | 服务进程、离线管理工具、配置文件和 systemd 单元 |
| `ohlc-client` | 支持交互和脚本执行的命令行客户端 |
| `ohlc-devel` | C 头文件、链接器符号链接、pkg-config 和 CMake 元数据 |
| `python3-ohlc` | Python 网络和嵌入式接口 |
| `ohlc-java` | Java API JAR |
| `ohlc-jni` | Java 嵌入式访问所需的本地桥接库 |

进入对应系统版本的 RPM 目录，验证校验清单并安装服务端和命令行客户端。
对于本地构建、未签名的 beta RPM，下面的 `--nogpgcheck` 仅作用于本次命令，不会更改系统全局的签名策略。

```sh
sha256sum -c SHA256SUMS
sudo dnf install --nogpgcheck ./ohlc-libs-[0-9]*.rpm ./ohlc-client-libs-[0-9]*.rpm \
  ./ohlc-server-[0-9]*.rpm ./ohlc-client-[0-9]*.rpm
sudo systemctl enable --now ohlc
sudo -u ohlc ohlc --socket /run/ohlc/ohlcd.sock --execute 'help examples;'
```

RPM 默认使用配置文件 `/etc/ohlc/ohlcd.conf`、数据库目录 `/var/lib/ohlc/database`，
以及私有套接字 `/run/ohlc/ohlcd.sock`。服务以 `ohlc` 账户运行。卸载软件包会保留数据库文件。
请使用同一发布版本的软件包和语言接口。

## 存储结构概览

当前布局将 **16 个证券代码 × 8 个时间位置** 组织为一个 4 KiB 数据块。
一份主分块布局服务于两个查询方向。数据块保存在大文件中；证券代码字典、时间映射和位置元数据常驻内存。
记录是否存在由显式的存在位标记。

这种布局有利于简化寻址和读取完整记录，同时也有明确的空间取舍：只写入了部分记录的数据块仍占用整个块。
如果股票的上市、退市时间分布较为分散，块内填充率可能降低，横截面查询的读放大可能增加，
尤其是在不断新增证券代码的情况下。检查点也会保留较旧的物理镜像；当前 beta 版本不会对其进行压实回收。

格式和算法详见[存储结构图](docs/images/ohlc-storage-index-overview.svg)、
[寻址示意图](docs/images/ohlc-block-addressing.svg)和[详细设计文档](docs/design.md)。

## 性能基准

测试日期为 **2026-09-27**，数据规模为 **85,800,000 条：10,000 只股票 × 8,580 根分钟线**。
测试运行于配备 NVMe 存储的 RHEL 9.4 x86-64 环境，TCP 客户端与服务端位于同一台主机。
每次计时查询都会接收并解码全部七个字段。以下结果针对特定配置，不代表普遍适用的性能排名。

| 测试项目 | ohlc beta.1 | MySQL 8.0.46 | ClickHouse 25.8.31.9 | InfluxDB 2.9.1 OSS |
|---|---:|---:|---:|---:|
| 批量提交耗时，秒 | 19.333 | 443.147 | 100.021 | 1,175.621 |
| 240 行单股历史，热缓存 p50，毫秒 | 0.257 | 0.400 | 2.629 | 5.011 |
| 10,000 行横截面，热缓存 p50，毫秒 | 4.313 | 5.054 | 59.884 | 421.611 |
| 重新打开后的横截面，中位数，毫秒 | 52.162 | 8.382 | 274.517 | 502.065 |
| 16 客户端混合读取，中位数 QPS | 3,792.5 | 5,256.9 | 389.7 | 失败：OOM |

ohlc 的批量提交速度约为 **444 万行/秒**。加上证券代码注册和导入后的检查点，这三个阶段共耗时
37.60 秒；该总时间不包含初始化和验证。测试中的写入配置为先持久化，再返回成功确认。

主机内存约为 125 GiB，每个服务的 cgroup 内存上限为 32 GiB。
ohlc 使用 64 MiB 块缓存和 1 GiB 引擎内存预算；MySQL 使用 16 GiB 缓冲池。
整个数据集能够容纳在主机内存中。首次查询测试仅在重新打开数据库之前驱逐测试数据库文件的页缓存；
启动过程会重新加载元数据。各产品的协议、缓存预算和存储布局有所不同。

批量导入是 ohlc 表现最突出的项目。在长区间查询的延迟中位数、16 客户端混合读取吞吐量和
重新打开后的首次横截面查询中，MySQL 更快。InfluxDB 的 16 客户端测试失败，
该结果保留为失败，不作为吞吐量数值报告。

[完整测试报告](docs/test-report.md)包含配置、样本数、尾延迟、磁盘与内存测量、正确性检查、
复现命令，以及额外的 15 种退市与新增替换场景。基准程序见
[tests/baseline_driver.c](tests/baseline_driver.c) 和 [tools/baseline_compare.py](tools/baseline_compare.py)。
原始证据归档作为独立产物保存，不纳入 Git 版本管理；报告列出了校验值，但尚未提供公开下载链接。

## Beta 版本范围

- **RHEL 9 x86-64：** 已在 Linux 测试服务器上验证，包括软件包安装和服务运行。
- **RHEL 8 x86-64：** 已在 RHEL 9 内核上使用 UBI 8 用户空间和 systemd 测试；
  尚未验证独立的 RHEL 8 内核环境。
- **Linux ARM64：** 保留源码构建支持；尚未提供经过验证的 ARM64 发布二进制文件或
  Linux ARM64 运行验收结果。
- macOS 用于开发，Linux 是目标部署环境。

当前 beta 不支持逐行删除、自动数据压实回收、复制、集群或跨进程共享文件访问。
系统原样保存调用方提供的复权因子，不计算或延续因子。文件可能保留历史检查点镜像，
因此规划磁盘容量时不能只按每条记录 32 字节计算。

20 年目标规模、多表持续生产负载、SELinux 强制模式、FIPS 模式，以及真实断电或设备故障下的行为
仍需单独验证。评估是否部署此 beta 时，请关注对应版本的[实现与验证边界](docs/design.md)。

## 开发与贡献

构建项目，并运行当前平台及已安装工具链所支持的检查：

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

运行 Unix 套接字测试时，请使用较短的临时目录路径。
仅限 Linux 的故障注入检查和可选的语言接口测试取决于平台与可用工具。

贡献代码应保持清晰的控制流程和所有权关系，使用 C17、英文代码注释、四空格缩进、
`char* pointer` 形式的指针声明，并且每条声明只声明一个变量。
格式化使用 clang-format 21.1.8，参见 [tools/requirements.txt](tools/requirements.txt) 和
[tools/check_format.py](tools/check_format.py)。完整编码规范见 [docs/design.md](docs/design.md)。

提交问题报告时，请提供 ohlc 版本、平台、配置、最小复现示例，以及预期结果和实际结果。
性能报告还应包含数据分布、缓存状态、结果大小，并注明测量的是服务端访问还是嵌入式访问。
请勿包含凭据或私有行情数据。

[设计文档](docs/design.md)是产品与技术约定的权威依据；[测试报告](docs/test-report.md)记录实测结果。
这两份文档目前均为中文。

## 许可证

Apache License 2.0，详见 [LICENSE](LICENSE)。

# extent 在线复用专项验证（2026-11-12）

本轮完成了[设计文档第 13.6 节](../design.md#136-存活表-extent-在线复用)对应的 Linux 专项检查和缩小规模空间测量，未发现需要修改引擎实现的问题。
测试、负载驱动和文档已加入工作区；引擎仍为 `58f4a56bd653b513255b579b75fe757ffb5d6e73`。
结果只覆盖下面记录的执行环境、数据和故障边界，不代表真实断电或大规模长期生产验收。

## 环境与来源

| 项目 | 本轮实际配置 |
| --- | --- |
| 主机与运行用户 | home01（192.168.7.188），`ohlc`，uid/gid 1001 |
| 测试目录 | `/home/ohlc/extent-reuse-20261112/` |
| 系统 | RHEL 9.4 用户空间，Linux `5.14.0-729.el9.x86_64` |
| CPU / 内存 | 双路 Xeon Platinum 8260，96 逻辑 CPU，约 125 GiB |
| 文件系统 | `/home`，XFS，`/dev/mapper/rhel-home` |
| 工具链 | GCC 11.5.0，CMake 3.31.8，Python 3.12.13 |
| 被测分支 | `extent-reuse`，`58f4a56bd653b513255b579b75fe757ffb5d6e73` |
| 对照分支 | `main`，`b3a4cc60236a6b9799475f28cc9d85faf94f77a3` |
| 构建 | Debug 完整构建；独立 ASan/UBSan、TSan 构建；空间测量使用 Release |

构建、临时库、混合负载及日志均由 `ohlc` 用户拥有。未改动已有测试库、生产服务或系统参数。
没有固定 CPU、关闭 SMT、限制 cgroup、清空系统页缓存或控制设备缓存，因此不作吞吐或延迟优劣结论。
home01 默认 Java 为 8，CMake 明确跳过要求 JDK 17+ 的 Java/JNI 构建；本轮未重新验证 Java。
本地 home03 的 RHEL 8 / GCC 8.5 Debug 原有 13 项 CTest 也通过，但主要证据以下面的 home01 记录为准。

## 正确性与故障验证结果

新增 `tests/extent_reuse_test.c`，注册 `extent_reuse` 和 Linux 专用 `extent_reuse_crash` 两项 CTest。
测试采用独立临时库，检查完整行字节、NULL、整数边界、目录指向及实际数据/元数据文件长度。
成功用例删除自己创建的临时库；失败时保留现场。

| 对应要求 / 场景 | 实际执行与观察 | 结果 |
| --- | --- | --- |
| 13.6-1，反复覆盖 | 32 证券 × 128 时间，24 次完整重写；Q=32，第三轮起 P=96；跨数据卷复用 | 通过 |
| 13.6-2，旧快照 | 磁盘快照持有期间连续重写，原块原始字节不变，游标返回旧值；关闭旧游标、保持新游标开启后旧 extent 被复用，文件不增长 | 通过 |
| 13.6-3，缓存 | 同一组确实再次使用原卷号和偏移，随后读取全部行；同时读取另一张表的相同逻辑坐标 | 通过 |
| 13.6-4，崩溃 / 回退 | 在 data、meta、index、catalog、CURRENT、数据库目录同步之后分别退出进程；确认原物理位置已写成新值；正常恢复后再破坏较新 CURRENT，从较旧入口恢复全部已确认值，并再次检查点、重开核对 | 6/6 通过 |
| 13.6-5，重开 | 打开时禁止复用；第一次检查点增加 32 块，之后复用候选块且文件不再增加；重开前后结果一致 | 通过 |
| 13.6-6，合并失败 | 通过普通预算竞争强制连续两次内存合并失败；旧 backing 仍可读且待回收列表只记录一次，后续成功合并和多轮复用结果正确 | 通过 |
| 13.6-7，并发 | 4 个读者；每轮检查点捕获之后强制发生新提交；80 轮中 77 轮不增加物理块；TSan 最终运行完成 13,408 次完整快照查询 | 通过，无 TSan 报告 |
| 不等长 extent / 空洞 | 16 个 band，每组 extent 为 1–16 块；193 块卷容量跨越 128 项元数据页边界；12 轮覆盖，Q=272、P=449，最后四轮稳定；未写入的时间仍为空 | 通过 |

合并失败测试只占用普通引擎预算，不消耗检查点预留。第二轮显式清除测试中的内存压力提示，
使已释放竞争内存后的下一轮仍走并发合并路径；不修改产品的接纳逻辑。
崩溃注入复用已有 `tests/fault_inject.c`，在指定 `fsync` / `fdatasync` 成功后 `_exit(86)`；
这是进程崩溃验证，内核页缓存仍存在，未模拟真实断电、扇区撕裂或掉电写入重排。

| 构建 / 检查 | 结果 | 主要日志 |
| --- | --- | --- |
| Debug 最终完整 CTest | 15/15，12.85 秒 | `debug-ctest-final.log`、`debug-final-details.log` |
| ASan/UBSan | core、concurrency、pipeline、memory、extent_reuse 共 5/5；扩展不等长 extent 和重叠提交场景后重跑专项也通过 | `asan-ctest.log`、`asan-details.log`、`asan-extent-final.log` |
| TSan | concurrency、pipeline、extent_reuse 共 3/3；扩展场景后重跑专项通过，21.83 秒 | `tsan-ctest.log`、`tsan-details.log`、`tsan-extent-final.log` |

ASan 使用 `detect_leaks=1:halt_on_error=1`，UBSan 和 TSan 使用 `halt_on_error=1`，没有添加抑制规则。
故障注入测试仅在普通 Debug 构建中执行，未把 LD_PRELOAD 故障 shim 与 Sanitizer 混用。
最初快照测试错误地假定 next-fit 总会先返回最低偏移；已改为在可用范围内检查实际复用和文件不增长，
未因此修改分配算法。最终上述测试全部通过。

## 追加与历史替换的空间测量

驱动为 `tools/benchmark_extent_reuse.py`，两次运行使用同一脚本、参数和独立数据库，只替换加载的原生库：

- 256 个证券，UTC 分钟表；调用方生成从 `2026-01-01T00:00:00Z` 起的连续时间。
- 种子为 2,048 个时间、524,288 行，按每 128 个时间写入并检查点。
- 256 轮，每轮追加 8 个时间的全市场数据，再替换 8 个已有时间的全市场数据，然后检查点。
- 历史位置由轮次的确定性公式选择。每轮核对新增和替换的横截面；最终有效数据为 4,096 个时间、1,048,576 行。
- 关闭前和重新打开后各核对全部 1,048,576 行的八字段及 NULL；两种库均通过，总计 4,194,304 行完整核对，另有逐轮横截面检查。
- 引擎预算 512 MiB，块缓存 8 MiB，数据卷上限为 4 KiB 文件头加 8,192 个块，WAL 段 16 MiB。
- 负载连续运行、不按真实分钟等待，没有并行读者；不使用原始行情种子。本轮用于空间增长对照，不是 24 小时混合服务压测。

P 由所有数据卷的实际文件长度计算，包含未引用卷和空闲槽位；Q 按本用例已完整核对的稠密网格计算。
下表的字节数是逻辑文件长度；未将 `st_blocks`、预分配或设备写入量当成 P。

| 轮次 | Q | extent-reuse P | extent-reuse P/Q | main P | main P/Q |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 4,096 | 4,096 | 1.000 | 4,096 | 1.000 |
| 64 | 5,120 | 6,247 | 1.220 | 28,928 | 5.650 |
| 128 | 6,144 | 7,389 | 1.203 | 53,760 | 8.750 |
| 192 | 7,168 | 8,490 | 1.184 | 78,336 | 10.929 |
| 256 | 8,192 | 9,512 | 1.161 | 103,424 | 12.625 |

| 最终观测 | extent-reuse | main |
| --- | ---: | ---: |
| 数据卷数 | 2 | 13 |
| 数据文件总长度，含文件头 | 77,930,496 B（74.320 MiB） | 847,302,656 B（808.051 MiB） |
| 配套元数据文件总长度，含文件头 | 312,576 B | 3,362,816 B |
| 引擎累计数据写入字节 `data_bytes` | 847,249,408 B | 847,249,408 B |

这个负载下最终数据文件逻辑长度减少约 90.8%。累计数据写入量相同：检查点仍重写变化组，
复用减少的是分配增长。P/Q 的具体值取决于负载、快照寿命和碎片，不能把 1.161 视为产品容量保证。
原始 JSONL 包含执行时间，但这里只作空间比较，不把单轮、未隔离且受缓存影响的耗时解释为性能提升。

## 复现与证据

所有命令在相应源码根目录运行，构建输出和测试库位于源码目录之外。主要命令如下：

```sh
cmake -S . -B ../debug -DCMAKE_BUILD_TYPE=Debug -DPython3_EXECUTABLE=/usr/bin/python3.12
cmake --build ../debug -j6
TMPDIR=/home/ohlc/extent-reuse-20261112/tmp ctest --test-dir ../debug --output-on-failure -j2

cmake -S . -B ../asan -DCMAKE_BUILD_TYPE=Debug -DOHLC_SANITIZE=ON \
    -DOHLC_BUILD_NETWORK=OFF -DOHLC_BUILD_JAVA_CLIENT=OFF
cmake --build ../asan -j6
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
    ctest --test-dir ../asan --output-on-failure

cmake -S . -B ../tsan -DCMAKE_BUILD_TYPE=Debug -DOHLC_THREAD_SANITIZE=ON \
    -DOHLC_BUILD_NETWORK=OFF -DOHLC_BUILD_JAVA_CLIENT=OFF
cmake --build ../tsan -j6
TSAN_OPTIONS=halt_on_error=1 ctest --test-dir ../tsan --output-on-failure \
    -R '^(extent_reuse|concurrency|pipeline)$'

cmake -S . -B ../release -DCMAKE_BUILD_TYPE=Release -DOHLC_BUILD_TESTS=OFF \
    -DOHLC_BUILD_NETWORK=OFF -DOHLC_BUILD_JAVA_CLIENT=OFF
cmake --build ../release -j6
python3.12 tools/benchmark_extent_reuse.py --library ../release/libohlc.so \
    --database /home/ohlc/extent-reuse-20261112/mixed-reuse-new
```

对照库由上述 `main` 提交的独立源码按相同 Release 参数构建；将 `--library` 改为
`../main-release/libohlc.so`，并指定另一个不存在的 `--database` 路径。驱动拒绝覆盖已有目录。

证据根为 home01 的 `/home/ohlc/extent-reuse-20261112/evidence/`：

- `environment.txt`：用户、系统、CPU、文件系统和工具链。
- `*-configure.log`、`*-build*.log`、上述 CTest 日志：构建及执行输出。
- `mixed-reuse.jsonl`、`mixed-main.jsonl` 及对应 `.stderr`：空间轨迹和完整核对结果。
- `source.tar.gz`、`source-sha256.txt`、`binaries-sha256.txt`：被测引擎、测试、驱动和二进制指纹。
- `validation.patch`：本轮工作区改动，包含新增测试、驱动及文档。

混合测试库保留在同级 `mixed-reuse/` 和 `mixed-main/`，构建分别保留在 `debug/`、`asan/`、
`tsan/`、`release/` 和 `main-release/`。证据另复制至本地 `run/extent-reuse-home01-20261112/`。

Release 原生库 SHA-256：

- extent-reuse：`0622e27ca5c6c1daf41ac07bb97002eb4adf7ccd36b1a2df3af841e5dfabdeea`
- main：`8931f451dac4e950fa0e2d28d6ec7946ce32d0640c6204be68f76e4e44c59382`

## 限制与后续范围

本轮没有修改 `src/`、公开 API、磁盘格式或协议。新增测试没有在 macOS、ARM64 或真实断电环境运行。
百万行合成数据和短程混合负载不能替代历史 home01 亿行/十亿行数据上的长时间验证。
既有文件不会缩短；打开时的候选标记只覆盖已加载的数据卷，完全孤立且未加载的旧卷不由此找回。
长寿命读根、分配预算不足及碎片仍可能推迟复用；本轮未把这些条件下的空间占用归纳成固定上界。

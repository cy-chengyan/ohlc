# 0.1.0 首版 RPM 构建与安装验收报告（2026-11-12）

## 范围与结论

首个正式版为 `0.1.0`，RPM 发布号为 `1`。EL8、EL9 x86-64 软件包的构建与安装验收通过。
本报告记录实测环境与结果；包划分、部署和运行契约以[设计文档第 22 节](../design.md#22-部署与维护)为准。

每个目标提供八个功能 RPM、七个调试 RPM 和一个源码 RPM。功能包为 `ohlc-libs`、
`ohlc-client-libs`、`ohlc-server`、`ohlc-client`、`ohlc-devel`、`python3-ohlc`、`ohlc-java` 和 `ohlc-jni`。
源码、Python wheel、Java JAR、EL8/EL9 包及 SHA-256 清单通过
[0.1.0 正式版](https://github.com/cy-chengyan/ohlc/releases/tag/0.1.0)交付。

## 环境与方法

- 主机为 home01，RHEL 9.4 x86-64，内核 `5.14.0-729.el9.x86_64`。
- 构建入口为 `ohlc` 账号的 `/home/ohlc/build-rpm all`，环境位于 `/home/ohlc/rpm-env`。
  `rpmbuild` 和编译器以容器内 UID 1001 的 `ohlc-builder` 运行，使用只读构建根、私有网络和独立输出目录。
- EL8、EL9 构建依赖来自 Red Hat 公共 UBI 仓库，验证仓库 RPM 签名。
  安装验证使用独立、可写的 systemd 容器；RPM 数据库分别使用 BDB 和 SQLite。
- 原生包采用 `RelWithDebInfo`、发行版编译与链接选项，不启用 ASan、UBSan 或 TSan。
  JAR 使用 `javac --release 17`，全部 20 个 class 的字节码版本为 61，无预览特性。
- 测试库位于容器专用目录；不读取或修改 `/hd01/quotation`。宿主机不安装本次被测 RPM。

| 项目 | EL8 | EL9 |
| --- | --- | --- |
| 构建用户空间 | RHEL / UBI 8.10 | RHEL / UBI 9.8 |
| GCC | 8.5.0 | 11.5.0 |
| CMake | 3.26.5 | 3.31.8 |
| Python | 3.9.25（python39） | 3.9.25 |
| OpenSSL 库 | 1.1.1k | 3.5.8 |
| 构建 JDK | 17.0.20.1 | 17.0.20.1 |
| 安装后验证 JDK | 17.0.20.1、21.0.13 | 17.0.20.1、21.0.13 |

完整依赖版本保存在各目标的 `build-packages.txt` 和安装验证的 `environment.tsv` 中。

## 验证结果

| 验证项 | EL8 | EL9 |
| --- | --- | --- |
| RPM `%check`：全部 13 项 CTest | 通过 | 通过 |
| 八个功能包依赖安装、版本与运行库加载 | 通过 | 通过 |
| systemd 启动就绪、私有目录及 Unix socket 权限 | 通过 | 通过 |
| 服务 SIGKILL 后自动重启，已确认记录保持一致 | 通过 | 通过 |
| 安装后的 C 示例、pkg-config 与 CMake 消费 | 通过 | 通过 |
| 安装后的 Python 网络与嵌入式接口 | 通过 | 通过 |
| JDK 17 网络、TLS、JNI 与嵌入式示例 | 通过 | 通过 |
| JDK 21 网络、TLS、JNI 与嵌入式示例 | 通过 | 通过 |
| 原生二进制不含构建目录 RPATH/RUNPATH | 通过 | 通过 |
| 离线检查、备份、恢复及恢复库检查 | 通过 | 通过 |
| 卸载保留数据及修改过的配置，重装后读回一致 | 通过 | 通过 |
| 默认及自定义数据目录启动、写入和重启 | 通过 | 通过 |
| 数据目录权限拒绝、恢复权限后读回一致 | 通过 | 通过 |
| RPM 内容摘要、制品 SHA-256 校验 | 通过 | 通过 |

构建检查入口为 `packaging/ohlc.spec` 的 `%check`。安装验证使用
`tests/rpm_acceptance.sh`、`tests/rpm_java_compatibility.sh` 和 `tests/rpm_data_directory.sh`。
自定义数据目录测试包含预期拒绝场景：`ProtectSystem=strict` 阻止未授权路径写入，
root 所有的私有数据库目录阻止服务账号访问；恢复规定配置或权限后成功读回数据。

## 交付与证据

- 功能包合集：`ohlc-0.1.0-1.el8.x86_64.tar.gz`、`ohlc-0.1.0-1.el9.x86_64.tar.gz`，
  各含八个功能 RPM 与包内 `SHA256SUMS`。
- 调试包合集：`ohlc-0.1.0-1.el8.debug.tar.gz`、`ohlc-0.1.0-1.el9.debug.tar.gz`。
- 源码 RPM：`ohlc-0.1.0-1.el8.src.rpm`、`ohlc-0.1.0-1.el9.src.rpm`。
- `BUILD_INFO.json` 记录发布源码提交、构建环境和制品来源；发布页的 `SHA256SUMS` 覆盖交付文件。
- 构建与安装证据包：`ohlc-0.1.0-rpm-validation.tar.gz`，保存构建日志、依赖清单、安装验证日志和摘要。
- home01 构建证据：`/home/ohlc/rpm-env/artifacts/el8/latest/`、
  `/home/ohlc/rpm-env/artifacts/el9/latest/`；各目录包含具体来源提交和独立校验清单。
- home01 安装证据：`/home/ohlc/rpm-env/deployments/release-0.1.0-20261112-el8/`、
  `/home/ohlc/rpm-env/deployments/release-0.1.0-20261112-el9/`。
- 本地发布证据：`run/release-0.1.0-20261112/`；交付文件位于 `dist/ohlc-0.1.0-20261112/`。

## 覆盖边界

两套隔离用户空间共享 home01 的 RHEL 9 内核，EL8 结果不等同于独立 RHEL 8 内核测试。
本轮不包含 ARM64、macOS、SELinux 强制模式、FIPS、真实断电或完整 S/C/L 性能验收。
RPM 未使用发行者签名密钥，交付 SHA-256 摘要；摘要校验不代表发行者签名认证。

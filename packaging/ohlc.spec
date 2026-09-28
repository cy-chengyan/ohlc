# SPDX-License-Identifier: Apache-2.0
%global upstream_version 0.1.0-beta.1
%global python_version 3.9
%global python /usr/bin/python3.9
%global python_sitelib /usr/lib/python3.9/site-packages

Name:           ohlc
Version:        0.1.0~beta.1
Release:        7%{?dist}
Summary:        Fixed-row OHLC storage engine and service
License:        Apache-2.0
URL:            https://github.com/cy-chengyan/ohlc
Source0:        ohlc-%{upstream_version}.tar.gz
ExclusiveArch:  x86_64 aarch64

BuildRequires:  gcc
BuildRequires:  cmake >= 3.20
BuildRequires:  make
BuildRequires:  redhat-rpm-config
BuildRequires:  openssl-devel >= 1.1.1
BuildRequires:  openssl
BuildRequires:  systemd-rpm-macros
BuildRequires:  java-17-openjdk-devel
BuildRequires:  tzdata
%if 0%{?rhel} == 8
BuildRequires:  python39-devel
BuildRequires:  python39-setuptools
%else
BuildRequires:  python3-devel
BuildRequires:  python3-setuptools
%endif

%description
Dedicated OHLC storage using two-dimensional tiles, computed addressing and
uncompressed fixed-length rows. Packages provide a native library, daemon,
shell, and C, Python and Java interfaces.

%package libs
Summary:        OHLC embedded storage runtime
Requires:       tzdata
%description libs
Native storage engine for direct file access, with no server dependency.

%package client-libs
Summary:        OHLC network client runtime
Requires:       %{name}-libs%{?_isa} = %{version}-%{release}
%description client-libs
Native client library for Unix sockets and TCP with optional credentials and TLS.

%package server
Summary:        OHLC daemon and offline administration tools
Requires:       %{name}-client-libs%{?_isa} = %{version}-%{release}
Requires(pre):  shadow-utils
%{?systemd_requires}
%description server
Storage daemon, systemd unit, configuration and offline checksummed backup,
restore and verification tool. Package removal preserves database files.

%package client
Summary:        OHLC interactive command shell
Requires:       %{name}-client-libs%{?_isa} = %{version}-%{release}
%description client
Interactive and scriptable shell with series, cross, import, stats and help.

%package devel
Summary:        OHLC C headers and development metadata
Requires:       %{name}-libs%{?_isa} = %{version}-%{release}
Requires:       %{name}-client-libs%{?_isa} = %{version}-%{release}
%description devel
C headers, linker symlinks, pkg-config files and CMake package configuration.

%package -n python3-ohlc
Summary:        OHLC Python network and embedded interfaces
BuildArch:      noarch
Requires:       %{name}-client-libs = %{version}-%{release}
%if 0%{?rhel} == 8
Requires:       python39
%else
Requires:       python3 >= 3.9
%endif
%description -n python3-ohlc
Batch-oriented Python bindings using ctypes, with network Connection and
embedded Database APIs. Requires Python 3.9 or later.

%package java
Summary:        OHLC Java network and embedded API classes
BuildArch:      noarch
Requires:       java-17-openjdk-headless
%description java
Java 17 network client and embedded API. Network access needs no native library;
install ohlc-jni additionally to open local database files.

%package jni
Summary:        OHLC native bridge for Java embedded access
Requires:       %{name}-libs%{?_isa} = %{version}-%{release}
Requires:       %{name}-java = %{version}-%{release}
%description jni
JNI bridge for the embedded Java Database API.

%prep
%setup -q -n ohlc-%{upstream_version}

%build
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
cmake -S . -B build-rpm \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_C_FLAGS="%{optflags}" \
    -DCMAKE_EXE_LINKER_FLAGS="%{?build_ldflags}" \
    -DCMAKE_SHARED_LINKER_FLAGS="%{?build_ldflags}" \
    -DCMAKE_INSTALL_PREFIX=%{_prefix} \
    -DCMAKE_INSTALL_LIBDIR=%{_lib} \
    -DCMAKE_SKIP_RPATH=ON \
    -DPython3_EXECUTABLE=%{python} \
    -DJava_JAVA_EXECUTABLE=$JAVA_HOME/bin/java \
    -DJava_JAVAC_EXECUTABLE=$JAVA_HOME/bin/javac \
    -DJava_JAR_EXECUTABLE=$JAVA_HOME/bin/jar
cmake --build build-rpm --parallel %{?_smp_build_ncpus}
cd clients/python
%{python} setup.py build

%install
DESTDIR=%{buildroot} cmake --install build-rpm
install -Dpm0644 packaging/ohlc.service %{buildroot}%{_unitdir}/ohlc.service
install -Dpm0640 packaging/ohlcd.conf %{buildroot}%{_sysconfdir}/ohlc/ohlcd.conf
install -d -m0700 %{buildroot}%{_localstatedir}/lib/ohlc
cd clients/python
%{python} setup.py install --skip-build --root=%{buildroot} --prefix=%{_prefix}

%check
# TMPDIR may select a dedicated test disk; keep Unix socket paths short.
test_root=$(mktemp -d "${TMPDIR:-/tmp}/ohlc-check.XXXXXX")
trap 'rmdir "$test_root" || :' EXIT
export TMPDIR="$test_root"
export LD_LIBRARY_PATH="$PWD/build-rpm"
ctest --test-dir build-rpm --output-on-failure
test -f build-rpm/libohlc_jni.so
test -f build-rpm/ohlc-client.jar
test "$(build-rpm/ohlc --version)" = "ohlc %{upstream_version}"

%pre server
getent group ohlc >/dev/null || groupadd -r ohlc
getent passwd ohlc >/dev/null || \
    useradd -r -g ohlc -d %{_localstatedir}/lib/ohlc -s /sbin/nologin \
        -c 'OHLC storage service' ohlc

%post server
%systemd_post ohlc.service

%preun server
%systemd_preun ohlc.service

%postun server
%systemd_postun_with_restart ohlc.service

%files libs
%license LICENSE
%doc docs/design.md docs/test-report.md docs/images
%{_libdir}/libohlc.so.0*

%files client-libs
%{_libdir}/libohlc_client.so.0*

%files server
%{_bindir}/ohlcd
%{_bindir}/ohlc-admin
%{_unitdir}/ohlc.service
%dir %attr(0750,root,ohlc) %{_sysconfdir}/ohlc
%config(noreplace) %attr(0640,root,ohlc) %{_sysconfdir}/ohlc/ohlcd.conf
%dir %attr(0700,ohlc,ohlc) %{_localstatedir}/lib/ohlc
%dir %{_datadir}/ohlc
%{_datadir}/ohlc/ohlcd.conf

%files client
%{_bindir}/ohlc

%files devel
%{_includedir}/ohlc
%{_libdir}/libohlc.so
%{_libdir}/libohlc_client.so
%{_libdir}/pkgconfig/ohlc*.pc
%{_libdir}/cmake/ohlc

%files -n python3-ohlc
%{python_sitelib}/ohlc
%{python_sitelib}/ohlc_client-*.egg-info

%files java
%license LICENSE
%dir %{_datadir}/ohlc
%{_datadir}/ohlc/ohlc-client.jar

%files jni
%{_libdir}/libohlc_jni.so

%changelog
* Mon Sep 28 2026 OHLC contributors - 0.1.0~beta.1-7
- Complete insert help examples for second, minute, day, month and year bars.

* Mon Sep 28 2026 OHLC contributors - 0.1.0~beta.1-6
- Add second, month and year periods with uint32 time keys.
- Update shell help and C, Python, Java and JNI interfaces for the new periods.

* Mon Sep 28 2026 OHLC contributors - 0.1.0~beta.1-5
- Add inclusive shell series ranges and server-side cross ticker filters.
- Improve help layout and examples, keyboard editing, and history search.

* Mon Sep 28 2026 OHLC contributors - 0.1.0~beta.1-4
- Use per-table ticker dictionaries and atomic automatic ticker registration.
- Update the disk format to 5, network protocol to 4, and C ABI to 2.

* Sun Sep 27 2026 OHLC contributors - 0.1.0~beta.1-1
- Initial beta packaging for RHEL 8 and 9.

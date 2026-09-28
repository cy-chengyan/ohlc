cmake_minimum_required(VERSION 3.20)
project(ohlc VERSION 0.1.0 LANGUAGES C)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
if(APPLE)
    set(CMAKE_INSTALL_RPATH "@loader_path;@loader_path/../${CMAKE_INSTALL_LIBDIR}")
else()
    set(CMAKE_INSTALL_RPATH "$ORIGIN;$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
endif()

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(OHLC_BUILD_TESTS "Build invariant and recovery tests" ON)
option(OHLC_SANITIZE "Enable address and undefined behavior sanitizers" OFF)
option(OHLC_THREAD_SANITIZE "Enable the thread sanitizer (separate from ASan)" OFF)
option(OHLC_WARNINGS_AS_ERRORS "Treat project warnings as errors" ON)
option(OHLC_FORCE_SOFTWARE_CRC "Disable CRC CPU dispatch for portability checks" OFF)
option(OHLC_BUILD_NETWORK "Build TLS client library, server and shell" ON)
option(OHLC_BUILD_JAVA_CLIENT "Build the Java client when JDK 17+ is available" ON)
option(OHLC_BUILD_JAVA_EMBEDDED "Build the JNI bridge with the Java client" ON)

find_package(Threads REQUIRED)
if(OHLC_SANITIZE AND OHLC_THREAD_SANITIZE)
    message(FATAL_ERROR "ASan and TSan require separate builds")
endif()

add_library(ohlc SHARED
    src/format.c
    src/datetime.c
    src/index.c
    src/storage.c
    src/io.c
    src/dictionary.c
    src/engine.c
    src/recovery.c
)
target_include_directories(ohlc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_include_directories(ohlc PRIVATE src)
target_compile_definitions(ohlc PRIVATE _POSIX_C_SOURCE=200809L _FILE_OFFSET_BITS=64)
if(APPLE)
    target_compile_definitions(ohlc PRIVATE _DARWIN_C_SOURCE)
endif()
target_link_libraries(ohlc PRIVATE Threads::Threads)
if(OHLC_FORCE_SOFTWARE_CRC)
    target_compile_definitions(ohlc PRIVATE OHLC_FORCE_SOFTWARE_CRC=1)
endif()
set_target_properties(ohlc PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION 0)

function(ohlc_diagnostics target)
    target_compile_options(${target} PRIVATE
        -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Wstrict-prototypes
        -Wmissing-prototypes -Wformat=2 -Wundef -Wvla
    )
    if(OHLC_WARNINGS_AS_ERRORS)
        target_compile_options(${target} PRIVATE -Werror)
    endif()
    if(OHLC_SANITIZE)
        target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    endif()
    if(OHLC_THREAD_SANITIZE)
        target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=thread)
    endif()
endfunction()
ohlc_diagnostics(ohlc)

if(OHLC_BUILD_NETWORK)
    find_package(OpenSSL 1.1.1 REQUIRED)
    add_library(ohlc_client SHARED src/net.c src/client.c)
    target_include_directories(ohlc_client PRIVATE src)
    target_link_libraries(ohlc_client PUBLIC ohlc PRIVATE OpenSSL::SSL Threads::Threads)
    target_compile_definitions(ohlc_client PRIVATE _POSIX_C_SOURCE=200809L)
    set_target_properties(ohlc_client PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION 0)
    set_target_properties(ohlc_client PROPERTIES EXPORT_NAME client)
    ohlc_diagnostics(ohlc_client)

    add_executable(ohlcd src/server.c src/server_config.c)
    target_include_directories(ohlcd PRIVATE src)
    target_compile_definitions(ohlcd PRIVATE _POSIX_C_SOURCE=200809L)
    target_link_libraries(ohlcd PRIVATE ohlc_client OpenSSL::SSL Threads::Threads)
    ohlc_diagnostics(ohlcd)
    add_executable(ohlc_admin src/admin.c)
    set_target_properties(ohlc_admin PROPERTIES OUTPUT_NAME ohlc-admin)
    target_include_directories(ohlc_admin PRIVATE src)
    target_compile_definitions(ohlc_admin PRIVATE _POSIX_C_SOURCE=200809L _FILE_OFFSET_BITS=64)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_compile_definitions(ohlc_admin PRIVATE _GNU_SOURCE)
    elseif(APPLE)
        target_compile_definitions(ohlc_admin PRIVATE _DARWIN_C_SOURCE)
    endif()
    target_link_libraries(ohlc_admin PRIVATE ohlc OpenSSL::Crypto Threads::Threads)
    ohlc_diagnostics(ohlc_admin)
    add_executable(ohlc_shell src/shell.c src/shell_io.c)
    set_target_properties(ohlc_shell PROPERTIES OUTPUT_NAME ohlc)
    target_include_directories(ohlc_shell PRIVATE src)
    target_compile_definitions(ohlc_shell PRIVATE _POSIX_C_SOURCE=200809L)
    target_link_libraries(ohlc_shell PRIVATE ohlc_client OpenSSL::SSL Threads::Threads)
    ohlc_diagnostics(ohlc_shell)
    add_executable(ohlc_workload tests/workload.c)
    target_include_directories(ohlc_workload PRIVATE src)
    target_compile_definitions(ohlc_workload PRIVATE _POSIX_C_SOURCE=200809L)
    target_link_libraries(ohlc_workload PRIVATE ohlc_client Threads::Threads)
    ohlc_diagnostics(ohlc_workload)
    if(APPLE)
        target_compile_definitions(ohlc_client PRIVATE _DARWIN_C_SOURCE)
        target_compile_definitions(ohlcd PRIVATE _DARWIN_C_SOURCE)
        target_compile_definitions(ohlc_shell PRIVATE _DARWIN_C_SOURCE)
    endif()
endif()

add_executable(ohlc_example examples/embedded.c)
target_link_libraries(ohlc_example PRIVATE ohlc)
ohlc_diagnostics(ohlc_example)

add_executable(ohlc_benchmark tests/benchmark.c)
target_compile_definitions(ohlc_benchmark PRIVATE _POSIX_C_SOURCE=200809L)
target_link_libraries(ohlc_benchmark PRIVATE ohlc)
ohlc_diagnostics(ohlc_benchmark)

install(TARGETS ohlc EXPORT ohlcTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
configure_file(cmake/ohlc.pc.in ohlc.pc @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/ohlc.pc"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
if(OHLC_BUILD_NETWORK)
    install(FILES examples/ohlcd.conf DESTINATION ${CMAKE_INSTALL_DATADIR}/ohlc)
    install(TARGETS ohlc_client EXPORT ohlcTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    install(TARGETS ohlcd ohlc_shell ohlc_admin
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
    configure_file(cmake/ohlc-client.pc.in ohlc-client.pc @ONLY)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/ohlc-client.pc"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
endif()
configure_package_config_file(cmake/ohlcConfig.cmake.in ohlcConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ohlc)
write_basic_package_version_file(ohlcConfigVersion.cmake VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMinorVersion)
install(EXPORT ohlcTargets NAMESPACE ohlc:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ohlc)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/ohlcConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/ohlcConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ohlc)

if(OHLC_BUILD_JAVA_CLIENT)
    find_package(Java 17 QUIET COMPONENTS Development)
    if(Java_FOUND)
        set(OHLC_JAVA_CLASSES "${CMAKE_CURRENT_BINARY_DIR}/java")
        set(OHLC_JAVA_JAR "${CMAKE_CURRENT_BINARY_DIR}/ohlc-client.jar")
        set(OHLC_JNI_HEADERS "${CMAKE_CURRENT_BINARY_DIR}/jni")
        set(OHLC_JAVA_SOURCES
            "${CMAKE_CURRENT_SOURCE_DIR}/clients/java/io/ohlc/Ohlc.java"
            "${CMAKE_CURRENT_SOURCE_DIR}/clients/java/io/ohlc/Database.java")
        if(OHLC_BUILD_TESTS)
            list(APPEND OHLC_JAVA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/tests/JavaClientTest.java")
            list(APPEND OHLC_JAVA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/tests/JavaTransportTest.java")
            list(APPEND OHLC_JAVA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/tests/JavaEmbeddedTest.java")
        endif()
        add_custom_command(OUTPUT "${OHLC_JAVA_JAR}"
            BYPRODUCTS "${OHLC_JNI_HEADERS}/io_ohlc_Database_Native.h"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${OHLC_JAVA_CLASSES}"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${OHLC_JNI_HEADERS}"
            COMMAND "${Java_JAVAC_EXECUTABLE}" --release 17 -Xlint:all -Werror
                -h "${OHLC_JNI_HEADERS}" -d "${OHLC_JAVA_CLASSES}" ${OHLC_JAVA_SOURCES}
            COMMAND "${Java_JAR_EXECUTABLE}" --create --file "${OHLC_JAVA_JAR}"
                -C "${OHLC_JAVA_CLASSES}" io -C "${CMAKE_CURRENT_SOURCE_DIR}" LICENSE
            DEPENDS ${OHLC_JAVA_SOURCES} "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
            VERBATIM)
        add_custom_target(ohlc_java ALL DEPENDS "${OHLC_JAVA_JAR}")
        install(FILES "${OHLC_JAVA_JAR}" DESTINATION ${CMAKE_INSTALL_DATADIR}/ohlc)
        if(OHLC_BUILD_JAVA_EMBEDDED)
            # Only headers are needed: the JVM supplies the JNI function table.
            # Derive the include root from javac to support headless JDKs too.
            get_filename_component(OHLC_JAVAC_REAL "${Java_JAVAC_EXECUTABLE}" REALPATH)
            get_filename_component(OHLC_JAVA_BIN "${OHLC_JAVAC_REAL}" DIRECTORY)
            get_filename_component(OHLC_JAVA_HOME "${OHLC_JAVA_BIN}" DIRECTORY)
            find_path(OHLC_JNI_INCLUDE jni.h HINTS "${OHLC_JAVA_HOME}/include" REQUIRED)
            find_path(OHLC_JNI_PLATFORM_INCLUDE jni_md.h
                HINTS "${OHLC_JNI_INCLUDE}" PATH_SUFFIXES linux darwin REQUIRED)
            add_library(ohlc_jni SHARED src/java.c)
            add_dependencies(ohlc_jni ohlc_java)
            target_include_directories(ohlc_jni PRIVATE "${OHLC_JNI_HEADERS}")
            target_include_directories(ohlc_jni SYSTEM PRIVATE
                "${OHLC_JNI_INCLUDE}" "${OHLC_JNI_PLATFORM_INCLUDE}")
            target_link_libraries(ohlc_jni PRIVATE ohlc)
            ohlc_diagnostics(ohlc_jni)
            install(TARGETS ohlc_jni LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
        endif()
    else()
        message(STATUS "JDK 17+ not found; Java client and JNI bridge are skipped")
    endif()
endif()

if(OHLC_BUILD_TESTS)
    enable_testing()
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        add_library(ohlc_fault_inject MODULE tests/fault_inject.c)
        set_target_properties(ohlc_fault_inject PROPERTIES PREFIX "" OUTPUT_NAME "fault_inject")
        ohlc_diagnostics(ohlc_fault_inject)
    endif()
    add_executable(ohlc_core_test tests/core_test.c)
    target_compile_definitions(ohlc_core_test PRIVATE _POSIX_C_SOURCE=200809L)
    if(APPLE)
        target_compile_definitions(ohlc_core_test PRIVATE _DARWIN_C_SOURCE)
    endif()
    target_include_directories(ohlc_core_test PRIVATE src)
    target_link_libraries(ohlc_core_test PRIVATE ohlc Threads::Threads)
    ohlc_diagnostics(ohlc_core_test)
    add_test(NAME core COMMAND ohlc_core_test)
    add_executable(ohlc_concurrency_test tests/concurrency_test.c)
    target_compile_definitions(ohlc_concurrency_test PRIVATE _POSIX_C_SOURCE=200809L)
    if(APPLE)
        target_compile_definitions(ohlc_concurrency_test PRIVATE _DARWIN_C_SOURCE)
    endif()
    target_link_libraries(ohlc_concurrency_test PRIVATE ohlc Threads::Threads)
    ohlc_diagnostics(ohlc_concurrency_test)
    add_test(NAME concurrency COMMAND ohlc_concurrency_test)
    add_executable(ohlc_pipeline_test tests/pipeline_test.c)
    target_compile_definitions(ohlc_pipeline_test PRIVATE _POSIX_C_SOURCE=200809L)
    if(APPLE)
        target_compile_definitions(ohlc_pipeline_test PRIVATE _DARWIN_C_SOURCE)
    endif()
    target_include_directories(ohlc_pipeline_test PRIVATE src)
    target_link_libraries(ohlc_pipeline_test PRIVATE ohlc Threads::Threads)
    ohlc_diagnostics(ohlc_pipeline_test)
    add_test(NAME pipeline COMMAND ohlc_pipeline_test)
    set_tests_properties(core concurrency pipeline PROPERTIES TIMEOUT 60)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT OHLC_SANITIZE AND NOT OHLC_THREAD_SANITIZE)
        add_test(NAME commit_sync_failure COMMAND ${CMAKE_COMMAND} -E env
            "LD_PRELOAD=$<TARGET_FILE:ohlc_fault_inject>" OHLC_TEST_SYNC_FAILURES=1
            $<TARGET_FILE:ohlc_pipeline_test>)
        set_tests_properties(commit_sync_failure PROPERTIES TIMEOUT 60)
        add_test(NAME wal_reclaim_crash COMMAND ${CMAKE_COMMAND} -E env
            "LD_PRELOAD=$<TARGET_FILE:ohlc_fault_inject>" OHLC_TEST_RECLAIM_FAILURES=1
            $<TARGET_FILE:ohlc_core_test>)
        set_tests_properties(wal_reclaim_crash PROPERTIES TIMEOUT 60)
    endif()
    if(NOT OHLC_SANITIZE AND NOT OHLC_THREAD_SANITIZE)
        find_package(Python3 3.9 QUIET COMPONENTS Interpreter)
        if(Python3_FOUND)
            set(OHLC_EMBEDDED_JAVA_ARGS)
            if(TARGET ohlc_jni)
                list(APPEND OHLC_EMBEDDED_JAVA_ARGS $<TARGET_FILE:ohlc_jni>
                    "${OHLC_JAVA_CLASSES}" "${Java_JAVA_EXECUTABLE}")
            endif()
            add_test(NAME embedded COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/embedded_test.py"
                $<TARGET_FILE:ohlc> ${OHLC_EMBEDDED_JAVA_ARGS})
            set_tests_properties(embedded PROPERTIES TIMEOUT 90)
        endif()
    endif()
    if(OHLC_BUILD_NETWORK AND NOT OHLC_SANITIZE AND NOT OHLC_THREAD_SANITIZE)
        if(Python3_FOUND)
            set(OHLC_JAVA_TEST_ARGS)
            if(TARGET ohlc_java)
                list(APPEND OHLC_JAVA_TEST_ARGS "${OHLC_JAVA_CLASSES}" "${Java_JAVA_EXECUTABLE}")
            endif()
            add_test(NAME network COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/network_test.py"
                $<TARGET_FILE:ohlcd> $<TARGET_FILE:ohlc_client> ${OHLC_JAVA_TEST_ARGS})
            add_test(NAME transport COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/transport_test.py"
                $<TARGET_FILE:ohlcd> $<TARGET_FILE:ohlc_client> ${OHLC_JAVA_TEST_ARGS})
            add_test(NAME config COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/config_test.py"
                $<TARGET_FILE:ohlcd> $<TARGET_FILE:ohlc_client>)
            add_test(NAME admin COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/admin_test.py"
                $<TARGET_FILE:ohlc_admin> $<TARGET_FILE:ohlc>)
            set_tests_properties(admin PROPERTIES TIMEOUT 120)
            set_tests_properties(network transport config PROPERTIES TIMEOUT 120)
        endif()
    endif()
endif()

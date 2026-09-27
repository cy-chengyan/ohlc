/* SPDX-License-Identifier: Apache-2.0 */
#define _GNU_SOURCE
#include <errno.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

/* Linux-only test shim. It is never linked into a product binary. */
static _Atomic bool current_synced;

static bool fail_sync(int fd) {
    const char* selected = getenv("OHLC_TEST_FAIL_SYNC_FD");
    if (selected == NULL) {
        return false;
    }
    char descriptor[32];
    int length = snprintf(descriptor, sizeof(descriptor), "%d", fd);
    if (length > 0 && (size_t)length < sizeof(descriptor) && strcmp(selected, descriptor) == 0) {
        errno = EIO;
        return true;
    }
    return false;
}

static void crash_after_sync(int fd) {
    const char* arm = getenv("OHLC_TEST_CRASH_ARM");
    const char* phase = getenv("OHLC_TEST_CRASH_ON");
    const char* database = getenv("OHLC_TEST_DATABASE");
    if (arm == NULL || phase == NULL || database == NULL || access(arm, F_OK) != 0) {
        return;
    }
    char descriptor[64];
    int length = snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd);
    if (length <= 0 || (size_t)length >= sizeof(descriptor)) {
        return;
    }
    char path[4096];
    ssize_t count = readlink(descriptor, path, sizeof(path) - 1);
    if (count <= 0) {
        return;
    }
    path[(size_t)count] = '\0';
    size_t prefix = strlen(database);
    if (strncmp(path, database, prefix) != 0 || (path[prefix] != '/' && path[prefix] != '\0')) {
        return;
    }
    struct stat info;
    if (fstat(fd, &info) != 0) {
        return;
    }
    const char* name = strrchr(path, '/');
    name = name != NULL ? name + 1 : path;
    const char* kind = "other";
    if (strncmp(name, "CURRENT.", 8) == 0) {
        atomic_store(&current_synced, true);
        kind = "current";
    } else if (S_ISDIR(info.st_mode)) {
        if (strcmp(path, database) == 0 && atomic_load(&current_synced)) {
            kind = "directory";
        }
    } else if (info.st_size > 4096) {
        if (strncmp(name, "wal-", 4) == 0) {
            kind = "wal";
        } else if (strncmp(name, "data-", 5) == 0) {
            kind = strstr(name, ".meta") != NULL ? "meta" : "data";
        } else if (strncmp(name, "index-", 6) == 0) {
            kind = "index";
        } else if (strncmp(name, "catalog-", 8) == 0) {
            kind = "catalog";
        }
    }
    if (strcmp(kind, phase) == 0) {
        dprintf(STDERR_FILENO, "crash_boundary=%s file=%s bytes=%lld\n", kind, path,
                (long long)info.st_size);
        _exit(86);
    }
}

int fdatasync(int fd) {
    if (fail_sync(fd)) {
        return -1;
    }
    int result = (int)syscall(SYS_fdatasync, fd);
    if (result == 0) {
        crash_after_sync(fd);
    }
    return result;
}

int fsync(int fd) {
    if (fail_sync(fd)) {
        return -1;
    }
    int result = (int)syscall(SYS_fsync, fd);
    if (result == 0) {
        crash_after_sync(fd);
    }
    return result;
}

int unlinkat(int directory_fd, const char* path, int flags) {
    int result = (int)syscall(SYS_unlinkat, directory_fd, path, flags);
    const char* database = getenv("OHLC_TEST_RECLAIM_CRASH");
    if (result == 0 && database != NULL && strncmp(path, "wal-", 4) == 0) {
        char descriptor[64];
        int length = snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", directory_fd);
        if (length > 0 && (size_t)length < sizeof(descriptor)) {
            char directory[4096];
            ssize_t count = readlink(descriptor, directory, sizeof(directory) - 1);
            if (count > 0) {
                directory[(size_t)count] = '\0';
                if (strcmp(directory, database) == 0) {
                    dprintf(STDERR_FILENO, "crash_boundary=wal_reclaim file=%s\n", path);
                    _exit(86);
                }
            }
        }
    }
    return result;
}

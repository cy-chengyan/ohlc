/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <openssl/evp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define COPY_BYTES (1024u * 1024u)
#define PATH_BYTES 128u

typedef struct {
    int fd;
    EVP_MD_CTX* digest;
    uint8_t* buffer;
    bool writing;
} archive_stream;

static bool transfer(int fd, void* buffer, size_t size, bool writing) {
    uint8_t* bytes = buffer;
    while (size != 0) {
        ssize_t count = writing ? write(fd, bytes, size) : read(fd, bytes, size);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return false;
        }
        bytes += (size_t)count;
        size -= (size_t)count;
    }
    return true;
}

static bool stream_bytes(archive_stream* stream, void* bytes, size_t size) {
    return transfer(stream->fd, bytes, size, stream->writing) &&
           EVP_DigestUpdate(stream->digest, bytes, size) == 1;
}

static bool sync_directory(const char* path) {
    char* parent = strdup(path);
    if (parent == NULL) {
        return false;
    }
    char* slash = strrchr(parent, '/');
    if (slash == NULL) {
        strcpy(parent, ".");
    } else if (slash == parent) {
        slash[1] = '\0';
    } else {
        *slash = '\0';
    }
    int fd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    free(parent);
    if (fd < 0) {
        return false;
    }
    bool ok = fsync(fd) == 0;
    close(fd);
    return ok;
}

/* Both supported platforms provide atomic publication without replacing even
 * an empty destination directory. A successful rename is never rolled back. */
static bool publish(const char* temporary, const char* destination) {
#if defined(__linux__)
    int result = renameat2(AT_FDCWD, temporary, AT_FDCWD, destination, RENAME_NOREPLACE);
#elif defined(__APPLE__)
    int result = renamex_np(temporary, destination, RENAME_EXCL);
#else
    int result = -1;
    errno = ENOTSUP;
#endif
    if (result != 0) {
        return false;
    }
    if (!sync_directory(destination)) {
        fprintf(stderr, "Published %s, but parent directory sync failed\n", destination);
        return false;
    }
    return true;
}

static bool numbered(const char* name, const char* prefix, const char* suffix) {
    size_t length = strlen(prefix);
    if (strncmp(name, prefix, length) != 0) {
        return false;
    }
    const char* digits = name + length;
    const char* end = digits;
    uint64_t number = 0;
    while (*end >= '0' && *end <= '9') {
        if (end - digits >= 10) {
            return false;
        }
        number = number * 10 + (unsigned)(*end++ - '0');
    }
    if (number == 0 || number > UINT32_MAX || strcmp(end, suffix) != 0) {
        return false;
    }
    char canonical[64];
    int count = snprintf(canonical, sizeof(canonical), "%s%06" PRIu32 "%s", prefix,
                         (uint32_t)number, suffix);
    return count > 0 && (size_t)count < sizeof(canonical) && strcmp(name, canonical) == 0;
}

/* Archive paths are a closed grammar, not general filesystem paths. This
 * rejects traversal, alternate separators, symlinks and future file layouts. */
static bool file_path(const char* path) {
    if (strcmp(path, "CURRENT.0") == 0 || strcmp(path, "CURRENT.1") == 0 ||
        strcmp(path, "catalog-000001.dat") == 0 || numbered(path, "wal-", ".log")) {
        return true;
    }
    if (strlen(path) < 17 || strncmp(path, "tables/", 7) != 0 || path[15] != '/') {
        return false;
    }
    bool nonzero = false;
    for (size_t i = 7; i < 15; i++) {
        if (!((path[i] >= '0' && path[i] <= '9') || (path[i] >= 'a' && path[i] <= 'f'))) {
            return false;
        }
        nonzero = nonzero || path[i] != '0';
    }
    return nonzero &&
           (strcmp(path + 16, "index-000001.dat") == 0 || numbered(path + 16, "data-", ".dat") ||
            numbered(path + 16, "data-", ".meta"));
}

static bool copy_file(archive_stream* stream, int fd, uint64_t size) {
    while (size != 0) {
        size_t count = size < COPY_BYTES ? (size_t)size : COPY_BYTES;
        if (stream->writing) {
            if (!transfer(fd, stream->buffer, count, false) ||
                !stream_bytes(stream, stream->buffer, count)) {
                return false;
            }
        } else if (!stream_bytes(stream, stream->buffer, count) ||
                   !transfer(fd, stream->buffer, count, true)) {
            return false;
        }
        size -= count;
    }
    return stream->writing || fsync(fd) == 0;
}

static bool archive_directory(archive_stream* stream, int fd, const char* prefix, unsigned depth) {
    int duplicate = openat(fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    DIR* directory = duplicate < 0 ? NULL : fdopendir(duplicate);
    if (directory == NULL) {
        if (duplicate >= 0) {
            close(duplicate);
        }
        return false;
    }
    bool ok = true;
    for (;;) {
        errno = 0;
        struct dirent* entry = readdir(directory);
        if (entry == NULL) {
            ok = errno == 0;
            break;
        }
        const char* name = entry->d_name;
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 ||
            (depth == 0 && strcmp(name, "LOCK") == 0)) {
            continue;
        }
        char path[PATH_BYTES];
        int length = snprintf(path, sizeof(path), "%s%s", prefix, name);
        struct stat info;
        if (length <= 0 || (size_t)length >= sizeof(path) ||
            fstatat(fd, name, &info, AT_SYMLINK_NOFOLLOW) != 0) {
            ok = false;
            break;
        }
        if (S_ISDIR(info.st_mode) && depth < 2) {
            char next[PATH_BYTES];
            length = snprintf(next, sizeof(next), "%s/", path);
            int child = openat(fd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
            ok = child >= 0 && length > 0 && (size_t)length < sizeof(next) &&
                 archive_directory(stream, child, next, depth + 1);
            if (child >= 0) {
                close(child);
            }
        } else if (S_ISREG(info.st_mode) && info.st_size >= 0 && file_path(path)) {
            int input = openat(fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
            uint8_t header[12];
            ohlc_put_u32(header, (uint32_t)strlen(path));
            ohlc_put_u64(header + 4, (uint64_t)info.st_size);
            ok = input >= 0 && stream_bytes(stream, header, sizeof(header)) &&
                 stream_bytes(stream, path, strlen(path)) &&
                 copy_file(stream, input, (uint64_t)info.st_size);
            if (input >= 0) {
                close(input);
            }
        } else {
            fprintf(stderr, "Unsupported database entry: %s\n", path);
            ok = false;
        }
        if (!ok) {
            break;
        }
    }
    closedir(directory);
    return ok;
}

static bool finish_archive(archive_stream* stream) {
    uint8_t checksum[32];
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(stream->digest, checksum, &size) != 1 || size != sizeof(checksum)) {
        return false;
    }
    if (stream->writing) {
        return transfer(stream->fd, checksum, sizeof(checksum), true) && fsync(stream->fd) == 0;
    }
    uint8_t expected[32];
    if (!transfer(stream->fd, expected, sizeof(expected), false) ||
        memcmp(expected, checksum, sizeof(expected)) != 0) {
        fputs("Archive checksum mismatch or truncation\n", stderr);
        return false;
    }
    uint8_t extra;
    ssize_t count;
    do {
        count = read(stream->fd, &extra, 1);
    } while (count < 0 && errno == EINTR);
    return count == 0;
}

static bool stream_init(archive_stream* stream, int fd, bool writing) {
    *stream = (archive_stream){.fd = fd, .writing = writing};
    stream->buffer = malloc(COPY_BYTES);
    stream->digest = EVP_MD_CTX_new();
    return stream->buffer != NULL && stream->digest != NULL &&
           EVP_DigestInit_ex(stream->digest, EVP_sha256(), NULL) == 1;
}

static void stream_close(archive_stream* stream) {
    EVP_MD_CTX_free(stream->digest);
    free(stream->buffer);
    if (stream->fd >= 0) {
        close(stream->fd);
    }
}

static bool backup(ohlc_db* db, const char* destination) {
    size_t capacity = strlen(destination) + 32;
    char* temporary = malloc(capacity);
    if (temporary == NULL) {
        return false;
    }
    snprintf(temporary, capacity, "%s.partial.XXXXXX", destination);
    int fd = mkstemp(temporary);
    if (fd < 0) {
        free(temporary);
        return false;
    }
    archive_stream stream;
    bool ok = stream_init(&stream, fd, true);
    uint8_t header[36] = "OHLCBAK1";
    ohlc_put_u32(header + 8, OHLC_FORMAT_VERSION);
    ohlc_uuid(db, header + 12);
    ohlc_stats stats;
    ohlc_get_stats(db, &stats);
    ohlc_put_u64(header + 28, stats.commit_seq);
    uint8_t end[12] = {0};
    ok = ok && stream_bytes(&stream, header, sizeof(header)) &&
         archive_directory(&stream, db->directory_fd, "", 0) &&
         stream_bytes(&stream, end, sizeof(end)) && finish_archive(&stream);
    stream_close(&stream);
    if (ok) {
        ok = publish(temporary, destination);
    }
    if (!ok) {
        unlink(temporary);
    }
    free(temporary);
    return ok;
}

static int restore_file(int root, const char* path) {
    int directory = root;
    int tables = -1;
    if (strncmp(path, "tables/", 7) == 0) {
        if (mkdirat(root, "tables", 0700) != 0 && errno != EEXIST) {
            return -1;
        }
        tables = openat(root, "tables", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (tables < 0) {
            return -1;
        }
        char name[9];
        memcpy(name, path + 7, 8);
        name[8] = '\0';
        if (mkdirat(tables, name, 0700) != 0 && errno != EEXIST) {
            close(tables);
            return -1;
        }
        directory = openat(tables, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (directory < 0) {
            close(tables);
            return -1;
        }
    }
    const char* name = tables >= 0 ? path + 16 : path;
    int fd = openat(directory, name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    bool synced = fsync(directory) == 0;
    if (tables >= 0) {
        synced = fsync(tables) == 0 && synced;
        close(directory);
        close(tables);
    }
    if (!synced && fd >= 0) {
        close(fd);
        fd = -1;
    }
    return fd;
}

static bool restore_entries(archive_stream* stream, int directory) {
    for (;;) {
        uint8_t entry[12];
        if (!stream_bytes(stream, entry, sizeof(entry))) {
            return false;
        }
        uint32_t length = ohlc_get_u32(entry);
        uint64_t size = ohlc_get_u64(entry + 4);
        if (length == 0) {
            return size == 0 && finish_archive(stream) && fsync(directory) == 0;
        }
        char path[PATH_BYTES];
        if (length >= sizeof(path) || size > INT64_MAX || !stream_bytes(stream, path, length) ||
            memchr(path, 0, length) != NULL) {
            return false;
        }
        path[length] = '\0';
        if (!file_path(path)) {
            fputs("Invalid archive path\n", stderr);
            return false;
        }
        int fd = restore_file(directory, path);
        if (fd < 0) {
            return false;
        }
        bool ok = copy_file(stream, fd, size);
        close(fd);
        if (!ok) {
            return false;
        }
    }
}

static bool restore(const char* source, const char* destination, const ohlc_options* options) {
    int input = open(source, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (input < 0) {
        return false;
    }
    archive_stream stream;
    bool ok = stream_init(&stream, input, false);
    uint8_t header[36];
    struct stat info;
    ok = ok && fstat(input, &info) == 0 && S_ISREG(info.st_mode) &&
         stream_bytes(&stream, header, sizeof(header)) && memcmp(header, "OHLCBAK1", 8) == 0 &&
         ohlc_get_u32(header + 8) == OHLC_FORMAT_VERSION;
    size_t capacity = strlen(destination) + 32;
    char* temporary = malloc(capacity);
    if (!ok || temporary == NULL) {
        free(temporary);
        stream_close(&stream);
        return false;
    }
    snprintf(temporary, capacity, "%s.restore.XXXXXX", destination);
    if (mkdtemp(temporary) == NULL) {
        free(temporary);
        stream_close(&stream);
        return false;
    }
    int directory = open(temporary, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    ok = directory >= 0 && restore_entries(&stream, directory);
    if (directory >= 0) {
        close(directory);
    }
    stream_close(&stream);
    if (ok) {
        ohlc_db* db = NULL;
        ohlc_status status = ohlc_open(temporary, options, &db);
        ok = status == OHLC_OK;
        if (ok) {
            uint8_t uuid[16];
            ohlc_uuid(db, uuid);
            ohlc_stats stats;
            ohlc_get_stats(db, &stats);
            ok = memcmp(uuid, header + 12, sizeof(uuid)) == 0 &&
                 stats.commit_seq == ohlc_get_u64(header + 28);
            ohlc_status closed = ohlc_close(db);
            ok = closed == OHLC_OK && ok;
        }
    }
    if (ok) {
        ok = publish(temporary, destination);
    }
    if (!ok) {
        fprintf(stderr, "Restore failed; inspect staging path %s and destination %s\n", temporary,
                destination);
    }
    free(temporary);
    return ok;
}

static ohlc_status check_database(ohlc_db* db) {
    ohlc_stats stats;
    ohlc_get_stats(db, &stats);
    uint8_t buffer[256 * OHLC_RESULT_BYTES];
    uint64_t rows = 0;
    ohlc_table_info tables[16];
    for (uint64_t start = 1; start <= UINT32_MAX;) {
        size_t table_count = 0;
        uint64_t sequence;
        ohlc_status status =
            ohlc_table_list(db, (uint32_t)start, tables, 16, &table_count, &sequence);
        if (status != OHLC_OK) {
            return status;
        }
        if (table_count == 0) {
            break;
        }
        for (size_t table = 0; table < table_count; table++) {
            for (uint64_t ticker = 0; ticker < stats.ticker_count; ticker++) {
                ohlc_cursor* cursor = NULL;
                status = ohlc_series(db, tables[table].id, (uint32_t)ticker, 0,
                                     UINT64_C(4294967296), &cursor);
                if (status == OHLC_NOT_FOUND) {
                    continue;
                }
                size_t count = 1;
                while (status == OHLC_OK && count != 0) {
                    status = ohlc_cursor_next(cursor, buffer, 256, &count);
                    if (status == OHLC_OK) {
                        rows += count;
                    }
                }
                ohlc_cursor_close(cursor);
                if (status != OHLC_OK) {
                    return status;
                }
            }
        }
        start = (uint64_t)tables[table_count - 1].id + 1;
    }
    printf("verified_rows=%" PRIu64 " tables=%" PRIu32 " tickers=%" PRIu64 " commit_seq=%" PRIu64
           "\n",
           rows, stats.table_count, stats.ticker_count, stats.commit_seq);
    return OHLC_OK;
}

static bool positive_number(const char* value, uint64_t maximum, uint64_t* output) {
    if (*value < '0' || *value > '9') {
        return false;
    }
    errno = 0;
    char* end = NULL;
    unsigned long long number = strtoull(value, &end, 10);
    if (errno != 0 || *end != '\0' || number == 0 || number > maximum) {
        return false;
    }
    *output = (uint64_t)number;
    return true;
}

int main(int argc, char** argv) {
    if (argc == 2 && strcmp(argv[1], "--version") == 0) {
        printf("ohlc-admin %s\n", ohlc_version());
        return 0;
    }
    if (argc == 2 && strcmp(argv[1], "--help") == 0) {
        puts("Usage: ohlc-admin [--memory-mib N] [--max-tables N] COMMAND\n"
             "  check DATABASE              Read every visible row and verify block checksums\n"
             "  backup DATABASE ARCHIVE     Checkpoint and create a checksummed full archive\n"
             "  restore ARCHIVE NEW_DB      Verify and restore without replacing any path\n"
             "Stop the database owner first. Backups are uncompressed and require free space.\n"
             "check and backup may perform normal crash recovery. No repair is attempted.\n"
             "Restore leaves a named staging directory on failure for inspection.\n"
             "Defaults: memory-mib=1024, max-tables=1024. --version prints the release.");
        return 0;
    }
    ohlc_options options;
    ohlc_options_init(&options);
    options.cache_bytes = 0;
    options.max_query_ms = UINT32_MAX;
    int position = 1;
    while (position + 1 < argc && strncmp(argv[position], "--", 2) == 0) {
        bool memory = strcmp(argv[position], "--memory-mib") == 0;
        bool tables = strcmp(argv[position], "--max-tables") == 0;
        uint64_t value;
        if ((!memory && !tables) ||
            !positive_number(argv[position + 1], memory ? SIZE_MAX / 1048576u : 1048576u, &value)) {
            fputs("Invalid limit; use --help\n", stderr);
            return 2;
        }
        if (memory) {
            options.memory_limit = (size_t)value * 1048576u;
        } else {
            options.max_tables = (uint32_t)value;
        }
        position += 2;
    }
    int remaining = argc - position;
    if (remaining == 3 && strcmp(argv[position], "restore") == 0) {
        if (!restore(argv[position + 1], argv[position + 2], &options)) {
            fputs("Restore failed: invalid archive, destination, resource limit or I/O error\n",
                  stderr);
            return 1;
        }
        return 0;
    }
    bool checking = remaining == 2 && strcmp(argv[position], "check") == 0;
    bool copying = remaining == 3 && strcmp(argv[position], "backup") == 0;
    if (!checking && !copying) {
        fputs("Invalid arguments; use --help\n", stderr);
        return 2;
    }
    ohlc_db* db = NULL;
    ohlc_status status = ohlc_open(argv[position + 1], &options, &db);
    if (status == OHLC_OK) {
        status = checking ? check_database(db) : ohlc_checkpoint(db);
        if (copying && status == OHLC_OK && !backup(db, argv[position + 2])) {
            status = OHLC_IO;
        }
        ohlc_status closed = ohlc_close(db);
        if (status == OHLC_OK) {
            status = closed;
        }
    }
    if (status != OHLC_OK) {
        fprintf(stderr, "%s failed: %s\n", argv[position], ohlc_status_string(status));
        return 1;
    }
    return 0;
}

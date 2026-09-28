/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHECK(condition)                                                                           \
    do {                                                                                           \
        if (!(condition)) {                                                                        \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);                        \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)

#define OK(expression)                                                                             \
    do {                                                                                           \
        ohlc_status test_status = (expression);                                                    \
        if (test_status != OHLC_OK) {                                                              \
            fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, #expression,                    \
                    ohlc_status_string(test_status));                                              \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)

static void remove_directory(const char* path) {
    DIR* directory = opendir(path);
    CHECK(directory != NULL);
    struct dirent* entry;
    while ((entry = readdir(directory)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char child[1024];
        CHECK(snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) < (int)sizeof(child));
        struct stat information;
        CHECK(lstat(child, &information) == 0);
        if (S_ISDIR(information.st_mode)) {
            remove_directory(child);
        } else {
            CHECK(unlink(child) == 0);
        }
    }
    closedir(directory);
    CHECK(rmdir(path) == 0);
}

static ohlc_row value_for(uint32_t stock, uint32_t key) {
    ohlc_row row = {0};
    row.open = INT32_MIN + (int32_t)stock;
    row.high = INT32_MAX - (int32_t)stock;
    row.low = -(int32_t)key;
    row.close = (int32_t)key;
    row.volume = UINT32_MAX - key;
    row.amount = UINT64_MAX - (uint64_t)stock * 65537u - key;
    row.adjust_factor = key ^ stock;
    return row;
}

static void check_value(const uint8_t* encoded, uint32_t stock, uint32_t key) {
    ohlc_row row = value_for(stock, key);
    uint8_t expected[OHLC_ROW_BYTES];
    ohlc_row_encode(expected, &row);
    CHECK(memcmp(encoded, expected, sizeof(expected)) == 0);
}

static void test_format(void) {
    CHECK(ohlc_crc32c(0, "123456789", 9) == UINT32_C(0xe3069283));
    CHECK(ohlc_crc32c(ohlc_crc32c(0, "1234", 4), "56789", 5) == UINT32_C(0xe3069283));
    uint8_t arbitrary[8200];
    for (size_t i = 0; i < sizeof(arbitrary); i++) {
        arbitrary[i] = (uint8_t)(i * 73u);
    }
    for (size_t length = 0; length < 8192; length += 37) {
        size_t alignment = length % 8u;
        uint32_t seed = (uint32_t)length * 12345u;
        uint32_t crc = ~seed;
        for (size_t i = 0; i < length; i++) {
            crc ^= arbitrary[alignment + i];
            for (unsigned int bit = 0; bit < 8; bit++) {
                crc = (crc >> 1) ^ ((crc & 1u) != 0 ? UINT32_C(0x82f63b78) : 0u);
            }
        }
        CHECK(ohlc_crc32c(seed, arbitrary + alignment, length) == ~crc);
    }
    ohlc_row row = value_for(0, 17);
    uint8_t bytes[33];
    ohlc_row_encode(bytes + 1, &row);
    CHECK(ohlc_get_u32(bytes + 1) == UINT32_C(0x80000000));
    CHECK(ohlc_get_u64(bytes + 21) == UINT64_MAX - 17u);
    ohlc_row decoded;
    ohlc_row_decode(bytes + 1, &decoded);
    CHECK(decoded.open == row.open && decoded.high == row.high && decoded.low == row.low &&
          decoded.close == row.close && decoded.volume == row.volume &&
          decoded.amount == row.amount && decoded.adjust_factor == row.adjust_factor);
}

static void test_time_tree(void) {
    ohlc_allocator allocator = {0};
    allocator.limit = 128u * 1024u * 1024u;
    ohlc_time_node* root = NULL;
    const uint32_t count = 150001;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t key = (uint32_t)(((uint64_t)i * 7919u) % count);
        OK(ohlc_time_insert(&allocator, &root, key, key ^ 12345u));
    }
    ohlc_time_node* snapshot = root;
    ohlc_time_retain(snapshot);
    OK(ohlc_time_insert(&allocator, &root, UINT32_MAX, UINT32_MAX));
    uint32_t code = 0;
    CHECK(!ohlc_time_find(snapshot, UINT32_MAX, &code));
    CHECK(ohlc_time_find(root, UINT32_MAX, &code) && code == UINT32_MAX);
    ohlc_time_iterator iterator;
    ohlc_time_seek(root, 513, &iterator);
    for (uint32_t i = 513; i < count; i++) {
        uint32_t key = 0;
        CHECK(ohlc_time_next(&iterator, &key, &code));
        CHECK(key == i && code == (key ^ 12345u));
        uint32_t found = 0;
        CHECK(ohlc_time_find(root, key, &found) && found == code);
    }
    uint32_t key = 0;
    CHECK(ohlc_time_next(&iterator, &key, &code) && key == UINT32_MAX);
    CHECK(!ohlc_time_next(&iterator, &key, &code));
    ohlc_time_release(&allocator, snapshot);
    ohlc_time_release(&allocator, root);
    CHECK(atomic_load(&allocator.used) == 0);
}

static void test_datetime(void) {
    ohlc_table_info table = {0};
    table.period_unit = OHLC_MINUTE;
    memcpy(table.timezone, "Asia/Shanghai", sizeof("Asia/Shanghai"));
    uint32_t shanghai = 0;
    uint32_t utc = 0;
    OK(ohlc_time_parse(&table, "20260901 09:30:00", &shanghai));
    OK(ohlc_time_parse(&table, "2026-09-01T01:30:00Z", &utc));
    CHECK(shanghai == utc);
    CHECK(ohlc_time_parse(&table, "2026-09-01 09:30:01", &utc) == OHLC_INVALID);
    CHECK(ohlc_time_parse(&table, "2026-02-29 09:30:00", &utc) == OHLC_INVALID);
    CHECK(ohlc_time_parse(&table, "1969-12-31T23:59:00Z", &utc) == OHLC_INVALID);
    OK(ohlc_time_parse(&table, "1970-01-01T00:00:00Z", &utc));
    CHECK(utc == 0);
    memcpy(table.timezone, "America/New_York", sizeof("America/New_York"));
    CHECK(ohlc_time_parse(&table, "2026-03-08 02:30:00", &utc) == OHLC_INVALID);
    CHECK(ohlc_time_parse(&table, "2026-11-01 01:30:00", &utc) == OHLC_INVALID);
    OK(ohlc_time_parse(&table, "2026-11-01T01:30:00-04:00", &utc));
    uint32_t standard = 0;
    OK(ohlc_time_parse(&table, "2026-11-01T01:30:00-05:00", &standard));
    CHECK(standard - utc == 60);
    OK(ohlc_time_parse(&table, "2050-07-01 09:30:00", &utc));
    OK(ohlc_time_parse(&table, "2050-07-01T13:30:00Z", &standard));
    CHECK(standard == utc);
    OK(ohlc_time_parse(&table, "2050-01-01 09:30:00", &utc));
    OK(ohlc_time_parse(&table, "2050-01-01T14:30:00Z", &standard));
    CHECK(standard == utc);
    memcpy(table.timezone, "Australia/Sydney", sizeof("Australia/Sydney"));
    OK(ohlc_time_parse(&table, "2050-01-01 09:30:00", &utc));
    OK(ohlc_time_parse(&table, "2049-12-31T22:30:00Z", &standard));
    CHECK(standard == utc);
    char formatted[64];
    OK(ohlc_time_format(&table, UINT32_MAX, formatted, sizeof(formatted)));
    OK(ohlc_time_parse(&table, formatted, &utc));
    CHECK(utc == UINT32_MAX);
    CHECK(ohlc_time_parse(&table, "@4294967296", &utc) == OHLC_INVALID);
    table.period_unit = OHLC_DAY;
    table.timezone[0] = '\0';
    OK(ohlc_time_parse(&table, "19700101", &utc));
    CHECK(utc == 0);
    OK(ohlc_time_parse(&table, "20000229", &utc));
    CHECK(ohlc_time_parse(&table, "21000229", &standard) == OHLC_INVALID);
    OK(ohlc_time_format(&table, UINT32_MAX, formatted, sizeof(formatted)));
    OK(ohlc_time_parse(&table, formatted, &utc));
    CHECK(utc == UINT32_MAX);
}

static void test_extended_periods(void) {
    ohlc_period_unit unit = OHLC_DAY;
    uint32_t count = 7;
    OK(ohlc_period_parse("5s", &unit, &count));
    CHECK(unit == OHLC_SECOND && count == 5);
    OK(ohlc_period_parse("3mo", &unit, &count));
    CHECK(unit == OHLC_MONTH && count == 3);
    OK(ohlc_period_parse("4294967295y", &unit, &count));
    CHECK(unit == OHLC_YEAR && count == UINT32_MAX);
    const char* invalid[] = {"0s", "-1s", "1M", "1h", "1 mo", "4294967296y", "1mojunk"};
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        CHECK(ohlc_period_parse(invalid[i], &unit, &count) == OHLC_INVALID);
        CHECK(unit == OHLC_YEAR && count == UINT32_MAX);
    }

    ohlc_table_info table = {.period_unit = OHLC_SECOND, .period_count = 5};
    strcpy(table.timezone, "Asia/Shanghai");
    uint32_t key = 0;
    uint32_t other = 0;
    char formatted[64];
    OK(ohlc_time_parse(&table, "20260901 09:30:17", &key));
    CHECK(key == 1788226217u);
    OK(ohlc_time_format_local(&table, key, formatted, sizeof(formatted)));
    CHECK(strcmp(formatted, "2026-09-01T09:30:17+08:00") == 0);
    OK(ohlc_time_parse(&table, formatted, &other));
    CHECK(other == key);
    OK(ohlc_time_parse(&table, "1970-01-01T00:00:00Z", &key));
    CHECK(key == 0);
    OK(ohlc_time_parse(&table, "2106-02-07T14:28:15+08:00", &key));
    CHECK(key == UINT32_MAX);
    OK(ohlc_time_format(&table, key, formatted, sizeof(formatted)));
    CHECK(strcmp(formatted, "2106-02-07T06:28:15Z") == 0);
    const char* invalid_times[] = {"2106-02-07T06:28:16Z", "1969-12-31T23:59:59Z",
                                   "20260901 09:30:60", "20260901 09:30:00.1", "20260901"};
    for (size_t i = 0; i < sizeof(invalid_times) / sizeof(invalid_times[0]); i++) {
        CHECK(ohlc_time_parse(&table, invalid_times[i], &key) == OHLC_INVALID);
    }
    strcpy(table.timezone, "America/New_York");
    CHECK(ohlc_time_parse(&table, "2026-03-08 02:30:17", &key) == OHLC_INVALID);
    CHECK(ohlc_time_parse(&table, "2026-11-01 01:30:17", &key) == OHLC_INVALID);
    OK(ohlc_time_parse(&table, "2026-11-01T01:30:17-04:00", &key));
    OK(ohlc_time_parse(&table, "2026-11-01T01:30:17-05:00", &other));
    CHECK(other - key == 3600);
    OK(ohlc_time_format_local(&table, 0, formatted, sizeof(formatted)));
    OK(ohlc_time_parse(&table, formatted, &key));
    CHECK(key == 0);

    const ohlc_period_unit dates[] = {OHLC_MONTH, OHLC_YEAR};
    table.timezone[0] = '\0';
    for (size_t i = 0; i < sizeof(dates) / sizeof(dates[0]); i++) {
        table.period_unit = dates[i];
        OK(ohlc_time_parse(&table, "20260917", &key));
        CHECK(key == 20713);
        OK(ohlc_time_parse(&table, "20260918", &other));
        CHECK(other == key + 1);
        OK(ohlc_time_format(&table, key, formatted, sizeof(formatted)));
        CHECK(strcmp(formatted, "2026-09-17") == 0);
        OK(ohlc_time_parse(&table, "20240229", &key));
        CHECK(ohlc_time_parse(&table, "20260229", &key) == OHLC_INVALID);
        CHECK(ohlc_time_parse(&table, "20260917 00:00:00", &key) == OHLC_INVALID);
        OK(ohlc_time_format(&table, UINT32_MAX, formatted, sizeof(formatted)));
        OK(ohlc_time_parse(&table, formatted, &key));
        CHECK(key == UINT32_MAX);
    }
}

static void check_series(ohlc_db* db, uint32_t table, uint32_t stock, uint32_t first, uint32_t end,
                         bool backfill) {
    ohlc_cursor* cursor = NULL;
    OK(ohlc_series(db, table, stock, first, end, &cursor));
    uint8_t output[19 * OHLC_RESULT_BYTES];
    size_t count = 0;
    uint32_t expected = first;
    do {
        OK(ohlc_cursor_next(cursor, output, 19, &count));
        for (size_t i = 0; i < count; i++) {
            uint32_t key = ohlc_get_u32(output + i * OHLC_RESULT_BYTES);
            CHECK(key == expected);
            if (backfill && key == 0) {
                check_value(output + i * OHLC_RESULT_BYTES + 4, stock, 777);
            } else {
                check_value(output + i * OHLC_RESULT_BYTES + 4, stock, key);
            }
            expected++;
        }
    } while (count != 0);
    CHECK(expected == end);
    ohlc_cursor_close(cursor);
}

static void test_database(void) {
    const char* temporary = getenv("TMPDIR");
    char path[512];
    int path_length =
        snprintf(path, sizeof(path), "%s/ohlc-core-XXXXXX", temporary != NULL ? temporary : "/tmp");
    CHECK(path_length > 0 && (size_t)path_length < sizeof(path));
    CHECK(mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.memory_limit = 128u * 1024u * 1024u;
    options.cache_bytes = 1024u * 1024u;
    options.data_volume_bytes = 33u * OHLC_BLOCK_BYTES;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_db* duplicate = NULL;
    CHECK(ohlc_open(path, &options, &duplicate) == OHLC_BUSY);
    CHECK(duplicate == NULL);
    ohlc_table_definition definition = {"bars_3m", OHLC_MINUTE, 3, "Asia/Shanghai", "test"};
    ohlc_table_info table;
    OK(ohlc_table_create(db, &definition, &table));
    CHECK(table.id == 1 && table.created_seq == 1);
    CHECK(ohlc_table_create(db, &definition, &table) == OHLC_ALREADY_EXISTS);
    definition.name = "bars_5d";
    definition.period_unit = OHLC_DAY;
    definition.period_count = 5;
    definition.timezone = "";
    ohlc_table_info daily;
    OK(ohlc_table_create(db, &definition, &daily));
    CHECK(daily.id == 2);
    uint64_t seq = 0;
    for (uint32_t i = 0; i < 33; i++) {
        char name[32];
        int length = snprintf(name, sizeof(name), "S%u", i);
        ohlc_bytes ticker = {name, (size_t)length};
        uint32_t code = 0;
        OK(ohlc_register(db, table.id, ticker, &code, &seq));
        OK(ohlc_register(db, daily.id, ticker, &code, &seq));
        CHECK(code == i);
        OK(ohlc_register(db, table.id, ticker, &code, &seq));
        OK(ohlc_register(db, daily.id, ticker, &code, &seq));
        CHECK(code == i);
    }
    const size_t row_count = 33u * 1100u;
    uint8_t* rows = malloc(row_count * OHLC_WRITE_BYTES);
    CHECK(rows != NULL);
    size_t position = 0;
    for (uint32_t key = 1100; key != 0; key--) {
        for (uint32_t stock = 0; stock < 33; stock++) {
            ohlc_row row = value_for(stock, key);
            ohlc_write_encode(rows + position++ * OHLC_WRITE_BYTES, stock, key, &row);
        }
    }
    OK(ohlc_write(db, table.id, rows, row_count, &seq));
    uint8_t repeated[2 * OHLC_WRITE_BYTES];
    memcpy(repeated, rows, OHLC_WRITE_BYTES);
    memcpy(repeated + OHLC_WRITE_BYTES, rows, OHLC_WRITE_BYTES);
    CHECK(ohlc_write(db, table.id, repeated, 2, &seq) == OHLC_INVALID);
    size_t memory_limit = db->allocator.limit;
    db->allocator.limit = atomic_load(&db->allocator.used) + 16;
    uint64_t rejected_sequence = UINT64_MAX;
    CHECK(ohlc_write(db, table.id, rows, 1, &rejected_sequence) == OHLC_LIMIT);
    CHECK(rejected_sequence == UINT64_MAX);
    db->allocator.limit = memory_limit;
    check_series(db, table.id, 16, 1, 1101, false);
    ohlc_cursor* snapshot = NULL;
    OK(ohlc_cross(db, table.id, 17, &snapshot));
    ohlc_row zero = {0};
    uint8_t change[OHLC_WRITE_BYTES];
    ohlc_write_encode(change, 16, 17, &zero);
    OK(ohlc_write(db, table.id, change, 1, &seq));
    OK(ohlc_write(db, daily.id, change, 1, &seq));
    CHECK(ohlc_close(db) == OHLC_BUSY);
    OK(ohlc_checkpoint(db));
    uint8_t result[40 * OHLC_RESULT_BYTES];
    size_t count = 0;
    OK(ohlc_cursor_next(snapshot, result, 40, &count));
    CHECK(count == 33);
    for (size_t i = 0; i < count; i++) {
        CHECK(ohlc_get_u32(result + i * OHLC_RESULT_BYTES) == i);
        check_value(result + i * OHLC_RESULT_BYTES + 4, (uint32_t)i, 17);
    }
    ohlc_cursor_close(snapshot);
    ohlc_row original = value_for(16, 17);
    ohlc_write_encode(change, 16, 17, &original);
    OK(ohlc_write(db, table.id, change, 1, &seq));
    for (uint32_t stock = 0; stock < 33; stock++) {
        ohlc_row row = value_for(stock, 777);
        ohlc_write_encode(rows + (size_t)stock * OHLC_WRITE_BYTES, stock, 0, &row);
    }
    OK(ohlc_write(db, table.id, rows, 33, &seq));
    check_series(db, table.id, 16, 0, 1101, true);
    OK(ohlc_close(db));
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 16, 0, 1101, true);
    OK(ohlc_cross(db, daily.id, 17, &snapshot));
    OK(ohlc_cursor_next(snapshot, result, 40, &count));
    CHECK(count == 1 && ohlc_get_u32(result) == 16);
    uint8_t encoded_zero[32] = {0};
    CHECK(memcmp(result + 4, encoded_zero, 32) == 0);
    ohlc_cursor_close(snapshot);
    OK(ohlc_checkpoint(db));
    OK(ohlc_close(db));
    char current[1024];
    snprintf(current, sizeof(current), "%s/CURRENT.0", path);
    int fd = open(current, O_RDONLY);
    CHECK(fd >= 0);
    uint8_t retained_checkpoint[4096];
    CHECK(read(fd, retained_checkpoint, sizeof(retained_checkpoint)) == 4096);
    CHECK(close(fd) == 0);
    snprintf(current, sizeof(current), "%s/CURRENT.1", path);
    fd = open(current, O_RDONLY);
    CHECK(fd >= 0);
    uint8_t checkpoint_pointer[8];
    CHECK(pread(fd, checkpoint_pointer, 8, 48) == 8);
    CHECK(close(fd) == 0);
    char catalog[1024];
    snprintf(catalog, sizeof(catalog), "%s/catalog-000001.dat", path);
    fd = open(catalog, O_RDWR);
    CHECK(fd >= 0);
    uint64_t bad_offset = ohlc_get_u64(checkpoint_pointer) + 32;
    uint8_t catalog_byte = 0;
    CHECK(pread(fd, &catalog_byte, 1, (off_t)bad_offset) == 1);
    catalog_byte ^= 1u;
    CHECK(pwrite(fd, &catalog_byte, 1, (off_t)bad_offset) == 1);
    CHECK(close(fd) == 0);
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 16, 0, 1101, true);
    OK(ohlc_checkpoint(db));
    OK(ohlc_close(db));
    snprintf(current, sizeof(current), "%s/CURRENT.0", path);
    fd = open(current, O_RDONLY);
    CHECK(fd >= 0);
    uint8_t checkpoint_after_fallback[4096];
    CHECK(read(fd, checkpoint_after_fallback, sizeof(checkpoint_after_fallback)) == 4096);
    CHECK(close(fd) == 0);
    CHECK(memcmp(retained_checkpoint, checkpoint_after_fallback, 4096) == 0);
    snprintf(current, sizeof(current), "%s/CURRENT.1", path);
    fd = open(current, O_WRONLY);
    CHECK(fd >= 0);
    CHECK(pwrite(fd, "broken", 6, 0) == 6);
    CHECK(close(fd) == 0);
    char wal[1024];
    snprintf(wal, sizeof(wal), "%s/wal-000001.log", path);
    fd = open(wal, O_WRONLY | O_APPEND);
    CHECK(fd >= 0);
    CHECK(write(fd, "unfinished", 10) == 10);
    CHECK(close(fd) == 0);
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 16, 0, 1101, true);
    OK(ohlc_close(db));
    char orphan[1024];
    snprintf(orphan, sizeof(orphan), "%s/tables/00000001/data-000099.dat", path);
    fd = open(orphan, O_WRONLY | O_CREAT | O_EXCL, 0600);
    CHECK(fd >= 0 && write(fd, "orphan", 6) == 6);
    CHECK(close(fd) == 0);
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 16, 0, 1101, true);
    OK(ohlc_close(db));
    pid_t child = fork();
    CHECK(child >= 0);
    if (child == 0) {
        OK(ohlc_open(path, &options, &db));
        ohlc_row row = value_for(16, 2000);
        ohlc_write_encode(change, 16, 2000, &row);
        OK(ohlc_write(db, table.id, change, 1, &seq));
        _exit(0);
    }
    int child_status = 0;
    CHECK(waitpid(child, &child_status, 0) == child);
    CHECK(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 16, 2000, 2001, false);
    OK(ohlc_close(db));
    snprintf(orphan, sizeof(orphan), "%s/wal-000002.log", path);
    fd = open(orphan, O_WRONLY | O_CREAT | O_EXCL, 0600);
    CHECK(fd >= 0 && write(fd, "OHLCFIL", 7) == 7);
    CHECK(close(fd) == 0);
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 16, 2000, 2001, false);
    OK(ohlc_close(db));
    char data_path[1024];
    snprintf(data_path, sizeof(data_path), "%s/tables/00000001/data-000001.dat", path);
    fd = open(data_path, O_RDWR);
    CHECK(fd >= 0);
    uint8_t data_byte = 0;
    CHECK(pread(fd, &data_byte, 1, 4096) == 1);
    uint8_t damaged = (uint8_t)(data_byte ^ 1u);
    CHECK(pwrite(fd, &damaged, 1, 4096) == 1);
    CHECK(close(fd) == 0);
    OK(ohlc_open(path, &options, &db));
    OK(ohlc_cross(db, table.id, 1, &snapshot));
    CHECK(ohlc_cursor_next(snapshot, result, 40, &count) == OHLC_CORRUPT);
    ohlc_cursor_close(snapshot);
    OK(ohlc_close(db));
    fd = open(data_path, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, &data_byte, 1, 4096) == 1);
    CHECK(close(fd) == 0);
    snprintf(data_path, sizeof(data_path), "%s/tables/00000001/data-000001.meta", path);
    fd = open(data_path, O_RDWR);
    CHECK(fd >= 0 && pread(fd, &data_byte, 1, 4096) == 1);
    damaged = (uint8_t)(data_byte ^ 1u);
    CHECK(pwrite(fd, &damaged, 1, 4096) == 1);
    CHECK(close(fd) == 0);
    CHECK(ohlc_open(path, &options, &db) == OHLC_CORRUPT);
    CHECK(db == NULL);
    fd = open(data_path, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, &data_byte, 1, 4096) == 1);
    CHECK(close(fd) == 0);
    /* Covered WAL bytes are no longer part of startup validation. Damage a
     * new, uncheckpointed frame to exercise the required replay suffix. */
    OK(ohlc_open(path, &options, &db));
    ohlc_row pending = value_for(16, 3000);
    ohlc_write_encode(change, 16, 3000, &pending);
    snprintf(wal, sizeof(wal), "%s/wal-%06u.log", path, db->wal_id);
    uint64_t pending_offset = db->wal_size;
    OK(ohlc_write(db, table.id, change, 1, &seq));
    OK(ohlc_close(db));
    fd = open(wal, O_RDWR);
    CHECK(fd >= 0);
    uint8_t byte = 0;
    CHECK(pread(fd, &byte, 1, (off_t)pending_offset + 66) == 1);
    byte ^= 1u;
    CHECK(pwrite(fd, &byte, 1, (off_t)pending_offset + 66) == 1);
    CHECK(close(fd) == 0);
    CHECK(ohlc_open(path, &options, &db) == OHLC_CORRUPT);
    CHECK(db == NULL);
    free(rows);
    remove_directory(path);
}

static void compare_tables(ohlc_db* db, uint32_t first, uint32_t second, bool cross, uint32_t key) {
    ohlc_cursor* left = NULL;
    ohlc_cursor* right = NULL;
    if (cross) {
        OK(ohlc_cross(db, first, key, &left));
        OK(ohlc_cross(db, second, key, &right));
    } else {
        OK(ohlc_series(db, first, key, 0, 1000, &left));
        OK(ohlc_series(db, second, key, 0, 1000, &right));
    }
    for (;;) {
        uint8_t a[7 * OHLC_RESULT_BYTES];
        uint8_t b[7 * OHLC_RESULT_BYTES];
        size_t a_count = 0;
        size_t b_count = 0;
        OK(ohlc_cursor_next(left, a, 7, &a_count));
        OK(ohlc_cursor_next(right, b, 7, &b_count));
        CHECK(a_count == b_count);
        CHECK(memcmp(a, b, a_count * OHLC_RESULT_BYTES) == 0);
        if (a_count == 0) {
            break;
        }
    }
    ohlc_cursor_close(left);
    ohlc_cursor_close(right);
}

static void test_ordered_batches(void) {
    const char* temporary = getenv("TMPDIR");
    char path[512];
    int length = snprintf(path, sizeof(path), "%s/ohlc-batches-XXXXXX",
                          temporary != NULL ? temporary : "/tmp");
    CHECK(length > 0 && (size_t)length < sizeof(path));
    CHECK(mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 0;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_table_definition definition = {"ordered", OHLC_MINUTE, 1, "UTC", ""};
    ohlc_table_info ordered;
    ohlc_table_info shuffled;
    OK(ohlc_table_create(db, &definition, &ordered));
    definition.name = "shuffled";
    OK(ohlc_table_create(db, &definition, &shuffled));
    uint64_t sequence = 0;
    for (uint32_t i = 0; i < 37; i++) {
        char name[16];
        length = snprintf(name, sizeof(name), "S%u", i);
        uint32_t code = 0;
        OK(ohlc_register(db, ordered.id, (ohlc_bytes){name, (size_t)length}, &code, &sequence));
        OK(ohlc_register(db, shuffled.id, (ohlc_bytes){name, (size_t)length}, &code, &sequence));
    }
    uint8_t seed[OHLC_WRITE_BYTES];
    for (uint32_t i = 4; i != 0; i--) {
        ohlc_row row = value_for(13, (i - 1u) * 3u);
        ohlc_write_encode(seed, 13, (i - 1u) * 3u, &row);
        OK(ohlc_write(db, ordered.id, seed, 1, &sequence));
        OK(ohlc_write(db, shuffled.id, seed, 1, &sequence));
    }
    enum { ROWS = 160 * 20 };
    uint8_t* forward = malloc(ROWS * OHLC_WRITE_BYTES);
    uint8_t* reverse = malloc(ROWS * OHLC_WRITE_BYTES);
    CHECK(forward != NULL && reverse != NULL);
    for (uint32_t t = 0; t < 160; t++) {
        for (uint32_t stock = 13; stock < 33; stock++) {
            size_t index = (size_t)t * 20u + stock - 13u;
            ohlc_row row = value_for(stock, t * 3u);
            ohlc_write_encode(forward + index * OHLC_WRITE_BYTES, stock, t * 3u, &row);
            memcpy(reverse + (ROWS - index - 1u) * OHLC_WRITE_BYTES,
                   forward + index * OHLC_WRITE_BYTES, OHLC_WRITE_BYTES);
        }
    }
    OK(ohlc_write(db, ordered.id, forward, ROWS, &sequence));
    OK(ohlc_write(db, shuffled.id, reverse, ROWS, &sequence));
    /* Compare the optimized path with unordered input across partial stock
     * groups, band boundaries, missing real times and nonmonotone time codes. */
    for (unsigned int reopen = 0; reopen < 2; reopen++) {
        for (uint32_t stock = 12; stock < 34; stock++) {
            compare_tables(db, ordered.id, shuffled.id, false, stock);
        }
        for (uint32_t key = 0; key < 481; key++) {
            compare_tables(db, ordered.id, shuffled.id, true, key);
        }
        if (reopen == 0) {
            OK(ohlc_checkpoint(db));
            OK(ohlc_close(db));
            OK(ohlc_open(path, &options, &db));
        }
    }
    memcpy(forward + 20 * OHLC_WRITE_BYTES, forward, OHLC_WRITE_BYTES);
    uint64_t rejected = UINT64_MAX;
    CHECK(ohlc_write(db, ordered.id, forward, ROWS, &rejected) == OHLC_INVALID);
    CHECK(rejected == UINT64_MAX);
    free(forward);
    free(reverse);
    OK(ohlc_close(db));
    remove_directory(path);
}

static void test_wal_retention(void) {
    const char* temporary = getenv("TMPDIR");
    char path[512];
    int length =
        snprintf(path, sizeof(path), "%s/ohlc-wal-XXXXXX", temporary != NULL ? temporary : "/tmp");
    CHECK(length > 0 && (size_t)length < sizeof(path));
    CHECK(mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 0;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_table_definition definition = {"bars", OHLC_MINUTE, 1, "UTC", ""};
    ohlc_table_info table;
    uint32_t ticker = 0;
    uint64_t sequence = 0;
    OK(ohlc_table_create(db, &definition, &table));
    OK(ohlc_register(db, table.id, (ohlc_bytes){"A", 1}, &ticker, &sequence));
    /* Tiny internal segments exercise rollover without a large fixture. */
    db->options.wal_segment_bytes = 8192;
    uint8_t rows[100 * OHLC_WRITE_BYTES];
    unsigned int newest = 0;
    for (uint32_t batch = 0; batch < 3; batch++) {
        for (uint32_t i = 0; i < 100; i++) {
            uint32_t key = batch * 100u + i + 1u;
            ohlc_row row = value_for(0, key);
            ohlc_write_encode(rows + (size_t)i * OHLC_WRITE_BYTES, ticker, key, &row);
        }
        OK(ohlc_write(db, table.id, rows, 100, &sequence));
        CHECK(db->wal_id == batch + 2u);
        if (batch < 2) {
            OK(ohlc_checkpoint(db));
            newest = (unsigned int)((db->checkpoint_generation - 1u) & 1u);
        }
    }
    CHECK(db->wal_first_id == 2);
    CHECK(faccessat(db->directory_fd, "wal-000001.log", F_OK, 0) != 0);
    CHECK(faccessat(db->directory_fd, "wal-000002.log", F_OK, 0) == 0);
    OK(ohlc_close(db));
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 0, 1, 301, false);
    OK(ohlc_close(db));

    /* The older CURRENT must recover through retained segments after cleanup. */
    char filename[1024];
    snprintf(filename, sizeof(filename), "%s/CURRENT.%u", path, newest);
    int fd = open(filename, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, "broken", 6, 0) == 6);
    CHECK(close(fd) == 0);
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 0, 1, 301, false);
    OK(ohlc_checkpoint(db));
    if (getenv("OHLC_TEST_RECLAIM_FAILURES") != NULL) {
        OK(ohlc_close(db));
        pid_t child = fork();
        CHECK(child >= 0);
        if (child == 0) {
            OK(ohlc_open(path, &options, &db));
            CHECK(setenv("OHLC_TEST_RECLAIM_CRASH", path, 1) == 0);
            OK(ohlc_checkpoint(db));
            _exit(1);
        }
        int status = 0;
        CHECK(waitpid(child, &status, 0) == child);
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 86);
        OK(ohlc_open(path, &options, &db));
        check_series(db, table.id, 0, 1, 301, false);
    } else {
        OK(ohlc_checkpoint(db));
    }
    CHECK(db->wal_first_id == 4);
    CHECK(faccessat(db->directory_fd, "wal-000002.log", F_OK, 0) != 0);
    CHECK(faccessat(db->directory_fd, "wal-000003.log", F_OK, 0) != 0);
    OK(ohlc_close(db));

    /* Both checkpoints cover this prefix; recovery seeks past it. */
    snprintf(filename, sizeof(filename), "%s/wal-000004.log", path);
    fd = open(filename, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, "covered", 7, 4096) == 7);
    CHECK(close(fd) == 0);
    OK(ohlc_open(path, &options, &db));
    check_series(db, table.id, 0, 1, 301, false);
    OK(ohlc_close(db));
    CHECK(unlink(filename) == 0);
    CHECK(ohlc_open(path, &options, &db) == OHLC_CORRUPT && db == NULL);
    remove_directory(path);
}

static void test_drop_recovery(void) {
    char path[] = "/tmp/ohlc-drop-XXXXXX";
    CHECK(mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.max_tables = 1;
    options.cache_bytes = 0;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_table_definition definition = {"bars", OHLC_DAY, 1, "", ""};
    ohlc_table_info old;
    OK(ohlc_table_create(db, &definition, &old));
    uint32_t ticker;
    uint64_t sequence;
    OK(ohlc_register(db, old.id, (ohlc_bytes){"AAPL", 4}, &ticker, &sequence));
    uint8_t record[OHLC_WRITE_BYTES];
    ohlc_row row = value_for(ticker, 1);
    ohlc_write_encode(record, ticker, 1, &row);
    OK(ohlc_write(db, old.id, record, 1, &sequence));
    OK(ohlc_checkpoint(db));
    OK(ohlc_checkpoint(db));
    size_t budget = db->allocator.limit;
    db->allocator.limit = atomic_load(&db->allocator.used);
    uint64_t unchanged = sequence;
    CHECK(ohlc_table_drop(db, old.id, &sequence) == OHLC_LIMIT);
    CHECK(sequence == unchanged);
    db->allocator.limit = budget;
    OK(ohlc_table_get(db, old.id, &old));
    OK(ohlc_close(db));

    pid_t child = fork();
    CHECK(child >= 0);
    if (child == 0) {
        OK(ohlc_open(path, &options, &db));
        OK(ohlc_table_drop(db, old.id, &sequence));
        _exit(0);
    }
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    OK(ohlc_open(path, &options, &db));
    CHECK(ohlc_table_get(db, old.id, &old) == OHLC_NOT_FOUND);
    ohlc_table_info replacement;
    OK(ohlc_table_create(db, &definition, &replacement));
    CHECK(replacement.id > old.id);
    ohlc_bytes name = {"AAPL", 4};
    OK(ohlc_write_named(db, replacement.id, &name, 1, record, 1, &sequence));
    char directory[1024];
    snprintf(directory, sizeof(directory), "%s/tables/%08x", path, old.id);
    OK(ohlc_checkpoint(db));
    CHECK(access(directory, F_OK) == 0);
    ohlc_cursor* cursor = NULL;
    OK(ohlc_cross(db, replacement.id, 1, &cursor));
    OK(ohlc_checkpoint(db));
    CHECK(access(directory, F_OK) == 0);
    ohlc_cursor_close(cursor);
    OK(ohlc_checkpoint(db));
    CHECK(access(directory, F_OK) != 0);
    OK(ohlc_close(db));
    OK(ohlc_open(path, &options, &db));
    CHECK(ohlc_table_get(db, old.id, &old) == OHLC_NOT_FOUND);
    check_series(db, replacement.id, ticker, 1, 2, false);
    OK(ohlc_close(db));
    remove_directory(path);
}

typedef struct {
    ohlc_db* db;
    ohlc_status status;
} drop_checkpoint_job;

static void* checkpoint_during_drop(void* argument) {
    drop_checkpoint_job* job = argument;
    job->status = ohlc_checkpoint(job->db);
    return NULL;
}

static void test_drop_checkpoint_race(void) {
    char path[] = "/tmp/ohlc-drop-race-XXXXXX";
    CHECK(mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 0;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_table_definition definition = {"bars", OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    OK(ohlc_table_create(db, &definition, &table));
    uint32_t ticker;
    uint64_t sequence;
    OK(ohlc_register(db, table.id, (ohlc_bytes){"AAPL", 4}, &ticker, &sequence));
    uint8_t record[OHLC_WRITE_BYTES];
    ohlc_row row = value_for(ticker, 1);
    ohlc_write_encode(record, ticker, 1, &row);
    OK(ohlc_write(db, table.id, record, 1, &sequence));

    /* Pause file publication after the checkpoint pins the pre-drop root.
     * Its later merge must not put the removed table back into the live root. */
    ohlc_root* pinned = ohlc_root_acquire(db);
    pthread_mutex_lock(&db->storage_mutex);
    drop_checkpoint_job job = {.db = db};
    pthread_t worker;
    CHECK(pthread_create(&worker, NULL, checkpoint_during_drop, &job) == 0);
    uint64_t deadline = ohlc_monotonic_ms() + 5000;
    while (atomic_load(&pinned->refs) < 3) {
        CHECK(ohlc_monotonic_ms() < deadline);
        struct timespec delay = {.tv_nsec = 1000000};
        nanosleep(&delay, NULL);
    }
    OK(ohlc_table_drop(db, table.id, &sequence));
    pthread_mutex_unlock(&db->storage_mutex);
    CHECK(pthread_join(worker, NULL) == 0);
    CHECK(job.status == OHLC_OK);
    ohlc_root_release(db, pinned);
    CHECK(ohlc_table_get(db, table.id, &table) == OHLC_NOT_FOUND);
    OK(ohlc_checkpoint(db));
    OK(ohlc_checkpoint(db));
    OK(ohlc_close(db));
    OK(ohlc_open(path, &options, &db));
    CHECK(ohlc_table_get(db, table.id, &table) == OHLC_NOT_FOUND);
    OK(ohlc_close(db));
    remove_directory(path);
}

static void test_table_dictionaries(void) {
    char path[] = "/tmp/ohlc-dictionary-XXXXXX";
    CHECK(mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 0;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_table_definition definition = {"us", OHLC_DAY, 1, "", ""};
    ohlc_table_info us;
    ohlc_table_info hk;
    OK(ohlc_table_create(db, &definition, &us));
    definition.name = "hk";
    OK(ohlc_table_create(db, &definition, &hk));
    ohlc_bytes names[] = {{"AAPL", 4}, {"SHARED", 6}, {"NEW", 3}, {"NEW", 3}};
    uint8_t rows[2 * OHLC_WRITE_BYTES];
    ohlc_row value = value_for(0, 1);
    ohlc_write_encode(rows, 0, 1, &value);
    ohlc_write_encode(rows + OHLC_WRITE_BYTES, 1, 1, &value);
    uint64_t sequence = 0;
    OK(ohlc_write_named(db, us.id, names, 2, rows, 2, &sequence));
    OK(ohlc_write_named(db, hk.id, names + 1, 1, rows, 1, &sequence));
    uint32_t code = UINT32_MAX;
    OK(ohlc_resolve(db, us.id, names[1], &code));
    CHECK(code == 1);
    OK(ohlc_resolve(db, hk.id, names[1], &code));
    CHECK(code == 0);
    CHECK(ohlc_resolve(db, hk.id, names[0], &code) == OHLC_NOT_FOUND);
    ohlc_stats before;
    ohlc_get_stats(db, &before);
    CHECK(before.ticker_count == 3);
    uint64_t unchanged = sequence;
    /* Two different batch indexes name the same logical key. Neither name nor
     * rows may escape the rejected candidate. The next success still gets 1. */
    CHECK(ohlc_write_named(db, hk.id, names + 2, 2, rows, 2, &sequence) == OHLC_INVALID);
    CHECK(sequence == unchanged);
    CHECK(ohlc_resolve(db, hk.id, names[2], &code) == OHLC_NOT_FOUND);
    ohlc_stats after;
    ohlc_get_stats(db, &after);
    CHECK(after.commit_seq == before.commit_seq && after.ticker_count == before.ticker_count);
    size_t budget = db->allocator.limit;
    db->allocator.limit = atomic_load(&db->allocator.used);
    CHECK(ohlc_write_named(db, hk.id, names + 2, 1, rows, 1, &sequence) == OHLC_LIMIT);
    db->allocator.limit = budget;
    OK(ohlc_write_named(db, hk.id, names + 2, 1, rows, 1, &sequence));
    OK(ohlc_resolve(db, hk.id, names[2], &code));
    CHECK(code == 1);
    OK(ohlc_close(db));
    /* Rebuild names and rows from WAL before any data checkpoint exists. */
    OK(ohlc_open(path, &options, &db));
    OK(ohlc_resolve(db, hk.id, names[2], &code));
    CHECK(code == 1);
    OK(ohlc_checkpoint(db));
    OK(ohlc_checkpoint(db));
    OK(ohlc_close(db));
    OK(ohlc_open(path, &options, &db));
    uint8_t text[4096];
    ohlc_bytes ticker;
    OK(ohlc_ticker(db, hk.id, 0, text, &ticker));
    CHECK(ticker.size == 6 && memcmp(ticker.data, "SHARED", 6) == 0);
    ohlc_cursor* pinned = NULL;
    OK(ohlc_cross(db, hk.id, 1, &pinned));
    OK(ohlc_table_drop(db, hk.id, &sequence));
    CHECK(ohlc_resolve(db, hk.id, names[1], &code) == OHLC_NOT_FOUND);
    size_t count = 0;
    uint8_t results[2 * OHLC_RESULT_BYTES];
    OK(ohlc_cursor_next(pinned, results, 2, &count));
    CHECK(count == 2);
    ohlc_cursor_close(pinned);
    OK(ohlc_table_create(db, &definition, &hk));
    OK(ohlc_write_named(db, hk.id, names + 2, 1, rows, 1, &sequence));
    OK(ohlc_resolve(db, hk.id, names[2], &code));
    CHECK(code == 0);
    OK(ohlc_close(db));
    /* A valid old-version header must fail explicitly, without conversion. */
    char catalog[512];
    CHECK(snprintf(catalog, sizeof(catalog), "%s/catalog-000001.dat", path) > 0);
    int fd = open(catalog, O_RDWR);
    CHECK(fd >= 0);
    uint8_t header[OHLC_BLOCK_BYTES];
    OK(ohlc_read_full(fd, header, sizeof(header), 0));
    header[7] = '4';
    ohlc_put_u32(header + 8, 4);
    ohlc_put_u32(header + 4092, 0);
    ohlc_put_u32(header + 4092, ohlc_crc32c(0, header, sizeof(header)));
    OK(ohlc_write_full(fd, header, sizeof(header), 0));
    CHECK(close(fd) == 0);
    CHECK(ohlc_open(path, &options, &db) == OHLC_UNSUPPORTED && db == NULL);
    remove_directory(path);
}

static void test_selected_cross(void) {
    char path[] = "/tmp/ohlc-selected-XXXXXX";
    CHECK(mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 0;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_table_definition definition = {"selected", OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    OK(ohlc_table_create(db, &definition, &table));
    char labels[320][16];
    ohlc_bytes names[321];
    uint8_t writes[320 * OHLC_WRITE_BYTES];
    for (uint32_t i = 0; i < 320; i++) {
        int length = snprintf(labels[i], sizeof(labels[i]), "S%u", i);
        CHECK(length > 0 && length < (int)sizeof(labels[i]));
        names[i] = (ohlc_bytes){labels[i], (size_t)length};
        ohlc_row value = value_for(i, 7);
        ohlc_write_encode(writes + i * OHLC_WRITE_BYTES, i, 7, &value);
    }
    uint64_t sequence;
    OK(ohlc_write_named(db, table.id, names, 320, writes, 320, &sequence));
    uint32_t code;
    OK(ohlc_register(db, table.id, (ohlc_bytes){"NO_ROW", 6}, &code, &sequence));
    OK(ohlc_checkpoint(db));
    OK(ohlc_close(db));
    OK(ohlc_open(path, &options, &db));
    ohlc_bytes selection[] = {names[319], names[0], names[319], {"UNKNOWN", 7}, {"NO_ROW", 6}};
    ohlc_stats before;
    ohlc_stats after;
    ohlc_get_stats(db, &before);
    ohlc_cursor* cursor = NULL;
    OK(ohlc_cross_tickers(db, table.id, 7, selection, 5, &cursor));
    uint8_t results[7 * OHLC_RESULT_BYTES];
    size_t count;
    for (uint32_t i = 0; i < 2; i++) {
        OK(ohlc_cursor_next(cursor, results, 1, &count));
        uint32_t expected = i == 0 ? 0 : 319;
        CHECK(count == 1 && ohlc_get_u32(results) == expected);
        check_value(results + 4, expected, 7);
    }
    OK(ohlc_cursor_next(cursor, results, 1, &count));
    CHECK(count == 0);
    ohlc_cursor_close(cursor);
    ohlc_get_stats(db, &after);
    CHECK(after.disk_read_bytes - before.disk_read_bytes == 2 * OHLC_BLOCK_BYTES);
    CHECK(after.commit_seq == before.commit_seq && after.ticker_count == before.ticker_count);

    OK(ohlc_cross_tickers(db, table.id, 7, NULL, 0, &cursor));
    OK(ohlc_cursor_next(cursor, results, 1, &count));
    CHECK(count == 0);
    ohlc_cursor_close(cursor);
    OK(ohlc_cross_tickers(db, table.id, 8, selection, 5, &cursor));
    OK(ohlc_cursor_next(cursor, results, 1, &count));
    CHECK(count == 0);
    ohlc_cursor_close(cursor);
    CHECK(ohlc_cross_tickers(db, table.id, 7, NULL, 1, &cursor) == OHLC_INVALID);
    CHECK(cursor == NULL);
    CHECK(ohlc_cross_tickers(db, table.id, 7, names, OHLC_MAX_QUERY_TICKERS + 1, &cursor) ==
          OHLC_LIMIT);
    size_t budget = db->allocator.limit;
    size_t used = atomic_load(&db->allocator.used);
    OK(ohlc_cross(db, table.id, 7, &cursor));
    size_t cursor_budget = atomic_load(&db->allocator.used);
    ohlc_cursor_close(cursor);
    db->allocator.limit = cursor_budget;
    CHECK(ohlc_cross_tickers(db, table.id, 7, selection, 5, &cursor) == OHLC_LIMIT);
    CHECK(cursor == NULL && atomic_load(&db->allocator.used) == used);
    CHECK(atomic_load(&db->cursor_count) == 0);
    db->allocator.limit = budget;

    /* More than one read window, small output buffers, and a pinned selection
     * that survives a concurrent new ticker followed by table deletion. */
    for (size_t i = 0; i < 160; i++) {
        ohlc_bytes swapped = names[i];
        names[i] = names[319 - i];
        names[319 - i] = swapped;
    }
    names[320] = (ohlc_bytes){"LATER", 5};
    OK(ohlc_cross_tickers(db, table.id, 7, names, 321, &cursor));
    uint64_t snapshot = ohlc_cursor_sequence(cursor);
    OK(ohlc_write_named(db, table.id, names + 320, 1, writes, 1, &sequence));
    CHECK(sequence > snapshot);
    OK(ohlc_table_drop(db, table.id, &sequence));
    uint32_t received = 0;
    do {
        OK(ohlc_cursor_next(cursor, results, 7, &count));
        for (size_t i = 0; i < count; i++) {
            CHECK(ohlc_get_u32(results + i * OHLC_RESULT_BYTES) == received);
            check_value(results + i * OHLC_RESULT_BYTES + 4, received++, 7);
        }
    } while (count != 0);
    CHECK(received == 320 && ohlc_cursor_sequence(cursor) == snapshot);
    ohlc_cursor_close(cursor);
    OK(ohlc_close(db));
    remove_directory(path);
}

int main(void) {
    test_selected_cross();
    test_table_dictionaries();
    test_format();
    test_time_tree();
    test_datetime();
    test_extended_periods();
    test_database();
    test_wal_retention();
    test_ordered_batches();
    test_drop_recovery();
    test_drop_checkpoint_race();
    puts("format, indexes, snapshots, volume rollover, WAL, checkpoint and recovery: OK");
    return 0;
}

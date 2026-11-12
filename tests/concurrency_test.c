/* SPDX-License-Identifier: Apache-2.0 */
#include "ohlc/ohlc.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    ohlc_db* db;
    uint32_t table;
    _Atomic bool failed;
    _Atomic bool published;
    _Atomic unsigned int ready_readers;
} test_context;

static bool check_status(test_context* context, ohlc_status status) {
    if (status != OHLC_OK) {
        fprintf(stderr, "Concurrent operation: %s\n", ohlc_status_string(status));
        atomic_store(&context->failed, true);
        return false;
    }
    return true;
}

static void* write_batches(void* argument) {
    test_context* context = argument;
    uint8_t batch[32 * OHLC_WRITE_BYTES];
    while (atomic_load(&context->ready_readers) != 4 && !atomic_load(&context->failed)) {
        sched_yield();
    }
    for (uint32_t generation = 1; generation <= 100; generation++) {
        for (uint32_t ticker = 0; ticker < 32; ticker++) {
            ohlc_row row = {.flags = OHLC_FIELDS_ALL};
            row.close = (int32_t)generation;
            row.volume = ticker;
            row.amount = ((uint64_t)generation << 32) | ticker;
            ohlc_write_encode(batch + (size_t)ticker * OHLC_WRITE_BYTES, ticker, 42, &row);
        }
        uint64_t sequence = 0;
        if (!check_status(context, ohlc_write(context->db, context->table, batch, 32, &sequence))) {
            break;
        }
        atomic_store(&context->published, true);
        if (generation % 10 == 0 && !check_status(context, ohlc_checkpoint(context->db))) {
            break;
        }
    }
    return NULL;
}

static void* read_snapshots(void* argument) {
    test_context* context = argument;
    for (unsigned int iteration = 0; iteration < 300; iteration++) {
        ohlc_cursor* cursor = NULL;
        if (!check_status(context, ohlc_cross(context->db, context->table, 42, &cursor))) {
            break;
        }
        if (iteration == 0) {
            atomic_fetch_add(&context->ready_readers, 1);
            while (!atomic_load(&context->published) && !atomic_load(&context->failed)) {
                sched_yield();
            }
        }
        int64_t generation = -1;
        size_t rows = 0;
        while (true) {
            uint8_t result[3 * OHLC_RESULT_BYTES];
            size_t count = 0;
            if (!check_status(context, ohlc_cursor_next(cursor, result, 3, &count)) || count == 0) {
                break;
            }
            for (size_t i = 0; i < count; i++) {
                ohlc_row row;
                ohlc_row_decode(result + i * OHLC_RESULT_BYTES + 4, &row);
                if (generation < 0) {
                    generation = row.close;
                }
                if (row.close != generation || (iteration == 0 && generation != 0) ||
                    row.volume != rows ||
                    row.amount != ((uint64_t)(uint32_t)generation << 32 | rows)) {
                    atomic_store(&context->failed, true);
                }
                rows++;
            }
        }
        if (rows != 32) {
            atomic_store(&context->failed, true);
        }
        ohlc_cursor_close(cursor);
    }
    return NULL;
}

static bool create_fixture(char* path, size_t capacity, const char* prefix) {
    const char* temporary = getenv("TMPDIR");
    int length =
        snprintf(path, capacity, "%s/%s-XXXXXX", temporary != NULL ? temporary : "/tmp", prefix);
    if (length <= 0 || (size_t)length >= capacity) {
        return false;
    }
    return mkdtemp(path) != NULL;
}

/* Remove the files a fixture owns; a missing file fails. */
static bool remove_fixture(const char* path, const char* const* files, size_t count) {
    bool removed = true;
    char file[1024];
    for (size_t i = 0; i < count; i++) {
        snprintf(file, sizeof(file), "%s/%s", path, files[i]);
        if (unlink(file) != 0) {
            removed = false;
        }
    }
    snprintf(file, sizeof(file), "%s/tables/00000001", path);
    (void)rmdir(file);
    snprintf(file, sizeof(file), "%s/tables", path);
    (void)rmdir(file);
    (void)rmdir(path);
    return removed;
}

/* Cache churn: 64 tickers by 64 days form 32 tiles, while the cache holds one
 * sixteen-way set. Readers keep reusing entries that other readers are hitting,
 * and any borrowed entry that was reused would return another tile's rows. */
#define CHURN_TICKERS 64u
#define CHURN_TIMES 64u
#define CHURN_TILES ((CHURN_TICKERS / 16u) * (CHURN_TIMES / 8u))
#define CHURN_FIRST_TIME 20000u
#define CHURN_READERS 4u
#define CHURN_QUERIES 20000u
#define CHURN_CACHE_BYTES (192u * 1024u)

typedef struct {
    test_context* context;
    const uint32_t* ticker_codes;
    uint32_t seed;
} churn_reader;

static void churn_row(uint32_t ticker_code, uint32_t time_key, uint8_t output[OHLC_ROW_BYTES]) {
    uint64_t key = (uint64_t)ticker_code << 32 | time_key;
    ohlc_row row = {.flags = OHLC_FIELDS_ALL};
    row.open = (int64_t)key;
    row.high = (int64_t)key + 1;
    row.low = (int64_t)key - 1;
    row.close = -(int64_t)key;
    row.volume = key * 3u;
    row.amount = key * 5u;
    row.adjust_factor = key * 7u;
    row.reserved = (int32_t)(ticker_code * 1000u + time_key % 1000u);
    ohlc_row_encode(output, &row);
}

static uint32_t result_key(const uint8_t* result) {
    return (uint32_t)result[0] | (uint32_t)result[1] << 8 | (uint32_t)result[2] << 16 |
           (uint32_t)result[3] << 24;
}

/* Run one series or cross query and compare every returned row byte for byte. */
static bool churn_query(const churn_reader* reader, bool series, uint32_t index) {
    test_context* context = reader->context;
    ohlc_cursor* cursor = NULL;
    ohlc_status status = OHLC_OK;
    if (series) {
        status = ohlc_series(context->db, context->table, reader->ticker_codes[index],
                             CHURN_FIRST_TIME, CHURN_FIRST_TIME + CHURN_TIMES - 1u, &cursor);
    } else {
        status = ohlc_cross(context->db, context->table, CHURN_FIRST_TIME + index, &cursor);
    }
    if (!check_status(context, status)) {
        return false;
    }
    bool matched = true;
    size_t rows = 0;
    while (matched) {
        uint8_t results[16 * OHLC_RESULT_BYTES];
        size_t count = 0;
        if (!check_status(context, ohlc_cursor_next(cursor, results, 16, &count))) {
            matched = false;
            break;
        }
        if (count == 0) {
            break;
        }
        for (size_t i = 0; i < count && matched; i++, rows++) {
            const uint8_t* result = results + i * OHLC_RESULT_BYTES;
            uint32_t key = result_key(result);
            uint32_t ticker_code = series ? reader->ticker_codes[index] : key;
            uint32_t time_key = series ? key : CHURN_FIRST_TIME + index;
            uint32_t expected_key = series ? CHURN_FIRST_TIME + (uint32_t)rows
                                           : reader->ticker_codes[rows % CHURN_TICKERS];
            uint8_t expected[OHLC_ROW_BYTES];
            churn_row(ticker_code, time_key, expected);
            matched = key == expected_key && memcmp(result + 4, expected, sizeof(expected)) == 0;
        }
    }
    ohlc_cursor_close(cursor);
    return matched && rows == (series ? CHURN_TIMES : CHURN_TICKERS);
}

static void* churn_read(void* argument) {
    churn_reader* reader = argument;
    uint32_t state = reader->seed;
    for (uint32_t query = 0; query < CHURN_QUERIES; query++) {
        if (atomic_load(&reader->context->failed)) {
            break;
        }
        state = state * 1664525u + 1013904223u;
        bool series = (state >> 31) != 0;
        uint32_t index = (state >> 8) % (series ? CHURN_TICKERS : CHURN_TIMES);
        if (!churn_query(reader, series, index)) {
            fprintf(stderr, "Cache churn returned unexpected rows\n");
            atomic_store(&reader->context->failed, true);
        }
    }
    return NULL;
}

static bool churn_load(test_context* context, uint32_t ticker_codes[CHURN_TICKERS]) {
    ohlc_table_definition definition = {"churn", OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    if (!check_status(context, ohlc_table_create(context->db, &definition, &table))) {
        return false;
    }
    context->table = table.id;
    for (uint32_t i = 0; i < CHURN_TICKERS; i++) {
        char name[16];
        int name_length = snprintf(name, sizeof(name), "C%u", i);
        ohlc_bytes ticker = {name, (size_t)name_length};
        uint64_t sequence = 0;
        if (!check_status(context, ohlc_register(context->db, context->table, ticker,
                                                 &ticker_codes[i], &sequence))) {
            return false;
        }
    }
    for (uint32_t time = 0; time < CHURN_TIMES; time++) {
        uint8_t batch[CHURN_TICKERS * OHLC_WRITE_BYTES];
        for (uint32_t i = 0; i < CHURN_TICKERS; i++) {
            uint8_t* output = batch + (size_t)i * OHLC_WRITE_BYTES;
            uint8_t encoded[OHLC_ROW_BYTES];
            ohlc_row row;
            churn_row(ticker_codes[i], CHURN_FIRST_TIME + time, encoded);
            ohlc_row_decode(encoded, &row);
            ohlc_write_encode(output, ticker_codes[i], CHURN_FIRST_TIME + time, &row);
        }
        uint64_t sequence = 0;
        if (!check_status(context, ohlc_write(context->db, context->table, batch, CHURN_TICKERS,
                                              &sequence))) {
            return false;
        }
    }
    return check_status(context, ohlc_checkpoint(context->db));
}

static bool run_cache_churn(void) {
    char path[512];
    if (!create_fixture(path, sizeof(path), "ohlc-cache-churn")) {
        return false;
    }
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = CHURN_CACHE_BYTES;
    test_context context = {0};
    uint32_t ticker_codes[CHURN_TICKERS];
    if (!check_status(&context, ohlc_open(path, &options, &context.db))) {
        return false;
    }
    bool loaded = churn_load(&context, ticker_codes);
    (void)check_status(&context, ohlc_close(context.db));
    /* Reopen so that every query reads checkpointed tiles through the cache. */
    if (!loaded || !check_status(&context, ohlc_open(path, &options, &context.db))) {
        return false;
    }
    churn_reader readers[CHURN_READERS];
    pthread_t threads[CHURN_READERS];
    size_t started = 0;
    for (; started < CHURN_READERS; started++) {
        churn_reader* reader = &readers[started];
        reader->context = &context;
        reader->ticker_codes = ticker_codes;
        reader->seed = 0x9e3779b9u * (uint32_t)(started + 1u);
        if (pthread_create(&threads[started], NULL, churn_read, reader) != 0) {
            atomic_store(&context.failed, true);
            break;
        }
    }
    for (size_t i = 0; i < started; i++) {
        pthread_join(threads[i], NULL);
    }
    ohlc_stats stats;
    ohlc_get_stats(context.db, &stats);
    /* More misses than tiles show that entries were reused while others hit. */
    if (stats.cache_hits == 0 || stats.cache_misses <= CHURN_TILES) {
        fprintf(stderr, "Cache churn: hits=%llu misses=%llu\n",
                (unsigned long long)stats.cache_hits, (unsigned long long)stats.cache_misses);
        atomic_store(&context.failed, true);
    }
    (void)check_status(&context, ohlc_close(context.db));
    const char* const files[] = {"LOCK",
                                 "CURRENT.0",
                                 "CURRENT.1",
                                 "catalog-000001.dat",
                                 "wal-000001.log",
                                 "tables/00000001/data-000001.dat",
                                 "tables/00000001/data-000001.meta",
                                 "tables/00000001/index-000001.dat"};
    if (!remove_fixture(path, files, sizeof(files) / sizeof(files[0]))) {
        atomic_store(&context.failed, true);
    }
    return !atomic_load(&context.failed);
}

int main(void) {
    char path[512];
    if (!create_fixture(path, sizeof(path), "ohlc-concurrency")) {
        return 1;
    }
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 1024u * 1024u;
    test_context context = {0};
    if (!check_status(&context, ohlc_open(path, &options, &context.db))) {
        return 1;
    }
    ohlc_table_definition definition = {"concurrent", OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    if (!check_status(&context, ohlc_table_create(context.db, &definition, &table))) {
        return 1;
    }
    context.table = table.id;
    uint8_t initial_batch[32 * OHLC_WRITE_BYTES];
    for (uint32_t i = 0; i < 32; i++) {
        char name[16];
        int name_length = snprintf(name, sizeof(name), "S%u", i);
        ohlc_bytes ticker = {name, (size_t)name_length};
        uint32_t code = 0;
        uint64_t sequence = 0;
        if (!check_status(&context,
                          ohlc_register(context.db, context.table, ticker, &code, &sequence))) {
            return 1;
        }
        ohlc_row initial = {.flags = OHLC_FIELDS_MARKET};
        initial.volume = i;
        initial.amount = i;
        ohlc_write_encode(initial_batch + (size_t)i * OHLC_WRITE_BYTES, i, 42, &initial);
    }
    uint64_t initial_sequence = 0;
    if (!check_status(&context,
                      ohlc_write(context.db, table.id, initial_batch, 32, &initial_sequence))) {
        return 1;
    }
    pthread_t writer;
    pthread_t readers[4];
    if (pthread_create(&writer, NULL, write_batches, &context) != 0) {
        return 1;
    }
    for (size_t i = 0; i < 4; i++) {
        if (pthread_create(&readers[i], NULL, read_snapshots, &context) != 0) {
            return 1;
        }
    }
    pthread_join(writer, NULL);
    for (size_t i = 0; i < 4; i++) {
        pthread_join(readers[i], NULL);
    }
    (void)check_status(&context, ohlc_close(context.db));
    /* These are the complete files owned by this dedicated test fixture. */
    const char* const files[] = {"LOCK",
                                 "CURRENT.0",
                                 "CURRENT.1",
                                 "catalog-000001.dat",
                                 "wal-000001.log",
                                 "tables/00000001/data-000001.dat",
                                 "tables/00000001/data-000001.meta",
                                 "tables/00000001/index-000001.dat"};
    if (!remove_fixture(path, files, sizeof(files) / sizeof(files[0]))) {
        atomic_store(&context.failed, true);
    }
    if (atomic_load(&context.failed)) {
        return 1;
    }
    puts("concurrent writers, snapshots and checkpoints: OK");
    if (!run_cache_churn()) {
        return 1;
    }
    puts("cache hits during concurrent entry reuse: OK");
    return 0;
}

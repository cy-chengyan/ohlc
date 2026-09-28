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
            ohlc_row row = {0};
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
        int32_t generation = -1;
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

int main(void) {
    const char* temporary = getenv("TMPDIR");
    char path[512];
    int length = snprintf(path, sizeof(path), "%s/ohlc-concurrency-XXXXXX",
                          temporary != NULL ? temporary : "/tmp");
    if (length <= 0 || (size_t)length >= sizeof(path)) {
        return 1;
    }
    if (mkdtemp(path) == NULL) {
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
        ohlc_row initial = {0};
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
    const char* files[] = {"LOCK",
                           "CURRENT.0",
                           "CURRENT.1",
                           "catalog-000001.dat",
                           "wal-000001.log",
                           "tables/00000001/data-000001.dat",
                           "tables/00000001/data-000001.meta",
                           "tables/00000001/index-000001.dat"};
    char file[1024];
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        snprintf(file, sizeof(file), "%s/%s", path, files[i]);
        if (unlink(file) != 0) {
            atomic_store(&context.failed, true);
        }
    }
    snprintf(file, sizeof(file), "%s/tables/00000001", path);
    (void)rmdir(file);
    snprintf(file, sizeof(file), "%s/tables", path);
    (void)rmdir(file);
    (void)rmdir(path);
    if (atomic_load(&context.failed)) {
        return 1;
    }
    puts("concurrent writers, snapshots and checkpoints: OK");
    return 0;
}

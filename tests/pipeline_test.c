/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <dirent.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CHECK(expression)                                                                          \
    do {                                                                                           \
        if (!(expression)) {                                                                       \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expression);                       \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)
#define OK(expression) CHECK((expression) == OHLC_OK)

typedef struct {
    ohlc_db* db;
    uint32_t table;
    uint32_t key;
    uint32_t ticker;
    uint32_t value;
    uint64_t sequence;
    ohlc_status status;
} operation;

static void remove_directory(const char* path) {
    DIR* directory = opendir(path);
    CHECK(directory != NULL);
    struct dirent* entry;
    while ((entry = readdir(directory)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char child[1024];
        int length = snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
        CHECK(length > 0 && (size_t)length < sizeof(child));
        if (unlink(child) != 0) {
            remove_directory(child);
        }
    }
    closedir(directory);
    CHECK(rmdir(path) == 0);
}

static ohlc_row value_for(uint32_t ticker, uint32_t key) {
    ohlc_row row = {.open = -(int32_t)key,
                    .high = (int32_t)ticker,
                    .low = INT32_MIN,
                    .close = (int32_t)key,
                    .volume = ticker,
                    .amount = ((uint64_t)key << 32) | ticker,
                    .adjust_factor = UINT32_MAX};
    return row;
}

static void check_row(const uint8_t* data, uint32_t ticker, uint32_t key) {
    ohlc_row row = value_for(ticker, key);
    uint8_t encoded[OHLC_ROW_BYTES];
    ohlc_row_encode(encoded, &row);
    CHECK(memcmp(data, encoded, sizeof(encoded)) == 0);
}

static void check_query(ohlc_db* db, uint32_t table, bool cross, uint32_t first, uint32_t count) {
    ohlc_cursor* cursor = NULL;
    if (cross) {
        OK(ohlc_cross(db, table, first, &cursor));
    } else {
        OK(ohlc_series(db, table, 0, first, (uint64_t)first + count - 1, &cursor));
    }
    size_t seen = 0;
    for (;;) {
        uint8_t rows[3 * OHLC_RESULT_BYTES];
        size_t fetched = 0;
        OK(ohlc_cursor_next(cursor, rows, 3, &fetched));
        if (fetched == 0) {
            break;
        }
        for (size_t i = 0; i < fetched; i++) {
            uint32_t key = cross ? first : first + (uint32_t)seen;
            uint32_t ticker = cross ? (uint32_t)seen : 0;
            CHECK(ohlc_get_u32(rows + i * OHLC_RESULT_BYTES) == (cross ? ticker : key));
            check_row(rows + i * OHLC_RESULT_BYTES + 4, ticker, key);
            seen++;
        }
    }
    CHECK(seen == count);
    ohlc_cursor_close(cursor);
}

static void* read_tile(void* argument) {
    operation* op = argument;
    check_query(op->db, op->table, false, 0, 8);
    return NULL;
}

static void* write_row(void* argument) {
    operation* op = argument;
    uint8_t bytes[OHLC_WRITE_BYTES];
    ohlc_row row = value_for(op->ticker, op->value);
    ohlc_write_encode(bytes, op->ticker, op->key, &row);
    op->status = ohlc_write(op->db, op->table, bytes, 1, &op->sequence);
    return NULL;
}

static void* checkpoint(void* argument) {
    operation* op = argument;
    op->status = ohlc_checkpoint(op->db);
    return NULL;
}

static void check_one(ohlc_db* db, uint32_t table, uint32_t key, uint32_t value) {
    ohlc_cursor* cursor = NULL;
    OK(ohlc_series(db, table, 0, key, key, &cursor));
    uint8_t row[OHLC_RESULT_BYTES];
    size_t count = 0;
    OK(ohlc_cursor_next(cursor, row, 1, &count));
    CHECK(count == 1 && ohlc_get_u32(row) == key);
    check_row(row + 4, 0, value);
    ohlc_cursor_close(cursor);
}

static void wait_for_commits(ohlc_db* db, size_t active, size_t queued) {
    uint64_t deadline = ohlc_monotonic_ms() + 5000;
    for (;;) {
        pthread_mutex_lock(&db->commits.mutex);
        bool ready = db->commits.active == active && db->commits.count == queued;
        pthread_mutex_unlock(&db->commits.mutex);
        if (ready) {
            return;
        }
        CHECK(ohlc_monotonic_ms() < deadline);
        sched_yield();
    }
}

static void test_commit_group(ohlc_db* db, uint32_t table) {
    enum { WRITERS = 16 };
    operation operations[WRITERS];
    pthread_t threads[WRITERS];
    uint64_t syncs = atomic_load(&db->wal_syncs);
    pthread_mutex_lock(&db->writer);
    for (uint32_t i = 0; i < WRITERS; i++) {
        operations[i] = (operation){.db = db, .table = table, .key = 1000u + i, .value = 1000u + i};
        CHECK(pthread_create(&threads[i], NULL, write_row, &operations[i]) == 0);
    }
    /* One caller owns the commit slot while blocked on the writer lock.
     * All other requests must queue without holding that lock. */
    uint64_t deadline = ohlc_monotonic_ms() + 5000;
    for (;;) {
        pthread_mutex_lock(&db->commits.mutex);
        size_t queued = db->commits.count + db->commits.active;
        pthread_mutex_unlock(&db->commits.mutex);
        if (queued == WRITERS) {
            break;
        }
        CHECK(ohlc_monotonic_ms() < deadline);
        sched_yield();
    }
    pthread_mutex_unlock(&db->writer);
    for (size_t i = 0; i < WRITERS; i++) {
        pthread_join(threads[i], NULL);
        OK(operations[i].status);
        check_one(db, table, operations[i].key, operations[i].value);
        for (size_t j = 0; j < i; j++) {
            CHECK(operations[i].sequence != operations[j].sequence);
        }
    }
    CHECK(atomic_load(&db->wal_syncs) - syncs <= 2);
}

static void test_commit_queue(ohlc_db* db, uint32_t table) {
    operation owner = {.db = db, .table = table, .ticker = UINT32_MAX, .sequence = UINT64_MAX};
    operation queued[OHLC_COMMIT_QUEUE];
    pthread_t threads[OHLC_COMMIT_QUEUE];
    pthread_t caller;
    ohlc_stats before;
    ohlc_get_stats(db, &before);
    pthread_mutex_lock(&db->writer);
    CHECK(pthread_create(&caller, NULL, write_row, &owner) == 0);
    wait_for_commits(db, 1, 0);
    for (uint32_t i = 0; i < OHLC_COMMIT_QUEUE; i++) {
        queued[i] = (operation){.db = db,
                                .table = table,
                                .ticker = i == 31 ? UINT32_MAX : 0,
                                .key = 3001u + i,
                                .value = 3001u + i,
                                .sequence = UINT64_MAX};
        CHECK(pthread_create(&threads[i], NULL, write_row, &queued[i]) == 0);
        /* Waiting for admission makes the expected FIFO order deterministic. */
        wait_for_commits(db, 1, i + 1u);
    }
    operation overflow = {
        .db = db, .table = table, .key = 3065, .value = 3065, .sequence = UINT64_MAX};
    write_row(&overflow);
    CHECK(overflow.status == OHLC_BUSY && overflow.sequence == UINT64_MAX);
    pthread_mutex_unlock(&db->writer);
    CHECK(pthread_join(caller, NULL) == 0);
    CHECK(owner.status == OHLC_NOT_FOUND && owner.sequence == UINT64_MAX);
    uint64_t expected = before.commit_seq;
    for (size_t i = 0; i < OHLC_COMMIT_QUEUE; i++) {
        CHECK(pthread_join(threads[i], NULL) == 0);
        if (i == 31) {
            CHECK(queued[i].status == OHLC_NOT_FOUND && queued[i].sequence == UINT64_MAX);
        } else {
            OK(queued[i].status);
            CHECK(queued[i].sequence == ++expected);
            check_one(db, table, queued[i].key, queued[i].value);
        }
    }
    wait_for_commits(db, 0, 0);
    write_row(&overflow);
    OK(overflow.status);
    CHECK(overflow.sequence == expected + 1);
    check_one(db, table, overflow.key, overflow.value);
}

static void test_commit_sync_failure(ohlc_db* db, uint32_t table, bool worker_fails) {
    operation operations[3];
    pthread_t threads[3];
    ohlc_stats before;
    ohlc_stats after;
    ohlc_get_stats(db, &before);
    uint32_t first = worker_fails ? 4300u : 4200u;
    pthread_mutex_lock(&db->writer);
    for (uint32_t i = 0; i < 3; i++) {
        operations[i] = (operation){.db = db,
                                    .table = table,
                                    .ticker = worker_fails && i == 0 ? UINT32_MAX : 0,
                                    .key = first + i,
                                    .value = first + i,
                                    .sequence = UINT64_MAX};
        CHECK(pthread_create(&threads[i], NULL, write_row, &operations[i]) == 0);
        wait_for_commits(db, 1, i);
    }
    /* The Linux test shim returns EIO for this descriptor's sync. No I/O
     * executes while changing the environment; all writers are held above. */
    char descriptor[32];
    int length = snprintf(descriptor, sizeof(descriptor), "%d", db->wal_fd);
    CHECK(length > 0 && (size_t)length < sizeof(descriptor));
    CHECK(setenv("OHLC_TEST_FAIL_SYNC_FD", descriptor, 1) == 0);
    pthread_mutex_unlock(&db->writer);
    for (size_t i = 0; i < 3; i++) {
        CHECK(pthread_join(threads[i], NULL) == 0);
        ohlc_status expected = OHLC_OUTCOME_UNKNOWN;
        if (i == 0 && worker_fails) {
            expected = OHLC_NOT_FOUND;
        } else if (i != 0 && !worker_fails) {
            expected = OHLC_IO;
        }
        CHECK(operations[i].status == expected && operations[i].sequence == UINT64_MAX);
    }
    CHECK(unsetenv("OHLC_TEST_FAIL_SYNC_FD") == 0);
    wait_for_commits(db, 0, 0);
    ohlc_get_stats(db, &after);
    CHECK(after.commit_seq == before.commit_seq);
    write_row(&operations[0]);
    CHECK(operations[0].status == OHLC_IO && operations[0].sequence == UINT64_MAX);
    wait_for_commits(db, 0, 0);
}

static void test_checkpoint_overlap(ohlc_db* db, uint32_t table) {
    operation change = {.db = db, .table = table, .key = 2000, .value = 1};
    write_row(&change);
    OK(change.status);
    ohlc_root* before = ohlc_root_acquire(db);
    uint32_t refs = atomic_load(&before->refs);
    operation flush = {.db = db};
    pthread_t thread;
    pthread_mutex_lock(&db->storage_mutex);
    CHECK(pthread_create(&thread, NULL, checkpoint, &flush) == 0);
    uint64_t deadline = ohlc_monotonic_ms() + 5000;
    while (atomic_load(&before->refs) == refs) {
        CHECK(ohlc_monotonic_ms() < deadline);
        sched_yield();
    }
    /* The checkpoint has pinned the old root and cannot finish its storage
     * work. A hot-block write must still commit before storage is unblocked. */
    change.value = 2;
    write_row(&change);
    OK(change.status);
    uint32_t code = 0;
    uint64_t sequence = 0;
    OK(ohlc_register(db, table, (ohlc_bytes){"NEW", 3}, &code, &sequence));
    ohlc_table_definition definition = {"created_during_checkpoint", OHLC_DAY, 1, "", ""};
    ohlc_table_info added;
    OK(ohlc_table_create(db, &definition, &added));
    pthread_mutex_unlock(&db->storage_mutex);
    pthread_join(thread, NULL);
    OK(flush.status);
    CHECK(db->checkpoint_root->seq == before->seq);
    ohlc_root_release(db, before);
    check_one(db, table, 2000, 2);
}

int main(void) {
    const char* temporary = getenv("TMPDIR");
    char path[512];
    int length = snprintf(path, sizeof(path), "%s/ohlc-pipeline-XXXXXX",
                          temporary == NULL ? "/tmp" : temporary);
    CHECK(length > 0 && (size_t)length < sizeof(path) && mkdtemp(path) != NULL);
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 0;
    options.memory_limit = 128u * 1024u * 1024u;
    ohlc_db* db = NULL;
    OK(ohlc_open(path, &options, &db));
    ohlc_table_definition definition = {"bars", OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    OK(ohlc_table_create(db, &definition, &table));
    uint64_t sequence = 0;
    for (uint32_t ticker = 0; ticker < 257; ticker++) {
        char name[16];
        length = snprintf(name, sizeof(name), "S%u", ticker);
        uint32_t code = 0;
        OK(ohlc_register(db, table.id, (ohlc_bytes){name, (size_t)length}, &code, &sequence));
        CHECK(code == ticker);
    }
    size_t count = 257u * 128u;
    uint8_t* rows = malloc(count * OHLC_WRITE_BYTES);
    CHECK(rows != NULL);
    for (uint32_t key = 0; key < 128; key++) {
        for (uint32_t ticker = 0; ticker < 257; ticker++) {
            ohlc_row row = value_for(ticker, key);
            ohlc_write_encode(rows + ((size_t)key * 257u + ticker) * OHLC_WRITE_BYTES, ticker, key,
                              &row);
        }
    }
    OK(ohlc_write(db, table.id, rows, count, &sequence));
    free(rows);
    OK(ohlc_checkpoint_background(db));
    CHECK(db->checkpoint_root->seq == sequence);
    ohlc_stats before;
    ohlc_stats after;
    ohlc_get_stats(db, &before);
    check_query(db, table.id, false, 0, 128);
    ohlc_get_stats(db, &after);
    CHECK(after.disk_read_calls - before.disk_read_calls == 1);
    CHECK(after.disk_read_bytes - before.disk_read_bytes == 65536);
    before = after;
    check_query(db, table.id, true, 3, 257);
    ohlc_get_stats(db, &after);
    CHECK(after.disk_read_calls - before.disk_read_calls == 17);
    CHECK(after.disk_read_bytes - before.disk_read_bytes == 17u * 4096u);
    OK(ohlc_close(db));

    options.cache_bytes = 1024u * 1024u;
    options.read_workers = 4;
    OK(ohlc_open(path, &options, &db));
    check_query(db, table.id, true, 11, 257);
    operation read = {.db = db, .table = table.id};
    pthread_t readers[8];
    ohlc_get_stats(db, &before);
    for (size_t i = 0; i < 8; i++) {
        CHECK(pthread_create(&readers[i], NULL, read_tile, &read) == 0);
    }
    for (size_t i = 0; i < 8; i++) {
        pthread_join(readers[i], NULL);
    }
    ohlc_get_stats(db, &after);
    CHECK(after.disk_read_calls - before.disk_read_calls == 1);
    test_commit_group(db, table.id);
    uint64_t persisted = db->checkpoint_root->seq;
    OK(ohlc_checkpoint_background(db));
    CHECK(db->checkpoint_root->seq == persisted);
    test_commit_queue(db, table.id);
    test_checkpoint_overlap(db, table.id);
    OK(ohlc_close(db));

    options.read_workers = 0;
    OK(ohlc_open(path, &options, &db));
    check_one(db, table.id, 2000, 2);
    check_query(db, table.id, false, 1000, 16);
    for (uint32_t i = 0; i < OHLC_COMMIT_QUEUE; i++) {
        if (i != 31) {
            check_one(db, table.id, 3001u + i, 3001u + i);
        }
    }
    check_one(db, table.id, 3065, 3065);
    ohlc_table_info added;
    OK(ohlc_table_open(db, "created_during_checkpoint", &added));
    uint32_t code = 0;
    OK(ohlc_resolve(db, table.id, (ohlc_bytes){"NEW", 3}, &code));
    CHECK(code == 257);
    OK(ohlc_checkpoint(db));
    if (getenv("OHLC_TEST_SYNC_FAILURES") != NULL) {
        test_commit_sync_failure(db, table.id, false);
        OK(ohlc_close(db));
        OK(ohlc_open(path, &options, &db));
        /* EIO prevents acknowledgment, but the complete WAL frames written
         * by this shim remain readable and may legitimately recover. */
        check_one(db, table.id, 4200, 4200);
        test_commit_sync_failure(db, table.id, true);
        OK(ohlc_close(db));
        OK(ohlc_open(path, &options, &db));
        check_one(db, table.id, 4301, 4301);
        check_one(db, table.id, 4302, 4302);
        puts("WAL sync failure releases caller and worker ownership: OK");
    }
    OK(ohlc_close(db));
    remove_directory(path);
    puts("batched reads, shared cache loads, group commit and overlapping checkpoint: OK");
    return 0;
}

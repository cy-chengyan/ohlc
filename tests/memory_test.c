/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <dirent.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
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
    char path[512];
    ohlc_options options;
    ohlc_db* db;
    uint32_t table;
} fixture;

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

static void initialize(fixture* f, size_t memory, size_t cache) {
    const char* temporary = getenv("TMPDIR");
    int length = snprintf(f->path, sizeof(f->path), "%s/ohlc-memory-XXXXXX",
                          temporary == NULL ? "/tmp" : temporary);
    CHECK(length > 0 && (size_t)length < sizeof(f->path));
    CHECK(mkdtemp(f->path) != NULL);
    ohlc_options_init(&f->options);
    f->options.create_if_missing = true;
    f->options.memory_limit = memory;
    f->options.cache_bytes = cache;
}

static void create(fixture* f, uint32_t tickers) {
    OK(ohlc_open(f->path, &f->options, &f->db));
    ohlc_table_definition definition = {"bars", OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    OK(ohlc_table_create(f->db, &definition, &table));
    f->table = table.id;
    for (uint32_t i = 0; i < tickers; i++) {
        char name[32];
        int length = snprintf(name, sizeof(name), "S%u", i);
        uint32_t code = 0;
        uint64_t sequence = 0;
        OK(ohlc_register(f->db, f->table, (ohlc_bytes){name, (size_t)length}, &code, &sequence));
        CHECK(code == i);
    }
}

static void rows_for(uint8_t* rows, uint32_t first, uint32_t batch) {
    for (uint32_t time = 0; time < 8; time++) {
        uint32_t key = batch * 8u + time;
        ohlc_row row = {.close = (int64_t)key + 1, .flags = OHLC_FIELD_CLOSE};
        for (uint32_t ticker = 0; ticker < 16; ticker++) {
            ohlc_write_encode(rows + (time * 16u + ticker) * OHLC_WRITE_BYTES, first + ticker, key,
                              &row);
        }
    }
}

static void check_budget(ohlc_db* db) {
    CHECK(atomic_load(&db->allocator.used) <= db->allocator.limit);
    CHECK(atomic_load(&db->allocator.charged) <= db->allocator.limit);
}

static void verify_cursor(ohlc_cursor* cursor, uint32_t expected) {
    uint8_t rows[128u * OHLC_RESULT_BYTES];
    size_t count = 0;
    uint32_t seen = 0;
    do {
        OK(ohlc_cursor_next(cursor, rows, 128, &count));
        for (size_t i = 0; i < count; i++) {
            const uint8_t* result = rows + i * OHLC_RESULT_BYTES;
            ohlc_row row;
            ohlc_row_decode(result + 4, &row);
            CHECK(ohlc_get_u32(result) == seen && row.close == (int64_t)seen + 1);
            CHECK(row.flags == OHLC_FIELD_CLOSE);
            seen++;
        }
    } while (count != 0);
    CHECK(seen == expected);
}

static void verify(fixture* f, uint32_t tickers, uint32_t batches) {
    for (uint32_t ticker = 0; ticker < tickers; ticker++) {
        ohlc_cursor* cursor = NULL;
        OK(ohlc_series(f->db, f->table, ticker, 0, UINT32_MAX, &cursor));
        verify_cursor(cursor, batches * 8u);
        ohlc_cursor_close(cursor);
    }
    check_budget(f->db);
}

static uint32_t fill(fixture* f, uint32_t batch) {
    uint8_t rows[128u * OHLC_WRITE_BYTES];
    for (; batch < 10000; batch++) {
        rows_for(rows, 0, batch);
        uint64_t sequence = UINT64_MAX;
        ohlc_status status = ohlc_write(f->db, f->table, rows, 128, &sequence);
        check_budget(f->db);
        if (status == OHLC_BUSY) {
            CHECK(sequence == UINT64_MAX);
            return batch;
        }
        OK(status);
    }
    CHECK(false);
    return 0;
}

static void test_exhaustion(void) {
    fixture f = {0};
    initialize(&f, 2u * 1024u * 1024u, 0);
    create(&f, 16);
    uint8_t rows[128u * OHLC_WRITE_BYTES];
    rows_for(rows, 0, 0);
    uint64_t sequence = UINT64_MAX;
    atomic_store(&f.db->allocator.fail_system_alloc, true);
    CHECK(ohlc_write(f.db, f.table, rows, 128, &sequence) == OHLC_LIMIT);
    CHECK(sequence == UINT64_MAX);
    atomic_store(&f.db->allocator.fail_system_alloc, false);
    OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    ohlc_cursor* snapshot = NULL;
    OK(ohlc_series(f.db, f.table, 0, 0, UINT32_MAX, &snapshot));
    uint32_t batches = fill(&f, 1);
    CHECK(batches > 1);
    OK(ohlc_checkpoint(f.db));
    CHECK(f.db->checkpoint_root->seq == f.db->root->seq);
    verify_cursor(snapshot, 8);
    ohlc_cursor_close(snapshot);
    rows_for(rows, 0, batches);
    OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    batches++;
    OK(ohlc_checkpoint(f.db));
    /* Another thread may hold a named payload or query plan even when the
     * root is drained and no cursor exists. Such contention remains BUSY. */
    size_t available = f.db->allocator.limit - atomic_load(&f.db->allocator.charged);
    void* temporary = ohlc_alloc(&f.db->allocator, available - ohlc_allocation_bytes(0));
    CHECK(temporary != NULL);
    uint64_t unchanged = UINT64_MAX;
    CHECK(ohlc_write(f.db, f.table, rows, 128, &unchanged) == OHLC_BUSY);
    CHECK(unchanged == UINT64_MAX && f.db->root->seq == sequence);
    ohlc_free(&f.db->allocator, temporary);
    /* A single request exceeding the drained working set is atomic and does
     * not consume the checkpoint reservation or poison later small writes. */
    size_t count = 10000;
    uint8_t* oversized = malloc(count * OHLC_WRITE_BYTES);
    CHECK(oversized != NULL);
    ohlc_row row = {.close = 1, .flags = OHLC_FIELD_CLOSE};
    for (size_t i = 0; i < count; i++) {
        ohlc_write_encode(oversized + i * OHLC_WRITE_BYTES, 0, 100000u + (uint32_t)i, &row);
    }
    uint64_t rejected = UINT64_MAX;
    CHECK(ohlc_write(f.db, f.table, oversized, count, &rejected) == OHLC_LIMIT);
    CHECK(rejected == UINT64_MAX && f.db->root->seq == sequence);
    free(oversized);
    rows_for(rows, 0, batches);
    OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    batches++;
    OK(ohlc_checkpoint(f.db));
    CHECK(batches >= 32);
    size_t large_bytes = 32u * sizeof(rows);
    uint8_t* large = malloc(large_bytes);
    CHECK(large != NULL);
    for (uint32_t batch = 0; batch < 32; batch++) {
        rows_for(large + batch * sizeof(rows), 0, batch);
    }
    OK(ohlc_write(f.db, f.table, large, 32u * 128u, &sequence));
    free(large);
    CHECK(f.db->largest_frame == ohlc_allocation_bytes(96u + large_bytes));
    size_t recovery = f.db->recovery_reserve;
    OK(ohlc_checkpoint(f.db));
    CHECK(f.db->largest_frame == 0 && f.db->recovery_reserve < recovery);
    OK(ohlc_close(f.db));
    OK(ohlc_open(f.path, &f.options, &f.db));
    verify(&f, 16, batches);
    OK(ohlc_close(f.db));
    remove_directory(f.path);

    /* The frame fits on its own, but not alongside the minimum blocks and
     * checkpoint credit. Reject before changing reservations or writing WAL. */
    initialize(&f, 4u * 1024u * 1024u, 0);
    create(&f, 64);
    OK(ohlc_checkpoint(f.db));
    count = 32768;
    uint8_t* boundary = malloc(count * OHLC_WRITE_BYTES);
    CHECK(boundary != NULL);
    for (size_t i = 0; i < count; i++) {
        ohlc_write_encode(boundary + i * OHLC_WRITE_BYTES, (uint32_t)(i % 64u), (uint32_t)(i / 64u),
                          &row);
    }
    size_t checkpoint_reserve = f.db->checkpoint_reserve;
    size_t recovery_reserve = f.db->recovery_reserve;
    uint64_t wal_bytes = atomic_load(&f.db->wal_bytes);
    uint64_t before = f.db->root->seq;
    rejected = UINT64_MAX;
    CHECK(ohlc_write(f.db, f.table, boundary, count, &rejected) == OHLC_LIMIT);
    char names[64][16];
    ohlc_bytes tickers[64];
    for (size_t i = 0; i < 64; i++) {
        int length = snprintf(names[i], sizeof(names[i]), "S%zu", i);
        tickers[i] = (ohlc_bytes){names[i], (size_t)length};
    }
    CHECK(ohlc_write_named(f.db, f.table, tickers, 64, boundary, count, &rejected) == OHLC_LIMIT);
    CHECK(rejected == UINT64_MAX && f.db->root->seq == before);
    CHECK(atomic_load(&f.db->wal_bytes) == wal_bytes);
    CHECK(f.db->checkpoint_reserve == checkpoint_reserve);
    CHECK(f.db->recovery_reserve == recovery_reserve);
    /* Keep a successful request close to the boundary as an overestimate guard. */
    OK(ohlc_write(f.db, f.table, boundary, 24576, &sequence));
    OK(ohlc_checkpoint(f.db));
    OK(ohlc_write(f.db, f.table, boundary, 1, &sequence));
    free(boundary);
    OK(ohlc_close(f.db));
    remove_directory(f.path);
}

typedef struct {
    fixture* f;
    ohlc_status status;
} checkpoint_job;

static void* checkpoint_once(void* argument) {
    checkpoint_job* job = argument;
    job->status = ohlc_checkpoint(job->f->db);
    return NULL;
}

static void wait_checkpoint(ohlc_db* db) {
    uint64_t deadline = ohlc_monotonic_ms() + 5000;
    for (;;) {
        pthread_mutex_lock(&db->writer);
        bool active = db->checkpoint_source != NULL;
        pthread_mutex_unlock(&db->writer);
        if (active) {
            return;
        }
        CHECK(ohlc_monotonic_ms() < deadline);
        sched_yield();
    }
}

static void test_slow_checkpoint(void) {
    fixture f = {0};
    initialize(&f, 4u * 1024u * 1024u, 0);
    /* Rollover also tests that new volume metadata and slot arrays are reserved. */
    f.options.data_volume_bytes = OHLC_PAGE_BYTES + 16u * OHLC_BLOCK_BYTES;
    create(&f, 16);
    uint8_t rows[128u * OHLC_WRITE_BYTES];
    uint64_t sequence = 0;
    rows_for(rows, 0, 0);
    OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    pthread_mutex_lock(&f.db->storage_mutex);
    checkpoint_job job = {.f = &f};
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, checkpoint_once, &job) == 0);
    wait_checkpoint(f.db);
    uint32_t batches = fill(&f, 1);
    CHECK(batches > 1);
    size_t available = f.db->allocator.limit - atomic_load(&f.db->allocator.charged);
    CHECK(available >= ohlc_allocation_bytes(0));
    void* contention = ohlc_alloc(&f.db->allocator, available - ohlc_allocation_bytes(0));
    CHECK(contention != NULL);
    pthread_mutex_unlock(&f.db->storage_mutex);
    CHECK(pthread_join(thread, NULL) == 0);
    OK(job.status);
    /* Persistence succeeded but ordinary allocations could not rebase it.
     * The next checkpoint must gate every mutation and publish without rebase. */
    CHECK(f.db->root->seq > f.db->checkpoint_root->seq);
    CHECK(f.db->largest_frame == ohlc_allocation_bytes(96u + sizeof(rows)));
    CHECK(atomic_load(&f.db->memory_pressure));
    ohlc_free(&f.db->allocator, contention);
    pthread_mutex_lock(&f.db->storage_mutex);
    CHECK(pthread_create(&thread, NULL, checkpoint_once, &job) == 0);
    wait_checkpoint(f.db);
    uint64_t rejected = UINT64_MAX;
    uint32_t code = UINT32_MAX;
    CHECK(ohlc_write(f.db, f.table, rows, 128, &rejected) == OHLC_BUSY);
    CHECK(ohlc_register(f.db, f.table, (ohlc_bytes){"GATED", 5}, &code, &rejected) == OHLC_BUSY);
    ohlc_table_definition definition = {"gated", OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    CHECK(ohlc_table_create(f.db, &definition, &table) == OHLC_BUSY);
    CHECK(ohlc_table_drop(f.db, f.table, &rejected) == OHLC_BUSY);
    ohlc_bytes ticker = {"S0", 2};
    CHECK(ohlc_write_named(f.db, f.table, &ticker, 1, rows, 1, &rejected) == OHLC_BUSY);
    CHECK(rejected == UINT64_MAX && code == UINT32_MAX);
    pthread_mutex_unlock(&f.db->storage_mutex);
    CHECK(pthread_join(thread, NULL) == 0);
    OK(job.status);
    CHECK(f.db->root->seq == f.db->checkpoint_root->seq);
    rows_for(rows, 0, batches);
    OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    OK(ohlc_checkpoint(f.db));
    verify(&f, 16, batches + 1);
    OK(ohlc_close(f.db));
    remove_directory(f.path);
}

static void check_lease(ohlc_db* db) {
    CHECK(db->checkpoint_allocator.peak <= db->checkpoint_allocator.estimate);
    CHECK(db->checkpoint_allocator.shortages == 0);
    check_budget(db);
}

static void test_reservation_after_rollover(void) {
    fixture f = {0};
    initialize(&f, 8u * 1024u * 1024u, 0);
    f.options.data_volume_bytes = OHLC_PAGE_BYTES + 160u * OHLC_BLOCK_BYTES;
    create(&f, 16);
    uint8_t rows[128u * OHLC_WRITE_BYTES];
    uint64_t sequence = 0;
    for (uint32_t batch = 0; batch < 152; batch++) {
        rows_for(rows, 0, batch);
        OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    }
    pthread_mutex_lock(&f.db->storage_mutex);
    checkpoint_job job = {.f = &f};
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, checkpoint_once, &job) == 0);
    wait_checkpoint(f.db);
    for (uint32_t batch = 152; batch < 168; batch++) {
        rows_for(rows, 0, batch);
        OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    }
    size_t reserved = f.db->checkpoint_reserve;
    size_t available = f.db->allocator.limit - atomic_load(&f.db->allocator.charged);
    void* contention = ohlc_alloc(&f.db->allocator, available - ohlc_allocation_bytes(0));
    CHECK(contention != NULL);
    pthread_mutex_unlock(&f.db->storage_mutex);
    CHECK(pthread_join(thread, NULL) == 0);
    OK(job.status);
    check_lease(f.db);
    ohlc_table_files* files = ohlc_slot_get(&f.db->files, f.table - 1);
    ohlc_volume* volume = ohlc_slot_get(&files->volumes, 0);
    CHECK(atomic_load(&volume->meta_count) == 152);
    CHECK(f.db->root->seq > f.db->checkpoint_root->seq);
    CHECK(ohlc_checkpoint_bytes(f.db, f.db->root) <= reserved);
    /* Keep the ordinary budget occupied: the second checkpoint must consume
     * the already held reservation, even though the last volume is now full. */
    OK(ohlc_checkpoint(f.db));
    check_lease(f.db);
    CHECK(f.db->root->seq == sequence && f.db->checkpoint_root->seq == sequence);
    CHECK(atomic_load(&files->volume_count) > 1);
    ohlc_free(&f.db->allocator, contention);
    verify(&f, 16, 168);
    OK(ohlc_close(f.db));
    remove_directory(f.path);
}

static void test_lease_metadata_boundary(void) {
    fixture f = {0};
    initialize(&f, 8u * 1024u * 1024u, 0);
    f.options.data_volume_bytes = OHLC_PAGE_BYTES + 160u * OHLC_BLOCK_BYTES;
    create(&f, 16);
    uint8_t rows[128u * OHLC_WRITE_BYTES];
    uint64_t sequence = 0;
    for (uint32_t batch = 0; batch < 256; batch++) {
        rows_for(rows, 0, batch);
        OK(ohlc_write(f.db, f.table, rows, 128, &sequence));
    }
    OK(ohlc_checkpoint(f.db));
    check_lease(f.db);
    ohlc_table_files* files = ohlc_slot_get(&f.db->files, f.table - 1);
    CHECK(atomic_load(&files->volume_count) == 2);
    ohlc_volume* volume = ohlc_slot_get(&files->volumes, 0);
    CHECK(atomic_load(&volume->meta_count) == 160);
    CHECK(ohlc_slot_get(&volume->meta_pages, 1) != NULL);
    verify(&f, 16, 256);
    OK(ohlc_close(f.db));
    remove_directory(f.path);
}

typedef struct {
    fixture* f;
    _Atomic bool stop;
    _Atomic uint32_t busy;
} pressure_job;

typedef struct {
    pressure_job* pressure;
    uint32_t first;
} writer_job;

static void* checkpoint_loop(void* argument) {
    pressure_job* job = argument;
    while (!atomic_load(&job->stop)) {
        ohlc_checkpoint_wait(job->f->db, 5);
        OK(ohlc_checkpoint_background(job->f->db));
        check_budget(job->f->db);
    }
    return NULL;
}

static void* write_loop(void* argument) {
    writer_job* job = argument;
    fixture* f = job->pressure->f;
    uint8_t rows[128u * OHLC_WRITE_BYTES];
    uint64_t deadline = ohlc_monotonic_ms() + 30000;
    for (uint32_t batch = 0; batch < 200;) {
        rows_for(rows, job->first, batch);
        uint64_t sequence = UINT64_MAX;
        ohlc_status status = ohlc_write(f->db, f->table, rows, 128, &sequence);
        if (status == OHLC_BUSY) {
            CHECK(sequence == UINT64_MAX && ohlc_monotonic_ms() < deadline);
            atomic_fetch_add(&job->pressure->busy, 1);
            struct timespec delay = {.tv_nsec = 1000000};
            nanosleep(&delay, NULL);
            continue;
        }
        OK(status);
        batch++;
    }
    return NULL;
}

static void test_concurrent_pressure(void) {
    fixture f = {0};
    initialize(&f, 2u * 1024u * 1024u, 512u * 1024u);
    create(&f, 64);
    pressure_job pressure = {.f = &f};
    pthread_t checkpoint;
    CHECK(pthread_create(&checkpoint, NULL, checkpoint_loop, &pressure) == 0);
    writer_job jobs[4];
    pthread_t writers[4];
    for (uint32_t i = 0; i < 4; i++) {
        jobs[i] = (writer_job){.pressure = &pressure, .first = i * 16u};
        CHECK(pthread_create(&writers[i], NULL, write_loop, &jobs[i]) == 0);
    }
    for (size_t i = 0; i < 4; i++) {
        CHECK(pthread_join(writers[i], NULL) == 0);
    }
    atomic_store(&pressure.stop, true);
    CHECK(pthread_join(checkpoint, NULL) == 0);
    OK(ohlc_checkpoint(f.db));
    CHECK(f.db->checkpoint_generation > 3);
    OK(ohlc_close(f.db));
    OK(ohlc_open(f.path, &f.options, &f.db));
    verify(&f, 64, 200);
    OK(ohlc_close(f.db));
    remove_directory(f.path);
}

static void test_unreserved_wal_recovery(void) {
    fixture f = {0};
    initialize(&f, 2u * 1024u * 1024u, 0);
    int result[2];
    CHECK(pipe(result) == 0);
    pid_t child = fork();
    CHECK(child >= 0);
    if (child == 0) {
        close(result[0]);
        create(&f, 16);
        /* Build a valid WAL at the memory limit without admission reserves or
         * checkpoints, then exit before writing any database checkpoint. */
        ohlc_unreserve(&f.db->allocator, f.db->checkpoint_reserve + f.db->recovery_reserve);
        f.db->checkpoint_reserve = 0;
        f.db->recovery_reserve = 0;
        uint32_t batches = 0;
        uint8_t rows[128u * OHLC_WRITE_BYTES];
        for (; batches < 10000; batches++) {
            rows_for(rows, 0, batches);
            ohlc_root* candidate = NULL;
            ohlc_status status =
                ohlc_prepare_write(f.db, f.db->root, f.table, rows, 128, &candidate);
            if (status != OHLC_OK) {
                CHECK(status == OHLC_BUSY);
                break;
            }
            OK(ohlc_wal_append(f.db, 2, f.table, 128, candidate->seq, rows, sizeof(rows)));
            ohlc_root_publish(f.db, candidate);
        }
        CHECK(batches > 1 && batches < 10000);
        /* An interrupted first checkpoint can leave high orphan volume IDs.
         * Recovery must budget their sparse slot directory before replay. */
        char orphan[1024];
        int length = snprintf(orphan, sizeof(orphan), "%s/tables", f.path);
        CHECK(length > 0 && (size_t)length < sizeof(orphan));
        CHECK(mkdir(orphan, 0700) == 0);
        length = snprintf(orphan, sizeof(orphan), "%s/tables/00000001", f.path);
        CHECK(length > 0 && (size_t)length < sizeof(orphan));
        CHECK(mkdir(orphan, 0700) == 0);
        length = snprintf(orphan, sizeof(orphan), "%s/tables/00000001/data-065535.dat", f.path);
        CHECK(length > 0 && (size_t)length < sizeof(orphan));
        FILE* file = fopen(orphan, "wb");
        CHECK(file != NULL && fwrite("orphan", 1, 6, file) == 6 && fclose(file) == 0);
        CHECK(write(result[1], &batches, sizeof(batches)) == (ssize_t)sizeof(batches));
        _exit(0);
    }
    close(result[1]);
    uint32_t batches = 0;
    CHECK(read(result[0], &batches, sizeof(batches)) == (ssize_t)sizeof(batches));
    close(result[0]);
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    if (getenv("OHLC_TEST_MEMORY_CRASH") != NULL) {
        child = fork();
        CHECK(child >= 0);
        if (child == 0) {
            CHECK(setenv("OHLC_TEST_CRASH_ARM", f.path, 1) == 0);
            CHECK(setenv("OHLC_TEST_DATABASE", f.path, 1) == 0);
            CHECK(setenv("OHLC_TEST_CRASH_ON", "current", 1) == 0);
            (void)ohlc_open(f.path, &f.options, &f.db);
            _exit(1);
        }
        CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
              WEXITSTATUS(status) == 86);
    }
    OK(ohlc_open(f.path, &f.options, &f.db));
    f.table = 1;
    CHECK(f.db->checkpoint_root->seq > 0 && f.db->root->seq == 17u + batches);
    verify(&f, 16, batches);
    OK(ohlc_checkpoint(f.db));
    OK(ohlc_close(f.db));
    OK(ohlc_open(f.path, &f.options, &f.db));
    verify(&f, 16, batches);
    OK(ohlc_close(f.db));
    remove_directory(f.path);
}

int main(void) {
    if (getenv("OHLC_TEST_MEMORY_CRASH") == NULL) {
        test_exhaustion();
        test_slow_checkpoint();
        test_reservation_after_rollover();
        test_lease_metadata_boundary();
        test_concurrent_pressure();
    }
    test_unreserved_wal_recovery();
    puts("bounded admission, checkpoint progress and same-budget recovery: OK");
    return 0;
}

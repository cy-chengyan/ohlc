/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHECK(expression)                                                                          \
    do {                                                                                           \
        if (!(expression)) {                                                                       \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expression);                       \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)
#define OK(expression) CHECK((expression) == OHLC_OK)
#define TICKERS 32u
#define TIMES 128u

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
        struct stat info;
        CHECK(lstat(child, &info) == 0);
        if (S_ISDIR(info.st_mode)) {
            remove_directory(child);
        } else {
            CHECK(unlink(child) == 0);
        }
    }
    CHECK(closedir(directory) == 0);
    CHECK(rmdir(path) == 0);
}

static void initialize(fixture* f, size_t cache, size_t volume_blocks) {
    const char* temporary = getenv("TMPDIR");
    int length = snprintf(f->path, sizeof(f->path), "%s/ohlc-extents-XXXXXX",
                          temporary == NULL ? "/tmp" : temporary);
    CHECK(length > 0 && (size_t)length < sizeof(f->path));
    CHECK(mkdtemp(f->path) != NULL);
    ohlc_options_init(&f->options);
    f->options.create_if_missing = true;
    f->options.memory_limit = 64u * 1024u * 1024u;
    f->options.cache_bytes = cache;
    f->options.data_volume_bytes = OHLC_PAGE_BYTES + volume_blocks * OHLC_BLOCK_BYTES;
}

static uint32_t create_table(fixture* f, const char* name) {
    ohlc_table_definition definition = {name, OHLC_DAY, 1, "", ""};
    ohlc_table_info table;
    OK(ohlc_table_create(f->db, &definition, &table));
    for (uint32_t i = 0; i < TICKERS; i++) {
        char ticker[16];
        int length = snprintf(ticker, sizeof(ticker), "S%u", i);
        uint32_t code = 0;
        uint64_t sequence = 0;
        OK(ohlc_register(f->db, table.id, (ohlc_bytes){ticker, (size_t)length}, &code, &sequence));
        CHECK(code == i);
    }
    return table.id;
}

static void create(fixture* f) {
    OK(ohlc_open(f->path, &f->options, &f->db));
    f->table = create_table(f, "bars");
}

static void finish(fixture* f) {
    OK(ohlc_close(f->db));
    f->db = NULL;
    remove_directory(f->path);
}

static ohlc_row value_for(uint32_t ticker, uint32_t key, uint32_t version) {
    ohlc_row row = {.open = (int64_t)key + 1,
                    .high = INT64_MAX - (int64_t)ticker,
                    .low = -(int64_t)key - 7,
                    .close = version,
                    .volume = ticker,
                    .amount = ((uint64_t)version << 32) | key,
                    .adjust_factor = UINT64_MAX - key,
                    .reserved = -(int32_t)ticker - 1,
                    .flags = OHLC_FIELDS_ALL ^ (1u << (key % 3u))};
    return row;
}

static void write_range(fixture* f, uint32_t first, uint32_t count, uint32_t version) {
    size_t total = (size_t)count * TICKERS;
    uint8_t* rows = malloc(total * OHLC_WRITE_BYTES);
    CHECK(rows != NULL);
    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t ticker = 0; ticker < TICKERS; ticker++) {
            ohlc_row row = value_for(ticker, first + i, version);
            ohlc_write_encode(rows + ((size_t)i * TICKERS + ticker) * OHLC_WRITE_BYTES, ticker,
                              first + i, &row);
        }
    }
    uint64_t sequence = 0;
    OK(ohlc_write(f->db, f->table, rows, total, &sequence));
    free(rows);
}

static void verify_cursor(ohlc_cursor* cursor, uint32_t ticker, uint32_t first, uint32_t count,
                          const uint32_t* versions) {
    uint32_t seen = 0;
    for (;;) {
        uint8_t rows[17u * OHLC_RESULT_BYTES];
        size_t received = 0;
        OK(ohlc_cursor_next(cursor, rows, 17, &received));
        if (received == 0) {
            break;
        }
        for (size_t i = 0; i < received; i++) {
            CHECK(seen < count);
            uint32_t key = first + seen;
            const uint8_t* result = rows + i * OHLC_RESULT_BYTES;
            CHECK(ohlc_get_u32(result) == key);
            ohlc_row expected = value_for(ticker, key, versions[seen]);
            uint8_t encoded[OHLC_ROW_BYTES];
            ohlc_row_encode(encoded, &expected);
            CHECK(memcmp(encoded, result + 4, sizeof(encoded)) == 0);
            seen++;
        }
    }
    CHECK(seen == count);
}

static void verify(fixture* f, uint32_t first, uint32_t count, const uint32_t* versions) {
    for (uint32_t ticker = 0; ticker < TICKERS; ticker++) {
        ohlc_cursor* cursor = NULL;
        OK(ohlc_series(f->db, f->table, ticker, first, first + count - 1, &cursor));
        verify_cursor(cursor, ticker, first, count, versions);
        ohlc_cursor_close(cursor);
    }
}

static void verify_version(fixture* f, uint32_t version) {
    uint32_t versions[TIMES];
    for (size_t i = 0; i < TIMES; i++) {
        versions[i] = version;
    }
    verify(f, 0, TIMES, versions);
}

static ohlc_group disk_group(fixture* f, uint32_t group) {
    const ohlc_table* table = ohlc_root_table(f->db->checkpoint_root, f->table);
    const ohlc_group* entry = ohlc_group_find(table, 0, group);
    CHECK(entry != NULL && entry->flags == 0 && entry->mask != 0);
    return *entry;
}

static bool same_extent(const ohlc_group* left, const ohlc_group* right) {
    return left->volume == right->volume && left->base == right->base;
}

static ohlc_volume* get_volume(fixture* f, uint32_t id) {
    ohlc_table_files* files = ohlc_slot_get(&f->db->files, f->table - 1);
    CHECK(files != NULL);
    ohlc_volume* volume = ohlc_slot_get(&files->volumes, id - 1);
    CHECK(volume != NULL);
    return volume;
}

static size_t physical_blocks(fixture* f) {
    ohlc_table_files* files = ohlc_slot_get(&f->db->files, f->table - 1);
    CHECK(files != NULL);
    size_t blocks = 0;
    for (size_t i = 0; i < files->volume_count; i++) {
        ohlc_volume* volume = ohlc_slot_get(&files->volumes, i);
        if (volume == NULL) {
            continue;
        }
        struct stat data;
        struct stat metadata;
        CHECK(fstat(volume->data_fd, &data) == 0 && fstat(volume->meta_fd, &metadata) == 0);
        size_t count = atomic_load(&volume->meta_count);
        CHECK((uint64_t)data.st_size == OHLC_PAGE_BYTES + count * OHLC_BLOCK_BYTES);
        CHECK((uint64_t)metadata.st_size == OHLC_PAGE_BYTES + count * 32u);
        blocks += count;
    }
    return blocks;
}

/* Repeated full rewrites must rotate among three images. Cached reads use
 * the same logical coordinates at reused physical offsets, across volumes. */
static void test_convergence(void) {
    fixture f = {0};
    initialize(&f, 4u * 1024u * 1024u, 48);
    create(&f);
    uint32_t primary = f.table;
    uint32_t other = create_table(&f, "other");
    f.table = other;
    write_range(&f, 0, TIMES, 777);
    OK(ohlc_checkpoint(f.db));
    f.table = primary;
    ohlc_group first = {0};
    bool reused = false;
    for (uint32_t version = 1; version <= 24; version++) {
        write_range(&f, 0, TIMES, version);
        OK(ohlc_checkpoint(f.db));
        ohlc_group current = disk_group(&f, 0);
        if (version == 1) {
            first = current;
        } else if (same_extent(&first, &current)) {
            reused = true;
        }
        CHECK(physical_blocks(&f) == (version < 3 ? version : 3u) * 32u);
        verify_version(&f, version);
        f.table = other;
        verify_version(&f, 777);
        f.table = primary;
    }
    CHECK(reused);
    printf("convergence: rounds=24 Q=32 P=%zu cached reuse and table isolation: OK\n",
           physical_blocks(&f));
    finish(&f);
}

static void test_snapshot(void) {
    fixture f = {0};
    initialize(&f, 0, 1024);
    create(&f);
    write_range(&f, 0, TIMES, 1);
    OK(ohlc_checkpoint(f.db));
    ohlc_group first = disk_group(&f, 0);
    uint8_t before[OHLC_BLOCK_BYTES];
    OK(ohlc_read_full(get_volume(&f, first.volume)->data_fd, before, sizeof(before), first.base));
    ohlc_cursor* old = NULL;
    OK(ohlc_series(f.db, f.table, 0, 0, TIMES - 1, &old));
    for (uint32_t version = 2; version <= 6; version++) {
        write_range(&f, 0, TIMES, version);
        OK(ohlc_checkpoint(f.db));
        ohlc_group current = disk_group(&f, 0);
        CHECK(!same_extent(&first, &current));
    }
    uint8_t after[OHLC_BLOCK_BYTES];
    OK(ohlc_read_full(get_volume(&f, first.volume)->data_fd, after, sizeof(after), first.base));
    CHECK(memcmp(before, after, sizeof(before)) == 0);
    uint32_t versions[TIMES];
    for (size_t i = 0; i < TIMES; i++) {
        versions[i] = 1;
    }
    verify_cursor(old, 0, 0, TIMES, versions);
    /* A newer cursor stays open while the old image becomes reusable. */
    ohlc_cursor* recent = NULL;
    OK(ohlc_series(f.db, f.table, 0, 0, TIMES - 1, &recent));
    ohlc_cursor_close(old);
    size_t high_water = physical_blocks(&f);
    uint32_t final_version = 6;
    bool found = false;
    for (uint32_t version = 7; version <= 10; version++) {
        write_range(&f, 0, TIMES, version);
        OK(ohlc_checkpoint(f.db));
        CHECK(physical_blocks(&f) == high_water);
        ohlc_group current = disk_group(&f, 0);
        final_version = version;
        if (same_extent(&first, &current)) {
            found = true;
            break;
        }
    }
    CHECK(found);
    for (size_t i = 0; i < TIMES; i++) {
        versions[i] = 6;
    }
    verify_cursor(recent, 0, 0, TIMES, versions);
    ohlc_cursor_close(recent);
    verify_version(&f, final_version);
    puts("snapshot: pinned disk bytes unchanged; reuse with a newer cursor open: OK");
    finish(&f);
}

static void test_reopen(void) {
    fixture f = {0};
    initialize(&f, 1024u * 1024u, 1024);
    create(&f);
    for (uint32_t version = 1; version <= 3; version++) {
        write_range(&f, 0, TIMES, version);
        OK(ohlc_checkpoint(f.db));
    }
    size_t before = physical_blocks(&f);
    OK(ohlc_close(f.db));
    OK(ohlc_open(f.path, &f.options, &f.db));
    CHECK(!f.db->reuse_enabled);
    verify_version(&f, 3);
    write_range(&f, 0, TIMES, 4);
    OK(ohlc_checkpoint(f.db));
    CHECK(f.db->reuse_enabled && physical_blocks(&f) == before + 32u);
    write_range(&f, 0, TIMES, 5);
    OK(ohlc_checkpoint(f.db));
    CHECK(physical_blocks(&f) == before + 32u);
    verify_version(&f, 5);
    puts("reopen: append through the first durable checkpoint, then reuse candidates: OK");
    finish(&f);
}

/* Unequal extents exercise fragmented free runs, page boundaries and rollover.
 * Missing times remain absent even when their physical neighbors are reused. */
static void test_fragmented_extents(void) {
    fixture f = {0};
    initialize(&f, 4u * 1024u * 1024u, 193);
    create(&f);
    size_t high_water = 0;
    for (uint32_t version = 1; version <= 12; version++) {
        for (uint32_t band = 0; band < 16; band++) {
            write_range(&f, band * TIMES, (band + 1) * 8u, version);
            OK(ohlc_checkpoint(f.db));
        }
        for (uint32_t band = 0; band < 16; band++) {
            uint32_t versions[TIMES];
            for (size_t i = 0; i < TIMES; i++) {
                versions[i] = version;
            }
            verify(&f, band * TIMES, (band + 1) * 8u, versions);
            if (band < 15) {
                ohlc_cursor* missing = NULL;
                OK(ohlc_cross(f.db, f.table, band * TIMES + (band + 1) * 8u, &missing));
                uint8_t row[OHLC_RESULT_BYTES];
                size_t count = 1;
                OK(ohlc_cursor_next(missing, row, 1, &count));
                CHECK(count == 0);
                ohlc_cursor_close(missing);
            }
        }
        CHECK(physical_blocks(&f) <= 4u * 272u);
        if (version == 8) {
            high_water = physical_blocks(&f);
        } else if (version > 8) {
            CHECK(physical_blocks(&f) == high_water);
        }
    }
    printf("fragmentation: extents=1..16 blocks Q=272 P=%zu stable for final four rounds: OK\n",
           physical_blocks(&f));
    finish(&f);
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

/* Leave enough ordinary credit to record a reclaim batch, but not enough to
 * copy a group directory page during the memory-only checkpoint merge. */
static void fail_merge(fixture* f, uint32_t version) {
    atomic_store(&f->db->memory_pressure, false);
    write_range(f, 0, 8, version);
    pthread_mutex_lock(&f->db->storage_mutex);
    checkpoint_job job = {.f = f};
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, checkpoint_once, &job) == 0);
    wait_checkpoint(f->db);
    write_range(f, 0, 8, version + 1);
    size_t available = f->db->allocator.limit - atomic_load(&f->db->allocator.charged);
    CHECK(available > 4096u + ohlc_allocation_bytes(0));
    void* contention = ohlc_alloc(&f->db->allocator, available - 4096u - ohlc_allocation_bytes(0));
    CHECK(contention != NULL);
    pthread_mutex_unlock(&f->db->storage_mutex);
    CHECK(pthread_join(thread, NULL) == 0);
    OK(job.status);
    CHECK(f->db->root->seq > f->db->checkpoint_root->seq);
    CHECK(atomic_load(&f->db->memory_pressure));
    ohlc_free(&f->db->allocator, contention);
}

static void test_failed_merge(void) {
    fixture f = {0};
    initialize(&f, 0, 1024);
    create(&f);
    write_range(&f, 0, TIMES, 1);
    OK(ohlc_checkpoint(f.db));
    ohlc_group original = disk_group(&f, 0);
    fail_merge(&f, 2);
    CHECK(f.db->reclaim_batches != NULL);
    fail_merge(&f, 4);
    const ohlc_table* table = ohlc_root_table(f.db->root, f.table);
    const ohlc_group* entry = ohlc_group_find(table, 0, 0);
    CHECK(entry != NULL && entry->flags == OHLC_HOT_GROUP);
    const ohlc_hot_group* hot = (const ohlc_hot_group*)(uintptr_t)entry->base;
    CHECK(same_extent(&hot->backing, &original));
    size_t occurrences = 0;
    for (const ohlc_reclaim_batch* batch = f.db->reclaim_batches; batch != NULL;
         batch = batch->next) {
        for (size_t i = 0; i < batch->count; i++) {
            const ohlc_dead_extent* dead = &batch->extents[i];
            if (dead->table == f.table && dead->volume == original.volume &&
                dead->base == original.base) {
                CHECK(batch->barrier == 0);
                occurrences++;
            }
        }
    }
    CHECK(occurrences == 1);
    uint32_t versions[TIMES];
    for (size_t i = 0; i < TIMES; i++) {
        versions[i] = i < 8 ? 5 : 1;
    }
    verify(&f, 0, TIMES, versions);
    /* The unchanged tiles must still be read from the original backing. */
    OK(ohlc_checkpoint(f.db));
    verify(&f, 0, TIMES, versions);
    size_t high_water = physical_blocks(&f);
    for (uint32_t version = 6; version <= 12; version++) {
        write_range(&f, 0, TIMES, version);
        OK(ohlc_checkpoint(f.db));
        verify_version(&f, version);
        CHECK(physical_blocks(&f) == high_water);
    }
    puts("merge failure: two failed merges retain backing once; later reuse remains correct: OK");
    finish(&f);
}

typedef struct {
    fixture* f;
    _Atomic bool stop;
    _Atomic unsigned int ready;
    _Atomic size_t queries;
} readers_job;

static void* read_versions(void* argument) {
    readers_job* job = argument;
    atomic_fetch_add(&job->ready, 1);
    while (!atomic_load(&job->stop)) {
        ohlc_cursor* cursor = NULL;
        OK(ohlc_series(job->f->db, job->f->table, 0, 0, TIMES - 1, &cursor));
        uint32_t key = 0;
        uint32_t version = 0;
        for (;;) {
            uint8_t rows[7u * OHLC_RESULT_BYTES];
            size_t count = 0;
            OK(ohlc_cursor_next(cursor, rows, 7, &count));
            if (count == 0) {
                break;
            }
            for (size_t i = 0; i < count; i++) {
                const uint8_t* result = rows + i * OHLC_RESULT_BYTES;
                ohlc_row row;
                ohlc_row_decode(result + 4, &row);
                if (key == 0) {
                    CHECK(row.close > 0 && row.close <= 80);
                    version = (uint32_t)row.close;
                }
                ohlc_row expected = value_for(0, key, version);
                uint8_t encoded[OHLC_ROW_BYTES];
                ohlc_row_encode(encoded, &expected);
                CHECK(ohlc_get_u32(result) == key);
                CHECK(memcmp(encoded, result + 4, sizeof(encoded)) == 0);
                key++;
            }
            sched_yield();
        }
        CHECK(key == TIMES);
        ohlc_cursor_close(cursor);
        atomic_fetch_add(&job->queries, 1);
    }
    return NULL;
}

static void test_concurrent_reuse(void) {
    fixture f = {0};
    initialize(&f, 4u * 1024u * 1024u, 128);
    create(&f);
    write_range(&f, 0, TIMES, 1);
    OK(ohlc_checkpoint(f.db));
    readers_job job = {.f = &f};
    pthread_t readers[4];
    for (size_t i = 0; i < 4; i++) {
        CHECK(pthread_create(&readers[i], NULL, read_versions, &job) == 0);
    }
    while (atomic_load(&job.ready) != 4) {
        sched_yield();
    }
    size_t reused = 0;
    for (uint32_t version = 2; version <= 80; version++) {
        size_t before = physical_blocks(&f);
        write_range(&f, 0, TIMES, version);
        /* Force a commit after snapshot capture while readers are active.
         * Even identical values publish a new root and exercise rebase. */
        pthread_mutex_lock(&f.db->storage_mutex);
        checkpoint_job checkpoint = {.f = &f};
        pthread_t worker;
        CHECK(pthread_create(&worker, NULL, checkpoint_once, &checkpoint) == 0);
        wait_checkpoint(f.db);
        write_range(&f, 0, TIMES, version);
        pthread_mutex_unlock(&f.db->storage_mutex);
        CHECK(pthread_join(worker, NULL) == 0);
        OK(checkpoint.status);
        if (physical_blocks(&f) == before) {
            reused++;
        }
    }
    atomic_store(&job.stop, true);
    for (size_t i = 0; i < 4; i++) {
        CHECK(pthread_join(readers[i], NULL) == 0);
    }
    CHECK(reused > 0 && atomic_load(&job.queries) >= 4);
    verify_version(&f, 80);
    printf("concurrent reuse: readers=4 rounds=80 reused_rounds=%zu queries=%zu: OK\n", reused,
           atomic_load(&job.queries));
    finish(&f);
}

static void corrupt_newest_current(const char* path) {
    uint64_t generations[2];
    char filename[1024];
    for (unsigned int slot = 0; slot < 2; slot++) {
        int length = snprintf(filename, sizeof(filename), "%s/CURRENT.%u", path, slot);
        CHECK(length > 0 && (size_t)length < sizeof(filename));
        int fd = open(filename, O_RDONLY);
        CHECK(fd >= 0);
        uint8_t header[40];
        OK(ohlc_read_full(fd, header, sizeof(header), 0));
        generations[slot] = ohlc_get_u64(header + 32);
        CHECK(close(fd) == 0);
    }
    unsigned int newest = generations[1] > generations[0] ? 1u : 0u;
    int length = snprintf(filename, sizeof(filename), "%s/CURRENT.%u", path, newest);
    CHECK(length > 0 && (size_t)length < sizeof(filename));
    int fd = open(filename, O_RDWR);
    CHECK(fd >= 0);
    uint8_t zero = 0;
    OK(ohlc_write_full(fd, &zero, 1, 0));
    CHECK(fsync(fd) == 0 && close(fd) == 0);
}

static void test_crash(const char* phase) {
    fixture f = {0};
    initialize(&f, 0, 1024);
    pid_t child = fork();
    CHECK(child >= 0);
    if (child == 0) {
        create(&f);
        for (uint32_t version = 1; version <= 3; version++) {
            write_range(&f, 0, TIMES, version);
            OK(ohlc_checkpoint(f.db));
        }
        CHECK(physical_blocks(&f) == 96);
        write_range(&f, 0, TIMES, 4);
        CHECK(setenv("OHLC_TEST_CRASH_ARM", f.path, 1) == 0);
        CHECK(setenv("OHLC_TEST_DATABASE", f.path, 1) == 0);
        CHECK(setenv("OHLC_TEST_CRASH_ON", phase, 1) == 0);
        (void)ohlc_checkpoint(f.db);
        _exit(1);
    }
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 86);
    /* Prove the crash occurred after overwriting an old physical image. */
    char data[1024];
    int length = snprintf(data, sizeof(data), "%s/tables/00000001/data-000001.dat", f.path);
    CHECK(length > 0 && (size_t)length < sizeof(data));
    int fd = open(data, O_RDONLY);
    CHECK(fd >= 0);
    uint8_t actual[OHLC_ROW_BYTES];
    OK(ohlc_read_full(fd, actual, sizeof(actual), OHLC_PAGE_BYTES));
    CHECK(close(fd) == 0);
    ohlc_row expected = value_for(0, 0, 4);
    uint8_t encoded[OHLC_ROW_BYTES];
    ohlc_row_encode(encoded, &expected);
    CHECK(memcmp(actual, encoded, sizeof(actual)) == 0);
    f.table = 1;
    OK(ohlc_open(f.path, &f.options, &f.db));
    CHECK(physical_blocks(&f) == 96);
    verify_version(&f, 4);
    OK(ohlc_close(f.db));
    corrupt_newest_current(f.path);
    OK(ohlc_open(f.path, &f.options, &f.db));
    verify_version(&f, 4);
    /* Recovery must still publish a usable replacement for the damaged slot. */
    OK(ohlc_checkpoint(f.db));
    OK(ohlc_close(f.db));
    OK(ohlc_open(f.path, &f.options, &f.db));
    verify_version(&f, 4);
    printf("crash after %s sync: physical reuse, latest recovery and older CURRENT fallback: OK\n",
           phase);
    finish(&f);
}

int main(void) {
    if (getenv("OHLC_TEST_EXTENT_CRASH") != NULL) {
        const char* phases[] = {"data", "meta", "index", "catalog", "current", "directory"};
        for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
            test_crash(phases[i]);
        }
        return 0;
    }
    test_convergence();
    test_snapshot();
    test_reopen();
    test_fragmented_extents();
    test_failed_merge();
    test_concurrent_reuse();
    return 0;
}

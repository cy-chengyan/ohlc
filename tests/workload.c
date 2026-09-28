/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"
#include "ohlc/client.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

#define OHLC_WORKLOAD_ROWS 16384u
#define OHLC_WORKLOAD_RESERVE (UINT64_C(64) << 30)

typedef struct {
    ohlc_db* db;
    ohlc_client* client;
    ohlc_table_info table;
    const char* endpoint;
    uint32_t stocks;
    uint32_t times;
} workload;

typedef struct {
    workload* owner;
    uint32_t start;
    uint32_t count;
    uint32_t interval_ms;
    uint32_t checkpoint_every;
    _Atomic bool stop;
    pthread_t thread;
    bool started;
} writer;

static uint64_t clock_ns(clockid_t id) {
    struct timespec value;
    if (clock_gettime(id, &value) != 0) {
        perror("clock_gettime");
        exit(1);
    }
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static void require(ohlc_status status, const char* operation) {
    if (status != OHLC_OK) {
        fprintf(stderr, "%s: %s\n", operation, ohlc_status_string(status));
        exit(1);
    }
}

static uint32_t dimension(const char* text, uint32_t maximum) {
    char* end;
    errno = 0;
    unsigned long value = strtoul(text, &end, 10);
    if (errno != 0 || text[0] < '0' || text[0] > '9' || *end != '\0' || value > maximum) {
        fputs("Invalid workload dimension\n", stderr);
        exit(2);
    }
    return (uint32_t)value;
}

static ohlc_row value_for(uint32_t ticker, uint32_t time) {
    return (ohlc_row){(int32_t)ticker,  (int32_t)time,
                      -(int32_t)ticker, -(int32_t)time,
                      ticker + time,    UINT64_MAX - (uint64_t)ticker * 10000019u - time,
                      ticker ^ time};
}

static uint64_t consume(const uint8_t* records, size_t count, uint32_t stock, uint32_t start,
                        bool cross, size_t already, uint32_t expected_count) {
    if (count > expected_count - already) {
        fputs("Too many query rows\n", stderr);
        exit(1);
    }
    uint64_t digest = 0;
    for (size_t i = 0; i < count; i++) {
        const uint8_t* encoded = records + i * OHLC_RESULT_BYTES;
        uint32_t position = (uint32_t)(already + i);
        uint32_t expected_key = cross ? position : start + position;
        ohlc_row actual;
        ohlc_row_decode(encoded + 4, &actual);
        ohlc_row expected = value_for(cross ? position : stock, cross ? start : expected_key);
        if (ohlc_get_u32(encoded) != expected_key || actual.open != expected.open ||
            actual.high != expected.high || actual.low != expected.low ||
            actual.close != expected.close || actual.volume != expected.volume ||
            actual.amount != expected.amount || actual.adjust_factor != expected.adjust_factor) {
            fprintf(stderr, "Query mismatch at row %zu\n", already + i);
            exit(1);
        }
        digest += (uint32_t)actual.open + (uint64_t)(uint32_t)actual.high + (uint32_t)actual.low +
                  (uint32_t)actual.close + actual.volume + actual.amount + actual.adjust_factor;
    }
    return digest;
}

static uint64_t current_rss(void) {
#ifdef __linux__
    FILE* file = fopen("/proc/self/statm", "r");
    unsigned long long total = 0;
    unsigned long long resident = 0;
    if (file == NULL) {
        return 0;
    }
    int count = fscanf(file, "%llu %llu", &total, &resident);
    fclose(file);
    return count == 2 ? (uint64_t)resident * (uint64_t)sysconf(_SC_PAGESIZE) : 0;
#else
    return 0;
#endif
}

static void open_workload(workload* work, bool create) {
    uint64_t start = clock_ns(CLOCK_MONOTONIC);
    if (strncmp(work->endpoint, "unix:", 5) == 0 || strncmp(work->endpoint, "tcp:", 4) == 0) {
        ohlc_connection_options options;
        ohlc_connection_options_init(&options);
        options.timeout_ms = 300000;
        if (work->endpoint[0] == 'u') {
            options.socket_path = work->endpoint + 5;
        } else {
            options.port = work->endpoint + 4;
        }
        require(ohlc_client_connect(&options, &work->client), "connect");
        require(ohlc_client_table_open(work->client, "workload", &work->table), "open table");
    } else {
        ohlc_options options;
        ohlc_options_init(&options);
        options.create_if_missing = create;
        options.cache_bytes = 0;
        options.memory_limit = (size_t)4u << 30;
        options.max_query_ms = 300000;
        const char* wal_mib = getenv("OHLC_WORKLOAD_WAL_MIB");
        if (wal_mib != NULL) {
            uint32_t size = dimension(wal_mib, 1024);
            if (size < 16) {
                fputs("WAL segment size must be 16..1024 MiB\n", stderr);
                exit(2);
            }
            options.wal_segment_bytes = (uint64_t)size << 20;
        }
        require(ohlc_open(work->endpoint, &options, &work->db), "open database");
        if (create) {
            ohlc_table_definition definition = {"workload", OHLC_MINUTE, 1, "UTC",
                                                "Synthetic benchmark"};
            require(ohlc_table_create(work->db, &definition, &work->table), "create table");
        } else {
            require(ohlc_table_open(work->db, "workload", &work->table), "open table");
        }
    }
    printf("{\"event\":\"ready\",\"pid\":%ld,\"open_ns\":%" PRIu64 ",\"rss_bytes\":%" PRIu64
           ",\"allocator_bytes\":%zu}\n",
           (long)getpid(), clock_ns(CLOCK_MONOTONIC) - start, current_rss(),
           work->db != NULL ? atomic_load(&work->db->allocator.used) : 0);
    fflush(stdout);
}

static void close_workload(workload* work) {
    if (work->db != NULL) {
        require(ohlc_close(work->db), "close database");
    }
    ohlc_client_close(work->client);
}

static void check_space(const char* path) {
    struct statvfs space;
    if (statvfs(path, &space) != 0 ||
        (uint64_t)space.f_bavail * space.f_frsize < OHLC_WORKLOAD_RESERVE) {
        fputs("Benchmark stopped: less than 64 GiB free on the test filesystem\n", stderr);
        exit(1);
    }
}

static void load(workload* work, uint32_t first, uint32_t end) {
    if (first == 0 && mkdir(work->endpoint, 0700) != 0) {
        perror("A new benchmark database directory is required");
        exit(2);
    }
    check_space(work->endpoint);
    open_workload(work, first == 0);
    if (first == 0) {
        uint64_t register_start = clock_ns(CLOCK_MONOTONIC);
        for (uint32_t i = 0; i < work->stocks; i++) {
            char ticker[32];
            int size = snprintf(ticker, sizeof(ticker), "S%05u", i);
            uint32_t code;
            uint64_t sequence;
            require(ohlc_register(work->db, work->table.id, (ohlc_bytes){ticker, (size_t)size},
                                  &code, &sequence),
                    "register");
            if (code != i) {
                fputs("Unexpected ticker code\n", stderr);
                exit(1);
            }
        }
        printf("{\"event\":\"registered\",\"stocks\":%u,\"wall_ns\":%" PRIu64 "}\n", work->stocks,
               clock_ns(CLOCK_MONOTONIC) - register_start);
        fflush(stdout);
    } else {
        ohlc_stats stats;
        ohlc_get_stats(work->db, &stats);
        if (stats.ticker_count != work->stocks) {
            fputs("Ticker dimension mismatch\n", stderr);
            exit(1);
        }
        ohlc_cursor* cursor = NULL;
        require(ohlc_cross(work->db, work->table.id, first - 1, &cursor),
                "check extension boundary");
        uint8_t records[OHLC_RESULT_BYTES * 512];
        size_t rows = 0;
        for (;;) {
            size_t count;
            require(ohlc_cursor_next(cursor, records, 512, &count), "check existing rows");
            if (count == 0) {
                break;
            }
            (void)consume(records, count, 0, first - 1, true, rows, work->stocks);
            rows += count;
        }
        ohlc_cursor_close(cursor);
        if (rows != work->stocks) {
            fputs("Missing extension boundary\n", stderr);
            exit(1);
        }
    }
    uint32_t batch_times = OHLC_MAX_BATCH_ROWS / work->stocks;
    uint8_t* batch = malloc((size_t)batch_times * work->stocks * OHLC_WRITE_BYTES);
    if (batch == NULL) {
        exit(1);
    }
    uint64_t begin = clock_ns(CLOCK_MONOTONIC);
    uint64_t cpu_begin = clock_ns(CLOCK_PROCESS_CPUTIME_ID);
    uint64_t generation_ns = 0;
    uint64_t commit_ns = 0;
    uint64_t checkpoint_ns = 0;
    uint32_t next_report = first + 4096;
    for (uint32_t time = first; time < end;) {
        /* Align checkpoints to full 128-time bands to avoid measuring
         * avoidable rewrites caused by an arbitrary import batch boundary. */
        uint32_t stop = (time / 512u + 1u) * 512u;
        if (stop > end) {
            stop = end;
        }
        uint32_t limit = stop - time < batch_times ? stop : time + batch_times;
        uint64_t generated = clock_ns(CLOCK_MONOTONIC);
        size_t count = 0;
        for (uint32_t t = time; t < limit; t++) {
            for (uint32_t stock = 0; stock < work->stocks; stock++) {
                ohlc_row row = value_for(stock, t);
                ohlc_write_encode(batch + count++ * OHLC_WRITE_BYTES, stock, t, &row);
            }
        }
        generation_ns += clock_ns(CLOCK_MONOTONIC) - generated;
        uint64_t sequence;
        uint64_t started = clock_ns(CLOCK_MONOTONIC);
        require(ohlc_write(work->db, work->table.id, batch, count, &sequence), "load write");
        commit_ns += clock_ns(CLOCK_MONOTONIC) - started;
        time = limit;
        if (time == stop) {
            started = clock_ns(CLOCK_MONOTONIC);
            require(ohlc_checkpoint(work->db), "load checkpoint");
            checkpoint_ns += clock_ns(CLOCK_MONOTONIC) - started;
            check_space(work->endpoint);
        }
        if (time >= next_report || time == end) {
            printf("{\"event\":\"load_progress\",\"times\":%u,\"rows\":%" PRIu64
                   ",\"elapsed_ns\":%" PRIu64 ",\"rss_bytes\":%" PRIu64 "}\n",
                   time, (uint64_t)work->stocks * time, clock_ns(CLOCK_MONOTONIC) - begin,
                   current_rss());
            fflush(stdout);
            next_report = time + 4096;
        }
    }
    printf("{\"event\":\"load_complete\",\"stocks\":%u,\"first\":%u,\"times\":%u,"
           "\"added_rows\":%" PRIu64 ",\"wall_ns\":%" PRIu64 ",\"cpu_ns\":%" PRIu64
           ",\"generation_ns\":%" PRIu64 ",\"commit_ns\":%" PRIu64 ",\"checkpoint_ns\":%" PRIu64
           ",\"rss_bytes\":%" PRIu64 ",\"allocator_bytes\":%zu}\n",
           work->stocks, first, end, (uint64_t)(end - first) * work->stocks,
           clock_ns(CLOCK_MONOTONIC) - begin, clock_ns(CLOCK_PROCESS_CPUTIME_ID) - cpu_begin,
           generation_ns, commit_ns, checkpoint_ns, current_rss(),
           atomic_load(&work->db->allocator.used));
    free(batch);
    close_workload(work);
}

typedef struct {
    uint64_t wall_ns;
    uint64_t cpu_ns;
    uint64_t first_chunk_ns;
    uint64_t verify_ns;
    uint64_t sequence;
    uint64_t digest;
    size_t rows;
} query_measurement;

static query_measurement measure_query(workload* work, bool cross, uint32_t stock, uint32_t first,
                                       uint32_t length) {
    uint8_t local[OHLC_WORKLOAD_ROWS * OHLC_RESULT_BYTES];
    uint64_t started = clock_ns(CLOCK_MONOTONIC);
    uint64_t cpu_started = clock_ns(CLOCK_THREAD_CPUTIME_ID);
    ohlc_cursor* cursor = NULL;
    if (work->db != NULL) {
        require(cross ? ohlc_cross(work->db, work->table.id, first, &cursor)
                      : ohlc_series(work->db, work->table.id, stock, first,
                                    (uint64_t)first + length, &cursor),
                "query start");
    } else {
        require(cross ? ohlc_client_cross(work->client, work->table.id, first)
                      : ohlc_client_series(work->client, work->table.id, stock, first,
                                           (uint64_t)first + length),
                "network query start");
    }
    uint64_t digest = 0;
    uint64_t verify_ns = 0;
    uint64_t first_chunk_ns = 0;
    uint64_t sequence = 0;
    size_t total = 0;
    uint32_t expected = cross ? work->stocks : length;
    bool final = false;
    while (!final) {
        const uint8_t* records = local;
        size_t count;
        if (cursor != NULL) {
            require(ohlc_cursor_next(cursor, local, OHLC_WORKLOAD_ROWS, &count), "query next");
            sequence = ohlc_cursor_sequence(cursor);
            final = count == 0;
        } else {
            ohlc_bytes view;
            uint32_t chunk_count;
            require(ohlc_client_next(work->client, &view, &chunk_count, &sequence, &final),
                    "network query next");
            count = chunk_count;
            records = view.data;
        }
        if (first_chunk_ns == 0) {
            first_chunk_ns = clock_ns(CLOCK_MONOTONIC) - started;
        }
        uint64_t verify_start = clock_ns(CLOCK_MONOTONIC);
        digest += consume(records, count, stock, first, cross, total, expected);
        verify_ns += clock_ns(CLOCK_MONOTONIC) - verify_start;
        total += count;
    }
    ohlc_cursor_close(cursor);
    uint64_t wall_ns = clock_ns(CLOCK_MONOTONIC) - started;
    uint64_t cpu_ns = clock_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_started;
    if (total != expected) {
        fprintf(stderr, "Expected %u rows, received %zu\n", expected, total);
        exit(1);
    }
    return (query_measurement){wall_ns, cpu_ns, first_chunk_ns, verify_ns, sequence, digest, total};
}

static void query(workload* work, bool cross, uint32_t stock, uint32_t first, uint32_t length,
                  uint64_t id) {
    uint64_t read_bytes = work->db != NULL ? atomic_load(&work->db->disk_read_bytes) : 0;
    uint64_t read_calls = work->db != NULL ? atomic_load(&work->db->disk_read_calls) : 0;
    query_measurement result = measure_query(work, cross, stock, first, length);
    if (work->db != NULL) {
        read_bytes = atomic_load(&work->db->disk_read_bytes) - read_bytes;
        read_calls = atomic_load(&work->db->disk_read_calls) - read_calls;
    }
    printf("{\"event\":\"query\",\"id\":%" PRIu64 ",\"kind\":\"%s\",\"stock\":%u,"
           "\"first\":%u,\"length\":%u,\"rows\":%zu,\"wall_ns\":%" PRIu64
           ",\"first_chunk_ns\":%" PRIu64 ",\"thread_cpu_ns\":%" PRIu64 ",\"verify_ns\":%" PRIu64
           ",\"engine_read_bytes\":%" PRIu64 ",\"engine_read_calls\":%" PRIu64
           ",\"rss_bytes\":%" PRIu64 ",\"snapshot_seq\":%" PRIu64 ",\"digest\":\"%" PRIu64 "\"}\n",
           id, cross ? "cross" : "series", stock, first, length, result.rows, result.wall_ns,
           result.first_chunk_ns, result.cpu_ns, result.verify_ns, read_bytes, read_calls,
           current_rss(), result.sequence, result.digest);
    fflush(stdout);
}

#define OHLC_STRESS_SAMPLES 1000000u

typedef struct {
    uint64_t wall_ns;
    bool cross;
} latency_sample;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    uint32_t ready;
    uint64_t deadline;
    bool started;
} stress_gate;

typedef struct {
    workload connection;
    stress_gate* gate;
    latency_sample* samples;
    size_t count;
    size_t cross_count;
    uint64_t rows;
    uint64_t verify_ns;
    uint64_t finished_ns;
    uint32_t seed;
    uint32_t length;
    char kind;
    pthread_t thread;
} stress_reader;

static uint32_t random_next(uint32_t* state) {
    uint32_t value = *state;
    value ^= value << 13;
    value ^= value >> 17;
    value ^= value << 5;
    *state = value;
    return value;
}

static void* read_stress(void* argument) {
    stress_reader* reader = argument;
    workload* work = &reader->connection;
    if (work->db == NULL) {
        work->client = NULL;
        open_workload(work, false);
    }
    pthread_mutex_lock(&reader->gate->mutex);
    reader->gate->ready++;
    pthread_cond_broadcast(&reader->gate->condition);
    while (!reader->gate->started) {
        pthread_cond_wait(&reader->gate->condition, &reader->gate->mutex);
    }
    uint64_t deadline = reader->gate->deadline;
    pthread_mutex_unlock(&reader->gate->mutex);
    while (reader->count < OHLC_STRESS_SAMPLES && clock_ns(CLOCK_MONOTONIC) < deadline) {
        bool cross = reader->kind == 'X' || (reader->kind == 'M' && reader->count % 2 != 0);
        uint32_t stock = random_next(&reader->seed) % work->stocks;
        uint32_t first =
            random_next(&reader->seed) % (cross ? work->times : work->times - reader->length + 1);
        query_measurement result = measure_query(work, cross, stock, first, reader->length);
        reader->samples[reader->count++] = (latency_sample){result.wall_ns, cross};
        reader->cross_count += cross ? 1u : 0u;
        reader->rows += result.rows;
        reader->verify_ns += result.verify_ns;
    }
    reader->finished_ns = clock_ns(CLOCK_MONOTONIC);
    if (work->db == NULL) {
        ohlc_client_close(work->client);
    }
    return NULL;
}

static int latency_compare(const void* left, const void* right) {
    const latency_sample* first = left;
    const latency_sample* second = right;
    return (first->wall_ns > second->wall_ns) - (first->wall_ns < second->wall_ns);
}

static void print_percentiles(const latency_sample* samples, size_t total, size_t count,
                              bool cross) {
    uint64_t p50 = 0;
    uint64_t p95 = 0;
    uint64_t p99 = 0;
    uint64_t p999 = 0;
    size_t seen = 0;
    for (size_t i = 0; i < total; i++) {
        if (samples[i].cross != cross) {
            continue;
        }
        seen++;
        if (seen == (count * 50 + 99) / 100) {
            p50 = samples[i].wall_ns;
        }
        if (seen == (count * 95 + 99) / 100) {
            p95 = samples[i].wall_ns;
        }
        if (seen == (count * 99 + 99) / 100) {
            p99 = samples[i].wall_ns;
        }
        if (seen == (count * 999 + 999) / 1000) {
            p999 = samples[i].wall_ns;
        }
    }
    printf("{\"queries\":%zu,\"p50_ns\":%" PRIu64 ",\"p95_ns\":%" PRIu64 ",\"p99_ns\":%" PRIu64
           ",\"p999_ns\":%" PRIu64 "}",
           count, p50, p95, p99, p999);
}

static void stress(workload* work, uint64_t id, uint32_t duration_ms, uint32_t threads, char kind,
                   uint32_t length, uint32_t seed) {
    stress_gate gate = {.mutex = PTHREAD_MUTEX_INITIALIZER, .condition = PTHREAD_COND_INITIALIZER};
    stress_reader readers[32] = {0};
    latency_sample* samples = malloc((size_t)threads * OHLC_STRESS_SAMPLES * sizeof(*samples));
    if (samples == NULL) {
        fputs("Stress sample allocation failed\n", stderr);
        exit(1);
    }
    for (uint32_t i = 0; i < threads; i++) {
        readers[i].connection = *work;
        readers[i].gate = &gate;
        readers[i].samples = samples + (size_t)i * OHLC_STRESS_SAMPLES;
        readers[i].length = length;
        readers[i].kind = kind;
        readers[i].seed = seed ^ (i + 1u) * UINT32_C(2654435761);
        if (readers[i].seed == 0) {
            readers[i].seed = 1;
        }
        if (pthread_create(&readers[i].thread, NULL, read_stress, &readers[i]) != 0) {
            fputs("Stress thread creation failed\n", stderr);
            exit(1);
        }
    }
    pthread_mutex_lock(&gate.mutex);
    while (gate.ready != threads) {
        pthread_cond_wait(&gate.condition, &gate.mutex);
    }
    uint64_t read_bytes = work->db != NULL ? atomic_load(&work->db->disk_read_bytes) : 0;
    uint64_t read_calls = work->db != NULL ? atomic_load(&work->db->disk_read_calls) : 0;
    uint64_t cpu_start = clock_ns(CLOCK_PROCESS_CPUTIME_ID);
    uint64_t start = clock_ns(CLOCK_MONOTONIC);
    gate.deadline = start + (uint64_t)duration_ms * UINT64_C(1000000);
    gate.started = true;
    pthread_cond_broadcast(&gate.condition);
    pthread_mutex_unlock(&gate.mutex);
    size_t total = 0;
    size_t crosses = 0;
    uint64_t rows = 0;
    uint64_t verify_ns = 0;
    uint64_t finish = 0;
    bool sample_limit = false;
    for (uint32_t i = 0; i < threads; i++) {
        pthread_join(readers[i].thread, NULL);
        memmove(samples + total, readers[i].samples, readers[i].count * sizeof(*samples));
        total += readers[i].count;
        crosses += readers[i].cross_count;
        rows += readers[i].rows;
        verify_ns += readers[i].verify_ns;
        if (readers[i].finished_ns > finish) {
            finish = readers[i].finished_ns;
        }
        sample_limit = sample_limit || readers[i].count == OHLC_STRESS_SAMPLES;
    }
    uint64_t cpu_ns = clock_ns(CLOCK_PROCESS_CPUTIME_ID) - cpu_start;
    if (work->db != NULL) {
        read_bytes = atomic_load(&work->db->disk_read_bytes) - read_bytes;
        read_calls = atomic_load(&work->db->disk_read_calls) - read_calls;
    }
    uint64_t rss = current_rss();
    qsort(samples, total, sizeof(*samples), latency_compare);
    flockfile(stdout);
    printf("{\"event\":\"stress_complete\",\"id\":%" PRIu64
           ",\"readers\":%u,\"queries\":%zu,\"rows\":%" PRIu64 ",\"wall_ns\":%" PRIu64
           ",\"cpu_ns\":%" PRIu64 ",\"verify_ns\":%" PRIu64 ",\"rss_bytes\":%" PRIu64
           ",\"engine_read_bytes\":%" PRIu64 ",\"engine_read_calls\":%" PRIu64
           ",\"sample_limit\":%s,\"series\":",
           id, threads, total, rows, finish - start, cpu_ns, verify_ns, rss, read_bytes, read_calls,
           sample_limit ? "true" : "false");
    print_percentiles(samples, total, total - crosses, false);
    fputs(",\"cross\":", stdout);
    print_percentiles(samples, total, crosses, true);
    fputs("}\n", stdout);
    fflush(stdout);
    funlockfile(stdout);
    pthread_cond_destroy(&gate.condition);
    pthread_mutex_destroy(&gate.mutex);
    free(samples);
}

static void* write_live(void* argument) {
    writer* task = argument;
    workload* work = task->owner;
    workload connection = *work;
    connection.client = NULL;
    if (work->client != NULL) {
        open_workload(&connection, false);
    }
    uint8_t* rows = malloc((size_t)work->stocks * OHLC_WRITE_BYTES);
    if (rows == NULL) {
        exit(1);
    }
    uint64_t begin = clock_ns(CLOCK_MONOTONIC);
    uint64_t elapsed = 0;
    uint64_t maximum = 0;
    uint32_t committed = 0;
    for (uint32_t index = 0; index < task->count && !atomic_load(&task->stop); index++) {
        uint32_t time = task->start + index;
        for (uint32_t stock = 0; stock < work->stocks; stock++) {
            ohlc_row row = value_for(stock, time);
            ohlc_write_encode(rows + (size_t)stock * OHLC_WRITE_BYTES, stock, time, &row);
        }
        uint64_t sequence;
        uint64_t start = clock_ns(CLOCK_MONOTONIC);
        require(work->db != NULL
                    ? ohlc_write(work->db, work->table.id, rows, work->stocks, &sequence)
                    : ohlc_client_write(connection.client, work->table.id, rows, work->stocks,
                                        &sequence),
                "live write");
        uint64_t duration = clock_ns(CLOCK_MONOTONIC) - start;
        elapsed += duration;
        if (duration > maximum) {
            maximum = duration;
        }
        committed++;
        uint64_t checkpoint_ns = 0;
        if (work->db != NULL && task->checkpoint_every != 0 &&
            committed % task->checkpoint_every == 0) {
            start = clock_ns(CLOCK_MONOTONIC);
            require(ohlc_checkpoint(work->db), "live checkpoint");
            checkpoint_ns = clock_ns(CLOCK_MONOTONIC) - start;
        }
        printf("{\"event\":\"live_write\",\"time\":%u,\"rows\":%u,\"wall_ns\":%" PRIu64
               ",\"checkpoint_ns\":%" PRIu64 ",\"commit_seq\":%" PRIu64 "}\n",
               time, work->stocks, duration, checkpoint_ns, sequence);
        fflush(stdout);
        uint64_t due = begin + (uint64_t)committed * task->interval_ms * UINT64_C(1000000);
        while (!atomic_load(&task->stop) && clock_ns(CLOCK_MONOTONIC) < due) {
            struct timespec delay = {.tv_nsec = 1000000};
            nanosleep(&delay, NULL);
        }
    }
    printf("{\"event\":\"writer_complete\",\"batches\":%u,\"rows\":%" PRIu64
           ",\"total_commit_ns\":%" PRIu64 ",\"max_commit_ns\":%" PRIu64 "}\n",
           committed, (uint64_t)committed * work->stocks, elapsed, maximum);
    fflush(stdout);
    if (connection.client != NULL) {
        ohlc_client_close(connection.client);
    }
    free(rows);
    return NULL;
}

static void serve(workload* work) {
    open_workload(work, false);
    writer task = {.owner = work};
    atomic_init(&task.stop, false);
    char line[256];
    while (fgets(line, sizeof(line), stdin) != NULL) {
        if (strcmp(line, "STOP\n") == 0) {
            break;
        }
        if (line[0] == 'B') {
            uint64_t id;
            uint32_t duration;
            uint32_t threads;
            uint32_t length;
            uint32_t seed;
            char kind;
            if (sscanf(line, "B %" SCNu64 " %u %u %c %u %u", &id, &duration, &threads, &kind,
                       &length, &seed) != 6 ||
                duration == 0 || duration > 600000 || threads == 0 || threads > 32 || length == 0 ||
                length > work->times || (kind != 'S' && kind != 'X' && kind != 'M')) {
                fputs("Invalid stress command\n", stderr);
                exit(2);
            }
            stress(work, id, duration, threads, kind, length, seed);
            continue;
        }
        if (line[0] == 'W') {
            if (task.started ||
                sscanf(line, "W %u %u %u %u", &task.start, &task.count, &task.interval_ms,
                       &task.checkpoint_every) != 4 ||
                task.start < work->times || task.count > 10000 || task.interval_ms > 10000 ||
                task.start > INT32_MAX - task.count) {
                fputs("Invalid writer command\n", stderr);
                exit(2);
            }
            if (pthread_create(&task.thread, NULL, write_live, &task) != 0) {
                exit(1);
            }
            task.started = true;
            continue;
        }
        uint64_t id;
        uint32_t stock;
        uint32_t first;
        uint32_t length;
        char kind;
        if (sscanf(line, "%c %" SCNu64 " %u %u %u", &kind, &id, &stock, &first, &length) != 5 ||
            (kind != 'S' && kind != 'X') || stock >= work->stocks || first >= work->times ||
            (kind == 'S' && (length == 0 || length > work->times - first))) {
            fputs("Invalid query command\n", stderr);
            exit(2);
        }
        query(work, kind == 'X', stock, first, length, id);
    }
    if (task.started) {
        atomic_store(&task.stop, true);
        pthread_join(task.thread, NULL);
    }
    if (work->db != NULL && task.started) {
        require(ohlc_checkpoint(work->db), "final live checkpoint");
    }
    close_workload(work);
}

int main(int argc, char** argv) {
    if (argc != 5 && argc != 6) {
        fprintf(stderr,
                "Usage: %s load NEW_OR_EXISTING_DB STOCKS FIRST END\n"
                "       %s read DB|unix:SOCKET|tcp:PORT STOCKS TIMES\n",
                argv[0], argv[0]);
        return 2;
    }
    workload work = {.endpoint = argv[2], .stocks = dimension(argv[3], 20000)};
    if (work.stocks == 0) {
        return 2;
    }
    if (argc == 6 && strcmp(argv[1], "load") == 0) {
        uint32_t first = dimension(argv[4], 10000000);
        uint32_t end = dimension(argv[5], 10000000);
        if (end <= first || strncmp(work.endpoint, "unix:", 5) == 0 ||
            strncmp(work.endpoint, "tcp:", 4) == 0) {
            return 2;
        }
        load(&work, first, end);
    } else if (argc == 5 && strcmp(argv[1], "read") == 0) {
        work.times = dimension(argv[4], 10000000);
        if (work.times == 0) {
            return 2;
        }
        serve(&work);
    } else {
        return 2;
    }
    return 0;
}

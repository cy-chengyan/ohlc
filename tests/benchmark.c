/* SPDX-License-Identifier: Apache-2.0 */
#include "ohlc/ohlc.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

#define OHLC_BENCH_SAMPLES 100u

static void require(ohlc_status status) {
    if (status != OHLC_OK) {
        fprintf(stderr, "Benchmark failed: %s\n", ohlc_status_string(status));
        exit(1);
    }
}

static uint64_t nanoseconds(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        perror("clock_gettime");
        exit(1);
    }
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static ohlc_row make_value(uint32_t ticker, uint32_t time) {
    ohlc_row row = {(int32_t)ticker, (int32_t)time, -(int32_t)ticker,
                    -(int32_t)time,  ticker + time, UINT64_MAX - ticker - time,
                    ticker ^ time};
    return row;
}

static uint64_t value_sum(const ohlc_row* row) {
    return (uint32_t)row->open + (uint64_t)(uint32_t)row->high + (uint32_t)row->low +
           (uint32_t)row->close + row->volume + row->amount + row->adjust_factor;
}

static uint32_t key_decode(const uint8_t* bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8) | ((uint32_t)bytes[2] << 16) |
           ((uint32_t)bytes[3] << 24);
}

static int compare_u64(const void* left, const void* right) {
    uint64_t a = *(const uint64_t*)left;
    uint64_t b = *(const uint64_t*)right;
    return (a > b) - (a < b);
}

static void measure(ohlc_db* db, uint32_t table, uint32_t stocks, uint32_t times, bool cross,
                    const char* storage) {
    uint64_t latencies[OHLC_BENCH_SAMPLES];
    uint8_t output[1024 * OHLC_RESULT_BYTES];
    ohlc_stats before;
    ohlc_stats after;
    ohlc_get_stats(db, &before);
    uint64_t total_rows = 0;
    uint64_t digest = 0;
    for (uint32_t sample = 0; sample < OHLC_BENCH_SAMPLES; sample++) {
        uint32_t stock = (sample * 17u) % stocks;
        uint32_t time = (sample * 31u) % times;
        uint64_t expected_sum = 0;
        uint32_t expected_count = cross ? stocks : times;
        for (uint32_t i = 0; i < expected_count; i++) {
            ohlc_row expected = make_value(cross ? i : stock, cross ? time : i);
            expected_sum += value_sum(&expected);
        }
        uint64_t start = nanoseconds();
        ohlc_cursor* cursor = NULL;
        require(cross ? ohlc_cross(db, table, time, &cursor)
                      : ohlc_series(db, table, stock, 0, times - 1, &cursor));
        uint64_t sum = 0;
        size_t rows = 0;
        while (true) {
            size_t count = 0;
            require(ohlc_cursor_next(cursor, output, 1024, &count));
            if (count == 0) {
                break;
            }
            for (size_t i = 0; i < count; i++) {
                const uint8_t* record = output + i * OHLC_RESULT_BYTES;
                if (key_decode(record) != rows) {
                    fputs("Query ordering mismatch\n", stderr);
                    exit(1);
                }
                ohlc_row row;
                ohlc_row_decode(record + 4, &row);
                sum += value_sum(&row);
                rows++;
            }
        }
        ohlc_cursor_close(cursor);
        latencies[sample] = nanoseconds() - start;
        if (rows != expected_count || sum != expected_sum) {
            fputs("Query content mismatch\n", stderr);
            exit(1);
        }
        total_rows += rows;
        digest += sum;
    }
    ohlc_get_stats(db, &after);
    qsort(latencies, OHLC_BENCH_SAMPLES, sizeof(latencies[0]), compare_u64);
    printf("{\"query\":\"%s\",\"storage\":\"%s\",\"samples\":%u,\"rows\":%" PRIu64
           ",\"p50_us\":%.3f,\"p95_us\":%.3f,\"p99_us\":%.3f,\"read_bytes\":%" PRIu64
           ",\"read_calls\":%" PRIu64 ",\"digest\":%" PRIu64 "}\n",
           cross ? "cross" : "series", storage, OHLC_BENCH_SAMPLES, total_rows,
           (double)latencies[49] / 1000.0, (double)latencies[94] / 1000.0,
           (double)latencies[98] / 1000.0, after.disk_read_bytes - before.disk_read_bytes,
           after.disk_read_calls - before.disk_read_calls, digest);
}

static uint32_t argument_count(const char* value, uint32_t maximum) {
    errno = 0;
    char* end = NULL;
    unsigned long parsed = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0 || parsed > maximum) {
        fputs("Invalid benchmark dimension\n", stderr);
        exit(2);
    }
    return (uint32_t)parsed;
}

int main(int argc, char** argv) {
    if (argc < 2 || argc > 4) {
        fprintf(stderr, "Usage: %s NEW_DATABASE_PATH [STOCKS=256] [TIMES=2048]\n", argv[0]);
        return 2;
    }
    uint32_t stocks = argc >= 3 ? argument_count(argv[2], 20000) : 256;
    uint32_t times = argc >= 4 ? argument_count(argv[3], 1000000) : 2048;
    if (mkdir(argv[1], 0700) != 0) {
        perror("A new, dedicated database directory is required");
        return 2;
    }
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    options.cache_bytes = 0;
    ohlc_db* db = NULL;
    require(ohlc_open(argv[1], &options, &db));
    ohlc_table_definition definition = {"benchmark", OHLC_MINUTE, 1, "UTC", ""};
    ohlc_table_info table;
    require(ohlc_table_create(db, &definition, &table));
    for (uint32_t i = 0; i < stocks; i++) {
        char name[32];
        int length = snprintf(name, sizeof(name), "S%u", i);
        ohlc_bytes ticker = {name, (size_t)length};
        uint32_t code = 0;
        uint64_t sequence = 0;
        require(ohlc_register(db, table.id, ticker, &code, &sequence));
        if (code != i) {
            return 1;
        }
    }
    uint32_t batch_times = OHLC_MAX_BATCH_ROWS / stocks;
    if (batch_times > 128) {
        batch_times = 128;
    }
    size_t capacity = (size_t)batch_times * stocks;
    uint8_t* batch = malloc(capacity * OHLC_WRITE_BYTES);
    if (batch == NULL) {
        return 1;
    }
    uint64_t start = nanoseconds();
    uint32_t last_flush = 0;
    for (uint32_t first = 0; first < times; first += batch_times) {
        uint32_t end = times - first < batch_times ? times : first + batch_times;
        size_t count = 0;
        for (uint32_t time = first; time < end; time++) {
            for (uint32_t stock = 0; stock < stocks; stock++) {
                ohlc_row row = make_value(stock, time);
                ohlc_write_encode(batch + count++ * OHLC_WRITE_BYTES, stock, time, &row);
            }
        }
        uint64_t sequence = 0;
        require(ohlc_write(db, table.id, batch, count, &sequence));
        if (end - last_flush >= 512 && end != times) {
            require(ohlc_checkpoint(db));
            last_flush = end;
        }
    }
    uint64_t write_ns = nanoseconds() - start;
    free(batch);
    printf("{\"stocks\":%u,\"times\":%u,\"inserted_rows\":%" PRIu64
           ",\"durable_ingest_ms\":%.3f}\n",
           stocks, times, (uint64_t)stocks * times, (double)write_ns / 1000000.0);
    measure(db, table.id, stocks, times, false, "mixed_committed");
    measure(db, table.id, stocks, times, true, "mixed_committed");
    start = nanoseconds();
    require(ohlc_checkpoint(db));
    printf("{\"final_checkpoint_ms\":%.3f}\n", (double)(nanoseconds() - start) / 1000000.0);
    require(ohlc_close(db));
    options.create_if_missing = false;
    require(ohlc_open(argv[1], &options, &db));
    measure(db, table.id, stocks, times, false, "buffered_no_application_cache");
    measure(db, table.id, stocks, times, true, "buffered_no_application_cache");
    ohlc_stats stats;
    ohlc_get_stats(db, &stats);
    printf("{\"resident_allocator_bytes\":%" PRIu64 ",\"commit_seq\":%" PRIu64 "}\n",
           stats.memory_bytes, stats.commit_seq);
    require(ohlc_close(db));
    return 0;
}

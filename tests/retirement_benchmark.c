/* SPDX-License-Identifier: Apache-2.0 */
#include "ohlc/client.h"

#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define OHLC_BENCH_BATCH_ROWS 100000u
#define OHLC_BENCH_MAX_SECURITIES 100000u
#define OHLC_BENCH_BASE_MINUTE 29803050u

typedef struct {
    uint32_t birth;
    uint32_t end;
} lifetime;

typedef struct {
    uint32_t key;
    ohlc_row value;
} result_row;

typedef struct {
    ohlc_client* client;
    ohlc_table_info table;
    lifetime* lives;
    uint32_t securities;
    uint32_t times;
    result_row* results;
    size_t capacity;
    size_t count;
} benchmark;

static void require(bool condition, const char* message) {
    if (!condition) {
        fprintf(stderr, "%s\n", message);
        exit(EXIT_FAILURE);
    }
}

static void check(ohlc_status status, const char* operation) {
    if (status != OHLC_OK) {
        fprintf(stderr, "%s: %s\n", operation, ohlc_status_string(status));
        exit(EXIT_FAILURE);
    }
}

static uint64_t now_ns(void) {
    struct timespec value;
    require(clock_gettime(CLOCK_MONOTONIC, &value) == 0, "Clock read failed");
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static uint32_t number(const char* text, uint32_t limit) {
    require(text != NULL && *text >= '0' && *text <= '9', "Invalid number");
    errno = 0;
    char* end = NULL;
    unsigned long value = strtoul(text, &end, 10);
    require(errno == 0 && *end == '\0' && value <= limit, "Number out of range");
    return (uint32_t)value;
}

static uint64_t mix(uint64_t value) {
    value += UINT64_C(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static uint32_t time_key(uint32_t minute) {
    uint32_t session = minute / 390u;
    uint32_t day = session / 5u * 7u + session % 5u;
    return OHLC_BENCH_BASE_MINUTE + day * 1440u + minute % 390u;
}

static ohlc_row value_for(uint32_t stock, uint32_t minute) {
    uint64_t random = mix(((uint64_t)stock << 32) | minute);
    int32_t open = 10000 + (int32_t)(stock * 97u) + (int32_t)(minute % 390u);
    int32_t close = open + (int32_t)(random % 101u) - 50;
    uint32_t volume = 100u + (uint32_t)((random >> 24) % 100000u);
    return (ohlc_row){
        .open = open,
        .high = (open > close ? open : close) + (int32_t)((random >> 8) % 100u),
        .low = (open < close ? open : close) - (int32_t)((random >> 16) % 100u),
        .close = close,
        .volume = volume,
        .amount = (uint64_t)(uint32_t)((open + close) / 2) * volume,
        .adjust_factor = 1000000u,
    };
}

static void read_lifetimes(benchmark* state, const char* path) {
    FILE* source = fopen(path, "r");
    require(source != NULL, "Cannot open lifecycle fixture");
    state->lives = calloc(OHLC_BENCH_MAX_SECURITIES, sizeof(*state->lives));
    require(state->lives != NULL, "Lifecycle allocation failed");
    for (;;) {
        uint32_t stock = 0;
        lifetime item = {0};
        int fields =
            fscanf(source, "%" SCNu32 " %" SCNu32 " %" SCNu32, &stock, &item.birth, &item.end);
        if (fields == EOF) {
            require(!ferror(source), "Lifecycle read failed");
            break;
        }
        require(fields == 3 && stock == state->securities && stock < OHLC_BENCH_MAX_SECURITIES,
                "Invalid lifecycle stock sequence");
        require(item.birth < item.end && item.end <= state->times, "Invalid lifecycle interval");
        state->lives[state->securities++] = item;
    }
    require(fclose(source) == 0 && state->securities > 0, "Empty or unreadable lifecycle fixture");
    state->capacity = state->securities > state->times ? state->securities : state->times;
    state->results = calloc(state->capacity, sizeof(*state->results));
    require(state->results != NULL, "Result allocation failed");
}

static void connect_client(benchmark* state, const char* port, bool create) {
    ohlc_connection_options options;
    ohlc_connection_options_init(&options);
    options.port = port;
    options.timeout_ms = 120000;
    check(ohlc_client_connect(&options, &state->client), "Connect");
    if (create) {
        ohlc_table_definition definition = {"bars", OHLC_MINUTE, 1, "UTC", "Lifecycle benchmark"};
        check(ohlc_client_table_create(state->client, &definition, &state->table), "Create table");
    } else {
        check(ohlc_client_table_open(state->client, "bars", &state->table), "Open table");
    }
}

static bool exists_at(const benchmark* state, uint32_t stock, uint32_t minute) {
    return state->lives[stock].birth <= minute && minute < state->lives[stock].end;
}

static void register_range(benchmark* state, uint32_t first, uint32_t end) {
    require(first < end && end <= state->securities, "Invalid registration range");
    uint64_t started = now_ns();
    for (uint32_t stock = first; stock < end; stock++) {
        char ticker[24];
        int length = snprintf(ticker, sizeof(ticker), "S%05" PRIu32, stock);
        require(length > 0 && (size_t)length < sizeof(ticker), "Ticker formatting failed");
        uint32_t actual = UINT32_MAX;
        uint64_t sequence = 0;
        check(ohlc_client_register(state->client, state->table.id,
                                   (ohlc_bytes){ticker, (size_t)length}, &actual, &sequence),
              "Register ticker");
        require(actual == stock, "Unstable or unexpected ticker code");
    }
    printf("{\"operation\":\"register\",\"first\":%" PRIu32 ",\"end\":%" PRIu32
           ",\"wall_ns\":%" PRIu64 "}\n",
           first, end, now_ns() - started);
}

static void load_range(benchmark* state, uint32_t first, uint32_t end) {
    require(first < end && end <= state->times, "Invalid write interval");
    uint8_t* buffer = malloc((size_t)OHLC_BENCH_BATCH_ROWS * OHLC_WRITE_BYTES);
    require(buffer != NULL, "Write allocation failed");
    uint64_t started = now_ns();
    uint64_t request_ns = 0;
    uint64_t rows = 0;
    uint32_t pending = 0;
    uint32_t batches = 0;
    for (uint32_t minute = first; minute < end; minute++) {
        for (uint32_t stock = 0; stock < state->securities; stock++) {
            if (!exists_at(state, stock, minute)) {
                continue;
            }
            ohlc_row value = value_for(stock, minute);
            ohlc_write_encode(buffer + (size_t)pending * OHLC_WRITE_BYTES, stock, time_key(minute),
                              &value);
            pending++;
            rows++;
            if (pending == OHLC_BENCH_BATCH_ROWS) {
                uint64_t sequence = 0;
                uint64_t before = now_ns();
                check(ohlc_client_write(state->client, state->table.id, buffer, pending, &sequence),
                      "Write batch");
                request_ns += now_ns() - before;
                pending = 0;
                batches++;
            }
        }
    }
    if (pending > 0) {
        uint64_t sequence = 0;
        uint64_t before = now_ns();
        check(ohlc_client_write(state->client, state->table.id, buffer, pending, &sequence),
              "Write last batch");
        request_ns += now_ns() - before;
        batches++;
    }
    printf("{\"operation\":\"write\",\"first\":%" PRIu32 ",\"end\":%" PRIu32 ",\"rows\":%" PRIu64
           ",\"batches\":%" PRIu32 ",\"wall_ns\":%" PRIu64 ",\"request_ns\":%" PRIu64 "}\n",
           first, end, rows, batches, now_ns() - started, request_ns);
    free(buffer);
}

static uint32_t little_u32(const uint8_t* bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8) | ((uint32_t)bytes[2] << 16) |
           ((uint32_t)bytes[3] << 24);
}

static void check_row(const result_row* actual, uint32_t key, uint32_t stock, uint32_t minute) {
    ohlc_row expected = value_for(stock, minute);
    const ohlc_row* value = &actual->value;
    require(actual->key == key, "Wrong result key or ordering");
    require(value->open == expected.open && value->high == expected.high &&
                value->low == expected.low && value->close == expected.close &&
                value->volume == expected.volume && value->amount == expected.amount &&
                value->adjust_factor == expected.adjust_factor,
            "Seven-field oracle mismatch");
}

static void verify_query(const benchmark* state, bool cross, uint32_t stock, uint32_t first,
                         uint32_t length) {
    size_t position = 0;
    uint32_t begin = cross ? 0 : first;
    uint32_t end = cross ? state->securities : first + length;
    for (uint32_t index = begin; index < end; index++) {
        uint32_t security = cross ? index : stock;
        uint32_t minute = cross ? first : index;
        if (exists_at(state, security, minute)) {
            require(position < state->count, "Missing expected record");
            check_row(&state->results[position++], cross ? security : time_key(minute), security,
                      minute);
        }
    }
    require(position == state->count, "Unexpected record outside a ticker lifetime");
}

static uint64_t query(benchmark* state, bool cross, uint32_t stock, uint32_t first, uint32_t length,
                      bool report) {
    require(stock < state->securities && length > 0 && first < state->times &&
                length <= state->times - first,
            "Invalid query interval");
    ohlc_stats before = {0};
    if (report) {
        check(ohlc_client_stats(state->client, &before), "Read stats before query");
    }
    state->count = 0;
    uint64_t started = now_ns();
    ohlc_status status =
        cross ? ohlc_client_cross(state->client, state->table.id, time_key(first))
              : ohlc_client_series(state->client, state->table.id, stock, time_key(first),
                                   (uint64_t)time_key(first + length - 1u) + 1u);
    check(status, "Query start");
    bool final = false;
    while (!final) {
        ohlc_bytes bytes;
        uint32_t count = 0;
        uint64_t sequence = 0;
        check(ohlc_client_next(state->client, &bytes, &count, &sequence, &final), "Query chunk");
        require(count <= state->capacity - state->count, "Result capacity exceeded");
        const uint8_t* data = bytes.data;
        for (uint32_t i = 0; i < count; i++) {
            result_row* row = &state->results[state->count++];
            row->key = little_u32(data + (size_t)i * OHLC_RESULT_BYTES);
            ohlc_row_decode(data + (size_t)i * OHLC_RESULT_BYTES + 4, &row->value);
        }
    }
    uint64_t elapsed = now_ns() - started;
    uint64_t verification_started = now_ns();
    verify_query(state, cross, stock, first, length);
    uint64_t verification_ns = now_ns() - verification_started;
    if (report) {
        ohlc_stats after;
        check(ohlc_client_stats(state->client, &after), "Read stats after query");
        printf("{\"operation\":\"query\",\"kind\":\"%c\",\"stock\":%" PRIu32 ",\"first\":%" PRIu32
               ",\"length\":%" PRIu32 ",\"rows\":%zu,"
               "\"wall_ns\":%" PRIu64 ",\"verify_ns\":%" PRIu64 ",\"read_bytes\":%" PRIu64
               ",\"read_calls\":%" PRIu64 ",\"cache_hits\":%" PRIu64 ",\"cache_misses\":%" PRIu64
               "}\n",
               cross ? 'X' : 'S', stock, first, length, state->count, elapsed, verification_ns,
               after.disk_read_bytes - before.disk_read_bytes,
               after.disk_read_calls - before.disk_read_calls, after.cache_hits - before.cache_hits,
               after.cache_misses - before.cache_misses);
    }
    return state->count;
}

static void validate_range(benchmark* state, uint32_t first, uint32_t end) {
    require(first < end && end <= state->times, "Invalid validation interval");
    uint64_t started = now_ns();
    uint64_t rows = 0;
    for (uint32_t minute = first; minute < end; minute++) {
        rows += query(state, true, 0, minute, 1, false);
    }
    printf("{\"operation\":\"validate\",\"first\":%" PRIu32 ",\"end\":%" PRIu32
           ",\"verified_rows\":%" PRIu64 ",\"wall_ns\":%" PRIu64 "}\n",
           first, end, rows, now_ns() - started);
}

static void print_stats(benchmark* state) {
    ohlc_stats stats;
    check(ohlc_client_stats(state->client, &stats), "Read stats");
    printf("{\"operation\":\"stats\",\"tickers\":%" PRIu64 ",\"memory_bytes\":%" PRIu64
           ",\"data_bytes\":%" PRIu64 ",\"wal_bytes\":%" PRIu64 ",\"commit_seq\":%" PRIu64
           ",\"checkpoint_seq\":%" PRIu64 "}\n",
           stats.ticker_count, stats.memory_bytes, stats.data_bytes, stats.wal_bytes,
           stats.commit_seq, stats.checkpoint_seq);
}

int main(int argc, char** argv) {
    require(argc == 5, "Use PORT LIFETIMES TIMES create|open");
    (void)number(argv[1], 65535);
    benchmark state = {.times = number(argv[3], 8580)};
    require(state.times > 0, "Empty time axis");
    bool create = strcmp(argv[4], "create") == 0;
    require(create || strcmp(argv[4], "open") == 0, "Invalid open mode");
    read_lifetimes(&state, argv[2]);
    connect_client(&state, argv[1], create);
    puts("{\"operation\":\"ready\"}");
    fflush(stdout);
    char line[128];
    while (fgets(line, sizeof(line), stdin) != NULL) {
        if (strcmp(line, "STOP\n") == 0) {
            break;
        }
        char operation = '\0';
        char kind = '\0';
        uint32_t first = 0;
        uint32_t end = 0;
        uint32_t stock = 0;
        if (sscanf(line, "Q %c %" SCNu32 " %" SCNu32 " %" SCNu32, &kind, &stock, &first, &end) ==
            4) {
            require(kind == 'X' || kind == 'S', "Invalid query kind");
            (void)query(&state, kind == 'X', stock, first, end, true);
        } else if (sscanf(line, "%c %" SCNu32 " %" SCNu32, &operation, &first, &end) == 3) {
            if (operation == 'R') {
                register_range(&state, first, end);
            } else if (operation == 'W') {
                load_range(&state, first, end);
            } else if (operation == 'V') {
                validate_range(&state, first, end);
            } else {
                require(false, "Unknown interval operation");
            }
        } else if (strcmp(line, "T\n") == 0) {
            print_stats(&state);
        } else if (strcmp(line, "C\n") == 0) {
            uint64_t started = now_ns();
            check(ohlc_client_checkpoint(state.client), "Checkpoint");
            printf("{\"operation\":\"checkpoint\",\"wall_ns\":%" PRIu64 "}\n", now_ns() - started);
        } else {
            require(false, "Invalid driver command");
        }
        fflush(stdout);
    }
    require(!ferror(stdin), "Driver command read failed");
    ohlc_client_close(state.client);
    free(state.results);
    free(state.lives);
    return EXIT_SUCCESS;
}

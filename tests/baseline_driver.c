/* SPDX-License-Identifier: Apache-2.0 */
#include "ohlc/client.h"

#include <curl/curl.h>
#include <mysql.h>

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define BATCH_ROWS 100000u
#define MAX_ROWS 10000u
#define HTTP_BYTES (64u * 1024u * 1024u)
#define BASE_MINUTE 29803050u

typedef struct {
    uint32_t key;
    ohlc_row value;
} result_row;

typedef struct {
    char* data;
    size_t size;
    size_t capacity;
} byte_buffer;

typedef struct {
    const char* data;
    size_t size;
    size_t position;
} bulk_input;

typedef struct {
    const char* backend;
    uint32_t stocks;
    uint32_t times;
    unsigned port;
    ohlc_client* ohlc;
    ohlc_table_info table;
    MYSQL* mysql;
    MYSQL_STMT* series;
    MYSQL_STMT* cross;
    CURL* http;
    struct curl_slist* headers;
    byte_buffer response;
    bulk_input input;
    result_row* rows;
    size_t count;
    uint64_t payload_bytes;
} connection;

static void require(bool condition, const char* message) {
    if (!condition) {
        fprintf(stderr, "%s\n", message);
        exit(1);
    }
}

static void require_ohlc(ohlc_status status, const char* message) {
    if (status != OHLC_OK) {
        fprintf(stderr, "%s: %s\n", message, ohlc_status_string(status));
        exit(1);
    }
}

static uint64_t now_ns(void) {
    struct timespec value;
    require(clock_gettime(CLOCK_MONOTONIC, &value) == 0, "clock_gettime failed");
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static uint64_t number(const char* text, uint64_t limit) {
    require(text != NULL && *text >= '0' && *text <= '9', "Invalid unsigned integer");
    errno = 0;
    char* end = NULL;
    unsigned long long value = strtoull(text, &end, 10);
    require(errno == 0 && *end == '\0' && value <= limit, "Integer out of range");
    return (uint64_t)value;
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
    return BASE_MINUTE + day * 1440u + minute % 390u;
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

static FILE* open_file(const char* directory, const char* name, const char* mode) {
    char path[4096];
    int length = snprintf(path, sizeof(path), "%s/%s", directory, name);
    require(length > 0 && (size_t)length < sizeof(path), "Path too long");
    FILE* file = fopen(path, mode);
    require(file != NULL, path);
    return file;
}

static void generate(const char* directory, uint32_t stocks, uint32_t times) {
    FILE* binary = open_file(directory, "bars.bin", "wbx");
    FILE* tsv = open_file(directory, "bars.tsv", "wx");
    FILE* line = open_file(directory, "bars.lp", "wx");
    FILE* index = open_file(directory, "batches.tsv", "wx");
    uint64_t sums[7] = {0};
    uint64_t total = (uint64_t)stocks * times;
    uint64_t started = now_ns();
    for (uint64_t first = 0; first < total; first += BATCH_ROWS) {
        uint64_t remaining = total - first;
        uint32_t count = remaining < BATCH_ROWS ? (uint32_t)remaining : BATCH_ROWS;
        off_t binary_start = ftello(binary);
        off_t tsv_start = ftello(tsv);
        off_t line_start = ftello(line);
        require(binary_start >= 0 && tsv_start >= 0 && line_start >= 0, "ftello failed");
        for (uint32_t position = 0; position < count; position++) {
            uint64_t ordinal = first + position;
            uint32_t stock = (uint32_t)(ordinal % stocks);
            uint32_t minute = (uint32_t)(ordinal / stocks);
            uint32_t key = time_key(minute);
            ohlc_row row = value_for(stock, minute);
            uint8_t encoded[OHLC_WRITE_BYTES];
            ohlc_write_encode(encoded, stock, key, &row);
            require(fwrite(encoded, sizeof(encoded), 1, binary) == 1, "Binary write failed");
            require(fprintf(tsv,
                            "%" PRIu32 "\t%" PRIu32 "\t%" PRId32 "\t%" PRId32 "\t%" PRId32
                            "\t%" PRId32 "\t%" PRIu32 "\t%" PRIu64 "\t%" PRIu32 "\n",
                            stock, key, row.open, row.high, row.low, row.close, row.volume,
                            row.amount, row.adjust_factor) > 0,
                    "TSV write failed");
            require(fprintf(line,
                            "bars,ticker=%05" PRIu32 " open=%" PRId32 "i,high=%" PRId32
                            "i,low=%" PRId32 "i,close=%" PRId32 "i,volume=%" PRIu32
                            "u,amount=%" PRIu64 "u,adjust_factor=%" PRIu32 "u %" PRIu64 "\n",
                            stock, row.open, row.high, row.low, row.close, row.volume, row.amount,
                            row.adjust_factor, (uint64_t)key * 60u) > 0,
                    "Line protocol write failed");
            sums[0] += (uint32_t)row.open;
            sums[1] += (uint32_t)row.high;
            sums[2] += (uint32_t)row.low;
            sums[3] += (uint32_t)row.close;
            sums[4] += row.volume;
            sums[5] += row.amount;
            sums[6] += row.adjust_factor;
        }
        off_t binary_end = ftello(binary);
        off_t tsv_end = ftello(tsv);
        off_t line_end = ftello(line);
        require(binary_end >= binary_start && tsv_end >= tsv_start && line_end >= line_start,
                "File offset failure");
        require(fprintf(index,
                        "%" PRIu32 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64
                        "\t%" PRIu64 "\n",
                        count, (uint64_t)binary_start, (uint64_t)(binary_end - binary_start),
                        (uint64_t)tsv_start, (uint64_t)(tsv_end - tsv_start), (uint64_t)line_start,
                        (uint64_t)(line_end - line_start)) > 0,
                "Batch index write failed");
    }
    FILE* files[] = {binary, tsv, line, index};
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        require(fflush(files[i]) == 0 && fsync(fileno(files[i])) == 0, "Input sync failed");
        require(fclose(files[i]) == 0, "Input close failed");
    }
    printf(
        "{\"event\":\"generated\",\"rows\":%" PRIu64 ",\"wall_ns\":%" PRIu64 ",\"sums\":[%" PRIu64
        ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "]}\n",
        total, now_ns() - started, sums[0], sums[1], sums[2], sums[3], sums[4], sums[5], sums[6]);
}

static void reserve(byte_buffer* buffer, size_t needed) {
    require(needed <= HTTP_BYTES, "HTTP or batch buffer limit exceeded");
    if (needed > buffer->capacity) {
        size_t capacity = buffer->capacity == 0 ? 65536u : buffer->capacity;
        while (capacity < needed) {
            capacity *= 2u;
        }
        char* data = realloc(buffer->data, capacity);
        require(data != NULL, "Buffer allocation failed");
        buffer->data = data;
        buffer->capacity = capacity;
    }
}

static size_t receive_http(char* data, size_t size, size_t count, void* context) {
    byte_buffer* buffer = context;
    if (size != 0 && count > (HTTP_BYTES - buffer->size - 1u) / size) {
        return 0;
    }
    size_t bytes = size * count;
    reserve(buffer, buffer->size + bytes + 1u);
    memcpy(buffer->data + buffer->size, data, bytes);
    buffer->size += bytes;
    buffer->data[buffer->size] = '\0';
    return bytes;
}

static void http_request(connection* client, const char* path, const void* data, size_t size) {
    char url[1024];
    int length = snprintf(url, sizeof(url), "http://127.0.0.1:%u%s", client->port, path);
    require(length > 0 && (size_t)length < sizeof(url), "URL too long");
    client->response.size = 0;
    curl_easy_setopt(client->http, CURLOPT_URL, url);
    curl_easy_setopt(client->http, CURLOPT_POSTFIELDS, data);
    curl_easy_setopt(client->http, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)size);
    CURLcode result = curl_easy_perform(client->http);
    require(result == CURLE_OK, curl_easy_strerror(result));
    long status = 0;
    curl_easy_getinfo(client->http, CURLINFO_RESPONSE_CODE, &status);
    if (status < 200 || status >= 300) {
        fprintf(stderr, "HTTP status %ld: %.*s\n", status, 1000,
                client->response.data != NULL ? client->response.data : "");
        exit(1);
    }
    client->payload_bytes = client->response.size;
}

static int local_init(void** output, const char* filename, void* userdata) {
    if (strcmp(filename, "ohlc-benchmark-batch") != 0) {
        return 1;
    }
    bulk_input* input = userdata;
    input->position = 0;
    *output = input;
    return 0;
}

static int local_read(void* context, char* buffer, unsigned int capacity) {
    bulk_input* input = context;
    size_t remaining = input->size - input->position;
    size_t count = remaining < capacity ? remaining : capacity;
    if (count > INT_MAX) {
        count = INT_MAX;
    }
    memcpy(buffer, input->data + input->position, count);
    input->position += count;
    return (int)count;
}

static void local_end(void* context) {
    (void)context;
}

static int local_error(void* context, char* output, unsigned int capacity) {
    (void)context;
    if (capacity != 0) {
        snprintf(output, capacity, "Invalid benchmark input request");
    }
    return 1;
}

static void open_connection(connection* client, bool loading) {
    client->rows = calloc(MAX_ROWS, sizeof(*client->rows));
    require(client->rows != NULL, "Result allocation failed");
    if (strcmp(client->backend, "ohlc") == 0) {
        ohlc_connection_options options;
        ohlc_connection_options_init(&options);
        char port[16];
        snprintf(port, sizeof(port), "%u", client->port);
        options.port = port;
        options.timeout_ms = 120000;
        require_ohlc(ohlc_client_connect(&options, &client->ohlc), "OHLC connect");
        if (loading) {
            ohlc_table_definition definition = {"bars", OHLC_MINUTE, 1, "UTC",
                                                "Baseline edition 1"};
            require_ohlc(ohlc_client_table_create(client->ohlc, &definition, &client->table),
                         "OHLC create");
            for (uint32_t i = 0; i < client->stocks; i++) {
                char ticker[16];
                int length = snprintf(ticker, sizeof(ticker), "S%05" PRIu32, i);
                uint32_t code = UINT32_MAX;
                uint64_t sequence = 0;
                require_ohlc(ohlc_client_register(client->ohlc,
                                                  (ohlc_bytes){ticker, (size_t)length}, &code,
                                                  &sequence),
                             "OHLC register");
                require(code == i, "Unexpected ticker code");
            }
        } else {
            require_ohlc(ohlc_client_table_open(client->ohlc, "bars", &client->table),
                         "OHLC table");
        }
        return;
    }
    if (strcmp(client->backend, "mysql") == 0) {
        client->mysql = mysql_init(NULL);
        require(client->mysql != NULL, "MySQL allocation failed");
        unsigned int enabled = 1;
        unsigned int connect_timeout = 10;
        unsigned int query_timeout = 120;
        mysql_options(client->mysql, MYSQL_OPT_LOCAL_INFILE, &enabled);
        mysql_options(client->mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
        mysql_options(client->mysql, MYSQL_OPT_READ_TIMEOUT, &query_timeout);
        mysql_options(client->mysql, MYSQL_OPT_WRITE_TIMEOUT, &query_timeout);
        require(mysql_real_connect(client->mysql, "127.0.0.1", "root", "", "baseline", client->port,
                                   NULL, CLIENT_LOCAL_FILES) != NULL,
                mysql_error(client->mysql));
        require(mysql_get_ssl_cipher(client->mysql) == NULL,
                "This baseline requires plaintext loopback");
        mysql_set_local_infile_handler(client->mysql, local_init, local_read, local_end,
                                       local_error, &client->input);
        if (!loading) {
            const char* queries[] = {
                "SELECT time_key,`open`,high,low,`close`,volume,amount,adjust_factor FROM bars "
                "WHERE ticker=? AND time_key>=? AND time_key<? ORDER BY time_key",
                "SELECT ticker,`open`,high,low,`close`,volume,amount,adjust_factor FROM bars "
                "WHERE time_key=?",
            };
            MYSQL_STMT** statements[] = {&client->series, &client->cross};
            for (size_t i = 0; i < 2; i++) {
                *statements[i] = mysql_stmt_init(client->mysql);
                require(*statements[i] != NULL, "MySQL statement allocation failed");
                require(mysql_stmt_prepare(*statements[i], queries[i],
                                           (unsigned long)strlen(queries[i])) == 0,
                        mysql_stmt_error(*statements[i]));
            }
        }
        return;
    }
    require(strcmp(client->backend, "clickhouse") == 0 || strcmp(client->backend, "influxdb") == 0,
            "Unknown backend");
    client->http = curl_easy_init();
    require(client->http != NULL, "HTTP allocation failed");
    client->headers = curl_slist_append(client->headers, "Expect:");
    client->headers = curl_slist_append(client->headers, "Accept-Encoding: identity");
    if (strcmp(client->backend, "influxdb") == 0) {
        const char* token = getenv("OHLC_BENCH_INFLUX_TOKEN");
        require(token != NULL && strlen(token) < 2048, "Missing test InfluxDB token");
        char authorization[2100];
        snprintf(authorization, sizeof(authorization), "Authorization: Token %s", token);
        client->headers = curl_slist_append(client->headers, authorization);
        client->headers = curl_slist_append(client->headers, "Accept: application/csv");
        client->headers = curl_slist_append(
            client->headers, loading ? "Content-Type: text/plain"
                                     : "Content-Type: application/x-www-form-urlencoded");
    }
    curl_easy_setopt(client->http, CURLOPT_HTTPHEADER, client->headers);
    curl_easy_setopt(client->http, CURLOPT_WRITEFUNCTION, receive_http);
    curl_easy_setopt(client->http, CURLOPT_WRITEDATA, &client->response);
    curl_easy_setopt(client->http, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(client->http, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(client->http, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(client->http, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(client->http, CURLOPT_TIMEOUT, 120L);
}

static void close_connection(connection* client) {
    ohlc_client_close(client->ohlc);
    if (client->series != NULL) {
        mysql_stmt_close(client->series);
        mysql_stmt_close(client->cross);
    }
    if (client->mysql != NULL) {
        mysql_close(client->mysql);
    }
    curl_easy_cleanup(client->http);
    curl_slist_free_all(client->headers);
    free(client->response.data);
    free(client->rows);
}

static void load(connection* client, const char* directory) {
    bool mysql = strcmp(client->backend, "mysql") == 0;
    bool influx = strcmp(client->backend, "influxdb") == 0;
    bool ohlc = strcmp(client->backend, "ohlc") == 0;
    FILE* input = open_file(directory, mysql ? "bars.tsv" : influx ? "bars.lp" : "bars.bin", "rb");
    FILE* index = open_file(directory, "batches.tsv", "r");
    byte_buffer buffer = {0};
    const char* prefix = "INSERT INTO bars FORMAT RowBinary\n";
    size_t prefix_size = strcmp(client->backend, "clickhouse") == 0 ? strlen(prefix) : 0;
    uint64_t started = now_ns();
    uint64_t rows = 0;
    uint64_t request_ns = 0;
    uint32_t batch = 0;
    for (;;) {
        uint32_t count = 0;
        uint64_t offsets[6];
        int fields = fscanf(
            index,
            "%" SCNu32 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64,
            &count, &offsets[0], &offsets[1], &offsets[2], &offsets[3], &offsets[4], &offsets[5]);
        if (fields == EOF) {
            require(!ferror(index), "Batch index read failed");
            break;
        }
        require(fields == 7 && count > 0 && count <= BATCH_ROWS, "Invalid batch index");
        size_t column = mysql ? 2u : influx ? 4u : 0u;
        require(offsets[column] <= INT64_MAX && offsets[column + 1] < HTTP_BYTES - prefix_size,
                "Invalid input range");
        size_t bytes = (size_t)offsets[column + 1];
        reserve(&buffer, prefix_size + bytes);
        if (prefix_size != 0) {
            memcpy(buffer.data, prefix, prefix_size);
        }
        require(fseeko(input, (off_t)offsets[column], SEEK_SET) == 0, "Input seek failed");
        require(fread(buffer.data + prefix_size, 1, bytes, input) == bytes, "Input read failed");
        uint64_t begin = now_ns();
        if (ohlc) {
            require(bytes == (size_t)count * OHLC_WRITE_BYTES, "Invalid OHLC batch length");
            uint64_t sequence = 0;
            require_ohlc(
                ohlc_client_write(client->ohlc, client->table.id, buffer.data, count, &sequence),
                "OHLC write");
        } else if (mysql) {
            client->input = (bulk_input){buffer.data, bytes, 0};
            require(mysql_query(client->mysql,
                                "LOAD DATA LOCAL INFILE 'ohlc-benchmark-batch' INTO TABLE bars "
                                "FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'") == 0,
                    mysql_error(client->mysql));
            require(mysql_affected_rows(client->mysql) == count &&
                        mysql_warning_count(client->mysql) == 0,
                    "MySQL import count or warning mismatch");
        } else {
            const char* path = influx ? "/api/v2/write?org=ohlc-baseline&bucket=bars&precision=s"
                                      : "/?database=baseline&async_insert=0";
            http_request(client, path, buffer.data, prefix_size + bytes);
        }
        uint64_t elapsed = now_ns() - begin;
        request_ns += elapsed;
        rows += count;
        printf("{\"event\":\"batch\",\"batch\":%" PRIu32 ",\"rows\":%" PRIu32
               ",\"total_rows\":%" PRIu64 ",\"request_ns\":%" PRIu64 "}\n",
               batch++, count, rows, elapsed);
        fflush(stdout);
    }
    require(rows == (uint64_t)client->stocks * client->times, "Imported row count mismatch");
    printf("{\"event\":\"loaded\",\"rows\":%" PRIu64 ",\"wall_ns\":%" PRIu64
           ",\"request_ns\":%" PRIu64 "}\n",
           rows, now_ns() - started, request_ns);
    free(buffer.data);
    fclose(index);
    fclose(input);
}

static uint32_t little_u32(const uint8_t* value) {
    return (uint32_t)value[0] | ((uint32_t)value[1] << 8) | ((uint32_t)value[2] << 16) |
           ((uint32_t)value[3] << 24);
}

static void append_binary(connection* client, const uint8_t* bytes, size_t count) {
    require(count <= MAX_ROWS - client->count, "Too many result rows");
    for (size_t i = 0; i < count; i++) {
        result_row* row = &client->rows[client->count++];
        row->key = little_u32(bytes + i * OHLC_RESULT_BYTES);
        ohlc_row_decode(bytes + i * OHLC_RESULT_BYTES + 4u, &row->value);
    }
}

static void query_ohlc(connection* client, bool cross, uint32_t stock, uint32_t first,
                       uint32_t length) {
    ohlc_status status =
        cross ? ohlc_client_cross(client->ohlc, client->table.id, time_key(first))
              : ohlc_client_series(client->ohlc, client->table.id, stock, time_key(first),
                                   (uint64_t)time_key(first + length - 1u) + 1u);
    require_ohlc(status, "OHLC query");
    bool final = false;
    while (!final) {
        ohlc_bytes bytes;
        uint32_t count = 0;
        uint64_t sequence = 0;
        require_ohlc(ohlc_client_next(client->ohlc, &bytes, &count, &sequence, &final),
                     "OHLC next");
        append_binary(client, bytes.data, count);
        client->payload_bytes += bytes.size;
    }
}

static void query_mysql(connection* client, bool cross, uint32_t stock, uint32_t first,
                        uint32_t length) {
    MYSQL_STMT* statement = cross ? client->cross : client->series;
    uint32_t parameters[] = {stock, time_key(first), time_key(first + length - 1u) + 1u};
    MYSQL_BIND input[3] = {0};
    size_t parameter_count = cross ? 1u : 3u;
    for (size_t i = 0; i < parameter_count; i++) {
        input[i].buffer_type = MYSQL_TYPE_LONG;
        input[i].buffer = &parameters[cross ? 1u : i];
        input[i].is_unsigned = true;
    }
    require(mysql_stmt_bind_param(statement, input) == 0, mysql_stmt_error(statement));
    require(mysql_stmt_execute(statement) == 0, mysql_stmt_error(statement));
    result_row value = {0};
    void* values[] = {&value.key,          &value.value.open,         &value.value.high,
                      &value.value.low,    &value.value.close,        &value.value.volume,
                      &value.value.amount, &value.value.adjust_factor};
    MYSQL_BIND output[8] = {0};
    my_bool nulls[8] = {0};
    my_bool errors[8] = {0};
    for (size_t i = 0; i < 8; i++) {
        output[i].buffer_type = i == 6 ? MYSQL_TYPE_LONGLONG : MYSQL_TYPE_LONG;
        output[i].buffer = values[i];
        output[i].buffer_length = i == 6 ? sizeof(uint64_t) : sizeof(uint32_t);
        output[i].is_unsigned = i == 0 || i >= 5;
        output[i].is_null = &nulls[i];
        output[i].error = &errors[i];
    }
    require(mysql_stmt_bind_result(statement, output) == 0, mysql_stmt_error(statement));
    for (;;) {
        int status = mysql_stmt_fetch(statement);
        if (status == MYSQL_NO_DATA) {
            break;
        }
        require(status == 0, mysql_stmt_error(statement));
        for (size_t i = 0; i < 8; i++) {
            require(!nulls[i] && !errors[i], "MySQL NULL or truncated integer");
        }
        require(client->count < MAX_ROWS, "Too many MySQL rows");
        client->rows[client->count++] = value;
    }
    mysql_stmt_free_result(statement);
}

/* Decode one CSV record in place. The benchmark never accepts embedded NULs. */
static size_t csv_record(char** cursor, char** fields, size_t capacity) {
    char* read = *cursor;
    char* write = read;
    size_t count = 0;
    if (*read == '\0') {
        return 0;
    }
    for (;;) {
        require(count < capacity, "Too many CSV columns");
        fields[count++] = write;
        bool quoted = *read == '"';
        if (quoted) {
            read++;
        }
        while (*read != '\0') {
            if (quoted && *read == '"') {
                read++;
                if (*read == '"') {
                    *write++ = *read++;
                    continue;
                }
                quoted = false;
                break;
            }
            if (!quoted && (*read == ',' || *read == '\r' || *read == '\n')) {
                break;
            }
            *write++ = *read++;
        }
        require(!quoted, "Unterminated CSV quote");
        char separator = *read;
        if (separator != '\0') {
            read++;
        }
        *write++ = '\0';
        if (separator == ',') {
            continue;
        }
        require(separator == '\0' || separator == '\n' || separator == '\r', "Invalid CSV record");
        if (separator == '\r' && *read == '\n') {
            read++;
        }
        *cursor = read;
        return count;
    }
}

static void decode_influx(connection* client, bool cross) {
    char* cursor = client->response.data;
    char* fields[32];
    size_t columns = csv_record(&cursor, fields, 32);
    require(columns >= 9, "Missing InfluxDB CSV columns");
    const char* names[] = {cross ? "ticker" : "time",
                           "open",
                           "high",
                           "low",
                           "close",
                           "volume",
                           "amount",
                           "adjust_factor"};
    size_t mapping[8];
    for (size_t i = 0; i < 8; i++) {
        mapping[i] = SIZE_MAX;
        for (size_t j = 0; j < columns; j++) {
            if (strcmp(names[i], fields[j]) == 0) {
                mapping[i] = j;
            }
        }
        require(mapping[i] != SIZE_MAX, "InfluxDB CSV field missing");
    }
    for (;;) {
        size_t count = csv_record(&cursor, fields, 32);
        if (count == 0) {
            break;
        }
        require(count == columns && client->count < MAX_ROWS, "Invalid InfluxDB result shape");
        uint64_t values[8];
        for (size_t i = 0; i < 8; i++) {
            values[i] = number(fields[mapping[i]], UINT64_MAX);
        }
        uint64_t key = cross ? values[0] : values[0] / 60u;
        require(key <= UINT32_MAX && (cross || values[0] % 60u == 0), "Invalid result timestamp");
        for (size_t i = 1; i <= 4; i++) {
            require(values[i] <= INT32_MAX, "Unexpected generated price");
        }
        require(values[5] <= UINT32_MAX && values[7] <= UINT32_MAX, "Result integer overflow");
        client->rows[client->count++] = (result_row){
            .key = (uint32_t)key,
            .value = {(int32_t)values[1], (int32_t)values[2], (int32_t)values[3],
                      (int32_t)values[4], (uint32_t)values[5], values[6], (uint32_t)values[7]},
        };
    }
}

static void query_http(connection* client, bool cross, uint32_t stock, uint32_t first,
                       uint32_t length) {
    char sql[2048];
    uint32_t start = time_key(first);
    uint32_t end = time_key(first + length - 1u) + 1u;
    if (strcmp(client->backend, "clickhouse") == 0) {
        if (cross) {
            snprintf(sql, sizeof(sql),
                     "SELECT ticker,open,high,low,close,volume,amount,adjust_factor "
                     "FROM bars WHERE time_key=%" PRIu32 " FORMAT RowBinary",
                     start);
        } else {
            snprintf(sql, sizeof(sql),
                     "SELECT time_key,open,high,low,close,volume,amount,adjust_factor "
                     "FROM bars WHERE ticker=%" PRIu32 " AND time_key>=%" PRIu32
                     " AND time_key<%" PRIu32 " ORDER BY time_key FORMAT RowBinary",
                     stock, start, end);
        }
        http_request(client, "/?database=baseline&use_query_cache=0&use_query_condition_cache=0",
                     sql, strlen(sql));
        require(client->response.size % OHLC_RESULT_BYTES == 0, "Invalid ClickHouse result length");
        append_binary(client, (const uint8_t*)client->response.data,
                      client->response.size / OHLC_RESULT_BYTES);
        return;
    }
    if (cross) {
        snprintf(sql, sizeof(sql),
                 "SELECT open,high,low,close,volume,amount,adjust_factor,ticker "
                 "FROM bars WHERE time=%" PRIu64,
                 (uint64_t)start * UINT64_C(60000000000));
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT open,high,low,close,volume,amount,adjust_factor "
                 "FROM bars WHERE ticker='%05" PRIu32 "' AND time>=%" PRIu64 " AND time<%" PRIu64
                 " ORDER BY time",
                 stock, (uint64_t)start * UINT64_C(60000000000),
                 (uint64_t)end * UINT64_C(60000000000));
    }
    char* escaped = curl_easy_escape(client->http, sql, 0);
    require(escaped != NULL, "Query encoding failed");
    char body[8192];
    int size = snprintf(body, sizeof(body), "q=%s", escaped);
    curl_free(escaped);
    require(size > 0 && (size_t)size < sizeof(body), "Encoded query too long");
    http_request(client, "/query?db=baseline&epoch=s", body, (size_t)size);
    decode_influx(client, cross);
}

static void verify(connection* client, bool cross, uint32_t stock, uint32_t first,
                   uint32_t length) {
    require(client->count == (cross ? client->stocks : length), "Query row count mismatch");
    bool seen[MAX_ROWS] = {0};
    for (size_t i = 0; i < client->count; i++) {
        const result_row* actual = &client->rows[i];
        uint32_t code = cross ? actual->key : stock;
        uint32_t minute = cross ? first : first + (uint32_t)i;
        require(code < client->stocks, "Invalid result ticker");
        if (cross) {
            require(!seen[code], "Duplicate cross-section ticker");
            seen[code] = true;
        } else {
            require(actual->key == time_key(minute), "Time order or key mismatch");
        }
        ohlc_row expected = value_for(code, minute);
        const ohlc_row* value = &actual->value;
        require(value->open == expected.open && value->high == expected.high &&
                    value->low == expected.low && value->close == expected.close &&
                    value->volume == expected.volume && value->amount == expected.amount &&
                    value->adjust_factor == expected.adjust_factor,
                "Seven-field value mismatch");
    }
}

static void read_queries(connection* client) {
    puts("{\"event\":\"ready\"}");
    fflush(stdout);
    char command[256];
    while (fgets(command, sizeof(command), stdin) != NULL) {
        if (strcmp(command, "STOP\n") == 0) {
            return;
        }
        char kind = 0;
        uint32_t identifier = 0;
        uint32_t stock = 0;
        uint32_t first = 0;
        uint32_t length = 0;
        require(sscanf(command, "%c %" SCNu32 " %" SCNu32 " %" SCNu32 " %" SCNu32, &kind,
                       &identifier, &stock, &first, &length) == 5,
                "Invalid query command");
        require((kind == 'S' || kind == 'X') && stock < client->stocks && length > 0 &&
                    first < client->times && length <= client->times - first,
                "Query outside dataset");
        bool cross = kind == 'X';
        client->count = 0;
        client->payload_bytes = 0;
        uint64_t started = now_ns();
        if (client->ohlc != NULL) {
            query_ohlc(client, cross, stock, first, length);
        } else if (client->mysql != NULL) {
            query_mysql(client, cross, stock, first, length);
        } else {
            query_http(client, cross, stock, first, length);
        }
        uint64_t elapsed = now_ns() - started;
        uint64_t verify_started = now_ns();
        verify(client, cross, stock, first, length);
        printf("{\"event\":\"query\",\"id\":%" PRIu32 ",\"rows\":%zu,\"wall_ns\":%" PRIu64
               ",\"verify_ns\":%" PRIu64 ",\"payload_bytes\":%" PRIu64 "}\n",
               identifier, client->count, elapsed, now_ns() - verify_started,
               client->payload_bytes);
        fflush(stdout);
    }
    require(!ferror(stdin), "Query command input failed");
}

typedef struct {
    connection client;
    pthread_barrier_t* barrier;
    uint64_t* deadline;
    uint64_t seed;
    uint64_t queries[2];
    uint64_t* samples[2];
    size_t counts[2];
    uint64_t finished;
} stress_reader;

#define STRESS_SAMPLES 250000u

static void* stress_thread(void* context) {
    stress_reader* reader = context;
    open_connection(&reader->client, false);
    for (size_t i = 0; i < 2; i++) {
        reader->samples[i] = malloc(STRESS_SAMPLES * sizeof(uint64_t));
        require(reader->samples[i] != NULL, "Stress sample allocation failed");
    }
    pthread_barrier_wait(reader->barrier);
    pthread_barrier_wait(reader->barrier);
    uint64_t sequence = 0;
    while (now_ns() < *reader->deadline) {
        bool cross = (sequence++ & 1u) != 0;
        size_t kind = cross ? 1u : 0u;
        reader->seed = mix(reader->seed);
        uint32_t length = cross ? 1u : 240u;
        uint32_t stock = (uint32_t)(reader->seed % reader->client.stocks);
        uint32_t first = (uint32_t)((reader->seed >> 32) % (reader->client.times - length + 1u));
        connection* client = &reader->client;
        client->count = 0;
        client->payload_bytes = 0;
        uint64_t started = now_ns();
        if (client->ohlc != NULL) {
            query_ohlc(client, cross, stock, first, length);
        } else if (client->mysql != NULL) {
            query_mysql(client, cross, stock, first, length);
        } else {
            query_http(client, cross, stock, first, length);
        }
        uint64_t elapsed = now_ns() - started;
        verify(client, cross, stock, first, length);
        reader->queries[kind]++;
        if (reader->counts[kind] < STRESS_SAMPLES) {
            reader->samples[kind][reader->counts[kind]++] = elapsed;
        }
    }
    reader->finished = now_ns();
    close_connection(&reader->client);
    if (strcmp(reader->client.backend, "mysql") == 0) {
        mysql_thread_end();
    }
    return NULL;
}

static int compare_u64(const void* left, const void* right) {
    uint64_t a = *(const uint64_t*)left;
    uint64_t b = *(const uint64_t*)right;
    return (a > b) - (a < b);
}

static void stress(const connection* template, const char* directory, unsigned threads,
                   unsigned seconds, uint64_t seed) {
    pthread_barrier_t barrier;
    require(pthread_barrier_init(&barrier, NULL, threads + 1u) == 0,
            "Barrier initialization failed");
    stress_reader* readers = calloc(threads, sizeof(*readers));
    pthread_t* workers = calloc(threads, sizeof(*workers));
    require(readers != NULL && workers != NULL, "Stress allocation failed");
    uint64_t deadline = UINT64_MAX;
    for (unsigned i = 0; i < threads; i++) {
        readers[i].client = *template;
        readers[i].barrier = &barrier;
        readers[i].deadline = &deadline;
        readers[i].seed = mix(seed + i);
        require(pthread_create(&workers[i], NULL, stress_thread, &readers[i]) == 0,
                "Stress thread creation failed");
    }
    /* The second barrier keeps connection setup outside the measured window. */
    pthread_barrier_wait(&barrier);
    uint64_t started = now_ns();
    deadline = started + (uint64_t)seconds * UINT64_C(1000000000);
    pthread_barrier_wait(&barrier);
    uint64_t finished = started;
    for (unsigned i = 0; i < threads; i++) {
        require(pthread_join(workers[i], NULL) == 0, "Stress join failed");
        if (readers[i].finished > finished) {
            finished = readers[i].finished;
        }
    }
    printf("{\"event\":\"stress\",\"readers\":%u,\"wall_ns\":%" PRIu64 ",\"kinds\":[", threads,
           finished - started);
    for (size_t kind = 0; kind < 2; kind++) {
        uint64_t queries = 0;
        size_t count = 0;
        for (unsigned i = 0; i < threads; i++) {
            queries += readers[i].queries[kind];
            count += readers[i].counts[kind];
        }
        require(count > 0, "Stress interval returned no queries");
        uint64_t* samples = malloc(count * sizeof(*samples));
        require(samples != NULL, "Combined sample allocation failed");
        size_t offset = 0;
        for (unsigned i = 0; i < threads; i++) {
            memcpy(samples + offset, readers[i].samples[kind],
                   readers[i].counts[kind] * sizeof(*samples));
            offset += readers[i].counts[kind];
            free(readers[i].samples[kind]);
        }
        FILE* raw = open_file(directory, kind == 0 ? "series240.bin" : "cross.bin", "wbx");
        require(fwrite(samples, sizeof(*samples), count, raw) == count && fclose(raw) == 0,
                "Raw stress samples could not be saved");
        qsort(samples, count, sizeof(*samples), compare_u64);
        printf("%s{\"kind\":\"%s\",\"queries\":%" PRIu64 ",\"samples\":%zu,\"p50_ns\":%" PRIu64
               ",\"p95_ns\":%" PRIu64 ",\"p99_ns\":%" PRIu64 "}",
               kind == 0 ? "" : ",", kind == 0 ? "series240" : "cross", queries, count,
               samples[(count * 50u + 99u) / 100u - 1u], samples[(count * 95u + 99u) / 100u - 1u],
               samples[(count * 99u + 99u) / 100u - 1u]);
        free(samples);
    }
    puts("]}");
    free(readers);
    free(workers);
    pthread_barrier_destroy(&barrier);
}

int main(int argc, char** argv) {
    require(
        argc >= 2,
        "Use generate DIRECTORY STOCKS TIMES, or load/read BACKEND DIRECTORY PORT STOCKS TIMES");
    if (strcmp(argv[1], "generate") == 0) {
        require(argc == 5, "Invalid generator arguments");
        uint32_t stocks = (uint32_t)number(argv[3], MAX_ROWS);
        uint32_t times = (uint32_t)number(argv[4], 8580);
        require(stocks > 0 && times > 0, "Empty dataset");
        generate(argv[2], stocks, times);
        return 0;
    }
    bool stressing = strcmp(argv[1], "stress") == 0;
    require(argc == (stressing ? 10 : 7), "Invalid driver arguments");
    bool loading = strcmp(argv[1], "load") == 0;
    require(loading || stressing || strcmp(argv[1], "read") == 0, "Unknown operation");
    connection client = {.backend = argv[2],
                         .stocks = (uint32_t)number(argv[5], MAX_ROWS),
                         .times = (uint32_t)number(argv[6], 8580),
                         .port = (unsigned)number(argv[4], 65535)};
    require(client.stocks > 0 && client.times > 0 && client.port > 0,
            "Invalid connection dimensions");
    require(curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK, "HTTP initialization failed");
    bool mysql = strcmp(client.backend, "mysql") == 0;
    if (mysql) {
        require(mysql_library_init(0, NULL, NULL) == 0, "MySQL library initialization failed");
    }
    if (stressing) {
        unsigned threads = (unsigned)number(argv[7], 16);
        unsigned seconds = (unsigned)number(argv[8], 60);
        require(threads > 0 && seconds > 0 && client.times >= 240, "Invalid stress dimensions");
        stress(&client, argv[3], threads, seconds, number(argv[9], UINT64_MAX));
        if (mysql) {
            mysql_library_end();
        }
        curl_global_cleanup();
        return 0;
    }
    uint64_t opened = now_ns();
    open_connection(&client, loading);
    if (loading) {
        printf("{\"event\":\"setup\",\"wall_ns\":%" PRIu64 "}\n", now_ns() - opened);
        fflush(stdout);
        load(&client, argv[3]);
    } else {
        read_queries(&client);
    }
    close_connection(&client);
    if (mysql) {
        mysql_library_end();
    }
    curl_global_cleanup();
    return 0;
}

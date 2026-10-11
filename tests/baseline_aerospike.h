/* SPDX-License-Identifier: Apache-2.0 */
/* Optional Aerospike adapter, included after the shared benchmark helpers. */

static const char* const aerospike_fields[] = {"open",   "high",   "low",          "close",
                                               "volume", "amount", "adjust_factor"};

static void open_aerospike(connection* client, bool loading) {
    as_config config;
    as_config_init(&config);
    as_config_add_host(&config, "127.0.0.1", (uint16_t)client->port);
    config.policies.query.base.total_timeout = 120000;
    config.policies.query.base.socket_timeout = 120000;
    client->aerospike = aerospike_new(&config);
    require(client->aerospike != NULL, "Aerospike allocation");
    as_error error;
    require(aerospike_connect(client->aerospike, &error) == AEROSPIKE_OK, error.message);
    if (loading) {
        const char* bins[] = {"skey", "time_key"};
        const char* names[] = {"by_series", "by_time"};
        for (size_t i = 0; i < 2; i++) {
            as_index_task task;
            require(aerospike_index_create(client->aerospike, &error, &task, NULL, "bench", "bars",
                                           bins[i], names[i], AS_INDEX_NUMERIC) == AEROSPIKE_OK,
                    error.message);
            require(aerospike_index_create_wait(&error, &task, 100) == AEROSPIKE_OK, error.message);
        }
    }
}

static uint32_t aerospike_u32(const uint8_t* bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8) | ((uint32_t)bytes[2] << 16) |
           ((uint32_t)bytes[3] << 24);
}

static void write_aerospike(connection* client, const uint8_t* bytes, uint32_t count) {
    as_batch_records records;
    as_batch_records_init(&records, count);
    as_operations* operations = calloc(count, sizeof(*operations));
    require(operations != NULL, "Aerospike operations allocation");
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t* input = bytes + (size_t)i * OHLC_WRITE_BYTES;
        uint32_t ticker = aerospike_u32(input);
        uint32_t time = aerospike_u32(input + 4);
        int64_t key = (int64_t)(((uint64_t)ticker << 32) | time);
        ohlc_row row;
        ohlc_row_decode(input + 8, &row);
        require(row.flags == OHLC_FIELDS_MARKET && row.volume <= INT64_MAX &&
                    row.amount <= INT64_MAX && row.adjust_factor <= INT64_MAX,
                "Fixture exceeds Aerospike integer range");
        as_operations* ops = &operations[i];
        as_operations_init(ops, 10);
        require(as_operations_add_write_int64(ops, "skey", key) &&
                    as_operations_add_write_int64(ops, "ticker", ticker) &&
                    as_operations_add_write_int64(ops, "time_key", time),
                "Aerospike key operations");
        int64_t values[] = {row.open,
                            row.high,
                            row.low,
                            row.close,
                            (int64_t)row.volume,
                            (int64_t)row.amount,
                            (int64_t)row.adjust_factor};
        for (size_t field = 0; field < 7; field++) {
            require(as_operations_add_write_int64(ops, aerospike_fields[field], values[field]),
                    "Aerospike field operation");
        }
        as_batch_write_record* record = as_batch_write_reserve(&records);
        as_key_init_int64(&record->key, "bench", "bars", key);
        record->ops = ops;
    }
    as_error error;
    as_policy_batch policy;
    as_policy_batch_init(&policy);
    policy.base.total_timeout = 120000;
    policy.base.socket_timeout = 120000;
    require(aerospike_batch_write(client->aerospike, &error, &policy, &records) == AEROSPIKE_OK,
            error.message);
    for (uint32_t i = 0; i < count; i++) {
        as_batch_record* record = as_vector_get(&records.list, i);
        require(record->base.result == AEROSPIKE_OK, "Aerospike batch record failed");
        as_operations_destroy(&operations[i]);
    }
    as_batch_records_destroy(&records);
    free(operations);
}

typedef struct {
    connection* client;
    bool cross;
    pthread_mutex_t mutex;
} aerospike_results;

static bool collect_aerospike(const as_val* value, void* context) {
    if (value == NULL) {
        return true;
    }
    aerospike_results* result = context;
    as_record* record = as_record_fromval(value);
    require(record != NULL, "Aerospike query record");
    int64_t values[7];
    for (size_t field = 0; field < 7; field++) {
        as_bin_value* bin = as_record_get(record, aerospike_fields[field]);
        require(bin != NULL && as_val_type((as_val*)bin) == AS_INTEGER,
                "Aerospike missing or noninteger field");
        values[field] = as_record_get_int64(record, aerospike_fields[field], 0);
    }
    int64_t key = as_record_get_int64(record, result->cross ? "ticker" : "time_key", -1);
    require(key >= 0 && key <= UINT32_MAX && values[4] >= 0 && values[5] >= 0 && values[6] >= 0,
            "Aerospike result range");
    pthread_mutex_lock(&result->mutex);
    connection* client = result->client;
    require(client->count < MAX_ROWS, "Too many Aerospike rows");
    client->rows[client->count++] =
        (result_row){.key = (uint32_t)key,
                     .value = {values[0], values[1], values[2], values[3], (uint64_t)values[4],
                               (uint64_t)values[5], (uint64_t)values[6], 0, OHLC_FIELDS_MARKET}};
    pthread_mutex_unlock(&result->mutex);
    return true;
}

static int aerospike_compare_rows(const void* left, const void* right) {
    const result_row* a = left;
    const result_row* b = right;
    return (a->key > b->key) - (a->key < b->key);
}

static void query_aerospike(connection* client, bool cross, uint32_t ticker, uint32_t first,
                            uint32_t length) {
    as_query query;
    as_query_init(&query, "bench", "bars");
    as_query_where_init(&query, 1);
    if (cross) {
        require(as_query_where(&query, "time_key", as_integer_equals(time_key(first))),
                "Aerospike cross predicate");
    } else {
        int64_t begin = (int64_t)(((uint64_t)ticker << 32) | time_key(first));
        int64_t end = (int64_t)(((uint64_t)ticker << 32) | time_key(first + length - 1u));
        require(as_query_where(&query, "skey", as_integer_range(begin, end)),
                "Aerospike series predicate");
    }
    aerospike_results result = {.client = client, .cross = cross};
    require(pthread_mutex_init(&result.mutex, NULL) == 0, "Aerospike result mutex");
    as_error error;
    require(aerospike_query_foreach(client->aerospike, &error, NULL, &query, collect_aerospike,
                                    &result) == AEROSPIKE_OK,
            error.message);
    pthread_mutex_destroy(&result.mutex);
    as_query_destroy(&query);
    /* Secondary-index results are unordered; sorting is part of query latency. */
    qsort(client->rows, client->count, sizeof(*client->rows), aerospike_compare_rows);
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "ohlc/ohlc.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static bool successful(ohlc_status status) {
    if (status != OHLC_OK) {
        fprintf(stderr, "ohlc: %s\n", ohlc_status_string(status));
        return false;
    }
    return true;
}

static uint32_t result_key(const uint8_t* bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8) | ((uint32_t)bytes[2] << 16) |
           ((uint32_t)bytes[3] << 24);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s DATABASE_DIRECTORY\n", argv[0]);
        return 2;
    }
    ohlc_options options;
    ohlc_options_init(&options);
    options.create_if_missing = true;
    ohlc_db* db = NULL;
    ohlc_cursor* cursor = NULL;
    int result = 1;
    if (!successful(ohlc_open(argv[1], &options, &db))) {
        return result;
    }
    ohlc_table_definition definition = {"bars_3m", OHLC_MINUTE, 3, "Asia/Shanghai", "Example bars"};
    ohlc_table_info table;
    ohlc_status status = ohlc_table_create(db, &definition, &table);
    if (status == OHLC_ALREADY_EXISTS) {
        status = ohlc_table_open(db, definition.name, &table);
    }
    if (!successful(status)) {
        goto cleanup;
    }
    ohlc_bytes ticker = {"AAPL", 4};
    uint32_t code = 0;
    uint64_t sequence = 0;
    const char* times[] = {"20260901 09:30:00", "20260901 09:33:00", "20260901 09:36:00"};
    uint8_t batch[3 * OHLC_WRITE_BYTES];
    uint32_t keys[3];
    for (size_t i = 0; i < 3; i++) {
        if (!successful(ohlc_time_parse(&table, times[i], &keys[i]))) {
            goto cleanup;
        }
        ohlc_row row = {10000, 10100, 9950, 10080, 1200, UINT64_C(12100000), 1000000};
        row.close += (int32_t)i;
        ohlc_write_encode(batch + i * OHLC_WRITE_BYTES, code, keys[i], &row);
    }
    if (!successful(ohlc_write_named(db, table.id, &ticker, 1, batch, 3, &sequence)) ||
        !successful(ohlc_resolve(db, table.id, ticker, &code)) ||
        !successful(ohlc_series(db, table.id, code, keys[0], (uint64_t)keys[2] + 1, &cursor))) {
        goto cleanup;
    }
    printf("Committed sequence: %" PRIu64 "\n", sequence);
    uint8_t records[16 * OHLC_RESULT_BYTES];
    while (true) {
        size_t count = 0;
        if (!successful(ohlc_cursor_next(cursor, records, 16, &count))) {
            goto cleanup;
        }
        if (count == 0) {
            break;
        }
        for (size_t i = 0; i < count; i++) {
            const uint8_t* record = records + i * OHLC_RESULT_BYTES;
            char time[64];
            ohlc_row row;
            ohlc_row_decode(record + 4, &row);
            if (!successful(ohlc_time_format(&table, result_key(record), time, sizeof(time)))) {
                goto cleanup;
            }
            printf("%s: open=%" PRId32 " high=%" PRId32 " low=%" PRId32 " close=%" PRId32
                   " volume=%" PRIu32 " amount=%" PRIu64 " factor=%" PRIu32 "\n",
                   time, row.open, row.high, row.low, row.close, row.volume, row.amount,
                   row.adjust_factor);
        }
    }
    ohlc_cursor_close(cursor);
    cursor = NULL;
    if (!successful(ohlc_cross(db, table.id, keys[1], &cursor))) {
        goto cleanup;
    }
    size_t count = 0;
    if (!successful(ohlc_cursor_next(cursor, records, 16, &count))) {
        goto cleanup;
    }
    printf("Cross-section records: %zu\n", count);
    if (!successful(ohlc_checkpoint(db))) {
        goto cleanup;
    }
    result = 0;
cleanup:
    ohlc_cursor_close(cursor);
    if (!successful(ohlc_close(db))) {
        result = 1;
    }
    return result;
}

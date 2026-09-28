/* SPDX-License-Identifier: Apache-2.0 */
#ifndef OHLC_OHLC_H
#define OHLC_OHLC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define OHLC_VERSION "0.1.0-beta.1"
#define OHLC_ABI_VERSION 2u
#define OHLC_FORMAT_VERSION 5u
#define OHLC_ROW_BYTES 32u
#define OHLC_WRITE_BYTES 40u
#define OHLC_RESULT_BYTES 36u
#define OHLC_MAX_BATCH_ROWS 250000u
#define OHLC_MAX_QUERY_TICKERS 65536u

typedef enum {
    OHLC_OK = 0,
    OHLC_INVALID = 1,
    OHLC_NOT_FOUND = 2,
    OHLC_LIMIT = 3,
    OHLC_BUSY = 4,
    OHLC_IO = 5,
    OHLC_CORRUPT = 6,
    OHLC_UNSUPPORTED = 7,
    OHLC_UNAUTHORIZED = 8,
    OHLC_OUTCOME_UNKNOWN = 9,
    OHLC_CANCELLED = 10,
    OHLC_ALREADY_EXISTS = 11
} ohlc_status;

typedef enum { OHLC_MINUTE = 1, OHLC_DAY = 2 } ohlc_period_unit;

/* This native value is not the disk or wire layout. Always use the row codec. */
typedef struct {
    int32_t open;
    int32_t high;
    int32_t low;
    int32_t close;
    uint32_t volume;
    uint64_t amount;
    uint32_t adjust_factor;
} ohlc_row;

typedef struct {
    const void* data;
    size_t size;
} ohlc_bytes;

typedef struct {
    const char* name;
    ohlc_period_unit period_unit;
    uint32_t period_count;
    const char* timezone;
    const char* description;
} ohlc_table_definition;

typedef struct {
    uint32_t id;
    uint64_t created_seq;
    ohlc_period_unit period_unit;
    uint32_t period_count;
    char name[64];
    char timezone[256];
    char description[4097];
} ohlc_table_info;

typedef struct {
    size_t cache_bytes;
    size_t memory_limit;
    uint64_t data_volume_bytes;
    uint64_t wal_segment_bytes;
    uint32_t max_tables;
    uint32_t max_cursors;
    uint32_t max_query_ms;
    /* Default 0 reads in caller threads; 1..16 creates a shared read pool. */
    uint32_t read_workers;
    bool create_if_missing;
} ohlc_options;

typedef struct ohlc_db ohlc_db;
typedef struct ohlc_cursor ohlc_cursor;

typedef struct {
    uint64_t commit_seq;
    uint64_t checkpoint_seq;
    /* Sum of dictionary entries in live tables; cross-table names count twice. */
    uint64_t ticker_count;
    uint32_t table_count;
    uint64_t memory_bytes;
    uint64_t disk_read_bytes;
    uint64_t disk_read_calls;
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t wal_bytes;
    uint64_t data_bytes;
} ohlc_stats;

/* Process-wide constants; no allocation, initialization or ownership transfer.
 * Bindings must check the ABI before passing native structures to the library. */
const char* ohlc_version(void);
uint32_t ohlc_abi_version(void);
const char* ohlc_status_string(ohlc_status status);
void ohlc_options_init(ohlc_options* options);
/* Codec arguments must be non-NULL; native rows and encoded buffers must not
 * overlap. Codecs preserve every bit and do not validate market semantics. */
void ohlc_row_encode(uint8_t output[OHLC_ROW_BYTES], const ohlc_row* row);
void ohlc_row_decode(const uint8_t input[OHLC_ROW_BYTES], ohlc_row* row);
void ohlc_write_encode(uint8_t output[OHLC_WRITE_BYTES], uint32_t ticker_code, uint32_t time_key,
                       const ohlc_row* row);

/* Public calls are thread safe except cursor operations, which are single-owner.
 * The database owns its files exclusively. Database close requires all other
 * calls to have finished and every cursor to have been closed.
 * Output handles are set to NULL on failure. No API terminates the process. */
ohlc_status ohlc_open(const char* path, const ohlc_options* options, ohlc_db** output);
ohlc_status ohlc_close(ohlc_db* db);
void ohlc_uuid(ohlc_db* db, uint8_t output[16]);
void ohlc_get_stats(ohlc_db* db, ohlc_stats* output);

ohlc_status ohlc_table_create(ohlc_db* db, const ohlc_table_definition* definition,
                              ohlc_table_info* output);
/* Permanently remove a table by its stable ID. Success means durable deletion;
 * existing cursors keep their snapshots. Files are reclaimed after both
 * checkpoints cover the deletion and outstanding cursors have closed.
 * IDs are never reused. The output sequence is unchanged on failure. */
ohlc_status ohlc_table_drop(ohlc_db* db, uint32_t table_id, uint64_t* commit_seq);
ohlc_status ohlc_table_open(ohlc_db* db, const char* name, ohlc_table_info* output);
ohlc_status ohlc_table_get(ohlc_db* db, uint32_t id, ohlc_table_info* output);
ohlc_status ohlc_table_list(ohlc_db* db, uint32_t start_id, ohlc_table_info* output,
                            size_t capacity, size_t* count, uint64_t* snapshot_seq);

/* Ticker bytes are exact, may contain NUL, and have length 1..4096. */
ohlc_status ohlc_register(ohlc_db* db, uint32_t table_id, ohlc_bytes ticker, uint32_t* code,
                          uint64_t* seq);
ohlc_status ohlc_resolve(ohlc_db* db, uint32_t table_id, ohlc_bytes ticker, uint32_t* code);
/* Copy exact bytes into caller-owned storage; output borrows that 4096-byte buffer.
 * Codes belong to (database UUID, table ID). Registration is optional: named
 * writes create missing tickers in the same transaction as their rows. */
ohlc_status ohlc_ticker(ohlc_db* db, uint32_t table_id, uint32_t code, uint8_t buffer[4096],
                        ohlc_bytes* output);

/* Rows use indexes into tickers, not persistent codes. Only referenced names are
 * created. Names and rows are borrowed for this call. Duplicate logical keys
 * reject the entire batch, including new names. All write guarantees below apply. */
ohlc_status ohlc_write_named(ohlc_db* db, uint32_t table_id, const ohlc_bytes* tickers,
                             size_t ticker_count, const void* rows, size_t count,
                             uint64_t* commit_seq);

/* Encoded rows are (ticker_code:u32, time_key:u32, row:32), little endian.
 * The buffer is borrowed only for this call. Duplicate keys reject the entire
 * batch. Success means durable WAL and atomic visibility. Never automatically
 * retry OHLC_OUTCOME_UNKNOWN. Concurrent batches may share a WAL sync. A full
 * admission queue returns OHLC_BUSY before accepting the batch. An empty batch
 * is invalid. The output sequence is unchanged on failure. */
ohlc_status ohlc_write(ohlc_db* db, uint32_t table_id, const void* rows, size_t count,
                       uint64_t* commit_seq);

/* The range is half open; end_exclusive can be 2^32. Each cursor pins one
 * database snapshot, and must be closed even after a read error. */
ohlc_status ohlc_series(ohlc_db* db, uint32_t table_id, uint32_t ticker_code, uint32_t start,
                        uint64_t end_exclusive, ohlc_cursor** output);
ohlc_status ohlc_cross(ohlc_db* db, uint32_t table_id, uint32_t time_key, ohlc_cursor** output);
/* Select exact ticker bytes in the same snapshot as the rows. Unknown names and
 * missing rows are omitted; duplicates appear once, ordered by table-local code.
 * An empty list returns no rows. Names (1..4096 bytes each) are borrowed only for
 * this call; the cursor owns its selection. At most OHLC_MAX_QUERY_TICKERS names
 * are accepted. Invalid inputs return INVALID, budget/count limits return LIMIT,
 * and a missing table returns NOT_FOUND. Snapshot and thread rules above apply. */
ohlc_status ohlc_cross_tickers(ohlc_db* db, uint32_t table_id, uint32_t time_key,
                               const ohlc_bytes* tickers, size_t count, ohlc_cursor** output);
uint64_t ohlc_cursor_sequence(const ohlc_cursor* cursor);
/* Copies at most capacity result rows (key:u32, row:32) to caller storage.
 * A successful zero count means complete. Results are ordered by time for
 * series and by ticker code for cross. Capacity must be at least one. */
ohlc_status ohlc_cursor_next(ohlc_cursor* cursor, void* output, size_t capacity, size_t* count);
void ohlc_cursor_close(ohlc_cursor* cursor);

/* Persists a captured committed snapshot without blocking writes for file I/O.
 * Later commits remain WAL-protected and may need another checkpoint. Calls
 * serialize with other checkpoints and do not change the commit sequence. */
ohlc_status ohlc_checkpoint(ohlc_db* db);

/* Dates use YYYY-MM-DD or YYYYMMDD. Minute strings accept either date form
 * plus HH:MM:SS, and an optional Z or +/-HH:MM offset. Seconds must be zero.
 * An explicit offset overrides the table timezone. No bar rounding occurs. */
ohlc_status ohlc_time_parse(const ohlc_table_info* table, const char* input, uint32_t* time_key);
/* Minute keys are formatted as unambiguous UTC ISO timestamps; day keys as
 * calendar dates. The caller owns the output buffer. */
ohlc_status ohlc_time_format(const ohlc_table_info* table, uint32_t time_key, char* output,
                             size_t capacity);
/* Human-facing time in the table timezone, including an explicit UTC offset.
 * Day tables retain their calendar-date label. Machine output should also
 * retain the original integer time key. */
ohlc_status ohlc_time_format_local(const ohlc_table_info* table, uint32_t time_key, char* output,
                                   size_t capacity);

#ifdef __cplusplus
}
#endif
#endif

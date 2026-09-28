/* SPDX-License-Identifier: Apache-2.0 */
#ifndef OHLC_CLIENT_H
#define OHLC_CLIENT_H

#include "ohlc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ohlc_client ohlc_client;

typedef struct {
    const char* socket_path;
    const char* host;
    const char* port;
    const char* ca_file;
    ohlc_bytes token;
    uint32_t timeout_ms;
    bool tls;
} ohlc_connection_options;

typedef struct {
    uint8_t uuid[16];
    uint32_t max_frame_bytes;
    uint32_t max_write_rows;
    uint32_t max_query_ms;
    uint32_t capabilities;
} ohlc_connection_info;

/* A connection is single-owner and permits one outstanding request. Plain TCP
 * does not encrypt traffic. An empty token selects anonymous access when the
 * server has no credentials configured; otherwise provide a configured token.
 * TLS verifies the peer certificate and host. NULL ca_file uses system trust.
 * A socket_path selects a local Unix socket instead of host/port. Options and
 * token bytes are borrowed only during connect. No automatic retries occur. */
void ohlc_connection_options_init(ohlc_connection_options* options);
ohlc_status ohlc_client_connect(const ohlc_connection_options* options, ohlc_client** output);
void ohlc_client_close(ohlc_client* client);
void ohlc_client_info(const ohlc_client* client, ohlc_connection_info* output);
bool ohlc_client_connected(const ohlc_client* client);
/* Borrowed descriptor for event integration. Do not close it directly. A
 * single-owner caller may use shutdown(fd, SHUT_RDWR) to interrupt I/O. */
int ohlc_client_socket(const ohlc_client* client);

/* Low-level, single-frame metadata RPC. Opcodes 2..4 and 7..15 only.
 * The returned little-endian body is borrowed until the next client operation.
 * Malformed or truncated responses break the connection. A mutation whose
 * outcome cannot be established returns OHLC_OUTCOME_UNKNOWN. */
ohlc_status ohlc_client_call(ohlc_client* client, uint16_t opcode, const void* body, size_t size,
                             ohlc_bytes* response);
ohlc_status ohlc_client_table_open(ohlc_client* client, const char* name, ohlc_table_info* output);
ohlc_status ohlc_client_table_create(ohlc_client* client, const ohlc_table_definition* definition,
                                     ohlc_table_info* output);
/* Requires write access. Deletes this ID, never a replacement with the same
 * name. An uncertain acknowledgement returns OHLC_OUTCOME_UNKNOWN. */
ohlc_status ohlc_client_table_drop(ohlc_client* client, uint32_t table_id, uint64_t* sequence);
ohlc_status ohlc_client_resolve(ohlc_client* client, uint32_t table_id, ohlc_bytes ticker,
                                uint32_t* code);
ohlc_status ohlc_client_register(ohlc_client* client, uint32_t table_id, ohlc_bytes ticker,
                                 uint32_t* code, uint64_t* sequence);
/* Named batches have the same atomicity and ownership as ohlc_write_named. */
ohlc_status ohlc_client_write_named(ohlc_client* client, uint32_t table_id,
                                    const ohlc_bytes* tickers, size_t ticker_count,
                                    const void* rows, size_t count, uint64_t* sequence);
ohlc_status ohlc_client_write(ohlc_client* client, uint32_t table_id, const void* encoded_rows,
                              size_t count, uint64_t* sequence);
/* Stats require read access; checkpoint requires write access. Counters are
 * I/O counters are cumulative since open; memory_bytes is current allocation.
 * A disconnected checkpoint may have completed; no automatic retry occurs. */
ohlc_status ohlc_client_stats(ohlc_client* client, ohlc_stats* output);
ohlc_status ohlc_client_checkpoint(ohlc_client* client);

/* Query start transmits a request. Next validates every chunk, including
 * ordering, snapshot identity, cumulative counts, and the successful FINAL.
 * rows is a borrowed view of count packed 36-byte rows. A successful call with
 * final=true completes the query, including any rows returned by that call.
 * Closing a connection cancels a query; partial results are never complete. */
ohlc_status ohlc_client_series(ohlc_client* client, uint32_t table_id, uint32_t ticker,
                               uint32_t start, uint64_t end_exclusive);
ohlc_status ohlc_client_cross(ohlc_client* client, uint32_t table_id, uint32_t time_key);
/* Same selection semantics as ohlc_cross_tickers; borrows names during this call.
 * Also bounded by the negotiated frame limit. Uses the normal query/next flow. */
ohlc_status ohlc_client_cross_tickers(ohlc_client* client, uint32_t table_id, uint32_t time_key,
                                      const ohlc_bytes* tickers, size_t count);
ohlc_status ohlc_client_next(ohlc_client* client, ohlc_bytes* rows, uint32_t* count,
                             uint64_t* snapshot_sequence, bool* final);

#ifdef __cplusplus
}
#endif
#endif

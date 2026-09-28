/* SPDX-License-Identifier: Apache-2.0 */
#include "net.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

struct ohlc_client {
    ohlc_transport transport;
    ohlc_connection_info info;
    uint32_t timeout_ms;
    uint64_t request;
    uint8_t* buffer;
    size_t capacity;
    uint32_t chunk;
    uint32_t table;
    uint32_t last_key;
    uint32_t start;
    uint64_t end;
    uint64_t sequence;
    uint64_t emitted;
    uint64_t deadline;
    uint16_t opcode;
    bool active;
};

void ohlc_connection_options_init(ohlc_connection_options* options) {
    memset(options, 0, sizeof(*options));
    options->host = "127.0.0.1";
    options->port = "8765";
    options->timeout_ms = 30000;
}

static ohlc_status connect_address(ohlc_transport* transport, const struct sockaddr* address,
                                   socklen_t size, uint64_t deadline) {
    int fd = socket(address->sa_family, SOCK_STREAM, 0);
    if (fd < 0) {
        return OHLC_IO;
    }
    ohlc_status status = ohlc_net_prepare(fd);
    if (status == OHLC_OK && connect(fd, address, size) != 0) {
        status = errno == EINPROGRESS ? ohlc_net_wait(fd, POLLOUT, deadline) : OHLC_IO;
        int error = 0;
        socklen_t error_size = sizeof(error);
        if (status == OHLC_OK &&
            (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0 || error != 0)) {
            status = OHLC_IO;
        }
    }
    if (status == OHLC_OK) {
        transport->fd = fd;
    } else {
        close(fd);
    }
    return status;
}

static ohlc_status connect_transport(ohlc_client* client, const ohlc_connection_options* options) {
    uint64_t deadline = ohlc_monotonic_ms() + options->timeout_ms;
    if (options->socket_path != NULL) {
        struct sockaddr_un address = {.sun_family = AF_UNIX};
        size_t size = strlen(options->socket_path);
        if (size == 0 || size >= sizeof(address.sun_path) || options->tls) {
            return OHLC_INVALID;
        }
        memcpy(address.sun_path, options->socket_path, size + 1);
        return connect_address(&client->transport, (const struct sockaddr*)&address,
                               sizeof(address), deadline);
    }
    if (options->host == NULL || options->port == NULL) {
        return OHLC_INVALID;
    }
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
    struct addrinfo* addresses = NULL;
    if (getaddrinfo(options->host, options->port, &hints, &addresses) != 0) {
        return OHLC_IO;
    }
    ohlc_status status = OHLC_IO;
    for (const struct addrinfo* item = addresses; item != NULL; item = item->ai_next) {
        status = connect_address(&client->transport, item->ai_addr, item->ai_addrlen, deadline);
        if (status == OHLC_OK) {
            break;
        }
    }
    freeaddrinfo(addresses);
    if (status != OHLC_OK || !options->tls) {
        return status;
    }
    SSL_CTX* context = SSL_CTX_new(TLS_client_method());
    if (context == NULL) {
        return OHLC_IO;
    }
    SSL_CTX_set_verify(context, SSL_VERIFY_PEER, NULL);
    if (SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION) != 1 ||
        (options->ca_file != NULL ? SSL_CTX_load_verify_locations(context, options->ca_file, NULL)
                                  : SSL_CTX_set_default_verify_paths(context)) != 1) {
        SSL_CTX_free(context);
        return OHLC_IO;
    }
    client->transport.ssl = SSL_new(context);
    SSL_CTX_free(context);
    SSL* ssl = client->transport.ssl;
    if (ssl == NULL || ohlc_net_tls_attach(&client->transport) != OHLC_OK) {
        return OHLC_IO;
    }
    uint8_t ip[16];
    bool numeric =
        inet_pton(AF_INET, options->host, ip) == 1 || inet_pton(AF_INET6, options->host, ip) == 1;
    if (numeric) {
        if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), options->host) != 1) {
            return OHLC_INVALID;
        }
    } else if (SSL_set1_host(ssl, options->host) != 1 ||
               SSL_set_tlsext_host_name(ssl, options->host) != 1) {
        return OHLC_INVALID;
    }
    return ohlc_net_handshake(&client->transport, false, deadline);
}

static ohlc_status fail_connection(ohlc_client* client, ohlc_status status) {
    ohlc_net_close(&client->transport);
    client->active = false;
    return status;
}

static ohlc_status receive(ohlc_client* client, ohlc_frame* frame) {
    ohlc_status status = ohlc_net_header(&client->transport, frame, client->deadline);
    if (status != OHLC_OK) {
        return fail_connection(client, status);
    }
    if (frame->opcode != client->opcode || frame->request != client->request ||
        frame->chunk != client->chunk || (frame->flags & 1u) == 0 ||
        frame->size > client->info.max_frame_bytes ||
        (frame->status != 0 && (frame->flags != 3 || frame->size != 0))) {
        return fail_connection(client, OHLC_CORRUPT);
    }
    if (frame->size > client->capacity) {
        uint8_t* buffer = realloc(client->buffer, frame->size);
        if (buffer == NULL) {
            return fail_connection(client, OHLC_LIMIT);
        }
        client->buffer = buffer;
        client->capacity = frame->size;
    }
    status = ohlc_net_io(&client->transport, client->buffer, frame->size, false, client->deadline);
    if (status != OHLC_OK) {
        return fail_connection(client, status);
    }
    if (client->chunk == UINT32_MAX && frame->flags != 3) {
        return fail_connection(client, OHLC_CORRUPT);
    }
    client->chunk++;
    return OHLC_OK;
}

static ohlc_status start_request(ohlc_client* client, uint16_t opcode, const void* body,
                                 size_t size) {
    if (client == NULL || (body == NULL && size != 0)) {
        return OHLC_INVALID;
    }
    if (client->active) {
        return OHLC_BUSY;
    }
    if (client->transport.fd < 0) {
        return OHLC_IO;
    }
    if (size > client->info.max_frame_bytes || client->request == UINT64_MAX) {
        return OHLC_LIMIT;
    }
    client->request++;
    client->opcode = opcode;
    client->chunk = 0;
    client->deadline = ohlc_monotonic_ms() + client->timeout_ms;
    ohlc_frame frame = {.opcode = opcode, .request = client->request, .size = (uint32_t)size};
    ohlc_status status = ohlc_net_send(&client->transport, &frame, body, client->deadline);
    if (status != OHLC_OK) {
        return fail_connection(client, status);
    }
    return OHLC_OK;
}

static ohlc_status call(ohlc_client* client, uint16_t opcode, const void* body, size_t size,
                        ohlc_bytes* response) {
    bool mutation =
        opcode == 7 || opcode == 8 || opcode == 9 || opcode == 13 || opcode == 14 || opcode == 15;
    uint64_t previous = client->request;
    ohlc_status status = start_request(client, opcode, body, size);
    ohlc_frame frame;
    if (status == OHLC_OK) {
        status = receive(client, &frame);
        if (status == OHLC_OK && frame.flags != 3) {
            status = fail_connection(client, OHLC_CORRUPT);
        }
    }
    if (status != OHLC_OK) {
        return mutation && client->request != previous ? OHLC_OUTCOME_UNKNOWN : status;
    }
    if (frame.status != 0) {
        return (ohlc_status)frame.status;
    }
    *response = (ohlc_bytes){client->buffer, frame.size};
    return OHLC_OK;
}

ohlc_status ohlc_client_connect(const ohlc_connection_options* options, ohlc_client** output) {
    if (output == NULL) {
        return OHLC_INVALID;
    }
    *output = NULL;
    if (options == NULL || options->timeout_ms == 0 || options->token.size > 4096 ||
        (options->token.size != 0 && options->token.data == NULL)) {
        return OHLC_INVALID;
    }
    ohlc_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return OHLC_LIMIT;
    }
    client->transport.fd = -1;
    client->timeout_ms = options->timeout_ms;
    client->info.max_frame_bytes = OHLC_FRAME_LIMIT;
    ohlc_status status = connect_transport(client, options);
    if (status == OHLC_OK) {
        uint8_t body[4104];
        ohlc_put_u32(body, (uint32_t)options->token.size);
        if (options->token.size != 0) {
            memcpy(body + 4, options->token.data, options->token.size);
        }
        ohlc_bytes response;
        status = call(client, 1, body, 4u + options->token.size, &response);
        OPENSSL_cleanse(body, sizeof(body));
        if (status == OHLC_OK) {
            const uint8_t* bytes = response.data;
            if (response.size != 32 || ohlc_get_u32(bytes + 16) > OHLC_FRAME_LIMIT ||
                ohlc_get_u32(bytes + 16) < 32 || ohlc_get_u32(bytes + 20) == 0 ||
                ohlc_get_u32(bytes + 20) > OHLC_MAX_BATCH_ROWS || ohlc_get_u32(bytes + 24) == 0 ||
                (ohlc_get_u32(bytes + 28) & ~3u) != 0) {
                status = OHLC_CORRUPT;
            } else {
                memcpy(client->info.uuid, bytes, 16);
                client->info.max_frame_bytes = ohlc_get_u32(bytes + 16);
                client->info.max_write_rows = ohlc_get_u32(bytes + 20);
                client->info.max_query_ms = ohlc_get_u32(bytes + 24);
                client->info.capabilities = ohlc_get_u32(bytes + 28);
            }
        }
    }
    if (status != OHLC_OK) {
        ohlc_client_close(client);
    } else {
        *output = client;
    }
    return status;
}

void ohlc_client_close(ohlc_client* client) {
    if (client != NULL) {
        ohlc_net_close(&client->transport);
        free(client->buffer);
        free(client);
    }
}

bool ohlc_client_connected(const ohlc_client* client) {
    return client != NULL && client->transport.fd >= 0;
}

int ohlc_client_socket(const ohlc_client* client) {
    return client != NULL ? client->transport.fd : -1;
}

void ohlc_client_info(const ohlc_client* client, ohlc_connection_info* output) {
    *output = client->info;
}

ohlc_status ohlc_client_call(ohlc_client* client, uint16_t opcode, const void* body, size_t size,
                             ohlc_bytes* response) {
    if (client == NULL || response == NULL || opcode < 2 || opcode > 15 || opcode == 5 ||
        opcode == 6) {
        return OHLC_INVALID;
    }
    *response = (ohlc_bytes){0};
    return call(client, opcode, body, size, response);
}

ohlc_status ohlc_client_stats(ohlc_client* client, ohlc_stats* output) {
    if (output == NULL) {
        return OHLC_INVALID;
    }
    ohlc_bytes response;
    ohlc_status status = ohlc_client_call(client, 12, NULL, 0, &response);
    if (status != OHLC_OK) {
        return status;
    }
    const uint8_t* body = response.data;
    if (response.size != 88 || ohlc_get_u64(body + 24) > UINT32_MAX) {
        return fail_connection(client, OHLC_CORRUPT);
    }
    *output = (ohlc_stats){
        .commit_seq = ohlc_get_u64(body),
        .checkpoint_seq = ohlc_get_u64(body + 8),
        .ticker_count = ohlc_get_u64(body + 16),
        .table_count = (uint32_t)ohlc_get_u64(body + 24),
        .memory_bytes = ohlc_get_u64(body + 32),
        .disk_read_bytes = ohlc_get_u64(body + 40),
        .disk_read_calls = ohlc_get_u64(body + 48),
        .cache_hits = ohlc_get_u64(body + 56),
        .cache_misses = ohlc_get_u64(body + 64),
        .wal_bytes = ohlc_get_u64(body + 72),
        .data_bytes = ohlc_get_u64(body + 80),
    };
    return OHLC_OK;
}

ohlc_status ohlc_client_checkpoint(ohlc_client* client) {
    ohlc_bytes response;
    ohlc_status status = ohlc_client_call(client, 13, NULL, 0, &response);
    if (status == OHLC_OK && response.size != 0) {
        return fail_connection(client, OHLC_OUTCOME_UNKNOWN);
    }
    return status;
}

static ohlc_status ticker_call(ohlc_client* client, uint16_t opcode, uint32_t table_id,
                               ohlc_bytes ticker, uint32_t* code, uint64_t* sequence) {
    if (ticker.size == 0 || ticker.size > 4096 || ticker.data == NULL || code == NULL) {
        return OHLC_INVALID;
    }
    uint8_t body[4104];
    ohlc_put_u32(body, table_id);
    ohlc_put_u32(body + 4, (uint32_t)ticker.size);
    memcpy(body + 8, ticker.data, ticker.size);
    ohlc_bytes response;
    ohlc_status status = ohlc_client_call(client, opcode, body, ticker.size + 8, &response);
    if (status == OHLC_OK) {
        if (response.size != (opcode == 3 ? 4u : 12u)) {
            return fail_connection(client, opcode == 3 ? OHLC_CORRUPT : OHLC_OUTCOME_UNKNOWN);
        }
        *code = ohlc_get_u32(response.data);
        if (sequence != NULL) {
            *sequence = ohlc_get_u64((const uint8_t*)response.data + 4);
        }
    }
    return status;
}

ohlc_status ohlc_client_resolve(ohlc_client* client, uint32_t table_id, ohlc_bytes ticker,
                                uint32_t* code) {
    return ticker_call(client, 3, table_id, ticker, code, NULL);
}

ohlc_status ohlc_client_register(ohlc_client* client, uint32_t table_id, ohlc_bytes ticker,
                                 uint32_t* code, uint64_t* sequence) {
    return ticker_call(client, 7, table_id, ticker, code, sequence);
}

ohlc_status ohlc_client_write_named(ohlc_client* client, uint32_t table_id,
                                    const ohlc_bytes* tickers, size_t ticker_count,
                                    const void* rows, size_t count, uint64_t* sequence) {
    if (client == NULL || sequence == NULL) {
        return OHLC_INVALID;
    }
    if (count > client->info.max_write_rows) {
        return OHLC_LIMIT;
    }
    ohlc_allocator allocator = {.used = 0, .limit = OHLC_FRAME_LIMIT * 2u};
    uint8_t* payload = NULL;
    size_t size = 0;
    ohlc_status status =
        ohlc_named_pack(&allocator, tickers, ticker_count, rows, count, &payload, &size);
    if (status != OHLC_OK) {
        return status;
    }
    if (size + 8u > client->info.max_frame_bytes) {
        ohlc_free(&allocator, payload);
        return OHLC_LIMIT;
    }
    uint8_t* body = ohlc_alloc(&allocator, size + 8u);
    if (body == NULL) {
        ohlc_free(&allocator, payload);
        return OHLC_LIMIT;
    }
    ohlc_put_u32(body, table_id);
    ohlc_put_u32(body + 4, (uint32_t)count);
    memcpy(body + 8, payload, size);
    ohlc_bytes response;
    status = ohlc_client_call(client, 15, body, size + 8u, &response);
    if (status == OHLC_OK) {
        if (response.size != 8) {
            status = fail_connection(client, OHLC_OUTCOME_UNKNOWN);
        } else {
            *sequence = ohlc_get_u64(response.data);
        }
    }
    ohlc_free(&allocator, body);
    ohlc_free(&allocator, payload);
    return status;
}

ohlc_status ohlc_client_table_open(ohlc_client* client, const char* name, ohlc_table_info* output) {
    if (name == NULL || output == NULL || strlen(name) > 63) {
        return OHLC_INVALID;
    }
    uint8_t body[68];
    size_t size = strlen(name);
    ohlc_put_u32(body, (uint32_t)size);
    memcpy(body + 4, name, size);
    ohlc_bytes response;
    ohlc_status status = ohlc_client_call(client, 10, body, size + 4, &response);
    if (status == OHLC_OK) {
        size_t consumed = 0;
        status = ohlc_net_table_decode(response.data, response.size, output, &consumed);
        if (status != OHLC_OK || consumed != response.size) {
            return fail_connection(client, OHLC_CORRUPT);
        }
    }
    return status;
}

ohlc_status ohlc_client_table_create(ohlc_client* client, const ohlc_table_definition* definition,
                                     ohlc_table_info* output) {
    ohlc_status status = ohlc_definition_validate(definition);
    if (status != OHLC_OK || output == NULL) {
        return OHLC_INVALID;
    }
    ohlc_table_info info = {.period_unit = definition->period_unit,
                            .period_count = definition->period_count};
    strcpy(info.name, definition->name);
    strcpy(info.timezone, definition->timezone);
    strcpy(info.description, definition->description);
    uint8_t body[4500];
    size_t size = ohlc_definition_encode(body, &info);
    ohlc_bytes response;
    status = ohlc_client_call(client, 9, body, size, &response);
    if (status == OHLC_OK) {
        if (response.size != 12 || ohlc_get_u32(response.data) == 0 ||
            ohlc_get_u64((const uint8_t*)response.data + 4) == 0) {
            return fail_connection(client, OHLC_OUTCOME_UNKNOWN);
        }
        info.id = ohlc_get_u32(response.data);
        info.created_seq = ohlc_get_u64((const uint8_t*)response.data + 4);
        *output = info;
    }
    return status;
}

ohlc_status ohlc_client_table_drop(ohlc_client* client, uint32_t table_id, uint64_t* sequence) {
    if (table_id == 0 || sequence == NULL) {
        return OHLC_INVALID;
    }
    uint8_t body[4];
    ohlc_put_u32(body, table_id);
    ohlc_bytes response;
    ohlc_status status = ohlc_client_call(client, 14, body, sizeof(body), &response);
    if (status == OHLC_OK) {
        if (response.size != 8 || ohlc_get_u64(response.data) == 0) {
            return fail_connection(client, OHLC_OUTCOME_UNKNOWN);
        }
        *sequence = ohlc_get_u64(response.data);
    }
    return status;
}

ohlc_status ohlc_client_write(ohlc_client* client, uint32_t table_id, const void* encoded_rows,
                              size_t count, uint64_t* sequence) {
    if (client == NULL || encoded_rows == NULL || count == 0 || sequence == NULL) {
        return OHLC_INVALID;
    }
    if (count > client->info.max_write_rows ||
        count > (client->info.max_frame_bytes - 8u) / OHLC_WRITE_BYTES) {
        return OHLC_LIMIT;
    }
    size_t size = 8 + count * OHLC_WRITE_BYTES;
    uint8_t* body = malloc(size);
    if (body == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_put_u32(body, table_id);
    ohlc_put_u32(body + 4, (uint32_t)count);
    memcpy(body + 8, encoded_rows, count * OHLC_WRITE_BYTES);
    ohlc_bytes response;
    ohlc_status status = ohlc_client_call(client, 8, body, size, &response);
    free(body);
    if (status == OHLC_OK) {
        if (response.size != 8 || ohlc_get_u64(response.data) == 0) {
            return fail_connection(client, OHLC_OUTCOME_UNKNOWN);
        }
        *sequence = ohlc_get_u64(response.data);
    }
    return status;
}

static ohlc_status query_start(ohlc_client* client, uint16_t opcode, const uint8_t* body,
                               size_t size) {
    ohlc_status status = start_request(client, opcode, body, size);
    if (status == OHLC_OK) {
        client->table = ohlc_get_u32(body);
        client->active = true;
        client->emitted = 0;
        client->sequence = 0;
        client->start = opcode == 5 ? ohlc_get_u32(body + 8) : 0;
        client->end = opcode == 5 ? ohlc_get_u64(body + 12) : UINT64_C(4294967296);
        uint32_t timeout = client->timeout_ms < client->info.max_query_ms
                               ? client->timeout_ms
                               : client->info.max_query_ms;
        client->deadline = ohlc_monotonic_ms() + timeout;
    }
    return status;
}

ohlc_status ohlc_client_series(ohlc_client* client, uint32_t table_id, uint32_t ticker,
                               uint32_t start, uint64_t end_exclusive) {
    if (end_exclusive > UINT64_C(4294967296) || end_exclusive < start) {
        return OHLC_INVALID;
    }
    uint8_t body[20];
    ohlc_put_u32(body, table_id);
    ohlc_put_u32(body + 4, ticker);
    ohlc_put_u32(body + 8, start);
    ohlc_put_u64(body + 12, end_exclusive);
    return query_start(client, 5, body, sizeof(body));
}

ohlc_status ohlc_client_cross(ohlc_client* client, uint32_t table_id, uint32_t time_key) {
    uint8_t body[8];
    ohlc_put_u32(body, table_id);
    ohlc_put_u32(body + 4, time_key);
    return query_start(client, 6, body, sizeof(body));
}

ohlc_status ohlc_client_cross_tickers(ohlc_client* client, uint32_t table_id, uint32_t time_key,
                                      const ohlc_bytes* tickers, size_t count) {
    if (client == NULL || (count != 0 && tickers == NULL)) {
        return OHLC_INVALID;
    }
    if (count > OHLC_MAX_QUERY_TICKERS) {
        return OHLC_LIMIT;
    }
    size_t size = 12;
    for (size_t i = 0; i < count; i++) {
        if (tickers[i].data == NULL || tickers[i].size == 0 || tickers[i].size > 4096) {
            return OHLC_INVALID;
        }
        size += 4 + tickers[i].size;
        if (size > client->info.max_frame_bytes) {
            return OHLC_LIMIT;
        }
    }
    uint8_t* body = malloc(size);
    if (body == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_put_u32(body, table_id);
    ohlc_put_u32(body + 4, time_key);
    ohlc_put_u32(body + 8, (uint32_t)count);
    size_t position = 12;
    for (size_t i = 0; i < count; i++) {
        ohlc_put_u32(body + position, (uint32_t)tickers[i].size);
        memcpy(body + position + 4, tickers[i].data, tickers[i].size);
        position += 4 + tickers[i].size;
    }
    ohlc_status status = query_start(client, 6, body, size);
    free(body);
    return status;
}

ohlc_status ohlc_client_next(ohlc_client* client, ohlc_bytes* rows, uint32_t* count,
                             uint64_t* snapshot_sequence, bool* final) {
    if (client == NULL || rows == NULL || count == NULL || snapshot_sequence == NULL ||
        final == NULL || !client->active) {
        return OHLC_INVALID;
    }
    *rows = (ohlc_bytes){0};
    *count = 0;
    *final = false;
    ohlc_frame frame;
    ohlc_status status = receive(client, &frame);
    if (status != OHLC_OK) {
        return status;
    }
    if (frame.status != 0) {
        client->active = false;
        return (ohlc_status)frame.status;
    }
    const uint8_t* body = client->buffer;
    if (frame.size < 32) {
        return fail_connection(client, OHLC_CORRUPT);
    }
    uint32_t length = ohlc_get_u32(body + 16);
    uint64_t sequence = ohlc_get_u64(body);
    if (length > (frame.size - 32u) / OHLC_RESULT_BYTES ||
        frame.size != 32u + length * OHLC_RESULT_BYTES || (frame.flags != 3 && length == 0) ||
        ohlc_get_u32(body + 20) != (uint32_t)(client->opcode - 4) ||
        ohlc_get_u32(body + 24) != client->table || ohlc_get_u32(body + 28) != 0 ||
        UINT64_MAX - client->emitted < length ||
        ohlc_get_u64(body + 8) != client->emitted + length ||
        (client->chunk > 1 && client->sequence != sequence)) {
        return fail_connection(client, OHLC_CORRUPT);
    }
    for (uint32_t i = 0; i < length; i++) {
        uint32_t key = ohlc_get_u32(body + 32u + i * OHLC_RESULT_BYTES);
        if ((client->emitted + i != 0 && key <= client->last_key) || key < client->start ||
            key >= client->end) {
            return fail_connection(client, OHLC_CORRUPT);
        }
        client->last_key = key;
    }
    client->emitted += length;
    client->sequence = sequence;
    *snapshot_sequence = sequence;
    *rows = (ohlc_bytes){body + 32, (size_t)length * OHLC_RESULT_BYTES};
    *count = length;
    *final = frame.flags == 3;
    if (*final) {
        client->active = false;
    }
    return OHLC_OK;
}

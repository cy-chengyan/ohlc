/* SPDX-License-Identifier: Apache-2.0 */
#include "net.h"
#include "server_config.h"

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#define OHLC_RESPONSE_LIMIT (256u * 4500u + 12u)

typedef struct {
    ohlc_db* db;
    SSL_CTX* tls;
    uint8_t read_token[4096];
    uint8_t write_token[4096];
    size_t read_token_size;
    size_t write_token_size;
    ohlc_allocator buffers;
    uint32_t timeout_ms;
    uint32_t query_ms;
    _Atomic bool stopping;
} server;

typedef struct {
    server* owner;
    ohlc_transport transport;
    pthread_t thread;
    _Atomic bool done;
    bool started;
} connection;

static volatile sig_atomic_t interrupted;

static void signal_stop(int signal_number) {
    (void)signal_number;
    interrupted = 1;
}

/* The notification protocol needs only a Unix datagram, not libsystemd.
 * Absence of NOTIFY_SOCKET keeps standalone execution unchanged. */
static bool service_notify(const char* message) {
    const char* path = getenv("NOTIFY_SOCKET");
    if (path == NULL) {
        return true;
    }
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    size_t length = strlen(path);
    if (length < 2 || length >= sizeof(address.sun_path) || (path[0] != '/' && path[0] != '@')) {
        return false;
    }
    memcpy(address.sun_path, path, length + 1);
    if (path[0] == '@') {
        address.sun_path[0] = '\0';
    }
    socklen_t size =
        (socklen_t)(offsetof(struct sockaddr_un, sun_path) + length + (path[0] == '/' ? 1u : 0u));
    int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (fd < 0) {
        return false;
    }
    ssize_t sent;
    do {
        sent = sendto(fd, message, strlen(message), 0, (struct sockaddr*)&address, size);
    } while (sent < 0 && errno == EINTR);
    close(fd);
    return sent >= 0 && (size_t)sent == strlen(message);
}

static bool string_body(const uint8_t* body, size_t size, ohlc_bytes* output) {
    if (size < 4 || ohlc_get_u32(body) != size - 4) {
        return false;
    }
    *output = (ohlc_bytes){body + 4, size - 4};
    return true;
}

static uint32_t authenticate(server* instance, ohlc_bytes token) {
    if (instance->read_token_size == 0 && instance->write_token_size == 0) {
        return token.size == 0 ? OHLC_CAP_READ | OHLC_CAP_WRITE : 0;
    }
    if (instance->write_token_size != 0 && token.size == instance->write_token_size &&
        CRYPTO_memcmp(token.data, instance->write_token, token.size) == 0) {
        return OHLC_CAP_READ | OHLC_CAP_WRITE;
    }
    if (instance->read_token_size != 0 && token.size == instance->read_token_size &&
        CRYPTO_memcmp(token.data, instance->read_token, token.size) == 0) {
        return OHLC_CAP_READ;
    }
    return 0;
}

static ohlc_status dispatch(server* instance, const ohlc_frame* request, const uint8_t* body,
                            uint32_t capabilities, uint8_t* output, uint32_t* output_size) {
    ohlc_db* db = instance->db;
    size_t size = request->size;
    uint16_t opcode = request->opcode;
    *output_size = 0;
    if ((opcode == 7 || opcode == 8 || opcode == 9 || opcode == 13) &&
        (capabilities & OHLC_CAP_WRITE) == 0) {
        return OHLC_UNAUTHORIZED;
    }
    if (opcode == 2) {
        return size == 0 ? OHLC_OK : OHLC_INVALID;
    }
    if (opcode == 12) {
        if (size != 0) {
            return OHLC_INVALID;
        }
        ohlc_stats stats;
        ohlc_get_stats(db, &stats);
        uint64_t values[] = {stats.commit_seq,      stats.checkpoint_seq, stats.ticker_count,
                             stats.table_count,     stats.memory_bytes,   stats.disk_read_bytes,
                             stats.disk_read_calls, stats.cache_hits,     stats.cache_misses,
                             stats.wal_bytes,       stats.data_bytes};
        for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
            ohlc_put_u64(output + i * 8, values[i]);
        }
        *output_size = sizeof(values);
        return OHLC_OK;
    }
    if (opcode == 13) {
        return size == 0 ? ohlc_checkpoint(db) : OHLC_INVALID;
    }
    if (opcode == 3 || opcode == 7) {
        ohlc_bytes ticker;
        if (!string_body(body, size, &ticker)) {
            return OHLC_INVALID;
        }
        uint32_t code;
        uint64_t sequence;
        ohlc_status status = opcode == 3 ? ohlc_resolve(db, ticker, &code)
                                         : ohlc_register(db, ticker, &code, &sequence);
        if (status == OHLC_OK) {
            ohlc_put_u32(output, code);
            *output_size = 4;
            if (opcode == 7) {
                ohlc_put_u64(output + 4, sequence);
                *output_size = 12;
            }
        }
        return status;
    }
    if (opcode == 4) {
        if (size != 8) {
            return OHLC_INVALID;
        }
        uint64_t start = ohlc_get_u32(body);
        uint32_t limit = ohlc_get_u32(body + 4);
        if (limit == 0 || limit > 256) {
            return OHLC_INVALID;
        }
        ohlc_stats stats;
        ohlc_get_stats(db, &stats);
        size_t position = 12;
        uint32_t count = 0;
        while (start < stats.ticker_count && count < limit) {
            ohlc_bytes ticker;
            ohlc_status status = ohlc_ticker(db, (uint32_t)start, &ticker);
            if (status != OHLC_OK) {
                return status;
            }
            ohlc_put_u32(output + position, (uint32_t)start);
            ohlc_put_u32(output + position + 4, (uint32_t)ticker.size);
            memcpy(output + position + 8, ticker.data, ticker.size);
            position += 8 + ticker.size;
            count++;
            start++;
        }
        ohlc_put_u64(output, stats.commit_seq);
        ohlc_put_u32(output + 8, count);
        *output_size = (uint32_t)position;
        return OHLC_OK;
    }
    if (opcode == 8) {
        if (size < 8 || ohlc_get_u32(body + 4) > OHLC_MAX_BATCH_ROWS ||
            size != 8u + (uint64_t)ohlc_get_u32(body + 4) * OHLC_WRITE_BYTES) {
            return OHLC_INVALID;
        }
        uint64_t sequence;
        ohlc_status status =
            ohlc_write(db, ohlc_get_u32(body), body + 8, ohlc_get_u32(body + 4), &sequence);
        if (status == OHLC_OK) {
            ohlc_put_u64(output, sequence);
            *output_size = 8;
        }
        return status;
    }
    if (opcode == 9) {
        ohlc_table_info info = {0};
        if (ohlc_definition_decode(body, size, &info) != OHLC_OK) {
            return OHLC_INVALID;
        }
        ohlc_table_definition definition = {info.name, info.period_unit, info.period_count,
                                            info.timezone, info.description};
        ohlc_table_info created;
        ohlc_status status = ohlc_table_create(db, &definition, &created);
        if (status == OHLC_OK) {
            ohlc_put_u32(output, created.id);
            ohlc_put_u64(output + 4, created.created_seq);
            *output_size = 12;
        }
        return status;
    }
    if (opcode == 10) {
        ohlc_bytes name;
        if (!string_body(body, size, &name) || name.size == 0 || name.size > 63 ||
            memchr(name.data, 0, name.size) != NULL) {
            return OHLC_INVALID;
        }
        char text[64];
        memcpy(text, name.data, name.size);
        text[name.size] = '\0';
        ohlc_table_info info;
        ohlc_status status = ohlc_table_open(db, text, &info);
        if (status == OHLC_OK) {
            *output_size = (uint32_t)ohlc_net_table_encode(output, &info);
        }
        return status;
    }
    if (opcode == 11) {
        if (size != 8 || ohlc_get_u32(body + 4) == 0 || ohlc_get_u32(body + 4) > 256) {
            return OHLC_INVALID;
        }
        size_t count;
        uint64_t sequence;
        uint32_t limit = ohlc_get_u32(body + 4);
        ohlc_table_info* tables = ohlc_alloc(&instance->buffers, limit * sizeof(*tables));
        if (tables == NULL) {
            return OHLC_BUSY;
        }
        ohlc_status status =
            ohlc_table_list(db, ohlc_get_u32(body), tables, limit, &count, &sequence);
        if (status == OHLC_OK) {
            size_t position = 12;
            ohlc_put_u64(output, sequence);
            ohlc_put_u32(output + 8, (uint32_t)count);
            for (size_t i = 0; i < count; i++) {
                position += ohlc_net_table_encode(output + position, &tables[i]);
            }
            *output_size = (uint32_t)position;
        }
        ohlc_free(&instance->buffers, tables);
        return status;
    }
    return OHLC_UNSUPPORTED;
}

static ohlc_status send_query(connection* peer, const ohlc_frame* request, const uint8_t* body,
                              uint8_t* output, uint64_t deadline) {
    ohlc_cursor* cursor = NULL;
    ohlc_status status = OHLC_INVALID;
    if (request->opcode == 5 && request->size == 20) {
        status = ohlc_series(peer->owner->db, ohlc_get_u32(body), ohlc_get_u32(body + 4),
                             ohlc_get_u32(body + 8), ohlc_get_u64(body + 12), &cursor);
    } else if (request->opcode == 6 && request->size == 8) {
        status = ohlc_cross(peer->owner->db, ohlc_get_u32(body), ohlc_get_u32(body + 4), &cursor);
    }
    uint64_t emitted = 0;
    ohlc_frame response = {.opcode = request->opcode, .request = request->request};
    for (;;) {
        size_t count = 0;
        if (status == OHLC_OK) {
            status = ohlc_cursor_next(cursor, output + 32, OHLC_NET_CHUNK_ROWS, &count);
        }
        response.status = (uint32_t)status;
        response.flags = status != OHLC_OK || count == 0 ? 3 : 1;
        response.size = 0;
        if (status == OHLC_OK) {
            emitted += count;
            ohlc_put_u64(output, ohlc_cursor_sequence(cursor));
            ohlc_put_u64(output + 8, emitted);
            ohlc_put_u32(output + 16, (uint32_t)count);
            ohlc_put_u32(output + 20, (uint32_t)(request->opcode - 4));
            ohlc_put_u32(output + 24, ohlc_get_u32(body));
            ohlc_put_u32(output + 28, 0);
            response.size = 32u + (uint32_t)count * OHLC_RESULT_BYTES;
        }
        ohlc_status sent = ohlc_net_send(&peer->transport, &response, output, deadline);
        if (sent != OHLC_OK || response.flags == 3) {
            ohlc_cursor_close(cursor);
            return sent;
        }
        response.chunk++;
    }
}

static void* serve_connection(void* argument) {
    connection* peer = argument;
    server* instance = peer->owner;
    uint8_t* output = ohlc_alloc(&instance->buffers, OHLC_RESPONSE_LIMIT);
    uint32_t capabilities = 0;
    uint64_t previous_request = 0;
    bool hello = false;
    if (instance->tls != NULL) {
        peer->transport.ssl = SSL_new(instance->tls);
        if (peer->transport.ssl == NULL || ohlc_net_tls_attach(&peer->transport) != OHLC_OK ||
            ohlc_net_handshake(&peer->transport, true,
                               ohlc_monotonic_ms() + instance->timeout_ms) != OHLC_OK) {
            goto finished;
        }
    }
    while (output != NULL && !atomic_load(&instance->stopping)) {
        uint64_t deadline = ohlc_monotonic_ms() + instance->timeout_ms;
        ohlc_frame request;
        if (ohlc_net_header(&peer->transport, &request, deadline) != OHLC_OK ||
            request.flags != 0 || request.status != 0 || request.chunk != 0 ||
            request.request <= previous_request || (!hello && request.opcode != 1) ||
            (!hello && request.size > 4100) || (hello && request.opcode == 1)) {
            break;
        }
        previous_request = request.request;
        uint8_t* body = ohlc_alloc(&instance->buffers, request.size == 0 ? 1 : request.size);
        if (body == NULL) {
            ohlc_frame busy = {.opcode = request.opcode,
                               .request = request.request,
                               .flags = 3,
                               .status = OHLC_BUSY};
            (void)ohlc_net_send(&peer->transport, &busy, NULL, deadline);
            break;
        }
        ohlc_status status = ohlc_net_io(&peer->transport, body, request.size, false, deadline);
        if (status != OHLC_OK) {
            ohlc_free(&instance->buffers, body);
            break;
        }
        ohlc_frame response = {.opcode = request.opcode, .request = request.request, .flags = 3};
        if (!hello) {
            ohlc_bytes token;
            if (string_body(body, request.size, &token)) {
                capabilities = authenticate(instance, token);
            }
            OPENSSL_cleanse(body, request.size);
            status = capabilities != 0 ? OHLC_OK : OHLC_UNAUTHORIZED;
            hello = status == OHLC_OK;
            if (hello) {
                ohlc_uuid(instance->db, output);
                ohlc_put_u32(output + 16, OHLC_FRAME_LIMIT);
                ohlc_put_u32(output + 20, OHLC_MAX_BATCH_ROWS);
                ohlc_put_u32(output + 24, instance->query_ms);
                ohlc_put_u32(output + 28, capabilities);
                response.size = 32;
            }
        } else if (request.opcode == 5 || request.opcode == 6) {
            status =
                send_query(peer, &request, body, output, ohlc_monotonic_ms() + instance->query_ms);
            ohlc_free(&instance->buffers, body);
            if (status != OHLC_OK) {
                break;
            }
            continue;
        } else {
            status = dispatch(instance, &request, body, capabilities, output, &response.size);
        }
        ohlc_free(&instance->buffers, body);
        response.status = (uint32_t)status;
        if (status != OHLC_OK) {
            response.size = 0;
        }
        if (ohlc_net_send(&peer->transport, &response, output, deadline) != OHLC_OK || !hello) {
            break;
        }
    }
finished:
    ohlc_free(&instance->buffers, output);
    /* The accepting thread closes the socket only after joining this worker. */
    SSL_free(peer->transport.ssl);
    peer->transport.ssl = NULL;
    atomic_store(&peer->done, true);
    return NULL;
}

static void* checkpoint_worker(void* argument) {
    server* instance = argument;
    ohlc_status previous = OHLC_OK;
    uint64_t last_report = 0;
    while (!atomic_load(&instance->stopping)) {
        struct timespec delay = {.tv_nsec = 100000000};
        nanosleep(&delay, NULL);
        ohlc_status status = ohlc_checkpoint_background(instance->db);
        uint64_t now = ohlc_monotonic_ms();
        if (status != OHLC_OK && (status != previous || now - last_report >= 30000)) {
            fprintf(stderr, "checkpoint: %s\n", ohlc_status_string(status));
            last_report = now;
        } else if (status == OHLC_OK && previous != OHLC_OK) {
            fputs("checkpoint: recovered\n", stderr);
        }
        previous = status;
    }
    return NULL;
}

int main(int argc, char** argv) {
    if (argc == 2 && strcmp(argv[1], "--version") == 0) {
        printf("ohlcd %s (ABI %u, format %u, protocol 3)\n", ohlc_version(), OHLC_ABI_VERSION,
               OHLC_FORMAT_VERSION);
        return 0;
    }
    ohlc_server_config config;
    ohlc_config_result parsed = ohlc_server_config_parse(argc, argv, &config);
    if (parsed != OHLC_CONFIG_RUN) {
        return parsed == OHLC_CONFIG_EXIT ? 0 : 2;
    }
    const char* path = config.data;
    const char* socket_path = config.socket[0] != '\0' ? config.socket : NULL;
    const char* host = config.host;
    const char* certificate = config.certificate[0] != '\0' ? config.certificate : NULL;
    const char* private_key = config.private_key[0] != '\0' ? config.private_key : NULL;
    const char* read_token = config.read_token[0] != '\0' ? config.read_token : NULL;
    const char* write_token = config.write_token[0] != '\0' ? config.write_token : NULL;
    uint32_t port = config.port;
    uint32_t max_connections = config.connections;
    server instance = {.timeout_ms = config.timeout_ms, .query_ms = config.engine.max_query_ms};
    ohlc_status status =
        ohlc_net_token_file(read_token, instance.read_token, &instance.read_token_size);
    if (status == OHLC_OK) {
        status = ohlc_net_token_file(write_token, instance.write_token, &instance.write_token_size);
    }
    if (status != OHLC_OK) {
        fprintf(stderr, "Credential file: %s\n", ohlc_status_string(status));
        return 2;
    }
    struct sockaddr_storage address = {0};
    socklen_t address_size;
    if (socket_path != NULL) {
        struct sockaddr_un* unix_address = (struct sockaddr_un*)&address;
        if (strlen(socket_path) >= sizeof(unix_address->sun_path)) {
            fprintf(stderr, "Unix socket path is too long\n");
            return 2;
        }
        unix_address->sun_family = AF_UNIX;
        strcpy(unix_address->sun_path, socket_path);
        address_size = sizeof(*unix_address);
    } else {
        struct sockaddr_in* ipv4 = (struct sockaddr_in*)&address;
        struct sockaddr_in6* ipv6 = (struct sockaddr_in6*)&address;
        if (inet_pton(AF_INET, host, &ipv4->sin_addr) == 1) {
            ipv4->sin_family = AF_INET;
            ipv4->sin_port = htons((uint16_t)port);
            address_size = sizeof(*ipv4);
        } else if (inet_pton(AF_INET6, host, &ipv6->sin6_addr) == 1) {
            ipv6->sin6_family = AF_INET6;
            ipv6->sin6_port = htons((uint16_t)port);
            address_size = sizeof(*ipv6);
        } else {
            fprintf(stderr, "--host requires a numeric bind address\n");
            return 2;
        }
    }
    if (certificate != NULL) {
        instance.tls = SSL_CTX_new(TLS_server_method());
        if (instance.tls == NULL ||
            SSL_CTX_set_min_proto_version(instance.tls, TLS1_2_VERSION) != 1 ||
            SSL_CTX_use_certificate_chain_file(instance.tls, certificate) != 1 ||
            SSL_CTX_use_PrivateKey_file(instance.tls, private_key, SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(instance.tls) != 1) {
            fprintf(stderr, "Cannot initialize TLS\n");
            SSL_CTX_free(instance.tls);
            return 2;
        }
    }
    if (config.check_only) {
        SSL_CTX_free(instance.tls);
        puts("Configuration is valid; the database and listener were not opened.");
        return 0;
    }
    atomic_init(&instance.stopping, false);
    atomic_init(&instance.buffers.used, 0);
    instance.buffers.limit = config.network_bytes;
    status = ohlc_open(path, &config.engine, &instance.db);
    if (status != OHLC_OK) {
        fprintf(stderr, "Open database: %s\n", ohlc_status_string(status));
        SSL_CTX_free(instance.tls);
        return 1;
    }
    int listener = socket(address.ss_family, SOCK_STREAM, 0);
    int enabled = 1;
    if (listener >= 0) {
        (void)setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
    }
    mode_t old_mask = umask(0077);
    bool bound = listener >= 0 && ohlc_net_prepare(listener) == OHLC_OK &&
                 bind(listener, (struct sockaddr*)&address, address_size) == 0;
    umask(old_mask);
    if (!bound || listen(listener, 128) != 0) {
        fprintf(stderr, "Cannot bind/listen (an existing Unix socket is never removed)\n");
        if (listener >= 0) {
            close(listener);
        }
        if (bound && socket_path != NULL) {
            unlink(socket_path);
        }
        ohlc_close(instance.db);
        SSL_CTX_free(instance.tls);
        return 1;
    }
    connection* peers = calloc(max_connections, sizeof(*peers));
    pthread_t checkpoint_thread;
    if (peers == NULL ||
        pthread_create(&checkpoint_thread, NULL, checkpoint_worker, &instance) != 0) {
        free(peers);
        close(listener);
        if (socket_path != NULL) {
            unlink(socket_path);
        }
        ohlc_close(instance.db);
        SSL_CTX_free(instance.tls);
        return 1;
    }
    struct sigaction action = {.sa_handler = signal_stop};
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    bool notified = service_notify("READY=1\nSTATUS=Accepting connections");
    if (!notified) {
        fputs("Cannot notify the service manager\n", stderr);
        interrupted = 1;
    }
    fprintf(stderr, "ohlcd ready: %s%s%" PRIu32 "\n", socket_path != NULL ? socket_path : host,
            socket_path != NULL ? " connections=" : ":",
            socket_path != NULL ? max_connections : port);
    bool listener_failed = false;
    uint64_t last_accept_error = 0;
    while (!interrupted) {
        size_t available = max_connections;
        for (size_t i = 0; i < max_connections; i++) {
            if (peers[i].started && atomic_load(&peers[i].done)) {
                pthread_join(peers[i].thread, NULL);
                ohlc_net_close(&peers[i].transport);
                peers[i].started = false;
            }
            if (!peers[i].started) {
                available = i;
            }
        }
        struct pollfd descriptor = {.fd = listener, .events = POLLIN};
        int ready = poll(&descriptor, 1, 100);
        if (ready < 0 && errno != EINTR) {
            fprintf(stderr, "Listener poll failed: %s\n", strerror(errno));
            listener_failed = true;
            break;
        }
        if (ready > 0 && (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            fputs("Listener became unavailable\n", stderr);
            listener_failed = true;
            break;
        }
        if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
            continue;
        }
        int fd = accept(listener, NULL, NULL);
        if (fd < 0) {
            if (errno == EMFILE || errno == ENFILE || errno == ENOMEM || errno == ENOBUFS) {
                uint64_t now = ohlc_monotonic_ms();
                if (last_accept_error == 0 || now - last_accept_error >= 30000) {
                    fprintf(stderr, "Accept resource limit: %s\n", strerror(errno));
                    last_accept_error = now;
                }
                struct timespec delay = {.tv_nsec = 100000000};
                nanosleep(&delay, NULL);
            }
            continue;
        }
        if (available == max_connections || ohlc_net_prepare(fd) != OHLC_OK) {
            close(fd);
            continue;
        }
        connection* peer = &peers[available];
        peer->owner = &instance;
        peer->transport = (ohlc_transport){.fd = fd};
        atomic_init(&peer->done, false);
        if (pthread_create(&peer->thread, NULL, serve_connection, peer) != 0) {
            close(fd);
            continue;
        }
        peer->started = true;
    }
    (void)service_notify("STOPPING=1\nSTATUS=Draining connections and checkpointing");
    close(listener);
    atomic_store(&instance.stopping, true);
    for (size_t i = 0; i < max_connections; i++) {
        if (peers[i].started) {
            shutdown(peers[i].transport.fd, SHUT_RDWR);
            pthread_join(peers[i].thread, NULL);
            ohlc_net_close(&peers[i].transport);
        }
    }
    pthread_join(checkpoint_thread, NULL);
    free(peers);
    status = ohlc_checkpoint(instance.db);
    ohlc_status closed = ohlc_close(instance.db);
    if (socket_path != NULL) {
        unlink(socket_path);
    }
    SSL_CTX_free(instance.tls);
    OPENSSL_cleanse(instance.read_token, sizeof(instance.read_token));
    OPENSSL_cleanse(instance.write_token, sizeof(instance.write_token));
    if (status != OHLC_OK || closed != OHLC_OK) {
        fprintf(stderr, "Shutdown: %s / %s\n", ohlc_status_string(status),
                ohlc_status_string(closed));
        return 1;
    }
    return notified && !listener_failed ? 0 : 1;
}

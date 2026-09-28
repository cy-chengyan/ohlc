/* SPDX-License-Identifier: Apache-2.0 */
#ifndef OHLC_NET_H
#define OHLC_NET_H

#include "internal.h"
#include "ohlc/client.h"

#include <openssl/ssl.h>

#define OHLC_PROTOCOL_VERSION 4u
#define OHLC_NET_HEADER 32u
#define OHLC_NET_CHUNK_ROWS 16384u
#define OHLC_CAP_READ 1u
#define OHLC_CAP_WRITE 2u

typedef struct {
    int fd;
    SSL* ssl;
} ohlc_transport;

typedef struct {
    uint16_t opcode;
    uint32_t flags;
    uint32_t size;
    uint64_t request;
    uint32_t status;
    uint32_t chunk;
} ohlc_frame;

ohlc_status ohlc_net_prepare(int fd);
ohlc_status ohlc_net_tls_attach(ohlc_transport* transport);
void ohlc_net_close(ohlc_transport* transport);
ohlc_status ohlc_net_wait(int fd, short events, uint64_t deadline);
ohlc_status ohlc_net_handshake(ohlc_transport* transport, bool server, uint64_t deadline);
ohlc_status ohlc_net_io(ohlc_transport* transport, void* buffer, size_t size, bool writing,
                        uint64_t deadline);
ohlc_status ohlc_net_send(ohlc_transport* transport, const ohlc_frame* frame, const void* body,
                          uint64_t deadline);
ohlc_status ohlc_net_header(ohlc_transport* transport, ohlc_frame* frame, uint64_t deadline);
ohlc_status ohlc_net_token_file(const char* path, uint8_t output[4096], size_t* size);
size_t ohlc_net_table_encode(uint8_t* output, const ohlc_table_info* info);
ohlc_status ohlc_net_table_decode(const uint8_t* data, size_t size, ohlc_table_info* info,
                                  size_t* consumed);

#endif

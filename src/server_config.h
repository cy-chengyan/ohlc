/* SPDX-License-Identifier: Apache-2.0 */
#ifndef OHLC_SERVER_CONFIG_H
#define OHLC_SERVER_CONFIG_H

#include "ohlc/ohlc.h"

typedef struct {
    ohlc_options engine;
    char data[4096];
    char socket[4096];
    char host[256];
    char certificate[4096];
    char private_key[4096];
    char read_token[4096];
    char write_token[4096];
    uint32_t port;
    uint32_t connections;
    uint32_t timeout_ms;
    size_t network_bytes;
    bool tcp_explicit;
    bool check_only;
} ohlc_server_config;

typedef enum { OHLC_CONFIG_RUN, OHLC_CONFIG_EXIT, OHLC_CONFIG_ERROR } ohlc_config_result;

/* Load defaults, one explicit file, then command-line overrides. The output
 * owns all strings. Diagnostics go to stderr; no database or socket is opened.
 * Call once on the startup thread because getopt uses process-global state. */
ohlc_config_result ohlc_server_config_parse(int argc, char** argv, ohlc_server_config* config);

#endif

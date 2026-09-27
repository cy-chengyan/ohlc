/* SPDX-License-Identifier: Apache-2.0 */
#include "server_config.h"

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OHLC_CONFIG_LINE 8192u
#define OHLC_MIB UINT64_C(1048576)

typedef enum {
    OHLC_CONFIG_TEXT,
    OHLC_CONFIG_U32,
    OHLC_CONFIG_SIZE_MIB,
    OHLC_CONFIG_U64_MIB
} value_kind;

typedef struct {
    const char* name;
    value_kind kind;
    size_t offset;
    uint64_t minimum;
    uint64_t maximum;
} config_field;

/* Text maximums include space for the terminating NUL. Numeric bounds are in
 * the units named by the option; conversion happens only after validation. */
static const config_field fields[] = {
    {"data", OHLC_CONFIG_TEXT, offsetof(ohlc_server_config, data), 0, 4096},
    {"socket", OHLC_CONFIG_TEXT, offsetof(ohlc_server_config, socket), 0, 4096},
    {"host", OHLC_CONFIG_TEXT, offsetof(ohlc_server_config, host), 0, 256},
    {"port", OHLC_CONFIG_U32, offsetof(ohlc_server_config, port), 1, 65535},
    {"tls-cert", OHLC_CONFIG_TEXT, offsetof(ohlc_server_config, certificate), 0, 4096},
    {"tls-key", OHLC_CONFIG_TEXT, offsetof(ohlc_server_config, private_key), 0, 4096},
    {"read-token-file", OHLC_CONFIG_TEXT, offsetof(ohlc_server_config, read_token), 0, 4096},
    {"write-token-file", OHLC_CONFIG_TEXT, offsetof(ohlc_server_config, write_token), 0, 4096},
    {"connections", OHLC_CONFIG_U32, offsetof(ohlc_server_config, connections), 1, 1024},
    {"memory-mib", OHLC_CONFIG_SIZE_MIB, offsetof(ohlc_server_config, engine.memory_limit), 1,
     1048576},
    {"cache-mib", OHLC_CONFIG_SIZE_MIB, offsetof(ohlc_server_config, engine.cache_bytes), 0,
     524288},
    {"network-mib", OHLC_CONFIG_SIZE_MIB, offsetof(ohlc_server_config, network_bytes), 1, 1048576},
    {"timeout-ms", OHLC_CONFIG_U32, offsetof(ohlc_server_config, timeout_ms), 1, 3600000},
    {"query-ms", OHLC_CONFIG_U32, offsetof(ohlc_server_config, engine.max_query_ms), 1, 3600000},
    {"read-workers", OHLC_CONFIG_U32, offsetof(ohlc_server_config, engine.read_workers), 0, 16},
    {"wal-mib", OHLC_CONFIG_U64_MIB, offsetof(ohlc_server_config, engine.wal_segment_bytes), 16,
     1048576},
    {"data-volume-mib", OHLC_CONFIG_U64_MIB, offsetof(ohlc_server_config, engine.data_volume_bytes),
     1, 1048576},
    {"max-tables", OHLC_CONFIG_U32, offsetof(ohlc_server_config, engine.max_tables), 1, 1048576},
    {"max-cursors", OHLC_CONFIG_U32, offsetof(ohlc_server_config, engine.max_cursors), 1, 1048576},
};

#define OHLC_CONFIG_FIELDS (sizeof(fields) / sizeof(fields[0]))

static void usage(void) {
    puts("Usage: ohlcd [--config FILE] [--data DIR] [--socket PATH | --host IP --port PORT]\n"
         "  --config FILE                  Load key=value settings before CLI overrides\n"
         "  --check-config                 Validate settings, credentials and TLS, then exit\n"
         "  --version                      Print release, ABI, file and protocol versions\n"
         "  --tls-cert FILE --tls-key FILE   TLS certificate chain and private key\n"
         "  --read-token-file FILE          Read-only credential (mode 0600)\n"
         "  --write-token-file FILE         Read/write credential (mode 0600)\n"
         "  --connections N                 Connection bound (default 64, max 1024)\n"
         "  --memory-mib N                  Engine allocator cap (default 1024)\n"
         "  --cache-mib N                   Tile cache budget (default 64; 0 disables)\n"
         "  --network-mib N                 Shared network buffers (default 128)\n"
         "  --timeout-ms N                  Connection I/O timeout (default 30000)\n"
         "  --query-ms N                    Complete query deadline (default 30000)\n"
         "  --read-workers N                Shared disk read workers (0-16; default 0)\n"
         "  --wal-mib N                     WAL segment target (default 1024, minimum 16)\n"
         "  --data-volume-mib N             Data volume target (default 1048576)\n"
         "  --max-tables N                  Table bound (default 1024)\n"
         "  --max-cursors N                 Concurrent cursor bound (default 256)\n"
         "Default: 127.0.0.1:8765. Remote listeners require TLS and credentials.\n"
         "Relative paths use the working directory. Configuration changes require restart.\n"
         "SIGINT/SIGTERM drains workers and checkpoints before exit.");
}

static bool set_field(ohlc_server_config* config, size_t index, const char* value) {
    const config_field* field = &fields[index];
    void* target = (unsigned char*)config + field->offset;
    if (field->kind == OHLC_CONFIG_TEXT) {
        size_t length = strlen(value);
        if (length == 0 || length >= field->maximum) {
            return false;
        }
        memcpy(target, value, length + 1);
    } else {
        if (value[0] < '0' || value[0] > '9') {
            return false;
        }
        char* end = NULL;
        errno = 0;
        unsigned long long number = strtoull(value, &end, 10);
        if (errno != 0 || *end != '\0' || number < field->minimum || number > field->maximum) {
            return false;
        }
        if (field->kind == OHLC_CONFIG_U32) {
            *(uint32_t*)target = (uint32_t)number;
        } else if (field->kind == OHLC_CONFIG_SIZE_MIB) {
            if (number > SIZE_MAX / OHLC_MIB) {
                return false;
            }
            *(size_t*)target = (size_t)number * (size_t)OHLC_MIB;
        } else {
            *(uint64_t*)target = (uint64_t)number * OHLC_MIB;
        }
    }
    if (strcmp(field->name, "host") == 0 || strcmp(field->name, "port") == 0) {
        config->tcp_explicit = true;
    }
    return true;
}

static char* trim(char* text) {
    while (isspace((unsigned char)*text)) {
        text++;
    }
    size_t length = strlen(text);
    while (length != 0 && isspace((unsigned char)text[length - 1])) {
        text[--length] = '\0';
    }
    return text;
}

static bool load_file(ohlc_server_config* config, const char* path) {
    FILE* file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "%s: cannot open configuration: %s\n", path, strerror(errno));
        return false;
    }
    bool seen[OHLC_CONFIG_FIELDS] = {false};
    char line[OHLC_CONFIG_LINE];
    size_t number = 0;
    const char* error = NULL;
    for (;;) {
        size_t length = 0;
        int byte;
        while ((byte = fgetc(file)) != EOF && byte != '\n') {
            if (byte == 0 || length == sizeof(line) - 1) {
                error = byte == 0 ? "NUL is not allowed in configuration"
                                  : "configuration line is too long";
                break;
            }
            line[length++] = (char)byte;
        }
        if (byte == EOF && length == 0 && error == NULL) {
            break;
        }
        number++;
        if (error != NULL) {
            break;
        }
        line[length] = '\0';
        char* key = trim(line);
        if (*key == '\0' || *key == '#' || *key == ';') {
            continue;
        }
        char* equal = strchr(key, '=');
        if (equal == NULL) {
            error = "expected key=value";
            break;
        }
        *equal = '\0';
        key = trim(key);
        char* value = trim(equal + 1);
        length = strlen(value);
        if (length != 0 && value[0] == '"') {
            if (length < 2 || value[length - 1] != '"') {
                error = "unterminated quoted value";
                break;
            }
            value[--length] = '\0';
            value++;
        }
        size_t index = 0;
        while (index < OHLC_CONFIG_FIELDS && strcmp(key, fields[index].name) != 0) {
            index++;
        }
        if (index == OHLC_CONFIG_FIELDS) {
            error = "unknown configuration key";
        } else if (seen[index]) {
            error = "duplicate configuration key";
        } else if (!set_field(config, index, value)) {
            error = "invalid configuration value or range";
        }
        if (error != NULL) {
            break;
        }
        seen[index] = true;
    }
    if (error == NULL && ferror(file)) {
        error = "cannot read configuration";
    }
    if (fclose(file) != 0 && error == NULL) {
        error = "cannot close configuration";
    }
    if (error != NULL) {
        fprintf(stderr, "%s:%zu: %s\n", path, number, error);
    }
    return error == NULL;
}

ohlc_config_result ohlc_server_config_parse(int argc, char** argv, ohlc_server_config* config) {
    memset(config, 0, sizeof(*config));
    ohlc_options_init(&config->engine);
    config->engine.create_if_missing = true;
    config->engine.max_query_ms = 30000;
    strcpy(config->host, "127.0.0.1");
    config->port = 8765;
    config->connections = 64;
    config->timeout_ms = 30000;
    config->network_bytes = 128u * (size_t)OHLC_MIB;
    if (argc == 2 && strcmp(argv[1], "--help") == 0) {
        usage();
        return OHLC_CONFIG_EXIT;
    }
    struct option options[OHLC_CONFIG_FIELDS + 4] = {0};
    const char* overrides[OHLC_CONFIG_FIELDS] = {NULL};
    for (size_t i = 0; i < OHLC_CONFIG_FIELDS; i++) {
        options[i] = (struct option){fields[i].name, required_argument, NULL, (int)i};
    }
    options[OHLC_CONFIG_FIELDS] = (struct option){"config", required_argument, NULL, 1000};
    options[OHLC_CONFIG_FIELDS + 1] = (struct option){"check-config", no_argument, NULL, 1001};
    options[OHLC_CONFIG_FIELDS + 2] = (struct option){"help", no_argument, NULL, 1002};
    const char* path = NULL;
    for (;;) {
        int option = getopt_long(argc, argv, "", options, NULL);
        if (option == -1) {
            break;
        }
        if (option >= 0 && (size_t)option < OHLC_CONFIG_FIELDS) {
            overrides[option] = optarg;
        } else if (option == 1000 && path == NULL) {
            path = optarg;
        } else if (option == 1002) {
            usage();
            return OHLC_CONFIG_EXIT;
        } else if (option == 1001) {
            config->check_only = true;
        } else {
            fprintf(stderr, "Invalid arguments; use --help. Specify at most one --config.\n");
            return OHLC_CONFIG_ERROR;
        }
    }
    if (optind != argc || (path != NULL && !load_file(config, path))) {
        if (optind != argc) {
            fprintf(stderr, "Unexpected positional argument\n");
        }
        return OHLC_CONFIG_ERROR;
    }
    for (size_t i = 0; i < OHLC_CONFIG_FIELDS; i++) {
        if (overrides[i] != NULL && !set_field(config, i, overrides[i])) {
            fprintf(stderr, "Invalid value for --%s\n", fields[i].name);
            return OHLC_CONFIG_ERROR;
        }
    }
    if (config->data[0] == '\0' || (config->socket[0] != '\0' && config->tcp_explicit) ||
        (config->certificate[0] == '\0') != (config->private_key[0] == '\0') ||
        (config->socket[0] != '\0' && config->certificate[0] != '\0') ||
        config->engine.cache_bytes > config->engine.memory_limit / 2) {
        fprintf(stderr, "Invalid settings: data is required; socket/TCP and socket/TLS are "
                        "exclusive; TLS needs both files; cache-mib must not exceed half of "
                        "memory-mib\n");
        return OHLC_CONFIG_ERROR;
    }
    return OHLC_CONFIG_RUN;
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "shell.h"

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

volatile sig_atomic_t ohlc_shell_interrupted;
static volatile sig_atomic_t interrupt_socket = -1;

typedef struct symbol {
    struct symbol* next;
    size_t size;
    uint32_t code;
    uint32_t table_id;
    uint8_t bytes[];
} symbol;

typedef enum { SHELL_TABLE, SHELL_TSV, SHELL_CSV, SHELL_JSONL } output_format;

/* Command parsing distinguishes malformed command structure (unknown commands,
 * argument counts, option names and arity) from invalid argument values. */
typedef enum { PARSE_OK, PARSE_SYNTAX, PARSE_VALUE } parse_result;

typedef struct {
    ohlc_client* client;
    char* socket_path;
    char* host;
    char* port;
    char* ca_file;
    char* token_file;
    bool tls;
    bool interactive;
    bool timing;
    bool history;
    bool quitting;
    bool unknown;
    bool connection_error;
    bool syntax_error;
    /* Set once the current failure has been printed, so enclosing source levels
     * propagate it without printing it again. */
    bool error_reported;
    /* Interactive sessions announce each new connection identity on stderr. */
    bool banner;
    bool banner_shown;
    ohlc_connection_info banner_info;
    char unknown_command[32];
    uint64_t preview;
    /* Without an explicit format, output follows its destination. */
    bool format_set;
    output_format format;
    const char* output_path;
    ohlc_status last_status;
    unsigned int depth;
    symbol* symbols[1024];
    size_t symbol_bytes;
    ohlc_table_info tables[128];
    size_t table_count;
} shell;

/* A table description and whether it came from the session cache, in which
 * case its ID may be stale after another client re-created the table. */
typedef struct {
    ohlc_table_info info;
    bool cached;
} table_handle;

static shell* completion_state;

const char* ohlc_shell_complete(const char* prefix, size_t index) {
    size_t length = strlen(prefix);
    for (size_t i = 0; i < ohlc_shell_command_count; i++) {
        const char* name = ohlc_shell_commands[i].name;
        if (strncasecmp(name, prefix, length) == 0 && index-- == 0) {
            return name;
        }
    }
    static const char* options[] = {
        "--format",   "--output",      "--overwrite",  "--all",       "--period",
        "--timezone", "--description", "--batch-rows", "--no-header", "--socket",
        "--host",     "--port",        "--tls",        "--ca",        "--token-file",
        "from",       "and",           "ticker",       "in",          "keys",
        "examples"};
    for (size_t i = 0; i < sizeof(options) / sizeof(options[0]); i++) {
        if (strncmp(options[i], prefix, length) == 0 && index-- == 0) {
            return options[i];
        }
    }
    shell* state = completion_state;
    if (state == NULL) {
        return NULL;
    }
    for (size_t i = 0; i < state->table_count; i++) {
        if (strncmp(state->tables[i].name, prefix, length) == 0 && index-- == 0) {
            return state->tables[i].name;
        }
    }
    for (size_t i = 0; i < 1024; i++) {
        for (const symbol* entry = state->symbols[i]; entry != NULL; entry = entry->next) {
            bool simple = true;
            for (size_t j = 0; j < entry->size; j++) {
                uint8_t c = entry->bytes[j];
                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                      c == '_' || c == '.' || c == '-')) {
                    simple = false;
                    break;
                }
            }
            if (simple && length <= entry->size && memcmp(entry->bytes, prefix, length) == 0 &&
                index-- == 0) {
                return (const char*)entry->bytes;
            }
        }
    }
    return NULL;
}

static void interrupt(int signal_number) {
    (void)signal_number;
    ohlc_shell_interrupted = 1;
    int fd = interrupt_socket;
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
    }
}

static bool word_is(const shell_word* word, const char* text) {
    return word->size == strlen(text) && strncasecmp(word->text, text, word->size) == 0;
}

static bool text_word(const shell_word* word) {
    return word->text != NULL && memchr(word->text, 0, word->size) == NULL;
}

static const char* const format_names[] = {"table", "tsv", "csv", "jsonl"};

static bool format_parse(const shell_word* word, output_format* output) {
    for (size_t i = 0; i < sizeof(format_names) / sizeof(format_names[0]); i++) {
        if (word_is(word, format_names[i])) {
            *output = (output_format)i;
            return true;
        }
    }
    return false;
}

/* An explicit session format wins; otherwise a terminal gets a table and files
 * or pipes get TSV. */
static output_format effective_format(const shell* state, const char* path) {
    if (state->format_set) {
        return state->format;
    }
    return path == NULL && isatty(STDOUT_FILENO) ? SHELL_TABLE : SHELL_TSV;
}

static void disconnect(shell* state) {
    interrupt_socket = -1;
    ohlc_client_close(state->client);
    state->client = NULL;
    for (size_t i = 0; i < 1024; i++) {
        symbol* entry = state->symbols[i];
        while (entry != NULL) {
            symbol* next = entry->next;
            free(entry);
            entry = next;
        }
        state->symbols[i] = NULL;
    }
    state->symbol_bytes = 0;
    state->table_count = 0;
}

static const char* access_name(uint32_t capabilities) {
    return (capabilities & OHLC_CAP_WRITE) != 0 ? "read-write" : "read-only";
}

static void print_uuid(FILE* file, const uint8_t* uuid) {
    for (size_t i = 0; i < 16; i++) {
        fprintf(file, "%02x", uuid[i]);
    }
}

/* Announce the connected target and identity once per distinct connection, so
 * automatic reconnects after a cancelled preview stay quiet. */
static void print_banner(shell* state, const ohlc_connection_options* options) {
    ohlc_connection_info info;
    ohlc_client_info(state->client, &info);
    if (state->banner_shown && memcmp(info.uuid, state->banner_info.uuid, sizeof(info.uuid)) == 0 &&
        info.capabilities == state->banner_info.capabilities) {
        return;
    }
    state->banner_shown = true;
    state->banner_info = info;
    fputs("Connected to ", stderr);
    if (options->socket_path != NULL) {
        fputs("unix:", stderr);
        ohlc_shell_print_bytes(stderr, options->socket_path, strlen(options->socket_path));
    } else {
        bool bracket = strchr(options->host, ':') != NULL;
        fputs(options->tls ? "tls:" : "tcp:", stderr);
        if (bracket) {
            fputc('[', stderr);
        }
        ohlc_shell_print_bytes(stderr, options->host, strlen(options->host));
        if (bracket) {
            fputc(']', stderr);
        }
        fputc(':', stderr);
        ohlc_shell_print_bytes(stderr, options->port, strlen(options->port));
    }
    fputs(" uuid=", stderr);
    print_uuid(stderr, info.uuid);
    fprintf(stderr, " protocol=%u access=%s\n", OHLC_PROTOCOL_VERSION,
            access_name(info.capabilities));
}

static ohlc_status connect_server(shell* state) {
    disconnect(state);
    ohlc_connection_options options;
    ohlc_connection_options_init(&options);
    options.socket_path = state->socket_path;
    options.host = state->host != NULL ? state->host : "127.0.0.1";
    options.port = state->port != NULL ? state->port : "8765";
    options.ca_file = state->ca_file;
    options.tls = state->tls;
    uint8_t token[4096];
    size_t size;
    ohlc_status status = ohlc_net_token_file(state->token_file, token, &size);
    if (status == OHLC_OK) {
        options.token = (ohlc_bytes){token, size};
        status = ohlc_client_connect(&options, &state->client);
    }
    OPENSSL_cleanse(token, sizeof(token));
    if (status == OHLC_OK) {
        interrupt_socket = ohlc_client_socket(state->client);
        if (state->banner) {
            print_banner(state, &options);
        }
    }
    state->connection_error = status != OHLC_OK;
    return status;
}

static uint32_t symbol_bucket(const shell_word* ticker) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < ticker->size; i++) {
        hash = (hash ^ (uint8_t)ticker->text[i]) * 16777619u;
    }
    return hash & 1023u;
}

static void cache_symbol(shell* state, uint32_t table_id, const shell_word* ticker, uint32_t code) {
    if (state->symbol_bytes + sizeof(symbol) + ticker->size + 1 > 16u * 1024u * 1024u) {
        return;
    }
    uint32_t bucket = symbol_bucket(ticker);
    for (const symbol* entry = state->symbols[bucket]; entry != NULL; entry = entry->next) {
        if (entry->table_id == table_id && entry->size == ticker->size &&
            memcmp(entry->bytes, ticker->text, ticker->size) == 0) {
            return;
        }
    }
    symbol* entry = malloc(sizeof(*entry) + ticker->size + 1);
    if (entry == NULL) {
        return;
    }
    entry->table_id = table_id;
    entry->size = ticker->size;
    entry->code = code;
    memcpy(entry->bytes, ticker->text, ticker->size);
    entry->bytes[ticker->size] = 0;
    entry->next = state->symbols[bucket];
    state->symbols[bucket] = entry;
    state->symbol_bytes += sizeof(*entry) + ticker->size + 1;
}

static ohlc_status resolve(shell* state, uint32_t table_id, const shell_word* ticker,
                           uint32_t* code) {
    for (const symbol* entry = state->symbols[symbol_bucket(ticker)]; entry != NULL;
         entry = entry->next) {
        if (entry->table_id == table_id && entry->size == ticker->size &&
            memcmp(entry->bytes, ticker->text, ticker->size) == 0) {
            *code = entry->code;
            return OHLC_OK;
        }
    }
    ohlc_bytes bytes = {ticker->text, ticker->size};
    ohlc_status status = ohlc_client_resolve(state->client, table_id, bytes, code);
    if (status == OHLC_OK) {
        cache_symbol(state, table_id, ticker, *code);
    }
    return status;
}

static void cache_table(shell* state, const ohlc_table_info* table) {
    for (size_t i = 0; i < state->table_count; i++) {
        if (strcmp(table->name, state->tables[i].name) == 0) {
            state->tables[i] = *table;
            return;
        }
    }
    if (state->table_count < sizeof(state->tables) / sizeof(state->tables[0])) {
        state->tables[state->table_count++] = *table;
    }
}

static void evict_table(shell* state, const char* name) {
    for (size_t i = 0; i < state->table_count; i++) {
        if (strcmp(name, state->tables[i].name) == 0) {
            state->tables[i] = state->tables[--state->table_count];
            return;
        }
    }
}

/* Open a table by name on the server and refresh the cache entry for it. */
static ohlc_status reopen_table(shell* state, const char* name, table_handle* output) {
    output->cached = false;
    ohlc_status status = ohlc_client_table_open(state->client, name, &output->info);
    if (status == OHLC_OK) {
        cache_table(state, &output->info);
    } else if (status == OHLC_NOT_FOUND) {
        evict_table(state, name);
    }
    return status;
}

static ohlc_status open_table(shell* state, const shell_word* name, table_handle* output) {
    if (!text_word(name)) {
        return OHLC_INVALID;
    }
    for (size_t i = 0; i < state->table_count; i++) {
        if (strcmp(name->text, state->tables[i].name) == 0) {
            output->info = state->tables[i];
            output->cached = true;
            return OHLC_OK;
        }
    }
    return reopen_table(state, name->text, output);
}

/* After a definite NOT_FOUND for a request that used a cached table ID, reopen
 * the name once. Returns true only when the name now refers to a different
 * table, in which case the caller may repeat its definitely rejected request. */
static bool refresh_stale_table(shell* state, table_handle* table, ohlc_status status) {
    if (status != OHLC_NOT_FOUND || !table->cached || !ohlc_client_connected(state->client)) {
        return false;
    }
    char name[sizeof(table->info.name)];
    memcpy(name, table->info.name, sizeof(name));
    table_handle current;
    if (reopen_table(state, name, &current) != OHLC_OK || current.info.id == table->info.id) {
        return false;
    }
    *table = current;
    return true;
}

static ohlc_status parse_time(const ohlc_table_info* table, const shell_word* word,
                              uint32_t* output) {
    return text_word(word) ? ohlc_time_parse(table, word->text, output) : OHLC_INVALID;
}

static void print_definition(const ohlc_table_info* table) {
    printf("%" PRIu32 "\t%s\t%" PRIu32 "%s\t", table->id, table->name, table->period_count,
           ohlc_period_suffix(table->period_unit));
    ohlc_shell_print_bytes(stdout, table->timezone, strlen(table->timezone));
    fputc('\t', stdout);
    ohlc_shell_print_bytes(stdout, table->description, strlen(table->description));
    fputc('\n', stdout);
}

static ohlc_status list_metadata(shell* state, bool tables, uint32_t table_id,
                                 const shell_word* prefix) {
    uint64_t start = tables ? 1 : 0;
    while (start <= UINT32_MAX) {
        uint8_t request[12];
        size_t prefix_bytes = tables ? 0u : 4u;
        ohlc_put_u32(request, table_id);
        ohlc_put_u32(request + prefix_bytes, (uint32_t)start);
        ohlc_put_u32(request + prefix_bytes + 4, 128);
        ohlc_bytes response;
        ohlc_status status =
            ohlc_client_call(state->client, tables ? 11 : 4, request, 8u + prefix_bytes, &response);
        if (status != OHLC_OK) {
            return status;
        }
        if (response.size < 12) {
            return OHLC_CORRUPT;
        }
        const uint8_t* bytes = response.data;
        uint32_t count = ohlc_get_u32(bytes + 8);
        if (count > 128) {
            return OHLC_CORRUPT;
        }
        size_t position = 12;
        for (uint32_t i = 0; i < count; i++) {
            if (tables) {
                size_t consumed;
                ohlc_table_info table;
                status = ohlc_net_table_decode(bytes + position, response.size - position, &table,
                                               &consumed);
                if (status != OHLC_OK || table.id < start) {
                    return OHLC_CORRUPT;
                }
                print_definition(&table);
                position += consumed;
                start = (uint64_t)table.id + 1;
            } else {
                if (response.size - position < 8) {
                    return OHLC_CORRUPT;
                }
                uint32_t code = ohlc_get_u32(bytes + position);
                uint32_t length = ohlc_get_u32(bytes + position + 4);
                position += 8;
                if (code < start || length == 0 || length > 4096 ||
                    length > response.size - position) {
                    return OHLC_CORRUPT;
                }
                shell_word ticker = {.text = (char*)bytes + position, .size = length};
                cache_symbol(state, table_id, &ticker, code);
                if (prefix == NULL || (prefix->size <= length &&
                                       memcmp(prefix->text, ticker.text, prefix->size) == 0)) {
                    printf("%" PRIu32 "\t", code);
                    ohlc_shell_print_bytes(stdout, ticker.text, ticker.size);
                    fputc('\n', stdout);
                }
                position += length;
                start = (uint64_t)code + 1;
            }
        }
        if (position != response.size) {
            return OHLC_CORRUPT;
        }
        if (count == 0) {
            break;
        }
    }
    return ferror(stdout) ? OHLC_IO : OHLC_OK;
}

static parse_result parse_create(const shell_command* command, ohlc_table_definition* definition) {
    if (command->count < 2) {
        return PARSE_SYNTAX;
    }
    *definition =
        (ohlc_table_definition){.name = command->words[1].text, .timezone = "", .description = ""};
    parse_result result = text_word(&command->words[1]) ? PARSE_OK : PARSE_VALUE;
    bool period = false;
    for (size_t i = 2; i < command->count; i += 2) {
        const shell_word* option = &command->words[i];
        if (i + 1 == command->count ||
            (!word_is(option, "--period") && !word_is(option, "--timezone") &&
             !word_is(option, "--description"))) {
            return PARSE_SYNTAX;
        }
        const shell_word* value = &command->words[i + 1];
        if (word_is(option, "--period")) {
            period = true;
        }
        if (!text_word(value)) {
            result = PARSE_VALUE;
        } else if (word_is(option, "--period")) {
            if (ohlc_period_parse(value->text, &definition->period_unit,
                                  &definition->period_count) != OHLC_OK) {
                result = PARSE_VALUE;
            }
        } else if (word_is(option, "--timezone")) {
            definition->timezone = value->text;
        } else {
            definition->description = value->text;
        }
    }
    return period ? result : PARSE_SYNTAX;
}

static ohlc_status create_table(shell* state, const shell_command* command) {
    ohlc_table_definition definition;
    if (parse_create(command, &definition) != PARSE_OK) {
        return OHLC_INVALID;
    }
    ohlc_table_info info;
    ohlc_status status = ohlc_client_table_create(state->client, &definition, &info);
    if (status == OHLC_OK) {
        print_definition(&info);
        cache_table(state, &info);
        fprintf(stderr, "Created commit_seq=%" PRIu64 "\n", info.created_seq);
    }
    return status;
}

static ohlc_status drop_table(shell* state, const shell_command* command) {
    if (!text_word(&command->words[1])) {
        return OHLC_INVALID;
    }
    /* Resolve the current name rather than trusting a cached table handle.
     * The following mutation addresses that ID, so a concurrent replacement
     * can never be deleted accidentally. */
    table_handle table;
    ohlc_status status = reopen_table(state, command->words[1].text, &table);
    if (status == OHLC_OK) {
        uint64_t sequence;
        status = ohlc_client_table_drop(state->client, table.info.id, &sequence);
        evict_table(state, table.info.name);
        if (status == OHLC_OK) {
            fprintf(stderr, "Dropped %s commit_seq=%" PRIu64 "\n", table.info.name, sequence);
        }
    }
    return status;
}

static void print_row(FILE* file, output_format format, uint32_t key, const ohlc_row* row) {
    static const char* names[] = {"open",   "high",   "low",           "close",
                                  "volume", "amount", "adjust_factor", "reserved"};
    char values[8][32];
    snprintf(values[0], sizeof(values[0]), "%" PRId64, row->open);
    snprintf(values[1], sizeof(values[1]), "%" PRId64, row->high);
    snprintf(values[2], sizeof(values[2]), "%" PRId64, row->low);
    snprintf(values[3], sizeof(values[3]), "%" PRId64, row->close);
    snprintf(values[4], sizeof(values[4]), "%" PRIu64, row->volume);
    snprintf(values[5], sizeof(values[5]), "%" PRIu64, row->amount);
    snprintf(values[6], sizeof(values[6]), "%" PRIu64, row->adjust_factor);
    snprintf(values[7], sizeof(values[7]), "%" PRId32, row->reserved);
    if (format == SHELL_JSONL) {
        fprintf(file, "{\"key\":%" PRIu32, key);
        for (size_t i = 0; i < 8; i++) {
            if ((row->flags & (1u << i)) != 0) {
                fprintf(file, ",\"%s\":\"%s\"", names[i], values[i]);
            } else {
                fprintf(file, ",\"%s\":null", names[i]);
            }
        }
        fputs("}\n", file);
    } else {
        char separator = format == SHELL_CSV ? ',' : (format == SHELL_TABLE ? ' ' : '\t');
        fprintf(file, "%" PRIu32, key);
        for (size_t i = 0; i < 8; i++) {
            fprintf(file, "%c%s", separator, (row->flags & (1u << i)) != 0 ? values[i] : "NULL");
        }
        fputc('\n', file);
    }
}

/* Check the structure of "and ticker in (NAME, ...)". Name lengths are values
 * validated when the request is built. */
static bool cross_filter(const shell_command* command, size_t* options_start, size_t* count) {
    if (command->count < 8 || !word_is(&command->words[4], "ticker") ||
        !word_is(&command->words[5], "in") || command->words[6].punctuation != '(') {
        return false;
    }
    size_t position = 7;
    *count = 0;
    if (command->words[position].punctuation != ')') {
        for (;;) {
            const shell_word* ticker = &command->words[position];
            if (ticker->punctuation != 0) {
                return false;
            }
            (*count)++;
            if (++position == command->count) {
                return false;
            }
            if (command->words[position].punctuation == ')') {
                break;
            }
            if (command->words[position].punctuation != ',' || ++position == command->count) {
                return false;
            }
        }
    }
    *options_start = position + 1;
    return true;
}

static ohlc_status start_filtered_cross(shell* state, uint32_t table_id, uint32_t time_key,
                                        const shell_command* command, size_t count) {
    ohlc_bytes* tickers = count == 0 ? NULL : malloc(count * sizeof(*tickers));
    if (count != 0 && tickers == NULL) {
        return OHLC_LIMIT;
    }
    for (size_t i = 0; i < count; i++) {
        const shell_word* word = &command->words[7 + 2 * i];
        tickers[i] = (ohlc_bytes){word->text, word->size};
    }
    ohlc_status status =
        ohlc_client_cross_tickers(state->client, table_id, time_key, tickers, count);
    free(tickers);
    return status;
}

/* Options and structure of one SERIES or CROSS command. */
typedef struct {
    bool series;
    bool filtered;
    size_t ticker_count;
    bool format_set;
    output_format format;
    bool all;
    bool overwrite;
    const char* path;
} query_request;

static parse_result parse_query_options(const shell_command* command, size_t start,
                                        query_request* request) {
    parse_result result = PARSE_OK;
    for (size_t i = start; i < command->count; i++) {
        const shell_word* option = &command->words[i];
        if (word_is(option, "--all")) {
            request->all = true;
        } else if (word_is(option, "--overwrite")) {
            request->overwrite = true;
        } else if (word_is(option, "--format") && i + 1 < command->count) {
            if (format_parse(&command->words[++i], &request->format)) {
                request->format_set = true;
            } else {
                result = PARSE_VALUE;
            }
        } else if (word_is(option, "--output") && i + 1 < command->count) {
            if (text_word(&command->words[++i])) {
                request->path = command->words[i].text;
            } else {
                result = PARSE_VALUE;
            }
        } else {
            return PARSE_SYNTAX;
        }
    }
    return result;
}

static parse_result parse_query(const shell_command* command, bool series, query_request* request) {
    *request = (query_request){.series = series};
    size_t options_start = series ? 7 : 3;
    if (command->count < options_start) {
        return PARSE_SYNTAX;
    }
    if (series && (!word_is(&command->words[3], "from") || !word_is(&command->words[5], "and"))) {
        return PARSE_SYNTAX;
    }
    if (!series && command->count > 3 && word_is(&command->words[3], "and")) {
        request->filtered = true;
        if (!cross_filter(command, &options_start, &request->ticker_count)) {
            return PARSE_SYNTAX;
        }
    }
    return parse_query_options(command, options_start, request);
}

/* Destination of query rows: stdout, or a private temporary file that is
 * published under the target path only after the complete result is written. */
typedef struct {
    FILE* file;
    char* temporary;
} query_output;

typedef struct {
    uint64_t chunks;
    uint64_t shown;
    uint64_t received;
    uint64_t sequence;
    bool requested;
    bool stream_open;
    bool final;
    bool truncated;
} query_progress;

static ohlc_status output_absent(const char* path) {
    struct stat info;
    if (lstat(path, &info) == 0) {
        return OHLC_ALREADY_EXISTS;
    }
    return errno == ENOENT ? OHLC_OK : OHLC_IO;
}

static ohlc_status output_open(const char* path, query_output* output) {
    size_t length = strlen(path) + 24;
    output->temporary = malloc(length);
    if (output->temporary == NULL) {
        return OHLC_LIMIT;
    }
    snprintf(output->temporary, length, "%s.ohlc-XXXXXX", path);
    int fd = mkstemp(output->temporary);
    if (fd >= 0) {
        output->file = fdopen(fd, "w");
        if (output->file != NULL) {
            return OHLC_OK;
        }
        close(fd);
        unlink(output->temporary);
    }
    free(output->temporary);
    output->temporary = NULL;
    return OHLC_IO;
}

/* Regular files created by the shell get the usual 0666 mode filtered by the
 * umask. The shell is single-threaded, so reading the umask by briefly setting
 * it cannot race with other file creation. */
static mode_t file_creation_mode(void) {
    mode_t mask = umask(0);
    umask(mask);
    return 0666 & ~mask;
}

static ohlc_status sync_parent_directory(const char* path) {
    const char* slash = strrchr(path, '/');
    char* directory = NULL;
    if (slash == NULL) {
        directory = strdup(".");
    } else if (slash == path) {
        directory = strdup("/");
    } else {
        directory = strndup(path, (size_t)(slash - path));
    }
    if (directory == NULL) {
        return OHLC_LIMIT;
    }
    int fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    free(directory);
    if (fd < 0) {
        return OHLC_IO;
    }
    ohlc_status status = fsync(fd) == 0 ? OHLC_OK : OHLC_IO;
    close(fd);
    return status;
}

/* Close an export file. A complete result is made durable and published
 * atomically; otherwise the temporary file is removed and the target is left
 * untouched. Returns the final command status. */
static ohlc_status output_finish(query_output* output, const char* path, bool overwrite,
                                 ohlc_status status) {
    if (fflush(output->file) != 0 && status == OHLC_OK) {
        status = OHLC_IO;
    }
    int fd = fileno(output->file);
    if (status == OHLC_OK && (fchmod(fd, file_creation_mode()) != 0 || fsync(fd) != 0)) {
        status = OHLC_IO;
    }
    if (fclose(output->file) != 0 && status == OHLC_OK) {
        status = OHLC_IO;
    }
    bool renamed = false;
    if (status == OHLC_OK) {
        if (overwrite) {
            renamed = rename(output->temporary, path) == 0;
            status = renamed ? OHLC_OK : OHLC_IO;
        } else if (link(output->temporary, path) != 0) {
            /* Another process may create the target after the initial check. */
            status = errno == EEXIST ? OHLC_ALREADY_EXISTS : OHLC_IO;
        }
    }
    if (!renamed) {
        unlink(output->temporary);
    }
    if (status == OHLC_OK) {
        status = sync_parent_directory(path);
    }
    free(output->temporary);
    output->temporary = NULL;
    return status;
}

static void print_header(FILE* file, output_format format, bool series) {
    if (format == SHELL_JSONL) {
        return;
    }
    char separator = format == SHELL_CSV ? ',' : (format == SHELL_TABLE ? ' ' : '\t');
    if (format == SHELL_TABLE) {
        fputs("datetime ", file);
    }
    const char* names[] = {series ? "time_key" : "ticker_code",
                           "open",
                           "high",
                           "low",
                           "close",
                           "volume",
                           "amount",
                           "adjust_factor",
                           "reserved"};
    for (size_t i = 0; i < 9; i++) {
        fputs(names[i], file);
        fputc(i == 8 ? '\n' : separator, file);
    }
}

/* Parse the times with the current table definition and send the request.
 * Returns OK only after the request has been sent. */
static ohlc_status start_query(shell* state, const shell_command* command,
                               const query_request* request, const ohlc_table_info* table,
                               uint32_t* key) {
    const shell_word* words = command->words;
    ohlc_status status = parse_time(table, &words[request->series ? 4 : 2], key);
    if (status != OHLC_OK) {
        return status;
    }
    if (!request->series) {
        if (request->filtered) {
            return start_filtered_cross(state, table->id, *key, command, request->ticker_count);
        }
        return ohlc_client_cross(state->client, table->id, *key);
    }
    uint32_t last;
    status = parse_time(table, &words[6], &last);
    if (status != OHLC_OK) {
        return status;
    }
    if (*key > last) {
        return OHLC_INVALID;
    }
    uint32_t ticker;
    status = resolve(state, table->id, &words[2], &ticker);
    if (status != OHLC_OK) {
        return status;
    }
    return ohlc_client_series(state->client, table->id, ticker, *key, last);
}

/* Read result chunks until the stream ends, an error occurs, or the preview
 * limit is reached. The header is written with the first chunk, so a request
 * rejected before any rows leaves the output untouched. */
static ohlc_status read_query(shell* state, const ohlc_table_info* table, uint32_t key, bool series,
                              output_format format, bool all, FILE* file,
                              query_progress* progress) {
    while (!progress->final) {
        ohlc_bytes rows;
        uint32_t count;
        ohlc_status status =
            ohlc_client_next(state->client, &rows, &count, &progress->sequence, &progress->final);
        if (status != OHLC_OK || progress->final) {
            progress->stream_open = false;
        }
        if (status != OHLC_OK) {
            return status;
        }
        progress->received += count;
        if (progress->chunks++ == 0) {
            print_header(file, format, series);
        }
        for (uint32_t i = 0; i < count; i++) {
            if (!all && progress->shown >= state->preview) {
                progress->truncated = true;
                break;
            }
            const uint8_t* data = (const uint8_t*)rows.data + i * OHLC_RESULT_BYTES;
            ohlc_row row;
            ohlc_row_decode(data + 4, &row);
            if (format == SHELL_TABLE) {
                char time[80];
                status = ohlc_time_format_local(table, series ? ohlc_get_u32(data) : key, time,
                                                sizeof(time));
                if (status != OHLC_OK) {
                    return status;
                }
                fprintf(file, "%s ", time);
            }
            print_row(file, format, ohlc_get_u32(data), &row);
            progress->shown++;
        }
        if (ohlc_shell_interrupted) {
            return OHLC_CANCELLED;
        }
        if (ferror(file)) {
            return OHLC_IO;
        }
        if (progress->truncated ||
            (!all && progress->shown >= state->preview && !progress->final)) {
            progress->truncated = true;
            return OHLC_OK;
        }
    }
    return OHLC_OK;
}

static ohlc_status query(shell* state, const shell_command* command, bool series) {
    query_request request;
    if (parse_query(command, series, &request) != PARSE_OK) {
        return OHLC_INVALID;
    }
    const char* path = request.path != NULL ? request.path : state->output_path;
    output_format format = request.format_set ? request.format : effective_format(state, path);
    bool all = request.all || path != NULL || !state->interactive;
    ohlc_status status = OHLC_OK;
    if (path != NULL && !request.overwrite) {
        status = output_absent(path);
    }
    table_handle table;
    if (status == OHLC_OK) {
        status = open_table(state, &command->words[1], &table);
    }
    query_output output = {.file = stdout};
    if (status == OHLC_OK && path != NULL) {
        status = output_open(path, &output);
    }
    if (status != OHLC_OK) {
        return status;
    }
    query_progress progress = {0};
    do {
        uint32_t key = 0;
        status = start_query(state, command, &request, &table.info, &key);
        if (status == OHLC_OK) {
            progress.requested = true;
            progress.stream_open = true;
            status =
                read_query(state, &table.info, key, series, format, all, output.file, &progress);
        }
        /* A stale cached ID is rejected before any chunk, so nothing was printed. */
    } while (progress.chunks == 0 && refresh_stale_table(state, &table, status));
    if (progress.stream_open) {
        /* Cancel the unread remainder; the protocol has no per-query cancel. */
        disconnect(state);
    }
    if (path != NULL) {
        status = output_finish(&output, path, request.overwrite, status);
    } else if (fflush(stdout) != 0 && status == OHLC_OK) {
        status = OHLC_IO;
    }
    if (progress.truncated && progress.final && status == OHLC_OK) {
        fprintf(stderr, "Preview truncated: shown=%" PRIu64 ", total=%" PRIu64 ".\n",
                progress.shown, progress.received);
    } else if (progress.truncated) {
        fprintf(stderr,
                "Preview truncated: shown=%" PRIu64 ", total unknown; connection cancelled.\n",
                progress.shown);
    } else if (status == OHLC_OK && progress.final) {
        fprintf(stderr, "Complete rows=%" PRIu64 " snapshot_seq=%" PRIu64 "\n", progress.shown,
                progress.sequence);
    } else if (progress.requested) {
        fprintf(stderr, "Incomplete result after %" PRIu64 " rows\n", progress.shown);
    }
    return status;
}

static ohlc_status insert_once(shell* state, const shell_command* command,
                               const ohlc_table_info* table, const ohlc_row* row,
                               uint64_t* sequence) {
    uint32_t key;
    ohlc_status status = parse_time(table, &command->words[3], &key);
    if (status != OHLC_OK) {
        return status;
    }
    uint8_t record[OHLC_WRITE_BYTES];
    ohlc_write_encode(record, 0, key, row);
    ohlc_bytes ticker = {command->words[2].text, command->words[2].size};
    return ohlc_client_write_named(state->client, table->id, &ticker, 1, record, 1, sequence);
}

static ohlc_status insert_row(shell* state, const shell_command* command) {
    ohlc_row row;
    if (!ohlc_shell_row(command->words + 4, command->count - 4, &row)) {
        return OHLC_INVALID;
    }
    table_handle table;
    ohlc_status status = open_table(state, &command->words[1], &table);
    if (status != OHLC_OK) {
        return status;
    }
    uint64_t sequence;
    do {
        status = insert_once(state, command, &table.info, &row, &sequence);
        /* NOT_FOUND is a definite rejection, so one repeat cannot duplicate a write. */
    } while (refresh_stale_table(state, &table, status));
    if (status == OHLC_OK) {
        fprintf(stderr, "Committed rows=1 commit_seq=%" PRIu64 "\n", sequence);
    }
    return status;
}

/* Rows pending for one named write, plus totals already confirmed by the server. */
typedef struct {
    ohlc_bytes* names;
    uint8_t* records;
    size_t pending;
    size_t named_bytes;
    uint64_t confirmed_rows;
    uint64_t confirmed_batches;
} import_progress;

/* Commit the pending rows as one atomic batch. Names are released only after the
 * server confirms the batch; a failed batch stays pending so it can be reported. */
static ohlc_status commit_import_batch(shell* state, uint32_t table_id, import_progress* progress,
                                       uint64_t through_record) {
    uint64_t sequence;
    ohlc_status status =
        ohlc_client_write_named(state->client, table_id, progress->names, progress->pending,
                                progress->records, progress->pending, &sequence);
    if (status != OHLC_OK) {
        return status;
    }
    for (size_t i = 0; i < progress->pending; i++) {
        free((void*)progress->names[i].data);
    }
    progress->confirmed_rows += progress->pending;
    progress->confirmed_batches++;
    progress->pending = 0;
    progress->named_bytes = 4;
    fprintf(stderr,
            "Import confirmed batches=%" PRIu64 " rows=%" PRIu64 " through_record=%" PRIu64
            " commit_seq=%" PRIu64 "\n",
            progress->confirmed_batches, progress->confirmed_rows, through_record, sequence);
    return OHLC_OK;
}

typedef struct {
    bool csv;
    bool header;
    uint64_t batch;
} import_options;

static parse_result parse_import(const shell_command* command, import_options* options) {
    *options = (import_options){.header = true, .batch = 20000};
    if (command->count < 3) {
        return PARSE_SYNTAX;
    }
    parse_result result = text_word(&command->words[2]) ? PARSE_OK : PARSE_VALUE;
    for (size_t i = 3; i < command->count; i++) {
        const shell_word* option = &command->words[i];
        if (word_is(option, "--no-header")) {
            options->header = false;
        } else if (word_is(option, "--format") && i + 1 < command->count) {
            const shell_word* value = &command->words[++i];
            if (word_is(value, "tsv") || word_is(value, "csv")) {
                options->csv = word_is(value, "csv");
            } else {
                result = PARSE_VALUE;
            }
        } else if (word_is(option, "--batch-rows") && i + 1 < command->count) {
            if (!ohlc_shell_uint(&command->words[++i], OHLC_MAX_BATCH_ROWS, &options->batch) ||
                options->batch == 0) {
                result = PARSE_VALUE;
            }
        } else {
            return PARSE_SYNTAX;
        }
    }
    return result;
}

static ohlc_status import_file(shell* state, const shell_command* command) {
    import_options options;
    if (parse_import(command, &options) != PARSE_OK) {
        return OHLC_INVALID;
    }
    bool csv = options.csv;
    bool header = options.header;
    uint64_t batch = options.batch;
    ohlc_connection_info limits;
    ohlc_client_info(state->client, &limits);
    if (batch > limits.max_write_rows || batch > (limits.max_frame_bytes - 8u) / OHLC_WRITE_BYTES) {
        return OHLC_LIMIT;
    }
    if (!text_word(&command->words[1])) {
        return OHLC_INVALID;
    }
    /* A long import addresses the table's current ID rather than a cached one. */
    table_handle handle;
    ohlc_status status = reopen_table(state, command->words[1].text, &handle);
    if (status != OHLC_OK) {
        return status;
    }
    const ohlc_table_info* table = &handle.info;
    FILE* file = fopen(command->words[2].text, "r");
    import_progress progress = {.names = calloc((size_t)batch, sizeof(ohlc_bytes)),
                                .records = malloc((size_t)batch * OHLC_WRITE_BYTES),
                                .named_bytes = 4};
    shell_command* fields = malloc(sizeof(*fields));
    if (file == NULL || progress.records == NULL || fields == NULL || progress.names == NULL) {
        free(progress.names);
        free(progress.records);
        free(fields);
        if (file != NULL) {
            fclose(file);
        }
        return OHLC_IO;
    }
    uint64_t record_number = 0;
    size_t column_count = 0;
    bool eof = false;
    while (status == OHLC_OK && !eof && !ohlc_shell_interrupted) {
        status = ohlc_shell_text_record(file, csv, fields, &eof);
        if (status != OHLC_OK) {
            break;
        }
        if (fields->count == 0 && eof) {
            break;
        }
        record_number++;
        if ((fields->count != 9 && fields->count != 10) ||
            (column_count != 0 && fields->count != column_count)) {
            status = OHLC_INVALID;
            break;
        }
        /* The first record, header or data, fixes the column count for the file. */
        column_count = fields->count;
        if (header) {
            static const char* columns[] = {"ticker",        "time",    "open",   "high",
                                            "low",           "close",   "volume", "amount",
                                            "adjust_factor", "reserved"};
            for (size_t i = 0; i < fields->count; i++) {
                if (fields->words[i].size != strlen(columns[i]) ||
                    memcmp(fields->words[i].text, columns[i], strlen(columns[i])) != 0) {
                    status = OHLC_INVALID;
                    break;
                }
            }
            header = false;
            continue;
        }
        uint32_t key;
        ohlc_row row;
        if (!ohlc_shell_row(fields->words + 2, fields->count - 2, &row)) {
            status = OHLC_INVALID;
            break;
        }
        status = parse_time(table, &fields->words[1], &key);
        size_t length = fields->words[0].size;
        if (status == OHLC_OK && (length == 0 || length > 4096)) {
            status = OHLC_INVALID;
        }
        if (status == OHLC_OK && progress.pending != 0 &&
            progress.named_bytes + length + 4u + (progress.pending + 1u) * OHLC_WRITE_BYTES >
                OHLC_FRAME_LIMIT - 104u) {
            /* Commit the rows that fit before this record would exceed one frame. */
            status = commit_import_batch(state, table->id, &progress, record_number - 1);
        }
        if (status == OHLC_OK) {
            char* name = malloc(length);
            if (name == NULL) {
                status = OHLC_LIMIT;
            } else {
                memcpy(name, fields->words[0].text, length);
                progress.names[progress.pending] = (ohlc_bytes){name, length};
                progress.named_bytes += length + 4u;
            }
        }
        if (status != OHLC_OK) {
            break;
        }
        ohlc_write_encode(progress.records + progress.pending * OHLC_WRITE_BYTES,
                          (uint32_t)progress.pending, key, &row);
        progress.pending++;
        if (progress.pending == batch) {
            status = commit_import_batch(state, table->id, &progress, record_number);
        }
    }
    if (ohlc_shell_interrupted && status != OHLC_OUTCOME_UNKNOWN) {
        status = OHLC_CANCELLED;
    }
    if (header && status == OHLC_OK) {
        status = OHLC_INVALID;
    }
    if (status == OHLC_OK && progress.pending != 0) {
        status = commit_import_batch(state, table->id, &progress, record_number);
    }
    fprintf(stderr,
            "Import %s: confirmed_batches=%" PRIu64 " confirmed_rows=%" PRIu64
            " input_record=%" PRIu64
            " pending_rows=%zu; names commit with each successful batch.\n",
            status == OHLC_OK ? "complete" : "stopped", progress.confirmed_batches,
            progress.confirmed_rows, record_number, progress.pending);
    fclose(file);
    for (size_t i = 0; i < progress.pending; i++) {
        free((void*)progress.names[i].data);
    }
    free(progress.names);
    free(progress.records);
    free(fields);
    return status;
}

static ohlc_status assign(char** target, const shell_word* word) {
    if (!text_word(word)) {
        return OHLC_INVALID;
    }
    char* text = strdup(word->text);
    if (text == NULL) {
        return OHLC_LIMIT;
    }
    free(*target);
    *target = text;
    return OHLC_OK;
}

/* Words supplied to CONNECT; values are validated before any state changes. */
typedef struct {
    const shell_word* socket_path;
    const shell_word* host;
    const shell_word* port;
    const shell_word* ca_file;
    const shell_word* token_file;
    bool tls;
} connection_request;

static parse_result parse_connection(const shell_command* command, connection_request* request) {
    *request = (connection_request){0};
    for (size_t i = 1; i < command->count; i++) {
        const shell_word* option = &command->words[i];
        if (word_is(option, "--tls")) {
            request->tls = true;
            continue;
        }
        const shell_word** target = NULL;
        if (word_is(option, "--socket")) {
            target = &request->socket_path;
        } else if (word_is(option, "--host")) {
            target = &request->host;
        } else if (word_is(option, "--port")) {
            target = &request->port;
        } else if (word_is(option, "--ca")) {
            target = &request->ca_file;
        } else if (word_is(option, "--token-file")) {
            target = &request->token_file;
        }
        if (target == NULL || i + 1 == command->count) {
            return PARSE_SYNTAX;
        }
        *target = &command->words[++i];
    }
    if (request->socket_path != NULL && (request->host != NULL || request->port != NULL)) {
        return PARSE_SYNTAX;
    }
    return PARSE_OK;
}

static ohlc_status connection_options(shell* state, const shell_command* command) {
    connection_request request;
    if (parse_connection(command, &request) != PARSE_OK) {
        return OHLC_INVALID;
    }
    const shell_word* values[] = {request.socket_path, request.host, request.port, request.ca_file,
                                  request.token_file};
    char** targets[] = {&state->socket_path, &state->host, &state->port, &state->ca_file,
                        &state->token_file};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        if (values[i] != NULL && !text_word(values[i])) {
            return OHLC_INVALID;
        }
    }
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        if (values[i] != NULL) {
            ohlc_status status = assign(targets[i], values[i]);
            if (status != OHLC_OK) {
                return status;
            }
        }
    }
    if (request.tls) {
        state->tls = true;
    }
    if (request.host != NULL || request.port != NULL) {
        free(state->socket_path);
        state->socket_path = NULL;
    }
    if (request.socket_path != NULL) {
        state->tls = false;
    }
    /* An explicit connect always announces the resulting connection. */
    state->banner_shown = false;
    return connect_server(state);
}

/* Check command names, argument counts, option names and option arity. Only
 * structure is checked here; argument values are validated by each command. */
static bool command_shape_valid(const shell_command* command) {
    const shell_word* name = command->words;
    size_t count = command->count;
    static const char* const single_word[] = {"quit", "exit",  "disconnect", "status",
                                              "ping", "stats", "checkpoint", "tables"};
    for (size_t i = 0; i < sizeof(single_word) / sizeof(single_word[0]); i++) {
        if (word_is(name, single_word[i])) {
            return count == 1;
        }
    }
    if (word_is(name, "help")) {
        return count <= 2;
    }
    if (word_is(name, "source") || word_is(name, "drop") || word_is(name, "describe")) {
        return count == 2;
    }
    if (word_is(name, "tickers")) {
        return count == 2 || count == 3;
    }
    if (word_is(name, "resolve") || word_is(name, "register")) {
        return count == 3;
    }
    if (word_is(name, "insert") || word_is(name, "put")) {
        return count == 11 || count == 12;
    }
    if (word_is(name, "set")) {
        const shell_word* setting = &command->words[1];
        return count == 3 && (word_is(setting, "preview") || word_is(setting, "format") ||
                              word_is(setting, "timing") || word_is(setting, "history"));
    }
    if (word_is(name, "connect")) {
        connection_request request;
        return parse_connection(command, &request) == PARSE_OK;
    }
    if (word_is(name, "create")) {
        ohlc_table_definition definition;
        return parse_create(command, &definition) != PARSE_SYNTAX;
    }
    if (word_is(name, "import")) {
        import_options options;
        return parse_import(command, &options) != PARSE_SYNTAX;
    }
    if (word_is(name, "series") || word_is(name, "cross")) {
        query_request request;
        return parse_query(command, word_is(name, "series"), &request) != PARSE_SYNTAX;
    }
    return false;
}

static void show_status(const shell* state) {
    fprintf(stderr, "connected=%s last_status=%s unknown_mutation=%s unknown_command=%s\n",
            ohlc_client_connected(state->client) ? "yes" : "no",
            ohlc_status_string(state->last_status), state->unknown ? "yes" : "no",
            state->unknown_command);
    if (state->client != NULL) {
        ohlc_connection_info info;
        ohlc_client_info(state->client, &info);
        fputs("UUID=", stderr);
        print_uuid(stderr, info.uuid);
        fprintf(stderr,
                " protocol=%u access=%s max_frame=%" PRIu32 " max_rows=%" PRIu32
                " query_ms=%" PRIu32 "\n",
                OHLC_PROTOCOL_VERSION, access_name(info.capabilities), info.max_frame_bytes,
                info.max_write_rows, info.max_query_ms);
    }
}

static ohlc_status set_option(shell* state, const shell_command* command) {
    const shell_word* setting = &command->words[1];
    const shell_word* value = &command->words[2];
    if (word_is(setting, "preview")) {
        return ohlc_shell_uint(value, UINT32_MAX, &state->preview) && state->preview != 0
                   ? OHLC_OK
                   : OHLC_INVALID;
    }
    if (word_is(setting, "format")) {
        if (!format_parse(value, &state->format)) {
            return OHLC_INVALID;
        }
        state->format_set = true;
        return OHLC_OK;
    }
    if (!word_is(value, "on") && !word_is(value, "off")) {
        return OHLC_INVALID;
    }
    bool enabled = word_is(value, "on");
    if (word_is(setting, "timing")) {
        state->timing = enabled;
    } else {
        state->history = enabled;
    }
    return OHLC_OK;
}

static ohlc_status show_help(const shell* state, const shell_command* command) {
    const char* format = format_names[state->format];
    if (!state->format_set) {
        format = isatty(STDOUT_FILENO) ? "table (default; tsv for --output)" : "tsv (default)";
    }
    shell_help_settings settings = {.format = format,
                                    .preview = state->preview,
                                    .history = state->history,
                                    .timing = state->timing};
    return ohlc_shell_show_help(command, &settings);
}

static ohlc_status run(shell* state, FILE* file, bool interactive);

static ohlc_status source_script(shell* state, const shell_word* path) {
    if (!text_word(path)) {
        return OHLC_INVALID;
    }
    if (state->depth >= 8) {
        return OHLC_LIMIT;
    }
    FILE* source = fopen(path->text, "r");
    if (source == NULL) {
        return OHLC_IO;
    }
    state->depth++;
    ohlc_status status = run(state, source, false);
    state->depth--;
    fclose(source);
    return status;
}

static ohlc_status show_table(shell* state, const shell_word* name) {
    if (!text_word(name)) {
        return OHLC_INVALID;
    }
    table_handle table;
    ohlc_status status = reopen_table(state, name->text, &table);
    if (status == OHLC_OK) {
        print_definition(&table.info);
        puts("0 open int64\n1 high int64\n2 low int64\n3 close int64\n4 volume uint64\n5 "
             "amount uint64\n6 adjust_factor uint64\n7 reserved int32 (default NULL)\n"
             "All eight fields are nullable.");
    }
    return status;
}

static ohlc_status list_tickers(shell* state, const shell_command* command) {
    const shell_word* name = &command->words[1];
    if (!text_word(name)) {
        return OHLC_INVALID;
    }
    table_handle table;
    ohlc_status status = reopen_table(state, name->text, &table);
    if (status != OHLC_OK) {
        return status;
    }
    return list_metadata(state, false, table.info.id,
                         command->count == 3 ? &command->words[2] : NULL);
}

static ohlc_status ticker_code(shell* state, const shell_command* command, bool register_name) {
    table_handle table;
    ohlc_status status = open_table(state, &command->words[1], &table);
    if (status != OHLC_OK) {
        return status;
    }
    const shell_word* ticker = &command->words[2];
    uint32_t code;
    uint64_t sequence = 0;
    do {
        if (register_name) {
            ohlc_bytes bytes = {ticker->text, ticker->size};
            status = ohlc_client_register(state->client, table.info.id, bytes, &code, &sequence);
        } else {
            status = resolve(state, table.info.id, ticker, &code);
        }
        /* NOT_FOUND is a definite rejection, so one repeat cannot duplicate a write. */
    } while (refresh_stale_table(state, &table, status));
    if (status != OHLC_OK) {
        return status;
    }
    if (register_name) {
        fprintf(stderr, "Registered commit_seq=%" PRIu64 "\n", sequence);
        cache_symbol(state, table.info.id, ticker, code);
    }
    printf("%" PRIu32 "\n", code);
    return OHLC_OK;
}

static ohlc_status ping(shell* state) {
    ohlc_bytes response;
    uint64_t start = ohlc_monotonic_ms();
    ohlc_status status = ohlc_client_call(state->client, 2, NULL, 0, &response);
    if (status == OHLC_OK && response.size != 0) {
        status = OHLC_CORRUPT;
    }
    fprintf(stderr, "PING %s elapsed_ms=%" PRIu64 "\n", ohlc_status_string(status),
            ohlc_monotonic_ms() - start);
    return status;
}

static ohlc_status show_stats(shell* state) {
    ohlc_stats stats;
    ohlc_status status = ohlc_client_stats(state->client, &stats);
    if (status == OHLC_OK) {
        printf("commit_seq=%" PRIu64 "\ncheckpoint_seq=%" PRIu64 "\nticker_count=%" PRIu64
               "\ntable_count=%" PRIu32 "\nmemory_bytes=%" PRIu64 "\ndisk_read_bytes=%" PRIu64
               "\ndisk_read_calls=%" PRIu64 "\ncache_hits=%" PRIu64 "\ncache_misses=%" PRIu64
               "\nwal_bytes=%" PRIu64 "\ndata_bytes=%" PRIu64 "\n",
               stats.commit_seq, stats.checkpoint_seq, stats.ticker_count, stats.table_count,
               stats.memory_bytes, stats.disk_read_bytes, stats.disk_read_calls, stats.cache_hits,
               stats.cache_misses, stats.wal_bytes, stats.data_bytes);
    }
    return status;
}

/* Execute one command that needs the server connection. */
static ohlc_status execute_remote(shell* state, const shell_command* command) {
    const shell_word* word = command->words;
    if (word_is(word, "ping")) {
        return ping(state);
    }
    if (word_is(word, "checkpoint")) {
        return ohlc_client_checkpoint(state->client);
    }
    if (word_is(word, "stats")) {
        return show_stats(state);
    }
    if (word_is(word, "create")) {
        return create_table(state, command);
    }
    if (word_is(word, "drop")) {
        return drop_table(state, command);
    }
    if (word_is(word, "tables")) {
        return list_metadata(state, true, 0, NULL);
    }
    if (word_is(word, "tickers")) {
        return list_tickers(state, command);
    }
    if (word_is(word, "describe")) {
        return show_table(state, word + 1);
    }
    if (word_is(word, "resolve") || word_is(word, "register")) {
        return ticker_code(state, command, word_is(word, "register"));
    }
    if (word_is(word, "series") || word_is(word, "cross")) {
        return query(state, command, word_is(word, "series"));
    }
    if (word_is(word, "insert") || word_is(word, "put")) {
        return insert_row(state, command);
    }
    return import_file(state, command);
}

static ohlc_status execute(shell* state, const shell_command* command) {
    if (command->count == 0) {
        return OHLC_OK;
    }
    /* Structural errors are reported before connecting and count as syntax
     * errors for the exit status; value errors are ordinary failures. */
    if (!command_shape_valid(command)) {
        state->syntax_error = true;
        return OHLC_INVALID;
    }
    const shell_word* word = command->words;
    if (word_is(word, "help")) {
        return show_help(state, command);
    }
    if (word_is(word, "quit") || word_is(word, "exit")) {
        state->quitting = true;
        return OHLC_OK;
    }
    if (word_is(word, "disconnect")) {
        disconnect(state);
        return OHLC_OK;
    }
    if (word_is(word, "connect")) {
        return connection_options(state, command);
    }
    if (word_is(word, "status")) {
        show_status(state);
        return OHLC_OK;
    }
    if (word_is(word, "set")) {
        return set_option(state, command);
    }
    if (word_is(word, "source")) {
        return source_script(state, word + 1);
    }
    if (!ohlc_client_connected(state->client)) {
        ohlc_status status = connect_server(state);
        if (status != OHLC_OK) {
            return status;
        }
    }
    return execute_remote(state, command);
}

static ohlc_status run(shell* state, FILE* file, bool interactive) {
    shell_input input = {.file = file, .interactive = interactive};
    shell_command* command = malloc(sizeof(*command));
    if (command == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_status status = OHLC_OK;
    bool eof = false;
    while (!state->quitting && !eof) {
        input.history = state->history;
        ohlc_shell_interrupted = 0;
        state->error_reported = false;
        status = ohlc_shell_read(&input, command, &eof);
        if (status == OHLC_INVALID || status == OHLC_LIMIT) {
            state->syntax_error = true;
        }
        uint64_t start = ohlc_monotonic_ms();
        if (status == OHLC_OK) {
            bool previous_interactive = state->interactive;
            state->interactive = interactive;
            status = execute(state, command);
            state->interactive = previous_interactive;
        }
        if (status == OHLC_OUTCOME_UNKNOWN) {
            state->unknown = true;
            /* A nested script already recorded its innermost command. */
            if (!state->error_reported) {
                snprintf(state->unknown_command, sizeof(state->unknown_command), "%.*s",
                         command->count != 0 ? (int)command->words[0].size : 0,
                         command->count != 0 ? command->words[0].text : "");
            }
        } else if (ohlc_shell_interrupted) {
            status = OHLC_CANCELLED;
        }
        if (fflush(stdout) != 0 && status == OHLC_OK) {
            status = OHLC_IO;
        }
        state->last_status = status;
        if (status != OHLC_OK) {
            /* Report a failure once, where it occurred; enclosing source levels
             * only stop and propagate its status. */
            if (!state->error_reported) {
                fprintf(stderr, "%s%s\n", ohlc_status_string(status),
                        status == OHLC_OUTCOME_UNKNOWN
                            ? ": mutation may have committed; inspect before retrying"
                            : "");
                state->error_reported = true;
            }
            if (!interactive) {
                break;
            }
        }
        if (interactive) {
            state->syntax_error = false;
            state->connection_error = false;
        }
        if (state->timing && command->count != 0) {
            fprintf(stderr, "elapsed_ms=%" PRIu64 "\n", ohlc_monotonic_ms() - start);
        }
    }
    ohlc_shell_input_close(&input);
    free(command);
    return status;
}

static void usage(void) {
    puts("Usage: ohlc [--socket PATH | --host HOST --port PORT] [--tls --ca FILE]\n"
         "            [--token-file FILE] [--execute COMMAND | --file SCRIPT]\n"
         "            [--format table|tsv|csv|jsonl] [--output PATH] [--no-history]\n"
         "--output requires one SERIES/CROSS command in --execute.\n"
         "Use help; or help examples; for commands. --help requires no server.\n"
         "Default connection: 127.0.0.1:8765. Secrets are never command arguments.");
}

int main(int argc, char** argv) {
    if (argc == 2 && strcmp(argv[1], "--version") == 0) {
        printf("ohlc %s\n", ohlc_version());
        return 0;
    }
    shell* state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return 1;
    }
    state->preview = 100;
    state->history = true;
    state->interactive = isatty(STDIN_FILENO) && isatty(STDOUT_FILENO);
    state->format = SHELL_TSV;
    const char* script = NULL;
    const char* command_text = NULL;
    bool socket_option = false;
    bool tcp_option = false;
    int result = 0;
    FILE* owned_file = NULL;
    static const struct option options[] = {{"socket", required_argument, NULL, 's'},
                                            {"host", required_argument, NULL, 'h'},
                                            {"port", required_argument, NULL, 'p'},
                                            {"tls", no_argument, NULL, 't'},
                                            {"ca", required_argument, NULL, 'c'},
                                            {"token-file", required_argument, NULL, 'k'},
                                            {"execute", required_argument, NULL, 'e'},
                                            {"file", required_argument, NULL, 'f'},
                                            {"format", required_argument, NULL, 'm'},
                                            {"output", required_argument, NULL, 'o'},
                                            {"no-history", no_argument, NULL, 'n'},
                                            {"help", no_argument, NULL, '?'},
                                            {NULL, 0, NULL, 0}};
    for (;;) {
        int option = getopt_long(argc, argv, "", options, NULL);
        if (option == -1) {
            break;
        }
        shell_word value = {.text = optarg, .size = optarg != NULL ? strlen(optarg) : 0};
        ohlc_status status = OHLC_OK;
        switch (option) {
        case 's':
            status = assign(&state->socket_path, &value);
            socket_option = true;
            break;
        case 'h':
            status = assign(&state->host, &value);
            tcp_option = true;
            break;
        case 'p':
            status = assign(&state->port, &value);
            tcp_option = true;
            break;
        case 't':
            state->tls = true;
            break;
        case 'c':
            status = assign(&state->ca_file, &value);
            break;
        case 'k':
            status = assign(&state->token_file, &value);
            break;
        case 'e':
            command_text = optarg;
            break;
        case 'f':
            script = optarg;
            break;
        case 'm':
            status = format_parse(&value, &state->format) ? OHLC_OK : OHLC_INVALID;
            state->format_set = true;
            break;
        case 'o':
            state->output_path = optarg;
            break;
        case 'n':
            state->history = false;
            break;
        default:
            usage();
            result = argc == 2 && strcmp(argv[1], "--help") == 0 ? 0 : 2;
            goto cleanup;
        }
        if (status != OHLC_OK) {
            result = 2;
            goto cleanup;
        }
    }
    if (optind != argc || (socket_option && tcp_option) ||
        (script != NULL && command_text != NULL) ||
        (state->output_path != NULL && command_text == NULL)) {
        usage();
        result = 2;
        goto cleanup;
    }
    struct sigaction action = {.sa_handler = interrupt};
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    /* Interrupt connection setup too, before a socket exists to shut down. */
    ohlc_net_set_interrupt_flag(&ohlc_shell_interrupted);
    struct sigaction ignore = {.sa_handler = SIG_IGN};
    sigemptyset(&ignore.sa_mask);
    sigaction(SIGPIPE, &ignore, NULL);
    FILE* file = stdin;
    if (command_text != NULL) {
        file = fmemopen((void*)command_text, strlen(command_text), "r");
        owned_file = file;
    } else if (script != NULL && strcmp(script, "-") != 0) {
        file = fopen(script, "r");
        owned_file = file;
    }
    if (file == NULL) {
        fprintf(stderr, "Cannot open command input\n");
        result = 1;
        goto cleanup;
    }
    if (state->output_path != NULL) {
        shell_input input = {.file = file};
        shell_command* command = malloc(sizeof(*command));
        shell_command* extra = malloc(sizeof(*extra));
        bool eof = false;
        if (command == NULL || extra == NULL || ohlc_shell_read(&input, command, &eof) != OHLC_OK ||
            command->count == 0 ||
            (!word_is(command->words, "series") && !word_is(command->words, "cross")) ||
            ohlc_shell_read(&input, extra, &eof) != OHLC_OK || extra->count != 0) {
            result = 2;
        }
        free(command);
        free(extra);
        rewind(file);
    }
    if (result == 0) {
        bool interactive = state->interactive && script == NULL && command_text == NULL;
        completion_state = state;
        state->banner = interactive;
        ohlc_status status = run(state, file, interactive);
        if (state->unknown) {
            result = 4;
        } else if (status == OHLC_CANCELLED) {
            result = 130;
        } else if (status != OHLC_OK) {
            result = state->syntax_error ? 2 : (state->connection_error ? 3 : 1);
        }
    }
cleanup:
    if (owned_file != NULL) {
        fclose(owned_file);
    }
    disconnect(state);
    ohlc_shell_history_save(state->history);
    free(state->socket_path);
    free(state->host);
    free(state->port);
    free(state->ca_file);
    free(state->token_file);
    free(state);
    return result;
}

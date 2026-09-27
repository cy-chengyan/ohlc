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
#include <unistd.h>

const shell_help ohlc_shell_commands[] = {
    {"help", "help [command|examples]", "Display syntax, field types and examples; works offline.",
     "help series;"},
    {"connect",
     "connect [--socket PATH | --host HOST --port PORT] [--tls --ca FILE --token-file FILE]",
     "Connect and authenticate; credentials are read from a private file.",
     "connect --socket /run/ohlc.sock;"},
    {"disconnect", "disconnect", "Close the connection and discard identifier caches.",
     "disconnect;"},
    {"status", "status", "Show negotiated limits, UUID, permissions and last command status.",
     "status;"},
    {"ping", "ping", "Verify the connection and report round-trip time.", "ping;"},
    {"stats", "stats", "Show engine sequences, allocation and cumulative I/O counters.", "stats;"},
    {"checkpoint", "checkpoint", "Persist a committed snapshot; write access required.",
     "checkpoint;"},
    {"create", "create TABLE --period Nm|Nd [--timezone ZONE] [--description TEXT]",
     "Create a durable fixed-schema table; write access required. Minute tables require a "
     "timezone.",
     "create bars_3m --period 3m --timezone Asia/Shanghai;\ncreate bars_5d --period 5d;"},
    {"tables", "tables", "List tables in stable ID order using bounded pages.", "tables;"},
    {"drop", "drop TABLE",
     "Permanently delete a table and its rows; write access required.\n"
     "Existing queries finish on their snapshots before files are reclaimed.\n"
     "The name may be reused, but the new table receives a different ID.",
     "drop bars_3m;"},
    {"describe", "describe TABLE", "Show immutable table metadata and fixed field positions.",
     "describe bars_3m;"},
    {"tickers", "tickers [PREFIX]", "List exact ticker bytes, escaped for safe display.",
     "tickers AA;"},
    {"resolve", "resolve TICKER", "Resolve an existing ticker without registering it.",
     "resolve \"AAPL\";"},
    {"register", "register TICKER", "Durably register a ticker; write access required.",
     "register \"AAPL\";"},
    {"series", "series TABLE TICKER from START to END [options]",
     "Read [START, END) ordered by real time. END may be @4294967296. Read access required.",
     "series bars_3m AAPL from \"20260901 09:30:00\" to \"20260901 16:00:00\" --all;"},
    {"cross", "cross TABLE TIME [options]", "Read one time across tickers, ordered by ticker code.",
     "cross bars_3m \"20260901 09:30:00\";\ncross bars_3m @29803050 --format csv --output "
     "cross.csv;"},
    {"insert", "insert TABLE TICKER TIME OPEN HIGH LOW CLOSE VOLUME AMOUNT ADJUST_FACTOR",
     "Atomically insert/replace all seven fields. Table and ticker must exist; write access "
     "required.",
     "insert bars_3m AAPL \"20260901 09:30:00\" 10000 10100 9950 10080 1200 12100000 1000000;"},
    {"put", "put TABLE TICKER TIME OPEN HIGH LOW CLOSE VOLUME AMOUNT ADJUST_FACTOR",
     "Alias of insert.", "help insert;"},
    {"import", "import TABLE PATH [--format tsv|csv] [--batch-rows N] [--no-header]",
     "Import nine columns: ticker,time,open,high,low,close,volume,amount,adjust_factor.\n"
     "Default: TSV, matching header, 20000 rows/batch. Registers missing tickers.\n"
     "Stops on the first error. Earlier committed batches and registrations remain durable.",
     "import bars_3m bars.tsv --format tsv --batch-rows 20000;"},
    {"source", "source PATH", "Run a local script; stop that script on its first failure.",
     "source queries.ohlc;"},
    {"set", "set format table|tsv|csv|jsonl; set preview N; set timing on|off; set history on|off",
     "Set local display options. History is private and never stores credentials.",
     "set preview 100;"},
    {"quit", "quit", "Exit. An unknown mutation outcome keeps exit status 4.", "quit;"},
    {"exit", "exit", "Alias of quit.", "exit;"}};
const size_t ohlc_shell_command_count =
    sizeof(ohlc_shell_commands) / sizeof(ohlc_shell_commands[0]);
volatile sig_atomic_t ohlc_shell_interrupted;
static volatile sig_atomic_t interrupt_socket = -1;

typedef struct symbol {
    struct symbol* next;
    size_t size;
    uint32_t code;
    uint8_t bytes[];
} symbol;

typedef enum { SHELL_TABLE, SHELL_TSV, SHELL_CSV, SHELL_JSONL } output_format;

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
    char unknown_command[32];
    uint64_t preview;
    output_format format;
    const char* output_path;
    ohlc_status last_status;
    unsigned int depth;
    symbol* symbols[1024];
    size_t symbol_bytes;
    ohlc_table_info tables[128];
    size_t table_count;
} shell;

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
        "--format",      "--output",     "--overwrite",  "--all",    "--period", "--timezone",
        "--description", "--batch-rows", "--no-header",  "--socket", "--host",   "--port",
        "--tls",         "--ca",         "--token-file", "from",     "to"};
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

static bool format_parse(const shell_word* word, output_format* output) {
    static const char* names[] = {"table", "tsv", "csv", "jsonl"};
    for (size_t i = 0; i < 4; i++) {
        if (word_is(word, names[i])) {
            *output = (output_format)i;
            return true;
        }
    }
    return false;
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

static void cache_symbol(shell* state, const shell_word* ticker, uint32_t code) {
    if (state->symbol_bytes + sizeof(symbol) + ticker->size + 1 > 16u * 1024u * 1024u) {
        return;
    }
    uint32_t bucket = symbol_bucket(ticker);
    for (const symbol* entry = state->symbols[bucket]; entry != NULL; entry = entry->next) {
        if (entry->size == ticker->size && memcmp(entry->bytes, ticker->text, ticker->size) == 0) {
            return;
        }
    }
    symbol* entry = malloc(sizeof(*entry) + ticker->size + 1);
    if (entry == NULL) {
        return;
    }
    entry->size = ticker->size;
    entry->code = code;
    memcpy(entry->bytes, ticker->text, ticker->size);
    entry->bytes[ticker->size] = 0;
    entry->next = state->symbols[bucket];
    state->symbols[bucket] = entry;
    state->symbol_bytes += sizeof(*entry) + ticker->size + 1;
}

static ohlc_status resolve(shell* state, const shell_word* ticker, bool allow_register,
                           uint32_t* code) {
    for (const symbol* entry = state->symbols[symbol_bucket(ticker)]; entry != NULL;
         entry = entry->next) {
        if (entry->size == ticker->size && memcmp(entry->bytes, ticker->text, ticker->size) == 0) {
            *code = entry->code;
            return OHLC_OK;
        }
    }
    ohlc_bytes bytes = {ticker->text, ticker->size};
    ohlc_status status = ohlc_client_resolve(state->client, bytes, code);
    if (status == OHLC_NOT_FOUND && allow_register) {
        uint64_t sequence;
        status = ohlc_client_register(state->client, bytes, code, &sequence);
        if (status == OHLC_OK) {
            fprintf(stderr, "Registered ticker_code=%" PRIu32 " commit_seq=%" PRIu64 "\n", *code,
                    sequence);
        }
    }
    if (status == OHLC_OK) {
        cache_symbol(state, ticker, *code);
    }
    return status;
}

static ohlc_status open_table(shell* state, const shell_word* name, ohlc_table_info* output) {
    if (!text_word(name)) {
        return OHLC_INVALID;
    }
    for (size_t i = 0; i < state->table_count; i++) {
        if (strcmp(name->text, state->tables[i].name) == 0) {
            *output = state->tables[i];
            return OHLC_OK;
        }
    }
    ohlc_status status = ohlc_client_table_open(state->client, name->text, output);
    if (status == OHLC_OK && state->table_count < 128) {
        state->tables[state->table_count++] = *output;
    }
    return status;
}

static ohlc_status parse_time(const ohlc_table_info* table, const shell_word* word,
                              uint32_t* output) {
    return text_word(word) ? ohlc_time_parse(table, word->text, output) : OHLC_INVALID;
}

static void print_definition(const ohlc_table_info* table) {
    printf("%" PRIu32 "\t%s\t%" PRIu32 "%c\t", table->id, table->name, table->period_count,
           table->period_unit == OHLC_MINUTE ? 'm' : 'd');
    ohlc_shell_print_bytes(stdout, table->timezone, strlen(table->timezone));
    fputc('\t', stdout);
    ohlc_shell_print_bytes(stdout, table->description, strlen(table->description));
    fputc('\n', stdout);
}

static ohlc_status list_metadata(shell* state, bool tables, const shell_word* prefix) {
    uint64_t start = tables ? 1 : 0;
    while (start <= UINT32_MAX) {
        uint8_t request[8];
        ohlc_put_u32(request, (uint32_t)start);
        ohlc_put_u32(request + 4, 128);
        ohlc_bytes response;
        ohlc_status status =
            ohlc_client_call(state->client, tables ? 11 : 4, request, 8, &response);
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
                shell_word ticker = {(char*)bytes + position, length};
                cache_symbol(state, &ticker, code);
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

static ohlc_status create_table(shell* state, const shell_command* command) {
    if (command->count < 4 || !text_word(&command->words[1])) {
        return OHLC_INVALID;
    }
    ohlc_table_definition definition = {
        .name = command->words[1].text, .timezone = "", .description = ""};
    for (size_t i = 2; i < command->count; i += 2) {
        if (i + 1 == command->count || !text_word(&command->words[i + 1])) {
            return OHLC_INVALID;
        }
        shell_word value = command->words[i + 1];
        if (word_is(&command->words[i], "--period") && value.size >= 2) {
            char unit = value.text[value.size - 1];
            value.size--;
            uint64_t count;
            if ((unit != 'm' && unit != 'd') || !ohlc_shell_uint(&value, UINT32_MAX, &count) ||
                count == 0) {
                return OHLC_INVALID;
            }
            definition.period_unit = unit == 'm' ? OHLC_MINUTE : OHLC_DAY;
            definition.period_count = (uint32_t)count;
        } else if (word_is(&command->words[i], "--timezone")) {
            definition.timezone = value.text;
        } else if (word_is(&command->words[i], "--description")) {
            definition.description = value.text;
        } else {
            return OHLC_INVALID;
        }
    }
    ohlc_table_info info;
    ohlc_status status = ohlc_client_table_create(state->client, &definition, &info);
    if (status == OHLC_OK) {
        print_definition(&info);
        if (state->table_count < 128) {
            state->tables[state->table_count++] = info;
        }
        fprintf(stderr, "Created commit_seq=%" PRIu64 "\n", info.created_seq);
    }
    return status;
}

static ohlc_status drop_table(shell* state, const shell_command* command) {
    if (command->count != 2 || !text_word(&command->words[1])) {
        return OHLC_INVALID;
    }
    /* Resolve the current name rather than trusting a cached table handle.
     * The following mutation addresses that ID, so a concurrent replacement
     * can never be deleted accidentally. */
    ohlc_table_info table;
    ohlc_status status = ohlc_client_table_open(state->client, command->words[1].text, &table);
    if (status == OHLC_OK) {
        uint64_t sequence;
        status = ohlc_client_table_drop(state->client, table.id, &sequence);
        state->table_count = 0;
        if (status == OHLC_OK) {
            fprintf(stderr, "Dropped %s commit_seq=%" PRIu64 "\n", table.name, sequence);
        }
    }
    return status;
}

static void print_row(FILE* file, output_format format, uint32_t key, const ohlc_row* row) {
    if (format == SHELL_JSONL) {
        fprintf(file,
                "{\"key\":%" PRIu32 ",\"open\":%" PRId32 ",\"high\":%" PRId32 ",\"low\":%" PRId32
                ",\"close\":%" PRId32 ",\"volume\":%" PRIu32 ",\"amount\":\"%" PRIu64
                "\",\"adjust_factor\":%" PRIu32 "}\n",
                key, row->open, row->high, row->low, row->close, row->volume, row->amount,
                row->adjust_factor);
    } else {
        char separator = format == SHELL_CSV ? ',' : (format == SHELL_TABLE ? ' ' : '\t');
        fprintf(file,
                "%" PRIu32 "%c%" PRId32 "%c%" PRId32 "%c%" PRId32 "%c%" PRId32 "%c%" PRIu32
                "%c%" PRIu64 "%c%" PRIu32 "\n",
                key, separator, row->open, separator, row->high, separator, row->low, separator,
                row->close, separator, row->volume, separator, row->amount, separator,
                row->adjust_factor);
    }
}

static ohlc_status query(shell* state, const shell_command* command, bool series) {
    size_t options_start = series ? 7 : 3;
    if (command->count < options_start ||
        (series && (!word_is(&command->words[3], "from") || !word_is(&command->words[5], "to")))) {
        return OHLC_INVALID;
    }
    output_format format = state->format;
    bool all = !state->interactive;
    bool overwrite = false;
    const char* path = state->output_path;
    for (size_t i = options_start; i < command->count; i++) {
        if (word_is(&command->words[i], "--all")) {
            all = true;
        } else if (word_is(&command->words[i], "--overwrite")) {
            overwrite = true;
        } else if (word_is(&command->words[i], "--format") && i + 1 < command->count) {
            if (!format_parse(&command->words[++i], &format)) {
                return OHLC_INVALID;
            }
        } else if (word_is(&command->words[i], "--output") && i + 1 < command->count) {
            if (!text_word(&command->words[++i])) {
                return OHLC_INVALID;
            }
            path = command->words[i].text;
        } else {
            return OHLC_INVALID;
        }
    }
    ohlc_table_info table;
    ohlc_status status = open_table(state, &command->words[1], &table);
    uint32_t key;
    uint32_t ticker = 0;
    uint64_t end = 0;
    if (status == OHLC_OK) {
        status = parse_time(&table, &command->words[series ? 4 : 2], &key);
    }
    if (status == OHLC_OK && series) {
        if (word_is(&command->words[6], "@4294967296")) {
            end = UINT64_C(4294967296);
        } else {
            uint32_t last;
            status = parse_time(&table, &command->words[6], &last);
            end = status == OHLC_OK ? last : 0;
        }
        if (status == OHLC_OK) {
            status = resolve(state, &command->words[2], false, &ticker);
        }
    }
    if (status != OHLC_OK) {
        return status;
    }
    FILE* file = stdout;
    char* temporary = NULL;
    if (path != NULL) {
        all = true;
        size_t length = strlen(path) + 24;
        temporary = malloc(length);
        if (temporary == NULL) {
            return OHLC_LIMIT;
        }
        snprintf(temporary, length, "%s.ohlc-XXXXXX", path);
        int fd = mkstemp(temporary);
        if (fd < 0 || (file = fdopen(fd, "w")) == NULL) {
            if (fd >= 0) {
                close(fd);
                unlink(temporary);
            }
            free(temporary);
            return OHLC_IO;
        }
    }
    status = series ? ohlc_client_series(state->client, table.id, ticker, key, end)
                    : ohlc_client_cross(state->client, table.id, key);
    if (status == OHLC_OK && format != SHELL_JSONL) {
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
                               "adjust_factor"};
        for (size_t i = 0; i < 8; i++) {
            fputs(names[i], file);
            fputc(i == 7 ? '\n'
                         : (format == SHELL_CSV ? ',' : (format == SHELL_TABLE ? ' ' : '\t')),
                  file);
        }
    }
    uint64_t shown = 0;
    uint64_t sequence = 0;
    bool final = false;
    bool truncated = false;
    while (status == OHLC_OK && !final) {
        ohlc_bytes rows;
        uint32_t count;
        status = ohlc_client_next(state->client, &rows, &count, &sequence, &final);
        if (status != OHLC_OK) {
            break;
        }
        for (uint32_t i = 0; i < count; i++) {
            if (!all && shown >= state->preview) {
                truncated = true;
                break;
            }
            const uint8_t* data = (const uint8_t*)rows.data + i * OHLC_RESULT_BYTES;
            ohlc_row row;
            ohlc_row_decode(data + 4, &row);
            if (format == SHELL_TABLE) {
                char time[80];
                status = ohlc_time_format_local(&table, series ? ohlc_get_u32(data) : key, time,
                                                sizeof(time));
                if (status != OHLC_OK) {
                    break;
                }
                fprintf(file, "%s ", time);
            }
            print_row(file, format, ohlc_get_u32(data), &row);
            shown++;
        }
        if (status != OHLC_OK) {
            break;
        }
        if (ferror(file) || ohlc_shell_interrupted) {
            status = ohlc_shell_interrupted ? OHLC_CANCELLED : OHLC_IO;
            break;
        }
        if (truncated || (!all && shown >= state->preview && !final)) {
            truncated = true;
            break;
        }
    }
    if (!final || status != OHLC_OK) {
        disconnect(state);
    }
    if (fflush(file) != 0) {
        status = OHLC_IO;
    }
    if (path != NULL) {
        if (status == OHLC_OK && fsync(fileno(file)) != 0) {
            status = OHLC_IO;
        }
        if (fclose(file) != 0) {
            status = OHLC_IO;
        }
        if (status == OHLC_OK) {
            int result = overwrite ? rename(temporary, path) : link(temporary, path);
            if (result != 0) {
                status = OHLC_IO;
            }
        }
        unlink(temporary);
        free(temporary);
    }
    if (truncated) {
        fprintf(stderr,
                "Preview truncated: shown=%" PRIu64 ", total unknown; connection cancelled.\n",
                shown);
    } else if (status == OHLC_OK && final) {
        fprintf(stderr, "Complete rows=%" PRIu64 " snapshot_seq=%" PRIu64 "\n", shown, sequence);
    } else {
        fprintf(stderr, "Incomplete result after %" PRIu64 " rows\n", shown);
    }
    return status;
}

static ohlc_status insert_row(shell* state, const shell_command* command) {
    if (command->count != 11) {
        return OHLC_INVALID;
    }
    ohlc_row row;
    if (!ohlc_shell_row(command->words + 4, &row)) {
        return OHLC_INVALID;
    }
    ohlc_table_info table;
    ohlc_status status = open_table(state, &command->words[1], &table);
    uint32_t key;
    uint32_t ticker;
    if (status == OHLC_OK) {
        status = parse_time(&table, &command->words[3], &key);
    }
    if (status == OHLC_OK) {
        status = resolve(state, &command->words[2], false, &ticker);
    }
    if (status == OHLC_OK) {
        uint8_t record[OHLC_WRITE_BYTES];
        ohlc_write_encode(record, ticker, key, &row);
        uint64_t sequence;
        status = ohlc_client_write(state->client, table.id, record, 1, &sequence);
        if (status == OHLC_OK) {
            fprintf(stderr, "Committed rows=1 commit_seq=%" PRIu64 "\n", sequence);
        }
    }
    return status;
}

static ohlc_status import_file(shell* state, const shell_command* command) {
    if (command->count < 3 || !text_word(&command->words[2])) {
        return OHLC_INVALID;
    }
    bool csv = false;
    bool header = true;
    uint64_t batch = 20000;
    for (size_t i = 3; i < command->count; i++) {
        if (word_is(&command->words[i], "--no-header")) {
            header = false;
        } else if (word_is(&command->words[i], "--format") && i + 1 < command->count) {
            i++;
            if (!word_is(&command->words[i], "tsv") && !word_is(&command->words[i], "csv")) {
                return OHLC_INVALID;
            }
            csv = word_is(&command->words[i], "csv");
        } else if (word_is(&command->words[i], "--batch-rows") && i + 1 < command->count) {
            if (!ohlc_shell_uint(&command->words[++i], OHLC_MAX_BATCH_ROWS, &batch) || batch == 0) {
                return OHLC_INVALID;
            }
        } else {
            return OHLC_INVALID;
        }
    }
    ohlc_connection_info limits;
    ohlc_client_info(state->client, &limits);
    if (batch > limits.max_write_rows || batch > (limits.max_frame_bytes - 8u) / OHLC_WRITE_BYTES) {
        return OHLC_LIMIT;
    }
    ohlc_table_info table;
    ohlc_status status = open_table(state, &command->words[1], &table);
    if (status != OHLC_OK) {
        return status;
    }
    FILE* file = fopen(command->words[2].text, "r");
    uint8_t* records = malloc((size_t)batch * OHLC_WRITE_BYTES);
    shell_command* fields = malloc(sizeof(*fields));
    if (file == NULL || records == NULL || fields == NULL) {
        free(records);
        free(fields);
        if (file != NULL) {
            fclose(file);
        }
        return OHLC_IO;
    }
    uint64_t confirmed = 0;
    uint64_t batches = 0;
    uint64_t record_number = 0;
    size_t pending = 0;
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
        if (fields->count != 9) {
            status = OHLC_INVALID;
            break;
        }
        if (header) {
            static const char* names[] = {"ticker", "time",   "open",   "high",         "low",
                                          "close",  "volume", "amount", "adjust_factor"};
            for (size_t i = 0; i < 9; i++) {
                if (fields->words[i].size != strlen(names[i]) ||
                    memcmp(fields->words[i].text, names[i], strlen(names[i])) != 0) {
                    status = OHLC_INVALID;
                    break;
                }
            }
            header = false;
            continue;
        }
        uint32_t ticker;
        uint32_t key;
        ohlc_row row;
        if (!ohlc_shell_row(fields->words + 2, &row)) {
            status = OHLC_INVALID;
            break;
        }
        status = parse_time(&table, &fields->words[1], &key);
        if (status == OHLC_OK) {
            status = resolve(state, &fields->words[0], true, &ticker);
        }
        if (status != OHLC_OK) {
            break;
        }
        ohlc_write_encode(records + pending * OHLC_WRITE_BYTES, ticker, key, &row);
        pending++;
        if (pending == batch) {
            uint64_t sequence;
            status = ohlc_client_write(state->client, table.id, records, pending, &sequence);
            if (status == OHLC_OK) {
                confirmed += pending;
                pending = 0;
                batches++;
                fprintf(stderr,
                        "Import confirmed batches=%" PRIu64 " rows=%" PRIu64
                        " through_record=%" PRIu64 " commit_seq=%" PRIu64 "\n",
                        batches, confirmed, record_number, sequence);
            }
        }
    }
    if (ohlc_shell_interrupted && status != OHLC_OUTCOME_UNKNOWN) {
        status = OHLC_CANCELLED;
    }
    if (header && status == OHLC_OK) {
        status = OHLC_INVALID;
    }
    if (status == OHLC_OK && pending != 0) {
        uint64_t sequence;
        status = ohlc_client_write(state->client, table.id, records, pending, &sequence);
        if (status == OHLC_OK) {
            confirmed += pending;
            batches++;
            pending = 0;
        }
    }
    fprintf(stderr,
            "Import %s: confirmed_batches=%" PRIu64 " confirmed_rows=%" PRIu64
            " input_record=%" PRIu64 " pending_rows=%zu; registrations remain committed.\n",
            status == OHLC_OK ? "complete" : "stopped", batches, confirmed, record_number, pending);
    fclose(file);
    free(records);
    free(fields);
    return status;
}

static ohlc_status show_help(const shell* state, const shell_command* command) {
    if (command->count > 2) {
        return OHLC_INVALID;
    }
    bool examples = command->count == 2 && word_is(&command->words[1], "examples");
    bool matched = command->count == 1 || examples;
    for (size_t i = 0; i < ohlc_shell_command_count; i++) {
        const shell_help* entry = &ohlc_shell_commands[i];
        if (command->count == 1) {
            printf("%-12s %s\n", entry->name, entry->description);
        } else if (examples || word_is(&command->words[1], entry->name)) {
            matched = true;
            if (!examples) {
                printf("%s\nUsage: %s;\n\n", entry->description, entry->syntax);
            }
            printf("%s\n\n", entry->example);
        }
    }
    if (!matched) {
        fprintf(stderr, "Unknown help topic. Use help;\n");
        return OHLC_INVALID;
    }
    puts("Fields: open high low close = int32; volume/factor = uint32; amount = uint64.\n"
         "Time: quoted date/datetime or @key. No rounding or aggregation.\n"
         "Query options: --format table|tsv|csv|jsonl --output PATH --overwrite --all\n"
         "Defaults: terminal preview 100 rows; scripts/files read through successful FINAL.\n"
         "Tokens support quotes, backslash and \\xHH escapes. No system-command escapes.\n"
         "Uncertain writes are never automatically retried.");
    static const char* formats[] = {"table", "tsv", "csv", "jsonl"};
    printf("Current settings: format=%s preview=%" PRIu64 " history=%s timing=%s\n",
           formats[state->format], state->preview, state->history ? "on" : "off",
           state->timing ? "on" : "off");
    return OHLC_OK;
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

static ohlc_status connection_options(shell* state, const shell_command* command) {
    bool socket = false;
    bool tcp = false;
    for (size_t i = 1; i < command->count; i++) {
        if (word_is(&command->words[i], "--tls")) {
            state->tls = true;
            continue;
        }
        char** target = NULL;
        if (word_is(&command->words[i], "--socket")) {
            target = &state->socket_path;
            socket = true;
        } else if (word_is(&command->words[i], "--host")) {
            target = &state->host;
            tcp = true;
        } else if (word_is(&command->words[i], "--port")) {
            target = &state->port;
            tcp = true;
        } else if (word_is(&command->words[i], "--ca")) {
            target = &state->ca_file;
        } else if (word_is(&command->words[i], "--token-file")) {
            target = &state->token_file;
        }
        if (target == NULL || i + 1 == command->count || (socket && tcp)) {
            return OHLC_INVALID;
        }
        ohlc_status status = assign(target, &command->words[++i]);
        if (status != OHLC_OK) {
            return status;
        }
    }
    if (tcp) {
        free(state->socket_path);
        state->socket_path = NULL;
    }
    if (socket) {
        state->tls = false;
    }
    return connect_server(state);
}

static ohlc_status run(shell* state, FILE* file, bool interactive);

static ohlc_status execute(shell* state, const shell_command* command) {
    if (command->count == 0) {
        return OHLC_OK;
    }
    const shell_word* word = command->words;
    if (word_is(word, "help")) {
        return show_help(state, command);
    }
    if (word_is(word, "quit") || word_is(word, "exit")) {
        state->quitting = command->count == 1;
        return state->quitting ? OHLC_OK : OHLC_INVALID;
    }
    if (word_is(word, "disconnect") && command->count == 1) {
        disconnect(state);
        return OHLC_OK;
    }
    if (word_is(word, "connect")) {
        return connection_options(state, command);
    }
    if (word_is(word, "status") && command->count == 1) {
        fprintf(stderr, "connected=%s last_status=%s unknown_mutation=%s unknown_command=%s\n",
                ohlc_client_connected(state->client) ? "yes" : "no",
                ohlc_status_string(state->last_status), state->unknown ? "yes" : "no",
                state->unknown_command);
        if (state->client != NULL) {
            ohlc_connection_info info;
            ohlc_client_info(state->client, &info);
            fputs("UUID=", stderr);
            for (size_t i = 0; i < 16; i++) {
                fprintf(stderr, "%02x", info.uuid[i]);
            }
            fprintf(stderr,
                    " protocol=3 access=%s max_frame=%" PRIu32 " max_rows=%" PRIu32
                    " query_ms=%" PRIu32 "\n",
                    (info.capabilities & OHLC_CAP_WRITE) != 0 ? "read/write" : "read",
                    info.max_frame_bytes, info.max_write_rows, info.max_query_ms);
        }
        return OHLC_OK;
    }
    if (word_is(word, "set") && command->count == 3) {
        if (word_is(word + 1, "preview")) {
            return ohlc_shell_uint(word + 2, UINT32_MAX, &state->preview) && state->preview != 0
                       ? OHLC_OK
                       : OHLC_INVALID;
        }
        if (word_is(word + 1, "format")) {
            return format_parse(word + 2, &state->format) ? OHLC_OK : OHLC_INVALID;
        }
        if (word_is(word + 1, "timing") || word_is(word + 1, "history")) {
            if (!word_is(word + 2, "on") && !word_is(word + 2, "off")) {
                return OHLC_INVALID;
            }
            bool enabled = word_is(word + 2, "on");
            if (word_is(word + 1, "timing")) {
                state->timing = enabled;
            } else {
                state->history = enabled;
            }
            return OHLC_OK;
        }
        return OHLC_INVALID;
    }
    if (word_is(word, "source") && command->count == 2 && text_word(word + 1)) {
        if (state->depth >= 8) {
            return OHLC_LIMIT;
        }
        FILE* source = fopen(word[1].text, "r");
        if (source == NULL) {
            return OHLC_IO;
        }
        state->depth++;
        ohlc_status status = run(state, source, false);
        state->depth--;
        fclose(source);
        return status;
    }
    bool known = false;
    for (size_t i = 0; i < ohlc_shell_command_count; i++) {
        known = known || word_is(word, ohlc_shell_commands[i].name);
    }
    if (!known) {
        return OHLC_INVALID;
    }
    if (!ohlc_client_connected(state->client)) {
        ohlc_status status = connect_server(state);
        if (status != OHLC_OK) {
            return status;
        }
    }
    if (word_is(word, "ping") && command->count == 1) {
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
    if (word_is(word, "checkpoint") && command->count == 1) {
        return ohlc_client_checkpoint(state->client);
    }
    if (word_is(word, "stats") && command->count == 1) {
        ohlc_stats stats;
        ohlc_status status = ohlc_client_stats(state->client, &stats);
        if (status == OHLC_OK) {
            printf("commit_seq=%" PRIu64 "\ncheckpoint_seq=%" PRIu64 "\nticker_count=%" PRIu64
                   "\ntable_count=%" PRIu32 "\nmemory_bytes=%" PRIu64 "\ndisk_read_bytes=%" PRIu64
                   "\ndisk_read_calls=%" PRIu64 "\ncache_hits=%" PRIu64 "\ncache_misses=%" PRIu64
                   "\nwal_bytes=%" PRIu64 "\ndata_bytes=%" PRIu64 "\n",
                   stats.commit_seq, stats.checkpoint_seq, stats.ticker_count, stats.table_count,
                   stats.memory_bytes, stats.disk_read_bytes, stats.disk_read_calls,
                   stats.cache_hits, stats.cache_misses, stats.wal_bytes, stats.data_bytes);
        }
        return status;
    }
    if (word_is(word, "create")) {
        return create_table(state, command);
    }
    if (word_is(word, "drop")) {
        return drop_table(state, command);
    }
    if (word_is(word, "tables") && command->count == 1) {
        return list_metadata(state, true, NULL);
    }
    if (word_is(word, "tickers") && command->count <= 2) {
        return list_metadata(state, false, command->count == 2 ? word + 1 : NULL);
    }
    if (word_is(word, "describe") && command->count == 2) {
        ohlc_table_info table;
        ohlc_status status = open_table(state, word + 1, &table);
        if (status == OHLC_OK) {
            print_definition(&table);
            puts("0 open int32\n1 high int32\n2 low int32\n3 close int32\n4 volume uint32\n5 "
                 "amount uint64\n6 adjust_factor uint32");
        }
        return status;
    }
    if ((word_is(word, "resolve") || word_is(word, "register")) && command->count == 2) {
        uint32_t code;
        ohlc_status status;
        if (word_is(word, "register")) {
            uint64_t sequence;
            status = ohlc_client_register(state->client, (ohlc_bytes){word[1].text, word[1].size},
                                          &code, &sequence);
            if (status == OHLC_OK) {
                fprintf(stderr, "Registered commit_seq=%" PRIu64 "\n", sequence);
                cache_symbol(state, word + 1, code);
            }
        } else {
            status = resolve(state, word + 1, false, &code);
        }
        if (status == OHLC_OK) {
            printf("%" PRIu32 "\n", code);
        }
        return status;
    }
    if (word_is(word, "series") || word_is(word, "cross")) {
        return query(state, command, word_is(word, "series"));
    }
    if (word_is(word, "insert") || word_is(word, "put")) {
        return insert_row(state, command);
    }
    if (word_is(word, "import")) {
        return import_file(state, command);
    }
    return OHLC_INVALID;
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
            snprintf(state->unknown_command, sizeof(state->unknown_command), "%.*s",
                     command->count != 0 ? (int)command->words[0].size : 0,
                     command->count != 0 ? command->words[0].text : "");
        } else if (ohlc_shell_interrupted) {
            status = OHLC_CANCELLED;
        }
        if (fflush(stdout) != 0 && status == OHLC_OK) {
            status = OHLC_IO;
        }
        state->last_status = status;
        if (status != OHLC_OK) {
            fprintf(stderr, "%s%s\n", ohlc_status_string(status),
                    status == OHLC_OUTCOME_UNKNOWN
                        ? ": mutation may have committed; inspect before retrying"
                        : "");
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
    state->format = state->interactive ? SHELL_TABLE : SHELL_TSV;
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
        shell_word value = {optarg, optarg != NULL ? strlen(optarg) : 0};
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

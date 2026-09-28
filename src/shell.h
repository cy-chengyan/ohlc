/* SPDX-License-Identifier: Apache-2.0 */
#ifndef OHLC_SHELL_H
#define OHLC_SHELL_H

#include "net.h"

#include <signal.h>
#include <stdio.h>

#define OHLC_SHELL_COMMAND_BYTES 65536u
#define OHLC_SHELL_WORDS 8192u

typedef struct {
    char* text;
    size_t size;
    char punctuation;
} shell_word;

typedef struct {
    char data[OHLC_SHELL_COMMAND_BYTES + OHLC_SHELL_WORDS];
    shell_word words[OHLC_SHELL_WORDS];
    size_t count;
} shell_command;

typedef struct {
    FILE* file;
    bool interactive;
    bool history;
    char* line;
    size_t position;
    size_t length;
} shell_input;

typedef enum {
    SHELL_HELP_TABLES,
    SHELL_HELP_QUERIES,
    SHELL_HELP_TICKERS,
    SHELL_HELP_WRITES,
    SHELL_HELP_CONNECTION,
    SHELL_HELP_SESSION,
    SHELL_HELP_GROUP_COUNT
} shell_help_group;

typedef struct {
    const char* name;
    shell_help_group group;
    const char* summary;
    const char* syntax;
    const char* description;
    const char* example;
} shell_help;

typedef struct {
    const char* format;
    uint64_t preview;
    bool history;
    bool timing;
} shell_help_settings;

extern const shell_help ohlc_shell_commands[];
extern const size_t ohlc_shell_command_count;
extern volatile sig_atomic_t ohlc_shell_interrupted;

/* Print local help without connecting or executing examples. Settings are
 * borrowed for the call; output goes to stdout. Unknown topics return INVALID. */
ohlc_status ohlc_shell_show_help(const shell_command* command, const shell_help_settings* settings);

ohlc_status ohlc_shell_read(shell_input* input, shell_command* command, bool* eof);
/* Edit one terminal line. On success, output owns an allocated newline-terminated
 * string (free with free), or NULL on EOF. Restores terminal settings on return.
 * History and the kill buffer are session-local; calls must be serialized. */
ohlc_status ohlc_shell_edit_line(const char* prompt, bool keep_history, char** output, bool* eof);
void ohlc_shell_input_close(shell_input* input);
void ohlc_shell_history_save(bool enabled);
const char* ohlc_shell_complete(const char* prefix, size_t index);
void ohlc_shell_print_bytes(FILE* file, const void* bytes, size_t size);
bool ohlc_shell_uint(const shell_word* word, uint64_t maximum, uint64_t* output);
bool ohlc_shell_row(const shell_word* values, ohlc_row* row);
ohlc_status ohlc_shell_text_record(FILE* file, bool csv, shell_command* fields, bool* eof);

#endif

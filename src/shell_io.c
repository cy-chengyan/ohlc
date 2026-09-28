/* SPDX-License-Identifier: Apache-2.0 */
#include "shell.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

void ohlc_shell_print_bytes(FILE* file, const void* bytes, size_t size) {
    const uint8_t* data = bytes;
    for (size_t i = 0; i < size; i++) {
        unsigned int c = data[i];
        if (c == '\\') {
            fputs("\\\\", file);
        } else if (c >= 32 && c <= 126) {
            fputc((int)c, file);
        } else {
            fprintf(file, "\\x%02x", c);
        }
    }
}

static int hex_digit(unsigned char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

static ohlc_status split_command(const char* text, size_t size, shell_command* command) {
    size_t position = 0;
    size_t output = 0;
    command->count = 0;
    bool list_syntax = false;
    while (position < size) {
        while (position < size && isspace((unsigned char)text[position])) {
            position++;
        }
        if (position == size) {
            break;
        }
        if (command->count == OHLC_SHELL_WORDS) {
            return OHLC_LIMIT;
        }
        /* Punctuation is structural only in the CROSS ticker list. Other
         * command arguments retain their existing byte-preserving syntax. */
        if (command->count == 5 && strcasecmp(command->words[0].text, "cross") == 0 &&
            strcasecmp(command->words[3].text, "and") == 0 &&
            strcasecmp(command->words[4].text, "ticker") == 0) {
            list_syntax = true;
        }
        shell_word* word = &command->words[command->count++];
        word->text = command->data + output;
        word->punctuation = 0;
        if (list_syntax && strchr("(),", text[position]) != NULL) {
            word->punctuation = text[position++];
            command->data[output++] = word->punctuation;
            command->data[output++] = '\0';
            word->size = 1;
            if (word->punctuation == ')') {
                list_syntax = false;
            }
            continue;
        }
        char quote = text[position] == '\'' || text[position] == '"' ? text[position++] : 0;
        bool closed = quote == 0;
        while (position < size) {
            unsigned char c = (unsigned char)text[position++];
            if (quote != 0 && c == (unsigned char)quote) {
                closed = true;
                break;
            }
            if (quote == 0 && isspace(c)) {
                break;
            }
            if (quote == 0 && list_syntax && strchr("(),", c) != NULL) {
                position--;
                break;
            }
            if (quote == 0 && (c == '\'' || c == '"')) {
                return OHLC_INVALID;
            }
            if (c == '\\') {
                if (position == size) {
                    return OHLC_INVALID;
                }
                c = (unsigned char)text[position++];
                if (c == 'x') {
                    if (size - position < 2 || hex_digit((unsigned char)text[position]) < 0 ||
                        hex_digit((unsigned char)text[position + 1]) < 0) {
                        return OHLC_INVALID;
                    }
                    c = (unsigned char)(hex_digit((unsigned char)text[position]) * 16 +
                                        hex_digit((unsigned char)text[position + 1]));
                    position += 2;
                } else if (c != '\\' && c != '\'' && c != '"') {
                    return OHLC_INVALID;
                }
            }
            command->data[output++] = (char)c;
        }
        if (!closed || (quote != 0 && position < size && !isspace((unsigned char)text[position]) &&
                        !(list_syntax && strchr("(),", text[position]) != NULL))) {
            return OHLC_INVALID;
        }
        word->size = (size_t)(command->data + output - word->text);
        command->data[output++] = '\0';
    }
    return OHLC_OK;
}

ohlc_status ohlc_shell_read(shell_input* input, shell_command* command, bool* eof) {
    command->count = 0;
    char* text = malloc(OHLC_SHELL_COMMAND_BYTES + 1);
    if (text == NULL) {
        return OHLC_LIMIT;
    }
    size_t size = 0;
    char quote = 0;
    bool escaped = false;
    bool comment = false;
    *eof = false;
    ohlc_status status = OHLC_OK;
    for (;;) {
        int c;
        if (input->interactive) {
            if (input->position == input->length) {
                free(input->line);
                input->line = NULL;
                status = ohlc_shell_edit_line(size == 0 ? "ohlc> " : "...> ", input->history,
                                              &input->line, eof);
                if (status != OHLC_OK || *eof) {
                    break;
                }
                input->length = strlen(input->line);
                input->position = 0;
            }
            c = (unsigned char)input->line[input->position++];
        } else {
            c = fgetc(input->file);
        }
        if (c == EOF) {
            *eof = true;
            if (ferror(input->file)) {
                status = OHLC_IO;
            }
            break;
        }
        if (comment && c != '\n') {
            continue;
        }
        if (comment) {
            comment = false;
        }
        if (!escaped && quote == 0 && c == '#') {
            comment = true;
            continue;
        }
        if (!escaped && quote == 0 && c == ';') {
            break;
        }
        if (c == 0 || size == OHLC_SHELL_COMMAND_BYTES) {
            status = OHLC_LIMIT;
            break;
        }
        if (size == 0 && isspace((unsigned char)c)) {
            continue;
        }
        text[size++] = (char)c;
        if (escaped) {
            escaped = false;
        } else if (c == '\\') {
            escaped = true;
        } else if (c == quote) {
            quote = 0;
        } else if (quote == 0 && (c == '\'' || c == '"')) {
            quote = (char)c;
        }
        if (input->interactive && c == '\n' && quote == 0 && size >= 5 &&
            strncasecmp(text, "help", 4) == 0 && isspace((unsigned char)text[4])) {
            break;
        }
    }
    if (status == OHLC_OK) {
        status = quote != 0 || escaped || (input->interactive && *eof && size != 0)
                     ? OHLC_INVALID
                     : split_command(text, size, command);
    }
    free(text);
    return status;
}

void ohlc_shell_input_close(shell_input* input) {
    free(input->line);
    input->line = NULL;
}

bool ohlc_shell_uint(const shell_word* word, uint64_t maximum, uint64_t* output) {
    uint64_t value = 0;
    if (word->size == 0) {
        return false;
    }
    for (size_t i = 0; i < word->size; i++) {
        unsigned int digit = (unsigned char)word->text[i] - (unsigned int)'0';
        if (digit > 9 || value > maximum / 10 || (value == maximum / 10 && digit > maximum % 10)) {
            return false;
        }
        value = value * 10 + digit;
    }
    *output = value;
    return true;
}

bool ohlc_shell_row(const shell_word* values, ohlc_row* row) {
    int32_t prices[4];
    for (size_t i = 0; i < 4; i++) {
        shell_word word = values[i];
        bool negative = word.size != 0 && word.text[0] == '-';
        if (negative) {
            word.text++;
            word.size--;
        }
        uint64_t value;
        if (!ohlc_shell_uint(&word, negative ? UINT64_C(2147483648) : INT32_MAX, &value)) {
            return false;
        }
        prices[i] = negative ? (int32_t)(-(int64_t)value) : (int32_t)value;
    }
    uint64_t volume;
    uint64_t amount;
    uint64_t factor;
    if (!ohlc_shell_uint(&values[4], UINT32_MAX, &volume) ||
        !ohlc_shell_uint(&values[5], UINT64_MAX, &amount) ||
        !ohlc_shell_uint(&values[6], UINT32_MAX, &factor)) {
        return false;
    }
    *row = (ohlc_row){prices[0],        prices[1], prices[2],       prices[3],
                      (uint32_t)volume, amount,    (uint32_t)factor};
    return true;
}

ohlc_status ohlc_shell_text_record(FILE* file, bool csv, shell_command* fields, bool* eof) {
    fields->count = 0;
    *eof = false;
    size_t used = 0;
    int separator = csv ? ',' : '\t';
    for (;;) {
        if (fields->count == OHLC_SHELL_WORDS) {
            return OHLC_LIMIT;
        }
        shell_word* field = &fields->words[fields->count++];
        field->text = fields->data + used;
        field->punctuation = 0;
        int c = fgetc(file);
        bool quoted = csv && c == '"';
        bool ended_quote = !quoted;
        if (quoted) {
            c = fgetc(file);
        }
        for (;;) {
            if (used >= OHLC_SHELL_COMMAND_BYTES) {
                return OHLC_LIMIT;
            }
            if (c == EOF) {
                *eof = true;
                break;
            }
            if (quoted && c == '"') {
                c = fgetc(file);
                if (c != '"') {
                    ended_quote = true;
                    break;
                }
            } else if (!quoted && (c == separator || c == '\n' || c == '\r')) {
                break;
            }
            if (c == 0 || (!quoted && csv && c == '"')) {
                return OHLC_INVALID;
            }
            if (c == '\\') {
                c = fgetc(file);
                if (c == 'x') {
                    int high = fgetc(file);
                    int low = fgetc(file);
                    if (high == EOF || low == EOF || hex_digit((unsigned char)high) < 0 ||
                        hex_digit((unsigned char)low) < 0) {
                        return OHLC_INVALID;
                    }
                    c = hex_digit((unsigned char)high) * 16 + hex_digit((unsigned char)low);
                } else if (c != '\\') {
                    return OHLC_INVALID;
                }
            }
            fields->data[used++] = (char)c;
            c = fgetc(file);
        }
        if (!ended_quote || ferror(file)) {
            return OHLC_INVALID;
        }
        field->size = (size_t)(fields->data + used - field->text);
        fields->data[used++] = '\0';
        if (c == separator) {
            continue;
        }
        if (c == '\r') {
            c = fgetc(file);
            if (c != '\n' && c != EOF) {
                return OHLC_INVALID;
            }
        }
        if (c != '\n' && c != EOF) {
            return OHLC_INVALID;
        }
        if (*eof && fields->count == 1 && field->size == 0) {
            fields->count = 0;
        }
        return OHLC_OK;
    }
}

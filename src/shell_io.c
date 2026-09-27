/* SPDX-License-Identifier: Apache-2.0 */
#include "shell.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

#define OHLC_HISTORY_COUNT 200u
#define OHLC_HISTORY_LINE 4096u

static char* history[OHLC_HISTORY_COUNT];
static size_t history_count;
static bool history_loaded;

static void add_history(const char* line) {
    size_t length = strlen(line);
    if (length == 0 || length > OHLC_HISTORY_LINE ||
        (history_count != 0 && strcmp(history[history_count - 1], line) == 0)) {
        return;
    }
    char* copy = strdup(line);
    if (copy == NULL) {
        return;
    }
    if (history_count == OHLC_HISTORY_COUNT) {
        free(history[0]);
        memmove(history, history + 1, (OHLC_HISTORY_COUNT - 1) * sizeof(*history));
        history_count--;
    }
    history[history_count++] = copy;
}

static char* history_path(void) {
    const char* directory = getenv("XDG_STATE_HOME");
    const char* home_directory = getenv("HOME");
    bool xdg = directory != NULL && directory[0] == '/';
    if (!xdg) {
        directory = home_directory;
    }
    if (directory == NULL || directory[0] != '/') {
        return NULL;
    }
    const char* suffix = xdg ? "/ohlc-history" : "/.ohlc_history";
    size_t length = strlen(directory) + strlen(suffix) + 1;
    char* path = malloc(length);
    if (path != NULL) {
        snprintf(path, length, "%s%s", directory, suffix);
    }
    return path;
}

static void load_history(void) {
    if (history_loaded) {
        return;
    }
    history_loaded = true;
    char* path = history_path();
    if (path == NULL) {
        return;
    }
    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    free(path);
    struct stat info;
    if (fd < 0) {
        return;
    }
    if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) || (info.st_mode & 0077) != 0 ||
        info.st_uid != geteuid() || info.st_size > OHLC_HISTORY_COUNT * OHLC_HISTORY_LINE) {
        close(fd);
        return;
    }
    FILE* file = fdopen(fd, "r");
    if (file == NULL) {
        close(fd);
        return;
    }
    char line[OHLC_HISTORY_LINE + 2];
    while (fgets(line, sizeof(line), file) != NULL) {
        line[strcspn(line, "\r\n")] = '\0';
        add_history(line);
    }
    fclose(file);
}

void ohlc_shell_history_save(bool enabled) {
    char* path = history_loaded && enabled ? history_path() : NULL;
    if (path != NULL) {
        int fd = open(path, O_WRONLY | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
        struct stat info;
        if (fd >= 0) {
            if (fstat(fd, &info) == 0 && S_ISREG(info.st_mode) && info.st_uid == geteuid() &&
                (info.st_mode & 0077) == 0 && ftruncate(fd, 0) == 0) {
                for (size_t i = 0; i < history_count; i++) {
                    size_t length = strlen(history[i]);
                    if (write(fd, history[i], length) != (ssize_t)length ||
                        write(fd, "\n", 1) != 1) {
                        break;
                    }
                }
            }
            close(fd);
        }
        free(path);
    }
    for (size_t i = 0; i < history_count; i++) {
        free(history[i]);
    }
    history_count = 0;
}

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

static size_t rendered_width(unsigned char c) {
    return c == '\\' ? 2u : (c >= 32 && c <= 126 ? 1u : 4u);
}

static void redraw(const char* prompt, const char* text, size_t length, size_t cursor) {
    struct winsize window = {0};
    size_t columns =
        ioctl(STDERR_FILENO, TIOCGWINSZ, &window) == 0 && window.ws_col >= 20 ? window.ws_col : 80;
    size_t available = columns - strlen(prompt) - 2;
    size_t start = cursor;
    size_t width = 0;
    while (start > 0 &&
           width + rendered_width((unsigned char)text[start - 1]) < available * 3 / 4) {
        width += rendered_width((unsigned char)text[--start]);
    }
    size_t end = cursor;
    size_t behind = 0;
    while (end < length && width + rendered_width((unsigned char)text[end]) < available) {
        size_t part = rendered_width((unsigned char)text[end++]);
        width += part;
        behind += part;
    }
    fputs("\r\033[2K", stderr);
    fputs(prompt, stderr);
    /* Escape bytes consistently so arbitrary pasted strings cannot control
     * the terminal. Cursor displacement uses the same rendered width. */
    ohlc_shell_print_bytes(stderr, text + start, end - start);
    if (behind != 0) {
        fprintf(stderr, "\033[%zuD", behind);
    }
    fflush(stderr);
}

static ohlc_status edit_line(const char* prompt, bool keep_history, char** output, bool* eof) {
    *output = NULL;
    *eof = false;
    if (keep_history) {
        load_history();
    }
    struct termios original;
    if (tcgetattr(STDIN_FILENO, &original) != 0) {
        return OHLC_IO;
    }
    struct termios raw = original;
    raw.c_lflag &= (tcflag_t) ~(ICANON | ECHO | ISIG);
    raw.c_iflag &= (tcflag_t) ~(IXON | ICRNL);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;
    if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) != 0) {
        return OHLC_IO;
    }
    char* text = malloc(OHLC_SHELL_COMMAND_BYTES + 2);
    ohlc_status status = text == NULL ? OHLC_LIMIT : OHLC_OK;
    size_t length = 0;
    size_t cursor = 0;
    size_t selected = history_count;
    char search[OHLC_HISTORY_LINE + 1] = {0};
    bool searching = false;
    if (text != NULL) {
        text[0] = '\0';
        redraw(prompt, text, length, cursor);
    }
    while (status == OHLC_OK) {
        uint8_t c;
        if (read(STDIN_FILENO, &c, 1) != 1) {
            status = OHLC_IO;
            break;
        }
        if (c == 3) {
            status = OHLC_CANCELLED;
            break;
        }
        if (c == 4 && length == 0) {
            *eof = true;
            break;
        }
        if (c == '\r' || c == '\n') {
            if (keep_history && strncasecmp(text, "connect", 7) != 0) {
                add_history(text);
            }
            text[length++] = '\n';
            text[length] = '\0';
            break;
        }
        if (c == 1) {
            cursor = 0;
        } else if (c == 5) {
            cursor = length;
        } else if ((c == 127 || c == 8) && cursor != 0) {
            memmove(text + cursor - 1, text + cursor, length - cursor + 1);
            cursor--;
            length--;
        } else if (c == 18) {
            if (!searching) {
                size_t copied = length < OHLC_HISTORY_LINE ? length : OHLC_HISTORY_LINE;
                memcpy(search, text, copied);
                search[copied] = '\0';
                searching = true;
            }
            for (size_t i = selected; i > 0; i--) {
                if (strstr(history[i - 1], search) != NULL) {
                    selected = i - 1;
                    strcpy(text, history[selected]);
                    length = strlen(text);
                    cursor = length;
                    break;
                }
            }
        } else if (c == '\t') {
            size_t start = cursor;
            while (start != 0 && !isspace((unsigned char)text[start - 1])) {
                start--;
            }
            char prefix[4097];
            const char* match = NULL;
            if (cursor == length && length - start < sizeof(prefix)) {
                memcpy(prefix, text + start, length - start);
                prefix[length - start] = '\0';
                match = ohlc_shell_complete(prefix, 0);
                if (match != NULL && ohlc_shell_complete(prefix, 1) != NULL) {
                    match = NULL;
                }
            }
            if (match != NULL && start + strlen(match) + 1 < OHLC_SHELL_COMMAND_BYTES) {
                strcpy(text + start, match);
                length = start + strlen(match);
                text[length++] = ' ';
                text[length] = '\0';
                cursor = length;
            }
        } else if (c == 27) {
            uint8_t sequence[2];
            struct pollfd key = {.fd = STDIN_FILENO, .events = POLLIN};
            if (poll(&key, 1, 100) <= 0 || read(STDIN_FILENO, sequence, 1) != 1 ||
                sequence[0] != '[' || poll(&key, 1, 100) <= 0 ||
                read(STDIN_FILENO, sequence + 1, 1) != 1) {
                continue;
            }
            if (sequence[1] == 'D' && cursor != 0) {
                cursor--;
            } else if (sequence[1] == 'C' && cursor < length) {
                cursor++;
            } else if (sequence[1] == 'A' || sequence[1] == 'B') {
                if (sequence[1] == 'A' && selected > 0) {
                    selected--;
                } else if (sequence[1] == 'B' && selected < history_count) {
                    selected++;
                }
                strcpy(text, selected < history_count ? history[selected] : "");
                length = strlen(text);
                cursor = length;
            }
        } else if (c >= 32 && c != 127 && length < OHLC_SHELL_COMMAND_BYTES) {
            memmove(text + cursor + 1, text + cursor, length - cursor + 1);
            text[cursor++] = (char)c;
            length++;
        }
        if (c != 18) {
            searching = false;
        }
        redraw(prompt, text, length, cursor);
    }
    tcsetattr(STDIN_FILENO, TCSANOW, &original);
    fputc('\n', stderr);
    if (status == OHLC_OK) {
        *output = text;
    } else {
        free(text);
    }
    return status;
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
        shell_word* word = &command->words[command->count++];
        word->text = command->data + output;
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
        if (!closed || (quote != 0 && position < size && !isspace((unsigned char)text[position]))) {
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
                status =
                    edit_line(size == 0 ? "ohlc> " : "...> ", input->history, &input->line, eof);
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

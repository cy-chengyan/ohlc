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
static char killed_text[OHLC_SHELL_COMMAND_BYTES + 1];
static size_t killed_length;

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
    history_loaded = false;
    memset(killed_text, 0, sizeof(killed_text));
    killed_length = 0;
}

typedef struct {
    char text[OHLC_SHELL_COMMAND_BYTES + 2];
    size_t length;
    size_t cursor;
} edit_buffer;

typedef struct {
    edit_buffer line;
    edit_buffer draft;
    edit_buffer search_original;
    char search[OHLC_HISTORY_LINE + 1];
    size_t search_length;
    size_t search_selected;
    size_t selected;
    bool keep_history;
    bool searching;
    bool search_failed;
    bool last_kill;
} line_editor;

typedef enum {
    OHLC_KEY_ESCAPE = 256,
    OHLC_KEY_LEFT,
    OHLC_KEY_RIGHT,
    OHLC_KEY_UP,
    OHLC_KEY_DOWN,
    OHLC_KEY_HOME,
    OHLC_KEY_END,
    OHLC_KEY_DELETE,
    OHLC_KEY_WORD_LEFT,
    OHLC_KEY_WORD_RIGHT,
    OHLC_KEY_KILL_WORD,
    OHLC_KEY_KILL_PREVIOUS_WORD,
    OHLC_KEY_IGNORE
} edit_key;

static int read_byte(void) {
    unsigned char byte;
    ssize_t count;
    do {
        count = read(STDIN_FILENO, &byte, 1);
    } while (count < 0 && errno == EINTR);
    return count == 1 ? byte : -1;
}

static int escape_byte(void) {
    struct pollfd key = {.fd = STDIN_FILENO, .events = POLLIN};
    int ready;
    do {
        ready = poll(&key, 1, 100);
    } while (ready < 0 && errno == EINTR);
    return ready > 0 ? read_byte() : -1;
}

static int read_key(void) {
    int key = read_byte();
    if (key != 27) {
        return key;
    }
    int first = escape_byte();
    if (first < 0) {
        return OHLC_KEY_ESCAPE;
    }
    switch (first) {
    case 'b':
    case 'B':
        return OHLC_KEY_WORD_LEFT;
    case 'f':
    case 'F':
        return OHLC_KEY_WORD_RIGHT;
    case 'd':
    case 'D':
        return OHLC_KEY_KILL_WORD;
    case 8:
    case 127:
        return OHLC_KEY_KILL_PREVIOUS_WORD;
    default:
        break;
    }
    if (first != '[' && first != 'O') {
        return OHLC_KEY_IGNORE;
    }
    char sequence[16];
    size_t length = 0;
    while (length + 1 < sizeof(sequence)) {
        int next = escape_byte();
        if (next < 0) {
            return OHLC_KEY_IGNORE;
        }
        sequence[length++] = (char)next;
        if (next >= 0x40 && next <= 0x7e) {
            break;
        }
    }
    sequence[length] = '\0';
    if (strcmp(sequence, "A") == 0) {
        return OHLC_KEY_UP;
    }
    if (strcmp(sequence, "B") == 0) {
        return OHLC_KEY_DOWN;
    }
    if (strcmp(sequence, "C") == 0) {
        return OHLC_KEY_RIGHT;
    }
    if (strcmp(sequence, "D") == 0) {
        return OHLC_KEY_LEFT;
    }
    if (strcmp(sequence, "H") == 0 || strcmp(sequence, "1~") == 0 || strcmp(sequence, "7~") == 0) {
        return OHLC_KEY_HOME;
    }
    if (strcmp(sequence, "F") == 0 || strcmp(sequence, "4~") == 0 || strcmp(sequence, "8~") == 0) {
        return OHLC_KEY_END;
    }
    if (strcmp(sequence, "3~") == 0) {
        return OHLC_KEY_DELETE;
    }
    if (strcmp(sequence, "1;5D") == 0 || strcmp(sequence, "1;3D") == 0) {
        return OHLC_KEY_WORD_LEFT;
    }
    if (strcmp(sequence, "1;5C") == 0 || strcmp(sequence, "1;3C") == 0) {
        return OHLC_KEY_WORD_RIGHT;
    }
    return OHLC_KEY_IGNORE;
}

static size_t rendered_width(unsigned char c) {
    return c == '\\' ? 2u : (c >= 32 && c <= 126 ? 1u : 4u);
}

static void redraw(const line_editor* editor, const char* prompt) {
    struct winsize window = {0};
    size_t columns =
        ioctl(STDERR_FILENO, TIOCGWINSZ, &window) == 0 && window.ws_col >= 20 ? window.ws_col : 80;
    fputs("\r\033[2K", stderr);
    size_t prompt_width = strlen(prompt);
    if (editor->searching) {
        const char* label =
            editor->search_failed ? "(failed reverse-i-search)" : "(reverse-i-search)";
        if (columns < 60) {
            label = editor->search_failed ? "(failed)" : "(search)";
        }
        size_t query_start = editor->search_length;
        size_t query_width = 0;
        size_t query_budget = columns / 3;
        while (query_start > 0 &&
               query_width + rendered_width((unsigned char)editor->search[query_start - 1]) <=
                   query_budget) {
            query_width += rendered_width((unsigned char)editor->search[--query_start]);
        }
        fprintf(stderr, "%s`", label);
        ohlc_shell_print_bytes(stderr, editor->search + query_start,
                               editor->search_length - query_start);
        fputs("': ", stderr);
        prompt_width = strlen(label) + query_width + 4;
    } else {
        fputs(prompt, stderr);
    }
    size_t available = columns > prompt_width + 2 ? columns - prompt_width - 2 : 1;
    const edit_buffer* line = &editor->line;
    size_t start = line->cursor;
    size_t width = 0;
    while (start > 0 &&
           width + rendered_width((unsigned char)line->text[start - 1]) < available * 3 / 4) {
        width += rendered_width((unsigned char)line->text[--start]);
    }
    size_t end = line->cursor;
    size_t behind = 0;
    while (end < line->length &&
           width + rendered_width((unsigned char)line->text[end]) < available) {
        size_t part = rendered_width((unsigned char)line->text[end++]);
        width += part;
        behind += part;
    }
    /* Display pasted bytes and search terms safely; cursor widths use the same escaping. */
    ohlc_shell_print_bytes(stderr, line->text + start, end - start);
    if (behind != 0) {
        fprintf(stderr, "\033[%zuD", behind);
    }
    fflush(stderr);
}

static void insert_text(edit_buffer* line, const char* text, size_t length) {
    if (length > OHLC_SHELL_COMMAND_BYTES - line->length) {
        return;
    }
    memmove(line->text + line->cursor + length, line->text + line->cursor,
            line->length - line->cursor + 1);
    memcpy(line->text + line->cursor, text, length);
    line->length += length;
    line->cursor += length;
}

static void delete_range(edit_buffer* line, size_t start, size_t end) {
    memmove(line->text + start, line->text + end, line->length - end + 1);
    line->length -= end - start;
    line->cursor = start;
}

static void kill_range(line_editor* editor, size_t start, size_t end, bool backward) {
    size_t length = end - start;
    if (length == 0) {
        return;
    }
    if (!editor->last_kill || length > OHLC_SHELL_COMMAND_BYTES - killed_length) {
        killed_length = 0;
    }
    if (backward) {
        memmove(killed_text + length, killed_text, killed_length);
        memcpy(killed_text, editor->line.text + start, length);
    } else {
        memcpy(killed_text + killed_length, editor->line.text + start, length);
    }
    killed_length += length;
    killed_text[killed_length] = '\0';
    delete_range(&editor->line, start, end);
}

static bool word_byte(unsigned char c) {
    return isalnum(c) || c == '_';
}

static size_t previous_word(const edit_buffer* line, bool whitespace_only) {
    size_t position = line->cursor;
    while (position > 0 &&
           (whitespace_only ? isspace((unsigned char)line->text[position - 1])
                            : !word_byte((unsigned char)line->text[position - 1]))) {
        position--;
    }
    while (position > 0 && (whitespace_only ? !isspace((unsigned char)line->text[position - 1])
                                            : word_byte((unsigned char)line->text[position - 1]))) {
        position--;
    }
    return position;
}

static size_t next_word(const edit_buffer* line) {
    size_t position = line->cursor;
    while (position < line->length && !word_byte((unsigned char)line->text[position])) {
        position++;
    }
    while (position < line->length && word_byte((unsigned char)line->text[position])) {
        position++;
    }
    return position;
}

static void select_history(line_editor* editor, size_t selected) {
    editor->selected = selected;
    if (selected == history_count) {
        editor->line = editor->draft;
        return;
    }
    edit_buffer* line = &editor->line;
    line->length = strlen(history[selected]);
    memcpy(line->text, history[selected], line->length + 1);
    line->cursor = line->length;
}

static void move_history(line_editor* editor, bool backward) {
    if (!editor->keep_history || history_count == 0) {
        return;
    }
    if (backward && editor->selected > 0) {
        if (editor->selected == history_count) {
            editor->draft = editor->line;
        }
        select_history(editor, editor->selected - 1);
    } else if (!backward && editor->selected < history_count) {
        select_history(editor, editor->selected + 1);
    }
}

static void search_history(line_editor* editor, size_t before) {
    for (size_t i = before; i > 0; i--) {
        if (strstr(history[i - 1], editor->search) != NULL) {
            select_history(editor, i - 1);
            editor->search_failed = false;
            return;
        }
    }
    editor->search_failed = true;
}

/* Return true when the search consumes a key. Other editing keys accept the
 * displayed match and are then handled normally; Escape only accepts it. */
static bool search_key(line_editor* editor, int key) {
    if (!editor->searching) {
        if (key != 18 || !editor->keep_history) {
            return false;
        }
        editor->searching = true;
        editor->search_original = editor->line;
        editor->search_selected = editor->selected;
        editor->search_length = 0;
        editor->search[0] = '\0';
        if (editor->selected == history_count) {
            editor->draft = editor->line;
        }
        search_history(editor, editor->selected);
        return true;
    }
    if (key == 7) {
        editor->line = editor->search_original;
        editor->selected = editor->search_selected;
        editor->searching = false;
    } else if (key == 18) {
        search_history(editor, editor->selected);
    } else if (key == 127 || key == 8) {
        if (editor->search_length != 0) {
            editor->search[--editor->search_length] = '\0';
            size_t before = editor->search_selected;
            if (before < history_count) {
                before++;
            }
            search_history(editor, before);
        }
    } else if (key >= 32 && key <= 255) {
        if (editor->search_length < OHLC_HISTORY_LINE) {
            editor->search[editor->search_length++] = (char)key;
            editor->search[editor->search_length] = '\0';
            size_t before = editor->selected;
            if (before < history_count) {
                before++;
            }
            search_history(editor, before);
        }
    } else {
        editor->searching = false;
        return key == OHLC_KEY_ESCAPE;
    }
    return true;
}

static void complete_line(edit_buffer* line) {
    if (line->cursor != line->length) {
        return;
    }
    size_t start = line->cursor;
    while (start != 0 && !isspace((unsigned char)line->text[start - 1])) {
        start--;
    }
    const char* match = ohlc_shell_complete(line->text + start, 0);
    if (match == NULL || ohlc_shell_complete(line->text + start, 1) != NULL) {
        return;
    }
    size_t length = strlen(match);
    if (start + length + 1 > OHLC_SHELL_COMMAND_BYTES) {
        return;
    }
    memcpy(line->text + start, match, length);
    line->length = start + length;
    line->text[line->length++] = ' ';
    line->text[line->length] = '\0';
    line->cursor = line->length;
}

static void edit_keypress(line_editor* editor, int key) {
    edit_buffer* line = &editor->line;
    bool killing = false;
    switch (key) {
    case 1:
    case OHLC_KEY_HOME:
        line->cursor = 0;
        break;
    case 5:
    case OHLC_KEY_END:
        line->cursor = line->length;
        break;
    case 2:
    case OHLC_KEY_LEFT:
        if (line->cursor > 0) {
            line->cursor--;
        }
        break;
    case 6:
    case OHLC_KEY_RIGHT:
        if (line->cursor < line->length) {
            line->cursor++;
        }
        break;
    case OHLC_KEY_WORD_LEFT:
        line->cursor = previous_word(line, false);
        break;
    case OHLC_KEY_WORD_RIGHT:
        line->cursor = next_word(line);
        break;
    case 8:
    case 127:
        if (line->cursor > 0) {
            delete_range(line, line->cursor - 1, line->cursor);
        }
        break;
    case 4:
    case OHLC_KEY_DELETE:
        if (line->cursor < line->length) {
            delete_range(line, line->cursor, line->cursor + 1);
        }
        break;
    case 11:
        kill_range(editor, line->cursor, line->length, false);
        killing = true;
        break;
    case 21:
        kill_range(editor, 0, line->cursor, true);
        killing = true;
        break;
    case 23:
    case OHLC_KEY_KILL_PREVIOUS_WORD:
        kill_range(editor, previous_word(line, key == 23), line->cursor, true);
        killing = true;
        break;
    case OHLC_KEY_KILL_WORD:
        kill_range(editor, line->cursor, next_word(line), false);
        killing = true;
        break;
    case 25:
        insert_text(line, killed_text, killed_length);
        break;
    case 16:
    case OHLC_KEY_UP:
        move_history(editor, true);
        break;
    case 14:
    case OHLC_KEY_DOWN:
        move_history(editor, false);
        break;
    case 12:
        fputs("\033[H\033[2J", stderr);
        break;
    case 20:
        if (line->cursor != 0 && line->length > 1) {
            size_t right = line->cursor < line->length ? line->cursor : line->length - 1;
            char left = line->text[right - 1];
            line->text[right - 1] = line->text[right];
            line->text[right] = left;
            line->cursor = right + 1;
        }
        break;
    case '\t':
        complete_line(line);
        break;
    default:
        if (key >= 32 && key <= 255 && key != 127) {
            char byte = (char)key;
            insert_text(line, &byte, 1);
        }
        break;
    }
    editor->last_kill = killing;
}

ohlc_status ohlc_shell_edit_line(const char* prompt, bool keep_history, char** output, bool* eof) {
    *output = NULL;
    *eof = false;
    if (keep_history) {
        load_history();
    }
    line_editor* editor = calloc(1, sizeof(*editor));
    if (editor == NULL) {
        return OHLC_LIMIT;
    }
    editor->keep_history = keep_history;
    editor->selected = history_count;
    struct termios original;
    if (tcgetattr(STDIN_FILENO, &original) != 0) {
        free(editor);
        return OHLC_IO;
    }
    struct termios raw = original;
    raw.c_lflag &= (tcflag_t) ~(ICANON | ECHO | ISIG | IEXTEN);
    raw.c_iflag &= (tcflag_t) ~(IXON | ICRNL);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;
    if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) != 0) {
        free(editor);
        return OHLC_IO;
    }
    ohlc_status status = OHLC_OK;
    redraw(editor, prompt);
    for (;;) {
        int key = read_key();
        if (key < 0) {
            status = OHLC_IO;
            break;
        }
        if (key == 3) {
            status = OHLC_CANCELLED;
            break;
        }
        if (search_key(editor, key)) {
            editor->last_kill = false;
            redraw(editor, prompt);
            continue;
        }
        edit_buffer* line = &editor->line;
        if (key == 4 && line->length == 0) {
            *eof = true;
            break;
        }
        if (key == '\r' || key == '\n') {
            redraw(editor, prompt);
            if (keep_history && strncasecmp(line->text, "connect", 7) != 0) {
                add_history(line->text);
            }
            line->text[line->length++] = '\n';
            line->text[line->length] = '\0';
            *output = strdup(line->text);
            if (*output == NULL) {
                status = OHLC_LIMIT;
            }
            break;
        }
        edit_keypress(editor, key);
        redraw(editor, prompt);
    }
    if (tcsetattr(STDIN_FILENO, TCSANOW, &original) != 0 && status == OHLC_OK) {
        free(*output);
        *output = NULL;
        status = OHLC_IO;
    }
    fputc('\n', stderr);
    free(editor);
    return status;
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "shell.h"

#include <ctype.h>
#include <inttypes.h>
#include <string.h>
#include <strings.h>
#include <sys/ioctl.h>
#include <unistd.h>

const shell_help ohlc_shell_commands[] = {
    {.name = "help",
     .group = SHELL_HELP_SESSION,
     .summary = "Show command help and examples.",
     .syntax = "help [command|examples|keys];",
     .description = "Display syntax, field types and examples; works offline.",
     .example = "help series;"},
    {.name = "connect",
     .group = SHELL_HELP_CONNECTION,
     .summary = "Connect to a server.",
     .syntax =
         "connect [--socket PATH | --host HOST --port PORT] [--tls --ca FILE --token-file FILE];",
     .description = "Connect and authenticate; credentials are read from a private file.",
     .example = "connect --socket /run/ohlc.sock;"},
    {.name = "disconnect",
     .group = SHELL_HELP_CONNECTION,
     .summary = "Close the current connection.",
     .syntax = "disconnect;",
     .description = "Close the connection and discard identifier caches.",
     .example = "disconnect;"},
    {.name = "status",
     .group = SHELL_HELP_CONNECTION,
     .summary = "Show connection details and last command status.",
     .syntax = "status;",
     .description = "Show negotiated limits, UUID, permissions and last command status.",
     .example = "status;"},
    {.name = "ping",
     .group = SHELL_HELP_CONNECTION,
     .summary = "Check the connection and round-trip time.",
     .syntax = "ping;",
     .description = "Verify the connection and report round-trip time.",
     .example = "ping;"},
    {.name = "stats",
     .group = SHELL_HELP_QUERIES,
     .summary = "Show engine counters and resource usage.",
     .syntax = "stats;",
     .description = "Show engine sequences, allocation and cumulative I/O counters.",
     .example = "stats;"},
    {.name = "checkpoint",
     .group = SHELL_HELP_WRITES,
     .summary = "Persist a committed database snapshot.",
     .syntax = "checkpoint;",
     .description = "Persist a committed snapshot; write access required.",
     .example = "checkpoint;"},
    {.name = "create",
     .group = SHELL_HELP_TABLES,
     .summary = "Create a table and define its bar period.",
     .syntax = "create TABLE --period Ns|Nm|Nd|Nmo|Ny [--timezone ZONE] [--description TEXT];",
     .description =
         "Create a durable fixed-schema table; write access required. Second/minute tables "
         "require a timezone; day/month/year tables use date labels without a timezone. "
         "Period metadata never rounds timestamps or generates bars.",
     .example = "create bars_3m --period 3m --timezone Asia/Shanghai;\n"
                "create bars_1d --period 1d;\n"
                "create bars_5s --period 5s --timezone Asia/Shanghai;\n"
                "create bars_1mo --period 1mo;\n"
                "create bars_1y --period 1y;"},
    {.name = "tables",
     .group = SHELL_HELP_TABLES,
     .summary = "List tables.",
     .syntax = "tables;",
     .description = "List tables in stable ID order using bounded pages.",
     .example = "tables;"},
    {.name = "drop",
     .group = SHELL_HELP_TABLES,
     .summary = "Permanently delete a table and its data.",
     .syntax = "drop TABLE;",
     .description = "Permanently delete a table and its rows; write access required.\n"
                    "Existing queries finish on their snapshots before files are reclaimed.\n"
                    "The name may be reused, but the new table receives a different ID.",
     .example = "drop bars_3m;"},
    {.name = "describe",
     .group = SHELL_HELP_TABLES,
     .summary = "Show table metadata and field definitions.",
     .syntax = "describe TABLE;",
     .description = "Show immutable table metadata and fixed field positions.",
     .example = "describe bars_3m;"},
    {.name = "tickers",
     .group = SHELL_HELP_TICKERS,
     .summary = "List the securities in a table.",
     .syntax = "tickers TABLE [PREFIX];",
     .description = "List exact ticker bytes, escaped for safe display.",
     .example = "tickers bars_3m AA;"},
    {.name = "resolve",
     .group = SHELL_HELP_TICKERS,
     .summary = "Look up a security in a table.",
     .syntax = "resolve TABLE TICKER;",
     .description = "Resolve an existing ticker without registering it.",
     .example = "resolve bars_3m \"AAPL\";"},
    {.name = "register",
     .group = SHELL_HELP_TICKERS,
     .summary = "Optionally reserve a table-local security code.",
     .syntax = "register TABLE TICKER;",
     .description = "Optionally reserve a table-local ticker code; write access required.",
     .example = "register bars_3m \"AAPL\";"},
    {.name = "series",
     .group = SHELL_HELP_QUERIES,
     .summary = "Query one security over an inclusive time range.",
     .syntax = "series TABLE TICKER from START and END [options];",
     .description = "Include both START and END; START must not exceed END.\n"
                    "The table and security must exist. Read access required.\n"
                    "The old 'to' syntax is not accepted.",
     .example = "# Minute bars (3m)\n"
                "series bars_3m AAPL from \"20260901 09:30:00\" and \"20260901 16:00:00\" --all;\n"
                "\n"
                "# Daily bars (1d), including both dates\n"
                "series bars_1d AAPL from \"20260901\" and \"20260930\" --all;\n"
                "\n"
                "# Second bars (5s); endpoints remain exact seconds\n"
                "series bars_5s AAPL from \"20260901 09:30:17\" and \"20260901 09:30:22\";\n"
                "\n"
                "# Month/year bars preserve caller-supplied date labels\n"
                "series bars_1mo AAPL from \"20260101\" and \"20261231\";\n"
                "series bars_1y AAPL from \"20200101\" and \"20261231\";"},
    {.name = "cross",
     .group = SHELL_HELP_QUERIES,
     .summary = "Query one time, optionally for a security set.",
     .syntax = "cross TABLE TIME [and ticker in (TICKER, ...)] [options];",
     .description = "Results are ordered by table-local security code. Read access required.\n"
                    "Without a ticker set, query all securities at TIME.\n"
                    "The server filters the set in one snapshot. Unknown securities and missing "
                    "bars are omitted; duplicates appear once. An empty set returns no rows.",
     .example = "# Minute bars (3m)\n"
                "cross bars_3m \"20260901 09:30:00\";\n"
                "cross bars_3m \"20260901 09:30:00\" and ticker in ('AAPL', 'GOOGL', 'INTL');\n"
                "\n"
                "# Daily bars (1d)\n"
                "cross bars_1d \"20260901\";\n"
                "cross bars_1d \"20260901\" and ticker in ('AAPL', 'GOOGL', 'INTL');\n"
                "\n"
                "# Export daily bars to a local file\n"
                "cross bars_1d \"20260901\" --format csv --output \"daily.csv\";"},
    {.name = "insert",
     .group = SHELL_HELP_WRITES,
     .summary = "Insert or replace one complete bar.",
     .syntax = "insert TABLE TICKER TIME OPEN HIGH LOW CLOSE VOLUME AMOUNT ADJUST_FACTOR;",
     .description = "Atomically insert/replace all seven fields. Table must exist; missing tickers "
                    "are created "
                    "atomically; write access "
                    "required.",
     .example =
         "# Second bars (5s)\n"
         "insert bars_5s AAPL \"20260901 09:30:17\" 10000 10100 9950 10080 1200 12100000 1000000;\n"
         "\n"
         "# Minute bars (3m)\n"
         "insert bars_3m AAPL \"20260901 09:30:00\" 10000 10100 9950 10080 1200 12100000 1000000;\n"
         "\n"
         "# Daily bars (1d)\n"
         "insert bars_1d AAPL \"20260901\" 10000 10100 9950 10080 1200 12100000 1000000;\n"
         "\n"
         "# Monthly bars (1mo); caller-supplied date\n"
         "insert bars_1mo AAPL \"20260917\" 10000 10100 9950 10080 1200 12100000 1000000;\n"
         "\n"
         "# Yearly bars (1y); caller-supplied date\n"
         "insert bars_1y AAPL \"20260917\" 10000 10100 9950 10080 1200 12100000 1000000;"},
    {.name = "put",
     .group = SHELL_HELP_WRITES,
     .summary = "Alias of insert.",
     .syntax = "put TABLE TICKER TIME OPEN HIGH LOW CLOSE VOLUME AMOUNT ADJUST_FACTOR;",
     .description = "Alias of insert.",
     .example = "help insert;"},
    {.name = "import",
     .group = SHELL_HELP_WRITES,
     .summary = "Import bars from a local TSV or CSV file.",
     .syntax = "import TABLE PATH [--format tsv|csv] [--batch-rows N] [--no-header];",
     .description = "Read a local text file with a matching nine-column header.\n"
                    "Default: TSV, 20000 rows per batch. Write access required.\n"
                    "Missing securities are created atomically with each batch.\n"
                    "Stop on the first error; earlier committed batches remain durable.",
     .example = "import bars_3m bars.tsv --format tsv --batch-rows 20000;"},
    {.name = "source",
     .group = SHELL_HELP_SESSION,
     .summary = "Run commands from a local script.",
     .syntax = "source PATH;",
     .description = "Run a local script; stop that script on its first failure.",
     .example = "source queries.ohlc;"},
    {.name = "set",
     .group = SHELL_HELP_SESSION,
     .summary = "Change display, preview and history settings.",
     .syntax =
         "set format table|tsv|csv|jsonl;\nset preview N;\nset timing on|off;\nset history on|off;",
     .description = "Set local display options. History is private and never stores credentials.",
     .example = "set preview 100;"},
    {.name = "quit",
     .group = SHELL_HELP_SESSION,
     .summary = "Exit the shell.",
     .syntax = "quit;",
     .description = "Exit. An unknown mutation outcome keeps exit status 4.",
     .example = "quit;"},
    {.name = "exit",
     .group = SHELL_HELP_SESSION,
     .summary = "Alias of quit.",
     .syntax = "exit;",
     .description = "Alias of quit.",
     .example = "exit;"}};
const size_t ohlc_shell_command_count =
    sizeof(ohlc_shell_commands) / sizeof(ohlc_shell_commands[0]);

static const char* const help_groups[] = {"Tables", "Queries",    "Securities",
                                          "Writes", "Connection", "Shell"};

static size_t help_width(void) {
    struct winsize window = {0};
    if (isatty(STDOUT_FILENO) && ioctl(STDOUT_FILENO, TIOCGWINSZ, &window) == 0 &&
        window.ws_col != 0 && window.ws_col < 80) {
        return window.ws_col;
    }
    return 80;
}

static void help_indent(size_t count) {
    for (size_t i = 0; i < count; i++) {
        fputc(' ', stdout);
    }
}

static size_t help_token_length(const char* text, bool command) {
    size_t length = 0;
    char quote = 0;
    bool escaped = false;
    while (text[length] != '\0') {
        char c = text[length];
        if (!escaped && quote == 0 && isspace((unsigned char)c)) {
            break;
        }
        if (command) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (quote != 0 && c == quote) {
                quote = 0;
            } else if (quote == 0 && (c == '\'' || c == '"')) {
                quote = c;
            }
        }
        length++;
    }
    return length;
}

/* The caller has already printed the first-line prefix. Preserve explicit
 * paragraphs and keep quoted command arguments intact when wrapping examples. */
static void help_wrap(const char* text, size_t width, size_t column, size_t continuation,
                      bool command) {
    bool word_on_line = false;
    bool needs_indent = false;
    bool ended_line = false;
    bool comment_line = command && *text == '#';
    while (*text != '\0') {
        if (*text == '\n') {
            fputc('\n', stdout);
            text++;
            column = continuation;
            needs_indent = true;
            word_on_line = false;
            ended_line = true;
            comment_line = command && *text == '#';
            continue;
        }
        if (isspace((unsigned char)*text)) {
            text++;
            continue;
        }
        size_t length = help_token_length(text, command);
        if (word_on_line && column + 1 + length > width) {
            fputc('\n', stdout);
            column = continuation;
            needs_indent = true;
            word_on_line = false;
            if (comment_line) {
                help_indent(continuation);
                fputs("# ", stdout);
                column += 2;
                needs_indent = false;
            }
        }
        if (needs_indent) {
            help_indent(continuation);
            needs_indent = false;
        }
        if (word_on_line) {
            fputc(' ', stdout);
            column++;
        }
        fwrite(text, 1, length, stdout);
        column += length;
        text += length;
        word_on_line = true;
        ended_line = false;
    }
    if (!ended_line) {
        fputc('\n', stdout);
    }
}

static void help_text(const char* text, size_t width, bool command) {
    help_indent(2);
    help_wrap(text, width, 2, 2, command);
}

static void help_pair(const char* label, const char* description, size_t label_width,
                      size_t width) {
    size_t length = strlen(label);
    help_indent(2);
    fputs(label, stdout);
    if (width < 56 || length > label_width) {
        fputc('\n', stdout);
        help_indent(4);
        help_wrap(description, width, 4, 4, false);
        return;
    }
    help_indent(label_width - length + 2);
    size_t column = label_width + 4;
    help_wrap(description, width, column, column, false);
}

static void help_settings(const shell_help_settings* settings, size_t width) {
    puts("\nCurrent settings");
    char preview[64];
    snprintf(preview, sizeof(preview), "%" PRIu64 " rows", settings->preview);
    help_pair("format", settings->format, 12, width);
    help_pair("preview", preview, 12, width);
    help_pair("history", settings->history ? "on" : "off", 12, width);
    help_pair("timing", settings->timing ? "on" : "off", 12, width);
}

static void help_overview(size_t width) {
    puts("OHLC commands");
    help_text("Use help COMMAND; for details, help examples; for examples, or help keys; "
              "for keyboard shortcuts.",
              width, false);
    for (size_t group = 0; group < SHELL_HELP_GROUP_COUNT; group++) {
        printf("\n%s\n", help_groups[group]);
        for (size_t i = 0; i < ohlc_shell_command_count; i++) {
            const shell_help* entry = &ohlc_shell_commands[i];
            if ((size_t)entry->group == group) {
                help_pair(entry->name, entry->summary, 12, width);
            }
        }
    }
}

static void help_keys(size_t width) {
    puts("OHLC keyboard shortcuts");
    help_text("Available at the interactive prompt. Alt can also be entered as Esc then a key.",
              width, false);
    puts("\nMove");
    help_pair("Ctrl+A / Home", "Move to the beginning of the line.", 22, width);
    help_pair("Ctrl+E / End", "Move to the end of the line.", 22, width);
    help_pair("Ctrl+B/F / Left/Right", "Move one character backward/forward.", 22, width);
    help_pair("Alt+B/F", "Move one word backward/forward; Ctrl+Left/Right also work.", 22, width);
    puts("\nEdit");
    help_pair("Ctrl+K", "Cut from the cursor to the end of the line.", 22, width);
    help_pair("Ctrl+U", "Cut from the beginning of the line to the cursor.", 22, width);
    help_pair("Ctrl+W", "Cut the previous whitespace-delimited word.", 22, width);
    help_pair("Alt+D / Alt+Backspace", "Cut the next/previous word.", 22, width);
    help_pair("Ctrl+Y", "Paste the last cut text. Consecutive cuts accumulate.", 22, width);
    help_pair("Backspace / Ctrl+H", "Delete the character before the cursor.", 22, width);
    help_pair("Ctrl+D / Delete", "Delete at the cursor; Ctrl+D on an empty line exits.", 22, width);
    help_pair("Ctrl+T", "Swap the two characters at the cursor (or the last two at line end).", 22,
              width);
    help_pair("Ctrl+L", "Clear the screen and redraw the current input.", 22, width);
    help_pair("Ctrl+C", "Discard the current command, including unfinished lines.", 22, width);
    help_pair("Tab", "Complete a unique command, option or cached name.", 22, width);
    puts("\nHistory");
    help_pair("Ctrl+P/N / Up/Down",
              "Previous/next history line; moving past the newest restores "
              "your unfinished input and cursor.",
              22, width);
    help_pair("Ctrl+R",
              "Start reverse history search; type to refine, press Ctrl+R again for "
              "an older match, or Backspace to shorten the search.",
              22, width);
    help_pair("Enter in search", "Accept and submit the displayed match.", 22, width);
    help_pair("Esc in search", "Accept the displayed match for editing without submitting it.", 22,
              width);
    help_pair("Ctrl+G in search", "Cancel search and restore the original input and cursor.", 22,
              width);
    help_text("A failed search keeps the last match. Editing keys accept that match before "
              "editing it. History is unavailable while set history off is active.",
              width, false);
}

static void help_query_options(size_t width) {
    puts("\nOptions");
    help_pair("--format FORMAT", "table, tsv, csv or jsonl.", 18, width);
    help_pair("--all", "Read all rows; disable the terminal preview limit.", 18, width);
    help_pair("--output PATH", "Write all rows to a local file.", 18, width);
    help_pair("--overwrite", "Allow replacing an existing output file.", 18, width);
    puts("\nDefaults");
    help_text("An interactive terminal previews 100 rows in table format. Scripts use TSV. "
              "Files and --all read the complete result. Use set to change session defaults.",
              width, false);
}

static void help_times(size_t width) {
    puts("\nTime values");
    help_pair("Second/minute",
              "Quoted date/time, interpreted in the table's time zone unless "
              "an explicit offset is supplied.",
              16, width);
    help_pair("Day/month/year",
              "A full date such as \"20260917\". Month/year dates are not rounded "
              "or restricted to period boundaries.",
              16, width);
    help_pair("Debug keys",
              "@KEY is uint32: Unix seconds for s, Unix minutes for m, and days "
              "since 1970-01-01 for d/mo/y. It is not an internal time code.",
              16, width);
    help_text("Second keys cover 1970-01-01T00:00:00Z to 2106-02-07T06:28:15Z. "
              "Minute inputs require zero seconds. No fractional or leap seconds.",
              width, false);
    help_text("No rounding or aggregation. Names support quotes, backslash and \\xHH escapes.",
              width, false);
}

static void help_fields(bool importing, size_t width) {
    puts(importing ? "\nColumns (in order)" : "\nFields (in order)");
    if (importing) {
        help_text("ticker, time, open, high, low, close, volume, amount, adjust_factor", width,
                  false);
    } else {
        help_text("open, high, low, close, volume, amount, adjust_factor", width, false);
    }
    puts("\nInteger types");
    help_pair("Prices", "open, high, low, close: int32.", 12, width);
    help_pair("Volume", "uint32.", 12, width);
    help_pair("Amount", "uint64.", 12, width);
    help_pair("Factor", "adjust_factor: uint32.", 12, width);
}

static void help_detail(const shell_help* entry, size_t width) {
    puts(entry->name);
    help_text(entry->summary, width, false);
    puts("\nUsage");
    help_text(entry->syntax, width, true);
    bool series = strcmp(entry->name, "series") == 0;
    bool cross = strcmp(entry->name, "cross") == 0;
    bool inserting = strcmp(entry->name, "insert") == 0 || strcmp(entry->name, "put") == 0;
    bool importing = strcmp(entry->name, "import") == 0;
    if (strcmp(entry->name, "create") == 0) {
        puts("\nPeriods (N is a positive uint32 integer)");
        help_pair("Ns", "Second bars; time keys count Unix seconds.", 12, width);
        help_pair("Nm", "Minute bars; time keys count Unix minutes.", 12, width);
        help_pair("Nd", "Day bars; time keys count days since 1970-01-01.", 12, width);
        help_pair("Nmo", "Month bars; time keys retain the full date, in epoch days.", 12, width);
        help_pair("Ny", "Year bars; time keys retain the full date, in epoch days.", 12, width);
    }
    if (series || cross) {
        puts("\nArguments");
        help_pair("TABLE", "An existing table.", 12, width);
        if (series) {
            help_pair("TICKER", "A security in that table, for example AAPL.", 12, width);
            help_pair("START / END", "Start and end date/time or date; both are included.", 12,
                      width);
        } else {
            help_pair("TIME", "The date/time or date to query.", 12, width);
            help_pair("ticker in", "Optional comma-separated security names in parentheses.", 12,
                      width);
        }
        help_query_options(width);
    }
    if (importing) {
        puts("\nOptions");
        help_pair("--format FORMAT", "tsv (default) or csv.", 18, width);
        help_pair("--batch-rows N", "Rows per atomic batch; default 20000.", 18, width);
        help_pair("--no-header", "Treat the first input line as a data row.", 18, width);
    }
    puts("\nNotes");
    help_text(entry->description, width, false);
    if (entry->group == SHELL_HELP_WRITES) {
        help_text("Uncertain writes are never automatically retried.", width, false);
    }
    if (inserting || importing) {
        help_fields(importing, width);
    }
    if (series || cross || inserting || importing) {
        help_times(width);
    }
    puts("\nExamples");
    help_text(entry->example, width, true);
}

static bool help_topic(const shell_word* word, const char* name) {
    return word->size == strlen(name) && strncasecmp(word->text, name, word->size) == 0;
}

ohlc_status ohlc_shell_show_help(const shell_command* command,
                                 const shell_help_settings* settings) {
    if (command->count > 2) {
        return OHLC_INVALID;
    }
    size_t width = help_width();
    if (command->count == 1) {
        help_overview(width);
    } else if (help_topic(&command->words[1], "keys")) {
        help_keys(width);
    } else if (help_topic(&command->words[1], "examples")) {
        puts("OHLC examples");
        help_text("These commands are examples only. Tables must exist before querying or writing. "
                  "Create, drop, register, insert, import and checkpoint require write access.",
                  width, false);
        for (size_t group = 0; group < SHELL_HELP_GROUP_COUNT; group++) {
            printf("\n%s\n", help_groups[group]);
            for (size_t i = 0; i < ohlc_shell_command_count; i++) {
                const shell_help* entry = &ohlc_shell_commands[i];
                if ((size_t)entry->group == group) {
                    char caption[128];
                    snprintf(caption, sizeof(caption), "# %s", entry->summary);
                    fputc('\n', stdout);
                    help_text(caption, width, true);
                    help_text(entry->example, width, true);
                }
            }
        }
    } else {
        const shell_help* entry = NULL;
        for (size_t i = 0; i < ohlc_shell_command_count; i++) {
            if (help_topic(&command->words[1], ohlc_shell_commands[i].name)) {
                entry = &ohlc_shell_commands[i];
                break;
            }
        }
        if (entry == NULL) {
            fputs("Unknown help topic. Use help;\n", stderr);
            return OHLC_INVALID;
        }
        help_detail(entry, width);
    }
    help_settings(settings, width);
    return OHLC_OK;
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define OHLC_ZONE_TRANSITIONS 8192u
#define OHLC_ZONE_TYPES 256u
#define OHLC_ZONE_CACHE 8u

typedef struct {
    char kind;
    int day;
    int month;
    int week;
    int weekday;
    int seconds;
    char clock;
} ohlc_zone_rule;

typedef struct {
    char name[256];
    uint32_t transition_count;
    uint32_t type_count;
    int64_t transitions[OHLC_ZONE_TRANSITIONS];
    uint8_t types[OHLC_ZONE_TRANSITIONS];
    int32_t offsets[OHLC_ZONE_TYPES];
    bool footer;
    bool daylight;
    int32_t standard_offset;
    int32_t daylight_offset;
    ohlc_zone_rule start;
    ohlc_zone_rule end;
} ohlc_zone;

/* The bounded cache is independent of database lifetime. All access, including
 * replacement, is protected by this mutex. Parsing never changes process TZ. */
static pthread_mutex_t zone_mutex = PTHREAD_MUTEX_INITIALIZER;
static ohlc_zone zones[OHLC_ZONE_CACHE];
static size_t next_zone;

static bool leap_year(int64_t year) {
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

static int month_days(int64_t year, int month) {
    static const uint8_t days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return days[month - 1] + (month == 2 && leap_year(year) ? 1 : 0);
}

static int64_t days_before_year(int64_t year) {
    int64_t previous = year - 1;
    return (year - 1970) * 365 + previous / 4 - previous / 100 + previous / 400 - 477;
}

static int64_t civil_days(int64_t year, int month, int day) {
    int64_t days = days_before_year(year) + day - 1;
    for (int i = 1; i < month; i++) {
        days += month_days(year, i);
    }
    return days;
}

static void civil_date(int64_t days, int64_t* year, int* month, int* day) {
    int64_t low = 1;
    int64_t high = 12000000;
    while (low + 1 < high) {
        int64_t middle = low + (high - low) / 2;
        if (days_before_year(middle) <= days) {
            low = middle;
        } else {
            high = middle;
        }
    }
    *year = low;
    days -= days_before_year(low);
    *month = 1;
    while (*month < 12 && days >= month_days(low, *month)) {
        days -= month_days(low, *month);
        (*month)++;
    }
    *day = (int)days + 1;
}

static bool parse_digits(const char** input, unsigned int minimum, unsigned int maximum,
                         int* output) {
    const char* p = *input;
    int value = 0;
    unsigned int count = 0;
    while (*p >= '0' && *p <= '9' && count < maximum) {
        value = value * 10 + *p++ - '0';
        count++;
    }
    if (count < minimum) {
        return false;
    }
    *input = p;
    *output = value;
    return true;
}

static bool consume(const char** input, char character) {
    if (**input != character) {
        return false;
    }
    (*input)++;
    return true;
}

static bool abbreviation(const char** input) {
    const char* start = *input;
    if (consume(input, '<')) {
        size_t length = 0;
        while (**input != '\0' && **input != '>') {
            unsigned char c = (unsigned char)**input;
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                  c == '+' || c == '-')) {
                return false;
            }
            length++;
            (*input)++;
        }
        return length >= 3 && consume(input, '>');
    }
    while ((**input >= 'a' && **input <= 'z') || (**input >= 'A' && **input <= 'Z')) {
        (*input)++;
    }
    return *input - start >= 3;
}

static bool posix_seconds(const char** input, int maximum_hour, int* seconds) {
    int sign = 1;
    if (consume(input, '-')) {
        sign = -1;
    } else {
        (void)consume(input, '+');
    }
    int hours = 0;
    int minutes = 0;
    int remainder = 0;
    if (!parse_digits(input, 1, 3, &hours) || hours > maximum_hour) {
        return false;
    }
    if (consume(input, ':')) {
        if (!parse_digits(input, 1, 2, &minutes) || minutes > 59) {
            return false;
        }
        if (consume(input, ':') && (!parse_digits(input, 1, 2, &remainder) || remainder > 59)) {
            return false;
        }
    }
    *seconds = sign * (hours * 3600 + minutes * 60 + remainder);
    return true;
}

static bool parse_rule(const char** input, ohlc_zone_rule* rule) {
    memset(rule, 0, sizeof(*rule));
    rule->seconds = 7200;
    rule->clock = 'w';
    if (consume(input, 'M')) {
        rule->kind = 'M';
        if (!parse_digits(input, 1, 2, &rule->month) || !consume(input, '.') ||
            !parse_digits(input, 1, 1, &rule->week) || !consume(input, '.') ||
            !parse_digits(input, 1, 1, &rule->weekday) || rule->month < 1 || rule->month > 12 ||
            rule->week < 1 || rule->week > 5 || rule->weekday > 6) {
            return false;
        }
    } else {
        rule->kind = consume(input, 'J') ? 'J' : 'N';
        if (!parse_digits(input, 1, 3, &rule->day) || rule->day > 365 ||
            (rule->kind == 'J' && rule->day == 0)) {
            return false;
        }
    }
    if (consume(input, '/')) {
        if (!posix_seconds(input, 167, &rule->seconds)) {
            return false;
        }
        if (**input == 'w' || **input == 's' || **input == 'u' || **input == 'g' ||
            **input == 'z') {
            rule->clock = **input;
            (*input)++;
        }
    }
    return true;
}

static bool parse_footer(ohlc_zone* zone, const char* text) {
    if (*text == '\0') {
        return true;
    }
    const char* p = text;
    int offset = 0;
    if (!abbreviation(&p) || !posix_seconds(&p, 24, &offset)) {
        return false;
    }
    zone->standard_offset = -offset;
    zone->footer = true;
    if (*p == '\0') {
        return true;
    }
    if (!abbreviation(&p)) {
        return false;
    }
    zone->daylight = true;
    zone->daylight_offset = zone->standard_offset + 3600;
    if (*p != ',') {
        if (!posix_seconds(&p, 24, &offset)) {
            return false;
        }
        zone->daylight_offset = -offset;
    }
    return consume(&p, ',') && parse_rule(&p, &zone->start) && consume(&p, ',') &&
           parse_rule(&p, &zone->end) && *p == '\0';
}

static uint32_t big_u32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

static int64_t big_signed(const uint8_t* p, size_t width) {
    uint64_t value = 0;
    for (size_t i = 0; i < width; i++) {
        value = (value << 8) | p[i];
    }
    if (width == 4 && (value & UINT64_C(0x80000000)) != 0) {
        value |= UINT64_C(0xffffffff00000000);
    }
    int64_t result;
    memcpy(&result, &value, sizeof(result));
    return result;
}

static bool tzif_block_size(const uint8_t* header, size_t width, size_t* size) {
    uint64_t transitions = big_u32(header + 32);
    uint64_t types = big_u32(header + 36);
    uint64_t length = transitions * (width + 1u) + types * 6u + big_u32(header + 40) +
                      (uint64_t)big_u32(header + 28) * (width + 4u) + big_u32(header + 24) +
                      big_u32(header + 20);
    if (memcmp(header, "TZif", 4) != 0 || length > 1024u * 1024u || types == 0 ||
        types > OHLC_ZONE_TYPES || transitions > OHLC_ZONE_TRANSITIONS) {
        return false;
    }
    *size = (size_t)length;
    return true;
}

static ohlc_status zone_load(const char* name, ohlc_zone* zone) {
    size_t length = strnlen(name, 256);
    if (length == 0 || length > 255 || name[0] == '/' || strstr(name, "..") != NULL) {
        return OHLC_INVALID;
    }
    for (size_t i = 0; i < length; i++) {
        unsigned char c = (unsigned char)name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              c == '_' || c == '/' || c == '-' || c == '+')) {
            return OHLC_INVALID;
        }
    }
    char path[288];
    snprintf(path, sizeof(path), "/usr/share/zoneinfo/%s", name);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return errno == ENOENT ? OHLC_NOT_FOUND : OHLC_IO;
    }
    struct stat info;
    if (fstat(fd, &info) != 0 || info.st_size < 44 || info.st_size > 1024 * 1024) {
        close(fd);
        return OHLC_UNSUPPORTED;
    }
    size_t size = (size_t)info.st_size;
    uint8_t* bytes = malloc(size + 1);
    if (bytes == NULL) {
        close(fd);
        return OHLC_LIMIT;
    }
    ohlc_status status = ohlc_read_full(fd, bytes, size, 0);
    close(fd);
    if (status != OHLC_OK) {
        free(bytes);
        return status;
    }
    bytes[size] = 0;
    memset(zone, 0, sizeof(*zone));
    const uint8_t* header = bytes;
    size_t block_size = 0;
    size_t width = 4;
    if (!tzif_block_size(header, width, &block_size) || block_size > size - 44) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    if (header[4] == '2' || header[4] == '3' || header[4] == '4') {
        size_t position = 44u + block_size;
        if (size - position < 44) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        header = bytes + position;
        width = 8;
        if (!tzif_block_size(header, width, &block_size) || block_size > size - position - 44) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
    } else if (header[4] != 0) {
        status = OHLC_UNSUPPORTED;
        goto cleanup;
    }
    if (big_u32(header + 28) != 0) {
        status = OHLC_UNSUPPORTED;
        goto cleanup;
    }
    zone->transition_count = big_u32(header + 32);
    zone->type_count = big_u32(header + 36);
    const uint8_t* data = header + 44;
    for (uint32_t i = 0; i < zone->transition_count; i++) {
        zone->transitions[i] = big_signed(data + (size_t)i * width, width);
        if (i != 0 && zone->transitions[i - 1] >= zone->transitions[i]) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
    }
    data += (size_t)zone->transition_count * width;
    for (uint32_t i = 0; i < zone->transition_count; i++) {
        zone->types[i] = data[i];
        if (data[i] >= zone->type_count) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
    }
    data += zone->transition_count;
    for (uint32_t i = 0; i < zone->type_count; i++) {
        zone->offsets[i] = (int32_t)big_signed(data + i * 6u, 4);
        if (zone->offsets[i] < -89999 || zone->offsets[i] > 93599 || data[i * 6u + 4] > 1 ||
            data[i * 6u + 5] >= big_u32(header + 40)) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
    }
    size_t footer_offset = (size_t)(header - bytes) + 44u + block_size;
    if (width == 8) {
        if (size - footer_offset < 2 || bytes[footer_offset] != '\n' || bytes[size - 1] != '\n') {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        bytes[size - 1] = 0;
        if (!parse_footer(zone, (const char*)bytes + footer_offset + 1)) {
            status = OHLC_UNSUPPORTED;
            goto cleanup;
        }
    }
    memcpy(zone->name, name, length + 1);
cleanup:
    free(bytes);
    return status;
}

static ohlc_status zone_cached_locked(const char* name, const ohlc_zone** output) {
    for (size_t i = 0; i < OHLC_ZONE_CACHE; i++) {
        if (strcmp(zones[i].name, name) == 0) {
            *output = &zones[i];
            return OHLC_OK;
        }
    }
    ohlc_zone* target = &zones[next_zone++ % OHLC_ZONE_CACHE];
    ohlc_status status = zone_load(name, target);
    if (status != OHLC_OK) {
        target->name[0] = '\0';
        return status;
    }
    *output = target;
    return OHLC_OK;
}

ohlc_status ohlc_timezone_validate(const char* name) {
    if (name == NULL || name[0] == '\0' || strnlen(name, 256) == 256) {
        return OHLC_INVALID;
    }
    if (strcmp(name, "UTC") == 0 || strcmp(name, "Etc/UTC") == 0) {
        return OHLC_OK;
    }
    pthread_mutex_lock(&zone_mutex);
    const ohlc_zone* zone = NULL;
    ohlc_status status = zone_cached_locked(name, &zone);
    pthread_mutex_unlock(&zone_mutex);
    return status;
}

static int64_t rule_time(int64_t year, const ohlc_zone_rule* rule, int32_t previous_offset,
                         int32_t standard_offset) {
    int64_t days = days_before_year(year);
    if (rule->kind == 'M') {
        int64_t first = civil_days(year, rule->month, 1);
        int weekday = (int)((first + 4) % 7);
        if (weekday < 0) {
            weekday += 7;
        }
        int day = 1 + (rule->weekday - weekday + 7) % 7 + (rule->week - 1) * 7;
        if (day > month_days(year, rule->month)) {
            day -= 7;
        }
        days = first + day - 1;
    } else if (rule->kind == 'J') {
        days += rule->day - 1 + (leap_year(year) && rule->day >= 60 ? 1 : 0);
    } else {
        days += rule->day;
    }
    int32_t offset = previous_offset;
    if (rule->clock == 's') {
        offset = standard_offset;
    } else if (rule->clock != 'w') {
        offset = 0;
    }
    return days * 86400 + rule->seconds - offset;
}

static ohlc_status zone_offset(const ohlc_zone* zone, int64_t utc, int32_t* offset) {
    uint32_t low = 0;
    uint32_t high = zone->transition_count;
    while (low < high) {
        uint32_t middle = low + (high - low) / 2;
        if (zone->transitions[middle] <= utc) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if (low < zone->transition_count || !zone->footer) {
        if (low == zone->transition_count && low != 0 && !zone->footer &&
            utc > zone->transitions[low - 1]) {
            return OHLC_UNSUPPORTED;
        }
        *offset = zone->offsets[low == 0 ? 0 : zone->types[low - 1]];
        return OHLC_OK;
    }
    *offset = zone->standard_offset;
    if (!zone->daylight) {
        return OHLC_OK;
    }
    int64_t year = 0;
    int month = 0;
    int day = 0;
    civil_date(utc / 86400, &year, &month, &day);
    int64_t latest = INT64_MIN;
    for (int delta = -1; delta <= 1; delta++) {
        int64_t start =
            rule_time(year + delta, &zone->start, zone->standard_offset, zone->standard_offset);
        int64_t end =
            rule_time(year + delta, &zone->end, zone->daylight_offset, zone->standard_offset);
        if (start <= utc && start > latest) {
            latest = start;
            *offset = zone->daylight_offset;
        }
        if (end <= utc && end > latest) {
            latest = end;
            *offset = zone->standard_offset;
        }
    }
    return OHLC_OK;
}

static ohlc_status local_to_utc(const char* name, int64_t local, int64_t* utc) {
    if (strcmp(name, "UTC") == 0 || strcmp(name, "Etc/UTC") == 0) {
        *utc = local;
        return OHLC_OK;
    }
    pthread_mutex_lock(&zone_mutex);
    const ohlc_zone* zone = NULL;
    ohlc_status status = zone_cached_locked(name, &zone);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    unsigned int matches = 0;
    int32_t tried[OHLC_ZONE_TYPES + 2];
    size_t tried_count = 0;
    uint32_t candidate_count = zone->type_count + (zone->footer ? 2u : 0u);
    bool unsupported = false;
    for (uint32_t i = 0; i < candidate_count; i++) {
        int32_t offset = i < zone->type_count ? zone->offsets[i]
                                              : (i == zone->type_count ? zone->standard_offset
                                                                       : zone->daylight_offset);
        bool duplicate = false;
        for (size_t j = 0; j < tried_count; j++) {
            if (tried[j] == offset) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) {
            continue;
        }
        tried[tried_count++] = offset;
        int64_t candidate = local - offset;
        int32_t actual = 0;
        status = zone_offset(zone, candidate, &actual);
        if (status == OHLC_UNSUPPORTED) {
            unsupported = true;
        } else if (status == OHLC_OK && actual == offset) {
            *utc = candidate;
            matches++;
        }
    }
    status =
        matches == 1 ? OHLC_OK : (matches == 0 && unsupported ? OHLC_UNSUPPORTED : OHLC_INVALID);
cleanup:
    pthread_mutex_unlock(&zone_mutex);
    return status;
}

ohlc_status ohlc_time_parse(const ohlc_table_info* table, const char* input, uint32_t* time_key) {
    if (table == NULL || input == NULL || time_key == NULL || strnlen(input, 128) == 128 ||
        strnlen(table->timezone, sizeof(table->timezone)) == sizeof(table->timezone) ||
        (table->period_unit != OHLC_MINUTE && table->period_unit != OHLC_DAY)) {
        return OHLC_INVALID;
    }
    const char* p = input;
    if (consume(&p, '@')) {
        uint64_t value = 0;
        if (*p == '\0') {
            return OHLC_INVALID;
        }
        while (*p >= '0' && *p <= '9') {
            value = value * 10u + (unsigned int)(*p++ - '0');
            if (value > UINT32_MAX) {
                return OHLC_INVALID;
            }
        }
        if (*p != '\0') {
            return OHLC_INVALID;
        }
        *time_key = (uint32_t)value;
        return OHLC_OK;
    }
    int year = 0;
    int month = 0;
    int day = 0;
    size_t digits = 0;
    while (p[digits] >= '0' && p[digits] <= '9') {
        digits++;
    }
    bool dashed = p[digits] == '-';
    if (!parse_digits(&p, 4, dashed ? 8u : 4u, &year) || (dashed && !consume(&p, '-')) ||
        !parse_digits(&p, 2, 2, &month) || (dashed && !consume(&p, '-')) ||
        !parse_digits(&p, 2, 2, &day) || year < 1 || month < 1 || month > 12 || day < 1 ||
        day > month_days(year, month)) {
        return OHLC_INVALID;
    }
    int64_t days = civil_days(year, month, day);
    if (table->period_unit == OHLC_DAY) {
        if (*p != '\0' || days < 0 || days > UINT32_MAX) {
            return OHLC_INVALID;
        }
        *time_key = (uint32_t)days;
        return OHLC_OK;
    }
    int hour = 0;
    int minute = 0;
    int second = 0;
    if ((!consume(&p, ' ') && !consume(&p, 'T')) || !parse_digits(&p, 2, 2, &hour) ||
        !consume(&p, ':') || !parse_digits(&p, 2, 2, &minute) || !consume(&p, ':') ||
        !parse_digits(&p, 2, 2, &second) || hour > 23 || minute > 59 || second != 0) {
        return OHLC_INVALID;
    }
    int64_t utc = days * 86400 + hour * 3600 + minute * 60;
    ohlc_status status = OHLC_OK;
    if (consume(&p, 'Z')) {
        if (*p != '\0') {
            return OHLC_INVALID;
        }
    } else if (*p == '+' || *p == '-') {
        int sign = *p++ == '+' ? 1 : -1;
        int offset_hour = 0;
        int offset_minute = 0;
        if (!parse_digits(&p, 2, 2, &offset_hour) || !consume(&p, ':') ||
            !parse_digits(&p, 2, 2, &offset_minute) || *p != '\0' || offset_hour > 23 ||
            offset_minute > 59) {
            return OHLC_INVALID;
        }
        utc -= sign * (offset_hour * 3600 + offset_minute * 60);
    } else if (*p == '\0') {
        status = local_to_utc(table->timezone, utc, &utc);
    } else {
        return OHLC_INVALID;
    }
    if (status != OHLC_OK) {
        return status;
    }
    if (utc < 0 || utc % 60 != 0 || utc / 60 > UINT32_MAX) {
        return OHLC_INVALID;
    }
    *time_key = (uint32_t)(utc / 60);
    return OHLC_OK;
}

ohlc_status ohlc_time_format(const ohlc_table_info* table, uint32_t time_key, char* output,
                             size_t capacity) {
    if (table == NULL || output == NULL || capacity == 0 ||
        (table->period_unit != OHLC_MINUTE && table->period_unit != OHLC_DAY)) {
        return OHLC_INVALID;
    }
    int64_t year = 0;
    int month = 0;
    int day = 0;
    int64_t days = table->period_unit == OHLC_DAY ? time_key : time_key / 1440u;
    civil_date(days, &year, &month, &day);
    int length;
    if (table->period_unit == OHLC_DAY) {
        length = snprintf(output, capacity, "%04lld-%02d-%02d", (long long)year, month, day);
    } else {
        unsigned int hour = (time_key % 1440u) / 60u;
        unsigned int minute = time_key % 60u;
        length = snprintf(output, capacity, "%04lld-%02d-%02dT%02u:%02u:00Z", (long long)year,
                          month, day, hour, minute);
    }
    return length < 0 || (size_t)length >= capacity ? OHLC_LIMIT : OHLC_OK;
}

ohlc_status ohlc_time_format_local(const ohlc_table_info* table, uint32_t time_key, char* output,
                                   size_t capacity) {
    if (table == NULL || output == NULL || capacity == 0 ||
        strnlen(table->timezone, sizeof(table->timezone)) == sizeof(table->timezone)) {
        return OHLC_INVALID;
    }
    if (table->period_unit == OHLC_DAY || strcmp(table->timezone, "UTC") == 0 ||
        strcmp(table->timezone, "Etc/UTC") == 0) {
        return ohlc_time_format(table, time_key, output, capacity);
    }
    if (table->period_unit != OHLC_MINUTE) {
        return OHLC_INVALID;
    }
    pthread_mutex_lock(&zone_mutex);
    const ohlc_zone* zone = NULL;
    ohlc_status status = zone_cached_locked(table->timezone, &zone);
    int32_t offset = 0;
    if (status == OHLC_OK) {
        status = zone_offset(zone, (int64_t)time_key * 60, &offset);
    }
    pthread_mutex_unlock(&zone_mutex);
    if (status != OHLC_OK) {
        return status;
    }
    if (offset % 60 != 0) {
        /* Historical second-based offsets cannot round-trip through the
         * minute input grammar; preserve an exact UTC representation. */
        return ohlc_time_format(table, time_key, output, capacity);
    }
    int64_t seconds = (int64_t)time_key * 60 + offset;
    int64_t days = seconds / 86400;
    int64_t remainder = seconds % 86400;
    if (remainder < 0) {
        remainder += 86400;
        days--;
    }
    int64_t year;
    int month;
    int day;
    civil_date(days, &year, &month, &day);
    int64_t absolute_offset = offset < 0 ? -(int64_t)offset : offset;
    int length =
        snprintf(output, capacity, "%04lld-%02d-%02dT%02lld:%02lld:00%c%02lld:%02lld",
                 (long long)year, month, day, (long long)(remainder / 3600),
                 (long long)(remainder % 3600 / 60), offset < 0 ? '-' : '+',
                 (long long)(absolute_offset / 3600), (long long)(absolute_offset % 3600 / 60));
    return length < 0 || (size_t)length >= capacity ? OHLC_LIMIT : OHLC_OK;
}

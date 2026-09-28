/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#if defined(__x86_64__)
#include <nmmintrin.h>
#elif defined(__aarch64__)
#include <arm_acle.h>
#if defined(__linux__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
#endif

_Static_assert(CHAR_BIT == 8, "The file format requires eight-bit bytes");
_Static_assert(INT32_MIN == (-2147483647 - 1), "Two's complement int32_t is required");
_Static_assert(sizeof(ohlc_group) == 16, "Group entries must occupy sixteen bytes");
_Static_assert(sizeof(ohlc_time_node) == 4096, "Time nodes must occupy four KiB");
_Static_assert(sizeof(off_t) >= 8, "Large file offsets are required");

typedef union {
    max_align_t alignment;
    size_t size;
} ohlc_allocation;

static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;
static uint32_t (*crc_implementation)(uint32_t seed, const uint8_t* bytes, size_t size);

const char* ohlc_version(void) {
    return OHLC_VERSION;
}

uint32_t ohlc_abi_version(void) {
    return OHLC_ABI_VERSION;
}

uint16_t ohlc_get_u16(const uint8_t* p) {
    return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

uint32_t ohlc_get_u32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

uint64_t ohlc_get_u64(const uint8_t* p) {
    return (uint64_t)ohlc_get_u32(p) | ((uint64_t)ohlc_get_u32(p + 4) << 32);
}

void ohlc_put_u16(uint8_t* p, uint16_t value) {
    p[0] = (uint8_t)value;
    p[1] = (uint8_t)(value >> 8);
}

void ohlc_put_u32(uint8_t* p, uint32_t value) {
    for (unsigned int i = 0; i < 4; i++) {
        p[i] = (uint8_t)(value >> (i * 8));
    }
}

void ohlc_put_u64(uint8_t* p, uint64_t value) {
    ohlc_put_u32(p, (uint32_t)value);
    ohlc_put_u32(p + 4, (uint32_t)(value >> 32));
}

void ohlc_row_encode(uint8_t output[OHLC_ROW_BYTES], const ohlc_row* row) {
    ohlc_put_u32(output, (uint32_t)row->open);
    ohlc_put_u32(output + 4, (uint32_t)row->high);
    ohlc_put_u32(output + 8, (uint32_t)row->low);
    ohlc_put_u32(output + 12, (uint32_t)row->close);
    ohlc_put_u32(output + 16, row->volume);
    ohlc_put_u64(output + 20, row->amount);
    ohlc_put_u32(output + 28, row->adjust_factor);
}

static int32_t decode_signed(const uint8_t* p) {
    uint32_t bits = ohlc_get_u32(p);
    int32_t value;
    memcpy(&value, &bits, sizeof(value));
    return value;
}

void ohlc_row_decode(const uint8_t input[OHLC_ROW_BYTES], ohlc_row* row) {
    row->open = decode_signed(input);
    row->high = decode_signed(input + 4);
    row->low = decode_signed(input + 8);
    row->close = decode_signed(input + 12);
    row->volume = ohlc_get_u32(input + 16);
    row->amount = ohlc_get_u64(input + 20);
    row->adjust_factor = ohlc_get_u32(input + 28);
}

void ohlc_write_encode(uint8_t output[OHLC_WRITE_BYTES], uint32_t ticker_code, uint32_t time_key,
                       const ohlc_row* row) {
    ohlc_put_u32(output, ticker_code);
    ohlc_put_u32(output + 4, time_key);
    ohlc_row_encode(output + 8, row);
}

static uint32_t crc_software(uint32_t seed, const uint8_t* bytes, size_t size) {
    uint32_t crc = ~seed;
    for (size_t i = 0; i < size; i++) {
        crc = crc_table[(crc ^ bytes[i]) & 255u] ^ (crc >> 8);
    }
    return ~crc;
}

#if defined(__x86_64__)
__attribute__((target("sse4.2"))) static uint32_t
crc_accelerated(uint32_t seed, const uint8_t* bytes, size_t size) {
    uint32_t crc = ~seed;
    while (size >= 8) {
        crc = (uint32_t)_mm_crc32_u64(crc, ohlc_get_u64(bytes));
        bytes += 8;
        size -= 8;
    }
    while (size != 0) {
        crc = _mm_crc32_u8(crc, *bytes++);
        size--;
    }
    return ~crc;
}
#elif defined(__aarch64__)
#if defined(__clang__)
__attribute__((target("crc")))
#else
__attribute__((target("+crc")))
#endif
static uint32_t crc_accelerated(uint32_t seed, const uint8_t* bytes, size_t size) {
    uint32_t crc = ~seed;
    while (size >= 8) {
        crc = __crc32cd(crc, ohlc_get_u64(bytes));
        bytes += 8;
        size -= 8;
    }
    while (size != 0) {
        crc = __crc32cb(crc, *bytes++);
        size--;
    }
    return ~crc;
}
#endif

static void crc_initialize(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t value = i;
        for (unsigned int bit = 0; bit < 8; bit++) {
            value = (value >> 1) ^ ((value & 1u) != 0 ? UINT32_C(0x82f63b78) : 0u);
        }
        crc_table[i] = value;
    }
    crc_implementation = crc_software;
#ifdef OHLC_FORCE_SOFTWARE_CRC
    return;
#endif
#if defined(__x86_64__)
    __builtin_cpu_init();
    if (__builtin_cpu_supports("sse4.2")) {
        crc_implementation = crc_accelerated;
    }
#elif defined(__aarch64__) && defined(__linux__)
    if ((getauxval(AT_HWCAP) & HWCAP_CRC32) != 0) {
        crc_implementation = crc_accelerated;
    }
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    crc_implementation = crc_accelerated;
#endif
}

uint32_t ohlc_crc32c(uint32_t seed, const void* bytes, size_t size) {
    pthread_once(&crc_once, crc_initialize);
    return crc_implementation(seed, bytes, size);
}

unsigned int ohlc_popcount(uint16_t value) {
    return (unsigned int)__builtin_popcount((unsigned int)value);
}

uint64_t ohlc_monotonic_ms(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        return 0;
    }
    return (uint64_t)value.tv_sec * 1000u + (uint64_t)value.tv_nsec / 1000000u;
}

void* ohlc_alloc(ohlc_allocator* allocator, size_t size) {
    if (size > SIZE_MAX - sizeof(ohlc_allocation)) {
        return NULL;
    }
    size_t total = sizeof(ohlc_allocation) + size;
    size_t used = atomic_load_explicit(&allocator->used, memory_order_relaxed);
    do {
        if (total > allocator->limit || used > allocator->limit - total) {
            return NULL;
        }
    } while (!atomic_compare_exchange_weak_explicit(&allocator->used, &used, used + total,
                                                    memory_order_relaxed, memory_order_relaxed));
    ohlc_allocation* allocation = calloc(1, total);
    if (allocation == NULL) {
        atomic_fetch_sub_explicit(&allocator->used, total, memory_order_relaxed);
        return NULL;
    }
    allocation->size = total;
    return allocation + 1;
}

void ohlc_free(ohlc_allocator* allocator, void* pointer) {
    if (pointer != NULL) {
        ohlc_allocation* allocation = (ohlc_allocation*)pointer - 1;
        atomic_fetch_sub_explicit(&allocator->used, allocation->size, memory_order_relaxed);
        free(allocation);
    }
}

const char* ohlc_status_string(ohlc_status status) {
    static const char* const messages[] = {
        "OK",           "invalid argument", "not found",    "resource limit",
        "busy",         "I/O error",        "corrupt data", "unsupported",
        "unauthorized", "outcome unknown",  "cancelled",    "already exists"};
    if ((unsigned int)status >= sizeof(messages) / sizeof(messages[0])) {
        return "unknown status";
    }
    return messages[status];
}

static bool name_character(unsigned char c, bool first) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
           (!first && c >= '0' && c <= '9');
}

static bool valid_utf8(const char* text) {
    const unsigned char* p = (const unsigned char*)text;
    while (*p != 0) {
        uint32_t value = *p++;
        if (value < 128) {
            continue;
        }
        unsigned int remaining;
        uint32_t minimum;
        if (value >= 0xc2 && value <= 0xdf) {
            remaining = 1;
            minimum = 0x80;
            value &= 0x1f;
        } else if (value >= 0xe0 && value <= 0xef) {
            remaining = 2;
            minimum = 0x800;
            value &= 0x0f;
        } else if (value >= 0xf0 && value <= 0xf4) {
            remaining = 3;
            minimum = 0x10000;
            value &= 0x07;
        } else {
            return false;
        }
        for (unsigned int i = 0; i < remaining; i++) {
            if ((*p & 0xc0u) != 0x80u) {
                return false;
            }
            value = (value << 6) | (*p++ & 0x3fu);
        }
        if (value < minimum || value > 0x10ffff || (value >= 0xd800 && value <= 0xdfff)) {
            return false;
        }
    }
    return true;
}

const char* ohlc_period_suffix(ohlc_period_unit unit) {
    switch (unit) {
    case OHLC_SECOND:
        return "s";
    case OHLC_MINUTE:
        return "m";
    case OHLC_DAY:
        return "d";
    case OHLC_MONTH:
        return "mo";
    case OHLC_YEAR:
        return "y";
    default:
        return NULL;
    }
}

bool ohlc_period_is_date(ohlc_period_unit unit) {
    return unit == OHLC_DAY || unit == OHLC_MONTH || unit == OHLC_YEAR;
}

ohlc_status ohlc_period_parse(const char* text, ohlc_period_unit* unit, uint32_t* count) {
    if (text == NULL || unit == NULL || count == NULL) {
        return OHLC_INVALID;
    }
    const char* suffix = text;
    uint64_t value = 0;
    while (*suffix >= '0' && *suffix <= '9') {
        value = value * 10u + (unsigned int)(*suffix++ - '0');
        if (value > UINT32_MAX) {
            return OHLC_INVALID;
        }
    }
    if (value == 0) {
        return OHLC_INVALID;
    }
    static const ohlc_period_unit units[] = {OHLC_SECOND, OHLC_MINUTE, OHLC_DAY, OHLC_MONTH,
                                             OHLC_YEAR};
    for (size_t i = 0; i < sizeof(units) / sizeof(units[0]); i++) {
        if (strcmp(suffix, ohlc_period_suffix(units[i])) == 0) {
            *unit = units[i];
            *count = (uint32_t)value;
            return OHLC_OK;
        }
    }
    return OHLC_INVALID;
}

ohlc_status ohlc_definition_validate(const ohlc_table_definition* definition) {
    if (definition == NULL || definition->name == NULL || definition->timezone == NULL ||
        definition->description == NULL || definition->period_count == 0) {
        return OHLC_INVALID;
    }
    size_t length = strnlen(definition->name, 64);
    if (length == 0 || length > 63) {
        return OHLC_INVALID;
    }
    for (size_t i = 0; i < length; i++) {
        if (!name_character((unsigned char)definition->name[i], i == 0)) {
            return OHLC_INVALID;
        }
    }
    size_t zone_length = strnlen(definition->timezone, 256);
    if (zone_length > 255 || strnlen(definition->description, 4097) > 4096 ||
        !valid_utf8(definition->description)) {
        return OHLC_INVALID;
    }
    if (ohlc_period_suffix(definition->period_unit) == NULL ||
        (ohlc_period_is_date(definition->period_unit) ? zone_length != 0 : zone_length == 0)) {
        return OHLC_INVALID;
    }
    if (strstr(definition->timezone, "..") != NULL || definition->timezone[0] == '/') {
        return OHLC_INVALID;
    }
    for (size_t i = 0; i < zone_length; i++) {
        unsigned char c = (unsigned char)definition->timezone[i];
        if (!name_character(c, false) && c != '/' && c != '+' && c != '-') {
            return OHLC_INVALID;
        }
    }
    return OHLC_OK;
}

static size_t encode_string(uint8_t* output, const char* value) {
    size_t size = strlen(value);
    ohlc_put_u32(output, (uint32_t)size);
    memcpy(output + 4, value, size);
    return size + 4;
}

size_t ohlc_definition_encode(uint8_t* output, const ohlc_table_info* info) {
    size_t position = encode_string(output, info->name);
    ohlc_put_u32(output + position, (uint32_t)info->period_unit);
    ohlc_put_u32(output + position + 4, info->period_count);
    position += 8;
    position += encode_string(output + position, info->timezone);
    position += encode_string(output + position, info->description);
    return position;
}

static bool decode_string(const uint8_t* data, size_t size, size_t* position, char* output,
                          size_t capacity) {
    if (*position > size || size - *position < 4) {
        return false;
    }
    uint32_t length = ohlc_get_u32(data + *position);
    *position += 4;
    if (length >= capacity || length > size - *position ||
        memchr(data + *position, 0, length) != NULL) {
        return false;
    }
    memcpy(output, data + *position, length);
    output[length] = '\0';
    *position += length;
    return true;
}

ohlc_status ohlc_definition_decode(const uint8_t* data, size_t size, ohlc_table_info* info) {
    size_t position = 0;
    if (!decode_string(data, size, &position, info->name, sizeof(info->name)) ||
        size - position < 8) {
        return OHLC_CORRUPT;
    }
    info->period_unit = (ohlc_period_unit)ohlc_get_u32(data + position);
    info->period_count = ohlc_get_u32(data + position + 4);
    position += 8;
    if (!decode_string(data, size, &position, info->timezone, sizeof(info->timezone)) ||
        !decode_string(data, size, &position, info->description, sizeof(info->description)) ||
        position != size) {
        return OHLC_CORRUPT;
    }
    ohlc_table_definition definition = {info->name, info->period_unit, info->period_count,
                                        info->timezone, info->description};
    return ohlc_definition_validate(&definition) == OHLC_OK ? OHLC_OK : OHLC_CORRUPT;
}

ohlc_status ohlc_read_full(int fd, void* data, size_t size, uint64_t offset) {
    if (offset > INT64_MAX || size > (uint64_t)INT64_MAX - offset) {
        return OHLC_LIMIT;
    }
    uint8_t* p = data;
    while (size != 0) {
        size_t part = size > (size_t)SSIZE_MAX ? (size_t)SSIZE_MAX : size;
        ssize_t received = pread(fd, p, part, (off_t)offset);
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received <= 0) {
            return received == 0 ? OHLC_CORRUPT : OHLC_IO;
        }
        p += (size_t)received;
        size -= (size_t)received;
        offset += (uint64_t)received;
    }
    return OHLC_OK;
}

ohlc_status ohlc_write_full(int fd, const void* data, size_t size, uint64_t offset) {
    if (offset > INT64_MAX || size > (uint64_t)INT64_MAX - offset) {
        return OHLC_LIMIT;
    }
    const uint8_t* p = data;
    while (size != 0) {
        size_t part = size > (size_t)SSIZE_MAX ? (size_t)SSIZE_MAX : size;
        ssize_t written = pwrite(fd, p, part, (off_t)offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return OHLC_IO;
        }
        p += (size_t)written;
        size -= (size_t)written;
        offset += (uint64_t)written;
    }
    return OHLC_OK;
}

ohlc_status ohlc_sync(int fd) {
    int result;
    do {
#ifdef __APPLE__
        result = fsync(fd);
#else
        result = fdatasync(fd);
#endif
    } while (result != 0 && errno == EINTR);
    return result == 0 ? OHLC_OK : OHLC_IO;
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <assert.h>

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
_Static_assert(INT32_MIN == (-INT32_MAX - 1), "Two's complement int32_t is required");
_Static_assert(INT64_MIN == (-INT64_MAX - 1), "Two's complement int64_t is required");
_Static_assert(128u * OHLC_ROW_BYTES == OHLC_BLOCK_BYTES, "Tiles must contain 128 rows");
_Static_assert(sizeof(ohlc_group) == 16, "Group entries must occupy sixteen bytes");
_Static_assert(sizeof(ohlc_time_node) == 4096, "Time nodes must occupy four KiB");
_Static_assert(OHLC_MAX_BATCH_ROWS* OHLC_WRITE_BYTES + 104u <= OHLC_FRAME_LIMIT,
               "A maximum plain batch must fit in a frame");
_Static_assert(sizeof(off_t) >= 8, "Large file offsets are required");

typedef union {
    max_align_t alignment;
    struct {
        size_t size;
        uint64_t epoch;
    } allocation;
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

void ohlc_row_encode(uint8_t output[OHLC_ROW_BYTES], const ohlc_row* row) {
    ohlc_put_u64(output, (row->flags & (1u << 0)) != 0 ? (uint64_t)row->open : 0);
    ohlc_put_u64(output + 8, (row->flags & (1u << 1)) != 0 ? (uint64_t)row->high : 0);
    ohlc_put_u64(output + 16, (row->flags & (1u << 2)) != 0 ? (uint64_t)row->low : 0);
    ohlc_put_u64(output + 24, (row->flags & (1u << 3)) != 0 ? (uint64_t)row->close : 0);
    ohlc_put_u64(output + 32, (row->flags & (1u << 4)) != 0 ? row->volume : 0);
    ohlc_put_u64(output + 40, (row->flags & (1u << 5)) != 0 ? row->amount : 0);
    ohlc_put_u64(output + 48, (row->flags & (1u << 6)) != 0 ? row->adjust_factor : 0);
    ohlc_put_u32(output + 56,
                 (row->flags & OHLC_FIELD_RESERVED) != 0 ? (uint32_t)row->reserved : 0);
    ohlc_put_u32(output + 60, row->flags & OHLC_FIELDS_ALL);
}

static int64_t decode_signed(const uint8_t* p) {
    uint64_t bits = ohlc_get_u64(p);
    int64_t value;
    memcpy(&value, &bits, sizeof(value));
    return value;
}

void ohlc_row_decode(const uint8_t input[OHLC_ROW_BYTES], ohlc_row* row) {
    row->open = decode_signed(input);
    row->high = decode_signed(input + 8);
    row->low = decode_signed(input + 16);
    row->close = decode_signed(input + 24);
    row->volume = ohlc_get_u64(input + 32);
    row->amount = ohlc_get_u64(input + 40);
    row->adjust_factor = ohlc_get_u64(input + 48);
    uint32_t bits = ohlc_get_u32(input + 56);
    memcpy(&row->reserved, &bits, sizeof(bits));
    row->flags = ohlc_get_u32(input + 60);
}

/* Nonzero when a row breaks the canonical NULL encoding. */
static inline uint64_t row_invalid_bits(const uint8_t* row) {
    uint32_t flags = ohlc_get_u32(row + 60);
    uint64_t invalid = flags & ~OHLC_FIELDS_ALL;
    /* Present fields permit every payload bit pattern. Skipping their checks
     * together also avoids costly cross-row gathers in the tile loop. */
    if ((flags & OHLC_FIELDS_MARKET) != OHLC_FIELDS_MARKET) {
        for (unsigned int field = 0; field < 7; field++) {
            /* All ones for a NULL field, zero for a present field. */
            uint64_t null_mask = (uint64_t)((flags >> field) & 1u) - 1u;
            invalid |= ohlc_get_u64(row + field * 8u) & null_mask;
        }
    }
    uint32_t reserved_null_mask = ((flags >> 7) & 1u) - 1u;
    invalid |= ohlc_get_u32(row + 56) & reserved_null_mask;
    return invalid;
}

bool ohlc_row_valid(const uint8_t input[OHLC_ROW_BYTES]) {
    return row_invalid_bits(input) == 0;
}

bool ohlc_tile_rows_valid(const uint8_t tile[OHLC_BLOCK_BYTES]) {
    uint64_t invalid = 0;
    for (size_t slot = 0; slot < OHLC_BLOCK_BYTES / OHLC_ROW_BYTES; slot++) {
        invalid |= row_invalid_bits(tile + slot * OHLC_ROW_BYTES);
    }
    return invalid == 0;
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
#define OHLC_CRC_TARGET __attribute__((target("sse4.2")))
OHLC_CRC_TARGET static inline uint32_t crc_word(uint32_t crc, uint64_t word) {
    return (uint32_t)_mm_crc32_u64(crc, word);
}

OHLC_CRC_TARGET static inline uint32_t crc_byte(uint32_t crc, uint8_t byte) {
    return _mm_crc32_u8(crc, byte);
}
#elif defined(__aarch64__)
#if defined(__clang__)
#define OHLC_CRC_TARGET __attribute__((target("crc")))
#else
#define OHLC_CRC_TARGET __attribute__((target("+crc")))
#endif
OHLC_CRC_TARGET static inline uint32_t crc_word(uint32_t crc, uint64_t word) {
    return __crc32cd(crc, word);
}

OHLC_CRC_TARGET static inline uint32_t crc_byte(uint32_t crc, uint8_t byte) {
    return __crc32cb(crc, byte);
}
#endif

#ifdef OHLC_CRC_TARGET
#define OHLC_CRC_LONG 2048u
#define OHLC_CRC_SHORT 256u

typedef struct {
    uint32_t bytes[4][256];
} ohlc_crc_shift;

static ohlc_crc_shift crc_long_shift;
static ohlc_crc_shift crc_short_shift;

/* Advance a raw CRC register by block zero bytes, without complementing it. */
OHLC_CRC_TARGET static void crc_shift_initialize(ohlc_crc_shift* shift, size_t block) {
    for (unsigned int byte = 0; byte < 4; byte++) {
        for (uint32_t value = 0; value < 256; value++) {
            uint32_t crc = value << (byte * 8u);
            for (size_t i = 0; i < block; i += 8) {
                crc = crc_word(crc, 0);
            }
            shift->bytes[byte][value] = crc;
        }
    }
}

static inline uint32_t crc_shift(const ohlc_crc_shift* shift, uint32_t crc) {
    return shift->bytes[0][crc & 255u] ^ shift->bytes[1][(crc >> 8) & 255u] ^
           shift->bytes[2][(crc >> 16) & 255u] ^ shift->bytes[3][crc >> 24];
}

/* Raw CRC updates are linear: f(r, A||B) = shift_|B|(f(r, A)) ^ f(0, B).
 * Only the first chain inherits the seed; the others start at zero. */
OHLC_CRC_TARGET static uint32_t crc_interleaved(uint32_t crc, const uint8_t* bytes, size_t block,
                                                const ohlc_crc_shift* shift) {
    uint32_t middle = 0;
    uint32_t last = 0;
    for (size_t i = 0; i < block; i += 8) {
        crc = crc_word(crc, ohlc_get_u64(bytes + i));
        middle = crc_word(middle, ohlc_get_u64(bytes + block + i));
        last = crc_word(last, ohlc_get_u64(bytes + 2u * block + i));
    }
    crc = crc_shift(shift, crc) ^ middle;
    return crc_shift(shift, crc) ^ last;
}

OHLC_CRC_TARGET static uint32_t crc_accelerated(uint32_t seed, const uint8_t* bytes, size_t size) {
    uint32_t crc = ~seed;
    while (size >= 3u * OHLC_CRC_LONG) {
        crc = crc_interleaved(crc, bytes, OHLC_CRC_LONG, &crc_long_shift);
        bytes += 3u * OHLC_CRC_LONG;
        size -= 3u * OHLC_CRC_LONG;
    }
    while (size >= 3u * OHLC_CRC_SHORT) {
        crc = crc_interleaved(crc, bytes, OHLC_CRC_SHORT, &crc_short_shift);
        bytes += 3u * OHLC_CRC_SHORT;
        size -= 3u * OHLC_CRC_SHORT;
    }
    while (size >= 8) {
        crc = crc_word(crc, ohlc_get_u64(bytes));
        bytes += 8;
        size -= 8;
    }
    while (size != 0) {
        crc = crc_byte(crc, *bytes++);
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
#ifdef OHLC_CRC_TARGET
    if (crc_implementation == crc_accelerated) {
        crc_shift_initialize(&crc_long_shift, OHLC_CRC_LONG);
        crc_shift_initialize(&crc_short_shift, OHLC_CRC_SHORT);
    }
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

size_t ohlc_allocation_bytes(size_t size) {
    if (size > SIZE_MAX - sizeof(ohlc_allocation)) {
        return SIZE_MAX;
    }
    return sizeof(ohlc_allocation) + size;
}

bool ohlc_reserve(ohlc_allocator* allocator, size_t size) {
    size_t charged = atomic_load_explicit(&allocator->charged, memory_order_relaxed);
    do {
        if (size > allocator->limit || charged > allocator->limit - size) {
            return false;
        }
    } while (!atomic_compare_exchange_weak_explicit(&allocator->charged, &charged, charged + size,
                                                    memory_order_relaxed, memory_order_relaxed));
    return true;
}

void ohlc_unreserve(ohlc_allocator* allocator, size_t size) {
    atomic_fetch_sub_explicit(&allocator->charged, size, memory_order_relaxed);
}

/* Allocation and its error check run on the same thread. Do not infer the
 * cause from shared budget counters: another thread may have freed memory,
 * or libc may have failed despite a successful budget reservation. */
static _Thread_local const ohlc_allocator* failed_allocator;
static _Thread_local ohlc_status allocation_failure;

static void allocation_failed(const ohlc_allocator* allocator, ohlc_status status) {
    failed_allocator = allocator;
    allocation_failure = status;
}

ohlc_status ohlc_allocation_status(const ohlc_allocator* allocator) {
    assert(failed_allocator == allocator && "allocation failure must be read on its thread");
    return failed_allocator == allocator ? allocation_failure : OHLC_LIMIT;
}

static void* allocate(ohlc_allocator* allocator, size_t size, bool zeroed) {
    size_t total = ohlc_allocation_bytes(size);
    ohlc_allocator* owner = allocator->parent == NULL ? allocator : allocator->parent;
    if (total == SIZE_MAX) {
        allocation_failed(allocator, OHLC_LIMIT);
        return NULL;
    }
    if (allocator->parent != NULL) {
        if (total > allocator->available) {
            allocation_failed(allocator, OHLC_LIMIT);
            allocator->shortages++;
            assert(total <= allocator->available && "checkpoint lease underestimated");
            return NULL;
        }
        allocator->available -= total;
        size_t spent = allocator->estimate - allocator->available;
        if (spent > allocator->peak) {
            allocator->peak = spent;
        }
    } else if (!ohlc_reserve(owner, total)) {
        ohlc_status status = allocator->managed && total <= owner->limit ? OHLC_BUSY : OHLC_LIMIT;
        allocation_failed(allocator, status);
        return NULL;
    }
    ohlc_allocation* allocation = NULL;
    if (!atomic_load_explicit(&owner->fail_system_alloc, memory_order_relaxed)) {
        allocation = zeroed ? calloc(1, total) : malloc(total);
    }
    if (allocation == NULL) {
        allocation_failed(allocator, OHLC_LIMIT);
        if (allocator->parent != NULL) {
            allocator->available += total;
        } else {
            ohlc_unreserve(owner, total);
        }
        return NULL;
    }
    atomic_fetch_add_explicit(&owner->used, total, memory_order_relaxed);
    allocation->allocation.size = total;
    allocation->allocation.epoch = allocator->parent == NULL ? 0 : allocator->epoch;
    return allocation + 1;
}

void* ohlc_alloc(ohlc_allocator* allocator, size_t size) {
    return allocate(allocator, size, true);
}

void* ohlc_alloc_uninit(ohlc_allocator* allocator, size_t size) {
    return allocate(allocator, size, false);
}

ohlc_status ohlc_block_pool_init(ohlc_allocator* allocator, ohlc_block_pool* pool) {
    if (pthread_mutex_init(&pool->mutex, NULL) != 0) {
        return OHLC_LIMIT;
    }
    size_t capacity = allocator->limit / 16 / (sizeof(ohlc_allocation) + sizeof(ohlc_block));
    pool->limit = capacity < OHLC_BLOCK_POOL_MAX ? capacity : OHLC_BLOCK_POOL_MAX;
    pool->count = 0;
    allocator->blocks = pool;
    return OHLC_OK;
}

ohlc_block* ohlc_block_alloc(ohlc_allocator* allocator) {
    ohlc_block_pool* pool = allocator->blocks;
    if (pool != NULL) {
        pthread_mutex_lock(&pool->mutex);
        ohlc_block* block = NULL;
        if (pool->count != 0) {
            pool->count--;
            block = pool->stack[pool->count];
        }
        pthread_mutex_unlock(&pool->mutex);
        if (block != NULL) {
            return block;
        }
    }
    return ohlc_alloc_uninit(allocator, sizeof(ohlc_block));
}

void ohlc_block_free(ohlc_allocator* allocator, ohlc_block* block) {
    ohlc_block_pool* pool = allocator->blocks;
    if (pool != NULL) {
        pthread_mutex_lock(&pool->mutex);
        if (pool->count < pool->limit) {
            pool->stack[pool->count++] = block;
            pthread_mutex_unlock(&pool->mutex);
            return;
        }
        pthread_mutex_unlock(&pool->mutex);
    }
    ohlc_free(allocator, block);
}

void ohlc_block_pool_trim(ohlc_allocator* allocator) {
    ohlc_block_pool* pool = allocator->blocks;
    if (pool == NULL) {
        return;
    }
    pthread_mutex_lock(&pool->mutex);
    while (pool->count != 0) {
        ohlc_free(allocator, pool->stack[--pool->count]);
    }
    pthread_mutex_unlock(&pool->mutex);
}

void ohlc_free(ohlc_allocator* allocator, void* pointer) {
    if (pointer != NULL) {
        ohlc_allocation* allocation = (ohlc_allocation*)pointer - 1;
        ohlc_allocator* owner = allocator->parent == NULL ? allocator : allocator->parent;
        size_t total = allocation->allocation.size;
        atomic_fetch_sub_explicit(&owner->used, total, memory_order_relaxed);
        if (allocator->parent != NULL && allocation->allocation.epoch == allocator->epoch) {
            allocator->available += total;
        } else {
            ohlc_unreserve(owner, total);
        }
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

static bool iov_range_valid(const struct iovec* iov, int count, uint64_t offset) {
    if (offset > INT64_MAX) {
        return false;
    }
    uint64_t remaining = (uint64_t)INT64_MAX - offset;
    for (int i = 0; i < count; i++) {
        if (iov[i].iov_len > remaining) {
            return false;
        }
        remaining -= iov[i].iov_len;
    }
    return true;
}

static void advance_iov(struct iovec** iov, int* count, size_t transferred) {
    while (*count != 0 && transferred >= (*iov)->iov_len) {
        transferred -= (*iov)->iov_len;
        (*iov)++;
        (*count)--;
    }
    if (*count != 0 && transferred != 0) {
        (*iov)->iov_base = (uint8_t*)(*iov)->iov_base + transferred;
        (*iov)->iov_len -= transferred;
    }
}

ohlc_status ohlc_readv_full(int fd, struct iovec* iov, int count, uint64_t offset) {
    if (!iov_range_valid(iov, count, offset)) {
        return OHLC_LIMIT;
    }
    /* A single segment avoids preadv's per-segment overhead under contention. */
    if (count == 1) {
        return ohlc_read_full(fd, iov[0].iov_base, iov[0].iov_len, offset);
    }
    advance_iov(&iov, &count, 0);
    while (count != 0) {
        ssize_t received = preadv(fd, iov, count, (off_t)offset);
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received <= 0) {
            return received == 0 ? OHLC_CORRUPT : OHLC_IO;
        }
        advance_iov(&iov, &count, (size_t)received);
        offset += (uint64_t)received;
    }
    return OHLC_OK;
}

ohlc_status ohlc_writev_full(int fd, struct iovec* iov, int count, uint64_t offset) {
    if (!iov_range_valid(iov, count, offset)) {
        return OHLC_LIMIT;
    }
    advance_iov(&iov, &count, 0);
    while (count != 0) {
        ssize_t written = pwritev(fd, iov, count, (off_t)offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return OHLC_IO;
        }
        advance_iov(&iov, &count, (size_t)written);
        offset += (uint64_t)written;
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

/* SPDX-License-Identifier: Apache-2.0 */
#ifndef OHLC_INTERNAL_H
#define OHLC_INTERNAL_H

#include "ohlc/ohlc.h"

#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/uio.h>

#define OHLC_PAGE_BYTES 4096u
#define OHLC_BLOCK_BYTES 8192u
#define OHLC_TIME_CAPACITY 508u
#define OHLC_BRANCH_CAPACITY 256u
#define OHLC_MAX_DEPTH 16u
#define OHLC_HOT_GROUP 1u
#define OHLC_FRAME_LIMIT (16u * 1024u * 1024u)
#define OHLC_PAGE_ENTRIES 256u
#define OHLC_READ_WINDOW 16u
#define OHLC_RESULT_WINDOW (OHLC_READ_WINDOW * 16u)
#define OHLC_IO_QUEUE 64u
#define OHLC_IO_WORKERS 16u
#define OHLC_CACHE_SHARDS 64u
#define OHLC_CACHE_WAYS 16u
/* A shard is split only while each half keeps at least 64 entries. */
#define OHLC_CACHE_MIN_SETS 4u
#define OHLC_COMMIT_QUEUE 64u
#define OHLC_COMMIT_GROUP 16u
#define OHLC_BLOCK_POOL_MAX 1024u
#define OHLC_WAL_ZERO_BYTES (1024u * 1024u)
/* One commit group appends at most this many WAL bytes before its single sync.
 * Small-group zero refills keep the entire unsynced tail within this bound,
 * which recovery relies on to recognize a tail after a crash. */
#define OHLC_COMMIT_GROUP_BYTES OHLC_FRAME_LIMIT

static inline size_t ohlc_size_add(size_t left, size_t right) {
    return right > SIZE_MAX - left ? SIZE_MAX : left + right;
}

static inline size_t ohlc_size_multiply(size_t count, size_t size) {
    return size != 0 && count > SIZE_MAX / size ? SIZE_MAX : count * size;
}

bool ohlc_row_valid(const uint8_t input[OHLC_ROW_BYTES]);
bool ohlc_tile_rows_valid(const uint8_t tile[OHLC_BLOCK_BYTES]);
/* Only the cursor's owner may inspect whether all planned results are consumed. */
bool ohlc_cursor_exhausted(const ohlc_cursor* cursor);

typedef struct ohlc_allocator {
    _Atomic size_t used;
    /* Charged bytes include live allocations and unused reservations. */
    _Atomic size_t charged;
    size_t limit;
    struct ohlc_block_pool* blocks;
    /* A checkpoint lease spends already charged bytes. Only its owner uses it. */
    struct ohlc_allocator* parent;
    size_t available;
    size_t estimate;
    size_t peak;
    uint64_t shortages;
    uint64_t epoch;
    /* Normal engine allocations report temporary pressure as BUSY. */
    bool managed;
    /* Internal deterministic system-allocation fault injection; normally false. */
    _Atomic bool fail_system_alloc;
} ohlc_allocator;

typedef struct ohlc_time_node ohlc_time_node;
struct ohlc_time_node {
    _Atomic uint32_t refs;
    uint16_t count;
    uint8_t leaf;
    uint8_t reserved;
    _Atomic uint64_t disk_offset;
    uint64_t padding[2];
    union {
        struct {
            uint32_t keys[OHLC_TIME_CAPACITY];
            uint32_t codes[OHLC_TIME_CAPACITY];
        } values;
        struct {
            uint32_t keys[OHLC_BRANCH_CAPACITY];
            ohlc_time_node* children[OHLC_BRANCH_CAPACITY + 1u];
        } branch;
    } body;
};

typedef struct {
    const ohlc_time_node* nodes[OHLC_MAX_DEPTH];
    uint16_t positions[OHLC_MAX_DEPTH];
    uint8_t depth;
} ohlc_time_iterator;

typedef struct {
    _Atomic uint32_t refs;
    _Atomic uint64_t disk_offset;
    uint32_t keys[1024];
} ohlc_time_page;

typedef struct {
    _Atomic uint32_t refs;
    uint8_t presence[16];
    uint8_t data[OHLC_BLOCK_BYTES];
} ohlc_block;

/* Retained blocks remain charged, including allocation headers. Capacity is
 * at most one sixteenth of the memory limit, below the pressure threshold. */
typedef struct ohlc_block_pool {
    pthread_mutex_t mutex;
    ohlc_block* stack[OHLC_BLOCK_POOL_MAX];
    size_t count;
    size_t limit;
} ohlc_block_pool;

typedef struct {
    uint64_t base;
    uint16_t mask;
    uint16_t flags;
    uint32_t volume;
} ohlc_group;

typedef struct {
    _Atomic uint32_t refs;
    ohlc_group backing;
    ohlc_block* blocks[16];
} ohlc_hot_group;

typedef struct {
    _Atomic uint32_t refs;
    _Atomic uint64_t disk_offset;
    ohlc_group groups[OHLC_PAGE_ENTRIES];
} ohlc_group_page;

typedef struct {
    _Atomic uint32_t refs;
    uint32_t page_count;
    _Atomic uint64_t disk_offset;
    ohlc_group_page** pages;
} ohlc_band;

typedef struct ohlc_band_node ohlc_band_node;
struct ohlc_band_node {
    _Atomic uint32_t refs;
    uint16_t size;
    uint8_t level;
    uint8_t reserved;
    _Atomic uint64_t disk_offset;
    uint64_t bitmap[8];
    void* children[];
};

typedef struct ohlc_ticker_entry {
    _Atomic uint32_t refs;
    uint64_t hash;
    uint32_t code;
    uint32_t length;
    uint8_t bytes[];
} ohlc_ticker_entry;

typedef struct {
    _Atomic uint32_t refs;
    uint64_t count;
    size_t capacity;
    ohlc_ticker_entry** entries;
    ohlc_ticker_entry** buckets;
} ohlc_dictionary;

/* Admission-only caches are protected by writer; queries never inspect them.
 * Maxima may stay conservative until the next full checkpoint/open rebuild. */
typedef struct {
    size_t catalog;
    size_t copies;
    size_t storage;
    size_t scratch;
    size_t dictionary;
    size_t reverse;
    /* Dropped-table maxima are admission upper bounds, not request lower bounds. */
    bool maxima_stale;
} ohlc_memory_estimate;

typedef struct {
    _Atomic uint32_t refs;
    ohlc_table_info info;
    ohlc_dictionary* dictionary;
    ohlc_time_node* times;
    uint64_t time_count;
    size_t time_page_count;
    ohlc_time_page** time_pages;
    ohlc_band_node* bands;
    /* Upper bound for copying dirty band directories, including allocation headers. */
    size_t dirty_directory_bytes;
    size_t hot_groups;
    /* Recovery discovers orphan volume IDs before admitting WAL-created tables. */
    uint64_t next_volume_hint;
    uint64_t checkpoint_next_volume;
    size_t checkpoint_meta_count;
    ohlc_memory_estimate memory;
    _Atomic uint64_t disk_offset;
} ohlc_table;

typedef struct {
    _Atomic uint32_t refs;
    ohlc_table* tables[OHLC_PAGE_ENTRIES];
} ohlc_table_page;

typedef struct ohlc_root {
    _Atomic uint32_t refs;
    uint64_t seq;
    uint64_t ticker_count;
    uint32_t table_count;
    uint32_t last_table_id;
    size_t page_count;
    ohlc_table_page** pages;
    ohlc_memory_estimate memory;
    /* Publication tracking, protected by the publishing database's root_mutex.
     * A root that was never published has a NULL publisher and epoch 0. Live
     * published roots form a list in increasing epoch order. */
    ohlc_db* publisher;
    uint64_t epoch;
    struct ohlc_root* live_previous;
    struct ohlc_root* live_next;
} ohlc_root;

static inline size_t ohlc_root_page_entries(const ohlc_root* root, size_t page) {
    uint64_t first = (uint64_t)page * OHLC_PAGE_ENTRIES;
    if (first >= root->last_table_id) {
        return 0;
    }
    uint64_t remaining = root->last_table_id - first;
    return remaining < OHLC_PAGE_ENTRIES ? (size_t)remaining : OHLC_PAGE_ENTRIES;
}

typedef struct ohlc_retired_table {
    struct ohlc_retired_table* next;
    uint64_t sequence;
    uint32_t id;
} ohlc_retired_table;

/* An extent that a durable checkpoint root stopped referencing. It belongs to
 * exactly one (band, group) of one table. */
typedef struct {
    uint32_t table;
    uint32_t band;
    uint32_t group;
    uint32_t volume;
    uint64_t base;
    uint32_t blocks;
} ohlc_dead_extent;

/* Extents superseded by one checkpoint, held until no checkpoint slot and no
 * live read root can reference them. See design section 13.6. */
typedef struct ohlc_reclaim_batch {
    struct ohlc_reclaim_batch* next;
    /* Every live published root must reach this epoch before the extents are
     * reused. Zero until a later checkpoint is durable and the current root
     * was checked. */
    uint64_t barrier;
    size_t count;
    size_t capacity;
    ohlc_dead_extent* extents;
} ohlc_reclaim_batch;

/* A grow-only table of pointers that readers use without a lock. Only the
 * storage owner (the checkpoint owner, or recovery) stores slots. Growth
 * publishes a larger copy with release semantics; a replaced copy stays valid
 * through previous until close, because a reader may still be using it. */
typedef struct ohlc_slot_table ohlc_slot_table;
struct ohlc_slot_table {
    ohlc_slot_table* previous;
    size_t capacity;
    _Atomic(void*) slots[];
};
typedef _Atomic(ohlc_slot_table*) ohlc_slots;

static inline void* ohlc_slot_get(ohlc_slots* slots, size_t index) {
    ohlc_slot_table* table = atomic_load_explicit(slots, memory_order_acquire);
    if (table == NULL || index >= table->capacity) {
        return NULL;
    }
    return atomic_load_explicit(&table->slots[index], memory_order_acquire);
}

static inline size_t ohlc_slot_capacity(ohlc_slots* slots) {
    ohlc_slot_table* table = atomic_load_explicit(slots, memory_order_acquire);
    return table == NULL ? 0 : table->capacity;
}

typedef struct {
    uint32_t id;
    int data_fd;
    int meta_fd;
    /* Next append offset; only the storage owner uses it. */
    uint64_t size;
    /* Metadata entries below meta_count that a live root references are
     * immutable. The owner writes new entries, and any new pages, before
     * publishing the count with release. It rewrites the entries of reused
     * blocks before it publishes the root that references them. */
    _Atomic size_t meta_count;
    /* Pages of OHLC_META_PER_PAGE 32-byte entries followed by one free bit
     * per entry, indexed by page number. */
    ohlc_slots meta_pages;
    /* Only the checkpoint owner reads or writes persistence dirty flags. */
    bool dirty;
    /* Reuse state of the free bits; only the storage owner uses it. Free
     * blocks lie in [free_first, free_end). free_cursor is the next-fit
     * position. A search for free_miss or more consecutive blocks has failed
     * since the last release. */
    size_t free_blocks;
    size_t free_first;
    size_t free_end;
    size_t free_cursor;
    size_t free_miss;
} ohlc_volume;

typedef struct {
    uint32_t id;
    int index_fd;
    uint64_t index_size;
    uint64_t dictionary_disk_count;
    uint64_t dictionary_disk_offset;
    /* Highest loaded volume ID; admission also reads it to budget growth. */
    _Atomic size_t volume_count;
    /* ohlc_volume pointers indexed by volume ID - 1. */
    ohlc_slots volumes;
    _Atomic uint64_t next_volume_id;
    bool index_dirty;
} ohlc_table_files;

/* Lookup metadata; tile data lives at the same index in the shard's blocks.
 * Only the shard mutex holder changes identity, valid, status and loading;
 * status and loading are also read only under it. Identity, valid and version
 * are atomic so that hits on published entries can skip the mutex.
 *
 * pins counts borrowed views and is decremented with release outside the
 * mutex. Reuse claims an entry under the mutex by moving pins from zero to an
 * evicting mark with acquire, rewrites identity, increments version with
 * release, and then stores the loader's single pin with release. A hit
 * without the mutex never pins a marked entry and rechecks version after
 * pinning, so it cannot borrow an entry that was reused meanwhile.
 *
 * The reader that sets loading writes and validates data in place while
 * loading is true. It publishes valid with release under the mutex before
 * waiting on other entries; waiters read data only after loading becomes false. */
typedef struct {
    _Atomic uint64_t offset;
    _Atomic uint32_t table;
    _Atomic uint32_t volume;
    _Atomic uint32_t band;
    _Atomic uint32_t group;
    _Atomic uint32_t version;
    _Atomic uint32_t pins;
    ohlc_status status;
    _Atomic uint8_t tile;
    _Atomic bool valid;
    _Atomic bool visited;
    bool loading;
} ohlc_cache_entry;

/* Paddings keep fields written under the mutex off the lines that lock-free
 * hits read, here and in the next shard, without requiring stronger alignment
 * than the database allocation provides. */
typedef struct {
    /* Fixed after open. */
    size_t sets;
    ohlc_cache_entry* entries; /* sets * OHLC_CACHE_WAYS */
    uint8_t* blocks;           /* sets * OHLC_CACHE_WAYS * OHLC_BLOCK_BYTES */
    uint8_t fixed_padding[64];
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    size_t hand;
    uint8_t padding[64];
} ohlc_cache;

typedef struct {
    int fd;
    uint64_t offset;
    size_t size;
    struct iovec iov[OHLC_READ_WINDOW];
    int iov_count;
    ohlc_status status;
    size_t* pending;
} ohlc_io_request;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t workers[OHLC_IO_WORKERS];
    ohlc_io_request* queue[OHLC_IO_QUEUE];
    size_t head;
    size_t count;
    size_t worker_count;
    bool stopping;
} ohlc_io_pool;

/* A tile view is borrowed until storage_release; its owning root stays pinned. */
typedef struct {
    const ohlc_group* entry;
    uint32_t band;
    uint32_t group;
    uint8_t tile;
    const uint8_t* presence;
    const uint8_t* data;
    uint8_t* buffer;
    ohlc_cache* cache;
    ohlc_cache_entry* cached;
    bool owns_load;
    int fd;
    uint64_t offset;
} ohlc_tile_view;

typedef struct {
    uint32_t key;
    uint8_t view;
    uint8_t slot;
} ohlc_result_plan;

typedef struct {
    uint32_t table;
    const uint8_t* rows;
    size_t size;
    bool named;
    size_t count;
    uint64_t sequence;
    ohlc_status status;
    bool done;
    bool memory_busy;
} ohlc_write_request;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t worker;
    ohlc_write_request* queue[OHLC_COMMIT_QUEUE];
    size_t head;
    size_t count;
    /* Protected by mutex: requests owned by either one caller or the worker. */
    size_t active;
    bool stopping;
} ohlc_commit_queue;

/* Paddings separate fields that every query reads or updates, so that the
 * per-query atomics do not share cache lines with each other or with options. */
struct ohlc_db {
    ohlc_options options;
    uint8_t options_padding[64];
    ohlc_allocator allocator;
    ohlc_allocator checkpoint_allocator;
    ohlc_block_pool block_pool;
    char* path;
    uint8_t uuid[16];
    int lock_fd;
    int directory_fd;
    int catalog_fd;
    int wal_fd;
    uint32_t wal_id;
    uint64_t wal_size;
    /* Protected by writer, or owned exclusively during open and close. */
    uint64_t wal_zero_end;
    uint64_t wal_synced_size;
    /* Recovery and checkpoint cleanup own this cursor; writers never change it. */
    uint32_t wal_first_id;
    uint64_t catalog_size;
    _Atomic uint64_t checkpoint_generation;
    _Atomic uint64_t checkpoint_sequences[2];
    uint32_t checkpoint_wal_ids[2];
    uint64_t checkpoint_wal_bytes;
    uint64_t checkpoint_at_ms;
    ohlc_root* checkpoint_root;
    pthread_mutex_t checkpoint_mutex;
    pthread_mutex_t writer;
    pthread_cond_t memory_changed;
    pthread_cond_t memory_available;
    /* Incremented under writer; attempts may read without taking writer. */
    _Atomic uint64_t memory_generation;
    /* Protected by writer. Reservations are also included in allocator.charged. */
    size_t checkpoint_reserve;
    size_t recovery_reserve;
    size_t largest_frame;
    size_t checkpoint_tail_frame;
    size_t pending_frame;
    const ohlc_root* checkpoint_source;
    bool writes_paused;
    bool recovering;
    _Atomic bool memory_pressure;
    pthread_mutex_t root_mutex;
    /* Protected by root_mutex: live published roots, oldest first. */
    ohlc_root* live_first;
    ohlc_root* live_last;
    uint64_t root_epoch;
    /* Serializes storage mutations; queries and writers only read storage
     * objects, through ohlc_slots, and never take this lock. */
    pthread_mutex_t storage_mutex;
    ohlc_root* root;
    /* ohlc_table_files pointers indexed by table ID - 1. */
    ohlc_slots files;
    ohlc_retired_table* retired_tables;
    /* Owned by the checkpoint owner, or by single-threaded open and close.
     * Reuse stays disabled after open until one checkpoint is durable, because
     * the other checkpoint slot may still reference unmarked blocks. */
    ohlc_reclaim_batch* reclaim_batches;
    bool reuse_enabled;
    _Atomic bool reclaim_pending;
    /* Latest reclaim pass result; written by the checkpoint owner under
     * checkpoint_mutex (or during single-threaded open) and read under it. */
    ohlc_status reclaim_status;
    uint32_t reclaim_table;
    ohlc_cache cache[OHLC_CACHE_SHARDS];
    size_t cache_shard_count;
    /* Allocator bytes held by the cache shards from open until close. */
    size_t cache_reserved;
    uint8_t cache_padding[64];
    ohlc_io_pool io;
    bool io_initialized;
    ohlc_commit_queue commits;
    bool commits_initialized;
    uint8_t cursor_padding[64];
    _Atomic uint32_t cursor_count;
    uint8_t read_counter_padding[64];
    _Atomic uint64_t disk_read_bytes;
    _Atomic uint64_t disk_read_calls;
    _Atomic uint64_t cache_hits;
    _Atomic uint64_t cache_misses;
    uint8_t write_counter_padding[64];
    _Atomic uint64_t wal_bytes;
    _Atomic uint64_t wal_syncs;
    _Atomic uint64_t data_bytes;
    _Atomic bool failed;
    /* Written by recovery before open returns; read-only afterwards. */
    ohlc_wal_discard wal_discard;
};

struct ohlc_cursor {
    ohlc_db* db;
    ohlc_root* root;
    const ohlc_table* table;
    const ohlc_band* cross_band;
    ohlc_time_iterator times;
    uint64_t end;
    uint64_t next_code;
    uint64_t deadline_ms;
    uint32_t ticker;
    uint32_t cross_code;
    uint32_t* selected_codes;
    size_t selected_count;
    size_t selected_position;
    uint8_t kind;
    bool done;
    ohlc_status error;
    size_t view_count;
    size_t planned;
    size_t emitted;
    ohlc_tile_view views[OHLC_READ_WINDOW];
    ohlc_result_plan plan[OHLC_RESULT_WINDOW];
    uint8_t buffers[OHLC_READ_WINDOW * OHLC_BLOCK_BYTES];
};

/* Little-endian codecs for any alignment. They are inline because CRC, row
 * validation and result copying call them per word; compilers merge the byte
 * operations into single loads and stores. */
static inline uint16_t ohlc_get_u16(const uint8_t* p) {
    return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

static inline uint32_t ohlc_get_u32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static inline uint64_t ohlc_get_u64(const uint8_t* p) {
    return (uint64_t)ohlc_get_u32(p) | ((uint64_t)ohlc_get_u32(p + 4) << 32);
}

static inline void ohlc_put_u16(uint8_t* p, uint16_t value) {
    p[0] = (uint8_t)value;
    p[1] = (uint8_t)(value >> 8);
}

static inline void ohlc_put_u32(uint8_t* p, uint32_t value) {
    p[0] = (uint8_t)value;
    p[1] = (uint8_t)(value >> 8);
    p[2] = (uint8_t)(value >> 16);
    p[3] = (uint8_t)(value >> 24);
}

static inline void ohlc_put_u64(uint8_t* p, uint64_t value) {
    ohlc_put_u32(p, (uint32_t)value);
    ohlc_put_u32(p + 4, (uint32_t)(value >> 32));
}
uint32_t ohlc_crc32c(uint32_t seed, const void* bytes, size_t size);
uint64_t ohlc_monotonic_ms(void);
unsigned int ohlc_popcount(uint16_t value);
void* ohlc_alloc(ohlc_allocator* allocator, size_t size);
/* Contents are uninitialized; callers must write every byte before reading it. */
void* ohlc_alloc_uninit(ohlc_allocator* allocator, size_t size);
size_t ohlc_allocation_bytes(size_t size);
bool ohlc_reserve(ohlc_allocator* allocator, size_t size);
void ohlc_unreserve(ohlc_allocator* allocator, size_t size);
ohlc_status ohlc_allocation_status(const ohlc_allocator* allocator);
/* Initialize an empty pool and attach it on success; open owns this call. */
ohlc_status ohlc_block_pool_init(ohlc_allocator* allocator, ohlc_block_pool* pool);
/* Returns an uninitialized block owned by the caller. Thread-safe with free. */
ohlc_block* ohlc_block_alloc(ohlc_allocator* allocator);
/* Return an exclusively owned block to its allocator's pool, or free it. */
void ohlc_block_free(ohlc_allocator* allocator, ohlc_block* block);
void ohlc_block_pool_trim(ohlc_allocator* allocator);
void ohlc_free(ohlc_allocator* allocator, void* pointer);
ohlc_status ohlc_definition_validate(const ohlc_table_definition* definition);
bool ohlc_period_is_date(ohlc_period_unit unit);
ohlc_status ohlc_timezone_validate(const char* name);
size_t ohlc_definition_encode(uint8_t* output, const ohlc_table_info* info);
ohlc_status ohlc_definition_decode(const uint8_t* data, size_t size, ohlc_table_info* info);
ohlc_status ohlc_read_full(int fd, void* data, size_t size, uint64_t offset);
/* May consume and modify the supplied iovec array while completing the read. */
ohlc_status ohlc_readv_full(int fd, struct iovec* iov, int count, uint64_t offset);
ohlc_status ohlc_write_full(int fd, const void* data, size_t size, uint64_t offset);
/* Consumes and modifies the iovec array; the pointed-to data is only read. */
ohlc_status ohlc_writev_full(int fd, struct iovec* iov, int count, uint64_t offset);
ohlc_status ohlc_sync(int fd);
ohlc_status ohlc_io_init(ohlc_db* db);
void ohlc_io_destroy(ohlc_db* db);
void ohlc_io_read(ohlc_db* db, ohlc_io_request* requests, size_t count);

ohlc_status ohlc_time_insert(ohlc_allocator* a, ohlc_time_node** root, uint32_t key, uint32_t code);
bool ohlc_time_find(const ohlc_time_node* root, uint32_t key, uint32_t* code);
void ohlc_time_seek(const ohlc_time_node* root, uint32_t key, ohlc_time_iterator* iterator);
bool ohlc_time_next(ohlc_time_iterator* iterator, uint32_t* key, uint32_t* code);
/* Borrow a leaf suffix until the owning root is released. Advance by at most
 * the returned count before requesting another span. Neither call allocates. */
size_t ohlc_time_span(ohlc_time_iterator* iterator, const uint32_t** keys, const uint32_t** codes);
void ohlc_time_advance(ohlc_time_iterator* iterator, size_t count);
const ohlc_band* ohlc_band_find(const ohlc_table* table, uint32_t band);
void ohlc_time_retain(ohlc_time_node* node);
void ohlc_time_release(ohlc_allocator* a, ohlc_time_node* node);
ohlc_status ohlc_table_time_add(ohlc_db* db, ohlc_table* table, uint32_t key, uint32_t* code);

const ohlc_group* ohlc_group_find(const ohlc_table* table, uint32_t band, uint32_t group);
ohlc_status ohlc_group_edit(ohlc_db* db, ohlc_table* table, uint32_t band, uint32_t group,
                            ohlc_group** output);
ohlc_status ohlc_group_edit_alloc(ohlc_allocator* allocator, ohlc_table* table, uint32_t band,
                                  uint32_t group, ohlc_group** output);
void ohlc_table_measure_dirty(ohlc_table* table);
void ohlc_group_retain(ohlc_group* group);
void ohlc_group_release(ohlc_allocator* a, ohlc_group* group);
void ohlc_band_node_retain(ohlc_band_node* node);
void ohlc_band_node_release(ohlc_allocator* a, ohlc_band_node* node);
void ohlc_band_release(ohlc_allocator* a, ohlc_band* band);
ohlc_root* ohlc_root_new(ohlc_db* db);
ohlc_root* ohlc_root_copy(ohlc_db* db, const ohlc_root* root);
ohlc_root* ohlc_root_copy_alloc(const ohlc_root* root, ohlc_allocator* allocator);
ohlc_root* ohlc_root_acquire(ohlc_db* db);
void ohlc_root_release(ohlc_db* db, ohlc_root* root);
void ohlc_root_release_alloc(ohlc_root* root, ohlc_allocator* allocator);
void ohlc_root_publish(ohlc_db* db, ohlc_root* root);
/* Epoch of the oldest published root that is still referenced. */
uint64_t ohlc_root_oldest_epoch(ohlc_db* db);
const ohlc_table* ohlc_root_table(const ohlc_root* root, uint32_t id);
ohlc_status ohlc_root_edit_table(ohlc_db* db, ohlc_root* root, uint32_t id, ohlc_table** output);
ohlc_status ohlc_root_edit_table_alloc(ohlc_root* root, uint32_t id, ohlc_table** output,
                                       ohlc_allocator* allocator);
ohlc_status ohlc_root_set_table(ohlc_db* db, ohlc_root* root, uint32_t id, ohlc_table* table);
ohlc_status ohlc_root_add_table(ohlc_db* db, ohlc_root* root, const ohlc_table_info* info);
ohlc_status ohlc_root_drop_table(ohlc_db* db, ohlc_root* root, uint32_t id);
ohlc_status ohlc_prepare_drop(ohlc_db* db, const ohlc_root* source, uint32_t id,
                              ohlc_root** candidate, ohlc_retired_table** retired);
ohlc_status ohlc_storage_find_retired(ohlc_db* db);
void ohlc_storage_reclaim_tables(ohlc_db* db);
/* Returns the failure of the latest dropped-table reclaim pass and its table ID,
 * or OHLC_OK and 0 when that pass removed every due directory. Failed directories
 * stay retired and are retried by later checkpoints. Waits for a running checkpoint. */
ohlc_status ohlc_reclaim_failure(ohlc_db* db, uint32_t* table_id);
/* Reports whether a tables/ entry name belongs to a dropped table that awaits
 * reclamation and whose deletion both checkpoint slots cover, so neither
 * CURRENT slot references the directory. */
bool ohlc_storage_directory_reclaimable(ohlc_db* db, const char* name);
void ohlc_table_release(ohlc_allocator* a, ohlc_table* table);
ohlc_status ohlc_prepare_write(ohlc_db* db, const ohlc_root* source, uint32_t table_id,
                               const uint8_t* rows, size_t count, ohlc_root** output);
uint64_t ohlc_dictionary_count(const ohlc_table* table);
void ohlc_dictionary_retain(ohlc_dictionary* dictionary);
void ohlc_dictionary_release(ohlc_allocator* allocator, ohlc_dictionary* dictionary);
const ohlc_ticker_entry* ohlc_dictionary_find(const ohlc_dictionary* dictionary, ohlc_bytes ticker);
ohlc_status ohlc_dictionary_add(ohlc_db* db, ohlc_table* table, ohlc_bytes ticker, uint32_t* code);
ohlc_status ohlc_submit_named(ohlc_db* db, uint32_t table_id, const void* payload, size_t size,
                              size_t count, uint64_t* sequence);
ohlc_status ohlc_prepare_named(ohlc_db* db, const ohlc_root* source, uint32_t table_id,
                               const uint8_t* payload, size_t size, size_t count,
                               ohlc_root** output);
/* Pack a batch-local name dictionary followed by fixed-size indexed rows. */
ohlc_status ohlc_named_pack(ohlc_allocator* allocator, const ohlc_bytes* tickers,
                            size_t ticker_count, const void* rows, size_t count, uint8_t** output,
                            size_t* size);

/* Store one slot as the storage owner, growing the table when needed. */
ohlc_status ohlc_slot_set(ohlc_db* db, ohlc_slots* slots, size_t index, void* value);
/* Free every copy of a slot table at close; the slot values are not freed. */
void ohlc_slot_free(ohlc_db* db, ohlc_slots* slots);
ohlc_status ohlc_storage_open(ohlc_db* db, bool* fresh);
void ohlc_storage_close(ohlc_db* db);
ohlc_status ohlc_storage_table(ohlc_db* db, uint32_t table_id, bool create,
                               ohlc_table_files** output);
ohlc_status ohlc_storage_load_volume(ohlc_db* db, ohlc_table_files* files, uint32_t id);
ohlc_status ohlc_storage_tile(ohlc_db* db, uint32_t table, uint32_t band, uint32_t group,
                              const ohlc_group* entry, uint8_t tile, uint8_t* output);
ohlc_status ohlc_storage_read(ohlc_db* db, uint32_t table, ohlc_tile_view* views, size_t count);
void ohlc_storage_release(ohlc_tile_view* views, size_t count);
const uint8_t* ohlc_storage_presence(ohlc_db* db, uint32_t table, const ohlc_group* entry,
                                     uint8_t tile);
ohlc_status ohlc_storage_flush_group(ohlc_db* db, uint32_t table, uint32_t band, uint32_t group,
                                     const ohlc_group* entry, uint8_t* data, ohlc_group* output);
/* Extent reuse; see design section 13.6. Only the storage owner calls these.
 * Marks every block of every loaded volume as a reuse candidate. */
void ohlc_storage_reuse_candidates(ohlc_db* db);
/* Withdraws the blocks of a referenced on-disk extent from the candidates. */
void ohlc_storage_reuse_keep(ohlc_db* db, uint32_t table, const ohlc_group* entry);
/* Makes the blocks of a dead extent available to later flushes. The caller
 * guarantees that no checkpoint slot and no live root references them. */
void ohlc_storage_reuse_free(ohlc_db* db, const ohlc_dead_extent* extent);
/* Marks the blocks that the recovered checkpoint root does not reference. */
void ohlc_reclaim_mark(ohlc_db* db, const ohlc_root* root);
/* Called by the checkpoint owner once saved is durable and merged, without
 * writer. Records the extents that previous references and saved does not,
 * and seals the batches of earlier checkpoints. */
void ohlc_reclaim_checkpoint(ohlc_db* db, const ohlc_root* previous, const ohlc_root* saved);
/* Releases the sealed batches that no live root predates. */
void ohlc_reclaim_release(ohlc_db* db);
/* Drops every pending batch at close. */
void ohlc_reclaim_discard(ohlc_db* db);
/* Conservative allocation bounds; SIZE_MAX denotes an unrepresentable request. */
size_t ohlc_storage_checkpoint_bytes(ohlc_db* db, const ohlc_root* root);
size_t ohlc_storage_table_bytes(ohlc_db* db, const ohlc_table* table);
size_t ohlc_storage_root_bytes(ohlc_db* db, const ohlc_root* root);
void ohlc_storage_capture(ohlc_db* db, const ohlc_root* root);
ohlc_status ohlc_storage_volume_hint(ohlc_db* db, uint32_t table, uint64_t* next);
size_t ohlc_checkpoint_bytes(ohlc_db* db, const ohlc_root* root);
/* Writer owns admission and reservation changes. No function waits for I/O. */
/* Pure lower-bound check: no allocation, reservation change or WAL write. */
ohlc_status ohlc_memory_request(ohlc_db* db, const ohlc_root* root, size_t payload,
                                size_t minimum_blocks);
ohlc_status ohlc_write_check(ohlc_db* db, const ohlc_root* root, const uint8_t* rows, size_t count,
                             size_t payload, bool named);
ohlc_status ohlc_named_check(ohlc_db* db, const ohlc_root* root, const uint8_t* payload,
                             size_t size, size_t count);
ohlc_status ohlc_named_size(const ohlc_bytes* tickers, size_t ticker_count, size_t count,
                            size_t* size);
ohlc_status ohlc_memory_begin(ohlc_db* db, size_t payload);
ohlc_status ohlc_memory_admit(ohlc_db* db, ohlc_root* root);
ohlc_status ohlc_memory_admit_table(ohlc_db* db, ohlc_root* root, const ohlc_root* source,
                                    uint32_t table);
ohlc_status ohlc_memory_reject(ohlc_db* db, ohlc_status status);
ohlc_status ohlc_memory_checkpoint_begin(ohlc_db* db, const ohlc_root* source);
void ohlc_memory_checkpoint_end(ohlc_db* db);
void ohlc_memory_wal_frame(ohlc_db* db, uint64_t sequence, size_t bytes);
void ohlc_checkpoint_wait(ohlc_db* db, uint32_t milliseconds);
/* Per-calling-thread retry provenance; commit requests carry it across the
 * worker handoff. A queue rejection must never inherit another request's cause. */
uint64_t ohlc_memory_attempt(ohlc_db* db);
bool ohlc_memory_retryable(ohlc_db* db);
void ohlc_memory_note_busy(ohlc_db* db);
void ohlc_memory_wait(ohlc_db* db, uint64_t generation, uint32_t milliseconds);
ohlc_status ohlc_storage_header(ohlc_db* db, int fd, uint32_t type, uint32_t table, uint32_t volume,
                                uint64_t generation, bool create);
ohlc_status ohlc_recover(ohlc_db* db, bool fresh);
ohlc_status ohlc_wal_append(ohlc_db* db, uint16_t type, uint32_t table, uint32_t count,
                            uint64_t seq, const uint8_t* payload, size_t size);
/* Payload is borrowed and must remain unchanged through wal_write. */
typedef struct {
    uint8_t header[64];
    uint8_t tail[40]; /* Zero padding followed by the 32-byte footer. */
    const uint8_t* payload;
    size_t payload_size;
    size_t tail_size;
    size_t total;
} ohlc_wal_frame;

ohlc_status ohlc_wal_frame_init(uint16_t type, uint32_t table, uint32_t count, uint64_t seq,
                                const uint8_t* payload, size_t size, ohlc_wal_frame* frame);
ohlc_status ohlc_wal_write(ohlc_db* db, const ohlc_wal_frame* frame, uint64_t seq);
ohlc_status ohlc_wal_sync(ohlc_db* db);
ohlc_status ohlc_wal_commit_sync(ohlc_db* db);
/* Best-effort tail trimming after all commits stop; does not report errors. */
void ohlc_wal_close_tail(ohlc_db* db);
/* The checkpoint mutex serializes persistence; file I/O never holds writer.
 * Under pressure, mutation admission is gated until the snapshot is persisted. */
ohlc_status ohlc_checkpoint_run(ohlc_db* db);
ohlc_status ohlc_checkpoint_background(ohlc_db* db);

#endif

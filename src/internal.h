/* SPDX-License-Identifier: Apache-2.0 */
#ifndef OHLC_INTERNAL_H
#define OHLC_INTERNAL_H

#include "ohlc/ohlc.h"

#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>

#define OHLC_BLOCK_BYTES 4096u
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
#define OHLC_COMMIT_QUEUE 64u
#define OHLC_COMMIT_GROUP 16u

typedef struct {
    _Atomic size_t used;
    size_t limit;
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

typedef struct {
    _Atomic uint32_t refs;
    ohlc_table_info info;
    ohlc_dictionary* dictionary;
    ohlc_time_node* times;
    uint64_t time_count;
    size_t time_page_count;
    ohlc_time_page** time_pages;
    ohlc_band_node* bands;
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
} ohlc_root;

typedef struct ohlc_retired_table {
    struct ohlc_retired_table* next;
    uint64_t sequence;
    uint32_t id;
} ohlc_retired_table;

typedef struct {
    uint32_t id;
    int data_fd;
    int meta_fd;
    uint64_t size;
    size_t meta_count;
    size_t meta_capacity;
    uint8_t** meta_pages;
    /* Only the checkpoint owner reads or writes persistence dirty flags. */
    bool dirty;
} ohlc_volume;

typedef struct {
    uint32_t id;
    int index_fd;
    uint64_t index_size;
    uint64_t dictionary_disk_count;
    uint64_t dictionary_disk_offset;
    size_t volume_count;
    ohlc_volume** volumes;
    uint64_t next_volume_id;
    bool index_dirty;
} ohlc_table_files;

typedef struct {
    uint32_t table;
    uint32_t volume;
    uint32_t band;
    uint32_t group;
    uint64_t offset;
    uint8_t tile;
    bool valid;
    bool visited;
    bool loading;
    uint32_t pins;
    ohlc_status status;
    uint8_t data[OHLC_BLOCK_BYTES];
} ohlc_cache_entry;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    size_t count;
    size_t hand;
    ohlc_cache_entry* entries;
} ohlc_cache;

typedef struct {
    int fd;
    uint64_t offset;
    size_t size;
    uint8_t* data;
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

struct ohlc_db {
    ohlc_options options;
    ohlc_allocator allocator;
    char* path;
    uint8_t uuid[16];
    int lock_fd;
    int directory_fd;
    int catalog_fd;
    int wal_fd;
    uint32_t wal_id;
    uint64_t wal_size;
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
    pthread_mutex_t root_mutex;
    pthread_mutex_t storage_mutex;
    ohlc_root* root;
    ohlc_table_files** files;
    size_t file_capacity;
    ohlc_retired_table* retired_tables;
    _Atomic bool reclaim_pending;
    ohlc_cache cache[16];
    ohlc_io_pool io;
    bool io_initialized;
    ohlc_commit_queue commits;
    bool commits_initialized;
    _Atomic uint32_t cursor_count;
    _Atomic uint64_t disk_read_bytes;
    _Atomic uint64_t disk_read_calls;
    _Atomic uint64_t cache_hits;
    _Atomic uint64_t cache_misses;
    _Atomic uint64_t wal_bytes;
    _Atomic uint64_t wal_syncs;
    _Atomic uint64_t data_bytes;
    _Atomic bool failed;
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

uint16_t ohlc_get_u16(const uint8_t* p);
uint32_t ohlc_get_u32(const uint8_t* p);
uint64_t ohlc_get_u64(const uint8_t* p);
void ohlc_put_u16(uint8_t* p, uint16_t value);
void ohlc_put_u32(uint8_t* p, uint32_t value);
void ohlc_put_u64(uint8_t* p, uint64_t value);
uint32_t ohlc_crc32c(uint32_t seed, const void* bytes, size_t size);
uint64_t ohlc_monotonic_ms(void);
unsigned int ohlc_popcount(uint16_t value);
void* ohlc_alloc(ohlc_allocator* allocator, size_t size);
void ohlc_free(ohlc_allocator* allocator, void* pointer);
ohlc_status ohlc_definition_validate(const ohlc_table_definition* definition);
ohlc_status ohlc_timezone_validate(const char* name);
size_t ohlc_definition_encode(uint8_t* output, const ohlc_table_info* info);
ohlc_status ohlc_definition_decode(const uint8_t* data, size_t size, ohlc_table_info* info);
ohlc_status ohlc_read_full(int fd, void* data, size_t size, uint64_t offset);
ohlc_status ohlc_write_full(int fd, const void* data, size_t size, uint64_t offset);
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
void ohlc_group_retain(ohlc_group* group);
void ohlc_group_release(ohlc_allocator* a, ohlc_group* group);
void ohlc_band_node_retain(ohlc_band_node* node);
void ohlc_band_node_release(ohlc_allocator* a, ohlc_band_node* node);
void ohlc_band_release(ohlc_allocator* a, ohlc_band* band);
ohlc_root* ohlc_root_new(ohlc_db* db);
ohlc_root* ohlc_root_copy(ohlc_db* db, const ohlc_root* root);
ohlc_root* ohlc_root_acquire(ohlc_db* db);
void ohlc_root_release(ohlc_db* db, ohlc_root* root);
void ohlc_root_publish(ohlc_db* db, ohlc_root* root);
const ohlc_table* ohlc_root_table(const ohlc_root* root, uint32_t id);
ohlc_status ohlc_root_edit_table(ohlc_db* db, ohlc_root* root, uint32_t id, ohlc_table** output);
ohlc_status ohlc_root_set_table(ohlc_db* db, ohlc_root* root, uint32_t id, ohlc_table* table);
ohlc_status ohlc_root_add_table(ohlc_db* db, ohlc_root* root, const ohlc_table_info* info);
ohlc_status ohlc_root_drop_table(ohlc_db* db, ohlc_root* root, uint32_t id);
ohlc_status ohlc_prepare_drop(ohlc_db* db, const ohlc_root* source, uint32_t id,
                              ohlc_root** candidate, ohlc_retired_table** retired);
ohlc_status ohlc_storage_find_retired(ohlc_db* db);
ohlc_status ohlc_storage_reclaim_tables(ohlc_db* db);
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
ohlc_status ohlc_storage_header(ohlc_db* db, int fd, uint32_t type, uint32_t table, uint32_t volume,
                                uint64_t generation, bool create);
ohlc_status ohlc_recover(ohlc_db* db, bool fresh);
ohlc_status ohlc_wal_append(ohlc_db* db, uint16_t type, uint32_t table, uint32_t count,
                            uint64_t seq, const uint8_t* payload, size_t size);
ohlc_status ohlc_wal_encode(ohlc_db* db, uint16_t type, uint32_t table, uint32_t count,
                            uint64_t seq, const uint8_t* payload, size_t size, uint8_t** output,
                            size_t* length);
ohlc_status ohlc_wal_write(ohlc_db* db, const uint8_t* frame, size_t size, uint64_t seq);
ohlc_status ohlc_wal_sync(ohlc_db* db);
/* The checkpoint mutex serializes persistence; writers remain independent. */
ohlc_status ohlc_checkpoint_run(ohlc_db* db);
ohlc_status ohlc_checkpoint_background(ohlc_db* db);

#endif

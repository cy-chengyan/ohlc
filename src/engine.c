/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    uint64_t order;
    const uint8_t* encoded;
    uint32_t time_code;
} ohlc_prepared_row;

static ohlc_status commits_init(ohlc_db* db);
static void commits_destroy(ohlc_db* db);

void ohlc_options_init(ohlc_options* options) {
    if (options == NULL) {
        return;
    }
    memset(options, 0, sizeof(*options));
    options->cache_bytes = 64u * 1024u * 1024u;
    options->memory_limit = (size_t)1024u * 1024u * 1024u;
    options->data_volume_bytes = UINT64_C(1) << 40;
    options->wal_segment_bytes = UINT64_C(1) << 30;
    options->max_tables = 1024;
    options->max_cursors = 256;
    options->max_query_ms = 60000;
    options->read_workers = 0;
}

static void database_destroy(ohlc_db* db) {
    if (db->commits_initialized) {
        commits_destroy(db);
    }
    if (db->io_initialized) {
        ohlc_io_destroy(db);
    }
    ohlc_root_release(db, db->root);
    ohlc_root_release(db, db->checkpoint_root);
    for (size_t i = 0; i < 16; i++) {
        ohlc_free(&db->allocator, db->cache[i].entries);
        pthread_cond_destroy(&db->cache[i].changed);
        pthread_mutex_destroy(&db->cache[i].mutex);
    }
    ohlc_storage_close(db);
    ohlc_free(&db->allocator, db->path);
    pthread_mutex_destroy(&db->writer);
    pthread_mutex_destroy(&db->checkpoint_mutex);
    pthread_mutex_destroy(&db->root_mutex);
    pthread_mutex_destroy(&db->storage_mutex);
    free(db);
}

ohlc_status ohlc_open(const char* path, const ohlc_options* options, ohlc_db** output) {
    if (output == NULL) {
        return OHLC_INVALID;
    }
    *output = NULL;
    if (path == NULL || path[0] == '\0') {
        return OHLC_INVALID;
    }
    ohlc_options defaults;
    ohlc_options_init(&defaults);
    if (options == NULL) {
        options = &defaults;
    }
    if (options->memory_limit < 1024u * 1024u || options->cache_bytes > options->memory_limit / 2 ||
        options->max_tables == 0 || options->max_tables > 1048576 || options->max_cursors == 0 ||
        options->max_query_ms == 0 || options->read_workers > OHLC_IO_WORKERS ||
        options->data_volume_bytes < 17u * OHLC_BLOCK_BYTES ||
        options->data_volume_bytes > INT64_MAX || options->wal_segment_bytes < OHLC_FRAME_LIMIT ||
        options->wal_segment_bytes > INT64_MAX) {
        return OHLC_INVALID;
    }
    ohlc_db* db = calloc(1, sizeof(*db));
    if (db == NULL) {
        return OHLC_LIMIT;
    }
    db->options = *options;
    atomic_init(&db->allocator.used, 0);
    atomic_init(&db->cursor_count, 0);
    atomic_init(&db->disk_read_bytes, 0);
    atomic_init(&db->disk_read_calls, 0);
    atomic_init(&db->cache_hits, 0);
    atomic_init(&db->cache_misses, 0);
    atomic_init(&db->wal_bytes, 0);
    atomic_init(&db->wal_syncs, 0);
    atomic_init(&db->data_bytes, 0);
    atomic_init(&db->failed, false);
    atomic_init(&db->reclaim_pending, false);
    atomic_init(&db->checkpoint_generation, 0);
    atomic_init(&db->checkpoint_sequences[0], 0);
    atomic_init(&db->checkpoint_sequences[1], 0);
    db->allocator.limit = options->memory_limit;
    db->lock_fd = -1;
    db->directory_fd = -1;
    db->catalog_fd = -1;
    db->wal_fd = -1;
    pthread_mutex_t* locks[20] = {&db->writer, &db->root_mutex, &db->storage_mutex,
                                  &db->checkpoint_mutex};
    for (size_t i = 0; i < 16; i++) {
        locks[i + 4] = &db->cache[i].mutex;
    }
    for (size_t i = 0; i < 20; i++) {
        if (pthread_mutex_init(locks[i], NULL) != 0) {
            for (size_t j = 0; j < i; j++) {
                pthread_mutex_destroy(locks[j]);
            }
            free(db);
            return OHLC_LIMIT;
        }
    }
    for (size_t i = 0; i < 16; i++) {
        if (pthread_cond_init(&db->cache[i].changed, NULL) != 0) {
            for (size_t j = 0; j < i; j++) {
                pthread_cond_destroy(&db->cache[j].changed);
            }
            for (size_t j = 0; j < 20; j++) {
                pthread_mutex_destroy(locks[j]);
            }
            free(db);
            return OHLC_LIMIT;
        }
    }
    ohlc_status io_status = ohlc_io_init(db);
    if (io_status != OHLC_OK) {
        database_destroy(db);
        return io_status;
    }
    db->io_initialized = true;
    db->path = ohlc_alloc(&db->allocator, strlen(path) + 1);
    db->files = ohlc_alloc(&db->allocator, (size_t)options->max_tables * sizeof(*db->files));
    db->file_capacity = options->max_tables;
    db->root = ohlc_root_new(db);
    if (db->path == NULL || db->files == NULL || db->root == NULL) {
        database_destroy(db);
        return OHLC_LIMIT;
    }
    memcpy(db->path, path, strlen(path) + 1);
    size_t entries_per_shard = options->cache_bytes / 16 / sizeof(ohlc_cache_entry);
    entries_per_shard -= entries_per_shard % 4;
    for (size_t i = 0; i < 16; i++) {
        db->cache[i].count = entries_per_shard;
        if (entries_per_shard != 0) {
            db->cache[i].entries =
                ohlc_alloc(&db->allocator, entries_per_shard * sizeof(ohlc_cache_entry));
            if (db->cache[i].entries == NULL) {
                database_destroy(db);
                return OHLC_LIMIT;
            }
        }
    }
    bool fresh = false;
    ohlc_status status = ohlc_storage_open(db, &fresh);
    if (status == OHLC_OK) {
        status = ohlc_recover(db, fresh);
    }
    if (status == OHLC_OK) {
        status = commits_init(db);
    }
    if (status != OHLC_OK) {
        database_destroy(db);
        return status;
    }
    db->commits_initialized = true;
    *output = db;
    return OHLC_OK;
}

ohlc_status ohlc_close(ohlc_db* db) {
    if (db == NULL) {
        return OHLC_INVALID;
    }
    if (atomic_load_explicit(&db->cursor_count, memory_order_acquire) != 0) {
        return OHLC_BUSY;
    }
    database_destroy(db);
    return OHLC_OK;
}

void ohlc_uuid(ohlc_db* db, uint8_t output[16]) {
    if (db != NULL && output != NULL) {
        memcpy(output, db->uuid, 16);
    }
}

void ohlc_get_stats(ohlc_db* db, ohlc_stats* output) {
    if (db == NULL || output == NULL) {
        return;
    }
    pthread_mutex_lock(&db->writer);
    ohlc_root* root = ohlc_root_acquire(db);
    memset(output, 0, sizeof(*output));
    output->commit_seq = root->seq;
    output->table_count = root->table_count;
    output->ticker_count = root->ticker_count;
    ohlc_root_release(db, root);
    output->checkpoint_seq = db->checkpoint_sequences[0] > db->checkpoint_sequences[1]
                                 ? db->checkpoint_sequences[0]
                                 : db->checkpoint_sequences[1];
    pthread_mutex_unlock(&db->writer);
    output->memory_bytes = atomic_load_explicit(&db->allocator.used, memory_order_relaxed);
    output->disk_read_bytes = atomic_load_explicit(&db->disk_read_bytes, memory_order_relaxed);
    output->disk_read_calls = atomic_load_explicit(&db->disk_read_calls, memory_order_relaxed);
    output->cache_hits = atomic_load_explicit(&db->cache_hits, memory_order_relaxed);
    output->cache_misses = atomic_load_explicit(&db->cache_misses, memory_order_relaxed);
    output->wal_bytes = atomic_load_explicit(&db->wal_bytes, memory_order_relaxed);
    output->data_bytes = atomic_load_explicit(&db->data_bytes, memory_order_relaxed);
}

ohlc_status ohlc_table_get(ohlc_db* db, uint32_t id, ohlc_table_info* output) {
    if (db == NULL || output == NULL) {
        return OHLC_INVALID;
    }
    ohlc_root* root = ohlc_root_acquire(db);
    const ohlc_table* table = ohlc_root_table(root, id);
    if (table != NULL) {
        *output = table->info;
    }
    ohlc_root_release(db, root);
    return table == NULL ? OHLC_NOT_FOUND : OHLC_OK;
}

ohlc_status ohlc_table_open(ohlc_db* db, const char* name, ohlc_table_info* output) {
    if (db == NULL || name == NULL || output == NULL) {
        return OHLC_INVALID;
    }
    ohlc_root* root = ohlc_root_acquire(db);
    ohlc_status status = OHLC_NOT_FOUND;
    for (uint64_t id = 1; id <= root->last_table_id; id++) {
        const ohlc_table* table = ohlc_root_table(root, (uint32_t)id);
        if (table != NULL && strcmp(table->info.name, name) == 0) {
            *output = table->info;
            status = OHLC_OK;
            break;
        }
    }
    ohlc_root_release(db, root);
    return status;
}

ohlc_status ohlc_table_list(ohlc_db* db, uint32_t start_id, ohlc_table_info* output,
                            size_t capacity, size_t* count, uint64_t* snapshot_seq) {
    if (db == NULL || output == NULL || count == NULL || snapshot_seq == NULL || capacity == 0 ||
        capacity > 256) {
        return OHLC_INVALID;
    }
    ohlc_root* root = ohlc_root_acquire(db);
    *count = 0;
    *snapshot_seq = root->seq;
    for (uint64_t id = start_id == 0 ? 1 : start_id; id <= root->last_table_id && *count < capacity;
         id++) {
        const ohlc_table* table = ohlc_root_table(root, (uint32_t)id);
        if (table != NULL) {
            output[(*count)++] = table->info;
        }
    }
    ohlc_root_release(db, root);
    return OHLC_OK;
}

ohlc_status ohlc_table_create(ohlc_db* db, const ohlc_table_definition* definition,
                              ohlc_table_info* output) {
    if (db == NULL || output == NULL) {
        return OHLC_INVALID;
    }
    ohlc_status status = ohlc_definition_validate(definition);
    if (status != OHLC_OK) {
        return status;
    }
    if (!ohlc_period_is_date(definition->period_unit)) {
        status = ohlc_timezone_validate(definition->timezone);
        if (status != OHLC_OK) {
            return status;
        }
    }
    pthread_mutex_lock(&db->writer);
    ohlc_root* candidate = NULL;
    if (db->failed) {
        status = OHLC_IO;
        goto cleanup;
    }
    ohlc_root* source = db->root;
    for (uint64_t id = 1; id <= source->last_table_id; id++) {
        const ohlc_table* table = ohlc_root_table(source, (uint32_t)id);
        if (table != NULL && strcmp(table->info.name, definition->name) == 0) {
            status = OHLC_ALREADY_EXISTS;
            goto cleanup;
        }
    }
    if (source->seq == UINT64_MAX || source->last_table_id == UINT32_MAX ||
        source->table_count >= db->options.max_tables) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    ohlc_table_info info = {0};
    info.id = source->last_table_id + 1;
    info.created_seq = source->seq + 1;
    info.period_unit = definition->period_unit;
    info.period_count = definition->period_count;
    memcpy(info.name, definition->name, strlen(definition->name) + 1);
    memcpy(info.timezone, definition->timezone, strlen(definition->timezone) + 1);
    memcpy(info.description, definition->description, strlen(definition->description) + 1);
    candidate = ohlc_root_copy(db, source);
    if (candidate == NULL) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    status = ohlc_root_add_table(db, candidate, &info);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    candidate->seq++;
    uint8_t payload[4440];
    size_t size = ohlc_definition_encode(payload, &info);
    status = ohlc_wal_append(db, 3, info.id, 1, candidate->seq, payload, size);
    if (status == OHLC_OK) {
        ohlc_root_publish(db, candidate);
        candidate = NULL;
        *output = info;
    }
cleanup:
    ohlc_root_release(db, candidate);
    pthread_mutex_unlock(&db->writer);
    return status;
}

ohlc_status ohlc_prepare_drop(ohlc_db* db, const ohlc_root* source, uint32_t id,
                              ohlc_root** candidate, ohlc_retired_table** retired) {
    *candidate = NULL;
    *retired = NULL;
    if (ohlc_root_table(source, id) == NULL) {
        return OHLC_NOT_FOUND;
    }
    if (source->seq == UINT64_MAX) {
        return OHLC_LIMIT;
    }
    ohlc_root* root = ohlc_root_copy(db, source);
    ohlc_retired_table* entry = ohlc_alloc(&db->allocator, sizeof(*entry));
    ohlc_status status = root == NULL || entry == NULL ? OHLC_LIMIT : OHLC_OK;
    if (status == OHLC_OK) {
        status = ohlc_root_drop_table(db, root, id);
    }
    if (status != OHLC_OK) {
        ohlc_root_release(db, root);
        ohlc_free(&db->allocator, entry);
        return status;
    }
    root->seq++;
    entry->id = id;
    entry->sequence = root->seq;
    *candidate = root;
    *retired = entry;
    return OHLC_OK;
}

ohlc_status ohlc_table_drop(ohlc_db* db, uint32_t table_id, uint64_t* commit_seq) {
    if (db == NULL || commit_seq == NULL || table_id == 0) {
        return OHLC_INVALID;
    }
    pthread_mutex_lock(&db->writer);
    ohlc_root* candidate = NULL;
    ohlc_retired_table* retired = NULL;
    ohlc_status status =
        db->failed ? OHLC_IO : ohlc_prepare_drop(db, db->root, table_id, &candidate, &retired);
    if (status == OHLC_OK) {
        status = ohlc_wal_append(db, 4, table_id, 1, candidate->seq, (const uint8_t*)"", 0);
    }
    if (status == OHLC_OK) {
        *commit_seq = candidate->seq;
        ohlc_root_publish(db, candidate);
        candidate = NULL;
        retired->next = db->retired_tables;
        db->retired_tables = retired;
        atomic_store(&db->reclaim_pending, true);
        retired = NULL;
    }
    ohlc_root_release(db, candidate);
    ohlc_free(&db->allocator, retired);
    pthread_mutex_unlock(&db->writer);
    return status;
}

static int compare_prepared(const void* left, const void* right) {
    const ohlc_prepared_row* a = left;
    const ohlc_prepared_row* b = right;
    return (a->order > b->order) - (a->order < b->order);
}

static ohlc_status hot_group_edit(ohlc_db* db, ohlc_group* entry, ohlc_hot_group** output) {
    ohlc_hot_group* source =
        entry->flags == OHLC_HOT_GROUP ? (ohlc_hot_group*)(uintptr_t)entry->base : NULL;
    if (source != NULL && atomic_load_explicit(&source->refs, memory_order_acquire) == 1) {
        *output = source;
        return OHLC_OK;
    }
    ohlc_hot_group* hot = ohlc_alloc(&db->allocator, sizeof(*hot));
    if (hot == NULL) {
        return OHLC_LIMIT;
    }
    atomic_init(&hot->refs, 1);
    if (source == NULL) {
        hot->backing = *entry;
    } else {
        hot->backing = source->backing;
        for (size_t i = 0; i < 16; i++) {
            hot->blocks[i] = source->blocks[i];
            if (hot->blocks[i] != NULL) {
                atomic_fetch_add_explicit(&hot->blocks[i]->refs, 1, memory_order_relaxed);
            }
        }
        ohlc_group_release(&db->allocator, entry);
    }
    entry->flags = OHLC_HOT_GROUP;
    entry->base = (uint64_t)(uintptr_t)hot;
    entry->volume = 0;
    *output = hot;
    return OHLC_OK;
}

static ohlc_status block_edit(ohlc_db* db, uint32_t table, uint32_t band, uint32_t group,
                              ohlc_group* entry, ohlc_hot_group* hot, uint8_t tile,
                              ohlc_block** output) {
    ohlc_block* source = hot->blocks[tile];
    if (source != NULL && atomic_load_explicit(&source->refs, memory_order_acquire) == 1) {
        *output = source;
        return OHLC_OK;
    }
    ohlc_block* block = ohlc_alloc(&db->allocator, sizeof(*block));
    if (block == NULL) {
        return OHLC_LIMIT;
    }
    atomic_init(&block->refs, 1);
    if ((entry->mask & (1u << tile)) != 0) {
        ohlc_status status = ohlc_storage_tile(db, table, band, group, entry, tile, block->data);
        const uint8_t* presence = ohlc_storage_presence(db, table, entry, tile);
        if (status != OHLC_OK || presence == NULL) {
            ohlc_free(&db->allocator, block);
            return status == OHLC_OK ? OHLC_CORRUPT : status;
        }
        memcpy(block->presence, presence, 16);
    }
    if (source != NULL && atomic_fetch_sub_explicit(&source->refs, 1, memory_order_acq_rel) == 1) {
        ohlc_free(&db->allocator, source);
    }
    hot->blocks[tile] = block;
    entry->mask = (uint16_t)(entry->mask | (1u << tile));
    *output = block;
    return OHLC_OK;
}

static bool ordered_cross(const uint8_t* rows, size_t count) {
    uint32_t key = ohlc_get_u32(rows + 4);
    uint32_t previous = ohlc_get_u32(rows);
    for (size_t i = 1; i < count; i++) {
        const uint8_t* row = rows + i * OHLC_WRITE_BYTES;
        uint32_t ticker = ohlc_get_u32(row);
        if (ohlc_get_u32(row + 4) != key || ticker <= previous) {
            return false;
        }
        previous = ticker;
    }
    return true;
}

static size_t rectangle_width(const uint8_t* rows, size_t count) {
    uint32_t first_ticker = ohlc_get_u32(rows);
    uint32_t key = ohlc_get_u32(rows + 4);
    size_t width = 0;
    while (width < count && ohlc_get_u32(rows + width * OHLC_WRITE_BYTES + 4) == key) {
        width++;
    }
    if (width == count || count % width != 0 || width - 1u > UINT32_MAX - first_ticker) {
        return 0;
    }
    for (size_t offset = 0; offset < count; offset += width) {
        uint32_t next_key = ohlc_get_u32(rows + offset * OHLC_WRITE_BYTES + 4);
        if (offset != 0 && next_key <= key) {
            return 0;
        }
        key = next_key;
        for (size_t column = 0; column < width; column++) {
            const uint8_t* row = rows + (offset + column) * OHLC_WRITE_BYTES;
            if (ohlc_get_u32(row) != first_ticker + (uint32_t)column ||
                ohlc_get_u32(row + 4) != key) {
                return 0;
            }
        }
    }
    return width;
}

static ohlc_status prepare_rectangle(ohlc_db* db, ohlc_root* candidate, uint32_t table_id,
                                     const uint8_t* rows, size_t count, size_t width) {
    uint32_t first_ticker = ohlc_get_u32(rows);
    uint64_t ticker_end = (uint64_t)first_ticker + width;
    if (ticker_end > ohlc_dictionary_count(ohlc_root_table(candidate, table_id))) {
        return OHLC_NOT_FOUND;
    }
    size_t times = count / width;
    uint32_t* codes = ohlc_alloc(&db->allocator, times * sizeof(*codes));
    if (codes == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_table* table = NULL;
    ohlc_status status = ohlc_root_edit_table(db, candidate, table_id, &table);
    for (size_t t = 0; t < times && status == OHLC_OK; t++) {
        uint32_t key = ohlc_get_u32(rows + t * width * OHLC_WRITE_BYTES + 4);
        status = ohlc_table_time_add(db, table, key, &codes[t]);
    }
    /* Build one stock group at a time. The input remains borrowed in its
     * original order; only one code per distinct timestamp is allocated. */
    for (uint64_t first = first_ticker; first < ticker_end && status == OHLC_OK;) {
        uint32_t group = (uint32_t)(first / 16u);
        uint64_t end = ((uint64_t)group + 1u) * 16u;
        if (end > ticker_end) {
            end = ticker_end;
        }
        uint32_t previous_band = UINT32_MAX;
        uint8_t previous_tile = UINT8_MAX;
        ohlc_group* entry = NULL;
        ohlc_hot_group* hot = NULL;
        ohlc_block* block = NULL;
        for (size_t t = 0; t < times && status == OHLC_OK; t++) {
            uint32_t band = codes[t] / 128u;
            uint8_t tile = (uint8_t)((codes[t] % 128u) / 8u);
            if (band != previous_band) {
                status = ohlc_group_edit(db, table, band, group, &entry);
                if (status == OHLC_OK) {
                    status = hot_group_edit(db, entry, &hot);
                }
                previous_band = band;
                previous_tile = UINT8_MAX;
            }
            if (status == OHLC_OK && tile != previous_tile) {
                status = block_edit(db, table_id, band, group, entry, hot, tile, &block);
                previous_tile = tile;
            }
            if (status != OHLC_OK) {
                break;
            }
            uint32_t column = codes[t] % 8u;
            for (uint64_t ticker = first; ticker < end; ticker++) {
                size_t input = t * width + (size_t)(ticker - first_ticker);
                size_t stock = (size_t)(ticker % 16u);
                size_t slot = stock * 8u + column;
                memcpy(block->data + slot * OHLC_ROW_BYTES, rows + input * OHLC_WRITE_BYTES + 8,
                       OHLC_ROW_BYTES);
                block->presence[stock] = (uint8_t)(block->presence[stock] | (1u << column));
            }
        }
        first = end;
    }
    ohlc_free(&db->allocator, codes);
    return status;
}

static ohlc_status prepare_cross(ohlc_db* db, ohlc_root* candidate, uint32_t table_id,
                                 const uint8_t* rows, size_t count) {
    uint32_t last_ticker = ohlc_get_u32(rows + (count - 1) * OHLC_WRITE_BYTES);
    if (last_ticker >= ohlc_dictionary_count(ohlc_root_table(candidate, table_id))) {
        return OHLC_NOT_FOUND;
    }
    ohlc_table* table = NULL;
    ohlc_status status = ohlc_root_edit_table(db, candidate, table_id, &table);
    uint32_t code = 0;
    if (status == OHLC_OK) {
        status = ohlc_table_time_add(db, table, ohlc_get_u32(rows + 4), &code);
    }
    if (status != OHLC_OK) {
        return status;
    }
    uint32_t band = code / 128u;
    uint8_t tile = (uint8_t)((code % 128u) / 8u);
    uint32_t column = code % 8u;
    uint32_t previous_group = UINT32_MAX;
    ohlc_block* block = NULL;
    for (size_t i = 0; i < count; i++) {
        const uint8_t* row = rows + i * OHLC_WRITE_BYTES;
        uint32_t ticker = ohlc_get_u32(row);
        uint32_t group = ticker / 16u;
        if (group != previous_group) {
            ohlc_group* entry = NULL;
            ohlc_hot_group* hot = NULL;
            status = ohlc_group_edit(db, table, band, group, &entry);
            if (status == OHLC_OK) {
                status = hot_group_edit(db, entry, &hot);
            }
            if (status == OHLC_OK) {
                status = block_edit(db, table_id, band, group, entry, hot, tile, &block);
            }
            if (status != OHLC_OK) {
                return status;
            }
            previous_group = group;
        }
        uint32_t stock = ticker % 16u;
        memcpy(block->data + (stock * 8u + column) * OHLC_ROW_BYTES, row + 8, OHLC_ROW_BYTES);
        block->presence[stock] = (uint8_t)(block->presence[stock] | (1u << column));
    }
    return OHLC_OK;
}

ohlc_status ohlc_prepare_write(ohlc_db* db, const ohlc_root* source, uint32_t table_id,
                               const uint8_t* rows, size_t count, ohlc_root** output) {
    *output = NULL;
    if (ohlc_root_table(source, table_id) == NULL) {
        return OHLC_NOT_FOUND;
    }
    ohlc_prepared_row* plan = NULL;
    ohlc_root* candidate = ohlc_root_copy(db, source);
    ohlc_status status = OHLC_LIMIT;
    if (candidate == NULL) {
        goto cleanup;
    }
    if (ordered_cross(rows, count)) {
        status = prepare_cross(db, candidate, table_id, rows, count);
        if (status == OHLC_OK) {
            candidate->seq = source->seq + 1;
            *output = candidate;
            candidate = NULL;
        }
        goto cleanup;
    }
    size_t width = rectangle_width(rows, count);
    if (width != 0) {
        status = prepare_rectangle(db, candidate, table_id, rows, count, width);
        if (status == OHLC_OK) {
            candidate->seq = source->seq + 1;
            *output = candidate;
            candidate = NULL;
        }
        goto cleanup;
    }
    plan = ohlc_alloc(&db->allocator, count * sizeof(*plan));
    if (plan == NULL) {
        goto cleanup;
    }
    for (size_t i = 0; i < count; i++) {
        const uint8_t* row = rows + i * OHLC_WRITE_BYTES;
        uint32_t ticker = ohlc_get_u32(row);
        if (ticker >= ohlc_dictionary_count(ohlc_root_table(source, table_id))) {
            status = OHLC_NOT_FOUND;
            goto cleanup;
        }
        plan[i].encoded = row;
        plan[i].order = ((uint64_t)ohlc_get_u32(row + 4) << 32) | ticker;
    }
    qsort(plan, count, sizeof(*plan), compare_prepared);
    for (size_t i = 1; i < count; i++) {
        if (plan[i - 1].order == plan[i].order) {
            status = OHLC_INVALID;
            goto cleanup;
        }
    }
    ohlc_table* table = NULL;
    status = ohlc_root_edit_table(db, candidate, table_id, &table);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    uint32_t last_key = 0;
    uint32_t time_code = 0;
    for (size_t i = 0; i < count; i++) {
        uint32_t key = ohlc_get_u32(plan[i].encoded + 4);
        if (i == 0 || key != last_key) {
            status = ohlc_table_time_add(db, table, key, &time_code);
            if (status != OHLC_OK) {
                goto cleanup;
            }
            last_key = key;
        }
        plan[i].time_code = time_code;
        uint32_t ticker = ohlc_get_u32(plan[i].encoded);
        plan[i].order = ((uint64_t)(time_code / 128u) << 32) | ((uint64_t)(ticker / 16u) << 4) |
                        ((time_code % 128u) / 8u);
    }
    qsort(plan, count, sizeof(*plan), compare_prepared);
    ohlc_group* entry = NULL;
    ohlc_hot_group* hot = NULL;
    uint64_t previous_group = UINT64_MAX;
    for (size_t i = 0; i < count; i++) {
        uint32_t ticker = ohlc_get_u32(plan[i].encoded);
        uint32_t group = ticker / 16u;
        uint32_t band = plan[i].time_code / 128u;
        uint8_t tile = (uint8_t)((plan[i].time_code % 128u) / 8u);
        uint64_t identity = plan[i].order >> 4;
        if (hot == NULL || identity != previous_group) {
            status = ohlc_group_edit(db, table, band, group, &entry);
            if (status == OHLC_OK) {
                status = hot_group_edit(db, entry, &hot);
            }
            if (status != OHLC_OK) {
                goto cleanup;
            }
            previous_group = identity;
        }
        ohlc_block* block = NULL;
        status = block_edit(db, table_id, band, group, entry, hot, tile, &block);
        if (status != OHLC_OK) {
            goto cleanup;
        }
        uint32_t row = ticker % 16u;
        uint32_t column = plan[i].time_code % 8u;
        size_t slot = row * 8u + column;
        memcpy(block->data + slot * OHLC_ROW_BYTES, plan[i].encoded + 8, OHLC_ROW_BYTES);
        block->presence[row] = (uint8_t)(block->presence[row] | (1u << column));
    }
    candidate->seq = source->seq + 1;
    *output = candidate;
    candidate = NULL;
    status = OHLC_OK;
cleanup:
    ohlc_free(&db->allocator, plan);
    ohlc_root_release(db, candidate);
    return status;
}

static void commit_group(ohlc_db* db, ohlc_write_request** requests, size_t count) {
    pthread_mutex_lock(&db->writer);
    ohlc_root* candidate = NULL;
    for (size_t i = 0; i < count; i++) {
        ohlc_write_request* request = requests[i];
        const ohlc_root* base = candidate == NULL ? db->root : candidate;
        request->status = OHLC_IO;
        if (db->failed) {
            continue;
        }
        if (base->seq == UINT64_MAX) {
            request->status = OHLC_LIMIT;
            continue;
        }
        ohlc_root* next = NULL;
        uint8_t* frame = NULL;
        size_t length = 0;
        if (request->named) {
            request->status = ohlc_prepare_named(db, base, request->table, request->rows,
                                                 request->size, request->count, &next);
        } else {
            request->status =
                ohlc_prepare_write(db, base, request->table, request->rows, request->count, &next);
        }
        if (request->status == OHLC_OK) {
            request->status = ohlc_wal_encode(db, request->named ? 5 : 2, request->table,
                                              (uint32_t)request->count, next->seq, request->rows,
                                              request->size, &frame, &length);
        }
        if (request->status == OHLC_OK) {
            request->status = ohlc_wal_write(db, frame, length, next->seq);
        }
        if (request->status == OHLC_OK) {
            request->sequence = next->seq;
            ohlc_root_release(db, candidate);
            candidate = next;
            next = NULL;
        }
        ohlc_free(&db->allocator, frame);
        ohlc_root_release(db, next);
    }
    if (candidate != NULL) {
        ohlc_status status = ohlc_wal_sync(db);
        if (status == OHLC_OK) {
            ohlc_root_publish(db, candidate);
            candidate = NULL;
        } else {
            for (size_t i = 0; i < count; i++) {
                if (requests[i]->status == OHLC_OK) {
                    requests[i]->status = OHLC_OUTCOME_UNKNOWN;
                }
            }
        }
    }
    pthread_mutex_unlock(&db->writer);
    ohlc_root_release(db, candidate);
}

static void* commit_worker(void* argument) {
    ohlc_db* db = argument;
    ohlc_commit_queue* queue = &db->commits;
    pthread_mutex_lock(&queue->mutex);
    for (;;) {
        while (queue->active != 0 || (queue->count == 0 && !queue->stopping)) {
            pthread_cond_wait(&queue->changed, &queue->mutex);
        }
        if (queue->count == 0 && queue->stopping) {
            break;
        }
        ohlc_write_request* requests[OHLC_COMMIT_GROUP];
        size_t count = 0;
        size_t bytes = 0;
        while (queue->count != 0 && count < OHLC_COMMIT_GROUP) {
            ohlc_write_request* request = queue->queue[queue->head];
            size_t size = request->size + 96u;
            if (count != 0 && bytes + size > OHLC_FRAME_LIMIT) {
                break;
            }
            requests[count++] = request;
            bytes += size;
            queue->head = (queue->head + 1u) % OHLC_COMMIT_QUEUE;
            queue->count--;
        }
        queue->active = count;
        pthread_cond_broadcast(&queue->changed);
        pthread_mutex_unlock(&queue->mutex);
        commit_group(db, requests, count);
        pthread_mutex_lock(&queue->mutex);
        queue->active = 0;
        for (size_t i = 0; i < count; i++) {
            requests[i]->done = true;
        }
        pthread_cond_broadcast(&queue->changed);
    }
    pthread_mutex_unlock(&queue->mutex);
    return NULL;
}

static ohlc_status commits_init(ohlc_db* db) {
    ohlc_commit_queue* queue = &db->commits;
    if (pthread_mutex_init(&queue->mutex, NULL) != 0) {
        return OHLC_LIMIT;
    }
    if (pthread_cond_init(&queue->changed, NULL) != 0) {
        pthread_mutex_destroy(&queue->mutex);
        return OHLC_LIMIT;
    }
    if (pthread_create(&queue->worker, NULL, commit_worker, db) != 0) {
        pthread_cond_destroy(&queue->changed);
        pthread_mutex_destroy(&queue->mutex);
        return OHLC_LIMIT;
    }
    return OHLC_OK;
}

static void commits_destroy(ohlc_db* db) {
    ohlc_commit_queue* queue = &db->commits;
    pthread_mutex_lock(&queue->mutex);
    queue->stopping = true;
    pthread_cond_broadcast(&queue->changed);
    pthread_mutex_unlock(&queue->mutex);
    pthread_join(queue->worker, NULL);
    pthread_cond_destroy(&queue->changed);
    pthread_mutex_destroy(&queue->mutex);
}

static ohlc_status submit_write(ohlc_db* db, uint32_t table_id, const void* rows, size_t size,
                                size_t count, bool named, uint64_t* commit_seq) {
    if (db == NULL || rows == NULL || commit_seq == NULL || count == 0 ||
        count > OHLC_MAX_BATCH_ROWS) {
        return OHLC_INVALID;
    }
    ohlc_write_request request = {
        .table = table_id, .rows = rows, .size = size, .named = named, .count = count};
    ohlc_commit_queue* queue = &db->commits;
    pthread_mutex_lock(&queue->mutex);
    if (queue->active == 0 && queue->count == 0) {
        /* Reserve the same execution slot used by the worker. New arrivals
         * must queue until this caller has finished publishing its result. */
        queue->active = 1;
        pthread_mutex_unlock(&queue->mutex);
        ohlc_write_request* requests[] = {&request};
        commit_group(db, requests, 1);
        pthread_mutex_lock(&queue->mutex);
        queue->active = 0;
        if (queue->count != 0 || queue->stopping) {
            pthread_cond_broadcast(&queue->changed);
        }
    } else {
        if (queue->count == OHLC_COMMIT_QUEUE) {
            pthread_mutex_unlock(&queue->mutex);
            return OHLC_BUSY;
        }
        queue->queue[(queue->head + queue->count) % OHLC_COMMIT_QUEUE] = &request;
        queue->count++;
        pthread_cond_broadcast(&queue->changed);
        while (!request.done) {
            pthread_cond_wait(&queue->changed, &queue->mutex);
        }
    }
    pthread_mutex_unlock(&queue->mutex);
    if (request.status == OHLC_OK) {
        *commit_seq = request.sequence;
    }
    return request.status;
}

ohlc_status ohlc_write(ohlc_db* db, uint32_t table_id, const void* rows, size_t count,
                       uint64_t* commit_seq) {
    return submit_write(db, table_id, rows, count * OHLC_WRITE_BYTES, count, false, commit_seq);
}

ohlc_status ohlc_submit_named(ohlc_db* db, uint32_t table_id, const void* payload, size_t size,
                              size_t count, uint64_t* sequence) {
    return submit_write(db, table_id, payload, size, count, true, sequence);
}

ohlc_status ohlc_write_named(ohlc_db* db, uint32_t table_id, const ohlc_bytes* tickers,
                             size_t ticker_count, const void* rows, size_t count,
                             uint64_t* commit_seq) {
    if (db == NULL || commit_seq == NULL) {
        return OHLC_INVALID;
    }
    uint8_t* payload = NULL;
    size_t size = 0;
    ohlc_status status =
        ohlc_named_pack(&db->allocator, tickers, ticker_count, rows, count, &payload, &size);
    if (status == OHLC_OK) {
        status = submit_write(db, table_id, payload, size, count, true, commit_seq);
    }
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status cursor_new(ohlc_db* db, uint32_t table_id, ohlc_cursor** output) {
    if (output == NULL) {
        return OHLC_INVALID;
    }
    *output = NULL;
    if (db == NULL) {
        return OHLC_INVALID;
    }
    uint32_t count = atomic_load_explicit(&db->cursor_count, memory_order_relaxed);
    do {
        if (count >= db->options.max_cursors) {
            return OHLC_BUSY;
        }
    } while (!atomic_compare_exchange_weak_explicit(&db->cursor_count, &count, count + 1,
                                                    memory_order_acq_rel, memory_order_relaxed));
    ohlc_cursor* cursor = ohlc_alloc(&db->allocator, sizeof(*cursor));
    if (cursor == NULL) {
        atomic_fetch_sub_explicit(&db->cursor_count, 1, memory_order_release);
        return OHLC_LIMIT;
    }
    cursor->db = db;
    cursor->root = ohlc_root_acquire(db);
    cursor->table = ohlc_root_table(cursor->root, table_id);
    if (cursor->table == NULL) {
        ohlc_cursor_close(cursor);
        return OHLC_NOT_FOUND;
    }
    cursor->deadline_ms = ohlc_monotonic_ms() + db->options.max_query_ms;
    *output = cursor;
    return OHLC_OK;
}

ohlc_status ohlc_series(ohlc_db* db, uint32_t table_id, uint32_t ticker_code, uint32_t start,
                        uint64_t end, ohlc_cursor** output) {
    if (output == NULL) {
        return OHLC_INVALID;
    }
    *output = NULL;
    if (end > UINT32_MAX || start > end) {
        return OHLC_INVALID;
    }
    ohlc_cursor* cursor = NULL;
    ohlc_status status = cursor_new(db, table_id, &cursor);
    if (status != OHLC_OK) {
        return status;
    }
    if (ticker_code >= ohlc_dictionary_count(cursor->table)) {
        ohlc_cursor_close(cursor);
        return OHLC_NOT_FOUND;
    }
    cursor->kind = 1;
    cursor->ticker = ticker_code;
    /* Keep the internal scan half open, including the largest uint32 key. */
    cursor->end = end + 1;
    ohlc_time_seek(cursor->table->times, start, &cursor->times);
    *output = cursor;
    return OHLC_OK;
}

ohlc_status ohlc_cross(ohlc_db* db, uint32_t table_id, uint32_t time_key, ohlc_cursor** output) {
    ohlc_cursor* cursor = NULL;
    ohlc_status status = cursor_new(db, table_id, output);
    if (status != OHLC_OK) {
        return status;
    }
    cursor = *output;
    cursor->kind = 2;
    cursor->done = !ohlc_time_find(cursor->table->times, time_key, &cursor->cross_code);
    if (!cursor->done) {
        cursor->cross_band = ohlc_band_find(cursor->table, cursor->cross_code / 128u);
        cursor->done = cursor->cross_band == NULL;
    }
    return OHLC_OK;
}

uint64_t ohlc_cursor_sequence(const ohlc_cursor* cursor) {
    return cursor == NULL ? 0 : cursor->root->seq;
}

static int compare_ticker_codes(const void* left, const void* right) {
    uint32_t a = *(const uint32_t*)left;
    uint32_t b = *(const uint32_t*)right;
    return (a > b) - (a < b);
}

ohlc_status ohlc_cross_tickers(ohlc_db* db, uint32_t table_id, uint32_t time_key,
                               const ohlc_bytes* tickers, size_t count, ohlc_cursor** output) {
    if (output == NULL) {
        return OHLC_INVALID;
    }
    *output = NULL;
    if (count != 0 && tickers == NULL) {
        return OHLC_INVALID;
    }
    if (count > OHLC_MAX_QUERY_TICKERS) {
        return OHLC_LIMIT;
    }
    for (size_t i = 0; i < count; i++) {
        if (tickers[i].data == NULL || tickers[i].size == 0 || tickers[i].size > 4096) {
            return OHLC_INVALID;
        }
    }
    ohlc_cursor* cursor = NULL;
    ohlc_status status = ohlc_cross(db, table_id, time_key, &cursor);
    if (status != OHLC_OK) {
        return status;
    }
    cursor->kind = 3;
    if (count != 0 && !cursor->done) {
        cursor->selected_codes = ohlc_alloc(&db->allocator, count * sizeof(uint32_t));
        if (cursor->selected_codes == NULL) {
            ohlc_cursor_close(cursor);
            return OHLC_LIMIT;
        }
        for (size_t i = 0; i < count; i++) {
            const ohlc_ticker_entry* entry =
                ohlc_dictionary_find(cursor->table->dictionary, tickers[i]);
            if (entry != NULL) {
                cursor->selected_codes[cursor->selected_count++] = entry->code;
            }
        }
        qsort(cursor->selected_codes, cursor->selected_count, sizeof(uint32_t),
              compare_ticker_codes);
        size_t unique = 0;
        for (size_t i = 0; i < cursor->selected_count; i++) {
            uint32_t code = cursor->selected_codes[i];
            if (unique == 0 || cursor->selected_codes[unique - 1] != code) {
                cursor->selected_codes[unique++] = code;
            }
        }
        cursor->selected_count = unique;
    }
    cursor->done = cursor->done || cursor->selected_count == 0;
    *output = cursor;
    return OHLC_OK;
}

static void cursor_release_window(ohlc_cursor* cursor) {
    ohlc_storage_release(cursor->views, cursor->view_count);
    cursor->view_count = 0;
    cursor->planned = 0;
    cursor->emitted = 0;
}

static size_t cursor_find_view(const ohlc_cursor* cursor, uint32_t band, uint32_t group,
                               uint8_t tile) {
    for (size_t i = 0; i < cursor->view_count; i++) {
        const ohlc_tile_view* view = &cursor->views[i];
        if (view->band == band && view->group == group && view->tile == tile) {
            return i;
        }
    }
    return cursor->view_count;
}

static void cursor_add_view(ohlc_cursor* cursor, const ohlc_group* entry, uint32_t band,
                            uint32_t group, uint8_t tile, const uint8_t* presence) {
    size_t index = cursor->view_count++;
    cursor->views[index] = (ohlc_tile_view){.entry = entry,
                                            .band = band,
                                            .group = group,
                                            .tile = tile,
                                            .presence = presence,
                                            .buffer = cursor->buffers + index * OHLC_BLOCK_BYTES};
}

static ohlc_status plan_series(ohlc_cursor* cursor) {
    uint32_t group = cursor->ticker / 16u;
    uint32_t row = cursor->ticker % 16u;
    size_t scanned = 0;
    while (cursor->planned < OHLC_RESULT_WINDOW) {
        const uint32_t* keys = NULL;
        const uint32_t* codes = NULL;
        size_t span = ohlc_time_span(&cursor->times, &keys, &codes);
        if (span == 0 || keys[0] >= cursor->end) {
            cursor->done = true;
            break;
        }
        uint32_t code = codes[0];
        uint32_t band = code / 128u;
        uint8_t tile = (uint8_t)((code % 128u) / 8u);
        size_t run = 1;
        uint8_t columns = (uint8_t)(1u << (code % 8u));
        while (run < span && keys[run] < cursor->end && codes[run] / 8u == code / 8u) {
            columns = (uint8_t)(columns | (1u << (codes[run] % 8u)));
            run++;
        }
        size_t index = cursor_find_view(cursor, band, group, tile);
        const uint8_t* presence = NULL;
        if (index < cursor->view_count) {
            presence = cursor->views[index].presence;
        } else {
            const ohlc_group* entry = ohlc_group_find(cursor->table, band, group);
            if (entry != NULL && (entry->mask & (1u << tile)) != 0) {
                presence = ohlc_storage_presence(cursor->db, cursor->table->info.id, entry, tile);
                if (presence == NULL) {
                    return OHLC_CORRUPT;
                }
                if ((presence[row] & columns) != 0) {
                    if (cursor->view_count == OHLC_READ_WINDOW) {
                        break;
                    }
                    cursor_add_view(cursor, entry, band, group, tile, presence);
                }
            }
        }
        size_t consumed = 0;
        while (consumed < run && cursor->planned < OHLC_RESULT_WINDOW) {
            uint32_t column = codes[consumed] % 8u;
            if (presence != NULL && (presence[row] & (1u << column)) != 0) {
                cursor->plan[cursor->planned++] =
                    (ohlc_result_plan){.key = keys[consumed],
                                       .view = (uint8_t)index,
                                       .slot = (uint8_t)(row * 8u + column)};
            }
            consumed++;
        }
        ohlc_time_advance(&cursor->times, consumed);
        scanned += consumed;
        if (scanned >= 1024u) {
            if (ohlc_monotonic_ms() > cursor->deadline_ms) {
                return OHLC_CANCELLED;
            }
            scanned = 0;
        }
    }
    return OHLC_OK;
}

static ohlc_status plan_cross(ohlc_cursor* cursor) {
    uint32_t band = cursor->cross_code / 128u;
    uint8_t tile = (uint8_t)((cursor->cross_code % 128u) / 8u);
    uint32_t column = cursor->cross_code % 8u;
    size_t scanned = 0;
    while (cursor->view_count < OHLC_READ_WINDOW &&
           cursor->next_code < ohlc_dictionary_count(cursor->table)) {
        uint64_t first = cursor->next_code;
        uint32_t group = (uint32_t)(first / 16u);
        cursor->next_code += 16;
        const ohlc_group* entry = NULL;
        const ohlc_band* directory = cursor->cross_band;
        if (group / 256u < directory->page_count) {
            const ohlc_group_page* page = directory->pages[group / 256u];
            if (page != NULL) {
                entry = &page->groups[group % 256u];
            }
        }
        if (entry != NULL && (entry->mask & (1u << tile)) != 0) {
            const uint8_t* presence =
                ohlc_storage_presence(cursor->db, cursor->table->info.id, entry, tile);
            if (presence == NULL) {
                return OHLC_CORRUPT;
            }
            size_t index = cursor->view_count;
            size_t before = cursor->planned;
            for (uint32_t row = 0; row < 16 && first + row < ohlc_dictionary_count(cursor->table);
                 row++) {
                if ((presence[row] & (1u << column)) != 0) {
                    cursor->plan[cursor->planned++] =
                        (ohlc_result_plan){.key = (uint32_t)(first + row),
                                           .view = (uint8_t)index,
                                           .slot = (uint8_t)(row * 8u + column)};
                }
            }
            if (before != cursor->planned) {
                cursor_add_view(cursor, entry, band, group, tile, presence);
            }
        }
        if (++scanned % 64u == 0 && ohlc_monotonic_ms() > cursor->deadline_ms) {
            return OHLC_CANCELLED;
        }
    }
    cursor->done = cursor->next_code >= ohlc_dictionary_count(cursor->table);
    return OHLC_OK;
}

static ohlc_status plan_selected_cross(ohlc_cursor* cursor) {
    uint32_t band = cursor->cross_code / 128u;
    uint8_t tile = (uint8_t)((cursor->cross_code % 128u) / 8u);
    uint32_t column = cursor->cross_code % 8u;
    size_t scanned = 0;
    while (cursor->view_count < OHLC_READ_WINDOW &&
           cursor->selected_position < cursor->selected_count) {
        size_t first = cursor->selected_position;
        uint32_t group = cursor->selected_codes[first] / 16u;
        do {
            cursor->selected_position++;
        } while (cursor->selected_position < cursor->selected_count &&
                 cursor->selected_codes[cursor->selected_position] / 16u == group);
        const ohlc_group* entry = ohlc_group_find(cursor->table, band, group);
        if (entry != NULL && (entry->mask & (1u << tile)) != 0) {
            const uint8_t* presence =
                ohlc_storage_presence(cursor->db, cursor->table->info.id, entry, tile);
            if (presence == NULL) {
                return OHLC_CORRUPT;
            }
            size_t index = cursor->view_count;
            size_t before = cursor->planned;
            for (size_t i = first; i < cursor->selected_position; i++) {
                uint32_t code = cursor->selected_codes[i];
                uint32_t row = code % 16u;
                if ((presence[row] & (1u << column)) != 0) {
                    cursor->plan[cursor->planned++] = (ohlc_result_plan){
                        .key = code, .view = (uint8_t)index, .slot = (uint8_t)(row * 8u + column)};
                }
            }
            if (before != cursor->planned) {
                cursor_add_view(cursor, entry, band, group, tile, presence);
            }
        }
        if (++scanned % 64u == 0 && ohlc_monotonic_ms() > cursor->deadline_ms) {
            return OHLC_CANCELLED;
        }
    }
    cursor->done = cursor->selected_position == cursor->selected_count;
    return OHLC_OK;
}

ohlc_status ohlc_cursor_next(ohlc_cursor* cursor, void* output, size_t capacity, size_t* count) {
    if (cursor == NULL || output == NULL || count == NULL || capacity == 0 ||
        capacity > SIZE_MAX / OHLC_RESULT_BYTES) {
        return OHLC_INVALID;
    }
    *count = 0;
    if (cursor->error != OHLC_OK) {
        return cursor->error;
    }
    uint8_t* bytes = output;
    while (*count < capacity) {
        if (ohlc_monotonic_ms() > cursor->deadline_ms) {
            cursor->error = OHLC_CANCELLED;
            break;
        }
        if (cursor->emitted == cursor->planned) {
            cursor_release_window(cursor);
            if (cursor->done) {
                break;
            }
            if (cursor->kind == 1) {
                cursor->error = plan_series(cursor);
            } else if (cursor->kind == 3) {
                cursor->error = plan_selected_cross(cursor);
            } else {
                cursor->error = plan_cross(cursor);
            }
            if (cursor->error == OHLC_OK) {
                cursor->error = ohlc_storage_read(cursor->db, cursor->table->info.id, cursor->views,
                                                  cursor->view_count);
            }
            if (cursor->error != OHLC_OK) {
                break;
            }
        }
        size_t available = cursor->planned - cursor->emitted;
        size_t take = capacity - *count < available ? capacity - *count : available;
        for (size_t i = 0; i < take; i++) {
            const ohlc_result_plan* row = &cursor->plan[cursor->emitted++];
            const uint8_t* data = cursor->views[row->view].data;
            ohlc_put_u32(bytes + *count * OHLC_RESULT_BYTES, row->key);
            memcpy(bytes + *count * OHLC_RESULT_BYTES + 4,
                   data + (size_t)row->slot * OHLC_ROW_BYTES, OHLC_ROW_BYTES);
            (*count)++;
        }
    }
    if (cursor->error != OHLC_OK) {
        cursor_release_window(cursor);
    }
    return cursor->error;
}

void ohlc_cursor_close(ohlc_cursor* cursor) {
    if (cursor != NULL) {
        ohlc_db* db = cursor->db;
        cursor_release_window(cursor);
        ohlc_root_release(db, cursor->root);
        ohlc_free(&db->allocator, cursor->selected_codes);
        ohlc_free(&db->allocator, cursor);
        atomic_fetch_sub_explicit(&db->cursor_count, 1, memory_order_release);
    }
}

ohlc_status ohlc_checkpoint(ohlc_db* db) {
    if (db == NULL) {
        return OHLC_INVALID;
    }
    pthread_mutex_lock(&db->checkpoint_mutex);
    ohlc_status status = db->failed ? OHLC_IO : ohlc_checkpoint_run(db);
    pthread_mutex_unlock(&db->checkpoint_mutex);
    return status;
}

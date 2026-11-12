/* SPDX-License-Identifier: Apache-2.0 */
/* Online reuse of the extents of live tables; see design section 13.6.
 *
 * A checkpoint rewrites the whole extent of every changed group. This module
 * finds the extents that a durable root stopped referencing and decides when
 * the storage layer may hand their blocks to later flushes. Every function
 * runs on the checkpoint owner, or during single-threaded open and close. */
#include "internal.h"

#include <string.h>

#define OHLC_RECLAIM_FIRST_CAPACITY 64u

static void batch_free(ohlc_db* db, ohlc_reclaim_batch* batch) {
    ohlc_free(&db->allocator, batch->extents);
    ohlc_free(&db->allocator, batch);
}

/* Appends to the referenced batch, creating it on first use. Batches use the
 * ordinary budget, never the checkpoint lease. An extent that cannot be
 * recorded is not reused in this run; the next open marks its blocks again. */
static void batch_append(ohlc_db* db, ohlc_reclaim_batch** reference,
                         const ohlc_dead_extent* extent) {
    ohlc_reclaim_batch* batch = *reference;
    if (batch == NULL) {
        batch = ohlc_alloc(&db->allocator, sizeof(*batch));
        if (batch == NULL) {
            return;
        }
        *reference = batch;
    }
    if (batch->count == batch->capacity) {
        size_t capacity = batch->capacity == 0 ? OHLC_RECLAIM_FIRST_CAPACITY
                                               : ohlc_size_multiply(batch->capacity, 2);
        ohlc_dead_extent* extents =
            ohlc_alloc_uninit(&db->allocator, ohlc_size_multiply(capacity, sizeof(*extents)));
        if (extents == NULL) {
            return;
        }
        if (batch->count != 0) {
            memcpy(extents, batch->extents, batch->count * sizeof(*extents));
        }
        ohlc_free(&db->allocator, batch->extents);
        batch->extents = extents;
        batch->capacity = capacity;
    }
    batch->extents[batch->count++] = *extent;
}

/* An on-disk entry names its extent by volume and first byte. */
static bool same_extent(const ohlc_group* previous, const ohlc_group* saved) {
    return saved != NULL && saved->flags == 0 && saved->mask != 0 &&
           saved->base == previous->base && saved->volume == previous->volume;
}

static void collect_page(ohlc_db* db, uint32_t table, uint32_t band, uint32_t page,
                         const ohlc_group_page* previous, const ohlc_group_page* saved,
                         ohlc_reclaim_batch** batch) {
    for (uint32_t slot = 0; slot < OHLC_PAGE_ENTRIES; slot++) {
        const ohlc_group* entry = &previous->groups[slot];
        if (entry->flags != 0 || entry->mask == 0) {
            continue;
        }
        if (same_extent(entry, saved == NULL ? NULL : &saved->groups[slot])) {
            continue;
        }
        ohlc_dead_extent extent = {.table = table,
                                   .band = band,
                                   .group = page * OHLC_PAGE_ENTRIES + slot,
                                   .volume = entry->volume,
                                   .base = entry->base,
                                   .blocks = ohlc_popcount(entry->mask)};
        batch_append(db, batch, &extent);
    }
}

static void collect_band(ohlc_db* db, uint32_t table, uint32_t band, const ohlc_band* previous,
                         const ohlc_band* saved, ohlc_reclaim_batch** batch) {
    if (previous == saved) {
        return;
    }
    for (uint32_t page = 0; page < previous->page_count; page++) {
        const ohlc_group_page* old_page = previous->pages[page];
        if (old_page == NULL) {
            continue;
        }
        const ohlc_group_page* new_page =
            saved != NULL && page < saved->page_count ? saved->pages[page] : NULL;
        if (old_page != new_page) {
            collect_page(db, table, band, page, old_page, new_page, batch);
        }
    }
}

/* Consecutive durable roots share every directory object that did not change,
 * so the walk only descends into the paths that a checkpoint rewrote. */
static void collect_node(ohlc_db* db, uint32_t table, const ohlc_band_node* previous,
                         const ohlc_band_node* saved, uint32_t prefix, ohlc_reclaim_batch** batch) {
    if (previous == NULL || previous == saved) {
        return;
    }
    for (uint16_t i = 0; i < previous->size; i++) {
        if (previous->children[i] == NULL) {
            continue;
        }
        const void* new_child = saved != NULL && i < saved->size ? saved->children[i] : NULL;
        if (previous->level == 2) {
            collect_band(db, table, prefix | (uint32_t)i, previous->children[i], new_child, batch);
        } else {
            unsigned int shift = previous->level == 0 ? 17u : 8u;
            collect_node(db, table, previous->children[i], new_child,
                         prefix | ((uint32_t)i << shift), batch);
        }
    }
}

/* Lists the extents that previous references and saved does not. Comparing
 * durable roots, instead of recording backings during a flush, lists every
 * extent exactly once even when a failed merge makes a group flush twice. */
static ohlc_reclaim_batch* collect_superseded(ohlc_db* db, const ohlc_root* previous,
                                              const ohlc_root* saved) {
    ohlc_reclaim_batch* batch = NULL;
    for (uint64_t id = 1; id <= previous->last_table_id; id++) {
        const ohlc_table* old_table = ohlc_root_table(previous, (uint32_t)id);
        const ohlc_table* new_table = ohlc_root_table(saved, (uint32_t)id);
        /* The files of a dropped table are removed as a whole. */
        if (old_table == NULL || new_table == NULL || old_table == new_table) {
            continue;
        }
        collect_node(db, (uint32_t)id, old_table->bands, new_table->bands, 0, &batch);
    }
    return batch;
}

/* Reports whether a root reads the extent, directly or as the backing of a
 * hot group. A dropped table references nothing. */
static bool root_references(const ohlc_root* root, const ohlc_dead_extent* extent) {
    const ohlc_table* table = ohlc_root_table(root, extent->table);
    const ohlc_group* entry =
        table == NULL ? NULL : ohlc_group_find(table, extent->band, extent->group);
    if (entry == NULL) {
        return false;
    }
    if (entry->flags == OHLC_HOT_GROUP) {
        const ohlc_hot_group* hot = (const ohlc_hot_group*)(uintptr_t)entry->base;
        entry = &hot->backing;
    }
    return entry->mask != 0 && entry->base == extent->base && entry->volume == extent->volume;
}

/* Called with writer. Neither checkpoint slot references the batch any more.
 * Extents that the current root still reads, such as the backing of a hot
 * group whose merge failed, move to the deferred batch and wait for the next
 * checkpoint. Later roots derive from the current one, so they cannot start
 * referencing the remaining extents; only older live roots still can. */
static void seal_batch(ohlc_db* db, ohlc_reclaim_batch* batch, ohlc_reclaim_batch** deferred) {
    size_t kept = 0;
    for (size_t i = 0; i < batch->count; i++) {
        if (root_references(db->root, &batch->extents[i])) {
            batch_append(db, deferred, &batch->extents[i]);
        } else {
            batch->extents[kept++] = batch->extents[i];
        }
    }
    batch->count = kept;
    batch->barrier = db->root->epoch;
}

void ohlc_reclaim_checkpoint(ohlc_db* db, const ohlc_root* previous, const ohlc_root* saved) {
    ohlc_reclaim_batch* superseded =
        previous == NULL ? NULL : collect_superseded(db, previous, saved);
    pthread_mutex_lock(&db->writer);
    for (ohlc_reclaim_batch* batch = db->reclaim_batches; batch != NULL; batch = batch->next) {
        if (batch->barrier == 0) {
            seal_batch(db, batch, &superseded);
        }
    }
    pthread_mutex_unlock(&db->writer);
    /* The other checkpoint slot still references the new batch. */
    if (superseded != NULL) {
        superseded->next = db->reclaim_batches;
        db->reclaim_batches = superseded;
    }
    /* Both slots now hold roots of this run, which never reference the
     * blocks that open marked as candidates. */
    db->reuse_enabled = true;
}

void ohlc_reclaim_release(ohlc_db* db) {
    if (db->reclaim_batches == NULL) {
        return;
    }
    uint64_t oldest = ohlc_root_oldest_epoch(db);
    ohlc_reclaim_batch** position = &db->reclaim_batches;
    while (*position != NULL) {
        ohlc_reclaim_batch* batch = *position;
        if (batch->barrier == 0 || oldest < batch->barrier) {
            position = &batch->next;
            continue;
        }
        for (size_t i = 0; i < batch->count; i++) {
            ohlc_storage_reuse_free(db, &batch->extents[i]);
        }
        *position = batch->next;
        batch_free(db, batch);
    }
}

void ohlc_reclaim_discard(ohlc_db* db) {
    while (db->reclaim_batches != NULL) {
        ohlc_reclaim_batch* batch = db->reclaim_batches;
        db->reclaim_batches = batch->next;
        batch_free(db, batch);
    }
}

static void mark_node(ohlc_db* db, uint32_t table, const ohlc_band_node* node) {
    if (node == NULL) {
        return;
    }
    for (uint16_t i = 0; i < node->size; i++) {
        if (node->children[i] == NULL) {
            continue;
        }
        if (node->level != 2) {
            mark_node(db, table, node->children[i]);
            continue;
        }
        const ohlc_band* band = node->children[i];
        for (uint32_t page = 0; page < band->page_count; page++) {
            const ohlc_group_page* groups = band->pages[page];
            if (groups == NULL) {
                continue;
            }
            for (size_t slot = 0; slot < OHLC_PAGE_ENTRIES; slot++) {
                ohlc_storage_reuse_keep(db, table, &groups->groups[slot]);
            }
        }
    }
}

void ohlc_reclaim_mark(ohlc_db* db, const ohlc_root* root) {
    ohlc_storage_reuse_candidates(db);
    for (uint64_t id = 1; id <= root->last_table_id; id++) {
        const ohlc_table* table = ohlc_root_table(root, (uint32_t)id);
        if (table != NULL) {
            mark_node(db, (uint32_t)id, table->bands);
        }
    }
}

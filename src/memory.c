/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <assert.h>
#include <string.h>
#include <time.h>

static _Thread_local ohlc_db* memory_busy_db;

uint64_t ohlc_memory_attempt(ohlc_db* db) {
    memory_busy_db = NULL;
    return atomic_load(&db->memory_generation);
}

bool ohlc_memory_retryable(ohlc_db* db) {
    return memory_busy_db == db;
}

void ohlc_memory_note_busy(ohlc_db* db) {
    memory_busy_db = db;
}

static void include_estimate(ohlc_memory_estimate* total, const ohlc_memory_estimate* item) {
    total->catalog = ohlc_size_add(total->catalog, item->catalog);
    total->copies = ohlc_size_add(total->copies, item->copies);
    total->storage = ohlc_size_add(total->storage, item->storage);
    if (item->scratch > total->scratch) {
        total->scratch = item->scratch;
    }
    if (item->dictionary > total->dictionary) {
        total->dictionary = item->dictionary;
    }
    if (item->reverse > total->reverse) {
        total->reverse = item->reverse;
    }
}

static ohlc_memory_estimate table_estimate(ohlc_db* db, const ohlc_table* table) {
    ohlc_memory_estimate result = {0};
    if (table == NULL) {
        return result;
    }
    result.catalog = 44u + strlen(table->info.name) + strlen(table->info.timezone) +
                     strlen(table->info.description);
    result.dictionary = (size_t)ohlc_dictionary_count(table);
    result.reverse = table->time_page_count;
    if (table->disk_offset != 0 || (table->time_count == 0 && result.dictionary == 0)) {
        return result;
    }
    /* Charge a table page per dirty table. Sharing only makes the actual copy
     * cheaper; this additive bound avoids scanning a page to maintain a count. */
    result.copies =
        ohlc_allocation_bytes(sizeof(*table)) + ohlc_allocation_bytes(sizeof(ohlc_table_page));
    size_t reverse = ohlc_size_multiply(table->time_page_count, sizeof(ohlc_time_page*));
    if (reverse != 0) {
        result.copies = ohlc_size_add(result.copies, ohlc_allocation_bytes(reverse));
    }
    result.copies = ohlc_size_add(result.copies, table->dirty_directory_bytes);
    result.scratch = ohlc_size_add(table->dirty_directory_bytes, 64);
    size_t payload = ohlc_size_add(reverse, 48);
    if (payload > result.scratch) {
        result.scratch = payload;
    }
    result.storage = ohlc_storage_table_bytes(db, table);
    return result;
}

static ohlc_memory_estimate collect_estimate(ohlc_db* db, const ohlc_root* root, bool cache) {
    ohlc_memory_estimate total = {.catalog = 32};
    for (size_t page = 0; page < root->page_count; page++) {
        if (root->pages[page] == NULL) {
            continue;
        }
        size_t entries = ohlc_root_page_entries(root, page);
        for (size_t slot = 0; slot < entries; slot++) {
            ohlc_table* table = root->pages[page]->tables[slot];
            if (table != NULL) {
                ohlc_memory_estimate item = table_estimate(db, table);
                include_estimate(&total, &item);
                if (cache) {
                    table->memory = item;
                }
            }
        }
    }
    return total;
}

size_t ohlc_checkpoint_bytes(ohlc_db* db, const ohlc_root* root) {
    const ohlc_memory_estimate* total = &root->memory;
    if (total->catalog > 64u * 1024u * 1024u) {
        return SIZE_MAX;
    }
    size_t bytes = ohlc_allocation_bytes(16u * OHLC_BLOCK_BYTES);
    bytes = ohlc_size_add(bytes, total->copies);
    if (total->copies != 0) {
        bytes = ohlc_size_add(bytes, ohlc_allocation_bytes(sizeof(*root)));
        bytes = ohlc_size_add(bytes, ohlc_allocation_bytes(ohlc_size_multiply(
                                         root->page_count, sizeof(*root->pages))));
    }
    size_t scratch = total->scratch > total->catalog ? total->scratch : total->catalog;
    bytes = ohlc_size_add(bytes, ohlc_allocation_bytes(scratch));
    bytes = ohlc_size_add(bytes, total->storage);
    return ohlc_size_add(bytes, ohlc_storage_root_bytes(db, root));
}

static size_t recovery_bytes(ohlc_db* db, const ohlc_memory_estimate* memory, size_t frame) {
    if (db->recovering) {
        return 0;
    }
    size_t bytes = ohlc_size_add(memory->catalog, 128u * 1024u);
    bytes = ohlc_size_add(bytes, ohlc_size_multiply(memory->dictionary, sizeof(uint64_t)));
    bytes = ohlc_size_add(bytes, ohlc_size_multiply(memory->reverse, sizeof(uint64_t)));
    bytes = ohlc_allocation_bytes(bytes);
    return bytes > frame ? bytes : frame;
}

ohlc_status ohlc_memory_request(ohlc_db* db, const ohlc_root* root, size_t payload,
                                size_t minimum_blocks) {
    if (payload > OHLC_FRAME_LIMIT - 104u) {
        return OHLC_LIMIT;
    }
    size_t frame = ohlc_allocation_bytes(96u + ((payload + 7u) & ~(size_t)7u));
    ohlc_memory_estimate memory = root->memory;
    if (memory.maxima_stale) {
        /* A checkpoint can discard maxima left by dropped tables. They must
         * not turn temporary pressure into a permanent rejection. */
        memory.dictionary = 0;
        memory.reverse = 0;
    }
    size_t needed = recovery_bytes(db, &memory, frame);
    /* The lease always reserves the flush window and at least the empty
     * catalog payload, even for mutations that create no dirty data blocks. */
    size_t lease = ohlc_allocation_bytes(16u * OHLC_BLOCK_BYTES) + ohlc_allocation_bytes(32);
    if (minimum_blocks != 0) {
        needed = ohlc_size_add(
            needed, ohlc_size_multiply(minimum_blocks, ohlc_allocation_bytes(sizeof(ohlc_block))));
        /* Any changed block needs a three-level band path, a band directory
         * and a group page. Count only one pointer per array, even where the
         * implementation actually allocates a wider directory. */
        size_t directory = 3u * ohlc_allocation_bytes(sizeof(ohlc_band_node) + sizeof(void*));
        directory += ohlc_allocation_bytes(sizeof(ohlc_band));
        directory += ohlc_allocation_bytes(sizeof(ohlc_group_page*));
        directory += ohlc_allocation_bytes(sizeof(ohlc_group_page));
        size_t copies = ohlc_allocation_bytes(sizeof(ohlc_root));
        copies += ohlc_allocation_bytes(sizeof(ohlc_table_page*));
        copies += ohlc_allocation_bytes(sizeof(ohlc_table_page));
        copies += ohlc_allocation_bytes(sizeof(ohlc_table));
        copies += ohlc_allocation_bytes(sizeof(ohlc_time_page*));
        copies += directory;
        /* These private index objects remain live with the dirty blocks and
         * the checkpoint credit for copying them. Each hot group holds at
         * most 16 blocks. Do not count old resident data or temporary plans. */
        size_t groups = minimum_blocks / 16u + (minimum_blocks % 16u != 0);
        needed = ohlc_size_add(needed, copies);
        needed = ohlc_size_add(
            needed, ohlc_size_multiply(groups, ohlc_allocation_bytes(sizeof(ohlc_hot_group))));
        lease = ohlc_size_add(lease, copies);
    }
    needed = ohlc_size_add(needed, lease);
    return needed > db->allocator.limit ? OHLC_LIMIT : OHLC_OK;
}

static bool resize_reservation(ohlc_db* db, size_t checkpoint, size_t recovery) {
    size_t previous = ohlc_size_add(db->checkpoint_reserve, db->recovery_reserve);
    size_t needed = ohlc_size_add(checkpoint, recovery);
    if (needed == SIZE_MAX) {
        return false;
    }
    if (needed > previous && !ohlc_reserve(&db->allocator, needed - previous)) {
        ohlc_block_pool_trim(&db->allocator);
        if (!ohlc_reserve(&db->allocator, needed - previous)) {
            return false;
        }
    }
    if (needed < previous) {
        ohlc_unreserve(&db->allocator, previous - needed);
    }
    db->checkpoint_reserve = checkpoint;
    db->recovery_reserve = recovery;
    return true;
}

ohlc_status ohlc_memory_begin(ohlc_db* db, size_t payload) {
    db->pending_frame = 0;
    if (db->failed) {
        return OHLC_IO;
    }
    ohlc_status status = ohlc_memory_request(db, db->root, payload, 0);
    if (status != OHLC_OK) {
        return status;
    }
    if (db->writes_paused) {
        return OHLC_BUSY;
    }
    size_t frame = ohlc_allocation_bytes(96u + ((payload + 7u) & ~(size_t)7u));
    size_t recovery = frame > db->largest_frame ? frame : db->largest_frame;
    if (!resize_reservation(db, db->checkpoint_reserve,
                            recovery_bytes(db, &db->root->memory, recovery))) {
        return OHLC_BUSY;
    }
    db->pending_frame = frame;
    return OHLC_OK;
}

static ohlc_status admit_estimate(ohlc_db* db, const ohlc_root* root) {
    size_t checkpoint = ohlc_checkpoint_bytes(db, root);
    size_t frame = db->pending_frame > db->largest_frame ? db->pending_frame : db->largest_frame;
    size_t recovery = recovery_bytes(db, &root->memory, frame);
    if (!resize_reservation(db, checkpoint, recovery)) {
        return checkpoint == SIZE_MAX ? OHLC_LIMIT : OHLC_BUSY;
    }
    return OHLC_OK;
}

void ohlc_memory_wal_frame(ohlc_db* db, uint64_t sequence, size_t bytes) {
    size_t frame = ohlc_allocation_bytes(bytes);
    if (frame > db->largest_frame) {
        db->largest_frame = frame;
    }
    if (db->checkpoint_source != NULL && sequence > db->checkpoint_source->seq &&
        frame > db->checkpoint_tail_frame) {
        db->checkpoint_tail_frame = frame;
    }
}

ohlc_status ohlc_memory_admit(ohlc_db* db, ohlc_root* root) {
    root->memory = collect_estimate(db, root, true);
    return admit_estimate(db, root);
}

ohlc_status ohlc_memory_admit_table(ohlc_db* db, ohlc_root* root, const ohlc_root* source,
                                    uint32_t id) {
    root->memory = source->memory;
    const ohlc_table* before = ohlc_root_table(source, id);
    if (before != NULL) {
        root->memory.catalog -= before->memory.catalog;
        root->memory.copies -= before->memory.copies;
        root->memory.storage -= before->memory.storage;
    }
    ohlc_table* table = (ohlc_table*)ohlc_root_table(root, id);
    if (table != NULL) {
        table->memory = table_estimate(db, table);
        include_estimate(&root->memory, &table->memory);
    } else if (before != NULL) {
        root->memory.maxima_stale = true;
    }
#ifndef NDEBUG
    ohlc_memory_estimate full = collect_estimate(db, root, false);
    assert(root->memory.catalog == full.catalog);
    assert(root->memory.copies >= full.copies);
    assert(root->memory.storage >= full.storage);
    assert(root->memory.scratch >= full.scratch);
    assert(root->memory.dictionary >= full.dictionary);
    assert(root->memory.reverse >= full.reverse);
#endif
    return admit_estimate(db, root);
}

ohlc_status ohlc_memory_reject(ohlc_db* db, ohlc_status status) {
    db->pending_frame = 0;
    if (status != OHLC_BUSY) {
        return status;
    }
    atomic_store(&db->memory_pressure, true);
    pthread_cond_signal(&db->memory_changed);
    ohlc_block_pool_trim(&db->allocator);
    /* A clean root does not imply an idle allocator: packing, query plans and
     * old snapshots can still hold temporary memory. Without a proof that the
     * request itself exceeds the budget, keep the result retryable. */
    ohlc_memory_note_busy(db);
    return OHLC_BUSY;
}

ohlc_status ohlc_memory_checkpoint_begin(ohlc_db* db, const ohlc_root* source) {
    db->root->memory = collect_estimate(db, source, true);
    size_t bytes = ohlc_checkpoint_bytes(db, source);
    if (!resize_reservation(db, bytes, db->recovery_reserve)) {
        return OHLC_LIMIT;
    }
    bool concurrent = !atomic_load(&db->memory_pressure) && ohlc_reserve(&db->allocator, bytes);
    db->checkpoint_tail_frame = 0;
    ohlc_storage_capture(db, source);
    db->checkpoint_source = source;
    if (concurrent) {
        db->root->memory = collect_estimate(db, source, true);
        if (!resize_reservation(db, ohlc_checkpoint_bytes(db, source), db->recovery_reserve)) {
            ohlc_unreserve(&db->allocator, bytes);
            concurrent = false;
        }
    }
    if (!concurrent) {
        /* Transfer the current root's reservation to the checkpoint. With
         * mutations gated, no second dirty root can appear while it is spent. */
        db->writes_paused = true;
        db->checkpoint_reserve = 0;
        ohlc_block_pool_trim(&db->allocator);
    }
    /* Admission reserves a bound that remains valid until this root is fully
     * covered. Storage growth by an in-flight checkpoint is included before
     * publication, independently of whether its last volume still has room. */
    db->checkpoint_allocator.available = bytes;
    db->checkpoint_allocator.estimate = bytes;
    db->checkpoint_allocator.peak = 0;
    db->checkpoint_allocator.epoch++;
    return OHLC_OK;
}

void ohlc_memory_checkpoint_end(ohlc_db* db) {
    /* Transfer unused credit before resizing it. Ordinary allocations must
     * never see a gap between the finished lease and the next reservation. */
    db->checkpoint_reserve += db->checkpoint_allocator.available;
    db->checkpoint_allocator.available = 0;
    db->checkpoint_source = NULL;
    db->writes_paused = false;
    db->pending_frame = 0;
    ohlc_status status = ohlc_memory_admit(db, db->root);
    bool changed = db->checkpoint_root == NULL || db->checkpoint_root->seq != db->root->seq;
    atomic_store(&db->memory_pressure,
                 status != OHLC_OK || (changed && atomic_load(&db->memory_pressure)));
    atomic_fetch_add(&db->memory_generation, 1);
    pthread_cond_broadcast(&db->memory_available);
    pthread_cond_broadcast(&db->memory_changed);
}

void ohlc_checkpoint_wait(ohlc_db* db, uint32_t milliseconds) {
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return;
    }
    deadline.tv_sec += (time_t)(milliseconds / 1000u);
    deadline.tv_nsec += (long)(milliseconds % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&db->writer);
    if (!atomic_load(&db->memory_pressure)) {
        (void)pthread_cond_timedwait(&db->memory_changed, &db->writer, &deadline);
    }
    pthread_mutex_unlock(&db->writer);
}

void ohlc_memory_wait(ohlc_db* db, uint64_t generation, uint32_t milliseconds) {
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return;
    }
    if (milliseconds > 100) {
        milliseconds = 100;
    }
    deadline.tv_nsec += (long)milliseconds * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&db->writer);
    if (generation == atomic_load(&db->memory_generation)) {
        /* The condition wait releases writer. A separate condition keeps a
         * pressure notification from waking a writer instead of the flusher.
         * Increment and broadcast also hold writer, so this recheck cannot
         * lose a wakeup. An earlier stale load only causes an immediate retry. */
        (void)pthread_cond_timedwait(&db->memory_available, &db->writer, &deadline);
    }
    pthread_mutex_unlock(&db->writer);
}

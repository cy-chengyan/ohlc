/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <string.h>

static uint64_t ticker_hash(ohlc_bytes ticker) {
    const uint8_t* bytes = ticker.data;
    uint64_t hash = UINT64_C(14695981039346656037);
    for (size_t i = 0; i < ticker.size; i++) {
        hash = (hash ^ bytes[i]) * UINT64_C(1099511628211);
    }
    return hash ^ (hash >> 32);
}

uint64_t ohlc_dictionary_count(const ohlc_table* table) {
    return table == NULL || table->dictionary == NULL ? 0 : table->dictionary->count;
}

void ohlc_dictionary_retain(ohlc_dictionary* dictionary) {
    if (dictionary != NULL) {
        atomic_fetch_add_explicit(&dictionary->refs, 1, memory_order_relaxed);
    }
}

void ohlc_dictionary_release(ohlc_allocator* allocator, ohlc_dictionary* dictionary) {
    if (dictionary == NULL ||
        atomic_fetch_sub_explicit(&dictionary->refs, 1, memory_order_acq_rel) != 1) {
        return;
    }
    for (uint64_t i = 0; i < dictionary->count; i++) {
        ohlc_ticker_entry* entry = dictionary->entries[i];
        if (atomic_fetch_sub_explicit(&entry->refs, 1, memory_order_acq_rel) == 1) {
            ohlc_free(allocator, entry);
        }
    }
    ohlc_free(allocator, dictionary->entries);
    ohlc_free(allocator, dictionary->buckets);
    ohlc_free(allocator, dictionary);
}

const ohlc_ticker_entry* ohlc_dictionary_find(const ohlc_dictionary* dictionary,
                                              ohlc_bytes ticker) {
    if (dictionary == NULL) {
        return NULL;
    }
    uint64_t hash = ticker_hash(ticker);
    size_t mask = dictionary->capacity * 2u - 1u;
    size_t slot = (size_t)hash & mask;
    while (dictionary->buckets[slot] != NULL) {
        const ohlc_ticker_entry* entry = dictionary->buckets[slot];
        if (entry->hash == hash && entry->length == ticker.size &&
            memcmp(entry->bytes, ticker.data, ticker.size) == 0) {
            return entry;
        }
        slot = (slot + 1u) & mask;
    }
    return NULL;
}

static void dictionary_insert(ohlc_dictionary* dictionary, ohlc_ticker_entry* entry) {
    size_t mask = dictionary->capacity * 2u - 1u;
    size_t slot = (size_t)entry->hash & mask;
    while (dictionary->buckets[slot] != NULL) {
        slot = (slot + 1u) & mask;
    }
    dictionary->buckets[slot] = entry;
    dictionary->entries[dictionary->count++] = entry;
}

/* Copy pointer arrays only when adding a name to a shared snapshot or growing.
 * Immutable strings remain shared; ordinary writes do not copy the dictionary. */
static ohlc_status dictionary_edit(ohlc_db* db, ohlc_table* table) {
    ohlc_dictionary* source = table->dictionary;
    if (source != NULL && source->count < source->capacity &&
        atomic_load_explicit(&source->refs, memory_order_acquire) == 1) {
        return OHLC_OK;
    }
    size_t capacity = source == NULL ? 16u : source->capacity;
    if (source != NULL && source->count == capacity) {
        if (capacity > SIZE_MAX / 4u / sizeof(ohlc_ticker_entry*)) {
            return OHLC_LIMIT;
        }
        capacity *= 2;
    }
    ohlc_dictionary* copy = ohlc_alloc(&db->allocator, sizeof(*copy));
    if (copy == NULL) {
        return OHLC_LIMIT;
    }
    atomic_init(&copy->refs, 1);
    copy->capacity = capacity;
    copy->entries = ohlc_alloc(&db->allocator, capacity * sizeof(*copy->entries));
    copy->buckets = ohlc_alloc(&db->allocator, capacity * 2u * sizeof(*copy->buckets));
    if (copy->entries == NULL || copy->buckets == NULL) {
        ohlc_dictionary_release(&db->allocator, copy);
        return OHLC_LIMIT;
    }
    if (source != NULL) {
        for (uint64_t i = 0; i < source->count; i++) {
            ohlc_ticker_entry* entry = source->entries[i];
            atomic_fetch_add_explicit(&entry->refs, 1, memory_order_relaxed);
            dictionary_insert(copy, entry);
        }
    }
    ohlc_dictionary_release(&db->allocator, source);
    table->dictionary = copy;
    return OHLC_OK;
}

ohlc_status ohlc_dictionary_add(ohlc_db* db, ohlc_table* table, ohlc_bytes ticker, uint32_t* code) {
    if (ticker.data == NULL || ticker.size == 0 || ticker.size > 4096) {
        return OHLC_INVALID;
    }
    const ohlc_ticker_entry* existing = ohlc_dictionary_find(table->dictionary, ticker);
    if (existing != NULL) {
        *code = existing->code;
        return OHLC_OK;
    }
    if (ohlc_dictionary_count(table) > UINT32_MAX) {
        return OHLC_LIMIT;
    }
    ohlc_status status = dictionary_edit(db, table);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_ticker_entry* entry = ohlc_alloc(&db->allocator, sizeof(*entry) + ticker.size);
    if (entry == NULL) {
        return OHLC_LIMIT;
    }
    atomic_init(&entry->refs, 1);
    entry->code = (uint32_t)table->dictionary->count;
    entry->hash = ticker_hash(ticker);
    entry->length = (uint32_t)ticker.size;
    memcpy(entry->bytes, ticker.data, ticker.size);
    dictionary_insert(table->dictionary, entry);
    *code = entry->code;
    return OHLC_OK;
}

ohlc_status ohlc_resolve(ohlc_db* db, uint32_t table_id, ohlc_bytes ticker, uint32_t* code) {
    if (db == NULL || code == NULL || ticker.data == NULL || ticker.size == 0 ||
        ticker.size > 4096) {
        return OHLC_INVALID;
    }
    ohlc_root* root = ohlc_root_acquire(db);
    const ohlc_table* table = ohlc_root_table(root, table_id);
    const ohlc_ticker_entry* entry =
        table == NULL ? NULL : ohlc_dictionary_find(table->dictionary, ticker);
    ohlc_status status = OHLC_NOT_FOUND;
    if (entry != NULL) {
        *code = entry->code;
        status = OHLC_OK;
    }
    ohlc_root_release(db, root);
    return status;
}

ohlc_status ohlc_ticker(ohlc_db* db, uint32_t table_id, uint32_t code, uint8_t buffer[4096],
                        ohlc_bytes* output) {
    if (db == NULL || buffer == NULL || output == NULL) {
        return OHLC_INVALID;
    }
    ohlc_root* root = ohlc_root_acquire(db);
    const ohlc_table* table = ohlc_root_table(root, table_id);
    ohlc_status status = OHLC_NOT_FOUND;
    if (code < ohlc_dictionary_count(table)) {
        const ohlc_ticker_entry* entry = table->dictionary->entries[code];
        memcpy(buffer, entry->bytes, entry->length);
        *output = (ohlc_bytes){buffer, entry->length};
        status = OHLC_OK;
    }
    ohlc_root_release(db, root);
    return status;
}

ohlc_status ohlc_register(ohlc_db* db, uint32_t table_id, ohlc_bytes ticker, uint32_t* code,
                          uint64_t* seq) {
    if (db == NULL || code == NULL || seq == NULL || ticker.data == NULL || ticker.size == 0 ||
        ticker.size > 4096) {
        return OHLC_INVALID;
    }
    pthread_mutex_lock(&db->writer);
    ohlc_root* candidate = NULL;
    const ohlc_table* source = ohlc_root_table(db->root, table_id);
    ohlc_status status = OHLC_NOT_FOUND;
    if (source == NULL) {
        goto cleanup;
    }
    const ohlc_ticker_entry* existing = ohlc_dictionary_find(source->dictionary, ticker);
    if (existing != NULL) {
        *code = existing->code;
        *seq = db->root->seq;
        status = OHLC_OK;
        goto cleanup;
    }
    status = db->failed ? OHLC_IO : OHLC_LIMIT;
    if (db->failed || db->root->seq == UINT64_MAX) {
        goto cleanup;
    }
    candidate = ohlc_root_copy(db, db->root);
    if (candidate == NULL) {
        goto cleanup;
    }
    ohlc_table* table = NULL;
    uint32_t assigned = 0;
    status = ohlc_root_edit_table(db, candidate, table_id, &table);
    if (status == OHLC_OK) {
        status = ohlc_dictionary_add(db, table, ticker, &assigned);
    }
    if (status != OHLC_OK) {
        goto cleanup;
    }
    candidate->seq++;
    candidate->ticker_count++;
    uint8_t payload[4104];
    ohlc_put_u32(payload, assigned);
    ohlc_put_u32(payload + 4, (uint32_t)ticker.size);
    memcpy(payload + 8, ticker.data, ticker.size);
    status = ohlc_wal_append(db, 1, table_id, 1, candidate->seq, payload, ticker.size + 8);
    if (status == OHLC_OK) {
        *code = assigned;
        *seq = candidate->seq;
        ohlc_root_publish(db, candidate);
        candidate = NULL;
    }
cleanup:
    ohlc_root_release(db, candidate);
    pthread_mutex_unlock(&db->writer);
    return status;
}

ohlc_status ohlc_named_pack(ohlc_allocator* allocator, const ohlc_bytes* tickers,
                            size_t ticker_count, const void* rows, size_t count, uint8_t** output,
                            size_t* size) {
    *output = NULL;
    if (tickers == NULL || rows == NULL || ticker_count == 0 || ticker_count > count ||
        count == 0 || count > OHLC_MAX_BATCH_ROWS) {
        return OHLC_INVALID;
    }
    size_t length = 4u + count * OHLC_WRITE_BYTES;
    for (size_t i = 0; i < ticker_count; i++) {
        if (tickers[i].data == NULL || tickers[i].size == 0 || tickers[i].size > 4096) {
            return OHLC_INVALID;
        }
        length += 4u + tickers[i].size;
        if (length > OHLC_FRAME_LIMIT - 104u) {
            return OHLC_LIMIT;
        }
    }
    uint8_t* payload = ohlc_alloc(allocator, length);
    if (payload == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_put_u32(payload, (uint32_t)ticker_count);
    size_t position = 4;
    for (size_t i = 0; i < ticker_count; i++) {
        ohlc_put_u32(payload + position, (uint32_t)tickers[i].size);
        memcpy(payload + position + 4, tickers[i].data, tickers[i].size);
        position += 4u + tickers[i].size;
    }
    memcpy(payload + position, rows, count * OHLC_WRITE_BYTES);
    *output = payload;
    *size = length;
    return OHLC_OK;
}

ohlc_status ohlc_prepare_named(ohlc_db* db, const ohlc_root* source, uint32_t table_id,
                               const uint8_t* payload, size_t size, size_t count,
                               ohlc_root** output) {
    *output = NULL;
    if (count == 0 || count > OHLC_MAX_BATCH_ROWS || size < 4 || size > OHLC_FRAME_LIMIT - 104u) {
        return OHLC_INVALID;
    }
    uint32_t names = ohlc_get_u32(payload);
    if (names == 0 || names > count) {
        return OHLC_INVALID;
    }
    ohlc_bytes* tickers = ohlc_alloc(&db->allocator, names * sizeof(*tickers));
    uint64_t* mapping = ohlc_alloc(&db->allocator, names * sizeof(*mapping));
    uint8_t* rows = ohlc_alloc(&db->allocator, count * OHLC_WRITE_BYTES);
    ohlc_root* candidate = NULL;
    ohlc_status status = OHLC_LIMIT;
    if (tickers == NULL || mapping == NULL || rows == NULL) {
        goto cleanup;
    }
    size_t position = 4;
    status = OHLC_INVALID;
    for (uint32_t i = 0; i < names; i++) {
        if (size - position < 4) {
            goto cleanup;
        }
        uint32_t length = ohlc_get_u32(payload + position);
        position += 4;
        if (length == 0 || length > 4096 || length > size - position) {
            goto cleanup;
        }
        tickers[i] = (ohlc_bytes){payload + position, length};
        mapping[i] = UINT64_MAX;
        position += length;
    }
    if (size - position != count * OHLC_WRITE_BYTES) {
        goto cleanup;
    }
    candidate = ohlc_root_copy(db, source);
    status = OHLC_LIMIT;
    if (candidate == NULL) {
        goto cleanup;
    }
    ohlc_table* table = NULL;
    status = ohlc_root_edit_table(db, candidate, table_id, &table);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    uint64_t previous_count = ohlc_dictionary_count(table);
    memcpy(rows, payload + position, count * OHLC_WRITE_BYTES);
    for (size_t i = 0; i < count; i++) {
        uint8_t* row = rows + i * OHLC_WRITE_BYTES;
        uint32_t index = ohlc_get_u32(row);
        if (index >= names) {
            status = OHLC_INVALID;
            goto cleanup;
        }
        if (mapping[index] == UINT64_MAX) {
            uint32_t code = 0;
            status = ohlc_dictionary_add(db, table, tickers[index], &code);
            if (status != OHLC_OK) {
                goto cleanup;
            }
            mapping[index] = code;
        }
        ohlc_put_u32(row, (uint32_t)mapping[index]);
    }
    candidate->ticker_count += ohlc_dictionary_count(table) - previous_count;
    status = ohlc_prepare_write(db, candidate, table_id, rows, count, output);
cleanup:
    ohlc_root_release(db, candidate);
    ohlc_free(&db->allocator, rows);
    ohlc_free(&db->allocator, mapping);
    ohlc_free(&db->allocator, tickers);
    return status;
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define OHLC_RECORD_LIMIT (64u * 1024u * 1024u)

enum {
    OHLC_RECORD_TIME = 1,
    OHLC_RECORD_REVERSE = 2,
    OHLC_RECORD_GROUPS = 3,
    OHLC_RECORD_BAND = 4,
    OHLC_RECORD_RADIX = 5,
    OHLC_RECORD_TABLE = 6,
    OHLC_RECORD_TICKER = 7,
    OHLC_RECORD_CATALOG = 8
};

typedef struct {
    uint64_t generation;
    uint64_t seq;
    uint64_t catalog;
    uint32_t wal_id;
    uint64_t wal_offset;
    bool valid;
} ohlc_checkpoint_entry;

static ohlc_status record_write(ohlc_db* db, int fd, uint64_t* end, uint32_t type,
                                const uint8_t* payload, size_t size, uint64_t* offset) {
    if (size > OHLC_RECORD_LIMIT || *end > (uint64_t)INT64_MAX - size - 40u) {
        return OHLC_LIMIT;
    }
    size_t total = (32u + size + 7u) & ~(size_t)7u;
    uint8_t* record = ohlc_alloc(&db->allocator, total);
    if (record == NULL) {
        return OHLC_LIMIT;
    }
    memcpy(record, "OIR4", 4);
    ohlc_put_u32(record + 4, type);
    ohlc_put_u32(record + 8, (uint32_t)size);
    ohlc_put_u64(record + 16, db->checkpoint_generation + 1);
    memcpy(record + 32, payload, size);
    ohlc_put_u32(record + 12, ohlc_crc32c(0, record, total));
    ohlc_status status = ohlc_write_full(fd, record, total, *end);
    if (status == OHLC_OK) {
        *offset = *end;
        *end += total;
    } else {
        db->failed = true;
    }
    ohlc_free(&db->allocator, record);
    return status;
}

static ohlc_status record_save(ohlc_db* db, ohlc_table_files* files, uint32_t type,
                               const uint8_t* payload, size_t size, _Atomic uint64_t* saved) {
    uint64_t offset = 0;
    ohlc_status status =
        record_write(db, files->index_fd, &files->index_size, type, payload, size, &offset);
    if (status == OHLC_OK) {
        files->index_dirty = true;
        atomic_store_explicit(saved, offset, memory_order_release);
    }
    return status;
}

/* Parent offsets bound every child reference, so corrupt graphs cannot cycle. */
static ohlc_status record_read(ohlc_db* db, int fd, uint64_t offset, uint64_t parent, uint32_t type,
                               uint8_t** output, size_t* size) {
    *output = NULL;
    uint8_t header[32];
    if (offset < OHLC_BLOCK_BYTES || offset % 8u != 0 || offset >= parent ||
        parent - offset < sizeof(header)) {
        return OHLC_CORRUPT;
    }
    ohlc_status status = ohlc_read_full(fd, header, sizeof(header), offset);
    if (status != OHLC_OK) {
        return status;
    }
    *size = ohlc_get_u32(header + 8);
    size_t total = (32u + *size + 7u) & ~(size_t)7u;
    if (memcmp(header, "OIR4", 4) != 0 || ohlc_get_u32(header + 4) != type ||
        *size > OHLC_RECORD_LIMIT || total > parent - offset || ohlc_get_u64(header + 24) != 0) {
        return OHLC_CORRUPT;
    }
    uint8_t* record = ohlc_alloc(&db->allocator, total);
    if (record == NULL) {
        return OHLC_LIMIT;
    }
    status = ohlc_read_full(fd, record, total, offset);
    if (status == OHLC_OK) {
        uint32_t expected = ohlc_get_u32(record + 12);
        ohlc_put_u32(record + 12, 0);
        if (expected != ohlc_crc32c(0, record, total)) {
            status = OHLC_CORRUPT;
        }
        for (size_t i = 32u + *size; i < total; i++) {
            if (record[i] != 0) {
                status = OHLC_CORRUPT;
            }
        }
    }
    if (status != OHLC_OK) {
        ohlc_free(&db->allocator, record);
        return status;
    }
    memmove(record, record + 32, *size);
    *output = record;
    return OHLC_OK;
}

static ohlc_status save_time(ohlc_db* db, ohlc_table_files* files, ohlc_time_node* node) {
    if (node == NULL || node->disk_offset != 0) {
        return OHLC_OK;
    }
    uint8_t payload[4104] = {0};
    ohlc_put_u32(payload, node->leaf);
    ohlc_put_u32(payload + 4, node->count);
    size_t size = 8;
    if (node->leaf) {
        for (uint16_t i = 0; i < node->count; i++) {
            ohlc_put_u32(payload + size, node->body.values.keys[i]);
            ohlc_put_u32(payload + size + 4, node->body.values.codes[i]);
            size += 8;
        }
    } else {
        for (uint16_t i = 0; i <= node->count; i++) {
            ohlc_status status = save_time(db, files, node->body.branch.children[i]);
            if (status != OHLC_OK) {
                return status;
            }
            ohlc_put_u64(payload + size, node->body.branch.children[i]->disk_offset);
            size += 8;
            if (i < node->count) {
                ohlc_put_u32(payload + size, node->body.branch.keys[i]);
                size += 4;
            }
        }
    }
    return record_save(db, files, OHLC_RECORD_TIME, payload, size, &node->disk_offset);
}

static ohlc_status load_time(ohlc_db* db, ohlc_table_files* files, uint64_t offset, uint64_t parent,
                             unsigned int depth, ohlc_time_node** output) {
    *output = NULL;
    if (offset == 0) {
        return OHLC_OK;
    }
    if (depth >= OHLC_MAX_DEPTH) {
        return OHLC_CORRUPT;
    }
    uint8_t* payload = NULL;
    size_t size = 0;
    ohlc_status status =
        record_read(db, files->index_fd, offset, parent, OHLC_RECORD_TIME, &payload, &size);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_time_node* node = NULL;
    if (size < 8) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    uint32_t leaf = ohlc_get_u32(payload);
    uint32_t count = ohlc_get_u32(payload + 4);
    if (leaf > 1 || count == 0 || count > (leaf ? OHLC_TIME_CAPACITY : OHLC_BRANCH_CAPACITY) ||
        size != (leaf ? 8u + count * 8u : 16u + count * 12u)) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    node = ohlc_alloc(&db->allocator, sizeof(*node));
    if (node == NULL) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    atomic_init(&node->refs, 1);
    atomic_init(&node->disk_offset, 0);
    node->leaf = (uint8_t)leaf;
    node->count = (uint16_t)count;
    node->disk_offset = offset;
    size_t position = 8;
    if (leaf) {
        for (uint16_t i = 0; i < node->count; i++) {
            node->body.values.keys[i] = ohlc_get_u32(payload + position);
            node->body.values.codes[i] = ohlc_get_u32(payload + position + 4);
            if (i != 0 && node->body.values.keys[i - 1] >= node->body.values.keys[i]) {
                status = OHLC_CORRUPT;
                goto cleanup;
            }
            position += 8;
        }
    } else {
        for (uint16_t i = 0; i <= node->count; i++) {
            uint64_t child = ohlc_get_u64(payload + position);
            position += 8;
            if (child == 0) {
                status = OHLC_CORRUPT;
                goto cleanup;
            }
            status = load_time(db, files, child, offset, depth + 1, &node->body.branch.children[i]);
            if (status != OHLC_OK) {
                goto cleanup;
            }
            if (i < node->count) {
                node->body.branch.keys[i] = ohlc_get_u32(payload + position);
                if (i != 0 && node->body.branch.keys[i - 1] >= node->body.branch.keys[i]) {
                    status = OHLC_CORRUPT;
                    goto cleanup;
                }
                position += 4;
            }
        }
    }
    *output = node;
    node = NULL;
cleanup:
    ohlc_time_release(&db->allocator, node);
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status save_band(ohlc_db* db, ohlc_table_files* files, ohlc_band* band) {
    if (band->disk_offset != 0) {
        return OHLC_OK;
    }
    size_t size = 8u + (size_t)band->page_count * 8u;
    uint8_t* payload = ohlc_alloc(&db->allocator, size);
    if (payload == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_put_u32(payload, band->page_count);
    ohlc_status status = OHLC_OK;
    for (uint32_t i = 0; i < band->page_count; i++) {
        ohlc_group_page* page = band->pages[i];
        if (page == NULL) {
            continue;
        }
        if (page->disk_offset == 0) {
            uint8_t groups[OHLC_BLOCK_BYTES + 1024] = {0};
            for (size_t j = 0; j < OHLC_PAGE_ENTRIES; j++) {
                const ohlc_group* group = &page->groups[j];
                if (group->flags != 0) {
                    status = OHLC_CORRUPT;
                    goto cleanup;
                }
                ohlc_put_u64(groups + j * 16, group->base);
                ohlc_put_u16(groups + j * 16 + 8, group->mask);
                ohlc_put_u32(groups + j * 16 + 12, group->volume);
                uint32_t metadata_crc = 0;
                for (uint8_t tile = 0; tile < 16; tile++) {
                    if ((group->mask & (1u << tile)) != 0) {
                        const uint8_t* metadata = ohlc_storage_presence(db, files->id, group, tile);
                        if (metadata == NULL) {
                            status = OHLC_CORRUPT;
                            goto cleanup;
                        }
                        metadata_crc = ohlc_crc32c(metadata_crc, metadata, 32);
                    }
                }
                ohlc_put_u32(groups + OHLC_BLOCK_BYTES + j * 4, metadata_crc);
            }
            status = record_save(db, files, OHLC_RECORD_GROUPS, groups, sizeof(groups),
                                 &page->disk_offset);
            if (status != OHLC_OK) {
                goto cleanup;
            }
        }
        ohlc_put_u64(payload + 8u + (size_t)i * 8u, page->disk_offset);
    }
    status = record_save(db, files, OHLC_RECORD_BAND, payload, size, &band->disk_offset);
cleanup:
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status save_radix(ohlc_db* db, ohlc_table_files* files, ohlc_band_node* node) {
    if (node == NULL || node->disk_offset != 0) {
        return OHLC_OK;
    }
    uint8_t payload[4104] = {0};
    ohlc_put_u32(payload, node->level);
    ohlc_put_u32(payload + 4, node->size);
    for (uint16_t i = 0; i < node->size; i++) {
        if (node->children[i] == NULL) {
            continue;
        }
        uint64_t offset = 0;
        ohlc_status status;
        if (node->level == 2) {
            ohlc_band* band = node->children[i];
            status = save_band(db, files, band);
            offset = band->disk_offset;
        } else {
            ohlc_band_node* child = node->children[i];
            status = save_radix(db, files, child);
            offset = child->disk_offset;
        }
        if (status != OHLC_OK) {
            return status;
        }
        ohlc_put_u64(payload + 8u + (size_t)i * 8u, offset);
    }
    return record_save(db, files, OHLC_RECORD_RADIX, payload, 8u + (size_t)node->size * 8u,
                       &node->disk_offset);
}

static ohlc_status load_band(ohlc_db* db, ohlc_table_files* files, uint64_t offset, uint64_t parent,
                             uint32_t band_id, ohlc_band** output) {
    *output = NULL;
    uint8_t* payload = NULL;
    size_t size = 0;
    ohlc_status status =
        record_read(db, files->index_fd, offset, parent, OHLC_RECORD_BAND, &payload, &size);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_band* band = NULL;
    if (size < 8 || ohlc_get_u32(payload + 4) != 0 || ohlc_get_u32(payload) == 0 ||
        ohlc_get_u32(payload) > (UINT32_MAX / 16u / 256u + 1u) ||
        size != 8u + (uint64_t)ohlc_get_u32(payload) * 8u) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    band = ohlc_alloc(&db->allocator, sizeof(*band));
    if (band == NULL) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    atomic_init(&band->refs, 1);
    atomic_init(&band->disk_offset, 0);
    band->page_count = ohlc_get_u32(payload);
    band->disk_offset = offset;
    band->pages = ohlc_alloc(&db->allocator, (size_t)band->page_count * sizeof(*band->pages));
    if (band->pages == NULL) {
        band->page_count = 0;
        status = OHLC_LIMIT;
        goto cleanup;
    }
    for (uint32_t i = 0; i < band->page_count; i++) {
        uint64_t page_offset = ohlc_get_u64(payload + 8u + (size_t)i * 8u);
        if (page_offset == 0) {
            continue;
        }
        uint8_t* encoded = NULL;
        size_t encoded_size = 0;
        status = record_read(db, files->index_fd, page_offset, offset, OHLC_RECORD_GROUPS, &encoded,
                             &encoded_size);
        if (status != OHLC_OK) {
            goto cleanup;
        }
        if (encoded_size != OHLC_BLOCK_BYTES + 1024) {
            ohlc_free(&db->allocator, encoded);
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        ohlc_group_page* page = ohlc_alloc(&db->allocator, sizeof(*page));
        if (page == NULL) {
            ohlc_free(&db->allocator, encoded);
            status = OHLC_LIMIT;
            goto cleanup;
        }
        atomic_init(&page->refs, 1);
        atomic_init(&page->disk_offset, 0);
        page->disk_offset = page_offset;
        band->pages[i] = page;
        for (uint32_t j = 0; j < 256; j++) {
            ohlc_group* group = &page->groups[j];
            group->base = ohlc_get_u64(encoded + j * 16u);
            group->mask = ohlc_get_u16(encoded + j * 16u + 8);
            group->volume = ohlc_get_u32(encoded + j * 16u + 12);
            uint16_t flags = ohlc_get_u16(encoded + j * 16u + 10);
            if (flags != 0 || (group->mask == 0 && (group->base != 0 || group->volume != 0)) ||
                (group->mask != 0 &&
                 (group->base < OHLC_BLOCK_BYTES || group->base % OHLC_BLOCK_BYTES != 0 ||
                  group->base > INT64_MAX - 16u * OHLC_BLOCK_BYTES))) {
                status = OHLC_CORRUPT;
                break;
            }
            uint32_t metadata_crc = 0;
            if (group->mask != 0) {
                status = ohlc_storage_load_volume(db, files, group->volume);
                if (status != OHLC_OK) {
                    break;
                }
            }
            for (uint8_t tile = 0; tile < 16 && status == OHLC_OK; tile++) {
                if ((group->mask & (1u << tile)) != 0) {
                    const uint8_t* presence = ohlc_storage_presence(db, files->id, group, tile);
                    if (presence == NULL || ohlc_get_u32(presence + 28) != 0) {
                        status = OHLC_CORRUPT;
                    } else {
                        metadata_crc = ohlc_crc32c(metadata_crc, presence, 32);
                    }
                }
            }
            if (metadata_crc != ohlc_get_u32(encoded + OHLC_BLOCK_BYTES + j * 4)) {
                status = OHLC_CORRUPT;
            }
            if (status != OHLC_OK) {
                break;
            }
        }
        ohlc_free(&db->allocator, encoded);
        if (status != OHLC_OK) {
            goto cleanup;
        }
    }
    (void)band_id;
    *output = band;
    band = NULL;
cleanup:
    ohlc_band_release(&db->allocator, band);
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status load_radix(ohlc_db* db, ohlc_table_files* files, uint64_t offset,
                              uint64_t parent, uint8_t level, uint32_t prefix,
                              ohlc_band_node** output) {
    *output = NULL;
    if (offset == 0) {
        return OHLC_OK;
    }
    uint8_t* payload = NULL;
    size_t size = 0;
    ohlc_status status =
        record_read(db, files->index_fd, offset, parent, OHLC_RECORD_RADIX, &payload, &size);
    if (status != OHLC_OK) {
        return status;
    }
    uint16_t count = level == 1 ? 512 : 256;
    ohlc_band_node* node = NULL;
    if (size != 8u + (size_t)count * 8u || ohlc_get_u32(payload) != level ||
        ohlc_get_u32(payload + 4) != count) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    node = ohlc_alloc(&db->allocator, sizeof(*node) + (size_t)count * sizeof(void*));
    if (node == NULL) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    atomic_init(&node->refs, 1);
    atomic_init(&node->disk_offset, 0);
    node->level = level;
    node->size = count;
    node->disk_offset = offset;
    for (uint16_t i = 0; i < count; i++) {
        uint64_t child_offset = ohlc_get_u64(payload + 8u + (size_t)i * 8u);
        if (child_offset == 0) {
            continue;
        }
        if (level == 2) {
            ohlc_band* band = NULL;
            status = load_band(db, files, child_offset, offset, prefix | i, &band);
            node->children[i] = band;
        } else {
            ohlc_band_node* child = NULL;
            unsigned int shift = level == 0 ? 17u : 8u;
            status = load_radix(db, files, child_offset, offset, (uint8_t)(level + 1),
                                prefix | ((uint32_t)i << shift), &child);
            node->children[i] = child;
        }
        if (status != OHLC_OK) {
            goto cleanup;
        }
        node->bitmap[i / 64u] |= UINT64_C(1) << (i % 64u);
    }
    *output = node;
    node = NULL;
cleanup:
    ohlc_band_node_release(&db->allocator, node);
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status flush_radix(ohlc_db* db, ohlc_table* table, const ohlc_band_node* node,
                               uint32_t prefix, uint8_t** buffer) {
    if (node == NULL || node->disk_offset != 0) {
        return OHLC_OK;
    }
    for (uint16_t i = 0; i < node->size; i++) {
        if (node->children[i] == NULL) {
            continue;
        }
        if (node->level != 2) {
            unsigned int shift = node->level == 0 ? 17u : 8u;
            ohlc_status status =
                flush_radix(db, table, node->children[i], prefix | ((uint32_t)i << shift), buffer);
            if (status != OHLC_OK) {
                return status;
            }
            continue;
        }
        const ohlc_band* band = node->children[i];
        if (band->disk_offset != 0) {
            continue;
        }
        for (uint32_t page = 0; page < band->page_count; page++) {
            if (band->pages[page] == NULL || band->pages[page]->disk_offset != 0) {
                continue;
            }
            for (uint32_t slot = 0; slot < 256; slot++) {
                const ohlc_group* entry = &band->pages[page]->groups[slot];
                if (entry->flags != OHLC_HOT_GROUP) {
                    continue;
                }
                if (*buffer == NULL) {
                    *buffer = ohlc_alloc(&db->allocator, 16u * OHLC_BLOCK_BYTES);
                    if (*buffer == NULL) {
                        return OHLC_LIMIT;
                    }
                }
                ohlc_group flushed;
                ohlc_status status = ohlc_storage_flush_group(
                    db, table->info.id, prefix | i, page * 256u + slot, entry, *buffer, &flushed);
                if (status != OHLC_OK) {
                    return status;
                }
                ohlc_group* target = NULL;
                status = ohlc_group_edit(db, table, prefix | i, page * 256u + slot, &target);
                if (status != OHLC_OK) {
                    return status;
                }
                ohlc_group_release(&db->allocator, target);
                *target = flushed;
            }
        }
    }
    return OHLC_OK;
}

static ohlc_status save_table(ohlc_db* db, ohlc_table* table) {
    if (table->disk_offset != 0 || table->time_count == 0) {
        return OHLC_OK;
    }
    ohlc_table_files* files = NULL;
    ohlc_status status = ohlc_storage_table(db, table->info.id, true, &files);
    if (status != OHLC_OK) {
        return status;
    }
    status = save_time(db, files, table->times);
    if (status == OHLC_OK) {
        status = save_radix(db, files, table->bands);
    }
    if (status != OHLC_OK) {
        return status;
    }
    size_t size = 32u + table->time_page_count * 8u;
    uint8_t* payload = ohlc_alloc(&db->allocator, size);
    if (payload == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_put_u64(payload, table->time_count);
    ohlc_put_u64(payload + 8, table->times == NULL ? 0 : table->times->disk_offset);
    ohlc_put_u64(payload + 16, table->bands == NULL ? 0 : table->bands->disk_offset);
    ohlc_put_u64(payload + 24, table->time_page_count);
    for (size_t i = 0; i < table->time_page_count; i++) {
        ohlc_time_page* page = table->time_pages[i];
        if (page->disk_offset == 0) {
            uint8_t encoded[4096];
            for (size_t j = 0; j < 1024; j++) {
                ohlc_put_u32(encoded + j * 4, page->keys[j]);
            }
            status = record_save(db, files, OHLC_RECORD_REVERSE, encoded, sizeof(encoded),
                                 &page->disk_offset);
            if (status != OHLC_OK) {
                goto cleanup;
            }
        }
        ohlc_put_u64(payload + 32u + i * 8u, page->disk_offset);
    }
    status = record_save(db, files, OHLC_RECORD_TABLE, payload, size, &table->disk_offset);
cleanup:
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status load_table(ohlc_db* db, ohlc_table* table, uint64_t offset) {
    if (offset == 0) {
        return OHLC_OK;
    }
    ohlc_table_files* files = NULL;
    ohlc_status status = ohlc_storage_table(db, table->info.id, false, &files);
    if (status != OHLC_OK) {
        return status == OHLC_NOT_FOUND ? OHLC_CORRUPT : status;
    }
    uint8_t* payload = NULL;
    size_t size = 0;
    status = record_read(db, files->index_fd, offset, files->index_size, OHLC_RECORD_TABLE,
                         &payload, &size);
    if (status != OHLC_OK) {
        return status;
    }
    if (size < 32 || ohlc_get_u64(payload) == 0 || ohlc_get_u64(payload) > UINT64_C(1) << 32 ||
        ohlc_get_u64(payload + 24) != (ohlc_get_u64(payload) + 1023u) / 1024u ||
        size != 32u + ohlc_get_u64(payload + 24) * 8u) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    table->disk_offset = offset;
    table->time_count = ohlc_get_u64(payload);
    table->time_page_count = (size_t)ohlc_get_u64(payload + 24);
    table->time_pages =
        ohlc_alloc(&db->allocator, table->time_page_count * sizeof(*table->time_pages));
    if (table->time_pages == NULL) {
        table->time_page_count = 0;
        status = OHLC_LIMIT;
        goto cleanup;
    }
    for (size_t i = 0; i < table->time_page_count; i++) {
        uint64_t page_offset = ohlc_get_u64(payload + 32u + i * 8u);
        uint8_t* encoded = NULL;
        size_t encoded_size = 0;
        status = record_read(db, files->index_fd, page_offset, offset, OHLC_RECORD_REVERSE,
                             &encoded, &encoded_size);
        if (status != OHLC_OK) {
            goto cleanup;
        }
        if (encoded_size != 4096) {
            ohlc_free(&db->allocator, encoded);
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        ohlc_time_page* page = ohlc_alloc(&db->allocator, sizeof(*page));
        if (page == NULL) {
            ohlc_free(&db->allocator, encoded);
            status = OHLC_LIMIT;
            goto cleanup;
        }
        atomic_init(&page->refs, 1);
        atomic_init(&page->disk_offset, 0);
        page->disk_offset = page_offset;
        table->time_pages[i] = page;
        for (size_t j = 0; j < 1024; j++) {
            page->keys[j] = ohlc_get_u32(encoded + j * 4);
        }
        ohlc_free(&db->allocator, encoded);
    }
    status = load_time(db, files, ohlc_get_u64(payload + 8), offset, 0, &table->times);
    if (status == OHLC_OK) {
        status = load_radix(db, files, ohlc_get_u64(payload + 16), offset, 0, 0, &table->bands);
    }
    if (status != OHLC_OK) {
        goto cleanup;
    }
    ohlc_time_iterator iterator;
    ohlc_time_seek(table->times, 0, &iterator);
    uint32_t key = 0;
    uint32_t code = 0;
    uint32_t previous = 0;
    uint64_t count = 0;
    while (ohlc_time_next(&iterator, &key, &code)) {
        uint32_t found = 0;
        if ((count != 0 && key <= previous) || code >= table->time_count ||
            table->time_pages[code / 1024u]->keys[code % 1024u] != key ||
            !ohlc_time_find(table->times, key, &found) || found != code) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        count++;
        previous = key;
    }
    if (count != table->time_count) {
        status = OHLC_CORRUPT;
    }
cleanup:
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status save_dictionary(ohlc_db* db, uint64_t count) {
    uint8_t payload[4116];
    while (db->dictionary_disk_count < count) {
        uint32_t code = (uint32_t)db->dictionary_disk_count;
        pthread_mutex_lock(&db->dictionary_mutex);
        const ohlc_ticker_entry* entry = db->ticker_pages[code / 1024u][code % 1024u];
        pthread_mutex_unlock(&db->dictionary_mutex);
        ohlc_put_u64(payload, db->dictionary_disk_offset);
        ohlc_put_u32(payload + 8, code);
        ohlc_put_u32(payload + 12, entry->length);
        memcpy(payload + 16, entry->bytes, entry->length);
        uint64_t offset = 0;
        ohlc_status status = record_write(db, db->catalog_fd, &db->catalog_size, OHLC_RECORD_TICKER,
                                          payload, 16u + entry->length, &offset);
        if (status != OHLC_OK) {
            return status;
        }
        db->dictionary_disk_count++;
        db->dictionary_disk_offset = offset;
    }
    return OHLC_OK;
}

static ohlc_status save_catalog(ohlc_db* db, const ohlc_root* root, uint64_t* offset) {
    ohlc_status status = save_dictionary(db, root->ticker_count);
    if (status != OHLC_OK) {
        return status;
    }
    size_t size = 32;
    for (uint64_t id = 1; id <= root->table_count; id++) {
        const ohlc_table* table = ohlc_root_table(root, (uint32_t)id);
        size += 44u + strlen(table->info.name) + strlen(table->info.timezone) +
                strlen(table->info.description);
        if (size > OHLC_RECORD_LIMIT) {
            return OHLC_LIMIT;
        }
    }
    uint8_t* payload = ohlc_alloc(&db->allocator, size);
    if (payload == NULL) {
        return OHLC_LIMIT;
    }
    ohlc_put_u64(payload, root->seq);
    ohlc_put_u64(payload + 8, root->ticker_count);
    ohlc_put_u64(payload + 16, db->dictionary_disk_offset);
    ohlc_put_u32(payload + 24, root->table_count);
    size_t position = 32;
    for (uint64_t id = 1; id <= root->table_count; id++) {
        const ohlc_table* table = ohlc_root_table(root, (uint32_t)id);
        ohlc_put_u32(payload + position, table->info.id);
        ohlc_put_u64(payload + position + 8, table->info.created_seq);
        ohlc_put_u64(payload + position + 16, table->disk_offset);
        size_t definition_size = ohlc_definition_encode(payload + position + 24, &table->info);
        ohlc_put_u32(payload + position + 4, (uint32_t)definition_size);
        position += 24u + definition_size;
    }
    status = record_write(db, db->catalog_fd, &db->catalog_size, OHLC_RECORD_CATALOG, payload,
                          position, offset);
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status load_dictionary(ohlc_db* db, uint64_t tail, uint64_t count, uint64_t parent) {
    if (count > UINT64_C(1) << 32 || count > SIZE_MAX / sizeof(uint64_t)) {
        return OHLC_CORRUPT;
    }
    uint64_t* offsets = ohlc_alloc(&db->allocator, (size_t)count * sizeof(*offsets));
    if (offsets == NULL) {
        return OHLC_LIMIT;
    }
    uint64_t offset = tail;
    ohlc_status status = OHLC_OK;
    for (uint64_t i = count; i != 0; i--) {
        uint8_t* payload = NULL;
        size_t size = 0;
        status =
            record_read(db, db->catalog_fd, offset, parent, OHLC_RECORD_TICKER, &payload, &size);
        if (status != OHLC_OK) {
            goto cleanup;
        }
        if (size < 17 || size > 4112 || ohlc_get_u32(payload + 8) != i - 1 ||
            ohlc_get_u32(payload + 12) != size - 16) {
            ohlc_free(&db->allocator, payload);
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        offsets[i - 1] = offset;
        parent = offset;
        offset = ohlc_get_u64(payload);
        ohlc_free(&db->allocator, payload);
    }
    if (offset != 0) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    for (uint64_t i = 0; i < count; i++) {
        uint8_t* payload = NULL;
        size_t size = 0;
        status = record_read(db, db->catalog_fd, offsets[i], db->catalog_size, OHLC_RECORD_TICKER,
                             &payload, &size);
        if (status != OHLC_OK) {
            goto cleanup;
        }
        ohlc_bytes ticker = {payload + 16, size - 16};
        ohlc_ticker_entry* entry = NULL;
        status = ohlc_dictionary_prepare(db, ticker, (uint32_t)i, &entry);
        if (status == OHLC_OK) {
            ohlc_dictionary_publish(db, entry);
        }
        ohlc_free(&db->allocator, payload);
        if (status != OHLC_OK) {
            goto cleanup;
        }
    }
    db->dictionary_disk_count = count;
    db->dictionary_disk_offset = tail;
cleanup:
    ohlc_free(&db->allocator, offsets);
    return status;
}

static void reset_recovered_dictionary(ohlc_db* db) {
    for (size_t i = 0; i < db->ticker_bucket_count; i++) {
        ohlc_ticker_entry* entry = db->ticker_buckets[i];
        while (entry != NULL) {
            ohlc_ticker_entry* next = entry->next;
            ohlc_free(&db->allocator, entry);
            entry = next;
        }
        db->ticker_buckets[i] = NULL;
    }
    for (size_t i = 0; i < db->ticker_page_count; i++) {
        memset(db->ticker_pages[i], 0, 1024u * sizeof(ohlc_ticker_entry*));
    }
    db->dictionary_count = 0;
    db->dictionary_disk_count = 0;
    db->dictionary_disk_offset = 0;
}

static ohlc_status load_catalog(ohlc_db* db, const ohlc_checkpoint_entry* checkpoint) {
    uint8_t* payload = NULL;
    size_t size = 0;
    ohlc_status status = record_read(db, db->catalog_fd, checkpoint->catalog, db->catalog_size,
                                     OHLC_RECORD_CATALOG, &payload, &size);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_root* root = NULL;
    if (size < 32 || ohlc_get_u64(payload) != checkpoint->seq ||
        ohlc_get_u32(payload + 24) > db->options.max_tables || ohlc_get_u32(payload + 28) != 0) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    root = ohlc_root_new(db);
    if (root == NULL) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    root->seq = checkpoint->seq;
    root->ticker_count = ohlc_get_u64(payload + 8);
    uint32_t table_count = ohlc_get_u32(payload + 24);
    size_t position = 32;
    for (uint64_t id = 1; id <= table_count; id++) {
        if (size - position < 24 || ohlc_get_u32(payload + position) != id) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        uint32_t definition_size = ohlc_get_u32(payload + position + 4);
        ohlc_table_info info = {0};
        info.id = (uint32_t)id;
        info.created_seq = ohlc_get_u64(payload + position + 8);
        uint64_t table_offset = ohlc_get_u64(payload + position + 16);
        position += 24;
        if (definition_size > size - position || info.created_seq == 0 ||
            info.created_seq > root->seq) {
            status = OHLC_CORRUPT;
            goto cleanup;
        }
        status = ohlc_definition_decode(payload + position, definition_size, &info);
        if (status != OHLC_OK) {
            goto cleanup;
        }
        for (uint64_t previous = 1; previous < id; previous++) {
            if (strcmp(ohlc_root_table(root, (uint32_t)previous)->info.name, info.name) == 0) {
                status = OHLC_CORRUPT;
                goto cleanup;
            }
        }
        position += definition_size;
        status = ohlc_root_add_table(db, root, &info);
        if (status != OHLC_OK) {
            goto cleanup;
        }
        ohlc_table* table = NULL;
        status = ohlc_root_edit_table(db, root, info.id, &table);
        if (status == OHLC_OK) {
            status = load_table(db, table, table_offset);
        }
        if (status != OHLC_OK) {
            goto cleanup;
        }
    }
    if (position != size) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    status =
        load_dictionary(db, ohlc_get_u64(payload + 16), root->ticker_count, checkpoint->catalog);
    if (status != OHLC_OK) {
        reset_recovered_dictionary(db);
        goto cleanup;
    }
    ohlc_root_publish(db, root);
    root = NULL;
cleanup:
    ohlc_root_release(db, root);
    ohlc_free(&db->allocator, payload);
    return status;
}

static ohlc_status checkpoint_read(ohlc_db* db, unsigned int slot, ohlc_checkpoint_entry* entry) {
    memset(entry, 0, sizeof(*entry));
    char name[16];
    snprintf(name, sizeof(name), "CURRENT.%u", slot);
    int fd = openat(db->directory_fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return errno == ENOENT ? OHLC_OK : OHLC_IO;
    }
    uint8_t bytes[OHLC_BLOCK_BYTES];
    ohlc_status status = ohlc_read_full(fd, bytes, sizeof(bytes), 0);
    close(fd);
    if (status == OHLC_IO) {
        return status;
    }
    if (status != OHLC_OK) {
        return OHLC_OK;
    }
    uint32_t crc = ohlc_get_u32(bytes + 4092);
    ohlc_put_u32(bytes + 4092, 0);
    if (memcmp(bytes, "OHLCUR4\0", 8) != 0 || ohlc_get_u32(bytes + 8) != OHLC_FORMAT_VERSION ||
        memcmp(bytes + 16, db->uuid, 16) != 0 || crc != ohlc_crc32c(0, bytes, sizeof(bytes)) ||
        ohlc_get_u32(bytes + 12) != 0 || ohlc_get_u32(bytes + 60) != 0) {
        return OHLC_OK;
    }
    for (size_t i = 72; i < 4092; i++) {
        if (bytes[i] != 0) {
            return OHLC_OK;
        }
    }
    entry->generation = ohlc_get_u64(bytes + 32);
    entry->seq = ohlc_get_u64(bytes + 40);
    entry->catalog = ohlc_get_u64(bytes + 48);
    entry->wal_id = ohlc_get_u32(bytes + 56);
    entry->wal_offset = ohlc_get_u64(bytes + 64);
    entry->valid = entry->generation != 0 && entry->wal_id != 0 &&
                   entry->wal_offset >= OHLC_BLOCK_BYTES && entry->wal_offset <= INT64_MAX &&
                   entry->wal_offset % 8u == 0;
    return OHLC_OK;
}

static ohlc_status checkpoint_write(ohlc_db* db, uint64_t seq, uint64_t catalog, uint32_t wal_id,
                                    uint64_t wal_offset) {
    uint8_t bytes[OHLC_BLOCK_BYTES] = {0};
    uint64_t generation = db->checkpoint_generation + 1;
    unsigned int slot = (unsigned int)((generation - 1) & 1u);
    memcpy(bytes, "OHLCUR4\0", 8);
    ohlc_put_u32(bytes + 8, OHLC_FORMAT_VERSION);
    memcpy(bytes + 16, db->uuid, 16);
    ohlc_put_u64(bytes + 32, generation);
    ohlc_put_u64(bytes + 40, seq);
    ohlc_put_u64(bytes + 48, catalog);
    ohlc_put_u32(bytes + 56, wal_id);
    ohlc_put_u64(bytes + 64, wal_offset);
    ohlc_put_u32(bytes + 4092, ohlc_crc32c(0, bytes, sizeof(bytes)));
    char name[16];
    snprintf(name, sizeof(name), "CURRENT.%u", slot);
    int fd = openat(db->directory_fd, name, O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) {
        return OHLC_IO;
    }
    ohlc_status status = ohlc_write_full(fd, bytes, sizeof(bytes), 0);
    if (status == OHLC_OK) {
        status = ohlc_sync(fd);
    }
    close(fd);
    if (status == OHLC_OK && fsync(db->directory_fd) != 0) {
        status = OHLC_IO;
    }
    if (status == OHLC_OK) {
        db->checkpoint_generation = generation;
        db->checkpoint_sequences[slot] = seq;
        db->checkpoint_wal_ids[slot] = wal_id;
    } else {
        db->failed = true;
    }
    return status;
}

static ohlc_status wal_reclaim(ohlc_db* db) {
    uint32_t retain = 0;
    for (size_t slot = 0; slot < 2; slot++) {
        uint32_t id = db->checkpoint_wal_ids[slot];
        if (id != 0 && (retain == 0 || id < retain)) {
            retain = id;
        }
    }
    if (retain == 0) {
        return OHLC_CORRUPT;
    }
    bool changed = false;
    ohlc_status status = OHLC_OK;
    while (db->wal_first_id < retain) {
        char name[64];
        snprintf(name, sizeof(name), "wal-%06u.log", db->wal_first_id);
        if (unlinkat(db->directory_fd, name, 0) != 0 && errno != ENOENT) {
            status = OHLC_IO;
            break;
        }
        db->wal_first_id++;
        changed = true;
    }
    /* Both durable CURRENT entries precede deletion. A crash can leave extra
     * segments, but it cannot remove either entry's required replay range. */
    if (changed && fsync(db->directory_fd) != 0) {
        status = OHLC_IO;
    }
    if (status != OHLC_OK) {
        db->failed = true;
    }
    return status;
}

static bool same_group(const ohlc_group* left, const ohlc_group* right) {
    return left->base == right->base && left->mask == right->mask && left->flags == right->flags &&
           left->volume == right->volume;
}

static ohlc_status rebase_group(ohlc_db* db, ohlc_table* table, uint32_t band, uint32_t group,
                                const ohlc_group* original, const ohlc_group* flushed) {
    const ohlc_group* current = ohlc_group_find(table, band, group);
    if (current == NULL || current->flags != OHLC_HOT_GROUP) {
        return OHLC_OK;
    }
    ohlc_group replacement = *flushed;
    if (!same_group(current, original)) {
        const ohlc_hot_group* before = (const ohlc_hot_group*)(uintptr_t)original->base;
        const ohlc_hot_group* latest = (const ohlc_hot_group*)(uintptr_t)current->base;
        if (!same_group(&before->backing, &latest->backing)) {
            return OHLC_OK;
        }
        ohlc_hot_group* rebased = ohlc_alloc(&db->allocator, sizeof(*rebased));
        if (rebased == NULL) {
            return OHLC_LIMIT;
        }
        atomic_init(&rebased->refs, 1);
        rebased->backing = *flushed;
        for (size_t i = 0; i < 16; i++) {
            if (latest->blocks[i] != before->blocks[i] && latest->blocks[i] != NULL) {
                rebased->blocks[i] = latest->blocks[i];
                atomic_fetch_add_explicit(&rebased->blocks[i]->refs, 1, memory_order_relaxed);
            }
        }
        replacement.base = (uint64_t)(uintptr_t)rebased;
        replacement.flags = OHLC_HOT_GROUP;
        replacement.mask = current->mask;
        replacement.volume = 0;
    }
    ohlc_group* target = NULL;
    ohlc_status status = ohlc_group_edit(db, table, band, group, &target);
    if (status == OHLC_OK) {
        ohlc_group_release(&db->allocator, target);
        *target = replacement;
    } else {
        ohlc_group_release(&db->allocator, &replacement);
    }
    return status;
}

static ohlc_status rebase_radix(ohlc_db* db, ohlc_table* table, const ohlc_table* saved,
                                const ohlc_band_node* node, uint32_t prefix) {
    if (node == NULL || node->disk_offset != 0) {
        return OHLC_OK;
    }
    for (uint16_t i = 0; i < node->size; i++) {
        if (node->children[i] == NULL) {
            continue;
        }
        if (node->level != 2) {
            unsigned int shift = node->level == 0 ? 17u : 8u;
            ohlc_status status =
                rebase_radix(db, table, saved, node->children[i], prefix | ((uint32_t)i << shift));
            if (status != OHLC_OK) {
                return status;
            }
            continue;
        }
        const ohlc_band* band = node->children[i];
        for (uint32_t page = 0; page < band->page_count; page++) {
            if (band->pages[page] == NULL) {
                continue;
            }
            for (uint32_t slot = 0; slot < OHLC_PAGE_ENTRIES; slot++) {
                const ohlc_group* original = &band->pages[page]->groups[slot];
                if (original->flags != OHLC_HOT_GROUP) {
                    continue;
                }
                uint32_t group = page * OHLC_PAGE_ENTRIES + slot;
                const ohlc_group* flushed = ohlc_group_find(saved, prefix | i, group);
                if (flushed == NULL || flushed->flags != 0) {
                    return OHLC_CORRUPT;
                }
                ohlc_status status = rebase_group(db, table, prefix | i, group, original, flushed);
                if (status != OHLC_OK) {
                    return status;
                }
            }
        }
    }
    return OHLC_OK;
}

static void merge_checkpoint(ohlc_db* db, const ohlc_root* source, ohlc_root* saved) {
    pthread_mutex_lock(&db->writer);
    if (db->root->seq == source->seq) {
        atomic_fetch_add_explicit(&saved->refs, 1, memory_order_relaxed);
        ohlc_root_publish(db, saved);
        pthread_mutex_unlock(&db->writer);
        return;
    }
    ohlc_root* merged = ohlc_root_copy(db, db->root);
    ohlc_status status = merged == NULL ? OHLC_LIMIT : OHLC_OK;
    for (uint64_t id = 1; status == OHLC_OK && id <= source->table_count; id++) {
        const ohlc_table* before = ohlc_root_table(source, (uint32_t)id);
        const ohlc_table* current = ohlc_root_table(db->root, (uint32_t)id);
        ohlc_table* persisted = (ohlc_table*)ohlc_root_table(saved, (uint32_t)id);
        if (current == before) {
            status = ohlc_root_set_table(db, merged, (uint32_t)id, persisted);
        } else if (before != persisted) {
            ohlc_table* target = NULL;
            status = ohlc_root_edit_table(db, merged, (uint32_t)id, &target);
            if (status == OHLC_OK) {
                status = rebase_radix(db, target, persisted, before->bands, 0);
            }
        }
    }
    if (status == OHLC_OK) {
        ohlc_root_publish(db, merged);
        merged = NULL;
    }
    pthread_mutex_unlock(&db->writer);
    /* A failed memory-only rebase leaves the current root intact. The saved
     * checkpoint is already durable and its WAL recovery boundary is valid. */
    ohlc_root_release(db, merged);
}

ohlc_status ohlc_checkpoint_run(ohlc_db* db) {
    if (db->checkpoint_generation == UINT64_MAX) {
        return OHLC_LIMIT;
    }
    pthread_mutex_lock(&db->writer);
    ohlc_root* source_root = ohlc_root_acquire(db);
    uint64_t wal_boundary = atomic_load_explicit(&db->wal_bytes, memory_order_relaxed);
    uint32_t wal_id = db->wal_id;
    uint64_t wal_offset = db->wal_size;
    pthread_mutex_unlock(&db->writer);
    ohlc_root* root = ohlc_root_copy(db, source_root);
    if (root == NULL) {
        ohlc_root_release(db, source_root);
        return OHLC_LIMIT;
    }
    ohlc_status status = OHLC_OK;
    uint8_t* buffer = NULL;
    for (uint64_t id = 1; id <= root->table_count; id++) {
        const ohlc_table* source = ohlc_root_table(source_root, (uint32_t)id);
        if (source->disk_offset != 0 || source->time_count == 0) {
            continue;
        }
        ohlc_table* table = NULL;
        status = ohlc_root_edit_table(db, root, (uint32_t)id, &table);
        if (status == OHLC_OK) {
            status = flush_radix(db, table, source->bands, 0, &buffer);
        }
        if (status == OHLC_OK) {
            status = save_table(db, table);
        }
        if (status != OHLC_OK) {
            goto cleanup;
        }
    }
    uint64_t catalog = 0;
    status = save_catalog(db, root, &catalog);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    for (uint64_t id = 1; id <= root->table_count; id++) {
        ohlc_table_files* files = db->files[id - 1];
        if (files == NULL) {
            continue;
        }
        for (size_t v = 0; v < files->volume_count; v++) {
            if (files->volumes[v] == NULL || !files->volumes[v]->dirty) {
                continue;
            }
            if (ohlc_sync(files->volumes[v]->data_fd) != OHLC_OK ||
                ohlc_sync(files->volumes[v]->meta_fd) != OHLC_OK) {
                status = OHLC_IO;
                db->failed = true;
                goto cleanup;
            }
            files->volumes[v]->dirty = false;
        }
        if (files->index_dirty && ohlc_sync(files->index_fd) != OHLC_OK) {
            status = OHLC_IO;
            db->failed = true;
            goto cleanup;
        }
        files->index_dirty = false;
    }
    if (ohlc_sync(db->catalog_fd) != OHLC_OK) {
        status = OHLC_IO;
        db->failed = true;
        goto cleanup;
    }
    status = checkpoint_write(db, root->seq, catalog, wal_id, wal_offset);
    if (status == OHLC_OK) {
        merge_checkpoint(db, source_root, root);
        ohlc_root_release(db, db->checkpoint_root);
        db->checkpoint_root = root;
        root = NULL;
        db->checkpoint_wal_bytes = wal_boundary;
        db->checkpoint_at_ms = ohlc_monotonic_ms();
        status = wal_reclaim(db);
    }
cleanup:
    ohlc_free(&db->allocator, buffer);
    ohlc_root_release(db, root);
    ohlc_root_release(db, source_root);
    return status;
}

ohlc_status ohlc_checkpoint_background(ohlc_db* db) {
    if (pthread_mutex_trylock(&db->checkpoint_mutex) != 0) {
        return OHLC_OK;
    }
    ohlc_root* root = ohlc_root_acquire(db);
    const ohlc_root* saved = db->checkpoint_root;
    bool changed = saved == NULL || saved->seq != root->seq;
    uint64_t written = atomic_load_explicit(&db->wal_bytes, memory_order_relaxed);
    bool due = atomic_load_explicit(&db->allocator.used, memory_order_relaxed) >
                   db->options.memory_limit / 2 ||
               written - db->checkpoint_wal_bytes >= 64u * 1024u * 1024u ||
               ohlc_monotonic_ms() - db->checkpoint_at_ms >= 300000;
    for (uint64_t id = 1; changed && !due && id <= root->table_count; id++) {
        const ohlc_table* table = ohlc_root_table(root, (uint32_t)id);
        const ohlc_table* previous = saved == NULL ? NULL : ohlc_root_table(saved, (uint32_t)id);
        uint64_t old_count = previous == NULL ? 0 : previous->time_count;
        due = table->time_count / 128u > old_count / 128u;
    }
    ohlc_root_release(db, root);
    ohlc_status status = OHLC_OK;
    if (changed && due) {
        status = db->failed ? OHLC_IO : ohlc_checkpoint_run(db);
    }
    pthread_mutex_unlock(&db->checkpoint_mutex);
    return status;
}

static ohlc_status wal_create(ohlc_db* db, uint32_t id, uint64_t start_seq) {
    char name[64];
    snprintf(name, sizeof(name), "wal-%06u.log", id);
    int fd =
        openat(db->directory_fd, name, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) {
        return OHLC_IO;
    }
    ohlc_status status = ohlc_storage_header(db, fd, 2, 0, id, start_seq, true);
    if (status == OHLC_OK) {
        status = ohlc_sync(fd);
    }
    if (status == OHLC_OK && fsync(db->directory_fd) != 0) {
        status = OHLC_IO;
    }
    if (status != OHLC_OK) {
        close(fd);
        db->failed = true;
        return status;
    }
    if (db->wal_fd >= 0) {
        close(db->wal_fd);
    }
    db->wal_fd = fd;
    db->wal_id = id;
    db->wal_size = OHLC_BLOCK_BYTES;
    return OHLC_OK;
}

ohlc_status ohlc_wal_encode(ohlc_db* db, uint16_t type, uint32_t table, uint32_t count,
                            uint64_t seq, const uint8_t* payload, size_t size, uint8_t** output,
                            size_t* length) {
    if (size > OHLC_FRAME_LIMIT - 103u) {
        return OHLC_LIMIT;
    }
    size_t total = 96u + ((size + 7u) & ~(size_t)7u);
    uint8_t* frame = ohlc_alloc(&db->allocator, total);
    if (frame == NULL) {
        return OHLC_LIMIT;
    }
    memcpy(frame, "OHLCWTX4", 8);
    ohlc_put_u16(frame + 8, OHLC_FORMAT_VERSION);
    ohlc_put_u16(frame + 10, type);
    ohlc_put_u64(frame + 16, total);
    ohlc_put_u64(frame + 24, seq);
    ohlc_put_u32(frame + 32, count);
    ohlc_put_u32(frame + 36, table);
    ohlc_put_u64(frame + 40, size);
    ohlc_put_u32(frame + 48, ohlc_crc32c(0, frame, 64));
    memcpy(frame + 64, payload, size);
    uint8_t* footer = frame + total - 32;
    memcpy(footer, "OHLCEND4", 8);
    ohlc_put_u64(footer + 8, total);
    ohlc_put_u64(footer + 16, seq);
    ohlc_put_u32(footer + 24, ohlc_crc32c(0, frame, total));
    *output = frame;
    *length = total;
    return OHLC_OK;
}

ohlc_status ohlc_wal_sync(ohlc_db* db) {
    if (db->failed || ohlc_sync(db->wal_fd) != OHLC_OK) {
        db->failed = true;
        return OHLC_OUTCOME_UNKNOWN;
    }
    atomic_fetch_add_explicit(&db->wal_syncs, 1, memory_order_relaxed);
    return OHLC_OK;
}

ohlc_status ohlc_wal_write(ohlc_db* db, const uint8_t* frame, size_t size, uint64_t seq) {
    if (db->wal_size > db->options.wal_segment_bytes - size) {
        if (db->wal_id == UINT32_MAX) {
            return OHLC_LIMIT;
        }
        /* A sync group may cross a segment. Its previous segment must become
         * durable before wal_create closes that descriptor. */
        ohlc_status status = ohlc_wal_sync(db);
        if (status == OHLC_OK) {
            status = wal_create(db, db->wal_id + 1, seq);
        }
        if (status != OHLC_OK) {
            db->failed = true;
            return OHLC_OUTCOME_UNKNOWN;
        }
    }
    if (db->failed || ohlc_write_full(db->wal_fd, frame, size, db->wal_size) != OHLC_OK) {
        db->failed = true;
        return OHLC_OUTCOME_UNKNOWN;
    }
    db->wal_size += size;
    atomic_fetch_add_explicit(&db->wal_bytes, size, memory_order_relaxed);
    return OHLC_OK;
}

ohlc_status ohlc_wal_append(ohlc_db* db, uint16_t type, uint32_t table, uint32_t count,
                            uint64_t seq, const uint8_t* payload, size_t size) {
    uint8_t* frame = NULL;
    size_t length = 0;
    ohlc_status status =
        ohlc_wal_encode(db, type, table, count, seq, payload, size, &frame, &length);
    if (status == OHLC_OK) {
        status = ohlc_wal_write(db, frame, length, seq);
    }
    if (status == OHLC_OK) {
        status = ohlc_wal_sync(db);
    }
    ohlc_free(&db->allocator, frame);
    return status;
}

static ohlc_status replay_frame(ohlc_db* db, const uint8_t* frame) {
    uint16_t type = ohlc_get_u16(frame + 10);
    uint32_t count = ohlc_get_u32(frame + 32);
    uint32_t table = ohlc_get_u32(frame + 36);
    size_t size = (size_t)ohlc_get_u64(frame + 40);
    uint64_t seq = ohlc_get_u64(frame + 24);
    const uint8_t* payload = frame + 64;
    if (seq != db->root->seq + 1) {
        return OHLC_CORRUPT;
    }
    ohlc_root* candidate = NULL;
    ohlc_status status = OHLC_OK;
    if (type == 2) {
        if (count == 0 || count > OHLC_MAX_BATCH_ROWS || size != (size_t)count * 40u) {
            return OHLC_CORRUPT;
        }
        status = ohlc_prepare_write(db, db->root, table, payload, count, &candidate);
        if (status == OHLC_NOT_FOUND || status == OHLC_INVALID) {
            status = OHLC_CORRUPT;
        }
    } else {
        if (count != 1) {
            return OHLC_CORRUPT;
        }
        candidate = ohlc_root_copy(db, db->root);
        if (candidate == NULL) {
            return OHLC_LIMIT;
        }
        candidate->seq = seq;
        if (type == 1) {
            if (table != 0 || size < 9 || size > 4104 || ohlc_get_u32(payload + 4) != size - 8) {
                status = OHLC_CORRUPT;
            } else {
                ohlc_bytes ticker = {payload + 8, size - 8};
                ohlc_ticker_entry* entry = NULL;
                status = ohlc_dictionary_prepare(db, ticker, ohlc_get_u32(payload), &entry);
                if (status == OHLC_OK) {
                    ohlc_dictionary_publish(db, entry);
                    candidate->ticker_count++;
                }
            }
        } else if (type == 3) {
            ohlc_table_info info = {0};
            info.id = table;
            info.created_seq = seq;
            status = ohlc_definition_decode(payload, size, &info);
            if (status == OHLC_OK && table != candidate->table_count + 1) {
                status = OHLC_CORRUPT;
            }
            for (uint64_t id = 1; status == OHLC_OK && id <= candidate->table_count; id++) {
                if (strcmp(ohlc_root_table(candidate, (uint32_t)id)->info.name, info.name) == 0) {
                    status = OHLC_CORRUPT;
                }
            }
            if (status == OHLC_OK) {
                status = ohlc_root_add_table(db, candidate, &info);
            }
        } else {
            status = OHLC_CORRUPT;
        }
    }
    if (status == OHLC_OK) {
        ohlc_root_publish(db, candidate);
    } else {
        ohlc_root_release(db, candidate);
    }
    return status;
}

static ohlc_status wal_bounds(ohlc_db* db, uint32_t* last) {
    int directory_fd = openat(db->directory_fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory_fd < 0) {
        return OHLC_IO;
    }
    DIR* directory = fdopendir(directory_fd);
    if (directory == NULL) {
        close(directory_fd);
        return OHLC_IO;
    }
    *last = 0;
    db->wal_first_id = UINT32_MAX;
    ohlc_status status = OHLC_OK;
    while (true) {
        errno = 0;
        struct dirent* entry = readdir(directory);
        if (entry == NULL) {
            status = errno == 0 ? OHLC_OK : OHLC_IO;
            break;
        }
        if (strncmp(entry->d_name, "wal-", 4) != 0) {
            continue;
        }
        const char* p = entry->d_name + 4;
        uint64_t id = 0;
        while (*p >= '0' && *p <= '9' && id <= UINT32_MAX) {
            id = id * 10u + (unsigned int)(*p++ - '0');
        }
        if (id == 0 || id > UINT32_MAX || strcmp(p, ".log") != 0) {
            continue;
        }
        char canonical[64];
        snprintf(canonical, sizeof(canonical), "wal-%06u.log", (uint32_t)id);
        if (strcmp(canonical, entry->d_name) == 0) {
            if (id > *last) {
                *last = (uint32_t)id;
            }
            if (id < db->wal_first_id) {
                db->wal_first_id = (uint32_t)id;
            }
        }
    }
    closedir(directory);
    return status;
}

static ohlc_status replay_wal(ohlc_db* db, const ohlc_checkpoint_entry* checkpoint) {
    uint32_t last = 0;
    ohlc_status status = wal_bounds(db, &last);
    if (status != OHLC_OK) {
        return status;
    }
    if (last < checkpoint->wal_id) {
        return OHLC_CORRUPT;
    }
    uint64_t expected_seq = checkpoint->seq + 1;
    for (uint64_t id = checkpoint->wal_id; id <= last; id++) {
        char name[64];
        snprintf(name, sizeof(name), "wal-%06u.log", (uint32_t)id);
        int fd = openat(db->directory_fd, name, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0) {
            return errno == ENOENT ? OHLC_CORRUPT : OHLC_IO;
        }
        struct stat info;
        if (fstat(fd, &info) != 0 || info.st_size < 0) {
            close(fd);
            return OHLC_IO;
        }
        status = ohlc_storage_header(db, fd, 2, 0, (uint32_t)id, 0, false);
        if (status == OHLC_CORRUPT && id == last && id > checkpoint->wal_id &&
            info.st_size < OHLC_BLOCK_BYTES && expected_seq == db->root->seq + 1) {
            /* No transaction can be acknowledged before a new WAL header and
             * its directory entry are synced. Repair only this empty tail. */
            status = ohlc_storage_header(db, fd, 2, 0, (uint32_t)id, expected_seq, true);
            if (status == OHLC_OK) {
                status = ohlc_sync(fd);
            }
            if (status == OHLC_OK && fsync(db->directory_fd) != 0) {
                status = OHLC_IO;
            }
            info.st_size = OHLC_BLOCK_BYTES;
        }
        uint64_t position = id == checkpoint->wal_id ? checkpoint->wal_offset : OHLC_BLOCK_BYTES;
        if (status == OHLC_OK && position > (uint64_t)info.st_size) {
            status = OHLC_CORRUPT;
        }
        uint8_t start[8];
        if (status == OHLC_OK) {
            status = ohlc_read_full(fd, start, sizeof(start), 64);
            if (status == OHLC_OK) {
                uint64_t first_seq = ohlc_get_u64(start);
                if (first_seq == 0 || (position == OHLC_BLOCK_BYTES && first_seq != expected_seq) ||
                    (position != OHLC_BLOCK_BYTES && first_seq > checkpoint->seq)) {
                    status = OHLC_CORRUPT;
                }
            }
        }
        while (status == OHLC_OK && position < (uint64_t)info.st_size) {
            uint64_t remaining = (uint64_t)info.st_size - position;
            if (remaining < 64) {
                status = id == last ? OHLC_OK : OHLC_CORRUPT;
                break;
            }
            uint8_t header[64];
            status = ohlc_read_full(fd, header, sizeof(header), position);
            if (status != OHLC_OK) {
                break;
            }
            uint32_t header_crc = ohlc_get_u32(header + 48);
            ohlc_put_u32(header + 48, 0);
            uint64_t total = ohlc_get_u64(header + 16);
            uint64_t payload_size = ohlc_get_u64(header + 40);
            if (memcmp(header, "OHLCWTX4", 8) != 0 ||
                ohlc_get_u16(header + 8) != OHLC_FORMAT_VERSION || ohlc_get_u32(header + 12) != 0 ||
                total < 96 || total > OHLC_FRAME_LIMIT || total % 8u != 0 ||
                payload_size > total - 96 || total != 96u + ((payload_size + 7u) & ~UINT64_C(7)) ||
                header_crc != ohlc_crc32c(0, header, sizeof(header)) ||
                ohlc_get_u32(header + 52) != 0 || ohlc_get_u64(header + 56) != 0) {
                status = OHLC_CORRUPT;
                break;
            }
            if (total > remaining) {
                status = id == last ? OHLC_OK : OHLC_CORRUPT;
                break;
            }
            uint8_t* frame = ohlc_alloc(&db->allocator, (size_t)total);
            if (frame == NULL) {
                status = OHLC_LIMIT;
                break;
            }
            status = ohlc_read_full(fd, frame, (size_t)total, position);
            if (status == OHLC_OK) {
                uint8_t* footer = frame + total - 32;
                uint32_t crc = ohlc_get_u32(footer + 24);
                ohlc_put_u32(footer + 24, 0);
                if (memcmp(footer, "OHLCEND4", 8) != 0 || ohlc_get_u64(footer + 8) != total ||
                    ohlc_get_u64(footer + 16) != expected_seq ||
                    ohlc_get_u64(header + 24) != expected_seq || expected_seq == 0 ||
                    ohlc_get_u32(footer + 28) != 0 || crc != ohlc_crc32c(0, frame, (size_t)total)) {
                    status = OHLC_CORRUPT;
                }
                for (uint64_t i = 64u + payload_size; i < total - 32; i++) {
                    if (frame[i] != 0) {
                        status = OHLC_CORRUPT;
                    }
                }
                if (status == OHLC_OK && expected_seq > db->root->seq) {
                    status = replay_frame(db, frame);
                }
            }
            ohlc_free(&db->allocator, frame);
            if (status == OHLC_OK) {
                expected_seq++;
                position += total;
            }
        }
        if (status != OHLC_OK) {
            close(fd);
            return status;
        }
        if (id == last) {
            if (position < (uint64_t)info.st_size &&
                (ftruncate(fd, (off_t)position) != 0 || ohlc_sync(fd) != OHLC_OK)) {
                close(fd);
                return OHLC_IO;
            }
            db->wal_fd = fd;
            db->wal_id = (uint32_t)id;
            db->wal_size = position;
        } else {
            close(fd);
        }
    }
    return expected_seq - 1 == db->root->seq ? OHLC_OK : OHLC_CORRUPT;
}

ohlc_status ohlc_recover(ohlc_db* db, bool fresh) {
    if (fresh) {
        db->wal_first_id = 1;
        ohlc_status status = wal_create(db, 1, 1);
        if (status == OHLC_OK) {
            status = ohlc_checkpoint_run(db);
        }
        if (status == OHLC_OK) {
            status = ohlc_checkpoint_run(db);
        }
        return status;
    }
    ohlc_checkpoint_entry entries[2];
    for (unsigned int i = 0; i < 2; i++) {
        ohlc_status status = checkpoint_read(db, i, &entries[i]);
        if (status != OHLC_OK) {
            return status;
        }
    }
    unsigned int first = entries[1].generation > entries[0].generation ? 1u : 0u;
    unsigned int selected = first;
    ohlc_status status = OHLC_CORRUPT;
    for (unsigned int attempt = 0; attempt < 2; attempt++) {
        unsigned int slot = first ^ attempt;
        if (!entries[slot].valid) {
            continue;
        }
        status = load_catalog(db, &entries[slot]);
        if (status == OHLC_OK) {
            selected = slot;
            db->checkpoint_generation = entries[slot].generation;
            for (unsigned int i = 0; i < 2; i++) {
                db->checkpoint_sequences[i] = entries[i].valid ? entries[i].seq : 0;
                db->checkpoint_wal_ids[i] = entries[i].valid ? entries[i].wal_id : 0;
            }
            break;
        }
        if (status == OHLC_LIMIT || status == OHLC_IO) {
            return status;
        }
        entries[slot].valid = false;
    }
    if (status == OHLC_OK) {
        db->checkpoint_root = ohlc_root_acquire(db);
        db->checkpoint_at_ms = ohlc_monotonic_ms();
        status = replay_wal(db, &entries[selected]);
        if (status == OHLC_OK) {
            status = wal_reclaim(db);
        }
    }
    return status;
}

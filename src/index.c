/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <stddef.h>
#include <string.h>

static bool last_reference(_Atomic uint32_t* refs) {
    return atomic_fetch_sub_explicit(refs, 1, memory_order_acq_rel) == 1;
}

static bool unique_reference(const _Atomic uint32_t* refs) {
    return atomic_load_explicit(refs, memory_order_acquire) == 1;
}

void ohlc_time_retain(ohlc_time_node* node) {
    if (node != NULL) {
        atomic_fetch_add_explicit(&node->refs, 1, memory_order_relaxed);
    }
}

void ohlc_time_release(ohlc_allocator* a, ohlc_time_node* node) {
    if (node == NULL || !last_reference(&node->refs)) {
        return;
    }
    if (!node->leaf) {
        for (uint16_t i = 0; i <= node->count; i++) {
            ohlc_time_release(a, node->body.branch.children[i]);
        }
    }
    ohlc_free(a, node);
}

static ohlc_time_node* time_node_new(ohlc_allocator* a, bool leaf) {
    ohlc_time_node* node = ohlc_alloc(a, sizeof(*node));
    if (node != NULL) {
        atomic_init(&node->refs, 1);
        atomic_init(&node->disk_offset, 0);
        node->leaf = (uint8_t)leaf;
    }
    return node;
}

static ohlc_status time_node_edit(ohlc_allocator* a, ohlc_time_node** reference) {
    ohlc_time_node* source = *reference;
    if (unique_reference(&source->refs)) {
        source->disk_offset = 0;
        return OHLC_OK;
    }
    ohlc_time_node* node = time_node_new(a, source->leaf != 0);
    if (node == NULL) {
        return OHLC_LIMIT;
    }
    node->count = source->count;
    memcpy(&node->body, &source->body, sizeof(node->body));
    if (!node->leaf) {
        for (uint16_t i = 0; i <= node->count; i++) {
            ohlc_time_retain(node->body.branch.children[i]);
        }
    }
    ohlc_time_release(a, source);
    *reference = node;
    return OHLC_OK;
}

static uint16_t lower_bound(const uint32_t* keys, uint16_t count, uint32_t key) {
    uint16_t low = 0;
    uint16_t high = count;
    while (low < high) {
        uint16_t middle = (uint16_t)(low + (high - low) / 2);
        if (keys[middle] < key) {
            low = (uint16_t)(middle + 1);
        } else {
            high = middle;
        }
    }
    return low;
}

static uint16_t branch_position(const ohlc_time_node* node, uint32_t key) {
    uint16_t position = lower_bound(node->body.branch.keys, node->count, key);
    if (position < node->count && node->body.branch.keys[position] == key) {
        position++;
    }
    return position;
}

bool ohlc_time_find(const ohlc_time_node* root, uint32_t key, uint32_t* code) {
    const ohlc_time_node* node = root;
    while (node != NULL && !node->leaf) {
        node = node->body.branch.children[branch_position(node, key)];
    }
    if (node == NULL) {
        return false;
    }
    uint16_t position = lower_bound(node->body.values.keys, node->count, key);
    if (position == node->count || node->body.values.keys[position] != key) {
        return false;
    }
    *code = node->body.values.codes[position];
    return true;
}

/* Splits transfer child ownership. On failure the private candidate may have
 * changed; its caller must discard the entire candidate, never publish it. */
static ohlc_status time_insert_node(ohlc_allocator* a, ohlc_time_node** reference, uint32_t key,
                                    uint32_t code, ohlc_time_node** right, uint32_t* separator) {
    *right = NULL;
    ohlc_status status = time_node_edit(a, reference);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_time_node* node = *reference;
    if (node->leaf) {
        uint16_t position = lower_bound(node->body.values.keys, node->count, key);
        if (position < node->count && node->body.values.keys[position] == key) {
            return node->body.values.codes[position] == code ? OHLC_OK : OHLC_CORRUPT;
        }
        if (node->count < OHLC_TIME_CAPACITY) {
            size_t bytes = (size_t)(node->count - position) * sizeof(uint32_t);
            memmove(node->body.values.keys + position + 1, node->body.values.keys + position,
                    bytes);
            memmove(node->body.values.codes + position + 1, node->body.values.codes + position,
                    bytes);
            node->body.values.keys[position] = key;
            node->body.values.codes[position] = code;
            node->count++;
            return OHLC_OK;
        }
        ohlc_time_node* sibling = time_node_new(a, true);
        if (sibling == NULL) {
            return OHLC_LIMIT;
        }
        uint32_t keys[OHLC_TIME_CAPACITY + 1];
        uint32_t codes[OHLC_TIME_CAPACITY + 1];
        memcpy(keys, node->body.values.keys, (size_t)position * 4);
        memcpy(codes, node->body.values.codes, (size_t)position * 4);
        keys[position] = key;
        codes[position] = code;
        memcpy(keys + position + 1, node->body.values.keys + position,
               (OHLC_TIME_CAPACITY - position) * 4);
        memcpy(codes + position + 1, node->body.values.codes + position,
               (OHLC_TIME_CAPACITY - position) * 4);
        node->count = OHLC_TIME_CAPACITY / 2;
        sibling->count = (uint16_t)(OHLC_TIME_CAPACITY + 1u - node->count);
        memcpy(node->body.values.keys, keys, (size_t)node->count * 4);
        memcpy(node->body.values.codes, codes, (size_t)node->count * 4);
        memcpy(sibling->body.values.keys, keys + node->count, (size_t)sibling->count * 4);
        memcpy(sibling->body.values.codes, codes + node->count, (size_t)sibling->count * 4);
        *separator = sibling->body.values.keys[0];
        *right = sibling;
        return OHLC_OK;
    }
    uint16_t position = branch_position(node, key);
    ohlc_time_node* child_right = NULL;
    uint32_t child_separator = 0;
    status = time_insert_node(a, &node->body.branch.children[position], key, code, &child_right,
                              &child_separator);
    if (status != OHLC_OK || child_right == NULL) {
        return status;
    }
    if (node->count < OHLC_BRANCH_CAPACITY) {
        size_t tail = (size_t)(node->count - position);
        memmove(node->body.branch.keys + position + 1, node->body.branch.keys + position,
                tail * sizeof(uint32_t));
        memmove(node->body.branch.children + position + 2,
                node->body.branch.children + position + 1, tail * sizeof(ohlc_time_node*));
        node->body.branch.keys[position] = child_separator;
        node->body.branch.children[position + 1] = child_right;
        node->count++;
        return OHLC_OK;
    }
    ohlc_time_node* sibling = time_node_new(a, false);
    if (sibling == NULL) {
        ohlc_time_release(a, child_right);
        return OHLC_LIMIT;
    }
    uint32_t keys[OHLC_BRANCH_CAPACITY + 1];
    ohlc_time_node* children[OHLC_BRANCH_CAPACITY + 2];
    memcpy(keys, node->body.branch.keys, (size_t)position * sizeof(uint32_t));
    memcpy(children, node->body.branch.children, ((size_t)position + 1) * sizeof(children[0]));
    keys[position] = child_separator;
    children[position + 1] = child_right;
    memcpy(keys + position + 1, node->body.branch.keys + position,
           (OHLC_BRANCH_CAPACITY - position) * sizeof(uint32_t));
    memcpy(children + position + 2, node->body.branch.children + position + 1,
           (OHLC_BRANCH_CAPACITY - position) * sizeof(children[0]));
    uint16_t middle = (OHLC_BRANCH_CAPACITY + 1) / 2;
    node->count = middle;
    sibling->count = (uint16_t)(OHLC_BRANCH_CAPACITY - middle);
    memset(&node->body, 0, sizeof(node->body));
    memcpy(node->body.branch.keys, keys, (size_t)node->count * sizeof(uint32_t));
    memcpy(node->body.branch.children, children, ((size_t)node->count + 1) * sizeof(children[0]));
    memcpy(sibling->body.branch.keys, keys + middle + 1, (size_t)sibling->count * sizeof(uint32_t));
    memcpy(sibling->body.branch.children, children + middle + 1,
           ((size_t)sibling->count + 1) * sizeof(children[0]));
    *separator = keys[middle];
    *right = sibling;
    return OHLC_OK;
}

ohlc_status ohlc_time_insert(ohlc_allocator* a, ohlc_time_node** root, uint32_t key,
                             uint32_t code) {
    if (*root == NULL) {
        *root = time_node_new(a, true);
        if (*root == NULL) {
            return OHLC_LIMIT;
        }
    }
    ohlc_time_node* right = NULL;
    uint32_t separator = 0;
    ohlc_status status = time_insert_node(a, root, key, code, &right, &separator);
    if (status != OHLC_OK || right == NULL) {
        return status;
    }
    ohlc_time_node* parent = time_node_new(a, false);
    if (parent == NULL) {
        ohlc_time_release(a, right);
        return OHLC_LIMIT;
    }
    parent->count = 1;
    parent->body.branch.keys[0] = separator;
    parent->body.branch.children[0] = *root;
    parent->body.branch.children[1] = right;
    *root = parent;
    return OHLC_OK;
}

void ohlc_time_seek(const ohlc_time_node* root, uint32_t key, ohlc_time_iterator* iterator) {
    memset(iterator, 0, sizeof(*iterator));
    const ohlc_time_node* node = root;
    while (node != NULL) {
        uint8_t depth = iterator->depth++;
        iterator->nodes[depth] = node;
        if (node->leaf) {
            iterator->positions[depth] = lower_bound(node->body.values.keys, node->count, key);
            return;
        }
        uint16_t position = branch_position(node, key);
        iterator->positions[depth] = position;
        node = node->body.branch.children[position];
    }
}

size_t ohlc_time_span(ohlc_time_iterator* iterator, const uint32_t** keys, const uint32_t** codes) {
    while (iterator->depth != 0) {
        uint8_t depth = (uint8_t)(iterator->depth - 1);
        const ohlc_time_node* node = iterator->nodes[depth];
        uint16_t position = iterator->positions[depth];
        if (node->leaf && position < node->count) {
            *keys = node->body.values.keys + position;
            *codes = node->body.values.codes + position;
            return (size_t)node->count - position;
        }
        iterator->depth--;
        while (iterator->depth != 0) {
            depth = (uint8_t)(iterator->depth - 1);
            node = iterator->nodes[depth];
            position = (uint16_t)(iterator->positions[depth] + 1);
            if (position <= node->count) {
                iterator->positions[depth] = position;
                node = node->body.branch.children[position];
                while (true) {
                    depth = iterator->depth++;
                    iterator->nodes[depth] = node;
                    iterator->positions[depth] = 0;
                    if (node->leaf) {
                        break;
                    }
                    node = node->body.branch.children[0];
                }
                break;
            }
            iterator->depth--;
        }
    }
    *keys = NULL;
    *codes = NULL;
    return 0;
}

void ohlc_time_advance(ohlc_time_iterator* iterator, size_t count) {
    uint8_t depth = (uint8_t)(iterator->depth - 1);
    iterator->positions[depth] = (uint16_t)(iterator->positions[depth] + count);
}

bool ohlc_time_next(ohlc_time_iterator* iterator, uint32_t* key, uint32_t* code) {
    const uint32_t* keys = NULL;
    const uint32_t* codes = NULL;
    if (ohlc_time_span(iterator, &keys, &codes) == 0) {
        return false;
    }
    *key = keys[0];
    *code = codes[0];
    ohlc_time_advance(iterator, 1);
    return true;
}

ohlc_status ohlc_table_time_add(ohlc_db* db, ohlc_table* table, uint32_t key, uint32_t* code) {
    if (ohlc_time_find(table->times, key, code)) {
        return OHLC_OK;
    }
    if (table->time_count > UINT32_MAX) {
        return OHLC_LIMIT;
    }
    *code = (uint32_t)table->time_count;
    size_t page_number = *code / 1024u;
    if (page_number == table->time_page_count) {
        ohlc_time_page** pages = ohlc_alloc(&db->allocator, (page_number + 1) * sizeof(*pages));
        if (pages == NULL) {
            return OHLC_LIMIT;
        }
        if (table->time_page_count != 0) {
            memcpy(pages, table->time_pages, table->time_page_count * sizeof(*pages));
        }
        ohlc_free(&db->allocator, table->time_pages);
        table->time_pages = pages;
        table->time_page_count++;
    }
    ohlc_time_page* page = table->time_pages[page_number];
    if (page == NULL || !unique_reference(&page->refs)) {
        ohlc_time_page* copy = ohlc_alloc(&db->allocator, sizeof(*copy));
        if (copy == NULL) {
            return OHLC_LIMIT;
        }
        atomic_init(&copy->refs, 1);
        atomic_init(&copy->disk_offset, 0);
        if (page != NULL) {
            memcpy(copy->keys, page->keys, sizeof(copy->keys));
            if (last_reference(&page->refs)) {
                ohlc_free(&db->allocator, page);
            }
        }
        table->time_pages[page_number] = copy;
        page = copy;
    }
    ohlc_status status = ohlc_time_insert(&db->allocator, &table->times, key, *code);
    if (status != OHLC_OK) {
        return status;
    }
    page->disk_offset = 0;
    page->keys[*code % 1024u] = key;
    table->time_count++;
    return OHLC_OK;
}

void ohlc_group_retain(ohlc_group* group) {
    if (group->flags == OHLC_HOT_GROUP) {
        ohlc_hot_group* hot = (ohlc_hot_group*)(uintptr_t)group->base;
        atomic_fetch_add_explicit(&hot->refs, 1, memory_order_relaxed);
    }
}

void ohlc_group_release(ohlc_allocator* a, ohlc_group* group) {
    if (group->flags != OHLC_HOT_GROUP) {
        return;
    }
    ohlc_hot_group* hot = (ohlc_hot_group*)(uintptr_t)group->base;
    if (last_reference(&hot->refs)) {
        for (unsigned int i = 0; i < 16; i++) {
            ohlc_block* block = hot->blocks[i];
            if (block != NULL && last_reference(&block->refs)) {
                ohlc_free(a, block);
            }
        }
        ohlc_free(a, hot);
    }
}

static void group_page_release(ohlc_allocator* a, ohlc_group_page* page) {
    if (page != NULL && last_reference(&page->refs)) {
        for (size_t i = 0; i < OHLC_PAGE_ENTRIES; i++) {
            ohlc_group_release(a, &page->groups[i]);
        }
        ohlc_free(a, page);
    }
}

void ohlc_band_release(ohlc_allocator* a, ohlc_band* band) {
    if (band != NULL && last_reference(&band->refs)) {
        for (uint32_t i = 0; i < band->page_count; i++) {
            group_page_release(a, band->pages[i]);
        }
        ohlc_free(a, band->pages);
        ohlc_free(a, band);
    }
}

void ohlc_band_node_retain(ohlc_band_node* node) {
    if (node != NULL) {
        atomic_fetch_add_explicit(&node->refs, 1, memory_order_relaxed);
    }
}

void ohlc_band_node_release(ohlc_allocator* a, ohlc_band_node* node) {
    if (node == NULL || !last_reference(&node->refs)) {
        return;
    }
    for (uint16_t i = 0; i < node->size; i++) {
        if (node->level == 2) {
            ohlc_band_release(a, node->children[i]);
        } else {
            ohlc_band_node_release(a, node->children[i]);
        }
    }
    ohlc_free(a, node);
}

static ohlc_status band_node_edit(ohlc_allocator* a, ohlc_band_node** reference, uint8_t level) {
    ohlc_band_node* source = *reference;
    if (source != NULL && unique_reference(&source->refs)) {
        source->disk_offset = 0;
        return OHLC_OK;
    }
    uint16_t size = level == 1 ? 512 : 256;
    ohlc_band_node* copy = ohlc_alloc(a, sizeof(*copy) + (size_t)size * sizeof(void*));
    if (copy == NULL) {
        return OHLC_LIMIT;
    }
    atomic_init(&copy->refs, 1);
    atomic_init(&copy->disk_offset, 0);
    copy->level = level;
    copy->size = size;
    if (source != NULL) {
        memcpy(copy->children, source->children, (size_t)size * sizeof(void*));
        memcpy(copy->bitmap, source->bitmap, sizeof(copy->bitmap));
        for (uint16_t i = 0; i < size; i++) {
            if (copy->children[i] != NULL) {
                if (level == 2) {
                    ohlc_band* band = copy->children[i];
                    atomic_fetch_add_explicit(&band->refs, 1, memory_order_relaxed);
                } else {
                    ohlc_band_node_retain(copy->children[i]);
                }
            }
        }
        ohlc_band_node_release(a, source);
    }
    *reference = copy;
    return OHLC_OK;
}

static ohlc_status band_edit(ohlc_allocator* a, ohlc_band** reference, uint32_t pages_needed) {
    ohlc_band* source = *reference;
    if (source == NULL || !unique_reference(&source->refs)) {
        ohlc_band* copy = ohlc_alloc(a, sizeof(*copy));
        if (copy == NULL) {
            return OHLC_LIMIT;
        }
        atomic_init(&copy->refs, 1);
        atomic_init(&copy->disk_offset, 0);
        uint32_t count = source == NULL ? 0 : source->page_count;
        copy->page_count = count > pages_needed ? count : pages_needed;
        copy->pages = ohlc_alloc(a, (size_t)copy->page_count * sizeof(*copy->pages));
        if (copy->pages == NULL) {
            ohlc_free(a, copy);
            return OHLC_LIMIT;
        }
        for (uint32_t i = 0; i < count; i++) {
            copy->pages[i] = source->pages[i];
            if (copy->pages[i] != NULL) {
                atomic_fetch_add_explicit(&copy->pages[i]->refs, 1, memory_order_relaxed);
            }
        }
        ohlc_band_release(a, source);
        *reference = copy;
        return OHLC_OK;
    }
    source->disk_offset = 0;
    if (source->page_count < pages_needed) {
        ohlc_group_page** pages = ohlc_alloc(a, (size_t)pages_needed * sizeof(*pages));
        if (pages == NULL) {
            return OHLC_LIMIT;
        }
        memcpy(pages, source->pages, (size_t)source->page_count * sizeof(*pages));
        ohlc_free(a, source->pages);
        source->pages = pages;
        source->page_count = pages_needed;
    }
    return OHLC_OK;
}

const ohlc_band* ohlc_band_find(const ohlc_table* table, uint32_t band) {
    const ohlc_band_node* node = table->bands;
    uint16_t positions[3] = {(uint16_t)(band >> 17), (uint16_t)((band >> 8) & 511u),
                             (uint16_t)(band & 255u)};
    for (size_t i = 0; i < 2; i++) {
        if (node == NULL) {
            return NULL;
        }
        node = node->children[positions[i]];
    }
    if (node == NULL) {
        return NULL;
    }
    return node->children[positions[2]];
}

const ohlc_group* ohlc_group_find(const ohlc_table* table, uint32_t band, uint32_t group) {
    const ohlc_band* directory = ohlc_band_find(table, band);
    if (directory == NULL || group / 256u >= directory->page_count) {
        return NULL;
    }
    const ohlc_group_page* page = directory->pages[group / 256u];
    return page == NULL ? NULL : &page->groups[group % 256u];
}

ohlc_status ohlc_group_edit(ohlc_db* db, ohlc_table* table, uint32_t band, uint32_t group,
                            ohlc_group** output) {
    uint16_t positions[3] = {(uint16_t)(band >> 17), (uint16_t)((band >> 8) & 511u),
                             (uint16_t)(band & 255u)};
    ohlc_status status = band_node_edit(&db->allocator, &table->bands, 0);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_band_node* node = table->bands;
    for (uint8_t level = 0; level < 2; level++) {
        ohlc_band_node* child = node->children[positions[level]];
        status = band_node_edit(&db->allocator, &child, (uint8_t)(level + 1));
        if (status != OHLC_OK) {
            return status;
        }
        node->children[positions[level]] = child;
        node->bitmap[positions[level] / 64u] |= UINT64_C(1) << (positions[level] % 64u);
        node = child;
    }
    ohlc_band* directory = node->children[positions[2]];
    status = band_edit(&db->allocator, &directory, group / 256u + 1);
    if (status != OHLC_OK) {
        return status;
    }
    node->children[positions[2]] = directory;
    node->bitmap[positions[2] / 64u] |= UINT64_C(1) << (positions[2] % 64u);
    ohlc_group_page* page = directory->pages[group / 256u];
    if (page == NULL || !unique_reference(&page->refs)) {
        ohlc_group_page* copy = ohlc_alloc(&db->allocator, sizeof(*copy));
        if (copy == NULL) {
            return OHLC_LIMIT;
        }
        atomic_init(&copy->refs, 1);
        atomic_init(&copy->disk_offset, 0);
        if (page != NULL) {
            memcpy(copy->groups, page->groups, sizeof(copy->groups));
            for (size_t i = 0; i < OHLC_PAGE_ENTRIES; i++) {
                ohlc_group_retain(&copy->groups[i]);
            }
            group_page_release(&db->allocator, page);
        }
        directory->pages[group / 256u] = copy;
        page = copy;
    }
    page->disk_offset = 0;
    *output = &page->groups[group % 256u];
    return OHLC_OK;
}

void ohlc_table_release(ohlc_allocator* a, ohlc_table* table) {
    if (table == NULL || !last_reference(&table->refs)) {
        return;
    }
    ohlc_dictionary_release(a, table->dictionary);
    ohlc_time_release(a, table->times);
    ohlc_band_node_release(a, table->bands);
    for (size_t i = 0; i < table->time_page_count; i++) {
        ohlc_time_page* page = table->time_pages[i];
        if (page != NULL && last_reference(&page->refs)) {
            ohlc_free(a, page);
        }
    }
    ohlc_free(a, table->time_pages);
    ohlc_free(a, table);
}

static void table_page_release(ohlc_db* db, ohlc_table_page* page) {
    if (page != NULL && last_reference(&page->refs)) {
        for (size_t i = 0; i < OHLC_PAGE_ENTRIES; i++) {
            ohlc_table_release(&db->allocator, page->tables[i]);
        }
        ohlc_free(&db->allocator, page);
    }
}

ohlc_root* ohlc_root_new(ohlc_db* db) {
    ohlc_root* root = ohlc_alloc(&db->allocator, sizeof(*root));
    if (root == NULL) {
        return NULL;
    }
    atomic_init(&root->refs, 1);
    root->page_count = ((size_t)db->options.max_tables + 255) / 256;
    root->pages = ohlc_alloc(&db->allocator, root->page_count * sizeof(*root->pages));
    if (root->pages == NULL) {
        ohlc_free(&db->allocator, root);
        return NULL;
    }
    return root;
}

ohlc_root* ohlc_root_copy(ohlc_db* db, const ohlc_root* source) {
    ohlc_root* root = ohlc_root_new(db);
    if (root == NULL) {
        return NULL;
    }
    root->seq = source->seq;
    root->ticker_count = source->ticker_count;
    root->table_count = source->table_count;
    root->last_table_id = source->last_table_id;
    if (root->page_count < source->page_count) {
        ohlc_table_page** pages = ohlc_alloc(&db->allocator, source->page_count * sizeof(*pages));
        if (pages == NULL) {
            ohlc_root_release(db, root);
            return NULL;
        }
        ohlc_free(&db->allocator, root->pages);
        root->pages = pages;
        root->page_count = source->page_count;
    }
    for (size_t i = 0; i < source->page_count; i++) {
        root->pages[i] = source->pages[i];
        if (root->pages[i] != NULL) {
            atomic_fetch_add_explicit(&root->pages[i]->refs, 1, memory_order_relaxed);
        }
    }
    return root;
}

ohlc_root* ohlc_root_acquire(ohlc_db* db) {
    pthread_mutex_lock(&db->root_mutex);
    ohlc_root* root = db->root;
    atomic_fetch_add_explicit(&root->refs, 1, memory_order_relaxed);
    pthread_mutex_unlock(&db->root_mutex);
    return root;
}

void ohlc_root_release(ohlc_db* db, ohlc_root* root) {
    if (root != NULL && last_reference(&root->refs)) {
        for (size_t i = 0; i < root->page_count; i++) {
            table_page_release(db, root->pages[i]);
        }
        ohlc_free(&db->allocator, root->pages);
        ohlc_free(&db->allocator, root);
    }
}

void ohlc_root_publish(ohlc_db* db, ohlc_root* root) {
    pthread_mutex_lock(&db->root_mutex);
    ohlc_root* previous = db->root;
    db->root = root;
    pthread_mutex_unlock(&db->root_mutex);
    ohlc_root_release(db, previous);
}

const ohlc_table* ohlc_root_table(const ohlc_root* root, uint32_t id) {
    if (id == 0 || id > root->last_table_id || (id - 1u) / 256u >= root->page_count) {
        return NULL;
    }
    uint32_t slot = id - 1;
    const ohlc_table_page* page = root->pages[slot / 256u];
    return page == NULL ? NULL : page->tables[slot % 256u];
}

static ohlc_status root_table_slot(ohlc_db* db, ohlc_root* root, uint32_t id,
                                   ohlc_table*** output) {
    uint32_t slot = id - 1;
    size_t needed = (size_t)(slot / 256u) + 1;
    if (needed > root->page_count) {
        ohlc_table_page** pages = ohlc_alloc(&db->allocator, needed * sizeof(*pages));
        if (pages == NULL) {
            return OHLC_LIMIT;
        }
        memcpy(pages, root->pages, root->page_count * sizeof(*pages));
        ohlc_free(&db->allocator, root->pages);
        root->pages = pages;
        root->page_count = needed;
    }
    ohlc_table_page* page = root->pages[slot / 256u];
    if (page == NULL || !unique_reference(&page->refs)) {
        ohlc_table_page* copy = ohlc_alloc(&db->allocator, sizeof(*copy));
        if (copy == NULL) {
            return OHLC_LIMIT;
        }
        atomic_init(&copy->refs, 1);
        if (page != NULL) {
            memcpy(copy->tables, page->tables, sizeof(copy->tables));
            for (size_t i = 0; i < OHLC_PAGE_ENTRIES; i++) {
                if (copy->tables[i] != NULL) {
                    atomic_fetch_add_explicit(&copy->tables[i]->refs, 1, memory_order_relaxed);
                }
            }
            table_page_release(db, page);
        }
        root->pages[slot / 256u] = copy;
        page = copy;
    }
    *output = &page->tables[slot % 256u];
    return OHLC_OK;
}

ohlc_status ohlc_root_edit_table(ohlc_db* db, ohlc_root* root, uint32_t id, ohlc_table** output) {
    if (ohlc_root_table(root, id) == NULL) {
        return OHLC_NOT_FOUND;
    }
    ohlc_table** slot = NULL;
    ohlc_status status = root_table_slot(db, root, id, &slot);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_table* source = *slot;
    if (unique_reference(&source->refs)) {
        source->disk_offset = 0;
        *output = source;
        return OHLC_OK;
    }
    ohlc_table* copy = ohlc_alloc(&db->allocator, sizeof(*copy));
    if (copy == NULL) {
        return OHLC_LIMIT;
    }
    atomic_init(&copy->refs, 1);
    atomic_init(&copy->disk_offset, 0);
    copy->info = source->info;
    copy->time_count = source->time_count;
    copy->time_page_count = source->time_page_count;
    if (copy->time_page_count != 0) {
        copy->time_pages =
            ohlc_alloc(&db->allocator, copy->time_page_count * sizeof(*copy->time_pages));
        if (copy->time_pages == NULL) {
            ohlc_free(&db->allocator, copy);
            return OHLC_LIMIT;
        }
        for (size_t i = 0; i < copy->time_page_count; i++) {
            copy->time_pages[i] = source->time_pages[i];
            atomic_fetch_add_explicit(&copy->time_pages[i]->refs, 1, memory_order_relaxed);
        }
    }
    copy->dictionary = source->dictionary;
    ohlc_dictionary_retain(copy->dictionary);
    copy->times = source->times;
    copy->bands = source->bands;
    ohlc_time_retain(copy->times);
    ohlc_band_node_retain(copy->bands);
    ohlc_table_release(&db->allocator, source);
    *slot = copy;
    *output = copy;
    return OHLC_OK;
}

ohlc_status ohlc_root_set_table(ohlc_db* db, ohlc_root* root, uint32_t id, ohlc_table* table) {
    ohlc_table** slot = NULL;
    ohlc_status status = root_table_slot(db, root, id, &slot);
    if (status == OHLC_OK) {
        atomic_fetch_add_explicit(&table->refs, 1, memory_order_relaxed);
        ohlc_table_release(&db->allocator, *slot);
        *slot = table;
    }
    return status;
}

ohlc_status ohlc_root_add_table(ohlc_db* db, ohlc_root* root, const ohlc_table_info* info) {
    if (root->table_count >= db->options.max_tables || info->id <= root->last_table_id) {
        return OHLC_LIMIT;
    }
    ohlc_table** slot = NULL;
    ohlc_status status = root_table_slot(db, root, info->id, &slot);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_table* table = ohlc_alloc(&db->allocator, sizeof(*table));
    if (table == NULL) {
        return OHLC_LIMIT;
    }
    atomic_init(&table->refs, 1);
    atomic_init(&table->disk_offset, 0);
    table->info = *info;
    *slot = table;
    root->table_count++;
    root->last_table_id = info->id;
    return OHLC_OK;
}

ohlc_status ohlc_root_drop_table(ohlc_db* db, ohlc_root* root, uint32_t id) {
    if (ohlc_root_table(root, id) == NULL) {
        return OHLC_NOT_FOUND;
    }
    ohlc_table** slot = NULL;
    ohlc_status status = root_table_slot(db, root, id, &slot);
    if (status == OHLC_OK) {
        root->ticker_count -= ohlc_dictionary_count(*slot);
        ohlc_table_release(&db->allocator, *slot);
        *slot = NULL;
        root->table_count--;
    }
    return status;
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

/* Small metadata pages bound the cost of a sparse newly created volume.
 * This changes only the in-memory directory, not the on-disk metadata layout. */
#define OHLC_META_PER_PAGE 128u
#define OHLC_META_ENTRY_BYTES (OHLC_META_PER_PAGE * 32u)
/* Each in-memory page ends with one free bit per entry; see design section 13.6. */
#define OHLC_META_PAGE_BYTES (OHLC_META_ENTRY_BYTES + OHLC_META_PER_PAGE / 8u)
/* An extent holds at most 16 blocks, so no search for 17 blocks ever runs. */
#define OHLC_FREE_MISS_NONE 17u

static ohlc_allocator* storage_allocator(ohlc_db* db) {
    return db->checkpoint_source == NULL ? &db->allocator : &db->checkpoint_allocator;
}

/* A fresh slot directory may retain every power-of-two predecessor. Charge
 * their exact geometric sum, including one allocation header per array. This
 * bound cannot increase when an already budgeted array is actually allocated. */
static size_t slot_directory_bytes(size_t needed) {
    if (needed == 0) {
        return 0;
    }
    size_t capacity = 16;
    size_t arrays = 1;
    while (capacity < needed) {
        if (capacity > SIZE_MAX / 2) {
            return SIZE_MAX;
        }
        capacity *= 2;
        arrays++;
    }
    size_t entries = ohlc_size_add(capacity, capacity - 16);
    size_t headers = ohlc_size_multiply(arrays, ohlc_allocation_bytes(sizeof(ohlc_slot_table)));
    return ohlc_size_add(headers, ohlc_size_multiply(entries, sizeof(void*)));
}

static size_t slot_growth_bytes(ohlc_slots* slots, size_t needed) {
    size_t capacity = slots == NULL ? 0 : ohlc_slot_capacity(slots);
    if (capacity >= needed) {
        return 0;
    }
    size_t total = slot_directory_bytes(needed);
    return total == SIZE_MAX ? SIZE_MAX : total - slot_directory_bytes(capacity);
}

static size_t rollover_count(size_t groups, size_t capacity) {
    if (groups == 0) {
        return 0;
    }
    size_t blocks = ohlc_size_multiply(groups, 16);
    /* Ignore the current volume's free space. Each subsequent closed volume
     * contains at least capacity - 15 blocks; each group opens at most one. */
    size_t volumes = ohlc_size_add(blocks / (capacity - 15), 1);
    return volumes < groups ? volumes : groups;
}

size_t ohlc_storage_table_bytes(ohlc_db* db, const ohlc_table* table) {
    ohlc_table_files* files = ohlc_slot_get(&db->files, table->info.id - 1);
    size_t bytes = ohlc_allocation_bytes(sizeof(ohlc_table_files));
    size_t groups = table->hot_groups;
    if (groups == 0) {
        return bytes;
    }
    const ohlc_table* pending = db->checkpoint_source == NULL
                                    ? NULL
                                    : ohlc_root_table(db->checkpoint_source, table->info.id);
    size_t pending_groups = pending == NULL ? 0 : pending->hot_groups;
    size_t capacity =
        (size_t)((db->options.data_volume_bytes - OHLC_PAGE_BYTES) / OHLC_BLOCK_BYTES);
    size_t volumes = rollover_count(groups, capacity);
    size_t pending_volumes = rollover_count(pending_groups, capacity);
    size_t blocks = ohlc_size_multiply(groups, 16);
    size_t pending_blocks = ohlc_size_multiply(pending_groups, 16);
    size_t count = files == NULL ? 0 : atomic_load(&files->volume_count);
    ohlc_volume* last = count == 0 ? NULL : ohlc_slot_get(&files->volumes, count - 1);
    size_t existing = last == NULL ? 0 : atomic_load(&last->meta_count);
    uint64_t next = files == NULL ? table->next_volume_hint : atomic_load(&files->next_volume_id);
    if (pending != NULL) {
        /* Freeze the in-flight starting point. Adding its whole allowance to
         * an already advanced next ID would count that growth twice and make
         * unrelated tables require a rescan on every commit. */
        existing = pending->checkpoint_meta_count;
        next = pending->checkpoint_next_volume;
    }
    size_t last_id = ohlc_size_add((size_t)(next - 1), ohlc_size_add(pending_volumes, volumes));
    if (last_id > UINT32_MAX || blocks == SIZE_MAX || pending_blocks == SIZE_MAX) {
        return SIZE_MAX;
    }
    bytes =
        ohlc_size_add(bytes, slot_growth_bytes(files == NULL ? NULL : &files->volumes, last_id));
    bytes = ohlc_size_add(bytes,
                          ohlc_size_multiply(volumes, ohlc_allocation_bytes(sizeof(ohlc_volume))));
    /* Appending B blocks needs at most floor(B / P) + V + 1 new metadata
     * pages: V new volumes and one arbitrarily filled existing volume. */
    size_t pages = ohlc_size_add(blocks / OHLC_META_PER_PAGE, ohlc_size_add(volumes, 1));
    bytes = ohlc_size_add(bytes,
                          ohlc_size_multiply(pages, ohlc_allocation_bytes(OHLC_META_PAGE_BYTES)));
    /* Across new volumes, power-of-two arrays retain fewer than four slots
     * per metadata page, plus their minimum arrays and allocation headers. */
    size_t per_page = 4u * sizeof(void*) + ohlc_allocation_bytes(sizeof(ohlc_slot_table));
    bytes = ohlc_size_add(bytes, ohlc_size_multiply(pages, per_page));
    bytes = ohlc_size_add(bytes, ohlc_size_multiply(volumes, slot_directory_bytes(16)));
    /* The old last volume may grow or be replaced during the in-flight
     * checkpoint. Its eventual entry count is bounded by existing + pending
     * blocks. Budget its whole slot history, not just today's missing slots.
     * Together with separate rollover allowances, this makes the estimate for
     * an unchanged root nonincreasing when that checkpoint completes. */
    size_t entries = ohlc_size_add(existing, ohlc_size_add(pending_blocks, blocks));
    if (entries > capacity) {
        entries = capacity;
    }
    size_t last_pages = ohlc_size_add(entries, OHLC_META_PER_PAGE - 1) / OHLC_META_PER_PAGE;
    return ohlc_size_add(bytes, slot_directory_bytes(last_pages));
}

size_t ohlc_storage_root_bytes(ohlc_db* db, const ohlc_root* root) {
    return slot_growth_bytes(&db->files, root->last_table_id);
}

void ohlc_storage_capture(ohlc_db* db, const ohlc_root* root) {
    for (size_t page = 0; page < root->page_count; page++) {
        if (root->pages[page] == NULL) {
            continue;
        }
        size_t entries = ohlc_root_page_entries(root, page);
        for (size_t slot = 0; slot < entries; slot++) {
            ohlc_table* table = root->pages[page]->tables[slot];
            if (table == NULL) {
                continue;
            }
            ohlc_table_files* files = ohlc_slot_get(&db->files, table->info.id - 1);
            size_t count = files == NULL ? 0 : atomic_load(&files->volume_count);
            ohlc_volume* last = count == 0 ? NULL : ohlc_slot_get(&files->volumes, count - 1);
            table->checkpoint_meta_count = last == NULL ? 0 : atomic_load(&last->meta_count);
            table->checkpoint_next_volume =
                files == NULL ? table->next_volume_hint : atomic_load(&files->next_volume_id);
        }
    }
}

size_t ohlc_storage_checkpoint_bytes(ohlc_db* db, const ohlc_root* root) {
    size_t bytes = slot_growth_bytes(&db->files, root->last_table_id);
    for (size_t page = 0; page < root->page_count; page++) {
        if (root->pages[page] == NULL) {
            continue;
        }
        size_t entries = ohlc_root_page_entries(root, page);
        for (size_t slot = 0; slot < entries; slot++) {
            const ohlc_table* table = root->pages[page]->tables[slot];
            if (table == NULL || table->disk_offset != 0 ||
                (table->time_count == 0 && ohlc_dictionary_count(table) == 0)) {
                continue;
            }
            bytes = ohlc_size_add(bytes, ohlc_storage_table_bytes(db, table));
        }
    }
    return bytes;
}

static ohlc_status file_size(int fd, uint64_t* size) {
    struct stat information;
    if (fstat(fd, &information) != 0 || information.st_size < 0 || !S_ISREG(information.st_mode)) {
        return OHLC_IO;
    }
    *size = (uint64_t)information.st_size;
    return OHLC_OK;
}

ohlc_status ohlc_storage_header(ohlc_db* db, int fd, uint32_t type, uint32_t table, uint32_t volume,
                                uint64_t generation, bool create) {
    uint8_t header[OHLC_PAGE_BYTES] = {0};
    if (create) {
        memcpy(header, "OHLCFIL6", 8);
        ohlc_put_u32(header + 8, OHLC_FORMAT_VERSION);
        ohlc_put_u32(header + 12, type);
        memcpy(header + 16, db->uuid, 16);
        ohlc_put_u32(header + 32, table);
        ohlc_put_u32(header + 36, volume);
        ohlc_put_u32(header + 40, OHLC_BLOCK_BYTES);
        ohlc_put_u32(header + 44, OHLC_ROW_BYTES);
        ohlc_put_u32(header + 48, 16);
        ohlc_put_u32(header + 52, 8);
        ohlc_put_u32(header + 56, 128);
        ohlc_put_u64(header + 64, generation);
        ohlc_put_u32(header + 4092, ohlc_crc32c(0, header, sizeof(header)));
        return ohlc_write_full(fd, header, sizeof(header), 0);
    }
    ohlc_status status = ohlc_read_full(fd, header, sizeof(header), 0);
    if (status != OHLC_OK) {
        return status;
    }
    uint32_t checksum = ohlc_get_u32(header + 4092);
    ohlc_put_u32(header + 4092, 0);
    /* The magic prefix identifies an ohlc file; its final digit and the version
     * field name the format. An intact header of another format is unsupported. */
    if (memcmp(header, "OHLCFIL", 7) != 0 || checksum != ohlc_crc32c(0, header, sizeof(header))) {
        return OHLC_CORRUPT;
    }
    if (header[7] != '6' || ohlc_get_u32(header + 8) != OHLC_FORMAT_VERSION) {
        return OHLC_UNSUPPORTED;
    }
    if (ohlc_get_u32(header + 12) != type || memcmp(header + 16, db->uuid, 16) != 0 ||
        ohlc_get_u32(header + 32) != table || ohlc_get_u32(header + 36) != volume ||
        ohlc_get_u32(header + 40) != OHLC_BLOCK_BYTES ||
        ohlc_get_u32(header + 44) != OHLC_ROW_BYTES || ohlc_get_u32(header + 48) != 16 ||
        ohlc_get_u32(header + 52) != 8 || ohlc_get_u32(header + 56) != 128 ||
        ohlc_get_u32(header + 60) != 0) {
        return OHLC_CORRUPT;
    }
    for (size_t i = 72; i < 4092; i++) {
        if (header[i] != 0) {
            return OHLC_CORRUPT;
        }
    }
    return OHLC_OK;
}

static ohlc_status random_uuid(uint8_t output[16]) {
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return OHLC_IO;
    }
    size_t position = 0;
    while (position < 16) {
        ssize_t size = read(fd, output + position, 16 - position);
        if (size < 0 && errno == EINTR) {
            continue;
        }
        if (size <= 0) {
            close(fd);
            return OHLC_IO;
        }
        position += (size_t)size;
    }
    close(fd);
    output[6] = (uint8_t)((output[6] & 15u) | 64u);
    output[8] = (uint8_t)((output[8] & 63u) | 128u);
    return OHLC_OK;
}

/* The catalog name is published only after its header is durable, so a crash
 * leaves either no catalog or one whose identity recovery can validate. The
 * caller holds LOCK and has verified that the catalog does not exist. */
static ohlc_status create_catalog(ohlc_db* db) {
    const char* partial = "catalog-000001.dat.partial";
    int fd = openat(db->directory_fd, partial, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW,
                    0600);
    if (fd < 0) {
        return OHLC_IO;
    }
    ohlc_status status = ohlc_storage_header(db, fd, 1, 0, 1, 0, true);
    if (status == OHLC_OK) {
        status = ohlc_sync(fd);
    }
    if (status != OHLC_OK) {
        close(fd);
        (void)unlinkat(db->directory_fd, partial, 0);
        return OHLC_IO;
    }
    if (renameat(db->directory_fd, partial, db->directory_fd, "catalog-000001.dat") != 0) {
        close(fd);
        (void)unlinkat(db->directory_fd, partial, 0);
        return OHLC_IO;
    }
    db->catalog_fd = fd;
    return fsync(db->directory_fd) == 0 ? OHLC_OK : OHLC_IO;
}

ohlc_status ohlc_storage_open(ohlc_db* db, bool* fresh) {
    *fresh = false;
    bool created_directory = false;
    if (db->options.create_if_missing) {
        if (mkdir(db->path, 0700) == 0) {
            created_directory = true;
        } else if (errno != EEXIST) {
            return OHLC_IO;
        }
    }
    db->directory_fd = open(db->path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (db->directory_fd < 0) {
        return errno == ENOENT ? OHLC_NOT_FOUND : OHLC_IO;
    }
    if (created_directory) {
        int parent = openat(db->directory_fd, "..", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (parent < 0) {
            return OHLC_IO;
        }
        int result = fsync(parent);
        close(parent);
        if (result != 0) {
            return OHLC_IO;
        }
    }
    db->lock_fd = openat(db->directory_fd, "LOCK", O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (db->lock_fd < 0) {
        return OHLC_IO;
    }
    if (flock(db->lock_fd, LOCK_EX | LOCK_NB) != 0) {
        return errno == EWOULDBLOCK ? OHLC_BUSY : OHLC_IO;
    }
    db->catalog_fd =
        openat(db->directory_fd, "catalog-000001.dat", O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (db->catalog_fd < 0 && errno == ENOENT && db->options.create_if_missing) {
        struct stat existing;
        if (fstatat(db->directory_fd, "CURRENT.0", &existing, AT_SYMLINK_NOFOLLOW) == 0 ||
            fstatat(db->directory_fd, "CURRENT.1", &existing, AT_SYMLINK_NOFOLLOW) == 0 ||
            fstatat(db->directory_fd, "wal-000001.log", &existing, AT_SYMLINK_NOFOLLOW) == 0) {
            return OHLC_CORRUPT;
        }
        ohlc_status status = random_uuid(db->uuid);
        if (status != OHLC_OK) {
            return status;
        }
        status = create_catalog(db);
        if (status != OHLC_OK) {
            return status;
        }
        *fresh = true;
    } else if (db->catalog_fd < 0) {
        return errno == ENOENT ? OHLC_NOT_FOUND : OHLC_IO;
    } else {
        uint8_t identity[16];
        ohlc_status status = ohlc_read_full(db->catalog_fd, identity, 16, 16);
        if (status != OHLC_OK) {
            return status;
        }
        memcpy(db->uuid, identity, 16);
        status = ohlc_storage_header(db, db->catalog_fd, 1, 0, 1, 0, false);
        if (status != OHLC_OK) {
            return status;
        }
    }
    ohlc_status status = file_size(db->catalog_fd, &db->catalog_size);
    if (status == OHLC_OK) {
        db->catalog_size = (db->catalog_size + 7u) & ~UINT64_C(7);
    }
    return status;
}

ohlc_status ohlc_slot_set(ohlc_db* db, ohlc_slots* slots, size_t index, void* value) {
    /* The storage owner is the only writer, so it may read its own stores relaxed. */
    ohlc_slot_table* table = atomic_load_explicit(slots, memory_order_relaxed);
    if (table == NULL || index >= table->capacity) {
        size_t capacity = table == NULL ? 16u : table->capacity;
        while (capacity <= index) {
            if (capacity > SIZE_MAX / 2u / sizeof(table->slots[0])) {
                return OHLC_LIMIT;
            }
            capacity *= 2u;
        }
        ohlc_slot_table* grown =
            ohlc_alloc(storage_allocator(db), sizeof(*grown) + capacity * sizeof(grown->slots[0]));
        if (grown == NULL) {
            return OHLC_LIMIT;
        }
        grown->previous = table;
        grown->capacity = capacity;
        size_t copied = table == NULL ? 0 : table->capacity;
        for (size_t i = 0; i < capacity; i++) {
            void* existing =
                i < copied ? atomic_load_explicit(&table->slots[i], memory_order_relaxed) : NULL;
            atomic_init(&grown->slots[i], existing);
        }
        atomic_store_explicit(slots, grown, memory_order_release);
        table = grown;
    }
    atomic_store_explicit(&table->slots[index], value, memory_order_release);
    return OHLC_OK;
}

void ohlc_slot_free(ohlc_db* db, ohlc_slots* slots) {
    ohlc_slot_table* table = atomic_load_explicit(slots, memory_order_relaxed);
    while (table != NULL) {
        ohlc_slot_table* previous = table->previous;
        ohlc_free(storage_allocator(db), table);
        table = previous;
    }
    atomic_store_explicit(slots, NULL, memory_order_relaxed);
}

static void volume_close(ohlc_db* db, ohlc_volume* volume) {
    if (volume == NULL) {
        return;
    }
    if (volume->data_fd >= 0) {
        close(volume->data_fd);
    }
    if (volume->meta_fd >= 0) {
        close(volume->meta_fd);
    }
    size_t pages = ohlc_slot_capacity(&volume->meta_pages);
    for (size_t i = 0; i < pages; i++) {
        ohlc_free(storage_allocator(db), ohlc_slot_get(&volume->meta_pages, i));
    }
    ohlc_slot_free(db, &volume->meta_pages);
    ohlc_free(storage_allocator(db), volume);
}

static void table_files_close(ohlc_db* db, ohlc_table_files* files) {
    if (files == NULL) {
        return;
    }
    if (files->index_fd >= 0) {
        close(files->index_fd);
    }
    for (size_t i = 0; i < files->volume_count; i++) {
        volume_close(db, ohlc_slot_get(&files->volumes, i));
    }
    ohlc_slot_free(db, &files->volumes);
    ohlc_free(storage_allocator(db), files);
}

void ohlc_storage_close(ohlc_db* db) {
    size_t capacity = ohlc_slot_capacity(&db->files);
    for (size_t i = 0; i < capacity; i++) {
        table_files_close(db, ohlc_slot_get(&db->files, i));
    }
    ohlc_slot_free(db, &db->files);
    while (db->retired_tables != NULL) {
        ohlc_retired_table* next = db->retired_tables->next;
        ohlc_free(storage_allocator(db), db->retired_tables);
        db->retired_tables = next;
    }
    if (db->wal_fd >= 0) {
        close(db->wal_fd);
    }
    if (db->catalog_fd >= 0) {
        close(db->catalog_fd);
    }
    if (db->lock_fd >= 0) {
        close(db->lock_fd);
    }
    if (db->directory_fd >= 0) {
        close(db->directory_fd);
    }
}

/* Allocate the metadata pages for count entries before any of them is published.
 * Pages below the page of the current count already exist. */
static ohlc_status meta_reserve(ohlc_db* db, ohlc_volume* volume, size_t count) {
    size_t first =
        atomic_load_explicit(&volume->meta_count, memory_order_relaxed) / OHLC_META_PER_PAGE;
    size_t pages_needed = (count + OHLC_META_PER_PAGE - 1) / OHLC_META_PER_PAGE;
    for (size_t i = first; i < pages_needed; i++) {
        if (ohlc_slot_get(&volume->meta_pages, i) != NULL) {
            continue;
        }
        uint8_t* page = ohlc_alloc(storage_allocator(db), OHLC_META_PAGE_BYTES);
        if (page == NULL) {
            return OHLC_LIMIT;
        }
        ohlc_status status = ohlc_slot_set(db, &volume->meta_pages, i, page);
        if (status != OHLC_OK) {
            ohlc_free(storage_allocator(db), page);
            return status;
        }
    }
    return OHLC_OK;
}

/* The caller ensures that block lies below a published or reserved count. */
static uint8_t* meta_at(ohlc_volume* volume, uint64_t block) {
    uint8_t* page = ohlc_slot_get(&volume->meta_pages, (size_t)(block / OHLC_META_PER_PAGE));
    return page + (block % OHLC_META_PER_PAGE) * 32u;
}

/* A set free bit marks a block that no checkpoint slot and no live root
 * references. Pages are zeroed on allocation, so new blocks start as used.
 * Only the storage owner reads or writes the bits. */
static uint8_t* free_bits(ohlc_volume* volume, size_t page) {
    uint8_t* memory = ohlc_slot_get(&volume->meta_pages, page);
    return memory + OHLC_META_ENTRY_BYTES;
}

static bool block_is_free(ohlc_volume* volume, size_t block) {
    const uint8_t* bits = free_bits(volume, block / OHLC_META_PER_PAGE);
    size_t index = block % OHLC_META_PER_PAGE;
    return (bits[index / 8u] & (1u << (index % 8u))) != 0;
}

static void block_set_free(ohlc_volume* volume, size_t block, bool is_free) {
    uint8_t* bits = free_bits(volume, block / OHLC_META_PER_PAGE);
    size_t index = block % OHLC_META_PER_PAGE;
    uint8_t bit = (uint8_t)(1u << (index % 8u));
    if (is_free) {
        bits[index / 8u] |= bit;
    } else {
        bits[index / 8u] &= (uint8_t)~bit;
    }
}

static bool page_has_free_block(ohlc_volume* volume, size_t page) {
    const uint8_t* bits = free_bits(volume, page);
    for (size_t i = 0; i < OHLC_META_PER_PAGE / 8u; i++) {
        if (bits[i] != 0) {
            return true;
        }
    }
    return false;
}

/* Finds count consecutive free blocks within [start, end). */
static bool free_run_search(ohlc_volume* volume, size_t start, size_t end, size_t count,
                            size_t* first) {
    size_t run = 0;
    size_t block = start;
    while (block < end) {
        bool whole_page = block % OHLC_META_PER_PAGE == 0 && end - block >= OHLC_META_PER_PAGE;
        if (whole_page && !page_has_free_block(volume, block / OHLC_META_PER_PAGE)) {
            run = 0;
            block += OHLC_META_PER_PAGE;
            continue;
        }
        if (block_is_free(volume, block)) {
            run++;
            if (run == count) {
                *first = block + 1 - count;
                return true;
            }
        } else {
            run = 0;
        }
        block++;
    }
    return false;
}

/* Takes count consecutive free blocks by next fit. A failed search is
 * remembered until the next release, so that a fragmented volume is not
 * scanned again for every extent of the same or a larger size. */
static bool free_run_take(ohlc_volume* volume, size_t count, size_t* first) {
    if (volume->free_blocks < count || count >= volume->free_miss) {
        return false;
    }
    size_t cursor = volume->free_cursor;
    if (cursor < volume->free_first || cursor > volume->free_end) {
        cursor = volume->free_first;
    }
    bool found = free_run_search(volume, cursor, volume->free_end, count, first);
    if (!found) {
        /* A run that starts before the cursor may extend past it. */
        size_t end = cursor + count - 1;
        if (end > volume->free_end) {
            end = volume->free_end;
        }
        found = free_run_search(volume, volume->free_first, end, count, first);
    }
    if (!found) {
        volume->free_miss = count;
        return false;
    }
    for (size_t i = 0; i < count; i++) {
        block_set_free(volume, *first + i, false);
    }
    volume->free_blocks -= count;
    volume->free_cursor = *first + count;
    return true;
}

/* Marks [first, first + count) free. The caller ensures that the range lies
 * below the metadata count. Blocks that are already free are counted once. */
static void free_range_add(ohlc_volume* volume, size_t first, size_t count) {
    if (count == 0) {
        return;
    }
    if (volume->free_blocks == 0) {
        volume->free_first = first;
        volume->free_end = first + count;
        volume->free_cursor = first;
    } else {
        if (first < volume->free_first) {
            volume->free_first = first;
        }
        if (first + count > volume->free_end) {
            volume->free_end = first + count;
        }
    }
    for (size_t i = 0; i < count; i++) {
        if (!block_is_free(volume, first + i)) {
            block_set_free(volume, first + i, true);
            volume->free_blocks++;
        }
    }
    volume->free_miss = OHLC_FREE_MISS_NONE;
}

static ohlc_status open_volume(ohlc_db* db, int directory, uint32_t table, uint32_t id, bool create,
                               ohlc_volume** output) {
    *output = NULL;
    ohlc_volume* volume = ohlc_alloc(storage_allocator(db), sizeof(*volume));
    if (volume == NULL) {
        return OHLC_LIMIT;
    }
    volume->id = id;
    volume->data_fd = -1;
    volume->meta_fd = -1;
    volume->free_miss = OHLC_FREE_MISS_NONE;
    atomic_init(&volume->meta_count, 0);
    atomic_init(&volume->meta_pages, NULL);
    char data_name[32];
    char meta_name[32];
    snprintf(data_name, sizeof(data_name), "data-%06u.dat", id);
    snprintf(meta_name, sizeof(meta_name), "data-%06u.meta", id);
    int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW;
    if (create) {
        flags |= O_CREAT | O_EXCL;
    }
    bool data_created = false;
    bool meta_created = false;
    volume->data_fd = openat(directory, data_name, flags, 0600);
    ohlc_status status = OHLC_IO;
    if (volume->data_fd < 0) {
        status = errno == ENOENT ? OHLC_NOT_FOUND : OHLC_IO;
        goto cleanup;
    }
    data_created = create;
    volume->meta_fd = openat(directory, meta_name, flags, 0600);
    if (volume->meta_fd < 0) {
        status = errno == ENOENT ? OHLC_CORRUPT : OHLC_IO;
        goto cleanup;
    }
    meta_created = create;
    status = ohlc_storage_header(db, volume->data_fd, 3, table, id, db->checkpoint_generation + 1,
                                 create);
    if (status == OHLC_OK) {
        status = ohlc_storage_header(db, volume->meta_fd, 4, table, id,
                                     db->checkpoint_generation + 1, create);
    }
    if (status != OHLC_OK) {
        goto cleanup;
    }
    status = file_size(volume->data_fd, &volume->size);
    if (status != OHLC_OK || volume->size < OHLC_PAGE_BYTES) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    uint64_t meta_size = 0;
    status = file_size(volume->meta_fd, &meta_size);
    if (status != OHLC_OK || meta_size < OHLC_PAGE_BYTES) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    /* An unpublished tail may be incomplete. Retain complete physical pairs;
     * checkpoint references are validated separately before being accepted. */
    uint64_t blocks = (volume->size - OHLC_PAGE_BYTES) / OHLC_BLOCK_BYTES;
    uint64_t meta_blocks = (meta_size - OHLC_PAGE_BYTES) / 32u;
    if (meta_blocks < blocks) {
        blocks = meta_blocks;
    }
    if (blocks > SIZE_MAX) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    volume->size = OHLC_PAGE_BYTES + blocks * OHLC_BLOCK_BYTES;
    status = meta_reserve(db, volume, (size_t)blocks);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    size_t pages = ((size_t)blocks + OHLC_META_PER_PAGE - 1) / OHLC_META_PER_PAGE;
    for (size_t i = 0; i < pages; i++) {
        size_t count = (size_t)blocks - i * OHLC_META_PER_PAGE;
        if (count > OHLC_META_PER_PAGE) {
            count = OHLC_META_PER_PAGE;
        }
        status = ohlc_read_full(volume->meta_fd, ohlc_slot_get(&volume->meta_pages, i), count * 32u,
                                OHLC_PAGE_BYTES + (uint64_t)i * OHLC_META_PER_PAGE * 32u);
        if (status != OHLC_OK) {
            goto cleanup;
        }
    }
    /* Readers can reach the volume only after the owner publishes it. */
    atomic_store_explicit(&volume->meta_count, (size_t)blocks, memory_order_relaxed);
    if (create && (ohlc_sync(volume->data_fd) != OHLC_OK || ohlc_sync(volume->meta_fd) != OHLC_OK ||
                   fsync(directory) != 0)) {
        status = OHLC_IO;
        goto cleanup;
    }
    *output = volume;
    return OHLC_OK;
cleanup:
    volume_close(db, volume);
    /* Files created by a failed attempt are unpublished and unreferenced.
     * Removal is best effort: the caller never reuses this volume ID. */
    if (meta_created) {
        (void)unlinkat(directory, meta_name, 0);
    }
    if (data_created) {
        (void)unlinkat(directory, data_name, 0);
    }
    return status;
}

static ohlc_status table_directory(ohlc_db* db, uint32_t id, bool create, int* output) {
    *output = -1;
    if (create && mkdirat(db->directory_fd, "tables", 0700) != 0 && errno != EEXIST) {
        return OHLC_IO;
    }
    int parent =
        openat(db->directory_fd, "tables", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (parent < 0) {
        return errno == ENOENT ? OHLC_NOT_FOUND : OHLC_IO;
    }
    char name[16];
    snprintf(name, sizeof(name), "%08x", id);
    if (create && mkdirat(parent, name, 0700) != 0 && errno != EEXIST) {
        close(parent);
        return OHLC_IO;
    }
    int directory = openat(parent, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    ohlc_status status = OHLC_OK;
    if (directory < 0) {
        status = errno == ENOENT ? OHLC_NOT_FOUND : OHLC_IO;
    }
    if (create && (fsync(parent) != 0 || fsync(db->directory_fd) != 0)) {
        if (directory >= 0) {
            close(directory);
        }
        directory = -1;
        status = OHLC_IO;
    }
    close(parent);
    *output = directory;
    return status;
}

static uint32_t table_directory_id(const char* name) {
    if (strlen(name) != 8) {
        return 0;
    }
    uint32_t id = 0;
    for (size_t i = 0; i < 8; i++) {
        unsigned int digit;
        if (name[i] >= '0' && name[i] <= '9') {
            digit = (unsigned int)(name[i] - '0');
        } else if (name[i] >= 'a' && name[i] <= 'f') {
            digit = (unsigned int)(name[i] - 'a') + 10u;
        } else {
            return 0;
        }
        id = id * 16u + digit;
    }
    return id;
}

ohlc_status ohlc_storage_find_retired(ohlc_db* db) {
    if (db->root->last_table_id == db->root->table_count) {
        return OHLC_OK;
    }
    int fd = openat(db->directory_fd, "tables", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return errno == ENOENT ? OHLC_OK : OHLC_IO;
    }
    DIR* directory = fdopendir(fd);
    if (directory == NULL) {
        close(fd);
        return OHLC_IO;
    }
    ohlc_status status = OHLC_OK;
    for (;;) {
        errno = 0;
        struct dirent* entry = readdir(directory);
        if (entry == NULL) {
            status = errno == 0 ? OHLC_OK : OHLC_IO;
            break;
        }
        uint32_t id = table_directory_id(entry->d_name);
        if (id == 0 || id > db->root->last_table_id || ohlc_root_table(db->root, id) != NULL) {
            continue;
        }
        const ohlc_retired_table* existing = db->retired_tables;
        while (existing != NULL && existing->id != id) {
            existing = existing->next;
        }
        if (existing != NULL) {
            continue;
        }
        ohlc_retired_table* retired = ohlc_alloc(storage_allocator(db), sizeof(*retired));
        if (retired == NULL) {
            status = OHLC_LIMIT;
            break;
        }
        /* After a crash, use the recovered sequence as a conservative fence.
         * Two subsequent checkpoints make either recovery root independent. */
        retired->id = id;
        retired->sequence = db->root->seq;
        retired->next = db->retired_tables;
        db->retired_tables = retired;
        atomic_store(&db->reclaim_pending, true);
    }
    closedir(directory);
    return status;
}

static bool table_file_name(const char* name) {
    if (strcmp(name, "index-000001.dat") == 0) {
        return true;
    }
    if (strncmp(name, "data-", 5) != 0) {
        return false;
    }
    const char* suffix = name + 5;
    uint64_t volume = 0;
    while (*suffix >= '0' && *suffix <= '9' && volume <= UINT32_MAX) {
        volume = volume * 10u + (unsigned int)(*suffix++ - '0');
    }
    if (volume == 0 || volume > UINT32_MAX ||
        (strcmp(suffix, ".dat") != 0 && strcmp(suffix, ".meta") != 0)) {
        return false;
    }
    char canonical[64];
    snprintf(canonical, sizeof(canonical), "data-%06u%s", (uint32_t)volume, suffix);
    return strcmp(name, canonical) == 0;
}

static void keep_first_failure(ohlc_status* status, ohlc_status failure) {
    if (*status == OHLC_OK) {
        *status = failure;
    }
}

/* Removes one table file. Entries the engine did not create are left in place
 * and reported as unsupported; they keep the directory from being removed. */
static ohlc_status remove_table_file(int directory, const char* name) {
    if (!table_file_name(name)) {
        return OHLC_UNSUPPORTED;
    }
    struct stat information;
    if (fstatat(directory, name, &information, AT_SYMLINK_NOFOLLOW) != 0) {
        return OHLC_IO;
    }
    if (!S_ISREG(information.st_mode)) {
        return OHLC_UNSUPPORTED;
    }
    if (unlinkat(directory, name, 0) != 0) {
        return OHLC_IO;
    }
    return OHLC_OK;
}

/* Removes every engine file even after a failure, then removes the directory
 * only when it is empty. Returns the first failure. */
static ohlc_status remove_table_directory(ohlc_db* db, uint32_t id) {
    int fd = -1;
    ohlc_status status = table_directory(db, id, false, &fd);
    if (status != OHLC_OK) {
        return status == OHLC_NOT_FOUND ? OHLC_OK : status;
    }
    DIR* directory = fdopendir(fd);
    if (directory == NULL) {
        close(fd);
        return OHLC_IO;
    }
    bool removed = false;
    for (;;) {
        errno = 0;
        struct dirent* entry = readdir(directory);
        if (entry == NULL) {
            if (errno != 0) {
                keep_first_failure(&status, OHLC_IO);
            }
            break;
        }
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        ohlc_status removal = remove_table_file(fd, entry->d_name);
        if (removal == OHLC_OK) {
            removed = true;
        } else {
            keep_first_failure(&status, removal);
        }
    }
    if (removed && fsync(fd) != 0) {
        keep_first_failure(&status, OHLC_IO);
    }
    closedir(directory);
    if (status != OHLC_OK) {
        return status;
    }
    int parent =
        openat(db->directory_fd, "tables", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (parent < 0) {
        return OHLC_IO;
    }
    char name[16];
    snprintf(name, sizeof(name), "%08x", id);
    if (unlinkat(parent, name, AT_REMOVEDIR) != 0 || fsync(parent) != 0) {
        status = OHLC_IO;
    }
    close(parent);
    return status;
}

static void release_table_storage(ohlc_db* db, uint32_t id) {
    /* No reader can reach this table: it is absent from every live root,
     * and replaced slot-table copies are never consulted for its ID. */
    pthread_mutex_lock(&db->storage_mutex);
    ohlc_table_files* files = ohlc_slot_get(&db->files, id - 1);
    if (files != NULL) {
        /* Clearing an existing slot never grows the table, so it cannot fail. */
        (void)ohlc_slot_set(db, &db->files, id - 1, NULL);
        table_files_close(db, files);
    }
    pthread_mutex_unlock(&db->storage_mutex);
    for (size_t shard = 0; shard < db->cache_shard_count; shard++) {
        ohlc_cache* cache = &db->cache[shard];
        pthread_mutex_lock(&cache->mutex);
        for (size_t slot = 0; slot < cache->sets * OHLC_CACHE_WAYS; slot++) {
            ohlc_cache_entry* entry = &cache->entries[slot];
            if (atomic_load_explicit(&entry->table, memory_order_relaxed) == id) {
                atomic_store_explicit(&entry->valid, false, memory_order_relaxed);
            }
        }
        pthread_mutex_unlock(&cache->mutex);
    }
}

/* Called by the checkpoint owner; writer protects the retirement list.
 * Cursors increment their count before acquiring a root. A cursor that starts
 * after deletion cannot reference the retired table. Existing cursors defer
 * reclamation without adding a lookup to the query or row-reading path.
 * A directory that cannot be removed stays retired for a later attempt; the
 * other due directories are still processed. */
void ohlc_storage_reclaim_tables(ohlc_db* db) {
    if (!atomic_load(&db->reclaim_pending)) {
        return;
    }
    pthread_mutex_lock(&db->writer);
    if (atomic_load_explicit(&db->cursor_count, memory_order_acquire) != 0) {
        pthread_mutex_unlock(&db->writer);
        return;
    }
    ohlc_status failure = OHLC_OK;
    uint32_t failed_table = 0;
    ohlc_retired_table** position = &db->retired_tables;
    while (*position != NULL) {
        ohlc_retired_table* entry = *position;
        if (entry->sequence > db->checkpoint_sequences[0] ||
            entry->sequence > db->checkpoint_sequences[1]) {
            position = &entry->next;
            continue;
        }
        ohlc_status status = remove_table_directory(db, entry->id);
        if (status != OHLC_OK) {
            if (failure == OHLC_OK) {
                failure = status;
                failed_table = entry->id;
            }
            position = &entry->next;
            continue;
        }
        release_table_storage(db, entry->id);
        *position = entry->next;
        ohlc_free(storage_allocator(db), entry);
    }
    db->reclaim_status = failure;
    db->reclaim_table = failed_table;
    atomic_store(&db->reclaim_pending, db->retired_tables != NULL);
    pthread_mutex_unlock(&db->writer);
}

ohlc_status ohlc_reclaim_failure(ohlc_db* db, uint32_t* table_id) {
    pthread_mutex_lock(&db->checkpoint_mutex);
    ohlc_status status = db->reclaim_status;
    *table_id = db->reclaim_table;
    pthread_mutex_unlock(&db->checkpoint_mutex);
    return status;
}

bool ohlc_storage_directory_reclaimable(ohlc_db* db, const char* name) {
    uint32_t table_id = table_directory_id(name);
    if (table_id == 0) {
        return false;
    }
    bool reclaimable = false;
    pthread_mutex_lock(&db->writer);
    for (ohlc_retired_table* entry = db->retired_tables; entry != NULL; entry = entry->next) {
        if (entry->id != table_id) {
            continue;
        }
        reclaimable = entry->sequence <= atomic_load(&db->checkpoint_sequences[0]) &&
                      entry->sequence <= atomic_load(&db->checkpoint_sequences[1]);
        break;
    }
    pthread_mutex_unlock(&db->writer);
    return reclaimable;
}

static ohlc_status next_volume_id(int directory, uint64_t* next) {
    int fd = openat(directory, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        return OHLC_IO;
    }
    DIR* stream = fdopendir(fd);
    if (stream == NULL) {
        close(fd);
        return OHLC_IO;
    }
    *next = 1;
    ohlc_status status = OHLC_OK;
    while (true) {
        errno = 0;
        struct dirent* entry = readdir(stream);
        if (entry == NULL) {
            status = errno == 0 ? OHLC_OK : OHLC_IO;
            break;
        }
        if (strncmp(entry->d_name, "data-", 5) != 0) {
            continue;
        }
        const char* p = entry->d_name + 5;
        uint64_t id = 0;
        while (*p >= '0' && *p <= '9' && id <= UINT32_MAX) {
            id = id * 10u + (unsigned int)(*p++ - '0');
        }
        if (id == 0 || id > UINT32_MAX || (strcmp(p, ".dat") != 0 && strcmp(p, ".meta") != 0)) {
            continue;
        }
        char canonical[64];
        snprintf(canonical, sizeof(canonical), "data-%06u%s", (uint32_t)id, p);
        if (strcmp(canonical, entry->d_name) == 0 && *next <= id) {
            *next = id + 1;
        }
    }
    closedir(stream);
    return status;
}

ohlc_status ohlc_storage_volume_hint(ohlc_db* db, uint32_t table, uint64_t* next) {
    *next = 1;
    int directory = -1;
    ohlc_status status = table_directory(db, table, false, &directory);
    if (status == OHLC_NOT_FOUND) {
        return OHLC_OK;
    }
    if (status != OHLC_OK) {
        return status;
    }
    status = next_volume_id(directory, next);
    close(directory);
    return status;
}

/* The commit/checkpoint owner serializes calls that may create file objects. */
ohlc_status ohlc_storage_table(ohlc_db* db, uint32_t table_id, bool create,
                               ohlc_table_files** output) {
    if (table_id == 0) {
        return OHLC_CORRUPT;
    }
    ohlc_table_files* existing = ohlc_slot_get(&db->files, table_id - 1);
    if (existing != NULL) {
        *output = existing;
        return OHLC_OK;
    }
    int directory = -1;
    ohlc_status directory_status = table_directory(db, table_id, create, &directory);
    if (directory_status != OHLC_OK) {
        return directory_status;
    }
    ohlc_table_files* files = ohlc_alloc(storage_allocator(db), sizeof(*files));
    if (files == NULL) {
        close(directory);
        return OHLC_LIMIT;
    }
    files->id = table_id;
    atomic_init(&files->volumes, NULL);
    atomic_init(&files->volume_count, 0);
    files->index_fd = openat(directory, "index-000001.dat", O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    bool new_index = false;
    if (files->index_fd < 0 && errno == ENOENT && create) {
        files->index_fd = openat(directory, "index-000001.dat",
                                 O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        new_index = true;
    }
    ohlc_status status = OHLC_IO;
    if (files->index_fd < 0) {
        goto cleanup;
    }
    status = file_size(files->index_fd, &files->index_size);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    /* create is used only after the recovered graph has no file object for
     * this table. A short orphan header cannot be referenced by that graph. */
    if (create && !new_index && files->index_size < OHLC_PAGE_BYTES) {
        if (ftruncate(files->index_fd, 0) != 0) {
            status = OHLC_IO;
            goto cleanup;
        }
        new_index = true;
    }
    status = ohlc_storage_header(db, files->index_fd, 5, table_id, 1, db->checkpoint_generation + 1,
                                 new_index);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    status = file_size(files->index_fd, &files->index_size);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    files->index_size = (files->index_size + 7u) & ~UINT64_C(7);
    uint64_t next_id = 0;
    status = next_volume_id(directory, &next_id);
    atomic_init(&files->next_volume_id, next_id);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    if (new_index && (ohlc_sync(files->index_fd) != OHLC_OK || fsync(directory) != 0)) {
        status = OHLC_IO;
        goto cleanup;
    }
    pthread_mutex_lock(&db->storage_mutex);
    status = ohlc_slot_set(db, &db->files, table_id - 1, files);
    pthread_mutex_unlock(&db->storage_mutex);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    close(directory);
    *output = files;
    return OHLC_OK;
cleanup:
    if (files->index_fd >= 0) {
        close(files->index_fd);
    }
    ohlc_slot_free(db, &files->volumes);
    ohlc_free(storage_allocator(db), files);
    close(directory);
    return status;
}

/* Publish a fully opened volume to lock-free readers. */
static ohlc_status publish_volume(ohlc_db* db, ohlc_table_files* files, ohlc_volume* volume) {
    pthread_mutex_lock(&db->storage_mutex);
    ohlc_status status = ohlc_slot_set(db, &files->volumes, volume->id - 1, volume);
    pthread_mutex_unlock(&db->storage_mutex);
    if (status == OHLC_OK && volume->id > files->volume_count) {
        files->volume_count = volume->id;
    }
    return status;
}

/* Only volumes reachable from a checkpoint are required for recovery. An
 * incomplete, unpublished rollover file must not invalidate an older root. */
ohlc_status ohlc_storage_load_volume(ohlc_db* db, ohlc_table_files* files, uint32_t id) {
    if (id == 0) {
        return OHLC_CORRUPT;
    }
    if (ohlc_slot_get(&files->volumes, id - 1) != NULL) {
        return OHLC_OK;
    }
    int directory = -1;
    ohlc_status status = table_directory(db, files->id, false, &directory);
    if (status != OHLC_OK) {
        return status;
    }
    ohlc_volume* volume = NULL;
    status = open_volume(db, directory, files->id, id, false, &volume);
    close(directory);
    if (status != OHLC_OK) {
        return status == OHLC_NOT_FOUND ? OHLC_CORRUPT : status;
    }
    status = publish_volume(db, files, volume);
    if (status != OHLC_OK) {
        volume_close(db, volume);
    }
    return status;
}

/* Lock-free: a reader holds a root that references only volumes, and metadata,
 * which the storage owner published before that root. */
static ohlc_volume* find_volume(ohlc_db* db, uint32_t table, uint32_t volume) {
    if (table == 0 || volume == 0) {
        return NULL;
    }
    ohlc_table_files* files = ohlc_slot_get(&db->files, table - 1);
    return files == NULL ? NULL : ohlc_slot_get(&files->volumes, volume - 1);
}

static uint64_t tile_offset(const ohlc_group* group, uint8_t tile) {
    uint16_t lower = (uint16_t)((1u << tile) - 1u);
    return group->base +
           (uint64_t)ohlc_popcount((uint16_t)(group->mask & lower)) * OHLC_BLOCK_BYTES;
}

const uint8_t* ohlc_storage_presence(ohlc_db* db, uint32_t table, const ohlc_group* entry,
                                     uint8_t tile) {
    if (entry == NULL || tile >= 16 || (entry->mask & (1u << tile)) == 0) {
        return NULL;
    }
    if (entry->flags == OHLC_HOT_GROUP) {
        ohlc_hot_group* hot = (ohlc_hot_group*)(uintptr_t)entry->base;
        if (hot->blocks[tile] != NULL) {
            return hot->blocks[tile]->presence;
        }
        entry = &hot->backing;
    }
    uint64_t offset = tile_offset(entry, tile);
    ohlc_volume* volume = find_volume(db, table, entry->volume);
    if (volume == NULL || offset < OHLC_PAGE_BYTES ||
        (offset - OHLC_PAGE_BYTES) % OHLC_BLOCK_BYTES != 0) {
        return NULL;
    }
    uint64_t block = (offset - OHLC_PAGE_BYTES) / OHLC_BLOCK_BYTES;
    /* Load the count before its pages: the owner publishes them in that order. */
    if (block >= atomic_load_explicit(&volume->meta_count, memory_order_acquire)) {
        return NULL;
    }
    return meta_at(volume, block);
}

static uint32_t tile_checksum(const uint8_t* data, const uint8_t* metadata, uint32_t table,
                              uint32_t band, uint32_t group, uint8_t tile) {
    uint8_t coordinates[16];
    ohlc_put_u32(coordinates, table);
    ohlc_put_u32(coordinates + 4, band);
    ohlc_put_u32(coordinates + 8, group);
    ohlc_put_u32(coordinates + 12, tile);
    uint32_t crc = ohlc_crc32c(0, data, OHLC_BLOCK_BYTES);
    crc = ohlc_crc32c(crc, metadata, 24);
    return ohlc_crc32c(crc, coordinates, sizeof(coordinates));
}

static inline uint8_t* cache_block(const ohlc_cache* cache, const ohlc_cache_entry* entry) {
    return cache->blocks + (size_t)(entry - cache->entries) * OHLC_BLOCK_BYTES;
}

/* Marks an entry whose identity is being rewritten under the shard mutex.
 * Live pins are bounded by open views and never reach this bit. */
#define OHLC_CACHE_EVICTING 0x80000000u

/* Without the shard mutex, a match is only a candidate until the caller pins
 * the entry and confirms that its version is unchanged. */
static bool cache_entry_matches(const ohlc_cache_entry* entry, uint32_t table, uint32_t volume,
                                const ohlc_tile_view* view) {
    return atomic_load_explicit(&entry->offset, memory_order_relaxed) == view->offset &&
           atomic_load_explicit(&entry->table, memory_order_relaxed) == table &&
           atomic_load_explicit(&entry->volume, memory_order_relaxed) == volume &&
           atomic_load_explicit(&entry->band, memory_order_relaxed) == view->band &&
           atomic_load_explicit(&entry->group, memory_order_relaxed) == view->group &&
           atomic_load_explicit(&entry->tile, memory_order_relaxed) == view->tile;
}

static void cache_mark_visited(ohlc_cache_entry* entry) {
    /* Skip the store when already set, so that repeated hits keep the line shared. */
    if (!atomic_load_explicit(&entry->visited, memory_order_relaxed)) {
        atomic_store_explicit(&entry->visited, true, memory_order_relaxed);
    }
}

/* Pin an entry that was observed at version without the shard mutex. The
 * acquire CAS synchronizes with the release store that ends any reuse, so a
 * reuse completed before this pin is seen as a newer version. A reuse that
 * has not started cannot claim the entry while this pin is held. */
static bool cache_pin_version(ohlc_cache_entry* entry, uint32_t version) {
    uint32_t pins = atomic_load_explicit(&entry->pins, memory_order_relaxed);
    do {
        if ((pins & OHLC_CACHE_EVICTING) != 0) {
            return false;
        }
    } while (!atomic_compare_exchange_weak_explicit(&entry->pins, &pins, pins + 1u,
                                                    memory_order_acquire, memory_order_relaxed));
    if (atomic_load_explicit(&entry->version, memory_order_relaxed) == version) {
        return true;
    }
    atomic_fetch_sub_explicit(&entry->pins, 1, memory_order_release);
    return false;
}

/* Borrow a published entry without the shard mutex. On false the view is
 * unchanged and the caller repeats the lookup under the mutex. */
static bool cache_acquire_published(ohlc_cache* cache, size_t start, uint32_t table,
                                    uint32_t volume, ohlc_tile_view* view) {
    for (size_t i = 0; i < OHLC_CACHE_WAYS; i++) {
        ohlc_cache_entry* candidate = &cache->entries[start + i];
        /* Acquire orders the identity and valid loads after the reuse that
         * published this version. */
        uint32_t version = atomic_load_explicit(&candidate->version, memory_order_acquire);
        if (!cache_entry_matches(candidate, table, volume, view)) {
            continue;
        }
        /* Acquire pairs with the loader's release, making the block readable. */
        if (!atomic_load_explicit(&candidate->valid, memory_order_acquire)) {
            continue;
        }
        if (!cache_pin_version(candidate, version)) {
            return false;
        }
        cache_mark_visited(candidate);
        view->cache = cache;
        view->cached = candidate;
        view->data = cache_block(cache, candidate);
        return true;
    }
    return false;
}

/* Called with the shard mutex. Rewrite an unpinned entry for the new tile and
 * pin it once for the caller. False means a lock-free borrower pinned it after
 * the caller's check. */
static bool cache_take(ohlc_cache_entry* candidate, uint32_t table, uint32_t volume,
                       const ohlc_tile_view* view) {
    /* Acquire the final release of every earlier borrower before the block is
     * overwritten, and keep new lock-free pins out meanwhile. */
    uint32_t idle = 0;
    if (!atomic_compare_exchange_strong_explicit(&candidate->pins, &idle, OHLC_CACHE_EVICTING,
                                                 memory_order_acquire, memory_order_relaxed)) {
        return false;
    }
    atomic_store_explicit(&candidate->table, table, memory_order_relaxed);
    atomic_store_explicit(&candidate->volume, volume, memory_order_relaxed);
    atomic_store_explicit(&candidate->offset, view->offset, memory_order_relaxed);
    atomic_store_explicit(&candidate->band, view->band, memory_order_relaxed);
    atomic_store_explicit(&candidate->group, view->group, memory_order_relaxed);
    atomic_store_explicit(&candidate->tile, view->tile, memory_order_relaxed);
    atomic_store_explicit(&candidate->valid, false, memory_order_relaxed);
    atomic_store_explicit(&candidate->visited, true, memory_order_relaxed);
    candidate->loading = true;
    candidate->status = OHLC_OK;
    uint32_t version = atomic_load_explicit(&candidate->version, memory_order_relaxed);
    atomic_store_explicit(&candidate->version, version + 1u, memory_order_release);
    atomic_store_explicit(&candidate->pins, 1, memory_order_release);
    return true;
}

/* Called with the shard mutex. Loaders pin their entries until loading ends,
 * so an unpinned invalid entry holds no tile. */
static bool cache_entry_empty(const ohlc_cache_entry* entry) {
    return atomic_load_explicit(&entry->pins, memory_order_relaxed) == 0 &&
           !atomic_load_explicit(&entry->valid, memory_order_relaxed);
}

/* Called with the shard mutex. Take an empty way first, so that a set never
 * evicts a resident tile while it has room; otherwise take an unpinned entry
 * by CLOCK. Return it pinned once for the caller, who must load and publish
 * it. NULL means every way is pinned or recently used. */
static ohlc_cache_entry* cache_claim(ohlc_cache* cache, size_t start, uint32_t table,
                                     uint32_t volume, const ohlc_tile_view* view) {
    for (size_t i = 0; i < OHLC_CACHE_WAYS; i++) {
        ohlc_cache_entry* candidate = &cache->entries[start + i];
        if (cache_entry_empty(candidate) && cache_take(candidate, table, volume, view)) {
            return candidate;
        }
    }
    for (size_t i = 0; i < 2u * OHLC_CACHE_WAYS; i++) {
        ohlc_cache_entry* candidate = &cache->entries[start + cache->hand++ % OHLC_CACHE_WAYS];
        if (atomic_load_explicit(&candidate->pins, memory_order_relaxed) != 0) {
            continue;
        }
        if (atomic_load_explicit(&candidate->valid, memory_order_relaxed) &&
            atomic_load_explicit(&candidate->visited, memory_order_relaxed)) {
            atomic_store_explicit(&candidate->visited, false, memory_order_relaxed);
            continue;
        }
        if (cache_take(candidate, table, volume, view)) {
            return candidate;
        }
    }
    return NULL;
}

/* Matching an in-flight load is also a hit; an exhausted set is a miss. */
static bool cache_acquire_locked(ohlc_cache* cache, size_t start, uint32_t table, uint32_t volume,
                                 ohlc_tile_view* view) {
    pthread_mutex_lock(&cache->mutex);
    ohlc_cache_entry* chosen = NULL;
    bool hit = false;
    for (size_t i = 0; i < OHLC_CACHE_WAYS; i++) {
        ohlc_cache_entry* candidate = &cache->entries[start + i];
        bool usable =
            atomic_load_explicit(&candidate->valid, memory_order_relaxed) || candidate->loading;
        if (usable && cache_entry_matches(candidate, table, volume, view)) {
            chosen = candidate;
            hit = true;
            break;
        }
    }
    if (chosen != NULL) {
        atomic_fetch_add_explicit(&chosen->pins, 1, memory_order_relaxed);
        cache_mark_visited(chosen);
    } else {
        chosen = cache_claim(cache, start, table, volume, view);
        view->owns_load = chosen != NULL;
    }
    if (chosen != NULL) {
        view->cache = cache;
        view->cached = chosen;
        if (atomic_load_explicit(&chosen->valid, memory_order_relaxed) && !chosen->loading) {
            view->data = cache_block(cache, chosen);
        }
    }
    pthread_mutex_unlock(&cache->mutex);
    return hit;
}

/* Selects the shard that holds a block and the first way of its set. Returns
 * NULL when the cache is disabled. */
static ohlc_cache* cache_locate(ohlc_db* db, uint32_t table, uint32_t volume, uint64_t offset,
                                size_t* start) {
    uint64_t hash = ((offset - OHLC_PAGE_BYTES) / OHLC_BLOCK_BYTES) * UINT64_C(0x9e3779b185ebca87) ^
                    (uint64_t)table * UINT64_C(0xc2b2ae3d27d4eb4f) ^ volume;
    hash = (hash ^ (hash >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    hash = (hash ^ (hash >> 27)) * UINT64_C(0x94d049bb133111eb);
    hash ^= hash >> 31;
    ohlc_cache* cache = &db->cache[hash & (db->cache_shard_count - 1u)];
    if (cache->sets == 0) {
        return NULL;
    }
    /* Scale the high hash bits by the set count instead of dividing. The low
     * bits select the shard. The result is below sets for every set count,
     * and uniform while a shard has at most 2^32 sets. */
    uint64_t set = ((hash >> 32) * (uint64_t)cache->sets) >> 32;
    *start = (size_t)set * OHLC_CACHE_WAYS;
    return cache;
}

static bool cache_acquire(ohlc_db* db, uint32_t table, uint32_t volume, ohlc_tile_view* view) {
    size_t start = 0;
    ohlc_cache* cache = cache_locate(db, table, volume, view->offset, &start);
    if (cache == NULL) {
        return false;
    }
    if (cache_acquire_published(cache, start, table, volume, view)) {
        return true;
    }
    return cache_acquire_locked(cache, start, table, volume, view);
}

/* Drops every cached image of a block that is about to become reusable. The
 * same group can later be rewritten at the same offset, which would otherwise
 * match a stale entry. No reader references the block, so its entries hold no
 * borrowed views and no load is in flight. */
static void cache_forget(ohlc_db* db, uint32_t table, uint32_t volume, uint64_t offset) {
    size_t start = 0;
    ohlc_cache* cache = cache_locate(db, table, volume, offset, &start);
    if (cache == NULL) {
        return;
    }
    pthread_mutex_lock(&cache->mutex);
    for (size_t i = 0; i < OHLC_CACHE_WAYS; i++) {
        ohlc_cache_entry* entry = &cache->entries[start + i];
        if (atomic_load_explicit(&entry->offset, memory_order_relaxed) == offset &&
            atomic_load_explicit(&entry->table, memory_order_relaxed) == table &&
            atomic_load_explicit(&entry->volume, memory_order_relaxed) == volume) {
            atomic_store_explicit(&entry->valid, false, memory_order_relaxed);
        }
    }
    pthread_mutex_unlock(&cache->mutex);
}

static ohlc_status prepare_view(ohlc_db* db, uint32_t table, ohlc_tile_view* view, size_t* hits,
                                size_t* misses) {
    const ohlc_group* entry = view->entry;
    view->data = NULL;
    view->cached = NULL;
    view->cache = NULL;
    view->owns_load = false;
    if (entry == NULL || view->tile >= 16 || (entry->mask & (1u << view->tile)) == 0) {
        return OHLC_NOT_FOUND;
    }
    view->presence = ohlc_storage_presence(db, table, entry, view->tile);
    if (view->presence == NULL) {
        return OHLC_CORRUPT;
    }
    if (entry->flags == OHLC_HOT_GROUP) {
        const ohlc_hot_group* hot = (const ohlc_hot_group*)(uintptr_t)entry->base;
        if (hot->blocks[view->tile] != NULL) {
            view->data = hot->blocks[view->tile]->data;
            return OHLC_OK;
        }
        entry = &hot->backing;
    }
    if (ohlc_get_u32(view->presence + 28) != 0) {
        return OHLC_CORRUPT;
    }
    view->offset = tile_offset(entry, view->tile);
    ohlc_volume* volume = find_volume(db, table, entry->volume);
    view->fd = volume == NULL ? -1 : volume->data_fd;
    if (view->fd < 0) {
        return OHLC_CORRUPT;
    }
    if (cache_acquire(db, table, entry->volume, view)) {
        (*hits)++;
    } else {
        (*misses)++;
    }
    return OHLC_OK;
}

static void publish_view(ohlc_tile_view* view, ohlc_status status) {
    if (view->owns_load) {
        /* The pinned loader writes and validates in place while loading is
         * true. Waiters hold this mutex until loading is false, and lock-free
         * hits acquire valid, so both see the data written before publication. */
        pthread_mutex_lock(&view->cache->mutex);
        view->cached->status = status;
        atomic_store_explicit(&view->cached->valid, status == OHLC_OK, memory_order_release);
        view->cached->loading = false;
        pthread_cond_broadcast(&view->cache->changed);
        pthread_mutex_unlock(&view->cache->mutex);
        view->owns_load = false;
    }
}

void ohlc_storage_release(ohlc_tile_view* views, size_t count) {
    for (size_t i = 0; i < count; i++) {
        ohlc_tile_view* view = &views[i];
        if (view->cached != NULL) {
            /* Reuse acquires zero under the shard mutex, after all readers'
             * final data accesses have been published by these releases. */
            atomic_fetch_sub_explicit(&view->cached->pins, 1, memory_order_release);
            view->cached = NULL;
        }
        view->data = NULL;
    }
}

static bool view_needs_load(const ohlc_tile_view* view) {
    return view->data == NULL && (view->cached == NULL || view->owns_load);
}

/* Read, verify and publish the views that need a disk load; return the last
 * load failure. With claimed_only, load only the cache entries this caller
 * claimed, so that readers waiting on them receive their own results. */
static ohlc_status load_views(ohlc_db* db, uint32_t table, ohlc_tile_view* views, size_t count,
                              bool claimed_only) {
    ohlc_io_request requests[OHLC_READ_WINDOW] = {0};
    size_t request_for_view[OHLC_READ_WINDOW] = {0};
    bool selected[OHLC_READ_WINDOW] = {false};
    size_t request_count = 0;
    for (size_t i = 0; i < count; i++) {
        ohlc_tile_view* view = &views[i];
        if (!view_needs_load(view) || (claimed_only && !view->owns_load)) {
            continue;
        }
        selected[i] = true;
        uint8_t* target = view->owns_load ? cache_block(view->cache, view->cached) : view->buffer;
        ohlc_io_request* previous = request_count == 0 ? NULL : &requests[request_count - 1];
        if (previous != NULL && previous->fd == view->fd &&
            previous->offset + previous->size == view->offset &&
            previous->iov_count < (int)OHLC_READ_WINDOW) {
            /* Contiguous targets share one segment, so a read into adjacent
             * cursor buffers stays a plain pread. */
            struct iovec* last = &previous->iov[previous->iov_count - 1];
            if ((uint8_t*)last->iov_base + last->iov_len == target) {
                last->iov_len += OHLC_BLOCK_BYTES;
            } else {
                previous->iov[previous->iov_count++] = (struct iovec){target, OHLC_BLOCK_BYTES};
            }
            previous->size += OHLC_BLOCK_BYTES;
        } else {
            ohlc_io_request* request = &requests[request_count++];
            request->fd = view->fd;
            request->offset = view->offset;
            request->iov[0] = (struct iovec){target, OHLC_BLOCK_BYTES};
            request->iov_count = 1;
            request->size = OHLC_BLOCK_BYTES;
        }
        request_for_view[i] = request_count - 1;
    }
    ohlc_io_read(db, requests, request_count);
    ohlc_status status = OHLC_OK;
    for (size_t i = 0; i < count; i++) {
        if (!selected[i]) {
            continue;
        }
        ohlc_tile_view* view = &views[i];
        uint8_t* target = view->owns_load ? cache_block(view->cache, view->cached) : view->buffer;
        ohlc_status loaded = requests[request_for_view[i]].status;
        if (loaded == OHLC_OK &&
            tile_checksum(target, view->presence, table, view->band, view->group, view->tile) !=
                ohlc_get_u32(view->presence + 24)) {
            loaded = OHLC_CORRUPT;
        }
        if (loaded == OHLC_OK && !ohlc_tile_rows_valid(target)) {
            loaded = OHLC_CORRUPT;
        }
        publish_view(view, loaded);
        view->data = target;
        if (loaded != OHLC_OK) {
            status = loaded;
        }
    }
    return status;
}

ohlc_status ohlc_storage_read(ohlc_db* db, uint32_t table, ohlc_tile_view* views, size_t count) {
    if (count > OHLC_READ_WINDOW) {
        return OHLC_INVALID;
    }
    ohlc_status status = OHLC_OK;
    size_t prepared = 0;
    size_t hits = 0;
    size_t misses = 0;
    for (; prepared < count; prepared++) {
        status = prepare_view(db, table, &views[prepared], &hits, &misses);
        if (status != OHLC_OK) {
            break;
        }
    }
    if (hits != 0) {
        atomic_fetch_add_explicit(&db->cache_hits, hits, memory_order_relaxed);
    }
    if (misses != 0) {
        atomic_fetch_add_explicit(&db->cache_misses, misses, memory_order_relaxed);
    }
    if (status != OHLC_OK) {
        /* The failed view claimed no cache entry. Other readers may wait on
         * entries claimed by earlier views; they must not receive its error. */
        (void)load_views(db, table, views, prepared, true);
        ohlc_storage_release(views, prepared);
        return status;
    }
    /* Submit our loads before waiting on another window's loads. This avoids
     * cycles when two windows reserve overlapping cache entries. */
    status = load_views(db, table, views, count, false);
    for (size_t i = 0; i < count; i++) {
        ohlc_tile_view* view = &views[i];
        if (view->cached == NULL || view->data != NULL) {
            continue;
        }
        pthread_mutex_lock(&view->cache->mutex);
        while (view->cached->loading) {
            pthread_cond_wait(&view->cache->changed, &view->cache->mutex);
        }
        if (view->cached->status != OHLC_OK) {
            status = view->cached->status;
        }
        view->data = cache_block(view->cache, view->cached);
        pthread_mutex_unlock(&view->cache->mutex);
    }
    if (status != OHLC_OK) {
        ohlc_storage_release(views, count);
    }
    return status;
}

ohlc_status ohlc_storage_tile(ohlc_db* db, uint32_t table, uint32_t band, uint32_t group,
                              const ohlc_group* entry, uint8_t tile, uint8_t* output) {
    ohlc_tile_view view = {
        .entry = entry, .band = band, .group = group, .tile = tile, .buffer = output};
    ohlc_status status = ohlc_storage_read(db, table, &view, 1);
    if (status == OHLC_OK) {
        if (view.data != output) {
            memcpy(output, view.data, OHLC_BLOCK_BYTES);
        }
        ohlc_storage_release(&view, 1);
    }
    return status;
}

/* Chooses the blocks of a new extent and returns its first block. Reusable
 * blocks of any loaded volume come first; otherwise the extent is appended to
 * the last volume, rolling over when it does not fit. */
static ohlc_status allocate_extent(ohlc_db* db, ohlc_table_files* files, size_t blocks,
                                   ohlc_volume** output, size_t* first) {
    if (db->reuse_enabled) {
        for (size_t i = 0; i < files->volume_count; i++) {
            ohlc_volume* candidate = ohlc_slot_get(&files->volumes, i);
            if (candidate != NULL && free_run_take(candidate, blocks, first)) {
                *output = candidate;
                return OHLC_OK;
            }
        }
    }
    ohlc_volume* volume =
        files->volume_count == 0 ? NULL : ohlc_slot_get(&files->volumes, files->volume_count - 1);
    uint64_t bytes = (uint64_t)blocks * OHLC_BLOCK_BYTES;
    if (volume == NULL || volume->size > db->options.data_volume_bytes - bytes) {
        if (files->next_volume_id > UINT32_MAX) {
            return OHLC_LIMIT;
        }
        uint32_t id = (uint32_t)files->next_volume_id;
        int directory = -1;
        ohlc_status directory_status = table_directory(db, files->id, false, &directory);
        if (directory_status != OHLC_OK) {
            return directory_status;
        }
        /* Every creation attempt consumes its ID. A partial file left by a
         * failed attempt, or a conflicting name, cannot block later rollovers. */
        files->next_volume_id++;
        ohlc_status status = open_volume(db, directory, files->id, id, true, &volume);
        close(directory);
        if (status != OHLC_OK) {
            return status;
        }
        status = publish_volume(db, files, volume);
        if (status != OHLC_OK) {
            volume_close(db, volume);
            return status;
        }
    }
    *first = atomic_load_explicit(&volume->meta_count, memory_order_relaxed);
    pthread_mutex_lock(&db->storage_mutex);
    ohlc_status status = meta_reserve(db, volume, *first + blocks);
    pthread_mutex_unlock(&db->storage_mutex);
    *output = volume;
    return status;
}

ohlc_status ohlc_storage_flush_group(ohlc_db* db, uint32_t table, uint32_t band, uint32_t group,
                                     const ohlc_group* entry, uint8_t* data, ohlc_group* output) {
    if (entry->flags == 0) {
        *output = *entry;
        return OHLC_OK;
    }
    size_t blocks = ohlc_popcount(entry->mask);
    uint8_t metadata[16 * 32] = {0};
    size_t rank = 0;
    ohlc_status status = OHLC_OK;
    for (uint8_t tile = 0; tile < 16; tile++) {
        if ((entry->mask & (1u << tile)) == 0) {
            continue;
        }
        status =
            ohlc_storage_tile(db, table, band, group, entry, tile, data + rank * OHLC_BLOCK_BYTES);
        const uint8_t* presence = ohlc_storage_presence(db, table, entry, tile);
        if (status != OHLC_OK || presence == NULL) {
            status = status == OHLC_OK ? OHLC_CORRUPT : status;
            goto cleanup;
        }
        uint8_t* meta = metadata + rank * 32;
        memcpy(meta, presence, 16);
        ohlc_put_u64(meta + 16, db->checkpoint_generation + 1);
        ohlc_put_u32(meta + 24,
                     tile_checksum(data + rank * OHLC_BLOCK_BYTES, meta, table, band, group, tile));
        rank++;
    }
    ohlc_table_files* files = NULL;
    status = ohlc_storage_table(db, table, true, &files);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    ohlc_volume* volume = NULL;
    size_t first = 0;
    status = allocate_extent(db, files, blocks, &volume, &first);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    /* Data block N of a volume always pairs with metadata entry N. A reused
     * extent lies below the published count; an appended one starts at it. */
    size_t count = atomic_load_explicit(&volume->meta_count, memory_order_relaxed);
    bool appended = first == count;
    uint64_t base = OHLC_PAGE_BYTES + (uint64_t)first * OHLC_BLOCK_BYTES;
    status = ohlc_write_full(volume->data_fd, data, blocks * OHLC_BLOCK_BYTES, base);
    if (status == OHLC_OK) {
        status = ohlc_write_full(volume->meta_fd, metadata, blocks * 32,
                                 OHLC_PAGE_BYTES + (uint64_t)first * 32u);
    }
    if (status != OHLC_OK) {
        db->failed = true;
        goto cleanup;
    }
    /* No live root references reused entries until this checkpoint publishes
     * its root, so rewriting them in place is invisible to readers. */
    pthread_mutex_lock(&db->storage_mutex);
    for (size_t i = 0; i < blocks; i++) {
        memcpy(meta_at(volume, first + i), metadata + i * 32, 32);
    }
    if (appended) {
        /* Readers that observe the new count also observe these entries. */
        atomic_store_explicit(&volume->meta_count, count + blocks, memory_order_release);
        volume->size += blocks * OHLC_BLOCK_BYTES;
    }
    volume->dirty = true;
    pthread_mutex_unlock(&db->storage_mutex);
    atomic_fetch_add_explicit(&db->data_bytes, blocks * OHLC_BLOCK_BYTES, memory_order_relaxed);
    output->base = base;
    output->mask = entry->mask;
    output->flags = 0;
    output->volume = volume->id;
cleanup:
    return status;
}

/* Returns the first block of an on-disk extent, or false when the extent does
 * not lie within the loaded part of the volume. */
static bool extent_blocks(ohlc_volume* volume, uint64_t base, size_t blocks, size_t* first) {
    if (base < OHLC_PAGE_BYTES || (base - OHLC_PAGE_BYTES) % OHLC_BLOCK_BYTES != 0) {
        return false;
    }
    uint64_t block = (base - OHLC_PAGE_BYTES) / OHLC_BLOCK_BYTES;
    size_t count = atomic_load_explicit(&volume->meta_count, memory_order_relaxed);
    if (block > count || blocks > count - block) {
        return false;
    }
    *first = (size_t)block;
    return true;
}

void ohlc_storage_reuse_candidates(ohlc_db* db) {
    size_t table_slots = ohlc_slot_capacity(&db->files);
    for (size_t slot = 0; slot < table_slots; slot++) {
        ohlc_table_files* files = ohlc_slot_get(&db->files, slot);
        if (files == NULL) {
            continue;
        }
        for (size_t i = 0; i < files->volume_count; i++) {
            ohlc_volume* volume = ohlc_slot_get(&files->volumes, i);
            if (volume != NULL) {
                size_t count = atomic_load_explicit(&volume->meta_count, memory_order_relaxed);
                free_range_add(volume, 0, count);
            }
        }
    }
}

void ohlc_storage_reuse_keep(ohlc_db* db, uint32_t table, const ohlc_group* entry) {
    if (entry->flags != 0 || entry->mask == 0) {
        return;
    }
    ohlc_volume* volume = find_volume(db, table, entry->volume);
    size_t blocks = ohlc_popcount(entry->mask);
    size_t first = 0;
    if (volume == NULL || !extent_blocks(volume, entry->base, blocks, &first)) {
        return;
    }
    for (size_t i = 0; i < blocks; i++) {
        if (block_is_free(volume, first + i)) {
            block_set_free(volume, first + i, false);
            volume->free_blocks--;
        }
    }
}

void ohlc_storage_reuse_free(ohlc_db* db, const ohlc_dead_extent* extent) {
    /* The files of a dropped table may already be closed. */
    ohlc_volume* volume = find_volume(db, extent->table, extent->volume);
    size_t first = 0;
    if (volume == NULL || !extent_blocks(volume, extent->base, extent->blocks, &first)) {
        return;
    }
    for (size_t i = 0; i < extent->blocks; i++) {
        cache_forget(db, extent->table, extent->volume, extent->base + i * OHLC_BLOCK_BYTES);
    }
    free_range_add(volume, first, extent->blocks);
}

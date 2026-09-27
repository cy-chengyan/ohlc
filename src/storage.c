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

#define OHLC_META_PER_PAGE 4096u

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
    uint8_t header[OHLC_BLOCK_BYTES] = {0};
    if (create) {
        memcpy(header, "OHLCFIL4", 8);
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
    if (memcmp(header, "OHLCFIL", 7) != 0 || checksum != ohlc_crc32c(0, header, sizeof(header))) {
        return OHLC_CORRUPT;
    }
    if (ohlc_get_u32(header + 8) != OHLC_FORMAT_VERSION) {
        return OHLC_UNSUPPORTED;
    }
    if (header[7] != '4' || ohlc_get_u32(header + 12) != type ||
        memcmp(header + 16, db->uuid, 16) != 0 || ohlc_get_u32(header + 32) != table ||
        ohlc_get_u32(header + 36) != volume || ohlc_get_u32(header + 40) != OHLC_BLOCK_BYTES ||
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
        db->catalog_fd = openat(db->directory_fd, "catalog-000001.dat",
                                O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (db->catalog_fd < 0) {
            return OHLC_IO;
        }
        status = ohlc_storage_header(db, db->catalog_fd, 1, 0, 1, 0, true);
        if (status != OHLC_OK || ohlc_sync(db->catalog_fd) != OHLC_OK ||
            fsync(db->directory_fd) != 0) {
            return OHLC_IO;
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
    for (size_t i = 0; i < volume->meta_capacity; i++) {
        ohlc_free(&db->allocator, volume->meta_pages[i]);
    }
    ohlc_free(&db->allocator, volume->meta_pages);
    ohlc_free(&db->allocator, volume);
}

void ohlc_storage_close(ohlc_db* db) {
    if (db->files != NULL) {
        for (uint32_t i = 0; i < db->options.max_tables; i++) {
            ohlc_table_files* files = db->files[i];
            if (files == NULL) {
                continue;
            }
            if (files->index_fd >= 0) {
                close(files->index_fd);
            }
            for (size_t j = 0; j < files->volume_count; j++) {
                volume_close(db, files->volumes[j]);
            }
            ohlc_free(&db->allocator, files->volumes);
            ohlc_free(&db->allocator, files);
        }
    }
    ohlc_free(&db->allocator, db->files);
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

static ohlc_status meta_reserve(ohlc_db* db, ohlc_volume* volume, size_t count) {
    size_t pages_needed = (count + OHLC_META_PER_PAGE - 1) / OHLC_META_PER_PAGE;
    if (pages_needed > volume->meta_capacity) {
        uint8_t** pages = ohlc_alloc(&db->allocator, pages_needed * sizeof(*pages));
        if (pages == NULL) {
            return OHLC_LIMIT;
        }
        if (volume->meta_capacity != 0) {
            memcpy(pages, volume->meta_pages, volume->meta_capacity * sizeof(*pages));
        }
        ohlc_free(&db->allocator, volume->meta_pages);
        volume->meta_pages = pages;
        size_t previous = volume->meta_capacity;
        volume->meta_capacity = pages_needed;
        for (size_t i = previous; i < pages_needed; i++) {
            pages[i] = ohlc_alloc(&db->allocator, OHLC_META_PER_PAGE * 32u);
            if (pages[i] == NULL) {
                return OHLC_LIMIT;
            }
        }
    }
    for (size_t i = 0; i < pages_needed; i++) {
        if (volume->meta_pages[i] == NULL) {
            volume->meta_pages[i] = ohlc_alloc(&db->allocator, OHLC_META_PER_PAGE * 32u);
            if (volume->meta_pages[i] == NULL) {
                return OHLC_LIMIT;
            }
        }
    }
    return OHLC_OK;
}

static uint8_t* meta_at(ohlc_volume* volume, uint64_t block) {
    return volume->meta_pages[block / OHLC_META_PER_PAGE] + (block % OHLC_META_PER_PAGE) * 32u;
}

static ohlc_status open_volume(ohlc_db* db, int directory, uint32_t table, uint32_t id, bool create,
                               ohlc_volume** output) {
    *output = NULL;
    ohlc_volume* volume = ohlc_alloc(&db->allocator, sizeof(*volume));
    if (volume == NULL) {
        return OHLC_LIMIT;
    }
    volume->id = id;
    volume->data_fd = -1;
    volume->meta_fd = -1;
    char name[64];
    int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW;
    if (create) {
        flags |= O_CREAT | O_EXCL;
    }
    snprintf(name, sizeof(name), "data-%06u.dat", id);
    volume->data_fd = openat(directory, name, flags, 0600);
    ohlc_status status = OHLC_IO;
    if (volume->data_fd < 0) {
        status = errno == ENOENT ? OHLC_NOT_FOUND : OHLC_IO;
        goto cleanup;
    }
    snprintf(name, sizeof(name), "data-%06u.meta", id);
    volume->meta_fd = openat(directory, name, flags, 0600);
    if (volume->meta_fd < 0) {
        status = errno == ENOENT ? OHLC_CORRUPT : OHLC_IO;
        goto cleanup;
    }
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
    if (status != OHLC_OK || volume->size < OHLC_BLOCK_BYTES) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    uint64_t meta_size = 0;
    status = file_size(volume->meta_fd, &meta_size);
    if (status != OHLC_OK || meta_size < OHLC_BLOCK_BYTES) {
        status = OHLC_CORRUPT;
        goto cleanup;
    }
    /* An unpublished tail may be incomplete. Retain complete physical pairs;
     * checkpoint references are validated separately before being accepted. */
    uint64_t blocks = (volume->size - OHLC_BLOCK_BYTES) / OHLC_BLOCK_BYTES;
    uint64_t meta_blocks = (meta_size - OHLC_BLOCK_BYTES) / 32u;
    if (meta_blocks < blocks) {
        blocks = meta_blocks;
    }
    if (blocks > SIZE_MAX) {
        status = OHLC_LIMIT;
        goto cleanup;
    }
    volume->size = OHLC_BLOCK_BYTES + blocks * OHLC_BLOCK_BYTES;
    status = meta_reserve(db, volume, (size_t)blocks);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    volume->meta_count = (size_t)blocks;
    for (size_t i = 0; i < volume->meta_capacity; i++) {
        size_t count = volume->meta_count - i * OHLC_META_PER_PAGE;
        if (count > OHLC_META_PER_PAGE) {
            count = OHLC_META_PER_PAGE;
        }
        status = ohlc_read_full(volume->meta_fd, volume->meta_pages[i], count * 32u,
                                OHLC_BLOCK_BYTES + (uint64_t)i * OHLC_META_PER_PAGE * 32u);
        if (status != OHLC_OK) {
            goto cleanup;
        }
    }
    if (create && (ohlc_sync(volume->data_fd) != OHLC_OK || ohlc_sync(volume->meta_fd) != OHLC_OK ||
                   fsync(directory) != 0)) {
        status = OHLC_IO;
        goto cleanup;
    }
    *output = volume;
    return OHLC_OK;
cleanup:
    volume_close(db, volume);
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

/* The commit/checkpoint owner serializes calls that may create file objects. */
ohlc_status ohlc_storage_table(ohlc_db* db, uint32_t table_id, bool create,
                               ohlc_table_files** output) {
    if (table_id == 0 || table_id > db->options.max_tables) {
        return OHLC_CORRUPT;
    }
    if (db->files[table_id - 1] != NULL) {
        *output = db->files[table_id - 1];
        return OHLC_OK;
    }
    int directory = -1;
    ohlc_status directory_status = table_directory(db, table_id, create, &directory);
    if (directory_status != OHLC_OK) {
        return directory_status;
    }
    ohlc_table_files* files = ohlc_alloc(&db->allocator, sizeof(*files));
    if (files == NULL) {
        close(directory);
        return OHLC_LIMIT;
    }
    files->id = table_id;
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
    if (create && !new_index && files->index_size < OHLC_BLOCK_BYTES) {
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
    status = next_volume_id(directory, &files->next_volume_id);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    if (new_index && (ohlc_sync(files->index_fd) != OHLC_OK || fsync(directory) != 0)) {
        status = OHLC_IO;
        goto cleanup;
    }
    close(directory);
    pthread_mutex_lock(&db->storage_mutex);
    db->files[table_id - 1] = files;
    pthread_mutex_unlock(&db->storage_mutex);
    *output = files;
    return OHLC_OK;
cleanup:
    if (files->index_fd >= 0) {
        close(files->index_fd);
    }
    for (size_t i = 0; i < files->volume_count; i++) {
        volume_close(db, files->volumes[i]);
    }
    ohlc_free(&db->allocator, files->volumes);
    ohlc_free(&db->allocator, files);
    close(directory);
    return status;
}

/* Only volumes reachable from a checkpoint are required for recovery. An
 * incomplete, unpublished rollover file must not invalidate an older root. */
ohlc_status ohlc_storage_load_volume(ohlc_db* db, ohlc_table_files* files, uint32_t id) {
    if (id == 0) {
        return OHLC_CORRUPT;
    }
    if (id <= files->volume_count && files->volumes[id - 1] != NULL) {
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
    if (id > files->volume_count) {
        ohlc_volume** volumes = ohlc_alloc(&db->allocator, (size_t)id * sizeof(*volumes));
        if (volumes == NULL) {
            volume_close(db, volume);
            return OHLC_LIMIT;
        }
        pthread_mutex_lock(&db->storage_mutex);
        if (files->volume_count != 0) {
            memcpy(volumes, files->volumes, files->volume_count * sizeof(*volumes));
        }
        ohlc_free(&db->allocator, files->volumes);
        files->volumes = volumes;
        files->volume_count = id;
        pthread_mutex_unlock(&db->storage_mutex);
    }
    pthread_mutex_lock(&db->storage_mutex);
    files->volumes[id - 1] = volume;
    pthread_mutex_unlock(&db->storage_mutex);
    return OHLC_OK;
}

static ohlc_volume* find_volume(ohlc_db* db, uint32_t table, uint32_t volume) {
    if (table == 0 || table > db->options.max_tables || volume == 0) {
        return NULL;
    }
    ohlc_table_files* files = db->files[table - 1];
    if (files == NULL || volume > files->volume_count) {
        return NULL;
    }
    return files->volumes[volume - 1];
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
    const uint8_t* presence = NULL;
    pthread_mutex_lock(&db->storage_mutex);
    ohlc_volume* volume = find_volume(db, table, entry->volume);
    if (volume != NULL && offset >= OHLC_BLOCK_BYTES && offset % OHLC_BLOCK_BYTES == 0 &&
        (offset - OHLC_BLOCK_BYTES) / OHLC_BLOCK_BYTES < volume->meta_count) {
        presence = meta_at(volume, (offset - OHLC_BLOCK_BYTES) / OHLC_BLOCK_BYTES);
    }
    pthread_mutex_unlock(&db->storage_mutex);
    return presence;
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

static void cache_acquire(ohlc_db* db, uint32_t table, uint32_t volume, ohlc_tile_view* view) {
    uint64_t hash = (view->offset / OHLC_BLOCK_BYTES) * UINT64_C(0x9e3779b185ebca87) ^
                    (uint64_t)table * UINT64_C(0xc2b2ae3d27d4eb4f) ^ volume;
    hash = (hash ^ (hash >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    hash = (hash ^ (hash >> 27)) * UINT64_C(0x94d049bb133111eb);
    hash ^= hash >> 31;
    ohlc_cache* cache = &db->cache[hash & 15u];
    if (cache->count == 0) {
        atomic_fetch_add_explicit(&db->cache_misses, 1, memory_order_relaxed);
        return;
    }
    pthread_mutex_lock(&cache->mutex);
    size_t start = (size_t)((hash >> 4) % (cache->count / 4)) * 4;
    ohlc_cache_entry* chosen = NULL;
    for (size_t i = 0; i < 4; i++) {
        ohlc_cache_entry* candidate = &cache->entries[start + i];
        if ((candidate->valid || candidate->loading) && candidate->table == table &&
            candidate->volume == volume && candidate->offset == view->offset &&
            candidate->band == view->band && candidate->group == view->group &&
            candidate->tile == view->tile) {
            chosen = candidate;
            atomic_fetch_add_explicit(&db->cache_hits, 1, memory_order_relaxed);
            break;
        }
    }
    if (chosen == NULL) {
        atomic_fetch_add_explicit(&db->cache_misses, 1, memory_order_relaxed);
        for (size_t i = 0; i < 8; i++) {
            ohlc_cache_entry* candidate = &cache->entries[start + cache->hand++ % 4];
            if (candidate->pins != 0) {
                continue;
            }
            if (candidate->valid && candidate->visited) {
                candidate->visited = false;
                continue;
            }
            chosen = candidate;
            chosen->table = table;
            chosen->volume = volume;
            chosen->offset = view->offset;
            chosen->band = view->band;
            chosen->group = view->group;
            chosen->tile = view->tile;
            chosen->valid = false;
            chosen->loading = true;
            chosen->status = OHLC_OK;
            view->owns_load = true;
            break;
        }
    }
    if (chosen != NULL) {
        chosen->pins++;
        chosen->visited = true;
        view->cache = cache;
        view->cached = chosen;
    }
    pthread_mutex_unlock(&cache->mutex);
}

static ohlc_status prepare_view(ohlc_db* db, uint32_t table, ohlc_tile_view* view) {
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
    pthread_mutex_lock(&db->storage_mutex);
    ohlc_volume* volume = find_volume(db, table, entry->volume);
    view->fd = volume == NULL ? -1 : volume->data_fd;
    pthread_mutex_unlock(&db->storage_mutex);
    if (view->fd < 0) {
        return OHLC_CORRUPT;
    }
    cache_acquire(db, table, entry->volume, view);
    return OHLC_OK;
}

static void publish_view(ohlc_tile_view* view, ohlc_status status) {
    if (view->owns_load) {
        /* Pins prevent eviction; only this loader writes the data. */
        if (status == OHLC_OK) {
            memcpy(view->cached->data, view->buffer, OHLC_BLOCK_BYTES);
        }
        pthread_mutex_lock(&view->cache->mutex);
        view->cached->status = status;
        view->cached->valid = status == OHLC_OK;
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
            pthread_mutex_lock(&view->cache->mutex);
            view->cached->pins--;
            pthread_mutex_unlock(&view->cache->mutex);
            view->cached = NULL;
        }
        view->data = NULL;
    }
}

ohlc_status ohlc_storage_read(ohlc_db* db, uint32_t table, ohlc_tile_view* views, size_t count) {
    if (count > OHLC_READ_WINDOW) {
        return OHLC_INVALID;
    }
    ohlc_io_request requests[OHLC_READ_WINDOW] = {0};
    size_t request_for_view[OHLC_READ_WINDOW] = {0};
    size_t request_count = 0;
    ohlc_status status = OHLC_OK;
    size_t prepared = 0;
    for (; prepared < count; prepared++) {
        ohlc_tile_view* view = &views[prepared];
        status = prepare_view(db, table, view);
        if (status != OHLC_OK) {
            break;
        }
        if (view->data != NULL || (view->cached != NULL && !view->owns_load)) {
            continue;
        }
        ohlc_io_request* previous = request_count == 0 ? NULL : &requests[request_count - 1];
        if (previous != NULL && previous->fd == view->fd &&
            previous->offset + previous->size == view->offset &&
            previous->data + previous->size == view->buffer) {
            previous->size += OHLC_BLOCK_BYTES;
        } else {
            ohlc_io_request* request = &requests[request_count++];
            request->fd = view->fd;
            request->offset = view->offset;
            request->data = view->buffer;
            request->size = OHLC_BLOCK_BYTES;
        }
        request_for_view[prepared] = request_count - 1;
    }
    if (status != OHLC_OK) {
        for (size_t i = 0; i < prepared; i++) {
            publish_view(&views[i], status);
        }
        ohlc_storage_release(views, prepared);
        return status;
    }
    /* Submit our loads before waiting on another window's loads. This avoids
     * cycles when two windows reserve overlapping cache entries. */
    ohlc_io_read(db, requests, request_count);
    for (size_t i = 0; i < count; i++) {
        ohlc_tile_view* view = &views[i];
        if (view->data != NULL || (view->cached != NULL && !view->owns_load)) {
            continue;
        }
        ohlc_status loaded = requests[request_for_view[i]].status;
        if (loaded == OHLC_OK &&
            tile_checksum(view->buffer, view->presence, table, view->band, view->group,
                          view->tile) != ohlc_get_u32(view->presence + 24)) {
            loaded = OHLC_CORRUPT;
        }
        publish_view(view, loaded);
        view->data = view->buffer;
        if (loaded != OHLC_OK) {
            status = loaded;
        }
    }
    for (size_t i = 0; i < count; i++) {
        ohlc_tile_view* view = &views[i];
        if (view->cached == NULL) {
            continue;
        }
        pthread_mutex_lock(&view->cache->mutex);
        while (view->cached->loading) {
            pthread_cond_wait(&view->cache->changed, &view->cache->mutex);
        }
        if (view->cached->status != OHLC_OK) {
            status = view->cached->status;
        }
        view->data = view->cached->data;
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

static ohlc_status allocate_extent(ohlc_db* db, ohlc_table_files* files, size_t blocks,
                                   ohlc_volume** output) {
    ohlc_volume* volume = files->volume_count == 0 ? NULL : files->volumes[files->volume_count - 1];
    uint64_t bytes = (uint64_t)blocks * OHLC_BLOCK_BYTES;
    if (volume == NULL || volume->size > db->options.data_volume_bytes - bytes) {
        if (files->next_volume_id > UINT32_MAX) {
            return OHLC_LIMIT;
        }
        uint32_t id = (uint32_t)files->next_volume_id;
        ohlc_volume** volumes = ohlc_alloc(&db->allocator, (size_t)id * sizeof(*volumes));
        if (volumes == NULL) {
            return OHLC_LIMIT;
        }
        int directory = -1;
        ohlc_status directory_status = table_directory(db, files->id, false, &directory);
        if (directory_status != OHLC_OK) {
            ohlc_free(&db->allocator, volumes);
            return directory_status;
        }
        ohlc_status status = open_volume(db, directory, files->id, id, true, &volume);
        close(directory);
        if (status != OHLC_OK) {
            ohlc_free(&db->allocator, volumes);
            return status;
        }
        pthread_mutex_lock(&db->storage_mutex);
        if (files->volume_count != 0) {
            memcpy(volumes, files->volumes, files->volume_count * sizeof(*volumes));
        }
        volumes[id - 1] = volume;
        ohlc_free(&db->allocator, files->volumes);
        files->volumes = volumes;
        files->volume_count = id;
        files->next_volume_id++;
        pthread_mutex_unlock(&db->storage_mutex);
    }
    pthread_mutex_lock(&db->storage_mutex);
    ohlc_status status = meta_reserve(db, volume, volume->meta_count + blocks);
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
    status = allocate_extent(db, files, blocks, &volume);
    if (status != OHLC_OK) {
        goto cleanup;
    }
    uint64_t base = volume->size;
    status = ohlc_write_full(volume->data_fd, data, blocks * OHLC_BLOCK_BYTES, base);
    if (status == OHLC_OK) {
        status = ohlc_write_full(volume->meta_fd, metadata, blocks * 32,
                                 OHLC_BLOCK_BYTES + (uint64_t)volume->meta_count * 32u);
    }
    if (status != OHLC_OK) {
        db->failed = true;
        goto cleanup;
    }
    pthread_mutex_lock(&db->storage_mutex);
    for (size_t i = 0; i < blocks; i++) {
        memcpy(meta_at(volume, volume->meta_count + i), metadata + i * 32, 32);
    }
    volume->meta_count += blocks;
    volume->size += blocks * OHLC_BLOCK_BYTES;
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

/* SPDX-License-Identifier: Apache-2.0 */
#include "internal.h"

static void read_request(ohlc_db* db, ohlc_io_request* request) {
    request->status = ohlc_read_full(request->fd, request->data, request->size, request->offset);
    atomic_fetch_add_explicit(&db->disk_read_calls, 1, memory_order_relaxed);
    if (request->status == OHLC_OK) {
        atomic_fetch_add_explicit(&db->disk_read_bytes, request->size, memory_order_relaxed);
    }
}

static void* read_worker(void* argument) {
    ohlc_db* db = argument;
    ohlc_io_pool* pool = &db->io;
    pthread_mutex_lock(&pool->mutex);
    for (;;) {
        while (pool->count == 0 && !pool->stopping) {
            pthread_cond_wait(&pool->changed, &pool->mutex);
        }
        if (pool->count == 0 && pool->stopping) {
            break;
        }
        ohlc_io_request* request = pool->queue[pool->head];
        pool->head = (pool->head + 1u) % OHLC_IO_QUEUE;
        pool->count--;
        pthread_cond_broadcast(&pool->changed);
        pthread_mutex_unlock(&pool->mutex);
        read_request(db, request);
        pthread_mutex_lock(&pool->mutex);
        (*request->pending)--;
        pthread_cond_broadcast(&pool->changed);
    }
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
}

ohlc_status ohlc_io_init(ohlc_db* db) {
    ohlc_io_pool* pool = &db->io;
    if (pthread_mutex_init(&pool->mutex, NULL) != 0) {
        return OHLC_LIMIT;
    }
    if (pthread_cond_init(&pool->changed, NULL) != 0) {
        pthread_mutex_destroy(&pool->mutex);
        return OHLC_LIMIT;
    }
    for (size_t i = 0; i < db->options.read_workers; i++) {
        if (pthread_create(&pool->workers[i], NULL, read_worker, db) != 0) {
            ohlc_io_destroy(db);
            return OHLC_LIMIT;
        }
        pool->worker_count++;
    }
    return OHLC_OK;
}

void ohlc_io_destroy(ohlc_db* db) {
    ohlc_io_pool* pool = &db->io;
    pthread_mutex_lock(&pool->mutex);
    pool->stopping = true;
    pthread_cond_broadcast(&pool->changed);
    pthread_mutex_unlock(&pool->mutex);
    for (size_t i = 0; i < pool->worker_count; i++) {
        pthread_join(pool->workers[i], NULL);
    }
    pthread_cond_destroy(&pool->changed);
    pthread_mutex_destroy(&pool->mutex);
}

void ohlc_io_read(ohlc_db* db, ohlc_io_request* requests, size_t count) {
    ohlc_io_pool* pool = &db->io;
    if (count <= 1 || pool->worker_count == 0) {
        for (size_t i = 0; i < count; i++) {
            read_request(db, &requests[i]);
        }
        return;
    }
    size_t pending = count;
    pthread_mutex_lock(&pool->mutex);
    for (size_t i = 0; i < count; i++) {
        while (pool->count == OHLC_IO_QUEUE) {
            pthread_cond_wait(&pool->changed, &pool->mutex);
        }
        requests[i].pending = &pending;
        pool->queue[(pool->head + pool->count) % OHLC_IO_QUEUE] = &requests[i];
        pool->count++;
        pthread_cond_broadcast(&pool->changed);
    }
    while (pending != 0) {
        pthread_cond_wait(&pool->changed, &pool->mutex);
    }
    pthread_mutex_unlock(&pool->mutex);
}

/* SPDX-License-Identifier: Apache-2.0 */
#include "io_ohlc_Database_Native.h"
#include "ohlc/ohlc.h"

#include <stdint.h>
#include <string.h>

/* Java owns the lifecycle locks. No call may outlive its database read lease,
 * and operations on a cursor hold that cursor's monitor as well. JNI arrays
 * are copied before engine calls; direct buffers are borrowed for one call. */
_Static_assert(sizeof(uintptr_t) <= sizeof(jlong), "JNI handles must fit pointers");

static jlong long_bits(uint64_t value) {
    jlong result;
    memcpy(&result, &value, sizeof(result));
    return result;
}

static ohlc_db* database(jlong handle) {
    return (ohlc_db*)(uintptr_t)(uint64_t)handle;
}

static ohlc_cursor* cursor(jlong handle) {
    return (ohlc_cursor*)(uintptr_t)(uint64_t)handle;
}

static bool check(JNIEnv* env, ohlc_status status) {
    if (status == OHLC_OK) {
        return true;
    }
    if ((*env)->ExceptionCheck(env)) {
        return false;
    }
    jclass type = (*env)->FindClass(env, "io/ohlc/Ohlc$Failure");
    if (type == NULL) {
        return false;
    }
    jmethodID constructor = (*env)->GetMethodID(env, type, "<init>", "(ILjava/lang/String;)V");
    if (constructor == NULL) {
        return false;
    }
    jstring message = (*env)->NewStringUTF(env, ohlc_status_string(status));
    if (message == NULL) {
        return false;
    }
    jobject error = (*env)->NewObject(env, type, constructor, (jint)status, message);
    if (error != NULL) {
        (*env)->Throw(env, (jthrowable)error);
    }
    return false;
}

static bool uint32_valid(jlong value) {
    return value >= 0 && (uint64_t)value <= UINT32_MAX;
}

static bool copy_text(JNIEnv* env, jbyteArray input, char* output, size_t capacity) {
    if (input == NULL) {
        return check(env, OHLC_INVALID);
    }
    jsize size = (*env)->GetArrayLength(env, input);
    if ((size_t)size >= capacity) {
        return check(env, OHLC_INVALID);
    }
    (*env)->GetByteArrayRegion(env, input, 0, size, (jbyte*)output);
    if ((*env)->ExceptionCheck(env)) {
        return false;
    }
    if (memchr(output, 0, (size_t)size) != NULL) {
        return check(env, OHLC_INVALID);
    }
    output[size] = '\0';
    return true;
}

static jbyteArray bytes(JNIEnv* env, const void* data, size_t size) {
    jbyteArray result = (*env)->NewByteArray(env, (jsize)size);
    if (result != NULL) {
        (*env)->SetByteArrayRegion(env, result, 0, (jsize)size, data);
    }
    return result;
}

static jlongArray longs(JNIEnv* env, const uint64_t* values, size_t count) {
    jlong converted[11];
    for (size_t i = 0; i < count; i++) {
        converted[i] = long_bits(values[i]);
    }
    jlongArray result = (*env)->NewLongArray(env, (jsize)count);
    if (result != NULL) {
        (*env)->SetLongArrayRegion(env, result, 0, (jsize)count, converted);
    }
    return result;
}

static void put_u32(uint8_t* output, uint32_t value) {
    for (unsigned i = 0; i < 4; i++) {
        output[i] = (uint8_t)(value >> (8u * i));
    }
}

static size_t put_text(uint8_t* output, const char* value) {
    size_t size = strlen(value);
    put_u32(output, (uint32_t)size);
    memcpy(output + 4, value, size);
    return 4 + size;
}

static jbyteArray table_bytes(JNIEnv* env, const ohlc_table_info* info) {
    /* Encode UTF-8 bytes explicitly: JNI modified UTF-8 would change non-BMP
     * characters. Never expose a compiler-dependent native struct layout. */
    uint8_t output[32 + sizeof(info->name) + sizeof(info->timezone) + sizeof(info->description)];
    put_u32(output, info->id);
    put_u32(output + 4, (uint32_t)info->created_seq);
    put_u32(output + 8, (uint32_t)(info->created_seq >> 32));
    put_u32(output + 12, (uint32_t)info->period_unit);
    put_u32(output + 16, info->period_count);
    size_t size = 20;
    size += put_text(output + size, info->name);
    size += put_text(output + size, info->timezone);
    size += put_text(output + size, info->description);
    return bytes(env, output, size);
}

static void* direct_buffer(JNIEnv* env, jobject buffer, jint offset, size_t size) {
    if (buffer == NULL || offset < 0) {
        check(env, OHLC_INVALID);
        return NULL;
    }
    jlong capacity = (*env)->GetDirectBufferCapacity(env, buffer);
    void* data = (*env)->GetDirectBufferAddress(env, buffer);
    if (data == NULL || capacity < offset || size > (uint64_t)(capacity - offset)) {
        check(env, OHLC_INVALID);
        return NULL;
    }
    return (uint8_t*)data + offset;
}

JNIEXPORT jint JNICALL Java_io_ohlc_Database_00024Native_abiVersion(JNIEnv* env, jclass type) {
    (void)env;
    (void)type;
    return (jint)ohlc_abi_version();
}

JNIEXPORT jlongArray JNICALL Java_io_ohlc_Database_00024Native_defaults(JNIEnv* env, jclass type) {
    (void)type;
    ohlc_options options;
    ohlc_options_init(&options);
    uint64_t values[] = {options.cache_bytes,       options.memory_limit, options.data_volume_bytes,
                         options.wal_segment_bytes, options.max_tables,   options.max_cursors,
                         options.max_query_ms,      options.read_workers};
    return longs(env, values, 8);
}

JNIEXPORT jlong JNICALL Java_io_ohlc_Database_00024Native_open(JNIEnv* env, jclass type,
                                                               jbyteArray path, jlongArray values,
                                                               jboolean create) {
    (void)type;
    char directory[4096];
    if (!copy_text(env, path, directory, sizeof(directory))) {
        return 0;
    }
    if (values == NULL || (*env)->GetArrayLength(env, values) != 8) {
        check(env, OHLC_INVALID);
        return 0;
    }
    jlong v[8];
    (*env)->GetLongArrayRegion(env, values, 0, 8, v);
    if ((*env)->ExceptionCheck(env)) {
        return 0;
    }
    if (v[0] < 0 || v[1] <= 0 || (uint64_t)v[0] > SIZE_MAX || (uint64_t)v[1] > SIZE_MAX ||
        v[2] <= 0 || v[3] <= 0 || !uint32_valid(v[4]) || !uint32_valid(v[5]) ||
        !uint32_valid(v[6]) || !uint32_valid(v[7])) {
        check(env, OHLC_INVALID);
        return 0;
    }
    ohlc_options options;
    ohlc_options_init(&options);
    options.cache_bytes = (size_t)v[0];
    options.memory_limit = (size_t)v[1];
    options.data_volume_bytes = (uint64_t)v[2];
    options.wal_segment_bytes = (uint64_t)v[3];
    options.max_tables = (uint32_t)v[4];
    options.max_cursors = (uint32_t)v[5];
    options.max_query_ms = (uint32_t)v[6];
    options.read_workers = (uint32_t)v[7];
    options.create_if_missing = create != JNI_FALSE;
    ohlc_db* db = NULL;
    if (!check(env, ohlc_open(directory, &options, &db))) {
        return 0;
    }
    return long_bits((uintptr_t)db);
}

JNIEXPORT jint JNICALL Java_io_ohlc_Database_00024Native_close(JNIEnv* env, jclass type, jlong db) {
    (void)env;
    (void)type;
    return (jint)ohlc_close(database(db));
}

JNIEXPORT jbyteArray JNICALL Java_io_ohlc_Database_00024Native_uuid(JNIEnv* env, jclass type,
                                                                    jlong db) {
    (void)type;
    uint8_t uuid[16];
    ohlc_uuid(database(db), uuid);
    return bytes(env, uuid, sizeof(uuid));
}

JNIEXPORT jlongArray JNICALL Java_io_ohlc_Database_00024Native_stats(JNIEnv* env, jclass type,
                                                                     jlong db) {
    (void)type;
    ohlc_stats stats;
    ohlc_get_stats(database(db), &stats);
    uint64_t values[] = {stats.commit_seq,      stats.checkpoint_seq, stats.ticker_count,
                         stats.table_count,     stats.memory_bytes,   stats.disk_read_bytes,
                         stats.disk_read_calls, stats.cache_hits,     stats.cache_misses,
                         stats.wal_bytes,       stats.data_bytes};
    return longs(env, values, 11);
}

JNIEXPORT void JNICALL Java_io_ohlc_Database_00024Native_checkpoint(JNIEnv* env, jclass type,
                                                                    jlong db) {
    (void)type;
    check(env, ohlc_checkpoint(database(db)));
}

JNIEXPORT jbyteArray JNICALL Java_io_ohlc_Database_00024Native_table(JNIEnv* env, jclass type,
                                                                     jlong db, jbyteArray name,
                                                                     jlong id) {
    (void)type;
    ohlc_table_info info;
    ohlc_status status;
    if (name == NULL) {
        if (!uint32_valid(id)) {
            check(env, OHLC_INVALID);
            return NULL;
        }
        status = ohlc_table_get(database(db), (uint32_t)id, &info);
    } else {
        char text[64];
        if (!copy_text(env, name, text, sizeof(text))) {
            return NULL;
        }
        status = ohlc_table_open(database(db), text, &info);
    }
    return check(env, status) ? table_bytes(env, &info) : NULL;
}

JNIEXPORT jbyteArray JNICALL Java_io_ohlc_Database_00024Native_create(JNIEnv* env, jclass type,
                                                                      jlong db, jbyteArray name,
                                                                      jlong period, jboolean days,
                                                                      jbyteArray timezone,
                                                                      jbyteArray description) {
    (void)type;
    char name_text[64];
    char zone_text[256];
    char description_text[4097];
    if (!copy_text(env, name, name_text, sizeof(name_text)) ||
        !copy_text(env, timezone, zone_text, sizeof(zone_text)) ||
        !copy_text(env, description, description_text, sizeof(description_text))) {
        return NULL;
    }
    if (!uint32_valid(period)) {
        check(env, OHLC_INVALID);
        return NULL;
    }
    ohlc_table_definition definition = {.name = name_text,
                                        .period_unit = days ? OHLC_DAY : OHLC_MINUTE,
                                        .period_count = (uint32_t)period,
                                        .timezone = zone_text,
                                        .description = description_text};
    ohlc_table_info info;
    if (!check(env, ohlc_table_create(database(db), &definition, &info))) {
        return NULL;
    }
    return table_bytes(env, &info);
}

JNIEXPORT jlongArray JNICALL Java_io_ohlc_Database_00024Native_ticker(JNIEnv* env, jclass type,
                                                                      jlong db, jbyteArray ticker,
                                                                      jboolean registering) {
    (void)type;
    if (ticker == NULL) {
        check(env, OHLC_INVALID);
        return NULL;
    }
    jsize size = (*env)->GetArrayLength(env, ticker);
    if (size < 1 || size > 4096) {
        check(env, OHLC_INVALID);
        return NULL;
    }
    uint8_t data[4096];
    (*env)->GetByteArrayRegion(env, ticker, 0, size, (jbyte*)data);
    if ((*env)->ExceptionCheck(env)) {
        return NULL;
    }
    uint32_t code = 0;
    uint64_t sequence = 0;
    ohlc_bytes value = {data, (size_t)size};
    ohlc_status status = registering ? ohlc_register(database(db), value, &code, &sequence)
                                     : ohlc_resolve(database(db), value, &code);
    if (!check(env, status)) {
        return NULL;
    }
    uint64_t result[] = {code, sequence};
    return longs(env, result, 2);
}

JNIEXPORT jbyteArray JNICALL Java_io_ohlc_Database_00024Native_tickerAt(JNIEnv* env, jclass type,
                                                                        jlong db, jlong code) {
    (void)type;
    if (!uint32_valid(code)) {
        check(env, OHLC_INVALID);
        return NULL;
    }
    ohlc_bytes value;
    if (!check(env, ohlc_ticker(database(db), (uint32_t)code, &value))) {
        return NULL;
    }
    return bytes(env, value.data, value.size);
}

JNIEXPORT jlong JNICALL Java_io_ohlc_Database_00024Native_write(JNIEnv* env, jclass type, jlong db,
                                                                jlong table, jobject rows,
                                                                jint offset, jint size) {
    (void)type;
    if (!uint32_valid(table) || size <= 0 || size % (jint)OHLC_WRITE_BYTES != 0 ||
        size / (jint)OHLC_WRITE_BYTES > (jint)OHLC_MAX_BATCH_ROWS) {
        check(env, OHLC_INVALID);
        return 0;
    }
    void* data = direct_buffer(env, rows, offset, (size_t)size);
    if (data == NULL) {
        return 0;
    }
    uint64_t sequence = 0;
    check(env, ohlc_write(database(db), (uint32_t)table, data, (size_t)size / OHLC_WRITE_BYTES,
                          &sequence));
    return long_bits(sequence);
}

JNIEXPORT jlong JNICALL Java_io_ohlc_Database_00024Native_series(JNIEnv* env, jclass type, jlong db,
                                                                 jlong table, jlong ticker,
                                                                 jlong start, jlong end) {
    (void)type;
    if (!uint32_valid(table) || !uint32_valid(ticker) || !uint32_valid(start) || end < 0 ||
        (uint64_t)end > UINT64_C(4294967296)) {
        check(env, OHLC_INVALID);
        return 0;
    }
    ohlc_cursor* result = NULL;
    if (!check(env, ohlc_series(database(db), (uint32_t)table, (uint32_t)ticker, (uint32_t)start,
                                (uint64_t)end, &result))) {
        return 0;
    }
    return long_bits((uintptr_t)result);
}

JNIEXPORT jlong JNICALL Java_io_ohlc_Database_00024Native_cross(JNIEnv* env, jclass type, jlong db,
                                                                jlong table, jlong time) {
    (void)type;
    if (!uint32_valid(table) || !uint32_valid(time)) {
        check(env, OHLC_INVALID);
        return 0;
    }
    ohlc_cursor* result = NULL;
    if (!check(env, ohlc_cross(database(db), (uint32_t)table, (uint32_t)time, &result))) {
        return 0;
    }
    return long_bits((uintptr_t)result);
}

JNIEXPORT jlong JNICALL Java_io_ohlc_Database_00024Native_sequence(JNIEnv* env, jclass type,
                                                                   jlong handle) {
    (void)env;
    (void)type;
    return long_bits(ohlc_cursor_sequence(cursor(handle)));
}

JNIEXPORT jint JNICALL Java_io_ohlc_Database_00024Native_next(JNIEnv* env, jclass type,
                                                              jlong handle, jobject target,
                                                              jint offset, jint capacity) {
    (void)type;
    if (capacity < 1 || capacity > 16384) {
        check(env, OHLC_INVALID);
        return 0;
    }
    void* output = direct_buffer(env, target, offset, (size_t)capacity * OHLC_RESULT_BYTES);
    if (output == NULL) {
        return 0;
    }
    size_t count = 0;
    check(env, ohlc_cursor_next(cursor(handle), output, (size_t)capacity, &count));
    return (jint)count;
}

JNIEXPORT void JNICALL Java_io_ohlc_Database_00024Native_closeCursor(JNIEnv* env, jclass type,
                                                                     jlong handle) {
    (void)env;
    (void)type;
    ohlc_cursor_close(cursor(handle));
}

JNIEXPORT jlong JNICALL Java_io_ohlc_Database_00024Native_parse(JNIEnv* env, jclass type,
                                                                jboolean days, jbyteArray timezone,
                                                                jbyteArray time) {
    (void)type;
    ohlc_table_info info = {0};
    info.period_unit = days ? OHLC_DAY : OHLC_MINUTE;
    char input[4096];
    if (!copy_text(env, timezone, info.timezone, sizeof(info.timezone)) ||
        !copy_text(env, time, input, sizeof(input))) {
        return 0;
    }
    uint32_t key = 0;
    check(env, ohlc_time_parse(&info, input, &key));
    return (jlong)key;
}

JNIEXPORT jstring JNICALL Java_io_ohlc_Database_00024Native_format(JNIEnv* env, jclass type,
                                                                   jboolean days, jlong key) {
    (void)type;
    if (!uint32_valid(key)) {
        check(env, OHLC_INVALID);
        return NULL;
    }
    ohlc_table_info info = {0};
    info.period_unit = days ? OHLC_DAY : OHLC_MINUTE;
    char output[64];
    if (!check(env, ohlc_time_format(&info, (uint32_t)key, output, sizeof(output)))) {
        return NULL;
    }
    return (*env)->NewStringUTF(env, output);
}

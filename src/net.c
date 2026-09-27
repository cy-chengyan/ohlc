/* SPDX-License-Identifier: Apache-2.0 */
#include "net.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

static BIO_METHOD* socket_method;
static pthread_once_t socket_method_once = PTHREAD_ONCE_INIT;

static int socket_create(BIO* bio) {
    BIO_set_init(bio, 1);
    return 1;
}

static int socket_destroy(BIO* bio) {
    BIO_set_data(bio, NULL);
    return 1;
}

static int socket_read(BIO* bio, char* output, int size) {
    const ohlc_transport* transport = BIO_get_data(bio);
    BIO_clear_retry_flags(bio);
    ssize_t result = recv(transport->fd, output, (size_t)size, 0);
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        BIO_set_retry_read(bio);
    }
    return (int)result;
}

static int socket_write(BIO* bio, const char* input, int size) {
    const ohlc_transport* transport = BIO_get_data(bio);
    BIO_clear_retry_flags(bio);
    int flags = 0;
#ifdef MSG_NOSIGNAL
    flags = MSG_NOSIGNAL;
#endif
    ssize_t result = send(transport->fd, input, (size_t)size, flags);
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        BIO_set_retry_write(bio);
    }
    return (int)result;
}

static long socket_control(BIO* bio, int command, long argument, void* pointer) {
    (void)bio;
    (void)argument;
    (void)pointer;
    return command == BIO_CTRL_FLUSH ? 1 : 0;
}

static void socket_method_initialize(void) {
    BIO_METHOD* method = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "ohlc socket");
    if (method != NULL && BIO_meth_set_create(method, socket_create) == 1 &&
        BIO_meth_set_destroy(method, socket_destroy) == 1 &&
        BIO_meth_set_read(method, socket_read) == 1 &&
        BIO_meth_set_write(method, socket_write) == 1 &&
        BIO_meth_set_ctrl(method, socket_control) == 1) {
        socket_method = method;
    } else {
        BIO_meth_free(method);
    }
}

ohlc_status ohlc_net_tls_attach(ohlc_transport* transport) {
    /* A custom socket BIO suppresses SIGPIPE without changing process signal
     * dispositions. SSL owns the BIO; the transport alone owns the socket. */
    pthread_once(&socket_method_once, socket_method_initialize);
    if (socket_method == NULL) {
        return OHLC_LIMIT;
    }
    BIO* bio = BIO_new(socket_method);
    if (bio == NULL) {
        return OHLC_LIMIT;
    }
    BIO_set_data(bio, transport);
    SSL_set_bio(transport->ssl, bio, bio);
    return OHLC_OK;
}

ohlc_status ohlc_net_prepare(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0 ||
        fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
        return OHLC_IO;
    }
    int enabled = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
#ifdef SO_NOSIGPIPE
    if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) != 0) {
        return OHLC_IO;
    }
#endif
    return OHLC_OK;
}

void ohlc_net_close(ohlc_transport* transport) {
    SSL_free(transport->ssl);
    transport->ssl = NULL;
    if (transport->fd >= 0) {
        close(transport->fd);
        transport->fd = -1;
    }
}

ohlc_status ohlc_net_wait(int fd, short events, uint64_t deadline) {
    for (;;) {
        uint64_t now = ohlc_monotonic_ms();
        if (now >= deadline) {
            return OHLC_CANCELLED;
        }
        uint64_t remaining = deadline - now;
        int timeout = remaining > INT_MAX ? INT_MAX : (int)remaining;
        struct pollfd item = {.fd = fd, .events = events};
        int result = poll(&item, 1, timeout);
        if (result > 0) {
            return (item.revents & events) != 0 ? OHLC_OK : OHLC_IO;
        }
        if (result < 0 && errno != EINTR) {
            return OHLC_IO;
        }
    }
}

static ohlc_status ssl_wait(ohlc_transport* transport, int result, uint64_t deadline) {
    int error = SSL_get_error(transport->ssl, result);
    if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
        return ohlc_net_wait(transport->fd, error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT,
                             deadline);
    }
    return OHLC_IO;
}

ohlc_status ohlc_net_handshake(ohlc_transport* transport, bool server, uint64_t deadline) {
    for (;;) {
        ERR_clear_error();
        int result = server ? SSL_accept(transport->ssl) : SSL_connect(transport->ssl);
        if (result == 1) {
            return OHLC_OK;
        }
        ohlc_status status = ssl_wait(transport, result, deadline);
        if (status != OHLC_OK) {
            return status;
        }
    }
}

ohlc_status ohlc_net_io(ohlc_transport* transport, void* buffer, size_t size, bool writing,
                        uint64_t deadline) {
    uint8_t* bytes = buffer;
    while (size != 0) {
        if (ohlc_monotonic_ms() >= deadline) {
            return OHLC_CANCELLED;
        }
        ssize_t done;
        if (transport->ssl != NULL) {
            int part = size > INT_MAX ? INT_MAX : (int)size;
            ERR_clear_error();
            int result = writing ? SSL_write(transport->ssl, bytes, part)
                                 : SSL_read(transport->ssl, bytes, part);
            if (result <= 0) {
                ohlc_status status = ssl_wait(transport, result, deadline);
                if (status != OHLC_OK) {
                    return status;
                }
                continue;
            }
            done = result;
        } else {
            int flags = 0;
#ifdef MSG_NOSIGNAL
            flags = MSG_NOSIGNAL;
#endif
            done = writing ? send(transport->fd, bytes, size, flags)
                           : recv(transport->fd, bytes, size, 0);
            if (done < 0 && errno == EINTR) {
                continue;
            }
            if (done < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                ohlc_status status =
                    ohlc_net_wait(transport->fd, writing ? POLLOUT : POLLIN, deadline);
                if (status != OHLC_OK) {
                    return status;
                }
                continue;
            }
            if (done <= 0) {
                return OHLC_IO;
            }
        }
        size -= (size_t)done;
        bytes += done;
    }
    return OHLC_OK;
}

ohlc_status ohlc_net_send(ohlc_transport* transport, const ohlc_frame* frame, const void* body,
                          uint64_t deadline) {
    uint8_t header[OHLC_NET_HEADER] = {0};
    memcpy(header, "OHLC", 4);
    ohlc_put_u16(header + 4, OHLC_PROTOCOL_VERSION);
    ohlc_put_u16(header + 6, frame->opcode);
    ohlc_put_u32(header + 8, frame->flags);
    ohlc_put_u32(header + 12, frame->size);
    ohlc_put_u64(header + 16, frame->request);
    ohlc_put_u32(header + 24, frame->status);
    ohlc_put_u32(header + 28, frame->chunk);
    ohlc_status status = ohlc_net_io(transport, header, sizeof(header), true, deadline);
    if (status == OHLC_OK) {
        status = ohlc_net_io(transport, (void*)body, frame->size, true, deadline);
    }
    return status;
}

ohlc_status ohlc_net_header(ohlc_transport* transport, ohlc_frame* frame, uint64_t deadline) {
    uint8_t header[OHLC_NET_HEADER];
    ohlc_status status = ohlc_net_io(transport, header, sizeof(header), false, deadline);
    if (status != OHLC_OK) {
        return status;
    }
    if (memcmp(header, "OHLC", 4) != 0 || ohlc_get_u16(header + 4) != OHLC_PROTOCOL_VERSION) {
        return OHLC_UNSUPPORTED;
    }
    frame->opcode = ohlc_get_u16(header + 6);
    frame->flags = ohlc_get_u32(header + 8);
    frame->size = ohlc_get_u32(header + 12);
    frame->request = ohlc_get_u64(header + 16);
    frame->status = ohlc_get_u32(header + 24);
    frame->chunk = ohlc_get_u32(header + 28);
    if (frame->size > OHLC_FRAME_LIMIT || frame->request == 0 || frame->flags > 3 ||
        frame->status > OHLC_ALREADY_EXISTS) {
        return OHLC_INVALID;
    }
    return OHLC_OK;
}

ohlc_status ohlc_net_token_file(const char* path, uint8_t output[4096], size_t* size) {
    *size = 0;
    if (path == NULL) {
        return OHLC_OK;
    }
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return OHLC_IO;
    }
    struct stat info;
    ohlc_status status = OHLC_INVALID;
    if (fstat(fd, &info) == 0 && S_ISREG(info.st_mode) && info.st_size > 0 &&
        info.st_size <= 4096 && (info.st_mode & 0077) == 0 && info.st_uid == geteuid()) {
        *size = (size_t)info.st_size;
        status = ohlc_read_full(fd, output, *size, 0);
        while (*size > 0 && (output[*size - 1] == '\n' || output[*size - 1] == '\r')) {
            (*size)--;
        }
        if (*size == 0) {
            status = OHLC_INVALID;
        }
    }
    close(fd);
    return status;
}

size_t ohlc_net_table_encode(uint8_t* output, const ohlc_table_info* info) {
    ohlc_put_u32(output, info->id);
    ohlc_put_u64(output + 4, info->created_seq);
    return 12u + ohlc_definition_encode(output + 12, info);
}

ohlc_status ohlc_net_table_decode(const uint8_t* data, size_t size, ohlc_table_info* info,
                                  size_t* consumed) {
    if (size < 12) {
        return OHLC_CORRUPT;
    }
    size_t position = 12;
    for (unsigned int i = 0; i < 3; i++) {
        if (size - position < 4) {
            return OHLC_CORRUPT;
        }
        uint32_t length = ohlc_get_u32(data + position);
        position += 4;
        if (length > size - position) {
            return OHLC_CORRUPT;
        }
        position += length;
        if (i == 0) {
            if (size - position < 8) {
                return OHLC_CORRUPT;
            }
            position += 8;
        }
    }
    info->id = ohlc_get_u32(data);
    info->created_seq = ohlc_get_u64(data + 4);
    ohlc_status status = ohlc_definition_decode(data + 12, position - 12, info);
    if (status == OHLC_OK && (info->id == 0 || info->created_seq == 0)) {
        status = OHLC_CORRUPT;
    }
    if (status == OHLC_OK) {
        *consumed = position;
    }
    return status;
}

// SPDX-License-Identifier: GPL-2.0-only
/**
 * Copyright (c) 2025 Syswonder
 *
 * Syswonder Website:
 *      https://www.syswonder.org
 *
 * Authors:
 *      Guowei Li <2401213322@stu.pku.edu.cn>
 */
#define _GNU_SOURCE

#include "virtio_console.h"
#include "log.h"
#include "virtio.h"
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <termios.h>
#include <stdint.h>
#include <string.h>
#include <sys/epoll.h>

static ConsoleDev *init_console_dev() {
    ConsoleDev *dev = (ConsoleDev *)malloc(sizeof(ConsoleDev));
    dev->config.cols = 80;
    dev->config.rows = 25;
    dev->master_fd = -1;
    dev->slave_keepalive_fd = -1;
    dev->rx_ready = -1;
    dev->event = NULL;
    pthread_mutex_init(&dev->rx_lock, NULL);
    pthread_mutex_init(&dev->tx_lock, NULL);
    pthread_mutex_init(&dev->event_lock, NULL);
    dev->tx_head = NULL;
    dev->tx_tail = NULL;
    dev->tx_pending = false;
    return dev;
}

static int virtio_console_rearm_event(ConsoleDev *dev) {
    int events = EPOLLIN | EPOLLONESHOT;
    if (__atomic_load_n(&dev->tx_pending, __ATOMIC_ACQUIRE))
        events |= EPOLLOUT;
    pthread_mutex_lock(&dev->event_lock);
    int ret = update_event(dev->event, events);
    pthread_mutex_unlock(&dev->event_lock);
    return ret;
}

static int virtio_console_update_rx_event(ConsoleDev *dev, VirtQueue *vq) {
    if (!virtqueue_is_empty(vq)) {
        virtqueue_disable_notify(vq);
        return virtio_console_rearm_event(dev);
    }

    // Hand wakeup ownership back to the guest while no RX buffer is available.
    // Re-check after publishing the notification change so a descriptor posted
    // while notifications were suppressed cannot be missed.
    virtqueue_enable_notify(vq);
    rw_barrier();
    if (virtqueue_is_empty(vq)) {
        return 0;
    }

    virtqueue_disable_notify(vq);
    return virtio_console_rearm_event(dev);
}

static bool virtio_console_queue_tx(ConsoleDev *dev, VirtQueue *vq, uint16_t idx,
                                    uint8_t *data, size_t len, size_t off) {
    ConsoleTxPending *pending = calloc(1, sizeof(*pending));
    if (!pending) {
        log_error("failed to allocate console TX pending buffer");
        update_used_ring(vq, idx, 0);
        free(data);
        return false;
    }
    pending->idx = idx;
    pending->data = data;
    pending->len = len;
    pending->off = off;
    if (dev->tx_tail)
        dev->tx_tail->next = pending;
    else
        dev->tx_head = pending;
    dev->tx_tail = pending;
    __atomic_store_n(&dev->tx_pending, true, __ATOMIC_RELEASE);
    return true;
}

static bool virtio_console_flush_tx(ConsoleDev *dev, VirtQueue *vq) {
    bool used_buffer = false;
    while (dev->tx_head) {
        ConsoleTxPending *pending = dev->tx_head;
        while (pending->off < pending->len) {
            ssize_t written = write(dev->master_fd, pending->data + pending->off,
                                    pending->len - pending->off);
            if (written > 0) {
                pending->off += (size_t)written;
                continue;
            }
            if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                __atomic_store_n(&dev->tx_pending, true, __ATOMIC_RELEASE);
                return used_buffer;
            }
            log_error("failed to write console TX data, errno is %d", errno);
            pending->off = pending->len;
            break;
        }

        update_used_ring(vq, pending->idx, (uint32_t)pending->len);
        used_buffer = true;
        dev->tx_head = pending->next;
        if (!dev->tx_head)
            dev->tx_tail = NULL;
        free(pending->data);
        free(pending);
    }
    __atomic_store_n(&dev->tx_pending, false, __ATOMIC_RELEASE);
    return used_buffer;
}

static void virtio_console_event_handler(int fd, int epoll_type, void *param) {
    // log_debug("%s", __func__);
    VirtIODevice *vdev = (VirtIODevice *)param;
    ConsoleDev *dev = (ConsoleDev *)vdev->dev;
    VirtQueue *vq = &vdev->vqs[CONSOLE_QUEUE_RX];
    int n;
    ssize_t len;
    struct iovec *iov = NULL;
    uint16_t idx;
    if (fd != dev->master_fd || !(epoll_type & (EPOLLIN | EPOLLOUT))) {
        log_error("Invalid console event");
        return;
    }
    if (dev->master_fd <= 0 || vdev->type != VirtioTConsole) {
        log_error("console event handler should not be called");
        return;
    }

    bool tx_used_buffer = false;
    if ((epoll_type & EPOLLOUT) ||
        __atomic_load_n(&dev->tx_pending, __ATOMIC_ACQUIRE)) {
        pthread_mutex_lock(&dev->tx_lock);
        tx_used_buffer =
            virtio_console_flush_tx(dev, &vdev->vqs[CONSOLE_QUEUE_TX]);
        pthread_mutex_unlock(&dev->tx_lock);
        if (tx_used_buffer)
            virtio_inject_irq(&vdev->vqs[CONSOLE_QUEUE_TX]);
    }

    bool rx_used_buffer = false;
    pthread_mutex_lock(&dev->rx_lock);
    if (dev->rx_ready <= 0) {
        log_debug(
            "console RX paused until the guest enables its receive queue");
        virtio_console_rearm_event(dev);
        pthread_mutex_unlock(&dev->rx_lock);
        return;
    }
    if (vq->used_ring == NULL || vq->avail_ring == NULL) {
        log_debug(
            "console RX paused until the guest configures its receive queue");
        virtio_console_rearm_event(dev);
        pthread_mutex_unlock(&dev->rx_lock);
        return;
    }
    if (virtqueue_is_empty(vq)) {
        log_debug("console RX paused until the guest posts a receive buffer");
        if (virtio_console_update_rx_event(dev, vq) < 0) {
            log_error("failed to update console RX event");
        }
        pthread_mutex_unlock(&dev->rx_lock);
        return;
    }

    while (!virtqueue_is_empty(vq)) {
        n = process_descriptor_chain(vq, &idx, &iov, NULL, 0, false);
        if (n < 1) {
            log_error("process_descriptor_chain failed");
            break;
        }
        len = readv(dev->master_fd, iov, n);
        if (len < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            log_debug("no more bytes");
            vq->last_avail_idx--;
            free(iov);
            break;
        } else if (len < 0) {
            log_debug("Failed to read from console, errno is %d", errno);
            vq->last_avail_idx--;
            free(iov);
            break;
        }
        update_used_ring(vq, idx, len);
        rx_used_buffer = true;
        free(iov);
    }

    if (virtio_console_update_rx_event(dev, vq) < 0) {
        log_error("failed to update console RX event");
    }
    pthread_mutex_unlock(&dev->rx_lock);

    if (rx_used_buffer) {
        virtio_inject_irq(vq);
    }
    return;
}

static int virtio_console_init(VirtIODevice *vdev) {
    ConsoleDev *dev = (ConsoleDev *)vdev->dev;
    int master_fd, slave_fd;
    char *slave_name;
    struct termios term_io;

    master_fd = posix_openpt(O_RDWR | O_NOCTTY);
    if (master_fd < 0) {
        log_error("Failed to open master pty, errno is %d", errno);
    }
    if (grantpt(master_fd) < 0) {
        log_error("Failed to grant pty, errno is %d", errno);
    }
    if (unlockpt(master_fd) < 0) {
        log_error("Failed to unlock pty, errno is %d", errno);
    }
    dev->master_fd = master_fd;

    slave_name = ptsname(master_fd);
    if (slave_name == NULL) {
        log_error("Failed to get slave name, errno is %d", errno);
    }
    log_info("char device redirected to %s", slave_name);
    // Open and keep a slave fd in this process. Without a slave peer,
    // master poll/epoll keeps reporting EPOLLHUP and can cause busy loops.
    slave_fd = open(slave_name, O_RDWR);
    if (slave_fd < 0) {
        log_error("Failed to open slave pty, errno is %d", errno);
        return -1;
    }

    // Disable line discipline to prevent the TTY
    // from echoing the characters sent from the master back to the master.
    tcgetattr(slave_fd, &term_io);
    cfmakeraw(&term_io);
    term_io.c_oflag |= ONLCR;
    tcsetattr(slave_fd, TCSAFLUSH, &term_io);
    dev->slave_keepalive_fd = slave_fd;

    if (set_nonblocking(dev->master_fd) < 0) {
        log_error("Failed to set nonblocking mode");
        return -1;
    }

    dev->event = add_event(dev->master_fd, EPOLLIN | EPOLLONESHOT,
                           virtio_console_event_handler, vdev);

    if (dev->event == NULL) {
        log_error("Can't register console event");
        return -1;
    }

    return 0;
}

static int virtio_console_rxq_notify_handler(VirtIODevice *vdev,
                                             VirtQueue *vq) {
    log_debug("%s", __func__);
    ConsoleDev *dev = (ConsoleDev *)vdev->dev;

    pthread_mutex_lock(&dev->rx_lock);
    if (dev->rx_ready <= 0) {
        dev->rx_ready = 1;
    }
    if (virtio_console_update_rx_event(dev, vq) < 0) {
        log_error("failed to update console RX event");
    }
    pthread_mutex_unlock(&dev->rx_lock);
    return 0;
}

static bool virtq_tx_handle_one_request(ConsoleDev *dev, VirtQueue *vq) {
    int n;
    uint16_t idx;
    ssize_t len;
    struct iovec *iov = NULL;
    if (dev->master_fd <= 0) {
        log_error("Console master fd is not ready");
        return false;
    }

    n = process_descriptor_chain(vq, &idx, &iov, NULL, 0, false);

    if (n < 1) {
        return false;
    }

    size_t total = 0;
    for (int i = 0; i < n; i++)
        total += iov[i].iov_len;
    uint8_t *data = malloc(total ? total : 1);
    if (!data) {
        free(iov);
        return false;
    }
    size_t copied = 0;
    for (int i = 0; i < n; i++) {
        memcpy(data + copied, iov[i].iov_base, iov[i].iov_len);
        copied += iov[i].iov_len;
    }
    size_t off = 0;
    while (off < total) {
        len = write(dev->master_fd, data + off, total - off);
        if (len > 0) {
            off += (size_t)len;
            continue;
        }
        if (len < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!virtio_console_queue_tx(dev, vq, idx, data, total, off))
                return false;
            free(iov);
            return true;
        }
        log_error("Failed to write to console, errno is %d", errno);
        off = total;
    }
    update_used_ring(vq, idx, (uint32_t)total);
    free(data);
    free(iov);
    return false;
}

static int virtio_console_txq_notify_handler(VirtIODevice *vdev,
                                             VirtQueue *vq) {
    log_debug("%s", __func__);
    bool used_buffer = false;
    ConsoleDev *dev = (ConsoleDev *)vdev->dev;
    pthread_mutex_lock(&dev->tx_lock);
    used_buffer |= virtio_console_flush_tx(dev, vq);
    while (!dev->tx_pending && !virtqueue_is_empty(vq)) {
        virtqueue_disable_notify(vq);
        while (!virtqueue_is_empty(vq)) {
            if (virtq_tx_handle_one_request(dev, vq))
                break;
            used_buffer = true;
        }
        virtqueue_enable_notify(vq);
    }
    if (virtio_console_rearm_event(dev) < 0)
        log_error("failed to rearm console TX event");
    pthread_mutex_unlock(&dev->tx_lock);
    if (used_buffer)
        virtio_inject_irq(vq);
    return 0;
}

static void virtio_console_reset(VirtIODevice *vdev) { (void)vdev; }

static void virtio_console_close(VirtIODevice *vdev) {
    if (!vdev)
        return;

    ConsoleDev *dev = vdev->dev;
    if (dev) {
        ConsoleTxPending *pending = dev->tx_head;
        while (pending) {
            ConsoleTxPending *next = pending->next;
            free(pending->data);
            free(pending);
            pending = next;
        }
        if (dev->master_fd >= 0)
            close(dev->master_fd);
        if (dev->slave_keepalive_fd >= 0)
            close(dev->slave_keepalive_fd);
        remove_event(dev->event);
        free(dev->event);
        pthread_mutex_destroy(&dev->rx_lock);
        pthread_mutex_destroy(&dev->tx_lock);
        pthread_mutex_destroy(&dev->event_lock);
        free(dev);
        vdev->dev = NULL;
    }
    free(vdev->vqs);
    vdev->vqs = NULL;
    free(vdev);
}
static int virtio_console_do_init(VirtIODevice *vdev, const void *params) {
    (void)params;
    vdev->dev = init_console_dev();
    if (!vdev->dev)
        return -ENOMEM;
    return virtio_console_init(vdev);
}

const struct virtio_device_ops virtio_console_ops = {
    .type = VirtioTConsole,
    .features = CONSOLE_SUPPORTED_FEATURES,
    .num_queues = CONSOLE_MAX_QUEUES,
    .queue_max_size = VIRTQUEUE_CONSOLE_MAX_SIZE,
    .init = virtio_console_do_init,
    .close = virtio_console_close,
    .reset = virtio_console_reset,
    .notify_handlers =
        {
            [CONSOLE_QUEUE_RX] = virtio_console_rxq_notify_handler,
            [CONSOLE_QUEUE_TX] = virtio_console_txq_notify_handler,
        },
};

static int virtio_console_parse_params(const cJSON *json, void **out) {
    (void)json;
    *out = NULL;
    return 0;
}

static void virtio_console_free_params(void *params) { (void)params; }

const struct virtio_config_ops virtio_console_config_ops = {
    .parse = virtio_console_parse_params,
    .free = virtio_console_free_params,
};

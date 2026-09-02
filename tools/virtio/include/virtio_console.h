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
#ifndef _HVISOR_VIRTIO_CONSOLE_H
#define _HVISOR_VIRTIO_CONSOLE_H
#include "event_monitor.h"
#include "virtio.h"
#include <linux/virtio_console.h>

#define CONSOLE_SUPPORTED_FEATURES                                             \
    ((1ULL << VIRTIO_F_VERSION_1) | (1ULL << VIRTIO_CONSOLE_F_SIZE))
#define CONSOLE_MAX_QUEUES 2
#define VIRTQUEUE_CONSOLE_MAX_SIZE 64
#define CONSOLE_QUEUE_RX 0
#define CONSOLE_QUEUE_TX 1

typedef struct virtio_console_config ConsoleConfig;
typedef struct virtio_console_tx_pending {
    uint16_t idx;
    uint8_t *data;
    size_t len;
    size_t off;
    struct virtio_console_tx_pending *next;
} ConsoleTxPending;

typedef struct virtio_console_dev {
    ConsoleConfig config;
    int master_fd;
    int slave_keepalive_fd;
    int rx_ready;
    struct hvisor_event *event;
    pthread_mutex_t rx_lock;
    pthread_mutex_t tx_lock;
    pthread_mutex_t event_lock;
    ConsoleTxPending *tx_head;
    ConsoleTxPending *tx_tail;
    bool tx_pending;
} ConsoleDev;

extern const struct virtio_device_ops virtio_console_ops;
extern const struct virtio_config_ops virtio_console_config_ops;

#endif

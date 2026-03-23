// SPDX-License-Identifier: GPL-2.0-only
/**
 * Copyright (c) 2025 Syswonder
 *
 * Syswonder Website:
 *      https://www.syswonder.org
 *
 * Authors:
 *      Guowei Li <2401213322@stu.pku.edu.cn>
 */
#include "virtio_blk.h"
#include "log.h"
#include "virtio.h"
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/stat.h>

/* ---- Memory pool for blkp_req ---- */

static int blk_pool_init(BlkDev *dev) {
    int i;
    dev->pool_storage = malloc(sizeof(struct blkp_req) * VIRTQUEUE_BLK_MAX_SIZE);
    if (!dev->pool_storage)
        return -ENOMEM;
    SLIST_INIT(&dev->pool_free);
    for (i = 0; i < VIRTQUEUE_BLK_MAX_SIZE; i++)
        SLIST_INSERT_HEAD(&dev->pool_free, &dev->pool_storage[i], free_link);
    return 0;
}

static void blk_pool_destroy(BlkDev *dev) {
    free(dev->pool_storage);
    dev->pool_storage = NULL;
}

static struct blkp_req *blk_pool_alloc(BlkDev *dev) {
    struct blkp_req *req;
    pthread_mutex_lock(&dev->mtx);
    req = SLIST_FIRST(&dev->pool_free);
    if (req)
        SLIST_REMOVE_HEAD(&dev->pool_free, free_link);
    pthread_mutex_unlock(&dev->mtx);
    if (!req) {
        log_warn("blk pool exhausted, falling back to malloc");
        req = malloc(sizeof(struct blkp_req));
    }
    return req;
}

static void blk_pool_free(BlkDev *dev, struct blkp_req *req) {
    if (req >= dev->pool_storage &&
        req < dev->pool_storage + VIRTQUEUE_BLK_MAX_SIZE) {
        pthread_mutex_lock(&dev->mtx);
        SLIST_INSERT_HEAD(&dev->pool_free, req, free_link);
        pthread_mutex_unlock(&dev->mtx);
    } else {
        free(req);
    }
}

/* ---- Completion helper ---- */

static void complete_block_operation(BlkDev *dev, struct blkp_req *req,
                                     VirtQueue *vq, int err,
                                     ssize_t written_len) {
    uint8_t *vstatus = (uint8_t *)(req->iov[req->iovcnt - 1].iov_base);
    int is_empty = 0;
    if (err == EOPNOTSUPP)
        *vstatus = VIRTIO_BLK_S_UNSUPP;
    else if (err != 0)
        *vstatus = VIRTIO_BLK_S_IOERR;
    else
        *vstatus = VIRTIO_BLK_S_OK;
    if (err != 0) {
        log_error("virt blk err, num is %d", err);
    }
    update_used_ring(vq, req->idx, written_len + 1);
    pthread_mutex_lock(&dev->mtx);
    is_empty = TAILQ_EMPTY(&dev->procq);
    pthread_mutex_unlock(&dev->mtx);
    if (is_empty)
        virtio_inject_irq(vq);
    free(req->iov);
    blk_pool_free(dev, req);
}

// get a blk req from procq
static int get_breq(BlkDev *dev, struct blkp_req **req) {
    struct blkp_req *elem;
    elem = TAILQ_FIRST(&dev->procq);
    if (elem == NULL) {
        return 0;
    }
    TAILQ_REMOVE(&dev->procq, elem, link);
    *req = elem;
    return 1;
}

static void blkproc(BlkDev *dev, struct blkp_req *req, VirtQueue *vq) {
    struct iovec *iov = req->iov;
    int n = req->iovcnt, err = 0;
    ssize_t len, written_len = 0;

    switch (req->type) {
    case VIRTIO_BLK_T_IN:
        written_len = len = preadv(dev->img_fd, &iov[1], n - 2, req->offset);
        log_debug("preadv, len is %d, offset is %d", len, req->offset);
        if (len < 0) {
            log_error("pread failed");
            err = errno;
        }
        break;
    case VIRTIO_BLK_T_OUT:
        len = pwritev(dev->img_fd, &iov[1], n - 2, req->offset);
        log_debug("pwritev, len is %d, offset is %d", len, req->offset);
        if (len < 0) {
            log_error("pwrite failed");
            err = errno;
        }
        break;
    case VIRTIO_BLK_T_GET_ID: {
        char s[20] = "hvisor-virblk";
        strncpy(iov[1].iov_base, s, MIN(sizeof(s), iov[1].iov_len));
        break;
    }
    default:
        log_fatal("Operation is not supported");
        err = EOPNOTSUPP;
        break;
    }
    complete_block_operation(dev, req, vq, err, written_len);
}

#ifdef ENABLE_URING

/* ---- io_uring completion reaping ---- */

static void blk_reap_completions(BlkDev *dev, VirtQueue *vq) {
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;

    io_uring_for_each_cqe(&dev->ring, head, cqe) {
        struct blkp_req *req = (struct blkp_req *)io_uring_cqe_get_data(cqe);
        uint8_t *vstatus = (uint8_t *)(req->iov[req->iovcnt - 1].iov_base);
        ssize_t written_len = 0;

        if (cqe->res < 0) {
            log_error("io_uring cqe error: %s", strerror(-cqe->res));
            *vstatus = VIRTIO_BLK_S_IOERR;
        } else {
            *vstatus = VIRTIO_BLK_S_OK;
            if (req->type == VIRTIO_BLK_T_IN)
                written_len = cqe->res;
        }

        update_used_ring(vq, req->idx, written_len + 1);
        free(req->iov);
        blk_pool_free(dev, req);
        count++;
    }

    io_uring_cq_advance(&dev->ring, count);

    if (count > 0)
        virtio_inject_irq(vq);
}

static void blk_submit_uring(BlkDev *dev, struct blkp_req *req, VirtQueue *vq) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&dev->ring);
    if (!sqe) {
        /* SQ full: submit pending and reap to free space, then retry */
        io_uring_submit(&dev->ring);
        struct io_uring_cqe *cqe;
        io_uring_wait_cqe(&dev->ring, &cqe);
        blk_reap_completions(dev, vq);
        sqe = io_uring_get_sqe(&dev->ring);
        if (!sqe) {
            log_error("io_uring SQ still full after reap, falling back to sync");
            blkproc(dev, req, vq);
            return;
        }
    }

    if (req->type == VIRTIO_BLK_T_IN) {
        io_uring_prep_readv(sqe, dev->img_fd, &req->iov[1],
                            req->iovcnt - 2, req->offset);
    } else {
        io_uring_prep_writev(sqe, dev->img_fd, &req->iov[1],
                             req->iovcnt - 2, req->offset);
    }
    io_uring_sqe_set_data(sqe, req);
}

// io_uring worker thread
static void *blkproc_thread(void *arg) {
    VirtIODevice *vdev = arg;
    BlkDev *dev = vdev->dev;
    struct blkp_req *breq;
    int submitted;

    pthread_mutex_lock(&dev->mtx);

    for (;;) {
        submitted = 0;

        while (get_breq(dev, &breq)) {
            pthread_mutex_unlock(&dev->mtx);

            /* Non-I/O requests: handle synchronously */
            if (breq->type != VIRTIO_BLK_T_IN &&
                breq->type != VIRTIO_BLK_T_OUT) {
                blkproc(dev, breq, vdev->vqs);
                pthread_mutex_lock(&dev->mtx);
                continue;
            }

            blk_submit_uring(dev, breq, vdev->vqs);
            submitted++;
            pthread_mutex_lock(&dev->mtx);
        }

        /* Submit any queued SQEs */
        if (submitted > 0) {
            pthread_mutex_unlock(&dev->mtx);
            io_uring_submit(&dev->ring);
            /* Wait for at least one completion, then reap all */
            struct io_uring_cqe *cqe;
            io_uring_wait_cqe(&dev->ring, &cqe);
            blk_reap_completions(dev, vdev->vqs);
            pthread_mutex_lock(&dev->mtx);
            /* Loop back to check for new arrivals */
            continue;
        }

        if (dev->close) {
            pthread_mutex_unlock(&dev->mtx);
            break;
        }
        pthread_cond_wait(&dev->cond, &dev->mtx);
    }
    pthread_exit(NULL);
    return NULL;
}

#else /* !ENABLE_URING */

// Synchronous worker thread (original path with pool)
static void *blkproc_thread(void *arg) {
    VirtIODevice *vdev = arg;
    BlkDev *dev = vdev->dev;
    struct blkp_req *breq;
    // get_breq will access the critical section, so lock it.
    pthread_mutex_lock(&dev->mtx);

    for (;;) {
        while (get_breq(dev, &breq)) {
            // blk_proc don't access the critical section, so unlock.
            pthread_mutex_unlock(&dev->mtx);
            blkproc(dev, breq, vdev->vqs);
            pthread_mutex_lock(&dev->mtx);
        }

        if (dev->close) {
            pthread_mutex_unlock(&dev->mtx);
            break;
        }
        pthread_cond_wait(&dev->cond, &dev->mtx);
    }
    pthread_exit(NULL);
    return NULL;
}

#endif /* ENABLE_URING */

// create blk dev.
BlkDev *init_blk_dev(VirtIODevice *vdev) {
    BlkDev *dev = malloc(sizeof(BlkDev));
    vdev->dev = dev;
    dev->config.capacity = -1;
    dev->config.size_max = -1;
    dev->config.seg_max = BLK_SEG_MAX;
    dev->img_fd = -1;
    dev->close = 0;
    pthread_mutex_init(&dev->mtx, NULL);
    pthread_cond_init(&dev->cond, NULL);
    TAILQ_INIT(&dev->procq);
    if (blk_pool_init(dev) != 0) {
        log_error("failed to init blk pool");
        free(dev);
        return NULL;
    }
#ifdef ENABLE_URING
    if (io_uring_queue_init(VIRTQUEUE_BLK_MAX_SIZE, &dev->ring, 0) < 0) {
        log_error("failed to init io_uring");
        blk_pool_destroy(dev);
        free(dev);
        return NULL;
    }
#endif
    pthread_create(&dev->tid, NULL, blkproc_thread, vdev);
    return dev;
}

int virtio_blk_init(VirtIODevice *vdev, const char *img_path) {
    int img_fd = open(img_path, O_RDWR);
    BlkDev *dev = vdev->dev;
    struct stat st;
    uint64_t blk_size;
    if (img_fd == -1) {
        log_error("cannot open %s, Error code is %d", img_path, errno);
        close(img_fd);
        return -1;
    }
    if (fstat(img_fd, &st) == -1) {
        log_error("cannot stat %s, Error code is %d", img_path, errno);
        close(img_fd);
        return -1;
    }
    blk_size = st.st_size / 512; // 512 bytes per block
    dev->config.capacity = blk_size;
    dev->config.size_max = blk_size;
    dev->img_fd = img_fd;
    vdev->virtio_close = virtio_blk_close;
    log_info("debug: virtio_blk_init: %s, size is %lld", img_path,
             dev->config.capacity);
    return 0;
}

// handle one descriptor list
static struct blkp_req *virtq_blk_handle_one_request(BlkDev *dev,
                                                      VirtQueue *vq) {
    log_debug("virtq_blk_handle_one_request enter");
    struct blkp_req *breq;
    struct iovec *iov = NULL;
    uint16_t *flags;
    int i, n;
    BlkReqHead *hdr;
    breq = blk_pool_alloc(dev);
    if (!breq)
        return NULL;
    n = process_descriptor_chain(vq, &breq->idx, &iov, &flags, 0, true);
    breq->iov = iov;
    if (n < 2 || n > BLK_SEG_MAX + 2) {
        log_error("iov's num is wrong, n is %d", n);
        goto err_out;
    }

    if ((flags[0] & VRING_DESC_F_WRITE) != 0) {
        log_error("virt queue's desc chain header should not be writable!");
        goto err_out;
    }

    if (iov[0].iov_len != sizeof(BlkReqHead)) {
        log_error("the size of blk header is %d, it should be %d!",
                  iov[0].iov_len, sizeof(BlkReqHead));
        goto err_out;
    }

    if (iov[n - 1].iov_len != 1 || ((flags[n - 1] & VRING_DESC_F_WRITE) == 0)) {
        log_error(
            "status iov is invalid!, status len is %d, flag is %d, n is %d",
            iov[n - 1].iov_len, flags[n - 1], n);
        goto err_out;
    }

    hdr = (BlkReqHead *)(iov[0].iov_base);
    uint64_t offset = hdr->sector * SECTOR_BSIZE;
    breq->type = hdr->type;
    breq->iovcnt = n;
    breq->offset = offset;

    for (i = 1; i < n - 1; i++)
        if (((flags[i] & VRING_DESC_F_WRITE) == 0) !=
            (breq->type == VIRTIO_BLK_T_OUT)) {
            log_error("flag is conflict with operation");
            goto err_out;
        }

    free(flags);
    return breq;

err_out:
    free(flags);
    free(iov);
    blk_pool_free(dev, breq);
    return NULL;
}

int virtio_blk_notify_handler(VirtIODevice *vdev, VirtQueue *vq) {
    log_debug("virtio blk notify handler enter");
    BlkDev *blkDev = (BlkDev *)vdev->dev;
    struct blkp_req *breq;
    TAILQ_HEAD(, blkp_req) procq;
    TAILQ_INIT(&procq);
    while (!virtqueue_is_empty(vq)) {
        virtqueue_disable_notify(vq);
        while (!virtqueue_is_empty(vq)) {
            breq = virtq_blk_handle_one_request(blkDev, vq);
            if (breq)
                TAILQ_INSERT_TAIL(&procq, breq, link);
        }
        virtqueue_enable_notify(vq);
    }
    if (TAILQ_EMPTY(&procq)) {
        log_debug("virtio blk notify handler exit, procq is empty");
        return 0;
    }
    pthread_mutex_lock(&blkDev->mtx);
    TAILQ_CONCAT(&blkDev->procq, &procq, link);
    pthread_cond_signal(&blkDev->cond);
    pthread_mutex_unlock(&blkDev->mtx);
    return 0;
}

void virtio_blk_close(VirtIODevice *vdev) {
    BlkDev *dev = vdev->dev;
    pthread_mutex_lock(&dev->mtx);
    dev->close = 1;
    pthread_cond_signal(&dev->cond);
    pthread_mutex_unlock(&dev->mtx);
    pthread_join(dev->tid, NULL);
    pthread_mutex_destroy(&dev->mtx);
    pthread_cond_destroy(&dev->cond);
    close(dev->img_fd);
#ifdef ENABLE_URING
    io_uring_queue_exit(&dev->ring);
#endif
    blk_pool_destroy(dev);
    free(dev);
    free(vdev->vqs);
    free(vdev);
}

/*
 * queue.c -- 작업 큐 (7조)
 */
#include "queue.h"
#include "timing.h"

#include <stdlib.h>

int wq_init(work_queue_t *q, size_t cap)
{
    if (cap == 0) return -1;
    q->buf = calloc(cap, sizeof(wq_item_t));
    if (!q->buf) return -1;
    q->cap = cap;
    q->head = q->count = 0;
    q->closed = 0;
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
    return 0;
}

void wq_destroy(work_queue_t *q)
{
    while (q->count > 0) {
        free(q->buf[q->head].c.data);
        q->head = (q->head + 1) % q->cap;
        q->count--;
    }
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mu);
    free(q->buf);
    q->buf = NULL;
}

int wq_push(work_queue_t *q, const chunk_t *c)
{
    wq_item_t *it;

    pthread_mutex_lock(&q->mu);
    while (q->count == q->cap && !q->closed)
        pthread_cond_wait(&q->not_full, &q->mu);
    if (q->closed) {
        pthread_mutex_unlock(&q->mu);
        return -1;
    }
    it = &q->buf[(q->head + q->count) % q->cap];
    it->c = *c;
    it->t_enq = now_ns();          /* 실제로 큐에 들어간 시각 (가득 차서 기다린 시간은 뺌) */
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
    return 0;
}

int wq_pop(work_queue_t *q, chunk_t *out, uint64_t *wait_ns)
{
    wq_item_t *it;

    pthread_mutex_lock(&q->mu);
    while (q->count == 0 && !q->closed)
        pthread_cond_wait(&q->not_empty, &q->mu);
    if (q->count == 0) {
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    it = &q->buf[q->head];
    *out = it->c;
    if (wait_ns)
        *wait_ns = now_ns() - it->t_enq;
    q->head = (q->head + 1) % q->cap;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mu);
    return 1;
}

void wq_close(work_queue_t *q)
{
    pthread_mutex_lock(&q->mu);
    q->closed = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mu);
}

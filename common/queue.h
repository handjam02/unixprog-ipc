/*
 * queue.h -- 작업 큐 (7조, 기획서 6.3절)
 *
 * 유한 크기 원형 큐. mutex 1개 + 조건 변수 2개(not_full, not_empty).
 * 서버: 수신 스레드(생산자) -> 워커들(소비자). 클라이언트: 생성 스레드 <-> 송신 스레드.
 * 가득 차면 넣는 쪽이 기다린다(역압).
 *
 * 넣은 시각을 항목마다 같이 두어서 꺼낼 때 큐 대기 시간을 돌려준다(기획서 7.1절 "큐 대기").
 * 락을 잡은 채로 IPC 송수신이나 다른 락을 기다리지 않는다(6.3절 교착 규칙).
 */
#ifndef APP_QUEUE_H
#define APP_QUEUE_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include "ipc.h"

typedef struct {
    chunk_t  c;
    uint64_t t_enq;
} wq_item_t;

typedef struct {
    wq_item_t      *buf;
    size_t          cap, head, count;
    int             closed;
    pthread_mutex_t mu;
    pthread_cond_t  not_empty, not_full;
} work_queue_t;

int  wq_init(work_queue_t *q, size_t cap);    /* 0 성공, -1 실패 */
void wq_destroy(work_queue_t *q);             /* 남은 항목의 data는 free */

/* 0 = 넣음, -1 = 큐가 닫힘(c->data는 부른 쪽이 free) */
int  wq_push(work_queue_t *q, const chunk_t *c);

/* 1 = 꺼냄, 0 = 닫혔고 비었음. wait_ns(NULL 가능)에 큐에 머문 시간 */
int  wq_pop(work_queue_t *q, chunk_t *out, uint64_t *wait_ns);

/* 더 넣지 않는다. 남은 항목을 다 꺼낸 뒤 wq_pop이 0을 돌려준다 */
void wq_close(work_queue_t *q);

#endif

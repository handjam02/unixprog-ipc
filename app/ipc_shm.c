/*
 * ipc_shm.c -- ipc.h의 System V shared memory 구현 (7조, 기획서 6.3~6.4절, 8.3~8.4절)
 *
 * 자원 (8.3절)
 *   shm key 60070  세그먼트 1개 = 머리(슬롯 표) + 슬롯 8개. 슬롯마다
 *                  요청 링 16칸(APP_SHM_RING)과 응답 링 4칸(APP_SHM_RSP_RING).
 *                  한 칸은 헤더 24바이트 + 최대 청크 256KB.
 *                  크기: (16 + 4) x 256KB x 8 = 약 40MB (예측 P5의 32MB에 응답 링 8MB가 더해짐)
 *   sem key 60073  System V 세마포어 집합 1개 (33개)
 *                    슬롯마다 REQ_EMPTY(16), REQ_FULL(0), RSP_EMPTY(4), RSP_FULL(0)
 *                    맨 끝에 NOTIFY(0) 1개: 클라이언트가 무엇이든 넣으면 1 올린다
 *
 * 동기화 (6.3절)
 *   슬롯 표        세그먼트 안의 프로세스 공유 mutex. 접속(클라이언트)과 해제(서버) 때만 잡는다.
 *                  PTHREAD_MUTEX_ROBUST라서 잡은 채로 클라이언트가 죽어도 다음 사람이 이어 쓴다.
 *   요청 링        클라이언트 송신 스레드 1개 -> 서버 수신 스레드 1개. 1단계 벤치마크처럼
 *                  EMPTY/FULL 카운팅 세마포어로 칸을 주고받는다. 읽기/쓰기 위치는 각자 따로 센다.
 *   응답 링        서버 워커 여러 개 -> 클라이언트 수신 스레드 1개. 워커끼리는 슬롯별
 *                  mutex(wmu)로 줄을 세운다(pipe 버전의 응답 mutex와 같은 역할).
 *
 * 서버 수신 (6.2절): 수신 스레드가 NOTIFY 하나를 기다린 뒤 rr 다음 슬롯부터 링을 훑고,
 * 찬 칸을 하나 찾으면 큐 버퍼로 복사(기본판, 6.4절)하고 칸을 돌려준 뒤 반환한다.
 * 클라이언트는 REQ_FULL과 NOTIFY를 semop 한 번으로 함께 올린다.
 *
 * 접속: 클라이언트가 슬롯 표 mutex를 잡고 빈 슬롯을 USED로 바꾼 뒤 NOTIFY를 올린다.
 * 서버 수신 스레드는 훑을 때 슬롯 표의 세대(gen)가 바뀐 것을 보고 받아들인다.
 * 종료: 클라이언트는 요청 링에 BYE를 넣는다. 서버는 그 슬롯을 비운다.
 *
 * 상대가 kill -9로 죽었을 때 (8.4절 3번)
 *   세마포어는 상대가 죽어도 풀리지 않는다. 그래서 막힐 수 있는 대기는 모두 semtimedop
 *   (WAIT_MS)으로 하고, 시간이 지나면 상대 pid가 살아 있는지 kill(pid, 0)으로 본다.
 *   - 클라이언트: 서버가 죽었으면 client_send/client_recv가 -1 (client.c 351행 요구).
 *                서버가 정상 종료해 세마포어 집합을 지우면 EIDRM으로 바로 -1.
 *   - 서버: 수신 스레드가 WAIT_MS마다 USED 슬롯의 pid를 확인해 죽은 클라이언트 슬롯을 비운다.
 *           그 클라이언트에게 응답을 쓰려고 기다리던 워커는 closing 표시를 보고 -1을 돌려준다.
 *   SEM_UNDO는 쓰지 않는다. 카운팅 세마포어의 P와 V를 서로 다른 프로세스가 하기 때문이다
 *   (1단계 shm_bench.c와 같은 이유). 그래서 슬롯을 비울 때 서버가 세마포어 값을 처음 값으로 되돌린다.
 *
 * 설계 개선 E5: 제자리 처리 (서버 -X inplace, 기획서 6.4절, 7.2절)
 *   기본판:  클라이언트 -> 요청 칸 -> (수신 스레드 복사) 큐 버퍼 -> 워커 처리
 *            -> (워커 복사) 응답 칸 -> (클라이언트 복사) 클라이언트 버퍼.  수신 스레드가 칸을 바로 돌려줌
 *   제자리:  클라이언트 -> 요청 칸 -> 워커가 칸에서 바로 처리, 정규화 결과도 그 칸에 덮어씀
 *            -> 응답 칸에는 헤더만(통계 모드는 32바이트 통계도) -> 클라이언트가 요청 칸에서 복사
 *   복사가 정규화 모드에서 두 번, 통계 모드에서 한 번 줄고, 청크마다 하던 malloc/free와
 *   응답 칸의 큰 영역(페이지)을 쓰지 않는다.
 *   요청 칸은 서버가 아니라 응답을 다 읽은 클라이언트가 돌려준다(REQ_EMPTY를 클라이언트가 올림).
 *   클라이언트는 seq % 16 자리에 그 청크를 쓴 칸 번호를 적어 두었다가 응답의 seq로 찾는다.
 *   나가 있는 요청이 창 크기(최대 4) 이하이고 seq가 연속이라 16칸 안에서 겹치지 않는다.
 *   칸이 돌아오는 순서가 바뀌어도(창 2 이상) 다음에 쓸 칸은 16번 전에 쓴 칸이라 이미 비어 있다.
 *
 * 알려진 한계 (pipe 버전과 같음): 클라이언트가 BYE 없이 죽으면, 큐에 남아 있던 그 클라이언트의
 * 청크 응답이 같은 번호를 새로 받은 클라이언트에게 갈 수 있다. 빈 슬롯은 돌아가며 주어 바로
 * 재사용되지 않게 했고, 클라이언트는 seq와 크기를 검사해 잘못된 응답을 센다.
 */
#include "ipc.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>

#define SHM_MAGIC       0x37534D48u             /* "7SMH" */
#define WAIT_MS         100                     /* 막힐 수 있는 대기의 확인 간격 */
#define CELL_SIZE       ((sizeof(app_hdr_t) + APP_MAX_CHUNK + 63) & ~(size_t)63)
#define HDR_AREA        4096                    /* 머리 영역. 링은 페이지 경계부터 */

#define SEM_REQ_EMPTY(i) ((i) * 4 + 0)
#define SEM_REQ_FULL(i)  ((i) * 4 + 1)
#define SEM_RSP_EMPTY(i) ((i) * 4 + 2)
#define SEM_RSP_FULL(i)  ((i) * 4 + 3)
#define SEM_NOTIFY       (APP_MAX_CLIENTS * 4)
#define NSEMS            (SEM_NOTIFY + 1)

enum { SLOT_FREE = 0, SLOT_USED = 1 };

typedef union semun {                           /* Linux는 직접 선언해야 함 */
    int val;
    struct semid_ds *buf;
    unsigned short *array;
} semun_t;

typedef struct {
    uint32_t state;     /* SLOT_*. 쓰기는 table_mu 안에서, 서버 수신 스레드는 atomic으로 읽음 */
    uint32_t gen;       /* 접속할 때마다 1 늘림. 서버가 새 접속을 알아보는 데 씀 */
    int32_t  pid;       /* 클라이언트 pid */
    uint32_t pad;
} slot_entry_t;

typedef struct {
    uint32_t        magic;
    int32_t         server_pid;
    uint32_t        next_hint;                  /* 다음에 먼저 볼 빈 슬롯 (번호를 돌아가며 줌) */
    uint32_t        inplace;                    /* 1 = E5 제자리 처리. 클라이언트가 접속할 때 읽음 */
    pthread_mutex_t table_mu;                   /* 프로세스 공유, robust */
    slot_entry_t    slot[APP_MAX_CLIENTS];
} shm_head_t;

_Static_assert(sizeof(shm_head_t) <= HDR_AREA, "shm_head_t가 머리 영역보다 큼");

#define SLOT_SIZE   ((size_t)(APP_SHM_RING + APP_SHM_RSP_RING) * CELL_SIZE)
#define SHM_SIZE    (HDR_AREA + (size_t)APP_MAX_CLIENTS * SLOT_SIZE)

static uint8_t *req_cell(uint8_t *base, uint32_t slot, uint32_t k)
{
    return base + HDR_AREA + slot * SLOT_SIZE + (size_t)k * CELL_SIZE;
}

static uint8_t *rsp_cell(uint8_t *base, uint32_t slot, uint32_t k)
{
    return base + HDR_AREA + slot * SLOT_SIZE + (size_t)(APP_SHM_RING + k) * CELL_SIZE;
}

/* ------------------------------------------------------------------ */
/* 세마포어 도우미                                                      */
/* ------------------------------------------------------------------ */

/* 반환: 0 성공, 1 시간 초과(WAIT_MS), -1 오류(EIDRM, EINVAL: 집합이 지워짐) */
static int sem_wait_ms(int semid, unsigned short num)
{
    struct sembuf sb = { num, -1, 0 };
    struct timespec ts = { 0, WAIT_MS * 1000000L };

    for (;;) {
        if (semtimedop(semid, &sb, 1, &ts) == 0) return 0;
        if (errno == EINTR) continue;
        if (errno == EAGAIN) return 1;
        return -1;
    }
}

/* 기다리지 않는 P. 반환: 0 성공, 1 비어 있음, -1 오류 */
static int sem_trywait(int semid, unsigned short num)
{
    struct sembuf sb = { num, -1, IPC_NOWAIT };

    for (;;) {
        if (semop(semid, &sb, 1) == 0) return 0;
        if (errno == EINTR) continue;
        if (errno == EAGAIN) return 1;
        return -1;
    }
}

/* V. a와 b(b가 음수면 a만)를 semop 한 번에 함께 올린다 */
static int sem_post2(int semid, int a, int b)
{
    struct sembuf sb[2] = { { (unsigned short)a, 1, 0 }, { (unsigned short)b, 1, 0 } };

    for (;;) {
        if (semop(semid, sb, b >= 0 ? 2 : 1) == 0) return 0;
        if (errno == EINTR) continue;
        return -1;
    }
}

static int pid_alive(pid_t pid)
{
    return pid > 0 && (kill(pid, 0) == 0 || errno == EPERM);
}

/* 슬롯 하나의 세마포어를 처음 값으로 */
static int sem_reset_slot(int semid, uint32_t i)
{
    semun_t a;
    a.val = APP_SHM_RING;     if (semctl(semid, SEM_REQ_EMPTY(i), SETVAL, a) < 0) return -1;
    a.val = 0;                if (semctl(semid, SEM_REQ_FULL(i), SETVAL, a) < 0) return -1;
    a.val = APP_SHM_RSP_RING; if (semctl(semid, SEM_RSP_EMPTY(i), SETVAL, a) < 0) return -1;
    a.val = 0;                if (semctl(semid, SEM_RSP_FULL(i), SETVAL, a) < 0) return -1;
    return 0;
}

static void table_lock(shm_head_t *h)
{
    int r = pthread_mutex_lock(&h->table_mu);
    if (r == EOWNERDEAD)                        /* 잡은 클라이언트가 죽음. 표는 한 칸씩만 바뀌어 일관됨 */
        pthread_mutex_consistent(&h->table_mu);
}

/* ------------------------------------------------------------------ */
/* 서버                                                                */
/* ------------------------------------------------------------------ */

typedef struct {
    /* 수신 스레드만 만지는 것 */
    uint32_t   gen;         /* 받아들인 접속의 세대 */
    uint32_t   ridx;        /* 요청 링 다음에 읽을 칸 */

    /* 워커들과 같이 쓰는 것: wmu로 보호 */
    pthread_mutex_t wmu;
    int        active;      /* 수신 스레드가 wmu 안에서만 바꿈 */
    pid_t      pid;
    uint32_t   widx;        /* 응답 링 다음에 쓸 칸 */

    atomic_int closing;     /* 수신 스레드가 슬롯을 비우려 함 -> 기다리는 워커는 포기 */
} srv_slot_t;

static int opt_inplace;                         /* ipc_server_option("inplace") */

struct ipc_server {
    int          shmid, semid;
    int          inplace;
    uint8_t     *base;
    shm_head_t  *head;
    atomic_int   stop;
    uint32_t     rr;                /* 다음 server_recv가 먼저 볼 슬롯 */
    struct timespec last_check;     /* 마지막 생존 확인 */
    srv_slot_t   c[APP_MAX_CLIENTS];
};

/* 수신 스레드. 슬롯을 비운다: 응답을 쓰던 워커가 손을 떼게 한 뒤 세마포어를 되돌리고 표에서 뺀다 */
static void srv_drop_slot(ipc_server_t *s, uint32_t id)
{
    srv_slot_t *c = &s->c[id];

    atomic_store(&c->closing, 1);
    pthread_mutex_lock(&c->wmu);                /* 기다리던 워커는 WAIT_MS 안에 놓고 나감 */
    if (c->active) {
        c->active = 0;
        sem_reset_slot(s->semid, id);
    }
    pthread_mutex_unlock(&c->wmu);
    atomic_store(&c->closing, 0);

    table_lock(s->head);
    s->head->slot[id].state = SLOT_FREE;
    s->head->slot[id].pid = 0;
    pthread_mutex_unlock(&s->head->table_mu);
}

/* 수신 스레드. 슬롯 표를 보고 새 접속을 받아들인다. 1 = 새로 받아들임 */
static int srv_check_new(ipc_server_t *s, uint32_t id)
{
    slot_entry_t *e = &s->head->slot[id];
    srv_slot_t *c = &s->c[id];
    uint32_t st = __atomic_load_n(&e->state, __ATOMIC_ACQUIRE);
    uint32_t gen = __atomic_load_n(&e->gen, __ATOMIC_ACQUIRE);

    if (st != SLOT_USED || (c->active && c->gen == gen))
        return 0;
    if (c->active)                              /* 생기면 안 되지만, 옛 접속은 정리 */
        srv_drop_slot(s, id);

    table_lock(s->head);                        /* 접속 직후 pid를 확실히 읽으려고 */
    pid_t pid = e->pid;
    gen = e->gen;
    pthread_mutex_unlock(&s->head->table_mu);

    c->gen = gen;
    c->ridx = 0;
    pthread_mutex_lock(&c->wmu);
    c->pid = pid;
    c->widx = 0;
    c->active = 1;
    pthread_mutex_unlock(&c->wmu);
    return 1;
}

/* 수신 스레드. WAIT_MS마다 죽은 클라이언트 슬롯을 비운다. 1 = 하나라도 비움 */
static int srv_check_alive(ipc_server_t *s)
{
    struct timespec now;
    int dropped = 0;

    clock_gettime(CLOCK_MONOTONIC, &now);
    if ((now.tv_sec - s->last_check.tv_sec) * 1000L +
        (now.tv_nsec - s->last_check.tv_nsec) / 1000000L < WAIT_MS)
        return 0;
    s->last_check = now;

    for (uint32_t i = 0; i < APP_MAX_CLIENTS; i++) {
        srv_slot_t *c = &s->c[i];
        if (c->active && !pid_alive(c->pid)) {
            fprintf(stderr, "ipc_shm: client %u (pid %d)가 BYE 없이 끝남, 슬롯 비움\n",
                    i, (int)c->pid);
            srv_drop_slot(s, i);
            dropped = 1;
        }
    }
    return dropped;
}

/* 요청 링에서 하나 꺼내기.
 * 반환: 1 = 청크(out), 0 = 비어 있음, 2 = BYE 등으로 슬롯을 비움, -1 = 세마포어 오류 */
static int srv_take(ipc_server_t *s, uint32_t id, chunk_t *out)
{
    srv_slot_t *c = &s->c[id];
    int r = sem_trywait(s->semid, SEM_REQ_FULL(id));
    app_hdr_t h;
    uint8_t *cell;

    if (r != 0)
        return r > 0 ? 0 : -1;

    cell = req_cell(s->base, id, c->ridx);
    memcpy(&h, cell, sizeof(h));

    if (h.type == MSG_CHUNK && h.size <= APP_MAX_CHUNK) {
        out->hdr = h;
        out->hdr.client = id;                   /* 번호는 서버가 정한 값을 믿는다 */
        if (s->inplace) {                       /* E5: 칸을 그대로 넘기고 돌려주지 않는다 */
            out->data = cell + sizeof(h);
            c->ridx = (c->ridx + 1) % APP_SHM_RING;
            return 1;
        }
        out->data = xmalloc(h.size ? h.size : 1);
        memcpy(out->data, cell + sizeof(h), h.size);   /* 기본판: 칸 -> 큐 버퍼 (6.4절) */
    }
    c->ridx = (c->ridx + 1) % APP_SHM_RING;
    if (sem_post2(s->semid, SEM_REQ_EMPTY(id), -1) < 0)
        return -1;

    if (h.type == MSG_CHUNK && h.size <= APP_MAX_CHUNK)
        return 1;
    if (h.type != MSG_BYE)
        fprintf(stderr, "ipc_shm: client %u 잘못된 헤더 (type %u, size %lu), 끊음\n",
                id, h.type, (unsigned long)h.size);
    srv_drop_slot(s, id);
    return 2;
}

ipc_server_t *server_open(void)
{
    ipc_server_t *s = calloc(1, sizeof(*s));
    pthread_mutexattr_t ma;
    unsigned short init[NSEMS];
    semun_t arg;

    if (!s) return NULL;
    ipc_cleanup();                              /* 남은 같은 key 자원 정리 (8.4절 3번) */

    s->shmid = s->semid = -1;
    for (int i = 0; i < APP_MAX_CLIENTS; i++)
        pthread_mutex_init(&s->c[i].wmu, NULL);

    s->shmid = shmget(APP_SHM_KEY, SHM_SIZE, 0600 | IPC_CREAT | IPC_EXCL);
    if (s->shmid < 0) { perror("shmget"); goto fail; }
    s->base = shmat(s->shmid, NULL, 0);
    if (s->base == (void *)-1) { s->base = NULL; perror("shmat"); goto fail; }
    s->head = (shm_head_t *)s->base;

    s->semid = semget(APP_SEM_KEY, NSEMS, 0600 | IPC_CREAT | IPC_EXCL);
    if (s->semid < 0) { perror("semget"); goto fail; }
    for (uint32_t i = 0; i < APP_MAX_CLIENTS; i++) {
        init[SEM_REQ_EMPTY(i)] = APP_SHM_RING;
        init[SEM_REQ_FULL(i)]  = 0;
        init[SEM_RSP_EMPTY(i)] = APP_SHM_RSP_RING;
        init[SEM_RSP_FULL(i)]  = 0;
    }
    init[SEM_NOTIFY] = 0;
    arg.array = init;
    if (semctl(s->semid, 0, SETALL, arg) < 0) { perror("semctl SETALL"); goto fail; }

    /* 머리: 새 세그먼트는 0으로 채워져 있다. mutex와 표만 준비하고 마지막에 magic */
    pthread_mutexattr_init(&ma);
    pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&s->head->table_mu, &ma);
    pthread_mutexattr_destroy(&ma);
    s->head->server_pid = (int32_t)getpid();
    s->head->next_hint = 0;
    s->head->inplace = (uint32_t)opt_inplace;
    s->inplace = opt_inplace;
    if (s->inplace)
        fprintf(stderr, "ipc_shm: E5 제자리 처리 켬 (-X inplace)\n");
    __atomic_store_n(&s->head->magic, SHM_MAGIC, __ATOMIC_RELEASE);

    clock_gettime(CLOCK_MONOTONIC, &s->last_check);
    return s;

fail:
    server_close(s);
    return NULL;
}

int server_recv(ipc_server_t *s, chunk_t *out)
{
    for (;;) {
        int w = sem_wait_ms(s->semid, SEM_NOTIFY);
        int conn_event = 0;

        if (w < 0) {
            if (!atomic_load(&s->stop)) perror("ipc_shm: semtimedop NOTIFY");
            return -1;
        }
        if (atomic_load(&s->stop))              /* server_stop */
            return -1;

        conn_event |= srv_check_alive(s);

        for (uint32_t k = 0; k < APP_MAX_CLIENTS; k++) {    /* rr부터 차례로 */
            uint32_t id = (s->rr + k) % APP_MAX_CLIENTS;
            int r;

            conn_event |= srv_check_new(s, id);
            if (!s->c[id].active)
                continue;
            r = srv_take(s, id, out);
            if (r == 1) {
                s->rr = (id + 1) % APP_MAX_CLIENTS;
                return 1;
            }
            if (r == 2) {
                conn_event = 1;
                break;                          /* NOTIFY 하나에 하나씩 처리 */
            }
            if (r < 0)
                return -1;
        }
        if (conn_event)
            return 0;
        /* 찾은 게 없음: 접속 알림이었거나, 앞선 훑기에서 이미 꺼낸 것의 알림. 다시 기다림 */
    }
}

int server_send(ipc_server_t *s, const app_hdr_t *h, const void *payload)
{
    srv_slot_t *c;
    uint8_t *cell;
    int r = -1;

    if (h->client >= APP_MAX_CLIENTS || h->size > APP_MAX_CHUNK)
        return -1;
    c = &s->c[h->client];
    if (s->inplace && h->type == MSG_RSP_AUDIO) {     /* payload가 그 클라이언트의 요청 칸인지 */
        const uint8_t *p = payload, *r0 = req_cell(s->base, h->client, 0) + sizeof(app_hdr_t);
        if (p < r0 || p >= r0 + (size_t)APP_SHM_RING * CELL_SIZE || (size_t)(p - r0) % CELL_SIZE)
            return -1;
    }

    pthread_mutex_lock(&c->wmu);                /* 응답 하나를 끝까지 넣은 뒤 푼다 */
    if (!c->active)
        goto out;
    for (;;) {                                  /* 응답 링 빈 칸 기다리기 */
        int w = sem_wait_ms(s->semid, SEM_RSP_EMPTY(h->client));
        if (w == 0) break;
        if (w < 0) goto out;
        if (atomic_load(&c->closing) || !pid_alive(c->pid))
            goto out;                           /* 클라이언트가 나감 */
    }
    cell = rsp_cell(s->base, h->client, c->widx);
    memcpy(cell, h, sizeof(*h));
    if (s->inplace && h->type == MSG_RSP_AUDIO) {
        /* E5: 정규화 결과는 이미 요청 칸에 있다. 헤더만 넣고 클라이언트가 요청 칸에서 읽는다 */
    } else if (h->size) {
        memcpy(cell + sizeof(*h), payload, h->size);
    }
    c->widx = (c->widx + 1) % APP_SHM_RSP_RING;
    r = sem_post2(s->semid, SEM_RSP_FULL(h->client), -1);
out:
    pthread_mutex_unlock(&c->wmu);
    return r;
}

int ipc_server_option(const char *opt)
{
    if (strcmp(opt, "inplace") == 0) { opt_inplace = 1; return 0; }
    return -1;
}

void server_chunk_free(ipc_server_t *s, chunk_t *c)
{
    if (!s->inplace)                            /* 제자리: shm 칸이라 free하지 않음 */
        free(c->data);
    c->data = NULL;
}

void server_stop(ipc_server_t *s)
{
    atomic_store(&s->stop, 1);
    sem_post2(s->semid, SEM_NOTIFY, -1);        /* 수신 스레드를 깨움 */
}

/* 8.4절 1번 순서: (워커 join은 server.c) -> shmdt -> shmctl(IPC_RMID) -> semctl(IPC_RMID).
 * 세마포어를 지우면 아직 붙어 있는 클라이언트의 대기가 EIDRM으로 바로 끝난다 */
void server_close(ipc_server_t *s)
{
    if (!s) return;
    if (s->head)
        __atomic_store_n(&s->head->magic, 0, __ATOMIC_RELEASE);   /* 새 접속을 막음 */
    if (s->base && shmdt(s->base) < 0) perror("shmdt");
    if (s->shmid >= 0 && shmctl(s->shmid, IPC_RMID, NULL) < 0) perror("shmctl IPC_RMID");
    if (s->semid >= 0 && semctl(s->semid, 0, IPC_RMID) < 0) perror("semctl IPC_RMID");
    for (int i = 0; i < APP_MAX_CLIENTS; i++)
        pthread_mutex_destroy(&s->c[i].wmu);
    free(s);
}

/* ------------------------------------------------------------------ */
/* 클라이언트                                                          */
/* ------------------------------------------------------------------ */

struct ipc_client {
    int       semid;
    uint8_t  *base;
    uint32_t  id;
    pid_t     server_pid;
    uint32_t  widx;         /* 요청 링 다음에 쓸 칸 (송신 스레드만) */
    uint32_t  ridx;         /* 응답 링 다음에 읽을 칸 (수신 스레드만) */
    int       inplace;      /* E5: 서버가 제자리 처리 중 */
    uint32_t  cell_of[APP_SHM_RING];   /* E5: seq % 16 -> 그 청크를 쓴 요청 칸 (송신 쓰기, 수신 읽기) */
};

/* 막힐 수 있는 P. 서버가 죽었거나 집합이 지워졌으면 -1 */
static int cli_wait(ipc_client_t *c, unsigned short num)
{
    for (;;) {
        int w = sem_wait_ms(c->semid, num);
        if (w == 0) return 0;
        if (w < 0) return -1;                   /* 서버가 정상 종료하며 지움(EIDRM) */
        if (!pid_alive(c->server_pid)) return -1;   /* 서버가 kill -9 등으로 죽음 */
    }
}

ipc_client_t *client_connect(uint32_t *client_id)
{
    ipc_client_t *c = calloc(1, sizeof(*c));
    shm_head_t *h;
    int shmid;
    uint32_t id = UINT32_MAX;

    if (!c) return NULL;
    c->semid = -1;

    shmid = shmget(APP_SHM_KEY, 0, 0);          /* 서버가 없으면 ENOENT */
    if (shmid < 0) goto fail;
    c->base = shmat(shmid, NULL, 0);
    if (c->base == (void *)-1) { c->base = NULL; perror("shmat"); goto fail; }
    h = (shm_head_t *)c->base;
    if (__atomic_load_n(&h->magic, __ATOMIC_ACQUIRE) != SHM_MAGIC ||
        !pid_alive(h->server_pid)) {
        fprintf(stderr, "ipc_shm: 서버가 준비되지 않았거나 이미 끝남\n");
        goto fail;
    }
    c->server_pid = h->server_pid;
    c->inplace = (int)h->inplace;
    c->semid = semget(APP_SEM_KEY, 0, 0);
    if (c->semid < 0) { perror("semget"); goto fail; }

    /* 빈 슬롯 잡기 (6.4절 접속) */
    table_lock(h);
    for (uint32_t k = 0; k < APP_MAX_CLIENTS; k++) {
        uint32_t cand = (h->next_hint + k) % APP_MAX_CLIENTS;
        if (h->slot[cand].state == SLOT_FREE) { id = cand; break; }
    }
    if (id != UINT32_MAX) {
        h->slot[id].pid = (int32_t)getpid();
        __atomic_store_n(&h->slot[id].gen, h->slot[id].gen + 1, __ATOMIC_RELEASE);
        __atomic_store_n(&h->slot[id].state, SLOT_USED, __ATOMIC_RELEASE);
        h->next_hint = (id + 1) % APP_MAX_CLIENTS;
    }
    pthread_mutex_unlock(&h->table_mu);
    if (id == UINT32_MAX) {
        fprintf(stderr, "ipc_shm: 접속 거절 (클라이언트가 %d개 꽉 참)\n", APP_MAX_CLIENTS);
        goto fail;
    }

    if (sem_post2(c->semid, SEM_NOTIFY, -1) < 0) {   /* 서버 수신 스레드에 접속 알림 */
        perror("semop NOTIFY");
        goto fail;                              /* 슬롯은 서버의 생존 확인이 비운다 */
    }
    c->id = id;
    *client_id = id;
    return c;

fail:
    if (c->base) shmdt(c->base);
    free(c);
    return NULL;
}

int client_send(ipc_client_t *c, const app_hdr_t *h, const void *payload)
{
    uint8_t *cell;

    if (h->size > APP_MAX_CHUNK)
        return -1;
    if (cli_wait(c, SEM_REQ_EMPTY(c->id)) < 0)
        return -1;
    cell = req_cell(c->base, c->id, c->widx);
    memcpy(cell, h, sizeof(*h));
    if (h->size)
        memcpy(cell + sizeof(*h), payload, h->size);
    if (c->inplace)
        __atomic_store_n(&c->cell_of[h->seq % APP_SHM_RING], c->widx, __ATOMIC_RELEASE);
    c->widx = (c->widx + 1) % APP_SHM_RING;
    return sem_post2(c->semid, SEM_REQ_FULL(c->id), SEM_NOTIFY);   /* 칸과 알림을 한 번에 */
}

int client_recv(ipc_client_t *c, app_hdr_t *h, void *buf, size_t cap)
{
    uint8_t *cell, *req = NULL;
    const uint8_t *src = NULL;
    int r = 0;

    if (cli_wait(c, SEM_RSP_FULL(c->id)) < 0)
        return -1;
    cell = rsp_cell(c->base, c->id, c->ridx);
    memcpy(h, cell, sizeof(*h));
    if (c->inplace) {                           /* E5: 이 응답의 요청 칸 */
        uint32_t k = __atomic_load_n(&c->cell_of[h->seq % APP_SHM_RING], __ATOMIC_ACQUIRE);
        req = req_cell(c->base, c->id, k);
        if (h->type == MSG_RSP_AUDIO)
            src = req + sizeof(*h);             /* 정규화 결과는 요청 칸에 있다 */
    }
    if (h->size > cap || h->size > APP_MAX_CHUNK)
        r = -1;
    else if (h->size)
        memcpy(buf, src ? src : cell + sizeof(*h), h->size);
    c->ridx = (c->ridx + 1) % APP_SHM_RSP_RING;
    /* 응답 칸과(E5면) 요청 칸을 한 번에 돌려준다. 요청 칸은 복사를 마친 뒤에 */
    if (sem_post2(c->semid, SEM_RSP_EMPTY(c->id), req ? (int)SEM_REQ_EMPTY(c->id) : -1) < 0)
        return -1;
    return r;
}

void client_close(ipc_client_t *c)
{
    app_hdr_t bye = { MSG_BYE, 0, 0, 0, 0 };

    if (!c) return;
    bye.client = c->id;
    client_send(c, &bye, NULL);                 /* 실패해도(서버가 먼저 나감) 정리는 계속 */
    shmdt(c->base);
    free(c);
}

/* ------------------------------------------------------------------ */

void ipc_cleanup(void)
{
    int id;

    if ((id = shmget(APP_SHM_KEY, 0, 0)) >= 0 && shmctl(id, IPC_RMID, NULL) == 0)
        fprintf(stderr, "ipc_shm: 남은 shm(key %d)을 지움\n", (int)APP_SHM_KEY);
    if ((id = semget(APP_SEM_KEY, 0, 0)) >= 0 && semctl(id, 0, IPC_RMID) == 0)
        fprintf(stderr, "ipc_shm: 남은 세마포어(key %d)를 지움\n", (int)APP_SEM_KEY);
}

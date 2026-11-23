/*
 * shm_bench.c -- 버전 2: System V shared memory + System V 세마포어 (7조)
 *
 * 자원 (key는 7조 범위 60070~60079)
 *   shm key 60071 : 세그먼트 1개
 *   sem key 60072 : 세마포어 집합 1개 (채널당 3개 x 8채널 = 24개)
 *
 * 채널 ch 하나의 공유 메모리 구성
 *   [요청 링 버퍼 칸 0][칸 1]...[칸 Q-1][응답 칸]
 *   칸 크기 = 헤더 16B + 최대 메시지 크기 (64B 단위로 올림)
 *
 * 채널 ch의 세마포어 3개 (생산자-소비자 패턴)
 *   EMPTY(ch) 초기값 Q : 요청 링의 빈 칸 수   (클라이언트가 P, 서버가 V)
 *   FULL(ch)  초기값 0 : 요청 링의 찬 칸 수   (클라이언트가 V, 서버가 P)
 *   RESP(ch)  초기값 0 : 응답 칸에 응답이 있음 (서버가 V, 클라이언트가 P)
 *
 * 한 채널의 요청 링은 생산자(워커 1개)와 소비자(핸들러 1개)가 하나씩이라
 * head는 클라이언트만, tail은 서버만 바꾼다. 칸 접근 순서는 세마포어가 정한다.
 * 응답 칸은 서버가 쓰고 V(RESP) 한 뒤, 클라이언트가 P(RESP) 하고 읽는다.
 * 클라이언트는 응답을 다 읽고 나서야 다음 요청을 보내므로 응답 칸이 덮이지 않는다.
 *
 * SEM_UNDO를 쓰지 않는 이유: 카운팅 세마포어를 P와 V가 서로 다른 프로세스에서
 * 하므로, 한쪽이 죽을 때 커널이 되돌리면 값이 틀어진다. 대신 비정상 종료 시
 * 세마포어 집합을 통째로 지운다(IPC_RMID).
 */
#include "bench.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>

typedef union semun {          /* Linux는 직접 선언해야 함 */
    int val;
    struct semid_ds *buf;
    unsigned short *array;
} semun_t;

#define SEM_EMPTY(ch) ((ch) * 3 + 0)
#define SEM_FULL(ch)  ((ch) * 3 + 1)
#define SEM_RESP(ch)  ((ch) * 3 + 2)
#define NSEMS         (MAX_CLIENTS * 3)

static int shmid = -1, semid = -1;
static char *base;                 /* shmat 주소 (fork로 자식도 같은 곳에 붙어 있음) */
static size_t slot_sz;             /* 칸 하나 크기 */
static int qdepth;
static int head[MAX_CLIENTS];      /* 클라이언트만 사용 */
static int tail[MAX_CLIENTS];      /* 서버만 사용 */

static char *req_slot(int ch, int idx)
{
    return base + ((size_t)ch * (qdepth + 1) + (size_t)idx) * slot_sz;
}

static char *resp_slot(int ch)
{
    return base + ((size_t)ch * (qdepth + 1) + (size_t)qdepth) * slot_sz;
}

/* EINTR이면 다시, EIDRM/EINVAL(정리 중 삭제됨)이면 실패 */
static int sem_op(int num, int delta)
{
    struct sembuf sb = { (unsigned short)num, (short)delta, 0 };
    while (semop(semid, &sb, 1) == -1) {
        if (errno == EINTR) continue;
        return -1;
    }
    return 0;
}
#define P(n) sem_op((n), -1)
#define V(n) sem_op((n), +1)

/* 지난 실행이 kill -9 등으로 남긴 자원이 있으면 지운다 */
static void remove_stale(void)
{
    int id;
    if ((id = shmget(TEAM_SHM_KEY, 0, 0)) != -1) {
        fprintf(stderr, "[shm] 이전 실행이 남긴 shm(key %d)을 지웁니다.\n", (int)TEAM_SHM_KEY);
        shmctl(id, IPC_RMID, NULL);
    }
    if ((id = semget(TEAM_SEM_KEY, 0, 0)) != -1) {
        fprintf(stderr, "[shm] 이전 실행이 남긴 세마포어(key %d)를 지웁니다.\n", (int)TEAM_SEM_KEY);
        semctl(id, 0, IPC_RMID);
    }
}

static int s_setup(const bench_opts_t *o, size_t max_msg)
{
    unsigned short init[NSEMS];
    semun_t arg;
    size_t total;

    remove_stale();

    qdepth = o->qdepth;
    slot_sz = (HDR_SIZE + (max_msg > sizeof(srv_report_t) ? max_msg : sizeof(srv_report_t)) + 63) & ~(size_t)63;
    total = slot_sz * (size_t)(qdepth + 1) * MAX_CLIENTS;

    shmid = shmget(TEAM_SHM_KEY, total, 0600 | IPC_CREAT | IPC_EXCL);
    if (shmid == -1) { perror("shmget"); return -1; }
    base = shmat(shmid, NULL, 0);
    if (base == (void *)-1) { perror("shmat"); base = NULL; return -1; }
    memset(base, 0, total);        /* 페이지를 미리 붙여 둔다 (측정 중 첫 페이지 폴트 방지) */

    semid = semget(TEAM_SEM_KEY, NSEMS, 0600 | IPC_CREAT | IPC_EXCL);
    if (semid == -1) { perror("semget"); return -1; }
    for (int ch = 0; ch < MAX_CLIENTS; ch++) {
        init[SEM_EMPTY(ch)] = (unsigned short)qdepth;
        init[SEM_FULL(ch)] = 0;
        init[SEM_RESP(ch)] = 0;
    }
    arg.array = init;
    if (semctl(semid, 0, SETALL, arg) == -1) { perror("semctl SETALL"); return -1; }

    printf("[shm] shmid=%d (key %d, %zu bytes, 채널당 링 %d칸), semid=%d (key %d, %d개)\n",
           shmid, (int)TEAM_SHM_KEY, total, qdepth, semid, (int)TEAM_SEM_KEY, NSEMS);
    fflush(stdout);
    return 0;
}

static void s_after_fork_server(void) {}
static void s_after_fork_client(void) {}

/* 클라이언트: 빈 칸을 하나 얻어(P EMPTY) 복사하고, 찼다고 알린다(V FULL) */
static int s_cli_send(int ch, const void *buf, size_t len)
{
    if (P(SEM_EMPTY(ch)) != 0) return -1;
    memcpy(req_slot(ch, head[ch]), buf, len);
    head[ch] = (head[ch] + 1) % qdepth;
    return V(SEM_FULL(ch));
}

/* 클라이언트: 응답이 올 때까지 기다렸다가(P RESP) 응답 칸에서 복사 */
static int s_cli_recv(int ch, void *buf, size_t len)
{
    if (P(SEM_RESP(ch)) != 0) return -1;
    memcpy(buf, resp_slot(ch), len);
    return 0;
}

/* 서버: 찬 칸을 기다렸다가(P FULL) 로컬 버퍼로 복사하고 칸을 돌려준다(V EMPTY) */
static int s_srv_recv(int ch, void *buf, size_t cap)
{
    char *slot;
    msg_hdr_t h;

    if (P(SEM_FULL(ch)) != 0) return -1;
    slot = req_slot(ch, tail[ch]);
    memcpy(&h, slot, HDR_SIZE);
    if (HDR_SIZE + h.size > cap) return -1;
    memcpy(buf, slot, HDR_SIZE + h.size);
    tail[ch] = (tail[ch] + 1) % qdepth;
    return V(SEM_EMPTY(ch));
}

/* 서버: 응답 칸에 쓰고 알린다(V RESP) */
static int s_srv_send(int ch, const void *buf, size_t len)
{
    memcpy(resp_slot(ch), buf, len);
    return V(SEM_RESP(ch));
}

static void s_cleanup(void)
{
    /* 세마포어를 먼저 지운다: 대기 중인 스레드가 있으면 EIDRM으로 깨어나 멈춘다.
     * shmdt는 하지 않는다. 신호로 끝날 때 다른 스레드가 아직 복사 중일 수 있고,
     * 프로세스가 끝나면 커널이 알아서 떼어 낸다. IPC_RMID 표시만 하면
     * 마지막 프로세스가 떨어질 때 세그먼트가 사라진다. */
    if (semid != -1 && semctl(semid, 0, IPC_RMID) == -1) perror("semctl IPC_RMID");
    if (shmid != -1 && shmctl(shmid, IPC_RMID, NULL) == -1) perror("shmctl IPC_RMID");
    shmid = semid = -1;
}

static const transport_t shm_tp = {
    .name = "shm",
    .setup = s_setup,
    .after_fork_server = s_after_fork_server,
    .after_fork_client = s_after_fork_client,
    .cli_send = s_cli_send,
    .cli_recv = s_cli_recv,
    .srv_recv = s_srv_recv,
    .srv_send = s_srv_send,
    .cleanup = s_cleanup,
};

int main(int argc, char **argv)
{
    return bench_main(&shm_tp, argc, argv);
}

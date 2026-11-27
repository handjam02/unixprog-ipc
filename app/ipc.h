/*
 * ipc.h -- 응용(오디오 청크 분석 서버) IPC 공통 인터페이스 (7조) [초안, 11/26]
 *
 * server.c, client.c는 이 파일의 함수만 부른다.
 * ipc_pipe.c(손재민)와 ipc_shm.c(박건석)가 같은 함수를 각각 구현하고,
 * 링크할 때 둘 중 하나만 붙여 server_pipe / server_shm을 만든다(기획서 8.1절).
 *
 * 지킬 것
 *   1. 타임스탬프, 통계, 분석, 작업 큐는 IPC 층에 넣지 않는다. 전부 server.c/client.c 몫이다.
 *      그래야 두 버전의 차이가 IPC 부분에서만 나온다.
 *   2. 함수마다 부르는 스레드가 정해져 있다(아래 주석). 그 밖의 스레드에서 부르지 않는다.
 *   3. 서버 수신은 기본판에서 채널(FIFO 또는 shm 링 칸)의 내용을 큐 버퍼로 복사한다(기획서 6.4절).
 *      shm 제자리 처리는 설계 개선 후보(E5)라 기본판에 넣지 않는다.
 */
#ifndef IPC_H
#define IPC_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

/* ---- 자원 이름 (기획서 8.3절) ---- */
#define APP_SHM_KEY     ((key_t)60070)          /* 응용 shm */
#define APP_SEM_KEY     ((key_t)60073)          /* 응용 세마포어 집합 (System V) */
#define APP_FIFO_SRV    "/tmp/osproject_7_srv"  /* 공개 FIFO, HELLO만 */
#define APP_FIFO_FMT    "/tmp/osproject_7_%d_%s" /* pid, "req" 또는 "rsp" */

#define APP_MAX_CLIENTS 8
#define APP_MAX_CHUNK   (256u * 1024u)          /* 청크 최대 256KB */
#define APP_SHM_RING    16                      /* shm 링 채널당 16칸 (기획서 5.1, E1 근거) */
#define APP_SHM_RSP_RING 4                      /* 응답 링 채널당 4칸 (창 최대 4), 요청 링 뒤 같은 세그먼트(60070) */

/* ---- 메시지 종류 (기획서 6.1절) ---- */
enum {
    MSG_HELLO     = 1,  /* 클라이언트 -> 서버. payload: hello_t */
    MSG_CHUNK     = 2,  /* 클라이언트 -> 서버. payload: PCM (int16_t, 모노, 48kHz) */
    MSG_RSP_STATS = 3,  /* 서버 -> 클라이언트. payload: rsp_stats_t */
    MSG_RSP_AUDIO = 4,  /* 서버 -> 클라이언트. payload: 정규화된 PCM, 요청과 같은 크기 */
    MSG_BYE       = 5   /* 클라이언트 -> 서버. payload 없음 */
};

enum {
    MODE_STATS = 0,     /* 통계 모드: 응답 수십 바이트 */
    MODE_NORM  = 1      /* 정규화 모드: 요청과 같은 크기 응답 */
};

/* 모든 메시지 앞에 붙는 24바이트 헤더. 1단계 msg_hdr_t와 같은 생각으로,
 * 받는 쪽은 헤더를 먼저 읽고 size만큼 payload를 더 읽는다. */
typedef struct {
    uint32_t type;      /* MSG_* */
    uint32_t client;    /* 서버가 붙인 클라이언트 번호 0..7 (HELLO에서는 무시) */
    uint32_t seq;       /* 클라이언트가 붙이는 청크 순번. 응답은 요청의 seq를 그대로 돌려준다 */
    uint32_t mode;      /* MODE_* (CHUNK에서만 의미) */
    uint64_t size;      /* 뒤따르는 payload 바이트 수 */
} app_hdr_t;

typedef struct {
    int32_t pid;        /* pipe 버전은 이 pid로 FIFO 이름을 만든다. shm 버전도 기록용으로 채운다 */
} hello_t;

typedef struct {
    double   rms;            /* 이 청크 RMS */
    double   peak;           /* 이 청크 피크 (절댓값) */
    uint32_t silent_runs;    /* 무음 구간 수 */
    uint32_t pad;
    double   rms_ratio;      /* 이 청크 RMS / 지금까지 전체 평균 RMS (워커가 전체 통계를 읽는 이유) */
} rsp_stats_t;               /* 32바이트 */

/* 수신 스레드가 받아서 작업 큐에 넣는 단위 */
typedef struct {
    app_hdr_t hdr;
    uint8_t  *data;     /* hdr.size 바이트. server_recv가 할당하고 워커가 처리 뒤 free */
} chunk_t;

typedef struct ipc_server ipc_server_t;   /* 버전마다 내용이 다르다 */
typedef struct ipc_client ipc_client_t;

/* ---- 서버 ---- */

/* main 스레드. 같은 이름의 남은 자원이 있으면 지우고 새로 만든다. 실패하면 NULL */
ipc_server_t *server_open(void);

/* 수신 스레드만 부른다. 청크 하나를 다 받을 때까지 블로킹.
 * 반환: 1 = out에 CHUNK가 담김
 *       0 = 접속 또는 종료 처리만 했음(HELLO, BYE). out은 건드리지 않음
 *      -1 = 서버 종료(server_stop 뒤) 또는 오류
 * pipe: poll() + O_NONBLOCK 부분 읽기(6.5절). shm: 알림 세마포어를 기다린 뒤 링을 훑음 */
int  server_recv(ipc_server_t *s, chunk_t *out);

/* 워커 스레드들이 동시에 부른다. 내부에서 클라이언트별로 직렬화한다.
 * h->type은 MSG_RSP_STATS 또는 MSG_RSP_AUDIO, h->client와 h->seq는 요청 값 그대로.
 * payload를 다 보낼 때까지 반환하지 않는다. 반환: 0 성공, -1 오류(그 클라이언트가 나감 등) */
int  server_send(ipc_server_t *s, const app_hdr_t *h, const void *payload);

/* 시그널 스레드가 부른다. server_recv를 깨워 -1을 돌려주게 한다 */
void server_stop(ipc_server_t *s);

/* main 스레드, 워커 join 뒤. 정상 종료와 Ctrl+C가 같은 정리 순서를 탄다(8.4절) */
void server_close(ipc_server_t *s);

/* ---- 클라이언트 ---- */

/* main 스레드. 접속하고 서버가 준 번호를 *client_id에 담는다. 실패하면 NULL */
ipc_client_t *client_connect(uint32_t *client_id);

/* 송신 스레드만 부른다. 헤더와 payload를 다 보낼 때까지 블로킹. 0 성공, -1 오류 */
int  client_send(ipc_client_t *c, const app_hdr_t *h, const void *payload);

/* 수신 스레드만 부른다. 응답 하나를 다 받을 때까지 블로킹.
 * buf는 cap 바이트 이상이어야 하고, payload가 cap보다 크면 -1. 반환: 0 성공, -1 오류 */
int  client_recv(ipc_client_t *c, app_hdr_t *h, void *buf, size_t cap);

/* main 스레드, 송수신 스레드 join 뒤. BYE를 보내고 자기 자원(FIFO 등)을 정리한다 */
void client_close(ipc_client_t *c);

/* ---- 비정상 종료 정리 ----
 * make cleanipc와 server_open이 부른다. 남은 shm, 세마포어, FIFO를 지운다 */
void ipc_cleanup(void);

#endif

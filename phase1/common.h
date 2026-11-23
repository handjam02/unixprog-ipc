/*
 * common.h -- 1단계 IPC 벤치마크 공통 정의 (7조)
 *
 * pipe_bench, shm_bench 두 프로그램이 같이 쓰는 것들:
 *   - 메시지 헤더와 연산 코드
 *   - 측정 옵션(메시지 크기, 동시 클라이언트 수, 반복 횟수 ...)
 *   - 시간 측정, 통계(평균/중앙값/p99/최대), getrusage, VmRSS
 *   - CSV 출력
 */
#ifndef COMMON_H
#define COMMON_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/types.h>

/* ---- 7조 IPC 자원 이름 규칙 ----
 * System V key: 60070 ~ 60079 중에서 사용
 *   60071: shared memory 세그먼트
 *   60072: System V 세마포어 집합
 * (pipe 버전은 이름 있는 자원을 쓰지 않는다)
 */
#define TEAM_SHM_KEY   ((key_t)60071)
#define TEAM_SEM_KEY   ((key_t)60072)

#define MAX_CLIENTS    8          /* 서버는 채널 8개를 항상 열어 둔다 */
#define MAX_SIZES      16
#define MAX_CLIENT_SET 8

/* 메시지 앞에 붙는 16바이트 헤더. 서버는 이걸 보고 payload 길이를 안다. */
enum {
    OP_PING       = 1,   /* 같은 크기로 그대로 돌려준다 (왕복 지연) */
    OP_STREAM     = 2,   /* 받기만 하고 응답 없음 (처리량) */
    OP_STREAM_ACK = 3,   /* 받고 나서 헤더만 응답 (처리량 묶음의 마지막) */
    OP_REPORT     = 4,   /* 서버의 rusage, VmRSS를 응답 */
    OP_QUIT       = 5    /* 서버 스레드 종료 */
};

typedef struct {
    uint32_t op;
    uint32_t client;     /* 채널 번호 */
    uint64_t size;       /* 뒤따르는 payload 바이트 수 */
} msg_hdr_t;

#define HDR_SIZE ((size_t)sizeof(msg_hdr_t))

/* 서버가 OP_REPORT에 답하는 내용 */
typedef struct {
    struct rusage ru;
    long vmrss_kb;
    long bad_payload;    /* 서버가 받은 payload 양 끝 바이트가 약속한 값과 다른 횟수 (0이어야 정상) */
} srv_report_t;

/* 클라이언트 채널 ch가 보내는 payload의 b번째 바이트. 채널마다 값이 달라서
 * 다른 채널의 데이터가 섞이면 서버 검사나 클라이언트 memcmp에서 걸린다. */
static inline char pattern_byte(int ch, size_t b)
{
    return (char)(b * 31 + (size_t)ch * 7 + 1);
}

/* ---- 측정 옵션 ---- */
typedef struct {
    size_t sizes[MAX_SIZES];
    int    nsizes;
    int    clients[MAX_CLIENT_SET];
    int    nclients;
    int    warmup;        /* 클라이언트 스레드마다 버리는 왕복 횟수 */
    int    lat_reps;      /* 클라이언트 스레드마다 기록하는 왕복 횟수 */
    int    thr_reps;      /* 처리량 반복 횟수 (조건마다) */
    int    thr_warmup;    /* 처리량 워밍업 반복 횟수 */
    long   thr_msgs;      /* 처리량 1회에 스레드가 보내는 메시지 수, 0이면 자동 */
    int    do_lat;
    int    do_thr;
    int    qdepth;        /* shm 링 버퍼 칸 수 */
    int    global_warmup_ms; /* 첫 조건 전에 모든 채널을 돌려 두는 시간 */
    char   outdir[512];
} bench_opts_t;

void   opts_default(bench_opts_t *o);
int    opts_parse(bench_opts_t *o, int argc, char **argv, const char *prog);
size_t opts_max_size(const bench_opts_t *o);
long   thr_msgs_for(const bench_opts_t *o, size_t size);

/* ---- 시간 ---- */
static inline uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* ---- 통계 ---- */
typedef struct {
    size_t n;
    double mean, median, p99, max, min, stddev;
} stats_t;

void stats_compute(double *v, size_t n, stats_t *st);   /* v는 정렬된다 */

/* ---- 자원 사용량 ---- */
long read_vmrss_kb(void);
double tv_ms(struct timeval a, struct timeval b);       /* b - a, ms */

/* ---- 입출력 도우미 ---- */
ssize_t readn(int fd, void *buf, size_t n);
ssize_t writen(int fd, const void *buf, size_t n);
void   *xmalloc_touch(size_t n);                        /* 할당하고 페이지를 미리 건드림 */

/* ---- 결과 기록 ---- */
typedef struct {
    const char *ipc;     /* "pipe" / "shm" */
    const char *mode;    /* "latency" / "throughput" */
    size_t size;
    int clients;
    int warmup;
    int reps;
    long msgs_per_rep;   /* throughput일 때 스레드당 메시지 수 */
    stats_t st;          /* latency: 왕복 1회(us), throughput: 반복 1회 소요(us) */
    double msgs_per_sec_mean, msgs_per_sec_median;
    double bytes_per_sec_mean, bytes_per_sec_median;
    struct rusage cli_ru0, cli_ru1;
    long cli_vmrss_kb;
    srv_report_t srv0, srv1;
} result_row_t;

FILE *csv_open_summary(const char *outdir, const char *ipc);
void  csv_write_row(FILE *f, const result_row_t *r);
FILE *csv_open_raw(const char *outdir, const char *ipc);
void  print_row(const result_row_t *r);

/* ---- 신호 처리 ----
 * main에서 스레드를 만들기 전에 block_signals()를 부르면
 * 이후 만든 스레드가 전부 신호를 막은 상태를 물려받는다.
 * 신호는 전용 스레드가 sigwait로만 받는다.
 */
void block_signals(sigset_t *set);

#endif

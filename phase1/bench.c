/*
 * bench.c -- 1단계 측정 드라이버 (pipe, shm 공통) (7조)
 *
 * 프로세스 구조
 *   부모 = 클라이언트 프로세스
 *     - main 스레드: 조건(메시지 크기 x 동시 클라이언트 수)을 돌며 워커를 만들고
 *                    결과를 모아 통계를 내고 CSV에 쓴다. 출력은 측정 구간 밖에서만.
 *     - 신호 스레드: SIGINT/SIGTERM/SIGHUP/SIGQUIT/SIGCHLD를 sigwait로 받아
 *                    서버를 끝내고 IPC 자원을 지운 뒤 종료한다.
 *     - 워커 스레드 N개(N = 동시 클라이언트 수): 각자 자기 채널 하나만 쓴다.
 *     => 최소 3개 (N=1일 때 main + 신호 + 워커 1)
 *   자식 = 서버 프로세스 (fork)
 *     - main 스레드: 핸들러 스레드를 만들고 join
 *     - 신호 스레드: 종료 신호를 받으면 바로 끝낸다 (자원 정리는 부모 담당)
 *     - 핸들러 스레드 MAX_CLIENTS(8)개: 채널 하나씩 맡아 요청에 응답
 *     => 10개
 *
 * 경쟁 상태를 막는 방법
 *   - 채널 하나는 한 번에 워커 하나와 핸들러 하나만 쓴다. 버퍼와 결과 배열도
 *     스레드마다 따로 있어서 측정 중 공유 쓰기가 없다.
 *   - 워커 시작은 pthread_barrier로 맞춘다.
 *   - main은 pthread_join 뒤에만 워커 결과를 읽는다 (join이 메모리 순서를 보장).
 *   - 정리 함수는 atomic 플래그로 딱 한 번만 실행된다 (main과 신호 스레드가 겹쳐도).
 */
#include "bench.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/wait.h>
#include <sys/prctl.h>

static const transport_t *TP;
static pid_t server_pid = -1;
static atomic_int cleaned = 0;
static atomic_int shutting_down = 0;

/* 정리는 한 번만 */
static void do_cleanup(void)
{
    if (atomic_exchange(&cleaned, 1))
        return;
    if (TP && TP->cleanup)
        TP->cleanup();
}

/* ================================================================== */
/* 서버 프로세스                                                       */
/* ================================================================== */

static size_t srv_cap;
static atomic_long srv_bad_payload;   /* 핸들러들이 payload 검사에서 틀린 횟수 */

/* payload 첫 바이트와 마지막 바이트가 채널 ch의 값인지 본다.
 * 바이트 2개만 읽으므로 측정에 주는 영향은 무시할 만하다.
 * 복사가 끝까지 되지 않았거나 다른 채널 데이터가 섞이면 여기서 걸린다. */
static void check_payload(int ch, const char *payload, size_t size)
{
    if (size == 0) return;
    if (payload[0] != pattern_byte(ch, 0) || payload[size - 1] != pattern_byte(ch, size - 1))
        atomic_fetch_add_explicit(&srv_bad_payload, 1, memory_order_relaxed);
}

static void *server_signal_thread(void *arg)
{
    sigset_t *set = arg;
    int sig;
    for (;;) {
        if (sigwait(set, &sig) != 0) continue;
        /* Ctrl-C(SIGINT)는 프로세스 그룹 전체에 가므로 서버는 무시하고
         * 부모(클라이언트)가 보내는 SIGTERM으로만 끝난다. 자원 정리는 부모 담당. */
        if (sig == SIGTERM)
            _exit(0);
    }
    return NULL;
}

static void *server_handler(void *arg)
{
    int ch = (int)(intptr_t)arg;
    char *buf = xmalloc_touch(srv_cap);
    msg_hdr_t *h = (msg_hdr_t *)buf;

    for (;;) {
        if (TP->srv_recv(ch, buf, srv_cap) != 0)
            break;                               /* 상대가 끊었거나 자원이 지워짐 */
        if (h->op == OP_PING || h->op == OP_STREAM || h->op == OP_STREAM_ACK)
            check_payload(ch, buf + HDR_SIZE, h->size);
        switch (h->op) {
        case OP_PING:                            /* 받은 그대로(같은 크기) 돌려준다 */
            if (TP->srv_send(ch, buf, HDR_SIZE + h->size) != 0) goto out;
            break;
        case OP_STREAM:                          /* 받기만 한다 */
            break;
        case OP_STREAM_ACK:                      /* 묶음 끝: 헤더만 응답 */
            h->size = 0;
            if (TP->srv_send(ch, buf, HDR_SIZE) != 0) goto out;
            break;
        case OP_REPORT: {                        /* 서버 프로세스 자원 사용량 */
            srv_report_t rep;
            getrusage(RUSAGE_SELF, &rep.ru);
            rep.vmrss_kb = read_vmrss_kb();
            rep.bad_payload = atomic_load(&srv_bad_payload);
            h->size = sizeof(rep);
            memcpy(buf + HDR_SIZE, &rep, sizeof(rep));
            if (TP->srv_send(ch, buf, HDR_SIZE + sizeof(rep)) != 0) goto out;
            break;
        }
        case OP_QUIT:
            goto out;
        default:
            fprintf(stderr, "server: 알 수 없는 op %u (ch %d)\n", h->op, ch);
            goto out;
        }
    }
out:
    free(buf);
    return NULL;
}

static void server_main(size_t max_msg)
{
    pthread_t sig_tid, tids[MAX_CLIENTS];
    static sigset_t set;

    /* 부모가 SIGKILL로 죽어도 서버가 남지 않게 */
    prctl(PR_SET_PDEATHSIG, SIGTERM);
    if (getppid() == 1) _exit(0);

    TP->after_fork_server();
    srv_cap = HDR_SIZE + (max_msg > sizeof(srv_report_t) ? max_msg : sizeof(srv_report_t));

    /* 신호 마스크는 fork 전에 부모가 막아 둔 것을 물려받았다 */
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGHUP);
    sigaddset(&set, SIGQUIT);
    sigaddset(&set, SIGCHLD);
    pthread_create(&sig_tid, NULL, server_signal_thread, &set);

    for (int i = 0; i < MAX_CLIENTS; i++)
        pthread_create(&tids[i], NULL, server_handler, (void *)(intptr_t)i);
    for (int i = 0; i < MAX_CLIENTS; i++)
        pthread_join(tids[i], NULL);
    _exit(0);
}

/* ================================================================== */
/* 클라이언트 프로세스                                                 */
/* ================================================================== */

static void *client_signal_thread(void *arg)
{
    sigset_t *set = arg;
    int sig;

    for (;;) {
        if (sigwait(set, &sig) != 0) continue;
        if (sig == SIGCHLD) {
            if (atomic_load(&shutting_down))
                continue;                        /* 정상 종료 중이면 무시 */
            fprintf(stderr, "\n[%s] 서버 프로세스가 먼저 끝났습니다. 정리 후 종료합니다.\n", TP->name);
            do_cleanup();
            _exit(1);
        }
        atomic_store(&shutting_down, 1);
        fprintf(stderr, "\n[%s] 신호 %d(%s) 받음: 서버 종료, IPC 자원 정리 후 끝냅니다.\n",
                TP->name, sig, strsignal(sig));
        if (server_pid > 0) {
            kill(server_pid, SIGTERM);
            waitpid(server_pid, NULL, 0);
        }
        do_cleanup();
        _exit(128 + sig);
    }
    return NULL;
}

/* 워커 하나의 입력과 결과 */
typedef struct {
    int ch;
    size_t size;
    const bench_opts_t *o;
    pthread_barrier_t *bar;
    char *sbuf, *rbuf;
    /* latency */
    uint64_t *rtt_ns;            /* lat_reps개 */
    /* throughput */
    long k;                      /* 반복 1회에 보낼 메시지 수 */
    uint64_t *t_start, *t_end;   /* thr_reps개 */
    uint64_t deadline;           /* 전체 워밍업 끝 시각 */
    int err;
} worker_t;

/*
 * 왕복 지연 측정
 *   t0: 요청(헤더 16B + payload)을 IPC로 내보내기 직전.
 *       shm에서는 링 버퍼 빈 칸을 기다리는 P 연산과 복사가 이 뒤에 있으므로 포함된다.
 *   t1: 서버가 같은 크기로 돌려준 응답을 클라이언트 버퍼로 끝까지 읽은 직후.
 *   => t1 - t0 = 보내기 + 서버 수신/복사/응답 + 받기. 출력이나 검증은 구간 밖.
 */
static void *lat_worker(void *arg)
{
    worker_t *w = arg;
    const bench_opts_t *o = w->o;
    size_t len = HDR_SIZE + w->size;
    msg_hdr_t *h = (msg_hdr_t *)w->sbuf;
    int total = o->warmup + o->lat_reps;

    h->op = OP_PING;
    h->client = (uint32_t)w->ch;
    h->size = w->size;

    pthread_barrier_wait(w->bar);
    for (int i = 0; i < total; i++) {
        uint64_t t0 = now_ns();                                   /* (t0) */
        if (TP->cli_send(w->ch, w->sbuf, len) != 0 ||
            TP->cli_recv(w->ch, w->rbuf, len) != 0) {
            w->err = 1;
            break;
        }
        uint64_t t1 = now_ns();                                   /* (t1) */
        if (i >= o->warmup)
            w->rtt_ns[i - o->warmup] = t1 - t0;                   /* 배열에 저장만 */
    }
    /* 측정이 끝난 뒤 마지막 응답이 내 채널의 것이고 내용이 요청과 같은지 확인 */
    if (!w->err) {
        const msg_hdr_t *rh = (const msg_hdr_t *)w->rbuf;
        if (rh->op != OP_PING || rh->client != (uint32_t)w->ch || rh->size != w->size ||
            memcmp(w->sbuf + HDR_SIZE, w->rbuf + HDR_SIZE, w->size) != 0)
            w->err = 2;
    }
    return NULL;
}

/*
 * 전체 워밍업 (기록 안 함)
 * 첫 조건만 프로세스 시작 직후의 상태(캐시, 커널 pipe 페이지, CPU 클럭)에서
 * 측정되지 않도록, 모든 채널에서 측정할 크기들을 돌아가며 정해진 시간 동안 왕복한다.
 */
static void *warm_worker(void *arg)
{
    worker_t *w = arg;
    const bench_opts_t *o = w->o;
    msg_hdr_t *h = (msg_hdr_t *)w->sbuf;

    h->op = OP_PING;
    h->client = (uint32_t)w->ch;
    for (int i = 0; now_ns() < w->deadline; i++) {
        size_t size = o->sizes[i % o->nsizes];
        h->size = size;
        if (TP->cli_send(w->ch, w->sbuf, HDR_SIZE + size) != 0 ||
            TP->cli_recv(w->ch, w->rbuf, HDR_SIZE + size) != 0) {
            w->err = 1;
            break;
        }
    }
    return NULL;
}

/*
 * 처리량 측정 (반복 1회)
 *   t_start: 모든 워커가 barrier를 통과한 직후, 첫 메시지를 보내기 직전.
 *   t_end  : k개를 보내고, 마지막(STREAM_ACK)에 대한 서버 응답을 받은 직후.
 *            서버가 k개를 모두 읽었다는 것이 확인된 시점이다.
 *   main에서 반복마다 min(t_start) ~ max(t_end)를 전체 소요 시간으로 쓴다.
 */
static void *thr_worker(void *arg)
{
    worker_t *w = arg;
    const bench_opts_t *o = w->o;
    size_t len = HDR_SIZE + w->size;
    msg_hdr_t *h = (msg_hdr_t *)w->sbuf;
    int total = o->thr_warmup + o->thr_reps;

    h->client = (uint32_t)w->ch;
    h->size = w->size;

    for (int r = 0; r < total; r++) {
        pthread_barrier_wait(w->bar);
        uint64_t t0 = now_ns();                                   /* (t_start) */
        h->op = OP_STREAM;
        for (long j = 0; j < w->k - 1; j++)
            if (TP->cli_send(w->ch, w->sbuf, len) != 0) { w->err = 1; return NULL; }
        h->op = OP_STREAM_ACK;
        if (TP->cli_send(w->ch, w->sbuf, len) != 0 ||
            TP->cli_recv(w->ch, w->rbuf, HDR_SIZE) != 0) { w->err = 1; return NULL; }
        uint64_t t1 = now_ns();                                   /* (t_end) */
        if (r >= o->thr_warmup) {
            w->t_start[r - o->thr_warmup] = t0;
            w->t_end[r - o->thr_warmup] = t1;
        }
    }
    return NULL;
}

/* 채널 0으로 서버 자원 사용량을 묻는다 (워커가 없을 때만 호출) */
static int query_server(srv_report_t *rep)
{
    char buf[HDR_SIZE + sizeof(srv_report_t)];
    msg_hdr_t h = { OP_REPORT, 0, 0 };

    if (TP->cli_send(0, &h, HDR_SIZE) != 0) return -1;
    if (TP->cli_recv(0, buf, sizeof(buf)) != 0) return -1;
    memcpy(rep, buf + HDR_SIZE, sizeof(*rep));
    return 0;
}

static void fatal(const char *msg)
{
    fprintf(stderr, "[%s] %s\n", TP->name, msg);
    atomic_store(&shutting_down, 1);
    if (server_pid > 0) {
        kill(server_pid, SIGTERM);
        waitpid(server_pid, NULL, 0);
    }
    do_cleanup();
    exit(1);
}

/* 한 조건(크기, 클라이언트 수, 모드)을 측정하고 결과를 쓴다 */
static void run_condition(const bench_opts_t *o, size_t size, int n, int is_lat,
                          worker_t *ws, FILE *sum, FILE *raw)
{
    pthread_t tids[MAX_CLIENTS];
    pthread_barrier_t bar;
    result_row_t row;
    long k = thr_msgs_for(o, size);

    memset(&row, 0, sizeof(row));
    row.ipc = TP->name;
    row.mode = is_lat ? "latency" : "throughput";
    row.size = size;
    row.clients = n;
    row.warmup = is_lat ? o->warmup : o->thr_warmup;
    row.reps = is_lat ? o->lat_reps : o->thr_reps;
    row.msgs_per_rep = is_lat ? 1 : k;

    pthread_barrier_init(&bar, NULL, (unsigned)n);
    for (int i = 0; i < n; i++) {
        ws[i].size = size;
        ws[i].bar = &bar;
        ws[i].k = k;
        ws[i].err = 0;
    }

    /* 자원 사용량 시작점: 서버 먼저 묻고, 그다음 클라이언트 */
    if (query_server(&row.srv0) != 0) fatal("서버 응답 없음 (REPORT)");
    getrusage(RUSAGE_SELF, &row.cli_ru0);

    for (int i = 0; i < n; i++)
        pthread_create(&tids[i], NULL, is_lat ? lat_worker : thr_worker, &ws[i]);
    for (int i = 0; i < n; i++)
        pthread_join(tids[i], NULL);

    getrusage(RUSAGE_SELF, &row.cli_ru1);
    row.cli_vmrss_kb = read_vmrss_kb();
    if (query_server(&row.srv1) != 0) fatal("서버 응답 없음 (REPORT)");
    pthread_barrier_destroy(&bar);

    for (int i = 0; i < n; i++) {
        if (ws[i].err == 2) fatal("응답 헤더나 내용이 요청과 다릅니다");
        if (ws[i].err) fatal("측정 중 IPC 오류");
    }
    if (row.srv1.bad_payload != 0) fatal("서버가 받은 payload가 약속한 값과 다릅니다");

    /* ---- 여기부터 측정 구간 밖: 통계, 출력, 파일 기록 ---- */
    if (is_lat) {
        size_t cnt = (size_t)n * (size_t)o->lat_reps, idx = 0;
        double *v = malloc(cnt * sizeof(double));
        for (int i = 0; i < n; i++)
            for (int j = 0; j < o->lat_reps; j++) {
                v[idx++] = ws[i].rtt_ns[j] / 1000.0;
                fprintf(raw, "%s,%zu,%d,%d,%d,%llu\n", TP->name, size, n, i, j,
                        (unsigned long long)ws[i].rtt_ns[j]);
            }
        stats_compute(v, cnt, &row.st);
        /* 지연 모드의 msgs/s는 참고용: 스레드당 평균 왕복 시간으로 환산한 전체 왕복 수/초 */
        row.msgs_per_sec_mean = n * 1e6 / row.st.mean;
        row.msgs_per_sec_median = n * 1e6 / row.st.median;
        row.bytes_per_sec_mean = row.msgs_per_sec_mean * (double)size;
        row.bytes_per_sec_median = row.msgs_per_sec_median * (double)size;
        free(v);
        fflush(raw);
    } else {
        double *wall = malloc(o->thr_reps * sizeof(double));
        double *mps = malloc(o->thr_reps * sizeof(double));
        stats_t s2;
        for (int r = 0; r < o->thr_reps; r++) {
            uint64_t st = ws[0].t_start[r], en = ws[0].t_end[r];
            for (int i = 1; i < n; i++) {
                if (ws[i].t_start[r] < st) st = ws[i].t_start[r];
                if (ws[i].t_end[r] > en) en = ws[i].t_end[r];
            }
            wall[r] = (en - st) / 1000.0;                         /* us */
            mps[r] = (double)n * (double)k / ((en - st) / 1e9);   /* 전체 메시지/초 */
        }
        stats_compute(wall, o->thr_reps, &row.st);
        stats_compute(mps, o->thr_reps, &s2);
        row.msgs_per_sec_mean = s2.mean;
        row.msgs_per_sec_median = s2.median;
        row.bytes_per_sec_mean = s2.mean * (double)size;          /* payload 기준 */
        row.bytes_per_sec_median = s2.median * (double)size;
        free(wall);
        free(mps);
    }
    csv_write_row(sum, &row);
    print_row(&row);
}

int bench_main(const transport_t *tp, int argc, char **argv)
{
    bench_opts_t o;
    sigset_t set;
    pthread_t sig_tid;
    size_t max_msg;
    worker_t ws[MAX_CLIENTS];
    FILE *sum, *raw;

    TP = tp;
    opts_default(&o);
    if (opts_parse(&o, argc, argv, argv[0]) != 0)
        return 2;
    max_msg = opts_max_size(&o);

    /* 1) 신호를 먼저 막는다. 이후 fork/pthread_create로 생기는 모두가 물려받는다. */
    block_signals(&set);

    /* 2) IPC 채널 생성 */
    if (tp->setup(&o, max_msg) != 0) {
        do_cleanup();
        return 1;
    }

    /* 3) 서버 프로세스 */
    server_pid = fork();
    if (server_pid < 0) { perror("fork"); do_cleanup(); return 1; }
    if (server_pid == 0) {
        atomic_store(&cleaned, 1);   /* 자식은 정리하지 않는다 */
        server_main(max_msg);
    }
    tp->after_fork_client();

    /* 4) 클라이언트 신호 스레드 */
    pthread_create(&sig_tid, NULL, client_signal_thread, &set);

    /* 5) 워커 버퍼와 결과 배열은 미리 할당 (측정 중 malloc 없음) */
    for (int i = 0; i < MAX_CLIENTS; i++) {
        memset(&ws[i], 0, sizeof(ws[i]));
        ws[i].ch = i;
        ws[i].o = &o;
        ws[i].sbuf = xmalloc_touch(HDR_SIZE + max_msg);
        ws[i].rbuf = xmalloc_touch(HDR_SIZE + max_msg);
        ws[i].rtt_ns = calloc((size_t)o.lat_reps, sizeof(uint64_t));
        ws[i].t_start = calloc((size_t)o.thr_reps, sizeof(uint64_t));
        ws[i].t_end = calloc((size_t)o.thr_reps, sizeof(uint64_t));
        for (size_t b = 0; b < max_msg; b++)
            ws[i].sbuf[HDR_SIZE + b] = pattern_byte(i, b);
    }

    sum = csv_open_summary(o.outdir, tp->name);
    raw = csv_open_raw(o.outdir, tp->name);
    printf("[%s] 서버 pid=%d, 클라이언트 pid=%d, 결과 폴더 %s\n",
           tp->name, (int)server_pid, (int)getpid(), o.outdir);
    fflush(stdout);

    /* 6) 전체 워밍업: 8개 채널 모두 */
    if (o.global_warmup_ms > 0) {
        pthread_t wt[MAX_CLIENTS];
        uint64_t dl = now_ns() + (uint64_t)o.global_warmup_ms * 1000000ull;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            ws[i].deadline = dl;
            ws[i].err = 0;
            pthread_create(&wt[i], NULL, warm_worker, &ws[i]);
        }
        for (int i = 0; i < MAX_CLIENTS; i++)
            pthread_join(wt[i], NULL);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (ws[i].err) fatal("워밍업 중 IPC 오류");
    }

    /* 7) 조건 반복: 동시 클라이언트 수 x 메시지 크기 */
    for (int ci = 0; ci < o.nclients; ci++)
        for (int si = 0; si < o.nsizes; si++) {
            if (o.do_lat) run_condition(&o, o.sizes[si], o.clients[ci], 1, ws, sum, raw);
            if (o.do_thr) run_condition(&o, o.sizes[si], o.clients[ci], 0, ws, sum, raw);
        }

    fclose(sum);
    fclose(raw);

    /* 8) 정상 종료: 서버 스레드 8개에 QUIT, 서버 회수, 자원 정리 */
    atomic_store(&shutting_down, 1);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        msg_hdr_t h = { OP_QUIT, (uint32_t)i, 0 };
        tp->cli_send(i, &h, HDR_SIZE);
    }
    waitpid(server_pid, NULL, 0);
    do_cleanup();
    printf("[%s] 완료. IPC 자원 정리함.\n", tp->name);

    for (int i = 0; i < MAX_CLIENTS; i++) {
        free(ws[i].sbuf); free(ws[i].rbuf);
        free(ws[i].rtt_ns); free(ws[i].t_start); free(ws[i].t_end);
    }
    return 0;
}

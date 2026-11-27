/*
 * server.c -- 오디오 청크 분석 서버 (7조, 기획서 6장)
 *
 * ipc_pipe.c 또는 ipc_shm.c와 링크해 server_pipe / server_shm이 된다(8.1절).
 * 이 파일은 IPC 방식을 모른다. ipc.h 함수만 부른다.
 *
 * 스레드 (6.2절)
 *   main         : 준비, join, 결과 저장, 정리
 *   시그널   1개 : sigwait. SIGINT/SIGTERM이면 server_stop()
 *   수신     1개 : server_recv() -> 작업 큐
 *   워커     W개 : 큐 -> 분석 -> 통계 갱신 -> server_send() -> free
 *   통계 보고 1개: 일정 간격으로 전체 통계를 읽어 메모리에 남김(측정 중 출력 없음)
 *
 * 서버 쪽 구간 측정 (7.1절): 워커가 청크마다 큐 대기, 처리, 락 대기, 응답 송신 시간을
 * 자기 배열에 쌓고, 종료한 뒤 main이 CSV로 쓴다.
 *
 * 종료 (8.4절, 정상 종료와 Ctrl+C가 같은 순서)
 *   server_stop -> server_recv가 -1 -> 수신 스레드가 wq_close -> 워커가 큐를 비우고 끝
 *   -> 보고 스레드 끝 -> join -> CSV 저장 -> server_close
 *
 * rusage와 VmHWM은 프로세스 전체 값이라, 측정 스크립트는 조건마다 서버를 새로 띄운다.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/resource.h>

#include "ipc.h"
#include "audio.h"
#include "stats.h"
#include "queue.h"
#include "timing.h"
#include "util.h"
#include "csv.h"

#ifndef IPC_NAME
#define IPC_NAME "unknown"
#endif

#define MAX_WORKERS      64
#define MAX_SAMPLES      (1u << 20)   /* 워커당 구간 기록 상한. 넘으면 세기만 함 */
#define MAX_REPORTS      100000

/* ---- 옵션 ---- */
static struct {
    int    workers;      /* -t */
    size_t qcap;         /* -q */
    int    merge_k;      /* -K */
    int    client_lock;  /* -L per|table (E1) */
    int    global_lock;  /* -G rwlock|mutex (E2) */
    int    report_ms;    /* -I */
    const char *outdir;  /* -o */
    const char *label;   /* -l: 측정 스크립트가 붙이는 조건 이름 */
} opt = { 4, 16, 8, STATS_LOCK_PER_CLIENT, STATS_GLOCK_RWLOCK, 100, NULL, "run" };

/* ---- 워커별 구간 기록 ---- */
typedef struct {
    uint32_t client, seq, size, mode;
    uint64_t qwait_ns;   /* 큐 삽입 ~ 꺼내기 */
    uint64_t proc_ns;    /* 분석(+정규화) 시작 ~ 끝 */
    uint64_t lk_cli_ns, lk_grd_ns, lk_gwr_ns;
    uint64_t send_ns;    /* server_send 호출 ~ 반환 */
} sample_t;

typedef struct {
    pthread_t th;
    int       id;
    sample_t *s;
    size_t    n, cap;
    uint64_t  dropped;   /* MAX_SAMPLES를 넘어 기록 못 한 수 */
    uint64_t  send_fail;
} worker_t;

static worker_t workers[MAX_WORKERS];

/* ---- 통계 보고 스레드 기록 ---- */
typedef struct {
    uint64_t t_ns;
    uint64_t chunks;     /* 전체 통계에 반영된 청크 수 */
    double   mean_rms;
    uint64_t cli_chunks; /* 클라이언트별 표 합계 */
} report_t;

static report_t        *reports;
static size_t           n_reports;
static pthread_mutex_t  rep_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t   rep_cv;
static int              rep_stop;

/* ---- 그 밖의 공유 상태 ---- */
static work_queue_t  wq;
static ipc_server_t *srv;
static atomic_int    stopping;
static atomic_ulong  n_bad;      /* 잘못된 청크(번호, 크기) */

/* ------------------------------------------------------------------ */

static void *signal_main(void *arg)
{
    sigset_t *set = arg;
    int sig;

    for (;;) {
        if (sigwait(set, &sig) != 0) continue;
        if (sig == SIGUSR1)
            return NULL;
        atomic_store(&stopping, 1);
        server_stop(srv);
        return NULL;
    }
}

static void *recv_main(void *arg)
{
    chunk_t c;
    int r;

    (void)arg;
    while ((r = server_recv(srv, &c)) >= 0) {
        if (r == 0)
            continue;
        if (c.hdr.client >= APP_MAX_CLIENTS || c.hdr.size > APP_MAX_CHUNK ||
            c.hdr.size % sizeof(int16_t) != 0) {
            atomic_fetch_add(&n_bad, 1);
            free(c.data);
            continue;
        }
        if (wq_push(&wq, &c) < 0) {
            free(c.data);
            break;
        }
    }
    wq_close(&wq);
    return NULL;
}

static void record(worker_t *w, const sample_t *s)
{
    if (w->n == w->cap) {
        size_t ncap = w->cap ? w->cap * 2 : 4096;
        sample_t *p;
        if (w->cap >= MAX_SAMPLES || !(p = realloc(w->s, ncap * sizeof(sample_t)))) {
            w->dropped++;
            return;
        }
        w->s = p;
        w->cap = ncap;
    }
    w->s[w->n++] = *s;
}

static void *worker_main(void *arg)
{
    worker_t *w = arg;
    stats_pending_t pend = { 0, 0 };
    chunk_t c;
    uint64_t qwait;

    while (wq_pop(&wq, &c, &qwait) == 1) {
        int16_t *pcm = (int16_t *)c.data;
        size_t ns = c.hdr.size / sizeof(int16_t);
        lockwait_t lw = { 0, 0, 0 };
        audio_stats_t a;
        double mean_rms;
        app_hdr_t h;
        rsp_stats_t rs;
        const void *payload;
        uint64_t t0, t1, t2;
        sample_t s;

        t0 = now_ns();
        audio_analyze(pcm, ns, &a);
        t1 = now_ns();

        mean_rms = stats_global_mean(&lw);                     /* 전체 읽기 1회 */
        stats_client_add(c.hdr.client, c.hdr.size, &a, &lw);    /* 클라이언트별 쓰기 1회 */
        stats_global_add(&pend, a.rms, &lw);                    /* 전체 쓰기 1/K회 */

        h = c.hdr;
        if (c.hdr.mode == MODE_NORM) {
            t2 = now_ns();
            audio_normalize(pcm, ns, a.peak);   /* 받은 버퍼를 제자리에서 고쳐 돌려준다 */
            t1 += now_ns() - t2;                /* 처리 시간에 더함 */
            h.type = MSG_RSP_AUDIO;
            payload = pcm;
        } else {
            memset(&rs, 0, sizeof(rs));
            rs.rms = a.rms;
            rs.peak = a.peak;
            rs.silent_runs = a.silent_runs;
            rs.rms_ratio = mean_rms > 0 ? a.rms / mean_rms : 1.0;
            h.type = MSG_RSP_STATS;
            h.size = sizeof(rs);
            payload = &rs;
        }

        t2 = now_ns();
        if (server_send(srv, &h, payload) < 0)
            w->send_fail++;
        s.send_ns = now_ns() - t2;

        s.client = c.hdr.client;
        s.seq = c.hdr.seq;
        s.size = (uint32_t)c.hdr.size;
        s.mode = c.hdr.mode;
        s.qwait_ns = qwait;
        s.proc_ns = t1 - t0;
        s.lk_cli_ns = lw.cli_ns;
        s.lk_grd_ns = lw.grd_ns;
        s.lk_gwr_ns = lw.gwr_ns;
        record(w, &s);
        free(c.data);
    }
    stats_global_flush(&pend, NULL);
    return NULL;
}

static void *report_main(void *arg)
{
    struct timespec dl;

    (void)arg;
    clock_gettime(CLOCK_MONOTONIC, &dl);
    pthread_mutex_lock(&rep_mu);
    while (!rep_stop) {
        report_t r;
        client_stat_t cs;

        dl.tv_nsec += (long)opt.report_ms * 1000000L;
        dl.tv_sec += dl.tv_nsec / 1000000000L;
        dl.tv_nsec %= 1000000000L;
        while (!rep_stop && pthread_cond_timedwait(&rep_cv, &rep_mu, &dl) == 0)
            ;
        if (rep_stop) break;
        pthread_mutex_unlock(&rep_mu);

        /* 락을 겹쳐 잡지 않는다: 클라이언트별 하나씩, 그다음 전체 */
        r.t_ns = now_ns();
        r.cli_chunks = 0;
        for (uint32_t i = 0; i < APP_MAX_CLIENTS; i++) {
            stats_client_get(i, &cs);
            r.cli_chunks += cs.chunks;
        }
        {
            double sum;
            stats_global_get(&sum, &r.chunks);
            r.mean_rms = r.chunks ? sum / (double)r.chunks : 0;
        }
        if (n_reports < MAX_REPORTS)
            reports[n_reports++] = r;

        pthread_mutex_lock(&rep_mu);
    }
    pthread_mutex_unlock(&rep_mu);
    return NULL;
}

/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
        "사용법: %s [옵션]\n"
        "  -t N      워커 스레드 수 (기본 4, 최대 %d)\n"
        "  -q N      작업 큐 칸 수 (기본 16)\n"
        "  -K N      전체 통계 쓰기 주기, 청크 N개마다 1번 (기본 8)\n"
        "  -L MODE   클라이언트별 통계 락: per | table (기본 per, E1)\n"
        "  -G MODE   전체 통계 락: rwlock | mutex (기본 rwlock, E2)\n"
        "  -I MS     통계 보고 간격 (기본 100)\n"
        "  -o DIR    결과 폴더 (없으면 화면 요약만)\n"
        "  -l LABEL  결과 파일과 요약 줄에 붙일 조건 이름 (기본 run)\n",
        prog, MAX_WORKERS);
}

static int parse_opts(int argc, char **argv)
{
    int c;
    while ((c = getopt(argc, argv, "t:q:K:L:G:I:o:l:h")) != -1) {
        switch (c) {
        case 't': opt.workers = atoi(optarg); break;
        case 'q': opt.qcap = (size_t)atol(optarg); break;
        case 'K': opt.merge_k = atoi(optarg); break;
        case 'L':
            if (strcmp(optarg, "per") == 0) opt.client_lock = STATS_LOCK_PER_CLIENT;
            else if (strcmp(optarg, "table") == 0) opt.client_lock = STATS_LOCK_TABLE;
            else { usage(argv[0]); return -1; }
            break;
        case 'G':
            if (strcmp(optarg, "rwlock") == 0) opt.global_lock = STATS_GLOCK_RWLOCK;
            else if (strcmp(optarg, "mutex") == 0) opt.global_lock = STATS_GLOCK_MUTEX;
            else { usage(argv[0]); return -1; }
            break;
        case 'I': opt.report_ms = atoi(optarg); break;
        case 'o': opt.outdir = optarg; break;
        case 'l': opt.label = optarg; break;
        default: usage(argv[0]); return -1;
        }
    }
    if (opt.workers < 1 || opt.workers > MAX_WORKERS || opt.qcap < 1 ||
        opt.merge_k < 1 || opt.report_ms < 1) {
        usage(argv[0]);
        return -1;
    }
    return 0;
}

static void write_samples(void)
{
    char path[1024];
    FILE *f;

    snprintf(path, sizeof(path), "%s/server_%s_%s_chunks.csv", opt.outdir, IPC_NAME, opt.label);
    f = csv_open(path, "worker,client,seq,size,mode,qwait_ns,proc_ns,"
                       "lock_cli_ns,lock_grd_ns,lock_gwr_ns,send_ns", 0);
    if (!f) return;
    for (int i = 0; i < opt.workers; i++) {
        worker_t *w = &workers[i];
        for (size_t j = 0; j < w->n; j++) {
            sample_t *s = &w->s[j];
            fprintf(f, "%d,%u,%u,%u,%u,%lu,%lu,%lu,%lu,%lu,%lu\n", i, s->client, s->seq,
                    s->size, s->mode, (unsigned long)s->qwait_ns, (unsigned long)s->proc_ns,
                    (unsigned long)s->lk_cli_ns, (unsigned long)s->lk_grd_ns,
                    (unsigned long)s->lk_gwr_ns, (unsigned long)s->send_ns);
        }
    }
    csv_close(f);

    snprintf(path, sizeof(path), "%s/server_%s_%s_report.csv", opt.outdir, IPC_NAME, opt.label);
    f = csv_open(path, "t_ms,global_chunks,mean_rms,client_chunks", 0);
    if (!f) return;
    for (size_t i = 0; i < n_reports; i++)
        fprintf(f, "%.3f,%lu,%.6f,%lu\n", (reports[i].t_ns - reports[0].t_ns) / 1e6,
                (unsigned long)reports[i].chunks, reports[i].mean_rms,
                (unsigned long)reports[i].cli_chunks);
    csv_close(f);
}

static void report_summary(const struct rusage *ru0, const struct rusage *ru1)
{
    uint64_t chunks = 0, bytes = 0, dropped = 0, send_fail = 0;
    long rss = read_status_kb("VmRSS:"), hwm = read_status_kb("VmHWM:");
    client_stat_t cs;
    double gsum;
    uint64_t gn;

    for (uint32_t i = 0; i < APP_MAX_CLIENTS; i++) {
        stats_client_get(i, &cs);
        if (cs.chunks == 0) continue;
        chunks += cs.chunks;
        bytes += cs.bytes;
        printf("  client %u: chunks=%lu bytes=%lu mean_rms=%.4f max_peak=%.4f silent_runs=%lu\n",
               i, (unsigned long)cs.chunks, (unsigned long)cs.bytes,
               cs.sum_rms / (double)cs.chunks, cs.max_peak, (unsigned long)cs.silent_runs);
    }
    for (int i = 0; i < opt.workers; i++) {
        dropped += workers[i].dropped;
        send_fail += workers[i].send_fail;
    }
    stats_global_get(&gsum, &gn);

    /* 경쟁 상태 검사(6.3절): 전체 통계 청크 수 = 클라이언트별 합계. 클라이언트가 보낸 수와의
     * 비교는 측정 스크립트가 client_summary.csv의 sent_chunks, sent_bytes와 맞춰 본다 */
    printf("server[%s] %s: chunks=%lu bytes=%lu global=%lu%s bad=%lu send_fail=%lu dropped=%lu\n",
           IPC_NAME, opt.label, (unsigned long)chunks, (unsigned long)bytes, (unsigned long)gn,
           gn == chunks ? "" : " (불일치!)", (unsigned long)atomic_load(&n_bad),
           (unsigned long)send_fail, (unsigned long)dropped);
    printf("  utime=%.0fus stime=%.0fus nvcsw=%ld nivcsw=%ld VmRSS=%ldkB VmHWM=%ldkB\n",
           tv_us(ru0->ru_utime, ru1->ru_utime), tv_us(ru0->ru_stime, ru1->ru_stime),
           ru1->ru_nvcsw - ru0->ru_nvcsw, ru1->ru_nivcsw - ru0->ru_nivcsw, rss, hwm);
    fflush(stdout);

    if (opt.outdir) {
        char path[1024];
        FILE *f;
        snprintf(path, sizeof(path), "%s/server_summary.csv", opt.outdir);
        f = csv_open(path, "ipc,label,workers,qcap,K,client_lock,global_lock,"
                           "chunks,bytes,global_chunks,bad,send_fail,dropped,"
                           "utime_us,stime_us,nvcsw,nivcsw,vmrss_kb,vmhwm_kb", 1);
        if (!f) return;
        fprintf(f, "%s,%s,%d,%zu,%d,%s,%s,%lu,%lu,%lu,%lu,%lu,%lu,%.0f,%.0f,%ld,%ld,%ld,%ld\n",
                IPC_NAME, opt.label, opt.workers, opt.qcap, opt.merge_k,
                opt.client_lock == STATS_LOCK_TABLE ? "table" : "per",
                opt.global_lock == STATS_GLOCK_MUTEX ? "mutex" : "rwlock",
                (unsigned long)chunks, (unsigned long)bytes, (unsigned long)gn,
                (unsigned long)atomic_load(&n_bad), (unsigned long)send_fail,
                (unsigned long)dropped,
                tv_us(ru0->ru_utime, ru1->ru_utime), tv_us(ru0->ru_stime, ru1->ru_stime),
                ru1->ru_nvcsw - ru0->ru_nvcsw, ru1->ru_nivcsw - ru0->ru_nivcsw, rss, hwm);
        csv_close(f);
    }
}

int main(int argc, char **argv)
{
    sigset_t set;
    pthread_t sig_th, recv_th, rep_th;
    pthread_condattr_t ca;
    struct rusage ru0, ru1;

    if (parse_opts(argc, argv) < 0)
        return 2;
    if (opt.outdir && ensure_dir(opt.outdir) < 0)
        return 1;

    block_signals(&set);               /* 스레드 만들기 전에 (8.4절 2번) */

    stats_init(opt.client_lock, opt.global_lock, opt.merge_k);
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&rep_cv, &ca);
    pthread_condattr_destroy(&ca);
    reports = xmalloc(MAX_REPORTS * sizeof(report_t));
    if (wq_init(&wq, opt.qcap) < 0) {
        fprintf(stderr, "server: 큐 할당 실패\n");
        return 1;
    }
    srv = server_open();
    if (!srv) {
        fprintf(stderr, "server: server_open 실패\n");
        wq_destroy(&wq);
        return 1;
    }

    getrusage(RUSAGE_SELF, &ru0);
    pthread_create(&sig_th, NULL, signal_main, &set);
    pthread_create(&rep_th, NULL, report_main, NULL);
    for (int i = 0; i < opt.workers; i++) {
        workers[i].id = i;
        pthread_create(&workers[i].th, NULL, worker_main, &workers[i]);
    }
    pthread_create(&recv_th, NULL, recv_main, NULL);
    fprintf(stderr, "server[%s]: 준비됨 (Ctrl+C로 종료)\n", IPC_NAME);

    pthread_join(recv_th, NULL);
    if (!atomic_load(&stopping))       /* 오류로 끝났으면 시그널 스레드를 직접 깨운다 */
        pthread_kill(sig_th, SIGUSR1);
    for (int i = 0; i < opt.workers; i++)
        pthread_join(workers[i].th, NULL);
    pthread_mutex_lock(&rep_mu);
    rep_stop = 1;
    pthread_cond_signal(&rep_cv);
    pthread_mutex_unlock(&rep_mu);
    pthread_join(rep_th, NULL);
    pthread_join(sig_th, NULL);
    getrusage(RUSAGE_SELF, &ru1);

    report_summary(&ru0, &ru1);
    if (opt.outdir)
        write_samples();

    server_close(srv);
    wq_destroy(&wq);
    stats_destroy();
    for (int i = 0; i < opt.workers; i++)
        free(workers[i].s);
    free(reports);
    return 0;
}

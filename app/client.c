/*
 * client.c -- 오디오 청크를 보내고 응답 지연을 재는 클라이언트 (7조, 기획서 6장)
 *
 * ipc_pipe.c 또는 ipc_shm.c와 링크해 client_pipe / client_shm이 된다.
 * 동시 클라이언트 수는 측정 스크립트가 이 프로그램을 여러 개 띄워 만든다.
 *
 * 스레드 (6.2절)
 *   main : 접속, 신호 표 만들기, join, 결과 계산과 저장, client_close
 *   생성 : 빈 버퍼를 받아 PCM 청크를 채워 내부 큐(ready)에 넣음
 *   송신 : 창(window)에 자리가 나면 송신 시각을 찍고 client_send(), 버퍼는 빈 큐(free)로
 *   수신 : client_recv() -> 시각 -> 순번으로 왕복 시간 기록 -> 검증 -> 창 자리 반납
 *
 * 요청 지연(7.1절): 송신 스레드의 송신 직전 ~ 수신 스레드가 응답을 다 받은 직후.
 * 응답 검증은 타임스탬프를 찍은 뒤에 한다. 결과는 배열에 쌓았다가 끝난 뒤 저장한다.
 *
 * 창 크기 W(기본 1 = closed-loop, 5.1절). 나가 있는 요청은 W개 이하이고 seq가 연속이라
 * seq % W 칸에 송신 시각을 두면 응답 순서가 바뀌어도 겹치지 않는다.
 *
 * PCM은 시작할 때 시드로 5초 분량 표를 만들고, 생성 스레드는 거기서 잘라 복사만 한다.
 * 측정 중에 사인 계산이 CPU를 쓰지 않게 하려는 것이다(6.1절: 디스크 I/O 없이 메모리에서).
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/resource.h>

#include "ipc.h"
#include "audio.h"
#include "queue.h"
#include "timing.h"
#include "util.h"
#include "csv.h"

#ifndef IPC_NAME
#define IPC_NAME "unknown"
#endif

#define MAX_WINDOW 4                    /* = APP_SHM_RSP_RING */
#define NBUF       (MAX_WINDOW + 4)     /* 송신용 버퍼 수: 창 + 생성 스레드가 앞서 둘 몫 */
#define TABLE_SAMPLES (AUDIO_RATE * 5)  /* 5초 = 크기 변화 한 주기 */

/* ---- 옵션 ---- */
static struct {
    size_t size;            /* -s */
    int    mode;            /* -m */
    long   count;           /* -n: 기록하는 청크 수 */
    long   warmup;          /* -w: 버리는 청크 수 */
    int    window;          /* -W */
    int    nclients;        /* -c: 기록용 */
    int    run;             /* -r: 기록용, 30회 중 몇 번째 */
    long   seed;            /* -S: 기본은 서버가 준 번호 + 1 */
    const char *outdir;     /* -o */
} opt = { 16384, MODE_STATS, 1000, 100, 1, 1, 0, -1, NULL };

static ipc_client_t *cli;
static uint32_t client_id;
static int16_t *table;              /* TABLE_SAMPLES개 */

static work_queue_t free_q, ready_q;

static uint64_t send_ns[MAX_WINDOW];
static double  *rtt_us;             /* opt.count개 */
static uint64_t t_meas_start, t_meas_end;
static long     n_sent;
static uint64_t sent_bytes;

static pthread_mutex_t win_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  win_cv = PTHREAD_COND_INITIALIZER;
static int  outstanding;
static int  failed;
static long n_bad_rsp;

/* ------------------------------------------------------------------ */

static void set_failed(void)
{
    pthread_mutex_lock(&win_mu);
    failed = 1;
    pthread_cond_broadcast(&win_cv);
    pthread_mutex_unlock(&win_mu);
    wq_close(&free_q);
    wq_close(&ready_q);
}

static void *gen_main(void *arg)
{
    long total = opt.warmup + opt.count;
    size_t ns = opt.size / sizeof(int16_t);
    size_t pos = 0;
    chunk_t b;

    (void)arg;
    for (long seq = 0; seq < total; seq++) {
        size_t done = 0;

        if (wq_pop(&free_q, &b, NULL) == 0)
            break;
        while (done < ns) {                 /* 표 끝에서 처음으로 돌아가며 복사 */
            size_t k = ns - done;
            if (k > TABLE_SAMPLES - pos) k = TABLE_SAMPLES - pos;
            memcpy((int16_t *)b.data + done, table + pos, k * sizeof(int16_t));
            done += k;
            pos = (pos + k) % TABLE_SAMPLES;
        }
        b.hdr.type = MSG_CHUNK;
        b.hdr.client = client_id;
        b.hdr.seq = (uint32_t)seq;
        b.hdr.mode = (uint32_t)opt.mode;
        b.hdr.size = opt.size;
        if (wq_push(&ready_q, &b) < 0) {
            free(b.data);
            break;
        }
    }
    wq_close(&ready_q);
    return NULL;
}

static void *send_main(void *arg)
{
    chunk_t b;

    (void)arg;
    while (wq_pop(&ready_q, &b, NULL) == 1) {
        uint32_t seq = b.hdr.seq;
        uint64_t t;

        pthread_mutex_lock(&win_mu);
        while (outstanding >= opt.window && !failed)
            pthread_cond_wait(&win_cv, &win_mu);
        if (failed) {
            pthread_mutex_unlock(&win_mu);
            free(b.data);
            return NULL;
        }
        outstanding++;
        pthread_mutex_unlock(&win_mu);

        t = now_ns();
        __atomic_store_n(&send_ns[seq % opt.window], t, __ATOMIC_RELEASE);
        if (seq == (uint32_t)opt.warmup)
            t_meas_start = t;
        if (client_send(cli, &b.hdr, b.data) < 0) {
            fprintf(stderr, "client %u: client_send 실패 (seq %u)\n", client_id, seq);
            free(b.data);
            set_failed();
            return NULL;
        }
        n_sent++;
        sent_bytes += b.hdr.size;
        if (wq_push(&free_q, &b) < 0)
            free(b.data);
    }
    return NULL;
}

static void *recv_main(void *arg)
{
    long total = opt.warmup + opt.count;
    size_t cap = opt.size > sizeof(rsp_stats_t) ? opt.size : sizeof(rsp_stats_t);
    uint8_t *buf = xmalloc_touch(cap);
    uint32_t want_type = opt.mode == MODE_NORM ? MSG_RSP_AUDIO : MSG_RSP_STATS;
    uint64_t want_size = opt.mode == MODE_NORM ? opt.size : sizeof(rsp_stats_t);

    (void)arg;
    for (long i = 0; i < total; i++) {
        app_hdr_t h;
        uint64_t t1, t0;
        int ok;

        if (client_recv(cli, &h, buf, cap) < 0) {
            fprintf(stderr, "client %u: client_recv 실패 (%ld번째 응답)\n", client_id, i);
            set_failed();
            break;
        }
        t1 = now_ns();                      /* 검증보다 먼저 */

        ok = h.seq < (uint64_t)total && h.type == want_type && h.size == want_size &&
             h.client == client_id;
        if (ok) {
            t0 = __atomic_load_n(&send_ns[h.seq % opt.window], __ATOMIC_ACQUIRE);
            if (h.seq >= (uint64_t)opt.warmup)
                rtt_us[h.seq - opt.warmup] = (double)(t1 - t0) / 1e3;
            if (opt.mode == MODE_STATS) {
                const rsp_stats_t *r = (const rsp_stats_t *)buf;
                ok = r->rms >= 0 && r->rms <= 1 && r->peak >= 0 && r->peak <= 1;
            } else {
                audio_stats_t a;
                audio_analyze((const int16_t *)buf, h.size / 2, &a);
                ok = a.peak == 0 || (a.peak > AUDIO_NORM_TARGET - 0.01 &&
                                     a.peak < AUDIO_NORM_TARGET + 0.01);
            }
        }
        if (!ok)
            n_bad_rsp++;
        t_meas_end = t1;

        pthread_mutex_lock(&win_mu);
        outstanding--;
        pthread_cond_signal(&win_cv);
        pthread_mutex_unlock(&win_mu);
    }
    free(buf);
    return NULL;
}

/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
        "사용법: %s [옵션]\n"
        "  -s SIZE   청크 크기: 4K, 16K, 64K, 256K (짝수, 최대 256K, 기본 16K)\n"
        "  -m MODE   stats | norm (기본 stats)\n"
        "  -n N      기록하는 청크 수 (기본 1000)\n"
        "  -w N      워밍업으로 버리는 청크 수 (기본 100)\n"
        "  -W N      창 크기 1..%d (기본 1)\n"
        "  -c N      기록용: 이번 실행의 동시 클라이언트 수 (기본 1)\n"
        "  -r N      기록용: 실행 회차 (기본 0)\n"
        "  -S SEED   PCM 시드 (기본: 서버가 준 클라이언트 번호 + 1)\n"
        "  -o DIR    결과 폴더. 왕복 시간 원자료와 client_summary.csv\n",
        prog, MAX_WINDOW);
}

static size_t parse_size(const char *s)
{
    char *end;
    unsigned long long v = strtoull(s, &end, 10);
    if (*end == 'k' || *end == 'K') { v *= 1024; end++; }
    return *end ? 0 : (size_t)v;
}

static int parse_opts(int argc, char **argv)
{
    int c;
    while ((c = getopt(argc, argv, "s:m:n:w:W:c:r:S:o:h")) != -1) {
        switch (c) {
        case 's': opt.size = parse_size(optarg); break;
        case 'm':
            if (strcmp(optarg, "stats") == 0) opt.mode = MODE_STATS;
            else if (strcmp(optarg, "norm") == 0) opt.mode = MODE_NORM;
            else { usage(argv[0]); return -1; }
            break;
        case 'n': opt.count = atol(optarg); break;
        case 'w': opt.warmup = atol(optarg); break;
        case 'W': opt.window = atoi(optarg); break;
        case 'c': opt.nclients = atoi(optarg); break;
        case 'r': opt.run = atoi(optarg); break;
        case 'S': opt.seed = atol(optarg); break;
        case 'o': opt.outdir = optarg; break;
        default: usage(argv[0]); return -1;
        }
    }
    if (opt.size == 0 || opt.size > APP_MAX_CHUNK || opt.size % 2 || opt.count < 1 ||
        opt.warmup < 0 || opt.window < 1 || opt.window > MAX_WINDOW) {
        usage(argv[0]);
        return -1;
    }
    return 0;
}

static const char *mode_name(void)
{
    return opt.mode == MODE_NORM ? "norm" : "stats";
}

/* 원자료: 정렬 전 값을 남기려고 summarize보다 먼저 부른다 */
static void write_raw(void)
{
    char path[1024];
    FILE *f;

    snprintf(path, sizeof(path), "%s/client_%s_%s_%zu_c%d_r%d_id%u_raw.csv", opt.outdir,
             IPC_NAME, mode_name(), opt.size, opt.nclients, opt.run, client_id);
    if (!(f = csv_open(path, "seq,rtt_us", 0))) return;
    for (long i = 0; i < opt.count; i++)
        fprintf(f, "%ld,%.3f\n", i + opt.warmup, rtt_us[i]);
    csv_close(f);
}

static void write_summary(const summary_t *st, double cps, const struct rusage *ru0,
                          const struct rusage *ru1)
{
    char path[1024];
    FILE *f;

    snprintf(path, sizeof(path), "%s/client_summary.csv", opt.outdir);
    f = csv_open(path, "ipc,mode,size,clients,window,run,client_id,pid,warmup,samples,"
                       "mean_us,median_us,p99_us,max_us,min_us,stddev_us,chunks_per_s,mb_per_s,"
                       "sent_chunks,sent_bytes,bad_rsp,utime_us,stime_us,nvcsw,nivcsw,"
                       "vmrss_kb,vmhwm_kb", 1);
    if (!f) return;
    fprintf(f, "%s,%s,%zu,%d,%d,%d,%u,%d,%ld,%zu,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.1f,%.3f,"
               "%ld,%lu,%ld,%.0f,%.0f,%ld,%ld,%ld,%ld\n",
            IPC_NAME, mode_name(), opt.size, opt.nclients, opt.window, opt.run, client_id,
            (int)getpid(), opt.warmup, st->n, st->mean, st->median, st->p99, st->max, st->min,
            st->stddev, cps, cps * (double)opt.size / (1024.0 * 1024.0),
            n_sent, (unsigned long)sent_bytes, n_bad_rsp,
            tv_us(ru0->ru_utime, ru1->ru_utime), tv_us(ru0->ru_stime, ru1->ru_stime),
            ru1->ru_nvcsw - ru0->ru_nvcsw, ru1->ru_nivcsw - ru0->ru_nivcsw,
            read_status_kb("VmRSS:"), read_status_kb("VmHWM:"));
    csv_close(f);
}

int main(int argc, char **argv)
{
    pthread_t gen, snd, rcv;
    struct rusage ru0, ru1;
    summary_t st;
    double cps = 0;

    if (parse_opts(argc, argv) < 0)
        return 2;
    if (opt.outdir && ensure_dir(opt.outdir) < 0)
        return 1;
    signal(SIGPIPE, SIG_IGN);
    /* TODO(8.4절): 클라이언트 Ctrl+C 처리. 지금은 기본 동작(즉시 종료)이라
     * pipe 버전은 FIFO가 남을 수 있다 -> make cleanipc */

    cli = client_connect(&client_id);
    if (!cli) {
        fprintf(stderr, "client: client_connect 실패 (서버가 떠 있나요?)\n");
        return 1;
    }

    /* 측정 전 준비: 신호 표, 송신 버퍼, 결과 배열 (페이지를 미리 건드림) */
    table = xmalloc_touch(TABLE_SAMPLES * sizeof(int16_t));
    audio_synth(table, TABLE_SAMPLES, 0,
                (uint32_t)(opt.seed >= 0 ? opt.seed : (long)client_id + 1));
    rtt_us = xmalloc_touch((size_t)opt.count * sizeof(double));
    if (wq_init(&free_q, NBUF) < 0 || wq_init(&ready_q, NBUF) < 0) {
        fprintf(stderr, "client: 큐 할당 실패\n");
        return 1;
    }
    for (int i = 0; i < NBUF; i++) {
        chunk_t b = { .data = xmalloc_touch(opt.size) };
        wq_push(&free_q, &b);
    }

    getrusage(RUSAGE_SELF, &ru0);
    pthread_create(&rcv, NULL, recv_main, NULL);
    pthread_create(&gen, NULL, gen_main, NULL);
    pthread_create(&snd, NULL, send_main, NULL);
    pthread_join(gen, NULL);
    pthread_join(snd, NULL);
    /* 송신이 실패해도 수신 스레드가 끝나려면, 서버가 나갔을 때 client_recv가 -1을
     * 돌려줘야 한다. ipc_pipe.c는 확인함(서버 kill -9 -> EOF -> -1). ipc_shm.c도 지킬 것 */
    pthread_join(rcv, NULL);
    getrusage(RUSAGE_SELF, &ru1);

    if (failed || n_bad_rsp > 0) {
        fprintf(stderr, "client %u: 실패로 끝남 (잘못된 응답 %ld개). 결과를 기록하지 않음\n",
                client_id, n_bad_rsp);
        failed = 1;
    } else {
        if (opt.outdir)
            write_raw();
        if (t_meas_end > t_meas_start)
            cps = (double)opt.count / ((double)(t_meas_end - t_meas_start) / 1e9);
        summarize(rtt_us, (size_t)opt.count, &st);
        if (opt.outdir)
            write_summary(&st, cps, &ru0, &ru1);
        printf("client[%s] id=%u %s %zuB c=%d W=%d r=%d  mean=%.1fus median=%.1fus "
               "p99=%.1fus max=%.1fus  %.0f chunk/s  sent=%ld\n",
               IPC_NAME, client_id, mode_name(), opt.size, opt.nclients, opt.window, opt.run,
               st.mean, st.median, st.p99, st.max, cps, n_sent);
    }

    client_close(cli);
    wq_destroy(&free_q);                  /* 남은 버퍼 free */
    wq_destroy(&ready_q);
    free(rtt_us);
    free(table);
    return failed ? 1 : 0;
}

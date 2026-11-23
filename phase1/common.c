/*
 * common.c -- 1단계 IPC 벤치마크 공통 코드 (7조)
 */
#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>
#include <math.h>
#include <sys/stat.h>
#include <pthread.h>

/* ------------------------------------------------------------------ */
/* 옵션                                                                */
/* ------------------------------------------------------------------ */

void opts_default(bench_opts_t *o)
{
    static const size_t def_sizes[] = { 64, 1024, 16384, 262144, 1048576 };
    static const int def_clients[] = { 1, 2, 4, 8 };

    memset(o, 0, sizeof(*o));
    memcpy(o->sizes, def_sizes, sizeof(def_sizes));
    o->nsizes = 5;
    memcpy(o->clients, def_clients, sizeof(def_clients));
    o->nclients = 4;
    o->warmup = 20;
    o->lat_reps = 100;
    o->thr_reps = 30;
    o->thr_warmup = 2;
    o->thr_msgs = 0;
    o->do_lat = 1;
    o->do_thr = 1;
    o->qdepth = 4;
    o->global_warmup_ms = 1000;
    snprintf(o->outdir, sizeof(o->outdir), "results");
}

/* "64,1K,16k,256K,1M" 같은 목록 */
static int parse_size_list(const char *s, size_t *out, int max)
{
    char buf[256], *save = NULL, *tok;
    int n = 0;

    snprintf(buf, sizeof(buf), "%s", s);
    for (tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
        char *end;
        unsigned long long v = strtoull(tok, &end, 10);
        if (*end == 'k' || *end == 'K') { v *= 1024; end++; }
        else if (*end == 'm' || *end == 'M') { v *= 1024 * 1024; end++; }
        if (*end != '\0' || v == 0 || n >= max)
            return -1;
        out[n++] = (size_t)v;
    }
    return n;
}

static int parse_int_list(const char *s, int *out, int max, int lo, int hi)
{
    char buf[256], *save = NULL, *tok;
    int n = 0;

    snprintf(buf, sizeof(buf), "%s", s);
    for (tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
        char *end;
        long v = strtol(tok, &end, 10);
        if (*end != '\0' || v < lo || v > hi || n >= max)
            return -1;
        out[n++] = (int)v;
    }
    return n;
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "사용법: %s [옵션]\n"
        "  -s 64,1K,16K,256K,1M  메시지 크기 목록\n"
        "  -c 1,2,4,8            동시 클라이언트 수 목록 (최대 %d)\n"
        "  -w N    지연 측정 워밍업 왕복 수 (스레드당, 기본 20)\n"
        "  -r N    지연 측정 기록 왕복 수 (스레드당, 기본 100, 최소 30)\n"
        "  -t N    처리량 측정 반복 수 (기본 30, 최소 30)\n"
        "  -k N    처리량 1회에 스레드당 보낼 메시지 수 (기본: 크기에 따라 자동)\n"
        "  -m lat|thr|both  측정 종류 (기본 both)\n"
        "  -q N    shm 링 버퍼 칸 수 (기본 4, shm_bench만)\n"
        "  -W MS   첫 조건 전 전체 워밍업 시간 ms (기본 1000, 0이면 생략)\n"
        "  -o DIR  결과 폴더 (기본 results)\n",
        prog, MAX_CLIENTS);
}

int opts_parse(bench_opts_t *o, int argc, char **argv, const char *prog)
{
    int c;

    while ((c = getopt(argc, argv, "s:c:w:r:t:k:m:q:W:o:h")) != -1) {
        switch (c) {
        case 's':
            o->nsizes = parse_size_list(optarg, o->sizes, MAX_SIZES);
            if (o->nsizes <= 0) { fprintf(stderr, "잘못된 -s\n"); return -1; }
            break;
        case 'c':
            o->nclients = parse_int_list(optarg, o->clients, MAX_CLIENT_SET, 1, MAX_CLIENTS);
            if (o->nclients <= 0) { fprintf(stderr, "잘못된 -c\n"); return -1; }
            break;
        case 'w': o->warmup = atoi(optarg); break;
        case 'r': o->lat_reps = atoi(optarg); break;
        case 't': o->thr_reps = atoi(optarg); break;
        case 'k': o->thr_msgs = atol(optarg); break;
        case 'q': o->qdepth = atoi(optarg); break;
        case 'W': o->global_warmup_ms = atoi(optarg); break;
        case 'm':
            if (strcmp(optarg, "lat") == 0)       { o->do_lat = 1; o->do_thr = 0; }
            else if (strcmp(optarg, "thr") == 0)  { o->do_lat = 0; o->do_thr = 1; }
            else if (strcmp(optarg, "both") == 0) { o->do_lat = 1; o->do_thr = 1; }
            else { fprintf(stderr, "잘못된 -m\n"); return -1; }
            break;
        case 'o': snprintf(o->outdir, sizeof(o->outdir), "%s", optarg); break;
        default:
            usage(prog);
            return -1;
        }
    }
    if (o->lat_reps < 30 || o->thr_reps < 30)
        fprintf(stderr, "경고: 반복 횟수가 30보다 작습니다 (과제 기준은 30회 이상)\n");
    if (o->lat_reps < 1 || o->thr_reps < 1 || o->warmup < 0 || o->thr_warmup < 0 ||
        o->qdepth < 1 || o->thr_msgs < 0 || o->global_warmup_ms < 0) {
        fprintf(stderr, "잘못된 반복/워밍업 값\n");
        return -1;
    }
    return 0;
}

size_t opts_max_size(const bench_opts_t *o)
{
    size_t m = 0;
    for (int i = 0; i < o->nsizes; i++)
        if (o->sizes[i] > m) m = o->sizes[i];
    return m;
}

/* 처리량 1회에 스레드당 약 16MB를 보내도록 잡는다. 너무 작거나 크지 않게 16~10000개. */
long thr_msgs_for(const bench_opts_t *o, size_t size)
{
    long k;
    if (o->thr_msgs > 0)
        return o->thr_msgs;
    k = (long)((16u * 1024 * 1024) / size);
    if (k < 16) k = 16;
    if (k > 10000) k = 10000;
    return k;
}

/* ------------------------------------------------------------------ */
/* 통계                                                                */
/* ------------------------------------------------------------------ */

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* p99는 nearest-rank 방식: 정렬한 뒤 ceil(0.99 * n)번째 값 */
void stats_compute(double *v, size_t n, stats_t *st)
{
    double sum = 0, sq = 0;
    size_t rank;

    memset(st, 0, sizeof(*st));
    if (n == 0) return;
    qsort(v, n, sizeof(double), cmp_double);
    for (size_t i = 0; i < n; i++) sum += v[i];
    st->n = n;
    st->mean = sum / n;
    for (size_t i = 0; i < n; i++) sq += (v[i] - st->mean) * (v[i] - st->mean);
    st->stddev = n > 1 ? sqrt(sq / (n - 1)) : 0;
    st->median = (n % 2) ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
    rank = (size_t)ceil(0.99 * (double)n);
    if (rank < 1) rank = 1;
    st->p99 = v[rank - 1];
    st->min = v[0];
    st->max = v[n - 1];
}

/* ------------------------------------------------------------------ */
/* 자원 사용량                                                         */
/* ------------------------------------------------------------------ */

long read_vmrss_kb(void)
{
    FILE *f = fopen("/proc/self/status", "r");
    char line[256];
    long kb = -1;

    if (!f) return -1;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            kb = strtol(line + 6, NULL, 10);
            break;
        }
    }
    fclose(f);
    return kb;
}

double tv_ms(struct timeval a, struct timeval b)
{
    return (double)(b.tv_sec - a.tv_sec) * 1000.0 + (double)(b.tv_usec - a.tv_usec) / 1000.0;
}

/* ------------------------------------------------------------------ */
/* 입출력                                                              */
/* ------------------------------------------------------------------ */

/* pipe는 한 번에 다 안 읽히거나 안 써질 수 있어서 끝까지 반복한다 */
ssize_t readn(int fd, void *buf, size_t n)
{
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char *)buf + done, n - done);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return (ssize_t)done;   /* EOF */
        done += (size_t)r;
    }
    return (ssize_t)done;
}

ssize_t writen(int fd, const void *buf, size_t n)
{
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, (const char *)buf + done, n - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        done += (size_t)w;
    }
    return (ssize_t)done;
}

void *xmalloc_touch(size_t n)
{
    void *p = malloc(n);
    if (!p) { perror("malloc"); exit(1); }
    memset(p, 0x5a, n);      /* 측정 중에 첫 페이지 폴트가 나지 않게 미리 건드림 */
    return p;
}

/* ------------------------------------------------------------------ */
/* CSV                                                                  */
/* ------------------------------------------------------------------ */

static FILE *open_in_dir(const char *outdir, const char *name)
{
    char path[1024];
    FILE *f;

    mkdir(outdir, 0755);
    snprintf(path, sizeof(path), "%s/%s", outdir, name);
    f = fopen(path, "w");
    if (!f) { perror(path); exit(1); }
    return f;
}

FILE *csv_open_summary(const char *outdir, const char *ipc)
{
    char name[128];
    FILE *f;

    snprintf(name, sizeof(name), "%s_summary.csv", ipc);
    f = open_in_dir(outdir, name);
    fprintf(f,
        "ipc,mode,msg_size,clients,warmup,reps,msgs_per_rep,samples,"
        "mean_us,median_us,p99_us,max_us,min_us,stddev_us,"
        "msgs_per_sec_mean,msgs_per_sec_median,bytes_per_sec_mean,bytes_per_sec_median,"
        "cli_utime_ms,cli_stime_ms,cli_nvcsw,cli_nivcsw,cli_vmrss_kb,"
        "srv_utime_ms,srv_stime_ms,srv_nvcsw,srv_nivcsw,srv_vmrss_kb\n");
    return f;
}

void csv_write_row(FILE *f, const result_row_t *r)
{
    const struct rusage *c0 = &r->cli_ru0, *c1 = &r->cli_ru1;
    const struct rusage *s0 = &r->srv0.ru, *s1 = &r->srv1.ru;

    fprintf(f, "%s,%s,%zu,%d,%d,%d,%ld,%zu,"
               "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,"
               "%.1f,%.1f,%.1f,%.1f,"
               "%.3f,%.3f,%ld,%ld,%ld,"
               "%.3f,%.3f,%ld,%ld,%ld\n",
        r->ipc, r->mode, r->size, r->clients, r->warmup, r->reps, r->msgs_per_rep, r->st.n,
        r->st.mean, r->st.median, r->st.p99, r->st.max, r->st.min, r->st.stddev,
        r->msgs_per_sec_mean, r->msgs_per_sec_median,
        r->bytes_per_sec_mean, r->bytes_per_sec_median,
        tv_ms(c0->ru_utime, c1->ru_utime), tv_ms(c0->ru_stime, c1->ru_stime),
        c1->ru_nvcsw - c0->ru_nvcsw, c1->ru_nivcsw - c0->ru_nivcsw, r->cli_vmrss_kb,
        tv_ms(s0->ru_utime, s1->ru_utime), tv_ms(s0->ru_stime, s1->ru_stime),
        s1->ru_nvcsw - s0->ru_nvcsw, s1->ru_nivcsw - s0->ru_nivcsw, r->srv1.vmrss_kb);
    fflush(f);
}

FILE *csv_open_raw(const char *outdir, const char *ipc)
{
    char name[128];
    FILE *f;

    snprintf(name, sizeof(name), "%s_latency_raw.csv", ipc);
    f = open_in_dir(outdir, name);
    fprintf(f, "ipc,msg_size,clients,thread,seq,rtt_ns\n");
    return f;
}

void print_row(const result_row_t *r)
{
    if (strcmp(r->mode, "latency") == 0)
        printf("%-5s %-10s size=%-8zu cli=%d  mean=%9.2fus  median=%9.2fus  p99=%9.2fus  max=%9.2fus\n",
               r->ipc, r->mode, r->size, r->clients,
               r->st.mean, r->st.median, r->st.p99, r->st.max);
    else
        printf("%-5s %-10s size=%-8zu cli=%d  %12.0f msg/s  %9.1f MB/s (중앙값)\n",
               r->ipc, r->mode, r->size, r->clients,
               r->msgs_per_sec_median, r->bytes_per_sec_median / (1024.0 * 1024.0));
    fflush(stdout);
}

/* ------------------------------------------------------------------ */
/* 신호                                                                */
/* ------------------------------------------------------------------ */

void block_signals(sigset_t *set)
{
    sigemptyset(set);
    sigaddset(set, SIGINT);
    sigaddset(set, SIGTERM);
    sigaddset(set, SIGHUP);
    sigaddset(set, SIGQUIT);
    sigaddset(set, SIGCHLD);
    pthread_sigmask(SIG_BLOCK, set, NULL);
    signal(SIGPIPE, SIG_IGN);      /* 상대가 죽으면 write가 EPIPE로 끝나게 */
}

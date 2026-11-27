/*
 * stats.c -- 서버 공유 통계 (7조)
 */
#include "stats.h"
#include "timing.h"
#include "ipc.h"

#include <pthread.h>
#include <string.h>

static int client_lock_mode, global_lock_mode, merge_k = 8;

static client_stat_t   ctab[APP_MAX_CLIENTS];
static pthread_mutex_t cmu[APP_MAX_CLIENTS];   /* STATS_LOCK_PER_CLIENT */
static pthread_mutex_t tmu;                    /* STATS_LOCK_TABLE */

static struct {
    double   sum_rms;
    uint64_t n;
} g;
static pthread_rwlock_t g_rw;
static pthread_mutex_t  g_mu;

/* ---- 락 도우미: 대기 시간을 재면서 잡는다 ---- */

static pthread_mutex_t *client_mutex(uint32_t id)
{
    return client_lock_mode == STATS_LOCK_TABLE ? &tmu : &cmu[id];
}

static void cli_lock(uint32_t id, uint64_t *acc)
{
    uint64_t t0 = now_ns();
    pthread_mutex_lock(client_mutex(id));
    if (acc) *acc += now_ns() - t0;
}

static void cli_unlock(uint32_t id)
{
    pthread_mutex_unlock(client_mutex(id));
}

static void g_lock(int write, uint64_t *acc)
{
    uint64_t t0 = now_ns();
    if (global_lock_mode == STATS_GLOCK_MUTEX)
        pthread_mutex_lock(&g_mu);
    else if (write)
        pthread_rwlock_wrlock(&g_rw);
    else
        pthread_rwlock_rdlock(&g_rw);
    if (acc) *acc += now_ns() - t0;
}

static void g_unlock(void)
{
    if (global_lock_mode == STATS_GLOCK_MUTEX)
        pthread_mutex_unlock(&g_mu);
    else
        pthread_rwlock_unlock(&g_rw);
}

/* ---- 공개 함수 ---- */

void stats_init(int client_lock, int global_lock, int k)
{
    client_lock_mode = client_lock;
    global_lock_mode = global_lock;
    merge_k = k > 0 ? k : 1;
    memset(ctab, 0, sizeof(ctab));
    memset(&g, 0, sizeof(g));
    for (int i = 0; i < APP_MAX_CLIENTS; i++)
        pthread_mutex_init(&cmu[i], NULL);
    pthread_mutex_init(&tmu, NULL);
    pthread_rwlock_init(&g_rw, NULL);
    pthread_mutex_init(&g_mu, NULL);
}

void stats_destroy(void)
{
    for (int i = 0; i < APP_MAX_CLIENTS; i++)
        pthread_mutex_destroy(&cmu[i]);
    pthread_mutex_destroy(&tmu);
    pthread_rwlock_destroy(&g_rw);
    pthread_mutex_destroy(&g_mu);
}

double stats_global_mean(lockwait_t *lw)
{
    double m;
    g_lock(0, lw ? &lw->grd_ns : NULL);
    m = g.n ? g.sum_rms / (double)g.n : 0;
    g_unlock();
    return m;
}

void stats_client_add(uint32_t id, uint64_t bytes, const audio_stats_t *a, lockwait_t *lw)
{
    client_stat_t *c = &ctab[id];

    cli_lock(id, lw ? &lw->cli_ns : NULL);
    c->chunks++;
    c->bytes += bytes;
    c->sum_rms += a->rms;
    if (a->peak > c->max_peak) c->max_peak = a->peak;
    c->silent_runs += a->silent_runs;
    cli_unlock(id);
}

void stats_global_flush(stats_pending_t *p, lockwait_t *lw)
{
    if (p->n == 0) return;
    g_lock(1, lw ? &lw->gwr_ns : NULL);
    g.sum_rms += p->sum_rms;
    g.n += p->n;
    g_unlock();
    p->sum_rms = 0;
    p->n = 0;
}

void stats_global_add(stats_pending_t *p, double rms, lockwait_t *lw)
{
    p->sum_rms += rms;
    if (++p->n >= (uint64_t)merge_k)
        stats_global_flush(p, lw);
}

void stats_client_get(uint32_t id, client_stat_t *out)
{
    cli_lock(id, NULL);
    *out = ctab[id];
    cli_unlock(id);
}

void stats_global_get(double *sum_rms, uint64_t *n)
{
    g_lock(0, NULL);
    *sum_rms = g.sum_rms;
    *n = g.n;
    g_unlock();
}

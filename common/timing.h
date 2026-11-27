/*
 * timing.h -- 시간, 측정값 요약, rusage, /proc/self/status (7조, 기획서 8.1절)
 */
#ifndef APP_TIMING_H
#define APP_TIMING_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <sys/time.h>

static inline uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* 측정값 묶음 요약. 1단계 stats_compute와 같은 정의(p99는 nearest-rank) */
typedef struct {
    size_t n;
    double mean, median, p99, max, min, stddev;
} summary_t;

void summarize(double *v, size_t n, summary_t *s);   /* v는 정렬된다 */

long   read_status_kb(const char *field);   /* "VmRSS:", "VmHWM:". 실패하면 -1 */
double tv_us(struct timeval a, struct timeval b);    /* b - a, us */

#endif

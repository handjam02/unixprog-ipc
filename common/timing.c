/*
 * timing.c -- 시간, 측정값 요약, rusage, /proc/self/status (7조)
 */
#include "timing.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

void summarize(double *v, size_t n, summary_t *s)
{
    double sum = 0, sq = 0;
    size_t rank;

    memset(s, 0, sizeof(*s));
    if (n == 0) return;
    qsort(v, n, sizeof(double), cmp_double);
    for (size_t i = 0; i < n; i++) sum += v[i];
    s->n = n;
    s->mean = sum / n;
    for (size_t i = 0; i < n; i++) sq += (v[i] - s->mean) * (v[i] - s->mean);
    s->stddev = n > 1 ? sqrt(sq / (n - 1)) : 0;
    s->median = (n % 2) ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
    rank = (size_t)ceil(0.99 * (double)n);
    if (rank < 1) rank = 1;
    s->p99 = v[rank - 1];
    s->min = v[0];
    s->max = v[n - 1];
}

long read_status_kb(const char *field)
{
    FILE *f = fopen("/proc/self/status", "r");
    char line[256];
    size_t len = strlen(field);
    long kb = -1;

    if (!f) return -1;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, field, len) == 0) {
            kb = strtol(line + len, NULL, 10);
            break;
        }
    }
    fclose(f);
    return kb;
}

double tv_us(struct timeval a, struct timeval b)
{
    return (double)(b.tv_sec - a.tv_sec) * 1e6 + (double)(b.tv_usec - a.tv_usec);
}

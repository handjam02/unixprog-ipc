/*
 * memcpy_ref.c -- 기준값: IPC 없이 memcpy만 했을 때의 속도 (7조)
 *
 * shm 처리량은 결국 "클라이언트 -> 공유 메모리", "공유 메모리 -> 서버" 두 번의
 * memcpy와 세마포어 비용이다. 같은 컴퓨터에서 memcpy만 돌린 속도를 재 두면
 * shm 처리량이 상한에 얼마나 가까운지 보고서에서 비교할 수 있다.
 *
 * 스레드마다 원본 4칸(shm 링 4칸과 같은 구조)을 돌아가며 대상 버퍼로 복사한다.
 * 출력은 복사한 바이트 기준 GB/s (1GB = 10^9 B).
 *
 *   ./memcpy_ref                기본: 크기 64,1K,16K,256K,1M x 스레드 1,2,4,8
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>

#define RING 4

static size_t g_size;
static long g_iters;
static pthread_barrier_t g_bar;

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void *worker(void *arg)
{
    uint64_t *elapsed = arg;
    char *src = malloc(g_size * RING), *dst = malloc(g_size);
    memset(src, 1, g_size * RING);
    memset(dst, 2, g_size);

    pthread_barrier_wait(&g_bar);
    uint64_t t0 = now_ns();
    for (long i = 0; i < g_iters; i++) {
        memcpy(dst, src + (size_t)(i % RING) * g_size, g_size);
        __asm__ volatile("" : : "r"(dst) : "memory");    /* 복사를 컴파일러가 지우지 못하게 */
    }
    *elapsed = now_ns() - t0;
    free(src);
    free(dst);
    return NULL;
}

int main(void)
{
    static const size_t sizes[] = { 64, 1024, 16384, 262144, 1048576 };
    static const int threads[] = { 1, 2, 4, 8 };

    printf("size,threads,copy_GBps\n");
    for (size_t si = 0; si < sizeof(sizes) / sizeof(sizes[0]); si++) {
        for (size_t ti = 0; ti < sizeof(threads) / sizeof(threads[0]); ti++) {
            int n = threads[ti];
            pthread_t t[8];
            uint64_t el[8], maxel = 0;

            g_size = sizes[si];
            g_iters = (long)((256ull * 1024 * 1024) / g_size);   /* 스레드당 256MB 복사 */
            pthread_barrier_init(&g_bar, NULL, (unsigned)n);
            for (int i = 0; i < n; i++) pthread_create(&t[i], NULL, worker, &el[i]);
            for (int i = 0; i < n; i++) pthread_join(t[i], NULL);
            pthread_barrier_destroy(&g_bar);
            for (int i = 0; i < n; i++) if (el[i] > maxel) maxel = el[i];
            printf("%zu,%d,%.2f\n", g_size, n,
                   (double)n * (double)g_iters * (double)g_size / ((double)maxel / 1e9) / 1e9);
            fflush(stdout);
        }
    }
    return 0;
}

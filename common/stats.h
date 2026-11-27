/*
 * stats.h -- 서버 공유 통계 (7조, 기획서 6.3절)
 *
 * 청크 하나당 접근 (5.1절 워크로드):
 *   클라이언트별 통계 쓰기 1회   stats_client_add
 *   전체 통계 읽기 1회           stats_global_mean
 *   전체 통계 쓰기 1/K회         stats_global_add (워커가 K개 모았다가 한 번)
 *
 * 비교 실험 스위치 (7장, 실행 인자로 바꿈):
 *   E1 클라이언트별 통계 락: STATS_LOCK_PER_CLIENT(기본) / STATS_LOCK_TABLE(테이블 전체에 1개)
 *   E2 전체 통계 락       : STATS_GLOCK_RWLOCK(기본) / STATS_GLOCK_MUTEX, 쓰기 간격 K
 *
 * 락 대기 시간(7.1절): lock 호출 직전 ~ 반환 직후를 lockwait_t에 더한다.
 * 교착 규칙: 락 두 개를 동시에 잡을 때는 "클라이언트별 다음 전체" 순서만. 이 모듈은
 * 둘을 겹쳐 잡지 않는다.
 */
#ifndef APP_STATS_H
#define APP_STATS_H

#include <stdint.h>
#include "audio.h"

enum { STATS_LOCK_PER_CLIENT = 0, STATS_LOCK_TABLE = 1 };
enum { STATS_GLOCK_RWLOCK = 0, STATS_GLOCK_MUTEX = 1 };

typedef struct {
    uint64_t chunks, bytes;
    double   sum_rms;
    double   max_peak;
    uint64_t silent_runs;
} client_stat_t;

typedef struct {
    uint64_t cli_ns;      /* 클라이언트별 통계 락 */
    uint64_t grd_ns;      /* 전체 통계 읽기 락 */
    uint64_t gwr_ns;      /* 전체 통계 쓰기 락 (K번에 한 번만 0이 아님) */
} lockwait_t;

typedef struct {          /* 워커마다 하나. 아직 전체 통계에 안 넣은 몫 */
    double   sum_rms;
    uint64_t n;
} stats_pending_t;

void   stats_init(int client_lock, int global_lock, int merge_k);
void   stats_destroy(void);

double stats_global_mean(lockwait_t *lw);       /* 지금까지 전체 평균 RMS. 아직 없으면 0 */
void   stats_client_add(uint32_t id, uint64_t bytes, const audio_stats_t *a, lockwait_t *lw);
void   stats_global_add(stats_pending_t *p, double rms, lockwait_t *lw);
void   stats_global_flush(stats_pending_t *p, lockwait_t *lw);   /* 워커가 끝날 때 */

/* 보고 스레드와 종료 뒤 요약용 읽기 */
void   stats_client_get(uint32_t id, client_stat_t *out);
void   stats_global_get(double *sum_rms, uint64_t *n);

#endif

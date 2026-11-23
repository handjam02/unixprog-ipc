/*
 * bench.h -- 측정 드라이버와 IPC 구현(transport) 사이의 약속 (7조)
 *
 * 측정 순서, 스레드 구조, 타임스탬프 위치, 통계, CSV 기록은 bench.c 하나로
 * 두 IPC가 똑같이 쓴다. IPC마다 다른 부분(보내기/받기, 자원 생성/정리)만
 * pipe_bench.c, shm_bench.c에서 아래 함수들로 채운다.
 * 이렇게 해야 두 버전의 차이가 IPC 방식 차이에서만 나온다.
 */
#ifndef BENCH_H
#define BENCH_H

#include "common.h"

typedef struct {
    const char *name;                                   /* "pipe", "shm" */

    /* fork 전에 부모가 호출: 채널 MAX_CLIENTS개 생성 (max_msg = 가장 큰 payload) */
    int  (*setup)(const bench_opts_t *o, size_t max_msg);
    /* fork 후 정리: 자식(서버)과 부모(클라이언트)가 안 쓰는 쪽을 닫는다 */
    void (*after_fork_server)(void);
    void (*after_fork_client)(void);

    /* 클라이언트 쪽: 채널 ch로 len바이트(헤더 포함) 보내기 / 받기 */
    int  (*cli_send)(int ch, const void *buf, size_t len);
    int  (*cli_recv)(int ch, void *buf, size_t len);

    /* 서버 쪽: 헤더+payload를 buf로 받기 (cap = buf 크기) / 응답 보내기 */
    int  (*srv_recv)(int ch, void *buf, size_t cap);
    int  (*srv_send)(int ch, const void *buf, size_t len);

    /* 부모가 정확히 한 번 호출: 이름 있는 IPC 자원 삭제 */
    void (*cleanup)(void);
} transport_t;

int bench_main(const transport_t *tp, int argc, char **argv);

#endif

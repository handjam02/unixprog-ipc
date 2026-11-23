/*
 * pipe_bench.c -- 버전 1: 익명 pipe (7조)
 *
 * 채널 하나 = pipe 2개
 *   req[ch]  : 클라이언트 워커 ch -> 서버 핸들러 ch
 *   resp[ch] : 서버 핸들러 ch -> 클라이언트 워커 ch
 * pipe는 한 방향이라 요청과 응답을 한 pipe로 하면 자기 메시지를 자기가 읽을 수 있다.
 * 그래서 방향마다 따로 둔다. fork 전에 만들고, fork 뒤 안 쓰는 끝은 닫는다.
 *
 * 커널 pipe 버퍼는 기본 64KB라서 256KB, 1MB 메시지는 write/read가 여러 번에
 * 나눠 처리된다 (readn/writen이 끝까지 반복).
 * 이름 있는 자원이 없으므로 프로세스가 끝나면 커널이 알아서 회수한다.
 */
#include "bench.h"

#include <stdio.h>
#include <unistd.h>

static int req[MAX_CLIENTS][2];
static int resp[MAX_CLIENTS][2];

static int p_setup(const bench_opts_t *o, size_t max_msg)
{
    (void)o; (void)max_msg;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (pipe(req[i]) < 0 || pipe(resp[i]) < 0) {
            perror("pipe");
            return -1;
        }
    }
    return 0;
}

static void p_after_fork_server(void)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        close(req[i][1]);     /* 서버는 요청을 읽기만 */
        close(resp[i][0]);    /* 응답을 쓰기만 */
    }
}

static void p_after_fork_client(void)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        close(req[i][0]);
        close(resp[i][1]);
    }
}

static int p_cli_send(int ch, const void *buf, size_t len)
{
    return writen(req[ch][1], buf, len) == (ssize_t)len ? 0 : -1;
}

static int p_cli_recv(int ch, void *buf, size_t len)
{
    return readn(resp[ch][0], buf, len) == (ssize_t)len ? 0 : -1;
}

/* 헤더를 먼저 읽고, 헤더에 적힌 길이만큼 payload를 읽는다 */
static int p_srv_recv(int ch, void *buf, size_t cap)
{
    msg_hdr_t *h = buf;
    if (readn(req[ch][0], h, HDR_SIZE) != (ssize_t)HDR_SIZE)
        return -1;
    if (HDR_SIZE + h->size > cap)
        return -1;
    if (h->size && readn(req[ch][0], (char *)buf + HDR_SIZE, h->size) != (ssize_t)h->size)
        return -1;
    return 0;
}

static int p_srv_send(int ch, const void *buf, size_t len)
{
    return writen(resp[ch][1], buf, len) == (ssize_t)len ? 0 : -1;
}

static void p_cleanup(void)
{
    /* 익명 pipe는 fd만 닫으면 된다. ipcs에 나타나는 자원 없음. */
    for (int i = 0; i < MAX_CLIENTS; i++) {
        close(req[i][1]);
        close(resp[i][0]);
    }
}

static const transport_t pipe_tp = {
    .name = "pipe",
    .setup = p_setup,
    .after_fork_server = p_after_fork_server,
    .after_fork_client = p_after_fork_client,
    .cli_send = p_cli_send,
    .cli_recv = p_cli_recv,
    .srv_recv = p_srv_recv,
    .srv_send = p_srv_send,
    .cleanup = p_cleanup,
};

int main(int argc, char **argv)
{
    return bench_main(&pipe_tp, argc, argv);
}

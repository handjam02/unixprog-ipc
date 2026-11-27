/*
 * ipc_pipe.c -- ipc.h의 pipe(FIFO) 구현 (7조, 손재민, 기획서 6.4~6.5절)
 *
 * 자원 (8.3절)
 *   /tmp/osproject_7_srv          공개 FIFO. HELLO(32바이트)만 오간다. PIPE_BUF(4KB) 이하라
 *                                 여러 클라이언트가 동시에 써도 섞이지 않는다.
 *   /tmp/osproject_7_<pid>_req    클라이언트 -> 서버 요청. 클라이언트가 만들고 지운다.
 *   /tmp/osproject_7_<pid>_rsp    서버 -> 클라이언트 응답. 클라이언트가 만들고 지운다.
 *
 * 접속 순서 (FIFO open이 서로를 기다리며 막히지 않게 하는 순서)
 *   클라이언트: req, rsp mkfifo -> rsp를 O_RDONLY|O_NONBLOCK로 열기 -> 공개 FIFO에 HELLO
 *   서버 수신 스레드: rsp를 O_WRONLY|O_NONBLOCK로 열기(읽는 쪽이 이미 있어 바로 성공)
 *                    -> 블로킹으로 바꿈 -> req를 O_RDONLY|O_NONBLOCK로 열기 -> 번호 응답
 *   클라이언트: 번호 응답을 받고(5초 제한) -> req를 O_WRONLY로 열기(읽는 쪽이 있어 바로 성공)
 *
 * 서버 수신 (6.5절): 수신 스레드 하나가 poll()로 모든 요청 FIFO를 보고, 준비된 fd마다
 * read()를 한 번만 불러 클라이언트별 수신 상태(헤더/본문 읽은 양)를 채운다. 256KB 청크가
 * 64KB pipe 버퍼에 나뉘어 와도 다른 클라이언트가 기다리지 않는다. 청크 하나가 다 차면
 * 반환한다. 다음 호출은 그다음 클라이언트부터 훑는다(한 클라이언트가 독차지하지 않게).
 *
 * 서버 응답 (6.5절): 응답 FIFO는 블로킹. 워커가 클라이언트별 mutex를 잡고 헤더와 본문을
 * 끝까지 쓴다. 창 크기가 2 이상이면 워커 둘이 같은 클라이언트에 동시에 쓸 수 있고,
 * 4KB보다 큰 write는 원자적이지 않기 때문이다.
 *
 * 종료: BYE를 받거나 요청 FIFO가 EOF(클라이언트가 죽음)면 그 클라이언트를 정리한다.
 * BYE 없이 끊긴 경우에는 서버가 그 클라이언트의 FIFO 파일도 지운다.
 * server_stop은 자기 pipe에 1바이트를 써서 poll을 깨운다.
 *
 * 알려진 한계: 클라이언트가 BYE 없이 죽으면, 큐에 남아 있던 그 클라이언트의 청크 응답이
 * 같은 번호를 새로 받은 클라이언트에게 갈 수 있다. 번호는 돌아가며 주어 바로 재사용되지
 * 않게 했고, 클라이언트는 seq와 크기를 검사해 잘못된 응답을 센다.
 */
#include "ipc.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <poll.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>

#define HELLO_MSG_SIZE  (sizeof(app_hdr_t) + sizeof(hello_t))   /* 32 */
#define CONNECT_TIMEOUT_MS 5000
#define NO_CLIENT       UINT32_MAX

/* ------------------------------------------------------------------ */
/* 공통 도우미                                                          */
/* ------------------------------------------------------------------ */

static void fifo_path(char *buf, size_t n, int pid, const char *kind)
{
    snprintf(buf, n, APP_FIFO_FMT, pid, kind);
}

static int set_blocking(int fd)
{
    int fl = fcntl(fd, F_GETFL);
    if (fl < 0) return -1;
    return fcntl(fd, F_SETFL, fl & ~O_NONBLOCK);
}

/* 헤더와 본문을 writev로 함께 보내고, 나뉘어 써지면 남은 만큼 반복한다 */
static int write_msg(int fd, const app_hdr_t *h, const void *payload, size_t size)
{
    struct iovec iov[2] = {
        { (void *)h, sizeof(*h) },
        { (void *)payload, size },
    };
    int idx = 0, cnt = size ? 2 : 1;

    while (idx < cnt) {
        ssize_t w = writev(fd, iov + idx, cnt - idx);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;                       /* EPIPE: 상대가 나감 */
        }
        while (idx < cnt && (size_t)w >= iov[idx].iov_len) {
            w -= (ssize_t)iov[idx].iov_len;
            idx++;
        }
        if (idx < cnt) {
            iov[idx].iov_base = (char *)iov[idx].iov_base + w;
            iov[idx].iov_len -= (size_t)w;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* 서버                                                                */
/* ------------------------------------------------------------------ */

typedef struct {
    /* 수신 스레드만 만지는 것 */
    int       active;
    int       pid;
    int       req_fd;
    app_hdr_t hdr;
    size_t    hdr_got;      /* 읽은 헤더 바이트 */
    uint8_t  *body;         /* hdr.size 바이트, 다 차면 chunk_t로 넘긴다 */
    size_t    body_got;

    /* 워커들과 같이 쓰는 것: wmu로 보호 */
    pthread_mutex_t wmu;
    int       rsp_fd;       /* -1이면 끊김 */
} srv_client_t;

struct ipc_server {
    int          pub_fd;            /* 공개 FIFO 읽기 */
    int          pub_dummy_fd;      /* 공개 FIFO 쓰기. 클라이언트가 없을 때 EOF가 계속 뜨지 않게 */
    uint8_t      pub_buf[HELLO_MSG_SIZE * 16];
    size_t       pub_len;
    int          wake[2];           /* server_stop -> 수신 스레드 */
    uint32_t     next_id;           /* 번호를 돌아가며 준다 */
    uint32_t     rr;                /* 다음 server_recv가 먼저 볼 클라이언트 */
    srv_client_t c[APP_MAX_CLIENTS];
};

static void srv_drop_client(ipc_server_t *s, uint32_t id, int unlink_fifos)
{
    srv_client_t *c = &s->c[id];

    if (!c->active) return;
    close(c->req_fd);
    c->req_fd = -1;
    free(c->body);
    c->body = NULL;
    c->hdr_got = c->body_got = 0;

    pthread_mutex_lock(&c->wmu);
    if (c->rsp_fd >= 0) close(c->rsp_fd);
    c->rsp_fd = -1;
    pthread_mutex_unlock(&c->wmu);

    if (unlink_fifos) {                  /* BYE 없이 끊김: 클라이언트가 못 지운 파일 */
        char p[128];
        fifo_path(p, sizeof(p), c->pid, "req"); unlink(p);
        fifo_path(p, sizeof(p), c->pid, "rsp"); unlink(p);
    }
    c->active = 0;
}

/* HELLO 하나 처리. 빈 자리가 없거나 실패하면 NO_CLIENT를 답하거나 그냥 버린다 */
static void srv_hello(ipc_server_t *s, const app_hdr_t *h, const hello_t *hello)
{
    char rsp_path[128], req_path[128];
    app_hdr_t reply = { MSG_HELLO, NO_CLIENT, 0, 0, 0 };
    uint32_t id = NO_CLIENT;
    int rsp_fd, req_fd;

    if (h->type != MSG_HELLO || h->size != sizeof(hello_t) || hello->pid <= 0)
        return;
    fifo_path(rsp_path, sizeof(rsp_path), hello->pid, "rsp");
    fifo_path(req_path, sizeof(req_path), hello->pid, "req");

    rsp_fd = open(rsp_path, O_WRONLY | O_NONBLOCK | O_CLOEXEC);
    if (rsp_fd < 0)                      /* 클라이언트가 이미 없음(ENXIO, ENOENT) */
        return;
    if (set_blocking(rsp_fd) < 0) {
        close(rsp_fd);
        return;
    }

    for (uint32_t k = 0; k < APP_MAX_CLIENTS; k++) {
        uint32_t cand = (s->next_id + k) % APP_MAX_CLIENTS;
        if (!s->c[cand].active) { id = cand; break; }
    }
    if (id == NO_CLIENT ||
        (req_fd = open(req_path, O_RDONLY | O_NONBLOCK | O_CLOEXEC)) < 0) {
        write_msg(rsp_fd, &reply, NULL, 0);   /* 거절 */
        close(rsp_fd);
        return;
    }

    srv_client_t *c = &s->c[id];
    c->active = 1;
    c->pid = hello->pid;
    c->req_fd = req_fd;
    c->hdr_got = c->body_got = 0;
    c->body = NULL;
    pthread_mutex_lock(&c->wmu);
    c->rsp_fd = rsp_fd;
    pthread_mutex_unlock(&c->wmu);
    s->next_id = (id + 1) % APP_MAX_CLIENTS;

    reply.client = id;
    pthread_mutex_lock(&c->wmu);
    if (write_msg(rsp_fd, &reply, NULL, 0) < 0) {
        pthread_mutex_unlock(&c->wmu);
        srv_drop_client(s, id, 1);
        return;
    }
    pthread_mutex_unlock(&c->wmu);
}

/* 공개 FIFO 읽기. 1 = HELLO를 하나 이상 처리함 */
static int srv_read_public(ipc_server_t *s)
{
    ssize_t r = read(s->pub_fd, s->pub_buf + s->pub_len, sizeof(s->pub_buf) - s->pub_len);
    size_t off = 0;

    if (r <= 0)
        return 0;                         /* EAGAIN 등. dummy 쓰기 fd가 있어 EOF는 없음 */
    s->pub_len += (size_t)r;
    while (s->pub_len - off >= HELLO_MSG_SIZE) {
        app_hdr_t h;
        hello_t hello;
        memcpy(&h, s->pub_buf + off, sizeof(h));
        memcpy(&hello, s->pub_buf + off + sizeof(h), sizeof(hello));
        srv_hello(s, &h, &hello);
        off += HELLO_MSG_SIZE;
    }
    memmove(s->pub_buf, s->pub_buf + off, s->pub_len - off);
    s->pub_len -= off;
    return 1;
}

/* 요청 FIFO에서 read 한 번.
 * 반환: 1 = 청크 완성(out), 0 = 아직 덜 참 / BYE / 끊김 처리, */
static int srv_read_client(ipc_server_t *s, uint32_t id, chunk_t *out, int *conn_event)
{
    srv_client_t *c = &s->c[id];
    ssize_t r;

    if (c->hdr_got < sizeof(app_hdr_t)) {
        r = read(c->req_fd, (uint8_t *)&c->hdr + c->hdr_got, sizeof(app_hdr_t) - c->hdr_got);
    } else {
        r = read(c->req_fd, c->body + c->body_got, c->hdr.size - c->body_got);
    }
    if (r < 0) {
        if (errno == EAGAIN || errno == EINTR) return 0;
        srv_drop_client(s, id, 1);
        *conn_event = 1;
        return 0;
    }
    if (r == 0) {                         /* 쓰는 쪽이 모두 닫힘: BYE 없이 나감 */
        srv_drop_client(s, id, 1);
        *conn_event = 1;
        return 0;
    }

    if (c->hdr_got < sizeof(app_hdr_t)) {
        c->hdr_got += (size_t)r;
        if (c->hdr_got < sizeof(app_hdr_t))
            return 0;
        /* 헤더 완성 */
        if (c->hdr.type == MSG_BYE) {
            srv_drop_client(s, id, 0);    /* FIFO 파일은 클라이언트가 지운다 */
            *conn_event = 1;
            return 0;
        }
        if (c->hdr.type != MSG_CHUNK || c->hdr.size > APP_MAX_CHUNK) {
            fprintf(stderr, "ipc_pipe: client %u 잘못된 헤더 (type %u, size %lu), 끊음\n",
                    id, c->hdr.type, (unsigned long)c->hdr.size);
            srv_drop_client(s, id, 1);
            *conn_event = 1;
            return 0;
        }
        c->body = xmalloc(c->hdr.size);
        c->body_got = 0;
        if (c->hdr.size > 0)
            return 0;
    } else {
        c->body_got += (size_t)r;
        if (c->body_got < c->hdr.size)
            return 0;
    }

    /* 청크 완성 */
    out->hdr = c->hdr;
    out->hdr.client = id;                 /* 번호는 서버가 정한 값을 믿는다 */
    out->data = c->body;
    c->body = NULL;
    c->hdr_got = c->body_got = 0;
    return 1;
}

ipc_server_t *server_open(void)
{
    ipc_server_t *s = calloc(1, sizeof(*s));

    if (!s) return NULL;
    ipc_cleanup();                        /* 남은 FIFO 정리 (8.4절 3번) */

    s->pub_fd = s->pub_dummy_fd = s->wake[0] = s->wake[1] = -1;
    for (int i = 0; i < APP_MAX_CLIENTS; i++) {
        s->c[i].req_fd = s->c[i].rsp_fd = -1;
        pthread_mutex_init(&s->c[i].wmu, NULL);
    }

    if (mkfifo(APP_FIFO_SRV, 0666) < 0 && errno != EEXIST) {
        perror("mkfifo " APP_FIFO_SRV);
        goto fail;
    }
    s->pub_fd = open(APP_FIFO_SRV, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    if (s->pub_fd < 0) { perror("open " APP_FIFO_SRV); goto fail; }
    s->pub_dummy_fd = open(APP_FIFO_SRV, O_WRONLY | O_NONBLOCK | O_CLOEXEC);
    if (s->pub_dummy_fd < 0) { perror("open " APP_FIFO_SRV " (w)"); goto fail; }
    if (pipe2(s->wake, O_NONBLOCK | O_CLOEXEC) < 0) { perror("pipe2"); goto fail; }
    return s;

fail:
    server_close(s);
    return NULL;
}

int server_recv(ipc_server_t *s, chunk_t *out)
{
    struct pollfd pfd[2 + APP_MAX_CLIENTS];
    uint32_t ids[APP_MAX_CLIENTS];

    for (;;) {
        int n = 0, nc = 0, conn_event = 0;

        pfd[n++] = (struct pollfd){ s->wake[0], POLLIN, 0 };
        pfd[n++] = (struct pollfd){ s->pub_fd, POLLIN, 0 };
        for (uint32_t k = 0; k < APP_MAX_CLIENTS; k++) {     /* rr부터 차례로 */
            uint32_t id = (s->rr + k) % APP_MAX_CLIENTS;
            if (!s->c[id].active) continue;
            ids[nc++] = id;
            pfd[n++] = (struct pollfd){ s->c[id].req_fd, POLLIN, 0 };
        }

        if (poll(pfd, (nfds_t)n, -1) < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            return -1;
        }
        if (pfd[0].revents)                                  /* server_stop */
            return -1;

        for (int i = 0; i < nc; i++) {
            short ev = pfd[2 + i].revents;
            if (!ev) continue;
            if (!s->c[ids[i]].active) continue;              /* 이번 루프에서 끊김 */
            if (ev & (POLLIN | POLLHUP | POLLERR | POLLNVAL)) {
                /* POLLHUP이어도 남은 데이터가 있을 수 있어 먼저 읽는다. read가 0이면 끊김 */
                if (srv_read_client(s, ids[i], out, &conn_event) == 1) {
                    s->rr = (ids[i] + 1) % APP_MAX_CLIENTS;
                    return 1;
                }
            }
        }
        if (pfd[1].revents & POLLIN)
            conn_event |= srv_read_public(s);
        if (conn_event)
            return 0;
    }
}

int server_send(ipc_server_t *s, const app_hdr_t *h, const void *payload)
{
    srv_client_t *c;
    int r;

    if (h->client >= APP_MAX_CLIENTS)
        return -1;
    c = &s->c[h->client];
    pthread_mutex_lock(&c->wmu);          /* 응답 하나를 끝까지 쓴 뒤 푼다 */
    r = c->rsp_fd >= 0 ? write_msg(c->rsp_fd, h, payload, h->size) : -1;
    pthread_mutex_unlock(&c->wmu);
    return r;
}

void server_stop(ipc_server_t *s)
{
    char b = 1;
    if (write(s->wake[1], &b, 1) < 0) { /* 이미 차 있으면 이미 깨운 것 */ }
}

void server_close(ipc_server_t *s)
{
    if (!s) return;
    for (uint32_t i = 0; i < APP_MAX_CLIENTS; i++) {
        srv_drop_client(s, i, 1);
        pthread_mutex_destroy(&s->c[i].wmu);
    }
    if (s->pub_fd >= 0) close(s->pub_fd);
    if (s->pub_dummy_fd >= 0) close(s->pub_dummy_fd);
    if (s->wake[0] >= 0) close(s->wake[0]);
    if (s->wake[1] >= 0) close(s->wake[1]);
    unlink(APP_FIFO_SRV);
    free(s);
}

/* ------------------------------------------------------------------ */
/* 클라이언트                                                          */
/* ------------------------------------------------------------------ */

struct ipc_client {
    int  req_fd, rsp_fd;
    char req_path[128], rsp_path[128];
};

static void cli_free(ipc_client_t *c)
{
    if (c->req_fd >= 0) close(c->req_fd);
    if (c->rsp_fd >= 0) close(c->rsp_fd);
    unlink(c->req_path);
    unlink(c->rsp_path);
    free(c);
}

ipc_client_t *client_connect(uint32_t *client_id)
{
    ipc_client_t *c = calloc(1, sizeof(*c));
    uint8_t msg[HELLO_MSG_SIZE];
    app_hdr_t h = { MSG_HELLO, 0, 0, 0, sizeof(hello_t) };
    hello_t hello = { (int32_t)getpid() };
    struct pollfd pfd;
    int pub, pr;

    if (!c) return NULL;
    c->req_fd = c->rsp_fd = -1;
    fifo_path(c->req_path, sizeof(c->req_path), hello.pid, "req");
    fifo_path(c->rsp_path, sizeof(c->rsp_path), hello.pid, "rsp");
    unlink(c->req_path);                  /* 같은 pid로 남은 것 */
    unlink(c->rsp_path);
    if (mkfifo(c->req_path, 0600) < 0 || mkfifo(c->rsp_path, 0600) < 0) {
        perror("mkfifo");
        goto fail;
    }

    /* 1. 응답 FIFO를 먼저 읽기로 연다 (쓰는 쪽 없이도 바로 성공) */
    c->rsp_fd = open(c->rsp_path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    if (c->rsp_fd < 0) { perror("open rsp"); goto fail; }

    /* 2. HELLO. 서버가 없으면 ENXIO(읽는 쪽 없음) 또는 ENOENT */
    pub = open(APP_FIFO_SRV, O_WRONLY | O_NONBLOCK | O_CLOEXEC);
    if (pub < 0) goto fail;
    memcpy(msg, &h, sizeof(h));
    memcpy(msg + sizeof(h), &hello, sizeof(hello));
    if (writen(pub, msg, sizeof(msg)) != (ssize_t)sizeof(msg)) {   /* PIPE_BUF 이하: 원자적 */
        close(pub);
        goto fail;
    }
    close(pub);

    /* 3. 번호 응답을 기다린다 */
    pfd = (struct pollfd){ c->rsp_fd, POLLIN, 0 };
    do pr = poll(&pfd, 1, CONNECT_TIMEOUT_MS); while (pr < 0 && errno == EINTR);
    if (pr <= 0) {
        fprintf(stderr, "ipc_pipe: 서버 응답 없음\n");
        goto fail;
    }
    if (set_blocking(c->rsp_fd) < 0 ||
        readn(c->rsp_fd, &h, sizeof(h)) != (ssize_t)sizeof(h) ||
        h.type != MSG_HELLO || h.client >= APP_MAX_CLIENTS) {
        fprintf(stderr, "ipc_pipe: 접속 거절 (클라이언트가 %d개 꽉 참?)\n", APP_MAX_CLIENTS);
        goto fail;
    }

    /* 4. 요청 FIFO. 서버가 이미 읽기로 열었으므로 바로 성공 */
    c->req_fd = open(c->req_path, O_WRONLY | O_CLOEXEC);
    if (c->req_fd < 0) { perror("open req"); goto fail; }

    *client_id = h.client;
    return c;

fail:
    cli_free(c);
    return NULL;
}

int client_send(ipc_client_t *c, const app_hdr_t *h, const void *payload)
{
    return write_msg(c->req_fd, h, payload, h->size);
}

int client_recv(ipc_client_t *c, app_hdr_t *h, void *buf, size_t cap)
{
    if (readn(c->rsp_fd, h, sizeof(*h)) != (ssize_t)sizeof(*h))
        return -1;                        /* 서버가 나감(EOF) */
    if (h->size > cap)
        return -1;
    if (h->size && readn(c->rsp_fd, buf, h->size) != (ssize_t)h->size)
        return -1;
    return 0;
}

void client_close(ipc_client_t *c)
{
    app_hdr_t bye = { MSG_BYE, 0, 0, 0, 0 };

    if (!c) return;
    if (c->req_fd >= 0)
        write_msg(c->req_fd, &bye, NULL, 0);   /* 실패해도(서버가 먼저 나감) 정리는 계속 */
    cli_free(c);
}

/* ------------------------------------------------------------------ */

void ipc_cleanup(void)
{
    glob_t g;
    struct stat st;

    unlink(APP_FIFO_SRV);
    if (glob("/tmp/osproject_7_*", 0, NULL, &g) == 0) {
        for (size_t i = 0; i < g.gl_pathc; i++)
            if (lstat(g.gl_pathv[i], &st) == 0 && S_ISFIFO(st.st_mode))
                unlink(g.gl_pathv[i]);
        globfree(&g);
    }
}

/*
 * util.h -- 입출력, 메모리, 신호 도우미 (7조)
 * 기획서 8.1절 목록에는 없지만 timing/csv 어디에도 맞지 않아 따로 둔다.
 */
#ifndef APP_UTIL_H
#define APP_UTIL_H

#include <stddef.h>
#include <signal.h>
#include <sys/types.h>

/* pipe 구현용: 끝까지 반복. readn은 EOF면 그때까지 읽은 바이트 수 */
ssize_t readn(int fd, void *buf, size_t n);
ssize_t writen(int fd, const void *buf, size_t n);

void *xmalloc(size_t n);          /* 실패하면 종료 */
void *xmalloc_touch(size_t n);    /* 할당하고 페이지를 미리 건드림 */

/* main에서 스레드를 만들기 전에 부른다(기획서 8.4절 2번). 이후 스레드는 막힌 상태를
 * 물려받고, 신호는 시그널 스레드가 sigwait로만 받는다. SIGUSR1은 프로그램 안에서
 * 시그널 스레드를 깨워 끝내는 용도. SIGPIPE는 무시(EPIPE로 받기) */
void block_signals(sigset_t *set);

#endif

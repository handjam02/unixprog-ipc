# 1단계 IPC 벤치마크 Makefile (7조)
#   make          : 두 버전 모두 빌드
#   make pipe     : pipe 버전만
#   make shm      : shared memory 버전만
#   make memref   : 기준값용 memcpy 속도 측정 프로그램
#   make run      : 전체 측정 (run_all.sh)
#   make quick    : 짧게 동작만 확인 (결과는 results_quick/)
#   make ipc-check: 7조 key(60071, 60072)로 남은 IPC 자원이 있는지 확인
#   make ipc-clean: 남은 자원 강제 삭제 (kill -9 등으로 끝났을 때)
#   make clean

CC      = gcc
CFLAGS  ?= -O2 -g -Wall -Wextra -std=gnu11
LDFLAGS ?=
LDLIBS   = -pthread -lm
CFLAGS  += -pthread

COMMON_SRC = common.c bench.c
COMMON_HDR = common.h bench.h

.PHONY: all pipe shm memref run quick ipc-check ipc-clean clean

all: pipe_bench shm_bench memcpy_ref

pipe: pipe_bench
shm: shm_bench
memref: memcpy_ref

memcpy_ref: memcpy_ref.c
	$(CC) $(CFLAGS) -o $@ memcpy_ref.c $(LDFLAGS) $(LDLIBS)

pipe_bench: pipe_bench.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o $@ pipe_bench.c $(COMMON_SRC) $(LDFLAGS) $(LDLIBS)

shm_bench: shm_bench.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o $@ shm_bench.c $(COMMON_SRC) $(LDFLAGS) $(LDLIBS)

run: all
	bash ./run_all.sh

quick: all
	./pipe_bench -r 30 -t 30 -w 5 -k 64 -o results_quick
	./shm_bench  -r 30 -t 30 -w 5 -k 64 -o results_quick
	$(MAKE) --no-print-directory ipc-check

# 60071 = 0x0000eaa7, 60072 = 0x0000eaa8
ipc-check:
	@if ipcs -m -s | grep -Ei '0x0000eaa7|0x0000eaa8'; then \
		echo "남은 자원이 있습니다. make ipc-clean 으로 지우세요."; exit 1; \
	else echo "7조 key(60071, 60072)로 남은 IPC 자원 없음"; fi

ipc-clean:
	-ipcrm -M 60071
	-ipcrm -S 60072

clean:
	rm -f pipe_bench shm_bench memcpy_ref

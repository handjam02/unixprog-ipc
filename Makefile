# 7조 IPC 설계과제 Makefile (기획서 8.2절)
#
#   make            : 아래 전부 (bench + 있는 IPC 구현의 응용)
#   make bench      : 1단계 벤치마크 (phase1/)
#   make app_pipe   : app/server_pipe, app/client_pipe
#   make app_shm    : app/server_shm,  app/client_shm
#   make tsan       : 경쟁 상태 검사용 빌드 (-fsanitize=thread), app/*_tsan
#   make cleanipc   : 비정상 종료로 남은 IPC 자원 삭제 (8.4절 3번)
#   make clean      : 실행 파일 삭제
#
# 측정은 항상 기본 CFLAGS 빌드로 한다.

CC      = gcc
CFLAGS ?= -O2 -g -Wall -Wextra -std=gnu11
CFLAGS += -pthread -D_GNU_SOURCE -Icommon -Iapp
LDLIBS  = -pthread -lm
TSAN    = -O1 -g -fsanitize=thread

COMMON_SRC = common/audio.c common/stats.c common/queue.c common/timing.c common/csv.c common/util.c
COMMON_HDR = app/ipc.h $(COMMON_SRC:.c=.h)

# 있는 구현만 고른다 (ipc_shm.c가 아직 없어도 make가 실패하지 않게)
IMPLS := $(patsubst app/ipc_%.c,%,$(wildcard app/ipc_*.c))
APP_BINS  = $(foreach i,$(IMPLS),app/server_$(i) app/client_$(i) app/ipcclean_$(i))
TSAN_BINS = $(foreach i,$(IMPLS),app/server_$(i)_tsan app/client_$(i)_tsan)

.PHONY: all bench app_pipe app_shm tsan cleanipc clean

all: bench $(APP_BINS)
	@for i in pipe shm; do [ -f app/ipc_$$i.c ] || echo "참고: app/ipc_$$i.c가 없어 $$i 버전은 건너뜀"; done

bench:
	$(MAKE) -C phase1

app_pipe: app/server_pipe app/client_pipe app/ipcclean_pipe
app_shm:  app/server_shm  app/client_shm  app/ipcclean_shm
tsan: $(TSAN_BINS)

app/server_%: app/server.c app/ipc_%.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -DIPC_NAME='"$*"' -o $@ app/server.c app/ipc_$*.c $(COMMON_SRC) $(LDLIBS)

app/client_%: app/client.c app/ipc_%.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -DIPC_NAME='"$*"' -o $@ app/client.c app/ipc_$*.c $(COMMON_SRC) $(LDLIBS)

app/ipcclean_%: app/ipcclean.c app/ipc_%.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -DIPC_NAME='"$*"' -o $@ app/ipcclean.c app/ipc_$*.c $(COMMON_SRC) $(LDLIBS)

# 패턴 규칙이 위의 server_%에 먼저 걸리지 않도록 tsan은 명시적으로 만든다
define TSAN_RULE
app/server_$(1)_tsan: app/server.c app/ipc_$(1).c $$(COMMON_SRC) $$(COMMON_HDR)
	$$(CC) $$(CFLAGS) $$(TSAN) -DIPC_NAME='"$(1)"' -o $$@ app/server.c app/ipc_$(1).c $$(COMMON_SRC) $$(LDLIBS)
app/client_$(1)_tsan: app/client.c app/ipc_$(1).c $$(COMMON_SRC) $$(COMMON_HDR)
	$$(CC) $$(CFLAGS) $$(TSAN) -DIPC_NAME='"$(1)"' -o $$@ app/client.c app/ipc_$(1).c $$(COMMON_SRC) $$(LDLIBS)
endef
$(foreach i,$(IMPLS),$(eval $(call TSAN_RULE,$(i))))

# 8.4절 3번: 구현의 ipc_cleanup()을 부르고, 빌드가 없어도 지워지게 key와 FIFO를 직접 지운다.
# 60070 = 0x0000eaa6, 60071 = eaa7, 60072 = eaa8, 60073 = eaa9
cleanipc:
	-@for i in $(IMPLS); do [ -x app/ipcclean_$$i ] && app/ipcclean_$$i; done; true
	-@ipcrm -M 60070 2>/dev/null; ipcrm -M 60071 2>/dev/null; true
	-@ipcrm -S 60072 2>/dev/null; ipcrm -S 60073 2>/dev/null; true
	-@rm -f /tmp/osproject_7_*
	@echo "--- ipcs -m -s -q (7조 key) ---"
	@ipcs -m -s -q | grep -Ei '0x0000eaa[6-9]' || echo "7조 key(60070~60073)로 남은 IPC 자원 없음"
	@echo "--- ls /tmp/osproject_7_* ---"
	@ls /tmp/osproject_7_* 2>/dev/null || echo "남은 FIFO 없음"

clean:
	rm -f $(foreach i,pipe shm,app/server_$(i) app/client_$(i) app/ipcclean_$(i) app/server_$(i)_tsan app/client_$(i)_tsan)
	rm -f app/*.o common/*.o
	$(MAKE) -C phase1 clean

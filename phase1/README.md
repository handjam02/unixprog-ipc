# 1단계: IPC 기초 성능 측정 (7조, pipe + shared memory)

1인 팀이라 두 가지 방식만 구현했다.

| 버전 | 실행 파일 | IPC | 동기화 |
|---|---|---|---|
| 1 | `pipe_bench` | 익명 pipe (채널마다 요청용, 응답용 2개) | pipe 자체의 블로킹 read/write |
| 2 | `shm_bench` | System V shared memory (key 60071) | System V 세마포어 (key 60072, 채널당 3개) |

## 빌드와 실행

```bash
make            # 두 버전 모두 빌드
make pipe       # pipe 버전만
make shm        # shm 버전만
make memref     # 기준값용 memcpy 속도 측정 프로그램 (IPC 없음)
make quick      # 반복 수를 줄여 동작만 확인 (results_quick/)
make run        # 전체 측정 (= bash ./run_all.sh)
make ipc-check  # 7조 key로 남은 IPC 자원이 있는지 확인
make ipc-clean  # kill -9 등으로 남은 자원 강제 삭제
make clean
```

전체 측정 결과는 `results/<날짜_시각>/`에 쌓인다.

| 파일 | 내용 |
|---|---|
| `pipe_summary.csv`, `shm_summary.csv` | 조건별 통계 한 줄씩 (지연, 처리량 각각) |
| `pipe_latency_raw.csv`, `shm_latency_raw.csv` | 왕복 지연 원본 값(ns). 분포 그래프용 |
| `env.txt` | OS, 커널, CPU, gcc, 메모리, IPC 한도 |
| `pipe.log`, `shm.log` | 화면 출력 |
| `memcpy_ref.csv` | IPC 없이 memcpy만 했을 때 속도 (shm 처리량 상한 비교용) |
| `ipcs_after.txt` | 측정 직후 `ipcs -m -s` 결과와 남은 자원 확인 |

옵션(두 프로그램 공통, `run_all.sh` 뒤에 붙이면 그대로 넘어감):

```
-s 64,1K,16K,256K,1M   메시지 크기 (기본값이 이 5단계)
-c 1,2,4,8             동시 클라이언트 수 (최대 8)
-w 20                  지연 측정 워밍업 왕복 수 (클라이언트 스레드당)
-r 100                 지연 측정 기록 왕복 수 (클라이언트 스레드당)
-t 30                  처리량 측정 반복 수
-k N                   처리량 1회에 스레드당 보낼 메시지 수 (기본: 약 16MB가 되도록 자동, 16~10000개)
-m lat|thr|both        측정 종류
-q 4                   shm 요청 링 버퍼 칸 수 (shm_bench만)
-W 1000                첫 조건 전 전체 워밍업 시간(ms). 0이면 생략
-o DIR                 결과 폴더
```

## 측정 방법

### 왕복 지연 (latency)

클라이언트 스레드가 `헤더 16B + payload`를 보내면 서버 스레드가 같은 크기로 그대로 돌려준다. 스레드마다 워밍업 20회를 버리고 100회를 기록한다. 동시 클라이언트가 N이면 한 조건의 표본은 N x 100개다.

타임스탬프 위치 (`bench.c`의 `lat_worker`):

- t0: 요청을 IPC로 내보내기 직전. shm에서는 빈 칸을 기다리는 P 연산과 공유 메모리로의 복사가 t0 뒤에 있으므로 측정에 들어간다.
- t1: 응답을 클라이언트 버퍼로 끝까지 읽은 직후.
- 측정 중에는 `uint64_t` 배열에 저장만 한다. 응답 내용 검증(memcmp), 통계, printf, CSV 기록은 모두 측정이 끝난 뒤에 한다.

### 처리량 (throughput)

모든 클라이언트 스레드가 barrier에서 맞춰 출발해 각자 k개를 응답 없이 연달아 보낸다. 마지막 메시지만 `STREAM_ACK`로 보내고 서버는 그 메시지를 받은 뒤 헤더만 응답한다. 응답을 받았다는 것은 서버가 k개를 다 읽었다는 뜻이다.

- t_start: barrier 통과 직후, 첫 메시지를 보내기 직전.
- t_end: `STREAM_ACK`의 응답을 받은 직후.
- 반복 1회의 소요 시간은 모든 스레드 중 가장 이른 t_start부터 가장 늦은 t_end까지로 잡는다.
- 초당 메시지 수 = N x k / 소요 시간, 초당 바이트 = 초당 메시지 수 x payload 크기 (헤더 16B는 빼고 계산).
- 워밍업 2회 뒤 30회를 기록한다.

### 전체 워밍업과 데이터 검사

- 첫 조건 전에 8개 채널 모두 측정할 크기들을 돌아가며 1초 동안 왕복한다(기록 안 함). 첫 조건만 프로세스 시작 직후 상태에서 재는 일을 막는다. 조건마다 하는 워밍업(지연 20회, 처리량 2회)은 따로 있다.
- 클라이언트 채널마다 payload 값이 다르다(`pattern_byte`). 서버는 받은 메시지마다 payload 첫 바이트와 마지막 바이트를 확인하고(바이트 2개라 측정에 주는 영향은 무시할 만함), 틀린 횟수를 `OP_REPORT`로 알려 준다. 0이 아니면 클라이언트가 바로 멈춘다. 지연 측정은 끝난 뒤 마지막 응답의 헤더(채널 번호, 크기)와 payload 전체를 memcmp로 비교한다. 다른 채널 응답이 섞이거나 복사가 덜 되면 여기서 걸린다.

### 통계

- 평균, 중앙값, p99, 최대, 최소, 표준편차를 낸다.
- p99는 nearest-rank 방식이다. 정렬 후 `ceil(0.99 x n)`번째 값. 표본이 100개보다 적으면 p99가 최대값과 같게 나온다. 기본값(스레드당 100회)에서 클라이언트가 1개면 표본이 100개라 p99는 두 번째로 큰 값이다.
- 지연 모드 행의 `msgs_per_sec`, `bytes_per_sec`은 평균(중앙값) 왕복 시간으로 환산한 참고값이다. 처리량 비교에는 throughput 행을 쓴다.

### 자원 사용량

조건마다 시작 전과 끝난 뒤에 클라이언트와 서버 양쪽의 `getrusage(RUSAGE_SELF)`를 찍어 차이를 기록한다. 서버 값은 채널 0으로 `OP_REPORT`를 보내 받아 온다(측정 구간 밖).

- `cli_utime_ms`, `cli_stime_ms`, `cli_nvcsw`(자발적 문맥 교환), `cli_nivcsw`(비자발적), `srv_*`도 같은 뜻.
- `cli_vmrss_kb`, `srv_vmrss_kb`: 조건이 끝난 시점의 `/proc/self/status` VmRSS.
- 주의: VmRSS에는 그 프로세스가 건드린 공유 메모리 페이지도 들어간다. shm 클라이언트는 세그먼트 전체를 처음에 0으로 채우므로 세그먼트 크기만큼 크게 나온다. 보고서에서 pipe와 VmRSS를 비교할 때 이 점을 같이 적는 게 좋다.
- 스레드 생성과 join이 rusage 구간 안에 들어간다. 조건마다 같은 양이라 비교에는 영향이 작지만 64B처럼 짧은 조건에서는 섞여 있다.

## 값을 읽을 때 알아 둘 것

**클라이언트가 많을수록 지연이 낮게 나올 수 있다.** 코드 오류가 아니다. 클라이언트 1개일 때는 서버와 클라이언트가 번갈아 잠들고 깨어나는데, CPU가 쉬는 상태(WSL2에서는 가상 CPU가 하이퍼바이저에 반납된 상태)에서 깨어나는 데 수십 us가 걸리고 이 시간이 왕복 지연에 그대로 들어간다. 클라이언트가 8개면 다른 스레드들 때문에 CPU가 계속 깨어 있어서 이 비용이 줄거나 없어진다. 그래서 분포가 둘로 갈라지고(빠른 묶음과 느린 묶음), 평균이 중앙값보다 낮게 나오는 경우도 생긴다. `*_latency_raw.csv`로 히스토그램을 그려 보면 두 묶음이 보인다.

직접 확인하는 방법 (보고서에 넣으려면 네 컴퓨터에서 낸 값만 쓸 것):

```bash
./pipe_bench -s 64,1K -c 1 -m lat -o results_idle                   # 그냥 실행
taskset -c 0 ./pipe_bench -s 64,1K -c 1 -m lat -o results_pin       # 서버, 클라이언트를 CPU 0 하나에 고정
```

고정하면 상대 스레드를 다른 CPU에서 깨우지 않고 같은 CPU에서 바로 넘겨받으므로, 깨어나는 비용이 원인이라면 지연이 크게 줄어든다. 이 비교를 보고서의 분석 거리로 써도 된다.

**shm 처리량이 메모리 대역폭에 가깝게 나올 수 있다.** 256KB x 링 4칸 x 클라이언트 4개면 다루는 데이터가 약 4MB라 대부분 CPU 캐시 안에서 복사된다. 캐시 안 memcpy는 DRAM보다 훨씬 빠르다. `memcpy_ref.csv`(같은 크기, 같은 스레드 수로 memcpy만 한 속도)와 비교하면 shm이 상한에 얼마나 가까운지 볼 수 있다. shm은 메시지마다 복사를 두 번 하므로(클라이언트 -> 공유 메모리 -> 서버), payload 기준 처리량은 memcpy 속도의 절반 정도가 상한이다.

**값이 실행마다 흔들린다.** 특히 작은 메시지의 지연이 그렇다. `make run`을 두세 번 돌려서 경향이 같은지 확인하고 쓰는 게 안전하다.

## 프로세스와 스레드 구조

`fork`로 서버(자식)와 클라이언트(부모)를 나눈다. 두 버전이 같은 드라이버(`bench.c`)를 쓰고 IPC 부분만 다르다.

| 프로세스 | 스레드 | 수 |
|---|---|---|
| 클라이언트 | main(조건 반복, 통계, CSV) + 신호 처리 스레드 + 워커 N개 | 3 ~ 10 |
| 서버 | main + 신호 처리 스레드 + 채널 핸들러 8개 | 10 |

경쟁 상태를 막는 방법:

- 채널 하나는 한 번에 워커 하나와 핸들러 하나만 쓴다. 송수신 버퍼와 결과 배열도 스레드마다 따로 있다.
- 워커 출발은 `pthread_barrier`로 맞춘다. main은 `pthread_join` 뒤에만 워커 결과를 읽는다.
- shm 요청 링은 채널마다 생산자 1, 소비자 1이다. `EMPTY`(초기값 = 칸 수), `FULL`(0) 세마포어로 칸 순서를 지키고, 응답 칸은 `RESP`(0)로 넘겨준다. head는 클라이언트만, tail은 서버만 바꾼다.
- 정리 함수는 atomic 플래그로 한 번만 실행된다.

검사: 이 코드를 ThreadSanitizer(pipe)와 AddressSanitizer + UBSan(shm)으로 한 번씩 돌려 경고가 없는 것을 확인했다. 직접 돌려 보려면:

```bash
gcc -O1 -g -fsanitize=thread -pthread -o tsan_pipe pipe_bench.c common.c bench.c -lm
./tsan_pipe -s 64,16K -c 1,4 -r 30 -t 30 -k 32 -o results_tsan
```

## IPC 자원 이름과 정리

- System V key는 7조 범위(60070~60079)에서 60071(shm), 60072(세마포어)를 쓴다. 16진수로 `0x0000eaa7`, `0x0000eaa8`이라 `ipcs`에서는 이 값으로 보인다.
- pipe 버전은 이름 있는 자원이 없다. fd는 프로세스가 끝나면 커널이 닫는다.
- 신호 처리: main이 스레드를 만들기 전에 SIGINT, SIGTERM, SIGHUP, SIGQUIT, SIGCHLD를 막고, 전용 스레드가 `sigwait`로 받는다. 신호를 받으면 서버에 SIGTERM을 보내 회수하고, 세마포어와 shm을 `IPC_RMID`로 지운 뒤 끝난다. 서버가 먼저 죽어도(SIGCHLD) 같은 정리를 한다.
- 서버는 Ctrl-C(SIGINT)를 무시하고 부모가 보낸 SIGTERM으로만 끝난다. `PR_SET_PDEATHSIG`를 걸어 두어 부모가 kill -9로 죽어도 서버가 남지 않는다.
- kill -9는 잡을 수 없어서 shm과 세마포어가 남는다. 다음에 `shm_bench`를 실행하면 시작할 때 남은 자원을 지운다. 바로 지우려면 `make ipc-clean`.

확인 방법:

```bash
./shm_bench &          # 실행 중
ipcs -m -s             # 0x0000eaa7, 0x0000eaa8 이 보임
kill -INT %1           # 또는 Ctrl-C
make ipc-check         # "남은 IPC 자원 없음"
```

## 측정할 때

- Docker Desktop은 끄고, 다른 프로그램도 되도록 닫는다. `run_all.sh`가 dockerd가 돌고 있으면 경고를 띄운다.
- 노트북은 전원을 연결하고 Windows 전원 모드를 "최고 성능"으로 두는 게 좋다. 배터리 모드에서는 CPU 클럭이 내려가 값이 흔들린다.
- 같은 조건을 두세 번 돌려서 값이 비슷하게 나오는지 보고, 보고서에는 그중 한 번의 결과 폴더를 쓴다.

## 파일

| 파일 | 내용 |
|---|---|
| `common.h`, `common.c` | 헤더 형식, 옵션, 시간, 통계, rusage, VmRSS, CSV |
| `bench.h`, `bench.c` | 측정 드라이버: 서버/클라이언트 스레드, 타임스탬프, 신호 처리 |
| `pipe_bench.c` | 버전 1: pipe 송수신 |
| `shm_bench.c` | 버전 2: shared memory 링 버퍼 + 세마포어 |
| `memcpy_ref.c` | 기준값: IPC 없이 memcpy만 한 속도 |
| `Makefile`, `run_all.sh` | 빌드, 전체 측정 |

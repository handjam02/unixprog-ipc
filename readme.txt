UNIX 시스템 프로그래밍 설계과제 -- 7조 (손재민, 박건석)
pipe vs System V shared memory: 오디오 청크 분석 서버
==========================================================================

1. 개요
--------------------------------------------------------------------------
클라이언트가 16비트 PCM(모노, 48kHz) 청크를 보내면 서버가 RMS 음량, 피크,
무음 구간 수를 계산해 돌려준다. 정규화 모드에서는 피크가 0.9가 되도록 고친
오디오 전체를 돌려준다. 같은 server.c / client.c에 IPC 구현 하나를 붙여
pipe 버전과 shared memory 버전을 만들고 성능을 비교한다.

  버전   IPC                                   동기화
  pipe   FIFO (공개 1개 + 클라이언트별 req/rsp)  블로킹 read/write, poll()
  shm    System V shm (key 60070)              System V 세마포어 (key 60073)

2. 디렉터리
--------------------------------------------------------------------------
  Makefile        전체 빌드 (아래 4절)
  readme.txt      이 파일
  common/         공통 모듈 (두 버전이 같이 씀)
    audio.c       청크 분석, 정규화, 시험용 PCM 생성
    stats.c       서버 공유 통계 (클라이언트별 통계, 전체 통계)
    queue.c       작업 큐 (mutex + 조건변수 2개)
    timing.c      시간, 측정값 요약(평균/중앙값/p99), VmRSS
    csv.c         결과 CSV 저장
    util.c        readn/writen, 신호 막기
  app/            응용
    ipc.h         IPC 공통 인터페이스 (server.c, client.c는 이것만 부른다)
    ipc_pipe.c    pipe 버전 구현
    ipc_shm.c     shared memory 버전 구현
    server.c      서버
    client.c      클라이언트
    ipcclean.c    make cleanipc용 정리 프로그램
  phase1/         1단계 IPC 기초 성능 벤치마크 (pipe_bench, shm_bench)
    results/      1단계 측정 원자료 (CSV, env.txt)
  predictions/    성능 예측 원문 (구현 전 작성, 커밋 c89eed7)
  scripts/        응용 측정 스크립트 (run_app.sh, plot.py)
  data/           응용 측정 결과 CSV

3. 실행 환경
--------------------------------------------------------------------------
측정은 모두 아래 노트북 한 대에서 했다.

  CPU / 코어    Intel Core i5-1135G7 @ 2.40GHz, 4코어 8스레드 (nproc 8)
  메모리        WSL 할당 7.6GiB
  OS / 커널     Ubuntu 26.04.1 LTS / 6.18.58.1-microsoft-standard-WSL2
  컴파일러      gcc 15.2.0, GNU Make 4.4.1
  가상화        WSL2 (Windows 노트북)
  pipe 버퍼     기본 64KB, pipe-max-size 1048576
  작업 위치     ~/osproject (Linux 파일 시스템 안. /mnt/c 아래는 쓰지 않음)

측정 전에 Docker Desktop과 브라우저 등 무거운 프로그램을 끄고, 전원 어댑터를
꽂은 상태에서 쟀다. 측정 때의 환경 값은 결과 폴더의 env.txt에 남는다.

4. 빌드
--------------------------------------------------------------------------
  make            전부 빌드 (1단계 벤치마크 + 있는 IPC 구현의 응용)
  make bench      1단계 벤치마크만 (phase1/pipe_bench, phase1/shm_bench)
  make app_pipe   app/server_pipe, app/client_pipe
  make app_shm    app/server_shm,  app/client_shm
  make tsan       경쟁 상태 검사용 빌드 (-fsanitize=thread), app/*_tsan
  make cleanipc   비정상 종료로 남은 IPC 자원 삭제 (7절)
  make clean      실행 파일 삭제

컴파일 옵션: -O2 -g -Wall -Wextra -std=gnu11 -pthread
측정은 항상 기본 옵션 빌드로 한다. tsan 빌드는 검사용이라 측정에 쓰지 않는다.

5. 실행 방법
--------------------------------------------------------------------------
서버를 먼저 띄우고, 다른 터미널에서 클라이언트를 띄운다. 서버는 Ctrl+C로 끝낸다.
(아래는 pipe 버전 예. shm 버전은 _pipe를 _shm으로 바꾼다)

  $ app/server_pipe
  $ app/client_pipe -s 64K -m stats -n 1000

클라이언트 8개를 동시에:

  $ for i in $(seq 8); do app/client_pipe -s 256K -m norm -c 8 -o data/app & done; wait

서버 옵션 (app/server_pipe, app/server_shm)
  -t N        워커 스레드 수 (기본 4)
  -q N        작업 큐 칸 수 (기본 16)
  -K N        전체 통계 쓰기 주기: 청크 N개마다 1번 (기본 8)
  -L per|table     클라이언트별 통계 락: 클라이언트별 mutex / 테이블 전체에 mutex 1개 (실험 E1)
  -G rwlock|mutex  전체 통계 락 종류 (실험 E2)
  -I MS       통계 보고 스레드 간격 (기본 100ms)
  -o DIR      결과 폴더 (없으면 화면 요약만)
  -l LABEL    결과 파일 이름과 요약 줄에 붙일 조건 이름 (기본 run)
  -X OPT      설계 개선 실험용 IPC 옵션. 기본판(E0)은 쓰지 않는다
                shm: inplace = E5 제자리 처리 (요청 칸에서 바로 처리하고
                     정규화 결과도 그 칸에 덮어씀, 응답 칸에는 헤더만)

클라이언트 옵션 (app/client_pipe, app/client_shm)
  -s SIZE     청크 크기: 4K, 16K, 64K, 256K (기본 16K, 최대 256K)
  -m MODE     stats (응답 32바이트) | norm (요청과 같은 크기의 오디오)
  -n N        기록하는 청크 수 (기본 1000)
  -w N        워밍업으로 버리는 청크 수 (기본 100)
  -W N        창 크기 1..4: 응답을 기다리지 않고 내보낼 수 있는 요청 수 (기본 1)
  -c N        기록용: 이번 실행의 동시 클라이언트 수
  -r N        기록용: 실행 회차
  -S SEED     PCM 시드 (기본: 서버가 준 클라이언트 번호 + 1)
  -o DIR      결과 폴더

스레드 구성
  서버        수신 1, 워커 W(기본 4), 통계 보고 1, 시그널 1
  클라이언트  생성 1, 송신 1, 수신 1

6. 결과 파일
--------------------------------------------------------------------------
-o DIR을 주면 다음 파일이 생긴다. 측정 구간 안에서는 파일을 쓰지 않고,
값을 메모리 배열에 모았다가 끝난 뒤 저장한다.

  client_summary.csv
      클라이언트 실행마다 한 줄. 요청 지연(평균, 중앙값, p99, 최대, 최소,
      표준편차, us), 처리량(청크/초, MB/s), 보낸 청크와 바이트 수,
      잘못된 응답 수, CPU 시간, 컨텍스트 스위치, VmRSS, VmHWM
  client_<ipc>_<mode>_<size>_c<N>_r<회차>_id<번호>_raw.csv
      청크별 요청 지연 원자료 (seq, rtt_us)
  server_summary.csv
      서버 실행마다 한 줄. 처리한 청크와 바이트 수, CPU 시간,
      컨텍스트 스위치, VmRSS, VmHWM, 실험 설정(워커, 큐, K, 락 종류)
  server_<ipc>_<label>_chunks.csv
      청크별 서버 구간 시간(ns): 큐 대기, 처리, 락 대기 3종, 응답 송신
  server_<ipc>_<label>_report.csv
      통계 보고 스레드가 일정 간격으로 남긴 전체 통계

측정 지점
  요청 지연   클라이언트 송신 스레드의 송신 직전 ~ 수신 스레드가 응답을 다 받은 직후
  큐 대기     서버 수신 스레드가 큐에 넣은 때 ~ 워커가 꺼낸 때
  처리        워커의 분석(+정규화) 시작 ~ 끝
  락 대기     lock 호출 직전 ~ lock 반환 직후
시간은 모두 clock_gettime(CLOCK_MONOTONIC)으로 잰다.

동작 검증: 서버는 끝날 때 전체 통계 청크 수와 클라이언트별 합계가 같은지 출력한다.
클라이언트들의 sent_chunks 합과 서버 chunks가 같아야 한다.

7. 측정 재현
--------------------------------------------------------------------------
1단계 벤치마크
  $ cd phase1 && ./run_all.sh
  결과: phase1/results/<날짜_시각>/ (요약 CSV, 지연 원자료, env.txt, ipcs 확인)
  옵션과 측정 방법은 phase1/README.md 참고.

응용 측정
  $ scripts/run_app.sh -q          짧은 시험: 64조건 1회씩, 청크 200개 (약 20초, 전체 시간 추정)
  $ scripts/run_app.sh             E0 전체: 64조건 x 30회, 청크 1000개 (약 40분)
  $ scripts/run_app.sh -s "64K 256K" -c 8 -E "-L table" -T e1_table    추가 실험 예
  옵션 목록은 scripts/run_app.sh -h.
  결과: data/app/<실험 이름>_<날짜_시각>/ 아래 6절의 CSV와
    env.txt (측정 환경), checks.csv (조건마다 합계 검증, 실패 수, 남은 IPC 자원),
    run.log (진행 기록), ipcs_after.txt (측정 직후 ipcs). 원자료는 .gz로 압축한다.
  $ scripts/run_strace.sh         원인 분석용 시스템 콜 횟수 (strace -c -f, 대표 4조건 x pipe/shm, 약 1분)
  결과: data/app/strace_<날짜_시각>/ (조건별 strace -c 출력, summary.csv = 청크당 횟수).
    strace가 실행을 느리게 하므로 여기서 나온 시간 값은 쓰지 않는다.
  (run_app.sh는 조건마다 서버를 새로 띄워 서버 CPU 시간과 VmHWM이 조건별 값이 되게 하고,
   같은 조건의 pipe와 shm을 연달아 잰다)

8. IPC 자원 정리
--------------------------------------------------------------------------
  자원                     이름 또는 key
  응용 shm 세그먼트        System V shm key 60070
  응용 세마포어 집합       System V sem key 60073
  1단계 shm 세그먼트       System V shm key 60071
  1단계 세마포어 집합      System V sem key 60072
  공개 FIFO               /tmp/osproject_7_srv
  클라이언트별 FIFO        /tmp/osproject_7_<pid>_req, /tmp/osproject_7_<pid>_rsp

  - 정상 종료: 클라이언트는 BYE를 보내고 자기 FIFO를 지운다. 서버는 Ctrl+C(SIGINT)나
    SIGTERM을 받으면 시그널 스레드가 종료를 시작하고, 워커를 join한 뒤 자원을 해제한다.
  - 서버는 시작할 때 같은 이름의 남은 자원이 있으면 지우고 새로 만든다.
  - kill -9 등 비정상 종료 뒤에는:
      $ make cleanipc
    위 key와 FIFO를 모두 지운 뒤, ipcs -m -s -q와 ls /tmp/osproject_7_* 결과를 보여 준다.

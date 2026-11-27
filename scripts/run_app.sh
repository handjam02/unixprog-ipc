#!/bin/bash
# run_app.sh -- 응용 측정 E0 (7조, 기획서 7장)
#
#   scripts/run_app.sh                 E0 전체: IPC 2 x 청크 4 x 모드 2 x 클라이언트 4 = 64조건, 30회
#   scripts/run_app.sh -q              짧은 시험: 모든 조건 1회, 청크 200개 (전체 시간 추정용)
#   scripts/run_app.sh -s "64K 256K" -c 8 -R 5        일부 조건만
#   scripts/run_app.sh -E "-L table" -T e1_table      서버 옵션을 바꾼 추가 실험 (E1, E2 ...)
#
# 옵션
#   -i "pipe shm"            IPC
#   -s "4K 16K 64K 256K"     청크 크기
#   -m "stats norm"          응답 모드
#   -c "1 2 4 8"             동시 클라이언트 수
#   -R 30                    조건당 실행 횟수
#   -n 1000                  실행마다 클라이언트 하나가 기록하는 청크 수
#   -w 100                   워밍업 청크 수 (기록 안 함)
#   -W 1                     창 크기 (5.1절 기본 1, 추가 조건 4)
#   -E "..."                 서버에 더 넘길 옵션 (예: "-L table", "-G mutex -K 1")
#   -T TAG                   결과 폴더 이름에 붙일 실험 이름 (기본 e0, -q면 quick)
#   -P 2                     조건 사이 쉬는 초 (1단계 E5의 회차별 하락 대비, 10.3절)
#   -t 300                   클라이언트 하나의 시간 제한(초). 넘으면 실패로 기록
#   -q                       짧은 시험 (-R 1 -n 200)
#
# 순서 (10.3절: 비교할 두 버전은 연달아 잰다)
#   모드 -> 크기 -> 클라이언트 수 -> IPC 순으로 돌고, 한 조건(IPC 포함)마다 서버를 새로 띄워
#   R회를 연달아 잰다. 서버 CPU 시간과 VmHWM이 조건별 값이 되게 하려는 것이다(server.c 주석).
#
# 결과: data/app/<TAG>_<날짜_시각>/
#   env.txt              측정 환경 (3장)
#   client_summary.csv   클라이언트 실행마다 한 줄
#   client_*_raw.csv.gz  청크별 요청 지연 원자료
#   server_summary.csv   서버(조건)마다 한 줄
#   server_*_chunks.csv.gz, server_*_report.csv   서버 구간 측정, 통계 보고 스레드 기록
#   checks.csv           조건마다 합계 검증과 실패 수, 남은 IPC 자원 (6.3절, 8.4절)
#   run.log              진행 기록, 서버와 클라이언트 출력
#   ipcs_after.txt       측정 직후 IPC 자원 확인 (보고서 5.3절)
set -u
cd "$(dirname "$0")/.."

IPCS="pipe shm"; SIZES="4K 16K 64K 256K"; MODES="stats norm"; CLIENTS="1 2 4 8"
RUNS=30; COUNT=1000; WARMUP=100; WINDOW=1; SRV_EXTRA=""; TAG=""; PAUSE=2; CTIMEOUT=300; QUICK=0

while getopts "i:s:m:c:R:n:w:W:E:T:P:t:qh" o; do
    case $o in
    i) IPCS=$OPTARG ;; s) SIZES=$OPTARG ;; m) MODES=$OPTARG ;; c) CLIENTS=$OPTARG ;;
    R) RUNS=$OPTARG ;; n) COUNT=$OPTARG ;; w) WARMUP=$OPTARG ;; W) WINDOW=$OPTARG ;;
    E) SRV_EXTRA=$OPTARG ;; T) TAG=$OPTARG ;; P) PAUSE=$OPTARG ;; t) CTIMEOUT=$OPTARG ;;
    q) QUICK=1 ;;
    *) sed -n '2,26p' "$0"; exit 2 ;;
    esac
done

# -q는 옵션 순서와 상관없이 같은 뜻. -T를 주지 않았을 때만 이름을 quick으로
if [ "$QUICK" = 1 ]; then RUNS=1; COUNT=200; fi
[ -z "$TAG" ] && TAG=$([ "$QUICK" = 1 ] && echo quick || echo e0)

make --no-print-directory -s app_pipe app_shm || exit 1

OUT="data/app/${TAG}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
LOG="$OUT/run.log"
CHECKS="$OUT/checks.csv"
echo "ipc,mode,size,clients,runs,client_fail,server_chunks,client_sent_sum,match,leftover_ipc,seconds" > "$CHECKS"

# ---- 측정 환경 기록 (기획서 3장) ----
{
    echo "측정 시각: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "OS: $(. /etc/os-release && echo "$PRETTY_NAME")"
    echo "커널: $(uname -r)"
    echo "CPU: $(lscpu | sed -n 's/^Model name:[[:space:]]*//p')"
    echo "nproc: $(nproc)"
    echo "gcc: $(gcc --version | head -1)"
    echo "make: $(make --version | head -1)"
    echo "pipe 기본 버퍼: 64KB (pipe-max-size=$(cat /proc/sys/fs/pipe-max-size))"
    echo "git: $(git rev-parse --short HEAD 2>/dev/null)$(git diff --quiet 2>/dev/null || echo ' (커밋 안 한 변경 있음)')"
    echo "실행: $0 $*"
    echo "조건: ipc=[$IPCS] size=[$SIZES] mode=[$MODES] clients=[$CLIENTS] runs=$RUNS count=$COUNT warmup=$WARMUP window=$WINDOW server_extra=[$SRV_EXTRA]"
    echo
    free -h
    echo
    df -h /dev/shm
    echo
    ipcs -l
} > "$OUT/env.txt" 2>&1

if pgrep -x dockerd >/dev/null 2>&1 || pgrep -f "Docker Desktop" >/dev/null 2>&1; then
    echo "주의: Docker가 실행 중입니다. 끄고 측정하는 게 좋습니다 (기획서 3장)." | tee -a "$LOG"
fi
make --no-print-directory -s cleanipc > /dev/null 2>&1

# ---- Ctrl+C: 띄운 프로세스를 끝내고 자원 정리 ----
SRV_PID=""
on_abort() {
    echo; echo "중단됨. 서버와 클라이언트를 끝내고 IPC 자원을 정리합니다." | tee -a "$LOG"
    pkill -P $$ 2>/dev/null
    [ -n "$SRV_PID" ] && kill -INT "$SRV_PID" 2>/dev/null
    sleep 1
    pkill -9 -f "app/(server|client)_(pipe|shm)( |$)" 2>/dev/null
    make --no-print-directory -s cleanipc | tee -a "$LOG"
    exit 130
}
trap on_abort INT TERM

leftover() {   # 7조 key나 FIFO가 남았으면 1
    ipcs -m -s 2>/dev/null | grep -qEi '0x0000eaa[69]' && return 0
    ls /tmp/osproject_7_* >/dev/null 2>&1 && return 0
    return 1
}

n_cond=0
for m in $MODES; do for s in $SIZES; do for c in $CLIENTS; do for i in $IPCS; do
    n_cond=$((n_cond + 1)); done; done; done; done
echo "조건 $n_cond개 x $RUNS회, 결과: $OUT" | tee -a "$LOG"

T_ALL=$(date +%s)
k=0
for mode in $MODES; do
for size in $SIZES; do
for nc in $CLIENTS; do
for ipc in $IPCS; do
    k=$((k + 1))
    label="${mode}_${size}_c${nc}"
    t0=$(date +%s)
    printf '[%d/%d] %s %s ... ' "$k" "$n_cond" "$ipc" "$label" | tee -a "$LOG"

    # 서버 띄우고 준비될 때까지 기다림
    SRV_OUT="$OUT/.server_$$.out"
    # shellcheck disable=SC2086
    app/server_$ipc -o "$OUT" -l "$label" $SRV_EXTRA > "$SRV_OUT" 2>&1 &
    SRV_PID=$!
    for _ in $(seq 50); do
        grep -q "준비됨" "$SRV_OUT" 2>/dev/null && break
        kill -0 "$SRV_PID" 2>/dev/null || break
        sleep 0.1
    done
    if ! grep -q "준비됨" "$SRV_OUT" 2>/dev/null; then
        echo "서버 시작 실패" | tee -a "$LOG"
        cat "$SRV_OUT" >> "$LOG"
        kill -9 "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""
        make --no-print-directory -s cleanipc > /dev/null 2>&1
        echo "$ipc,$mode,$size,$nc,0,-1,0,0,0,0,0" >> "$CHECKS"
        continue
    fi

    fail=0
    for run in $(seq 1 "$RUNS"); do
        pids=()
        for _ in $(seq 1 "$nc"); do
            timeout "$CTIMEOUT" app/client_$ipc -s "$size" -m "$mode" -n "$COUNT" -w "$WARMUP" \
                -W "$WINDOW" -c "$nc" -r "$run" -o "$OUT" >> "$LOG" 2>&1 &
            pids+=($!)
        done
        for p in "${pids[@]}"; do
            wait "$p" || fail=$((fail + 1))
        done
    done

    # 서버 종료 (Ctrl+C와 같은 경로, 8.4절 2번)
    kill -INT "$SRV_PID" 2>/dev/null
    for _ in $(seq 100); do kill -0 "$SRV_PID" 2>/dev/null || break; sleep 0.1; done
    if kill -0 "$SRV_PID" 2>/dev/null; then
        echo "  서버가 10초 안에 안 끝나 kill -9" >> "$LOG"
        kill -9 "$SRV_PID"
    fi
    wait "$SRV_PID" 2>/dev/null
    SRV_PID=""
    cat "$SRV_OUT" >> "$LOG"
    rm -f "$SRV_OUT"

    # 합계 검증 (6.3절): 이 조건 클라이언트들의 sent_chunks 합 = 서버 chunks
    srv_chunks=$(awk -F, -v ipc="$ipc" -v lb="$label" '$1==ipc && $2==lb {v=$8} END {print v+0}' \
                 "$OUT/server_summary.csv" 2>/dev/null)
    cli_sum=$(awk -F, -v ipc="$ipc" -v m="$mode" -v sz="$(numfmt --from=iec "$size")" -v nc="$nc" \
              -v w="$WINDOW" 'NR>1 && $1==ipc && $2==m && $3==sz && $4==nc && $5==w {s+=$19} END {print s+0}' \
              "$OUT/client_summary.csv" 2>/dev/null)
    match=$([ "$srv_chunks" = "$cli_sum" ] && echo 1 || echo 0)
    left=0
    if leftover; then
        left=1
        make --no-print-directory -s cleanipc >> "$LOG" 2>&1
    fi
    # 원자료 압축 (E0 전체면 압축 전 약 290MB)
    [ -f "$OUT/server_${ipc}_${label}_chunks.csv" ] && gzip -f "$OUT/server_${ipc}_${label}_chunks.csv"
    gzip -f "$OUT"/client_${ipc}_${mode}_$(numfmt --from=iec "$size")_c${nc}_r*_raw.csv 2>/dev/null

    dt=$(( $(date +%s) - t0 ))
    echo "$ipc,$mode,$size,$nc,$RUNS,$fail,$srv_chunks,$cli_sum,$match,$left,$dt" >> "$CHECKS"
    msg="${dt}s"
    [ "$fail" -gt 0 ] && msg="$msg, 클라이언트 실패 $fail"
    [ "$match" = 0 ] && msg="$msg, 합계 불일치(서버 $srv_chunks / 클라이언트 $cli_sum)"
    [ "$left" = 1 ] && msg="$msg, 남은 IPC 자원 지움"
    echo "$msg" | tee -a "$LOG"

    sleep "$PAUSE"
done
done
done
done

# ---- 측정 후 IPC 자원 확인 (보고서 5.3절) ----
{
    echo "## ipcs -m -s -q (측정 직후, $(date '+%H:%M:%S'))"
    ipcs -m -s -q
    echo "## ls /tmp/osproject_7_*"
    ls /tmp/osproject_7_* 2>&1
} > "$OUT/ipcs_after.txt"

T=$(( $(date +%s) - T_ALL ))
bad=$(awk -F, 'NR>1 && ($6!=0 || $9!=1 || $10!=0)' "$CHECKS" | wc -l)
echo
echo "끝: ${T}초 ($((T / 60))분). 문제 있는 조건 ${bad}개 (checks.csv에서 client_fail, match, leftover_ipc 확인)" | tee -a "$LOG"
if [ "$QUICK" = 1 ]; then
    echo "전체 E0(30회, 청크 1000개) 예상: 약 $(( T * 30 * (1000 + 100) / (200 + 100) / 60 ))분 (조건 사이 쉬는 시간 포함 대략값)"
fi
echo "결과 폴더: $OUT"

#!/bin/bash
# run_strace.sh -- 원인 분석용 시스템 콜 횟수 측정 (7조, 기획서 2장 "원인 분석 근거", 4.4절)
#
#   scripts/run_strace.sh            대표 조건 4개 x pipe/shm, 조건마다 1회
#   scripts/run_strace.sh -n 200     짧은 시험
#
# strace -c -f로 서버와 클라이언트 각각의 시스템 콜 횟수와 오류 수를 센다.
# strace는 실행을 크게 느리게 하므로 여기서 나온 시간 값은 쓰지 않는다 (지연, 처리량은 run_app.sh 결과만 쓴다).
# 횟수는 청크 수로 나눠 "청크 하나당 시스템 콜 수"로 비교한다 (scripts/strace_sum.py).
#
# 옵션
#   -C "stats:4K:1 stats:4K:8 norm:256K:1 norm:256K:8"   조건 (모드:크기:클라이언트 수)
#   -i "pipe shm"     IPC
#   -n 1000           클라이언트 하나가 기록하는 청크 수 (워밍업 100은 따로)
#   -T strace         결과 폴더 이름
#
# 결과: data/app/<TAG>_<날짜_시각>/
#   strace_<ipc>_<label>_server.txt, strace_<ipc>_<label>_client<i>.txt   strace -c 출력
#   run.log, env.txt, summary.csv (strace_sum.py가 만든 청크당 횟수)

cd "$(dirname "$0")/.." || exit 1

CONDS="stats:4K:1 stats:4K:8 norm:256K:1 norm:256K:8"
IPCS="pipe shm"
COUNT=1000
WARMUP=100
TAG=strace
while getopts "C:i:n:T:h" o; do
    case $o in
    C) CONDS=$OPTARG ;; i) IPCS=$OPTARG ;; n) COUNT=$OPTARG ;; T) TAG=$OPTARG ;;
    *) sed -n '2,21p' "$0"; exit 2 ;;
    esac
done

command -v strace >/dev/null || { echo "strace가 없습니다: sudo apt install strace"; exit 1; }
make --no-print-directory -s app_pipe app_shm || exit 1

OUT="data/app/${TAG}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
LOG="$OUT/run.log"
{
    echo "측정 시각: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "커널: $(uname -r)"
    echo "strace: $(strace -V | head -1)"
    echo "git: $(git rev-parse --short HEAD 2>/dev/null)$(git diff --quiet 2>/dev/null || echo ' (커밋 안 한 변경 있음)')"
    echo "실행: $0 $*"
    echo "조건: conds=[$CONDS] ipc=[$IPCS] count=$COUNT warmup=$WARMUP"
} > "$OUT/env.txt"
make --no-print-directory -s cleanipc > /dev/null 2>&1

STR_PID=""
on_abort() {
    echo; echo "중단됨. 정리합니다." | tee -a "$LOG"
    pkill -P $$ 2>/dev/null
    pkill -INT -f "app/server_(pipe|shm)( |$)" 2>/dev/null
    sleep 1
    pkill -9 -f "app/(server|client)_(pipe|shm)( |$)" 2>/dev/null
    make --no-print-directory -s cleanipc | tee -a "$LOG"
    exit 130
}
trap on_abort INT TERM

echo "결과: $OUT" | tee -a "$LOG"
for cond in $CONDS; do
    IFS=: read -r mode size nc <<< "$cond"
    for ipc in $IPCS; do
        label="${mode}_${size}_c${nc}"
        printf '%s %s ... ' "$ipc" "$label" | tee -a "$LOG"
        t0=$(date +%s)

        SRV_OUT="$OUT/.server.out"
        strace -c -f -o "$OUT/strace_${ipc}_${label}_server.txt" \
            app/server_$ipc -l "$label" > "$SRV_OUT" 2>&1 &
        STR_PID=$!
        for _ in $(seq 100); do
            grep -q "준비됨" "$SRV_OUT" 2>/dev/null && break
            sleep 0.1
        done
        if ! grep -q "준비됨" "$SRV_OUT"; then
            echo "서버 시작 실패" | tee -a "$LOG"; cat "$SRV_OUT" >> "$LOG"
            kill -9 "$STR_PID" 2>/dev/null; make --no-print-directory -s cleanipc >/dev/null 2>&1
            continue
        fi
        SRV_PID=$(pgrep -P "$STR_PID" -x "server_$ipc")

        pids=()
        for i in $(seq 1 "$nc"); do
            timeout 600 strace -c -f -o "$OUT/strace_${ipc}_${label}_client$i.txt" \
                app/client_$ipc -s "$size" -m "$mode" -n "$COUNT" -w "$WARMUP" -c "$nc" >> "$LOG" 2>&1 &
            pids+=($!)
        done
        fail=0
        for p in "${pids[@]}"; do wait "$p" || fail=$((fail + 1)); done

        # 서버 종료: strace가 아니라 서버에 SIGINT (평소 종료 경로 그대로). 서버가 끝나면 strace가 요약을 쓴다
        kill -INT "$SRV_PID" 2>/dev/null
        for _ in $(seq 100); do kill -0 "$STR_PID" 2>/dev/null || break; sleep 0.1; done
        kill -0 "$STR_PID" 2>/dev/null && { echo "  서버가 안 끝나 kill -9" >> "$LOG"; kill -9 "$SRV_PID"; }
        wait "$STR_PID" 2>/dev/null
        cat "$SRV_OUT" >> "$LOG"; rm -f "$SRV_OUT"
        make --no-print-directory -s cleanipc > /dev/null 2>&1

        msg="$(( $(date +%s) - t0 ))s"
        [ "$fail" -gt 0 ] && msg="$msg, 클라이언트 실패 $fail"
        echo "$msg" | tee -a "$LOG"
        sleep 1
    done
done

python3 scripts/strace_sum.py "$OUT" | tee -a "$LOG"
echo "결과 폴더: $OUT"

#!/bin/bash
# run_all.sh -- 1단계 전체 측정 (7조)
#   ./run_all.sh            기본 조건 전체 (크기 5단계 x 클라이언트 1,2,4,8, 지연+처리량)
#   ./run_all.sh -r 200     뒤에 붙인 옵션은 두 프로그램에 그대로 넘어간다
# 결과: results/<날짜_시각>/ 아래에 CSV, 로그, 환경 정보, ipcs 확인 결과
set -e
cd "$(dirname "$0")"

make --no-print-directory all

OUT="results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"

# ---- 측정 환경 기록 (보고서용) ----
{
    echo "측정 시각: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "OS: $(. /etc/os-release && echo "$PRETTY_NAME")"
    echo "커널: $(uname -r)"
    echo "CPU: $(lscpu | sed -n 's/^Model name:[[:space:]]*//p')"
    echo "nproc: $(nproc)"
    echo "gcc: $(gcc --version | head -1)"
    echo "make: $(make --version | head -1)"
    echo "pipe 기본 버퍼: 64KB (pipe-max-size=$(cat /proc/sys/fs/pipe-max-size))"
    echo "실행 옵션: $*"
    echo
    free -h
    echo
    ipcs -l
} > "$OUT/env.txt" 2>&1

# ---- 측정 전 확인 ----
if pgrep -x dockerd >/dev/null 2>&1; then
    echo "주의: dockerd가 실행 중입니다. Docker Desktop을 끄고 측정하는 게 좋습니다."
fi
if ! make --no-print-directory -s ipc-check; then
    echo "이전 실행이 남긴 자원이 있습니다. shm_bench가 시작할 때 지웁니다."
fi

# ---- 측정 ----
echo "== pipe =="
./pipe_bench -o "$OUT" "$@" 2>&1 | tee "$OUT/pipe.log"
sleep 2
echo "== shared memory =="
./shm_bench -o "$OUT" "$@" 2>&1 | tee "$OUT/shm.log"

sleep 2
echo "== 기준값: memcpy 속도 =="
./memcpy_ref | tee "$OUT/memcpy_ref.csv"

# ---- 측정 후 IPC 자원 확인 ----
{
    echo "## ipcs -m -s (측정 직후)"
    ipcs -m -s
    echo
    make --no-print-directory -s ipc-check
} > "$OUT/ipcs_after.txt" 2>&1 || true
tail -1 "$OUT/ipcs_after.txt"

echo
echo "결과 폴더: $OUT"
ls -1 "$OUT"

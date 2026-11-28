#!/usr/bin/env python3
"""strace_sum.py DIR -- run_strace.sh 결과를 청크 하나당 시스템 콜 수로 요약한다.

DIR/strace_<ipc>_<mode>_<size>_c<N>_{server,client<i>}.txt (strace -c 출력)를 읽어
DIR/summary.csv에 (ipc, label, side, syscall, calls, errors, calls_per_chunk)를 쓰고,
화면에는 조건마다 주요 시스템 콜의 청크당 횟수를 보여 준다.
청크 수 = 클라이언트 수 x (기록 청크 + 워미업 청크). env.txt의 count, warmup을 쓴다.
"""
import collections
import csv
import glob
import os
import re
import sys

d = sys.argv[1]
env = open(os.path.join(d, "env.txt")).read()
count = int(re.search(r"count=(\d+)", env).group(1))
warmup = int(re.search(r"warmup=(\d+)", env).group(1))

# strace -c 표의 한 줄: % time, seconds, usecs/call, calls, [errors], syscall
ROW = re.compile(r"^\s*[\d.]+\s+[\d.]+\s+\d+\s+(\d+)\s+(\d+\s+)?(\w+)\s*$")


def parse(path):
    calls, errs = collections.Counter(), collections.Counter()
    for line in open(path):
        m = ROW.match(line)
        if m and m.group(3) != "total":
            calls[m.group(3)] += int(m.group(1))
            errs[m.group(3)] += int(m.group(2) or 0)
    return calls, errs


data = {}   # (ipc, label) -> {"server": (calls, errs), "client": (calls, errs)}
for f in sorted(glob.glob(os.path.join(d, "strace_*_*.txt"))):
    m = re.match(r"strace_(\w+?)_(\w+_\w+_c\d+)_(server|client\d+)\.txt", os.path.basename(f))
    if not m:
        continue
    ipc, label, side = m.groups()
    side = "server" if side == "server" else "client"
    c, e = parse(f)
    slot = data.setdefault((ipc, label), {}).setdefault(side, (collections.Counter(), collections.Counter()))
    slot[0].update(c)
    slot[1].update(e)

with open(os.path.join(d, "summary.csv"), "w", newline="") as fp:
    w = csv.writer(fp)
    w.writerow(["ipc", "label", "side", "syscall", "calls", "errors", "calls_per_chunk"])
    for (ipc, label), sides in sorted(data.items()):
        chunks = int(label.rsplit("_c", 1)[1]) * (count + warmup)
        for side, (c, e) in sorted(sides.items()):
            for sc, n in c.most_common():
                w.writerow([ipc, label, side, sc, n, e[sc], f"{n / chunks:.3f}"])

for (ipc, label), sides in sorted(data.items(), key=lambda x: (x[0][1], x[0][0])):
    chunks = int(label.rsplit("_c", 1)[1]) * (count + warmup)
    print(f"\n{ipc} {label}  (청크 {chunks}개, 청크당 횟수, 상위 6개)")
    for side in ("client", "server"):
        if side not in sides:
            continue
        c, e = sides[side]
        tot = sum(c.values())
        top = ", ".join(f"{sc} {n / chunks:.2f}" + (f"(오류 {e[sc] / chunks:.2f})" if e[sc] else "")
                        for sc, n in c.most_common(6))
        print(f"  {side:6s} 합 {tot / chunks:6.2f}: {top}")

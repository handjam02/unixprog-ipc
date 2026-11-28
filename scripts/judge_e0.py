#!/usr/bin/env python3
"""judge_e0.py [E0_DIR] -- E0 결과로 예측 P1~P5 판정에 쓰는 표를 만든다.

조건마다 (pipe, shm)의 값을 나란히 놓는다.
  지연   클라이언트 실행별 median_us, p99_us의 중앙값 (30회 x 클라이언트 수)
  처리량 같은 회차 클라이언트들의 chunks_per_s 합 -> 30회의 중앙값
  메모리 서버 VmHWM (조건마다 서버를 새로 띄움), 클라이언트 VmHWM 중앙값
배수는 pipe / shm (지연) 또는 shm / pipe (처리량). 1보다 크면 shm이 낫다.
"""
import csv
import statistics as st
import sys
from collections import defaultdict

d = sys.argv[1] if len(sys.argv) > 1 else "data/app/e0_20261127_230125"
SZ = {"4096": "4K", "16384": "16K", "65536": "64K", "262144": "256K"}

cl = defaultdict(list)
for r in csv.DictReader(open(f"{d}/client_summary.csv")):
    cl[(r["ipc"], r["mode"], SZ[r["size"]], int(r["clients"]))].append(r)
srv = {}
for r in csv.DictReader(open(f"{d}/server_summary.csv")):
    mode, size, c = r["label"].split("_")
    srv[(r["ipc"], mode, size, int(c[1:]))] = r


def val(k):
    rows = cl[k]
    runs = defaultdict(float)
    for r in rows:
        runs[r["run"]] += float(r["chunks_per_s"])
    return dict(med=st.median(float(r["median_us"]) for r in rows),
                p99=st.median(float(r["p99_us"]) for r in rows),
                thr=st.median(runs.values()),
                shwm=int(srv[k]["vmhwm_kb"]) / 1024,
                chwm=st.median(int(r["vmhwm_kb"]) for r in rows) / 1024)


print("mode size  c | median us pipe/shm (x) | p99 us pipe/shm (x) | chunks/s pipe/shm (x) "
      "| srv VmHWM MB pipe/shm (+) | cli VmHWM MB pipe/shm")
for mode in ("stats", "norm"):
    for size in ("4K", "16K", "64K", "256K"):
        for c in (1, 2, 4, 8):
            p, s = val(("pipe", mode, size, c)), val(("shm", mode, size, c))
            print(f"{mode:5s} {size:>4s} {c} | {p['med']:7.1f} {s['med']:7.1f} ({p['med']/s['med']:4.2f}) "
                  f"| {p['p99']:7.1f} {s['p99']:7.1f} ({p['p99']/s['p99']:4.2f}) "
                  f"| {p['thr']:7.0f} {s['thr']:7.0f} ({s['thr']/p['thr']:4.2f}) "
                  f"| {p['shwm']:5.1f} {s['shwm']:5.1f} ({s['shwm']-p['shwm']:+5.1f}) "
                  f"| {p['chwm']:4.1f} {s['chwm']:4.1f}")

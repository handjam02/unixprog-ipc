#!/usr/bin/env python3
"""
plot.py -- 7조 IPC 설계과제 그래프와 요약표 생성 (보고서 2장, 6장)

사용법 (저장소 루트에서)
    python3 scripts/plot.py                 # figures/ 아래 PNG, figures/tables/ 아래 요약 CSV
    python3 scripts/plot.py -o out_dir      # 출력 폴더 지정
    python3 scripts/plot.py --phase1 phase1/results/20261122_231005

필요: python3, pandas, matplotlib  (pip install --break-system-packages pandas matplotlib)

읽는 파일 (전부 저장소에 커밋된 요약 CSV)
    phase1/results/<날짜>/{pipe,shm}_summary.csv, *_latency_raw.csv, memcpy_ref.csv
    phase1/results/extra/e1_q16, e1_q256, e5_1..5/shm_summary.csv
    data/app/<실험>_<날짜_시각>/client_summary.csv, server_summary.csv, checks.csv,
                                server_*_report.csv, env.txt
    (있으면 추가로) server_*_chunks.csv[.gz], client_*_raw.csv[.gz]   <- git에는 없고 tar에만 있음

집계 방법 (보고서 6.2절에 그대로 적을 것)
    - 한 조건 = (mode, size, clients, ipc). 30회 x 클라이언트 N개 = 30N개의 client_summary 행.
    - 평균 지연  : 행들의 mean_us 평균 (행마다 청크 수가 1000개로 같아 전체 평균과 같음)
    - 중앙값 지연: 행들의 median_us의 중앙값 (한 실행의 대표 중앙값)
    - p99 지연   : 행들의 p99_us의 중앙값 (한 실행의 대표 p99)
    - 최대 지연  : 행들의 max_us 최대
    - 처리량     : 실행(run)마다 클라이언트들의 chunks_per_s, mb_per_s를 더한 뒤 30회의 중앙값
                   (mb_per_s는 요청 payload 기준, 1MB = 2^20 B)
    - 클라이언트 자원: (utime_us + stime_us) / sent_chunks, nvcsw / sent_chunks 의 평균, vmhwm_kb 중앙값
    - 서버 자원  : server_summary 한 줄(조건마다 서버를 새로 띄워 30회분 누적)을 chunks로 나눔
"""
import argparse
import glob
import gzip
import os
import re
import sys

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
IPC_STYLE = {"pipe": dict(color="#1f77b4", marker="o"),
             "shm": dict(color="#d62728", marker="s")}
SIZES_APP = [4096, 16384, 65536, 262144]
CLIENTS = [1, 2, 4, 8]


def size_label(b):
    b = int(b)
    if b >= 1 << 20:
        return f"{b >> 20}MB"
    if b >= 1 << 10:
        return f"{b >> 10}KB"
    return f"{b}B"


def log(msg):
    print(msg, file=sys.stderr)


# --------------------------------------------------------------------------
# 파일 찾기
# --------------------------------------------------------------------------

def latest_dir(base, prefix):
    """data/app/<prefix>_<날짜_시각> 중 가장 최근 것. 없으면 None"""
    c = sorted(d for d in glob.glob(os.path.join(base, prefix + "_2*")) if os.path.isdir(d))
    return c[-1] if c else None


def read_env(d):
    p = os.path.join(d, "env.txt")
    if not os.path.exists(p):
        return {}
    txt = open(p, encoding="utf-8").read()
    m = re.search(r"server_extra=\[(.*?)\]", txt)
    return {"server_extra": m.group(1).strip() if m else "", "text": txt}


def read_csv_maybe_gz(path):
    if path.endswith(".gz"):
        with gzip.open(path, "rt") as f:
            return pd.read_csv(f)
    return pd.read_csv(path)


# --------------------------------------------------------------------------
# 집계
# --------------------------------------------------------------------------

KEY = ["mode", "size", "clients", "ipc"]


def agg_client(d):
    c = pd.read_csv(os.path.join(d, "client_summary.csv"))
    c["cpu_us_per_chunk"] = (c.utime_us + c.stime_us) / c.sent_chunks
    c["utime_us_per_chunk"] = c.utime_us / c.sent_chunks
    c["stime_us_per_chunk"] = c.stime_us / c.sent_chunks
    c["nvcsw_per_chunk"] = c.nvcsw / c.sent_chunks
    c["nivcsw_per_chunk"] = c.nivcsw / c.sent_chunks
    g = c.groupby(KEY)
    lat = g.agg(rows=("run", "count"),
                mean_us=("mean_us", "mean"),
                median_us=("median_us", "median"),
                p99_us=("p99_us", "median"),
                max_us=("max_us", "max"),
                cli_utime_us_per_chunk=("utime_us_per_chunk", "mean"),
                cli_stime_us_per_chunk=("stime_us_per_chunk", "mean"),
                cli_cpu_us_per_chunk=("cpu_us_per_chunk", "mean"),
                cli_nvcsw_per_chunk=("nvcsw_per_chunk", "mean"),
                cli_nivcsw_per_chunk=("nivcsw_per_chunk", "mean"),
                cli_vmhwm_kb=("vmhwm_kb", "median"),
                bad_rsp=("bad_rsp", "sum"))
    thr = (c.groupby(KEY + ["run"]).agg(chunks_per_s=("chunks_per_s", "sum"),
                                         mb_per_s=("mb_per_s", "sum"))
           .groupby(KEY).median())
    return lat.join(thr).reset_index()


def parse_label(label):
    """stats_4K_c8 -> (stats, 4096, 8)"""
    m = re.match(r"(stats|norm)_(\d+)([KM])_c(\d+)", label)
    if not m:
        return None, None, None
    mult = 1024 if m.group(3) == "K" else 1024 * 1024
    return m.group(1), int(m.group(2)) * mult, int(m.group(4))


def agg_server(d):
    s = pd.read_csv(os.path.join(d, "server_summary.csv"))
    parsed = s.label.apply(parse_label)
    s["mode"] = [p[0] for p in parsed]
    s["size"] = [p[1] for p in parsed]
    s["clients"] = [p[2] for p in parsed]
    s["srv_utime_us_per_chunk"] = s.utime_us / s.chunks
    s["srv_stime_us_per_chunk"] = s.stime_us / s.chunks
    s["srv_cpu_us_per_chunk"] = (s.utime_us + s.stime_us) / s.chunks
    s["srv_nvcsw_per_chunk"] = s.nvcsw / s.chunks
    s["srv_nivcsw_per_chunk"] = s.nivcsw / s.chunks
    return s


def load_exp(base, prefix):
    d = latest_dir(base, prefix)
    if d is None:
        log(f"  (없음) {prefix}_*")
        return None
    log(f"  {prefix:10s} <- {os.path.relpath(d, ROOT)}")
    cl = agg_client(d)
    sv = agg_server(d)
    m = cl.merge(sv[KEY + ["chunks", "global_chunks", "bad", "send_fail", "K", "client_lock", "global_lock",
                           "srv_utime_us_per_chunk", "srv_stime_us_per_chunk", "srv_cpu_us_per_chunk",
                           "srv_nvcsw_per_chunk", "srv_nivcsw_per_chunk", "vmhwm_kb"]]
                 .rename(columns={"vmhwm_kb": "srv_vmhwm_kb"}), on=KEY, how="left")
    env = read_env(d)
    m["exp"] = prefix
    m["dir"] = os.path.basename(d)
    m["server_extra"] = env.get("server_extra", "")
    return {"dir": d, "agg": m, "env": env}


# --------------------------------------------------------------------------
# 그리기 도우미
# --------------------------------------------------------------------------

def errplot(ax, x, med, p99, mean, ipc, label=None, ls="-", offset=1.0):
    """중앙값: 선+점, p99: 위쪽 오차막대, 평균: 빈 점"""
    st = IPC_STYLE.get(ipc, dict(color="gray", marker="^"))
    x = [v * offset for v in x]
    up = [max(p - m, 0) for p, m in zip(p99, med)]
    ax.errorbar(x, med, yerr=[[0] * len(up), up], color=st["color"], marker=st["marker"],
                ls=ls, capsize=3, ms=5, lw=1.4, label=(label or ipc) + " median (bar: p99)")
    ax.plot(x, mean, ls="none", marker=st["marker"], mfc="none", mec=st["color"], ms=8,
            label=(label or ipc) + " mean")


def save(fig, out, name, made):
    p = os.path.join(out, name)
    fig.tight_layout()
    fig.savefig(p, dpi=150)
    plt.close(fig)
    made.append(p)


def set_size_axis(ax, sizes):
    ax.set_xscale("log", base=2)
    ax.set_xticks(sizes)
    ax.set_xticklabels([size_label(s) for s in sizes])
    ax.minorticks_off()


def set_client_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(CLIENTS)
    ax.set_xticklabels([str(c) for c in CLIENTS])
    ax.minorticks_off()


# --------------------------------------------------------------------------
# 1단계 (보고서 2장)
# --------------------------------------------------------------------------

def phase1(p1dir, out, tab, made):
    frames = []
    for ipc in ("pipe", "shm"):
        f = os.path.join(p1dir, f"{ipc}_summary.csv")
        if os.path.exists(f):
            frames.append(pd.read_csv(f))
    if not frames:
        log(f"1단계 요약 CSV 없음: {p1dir}")
        return
    p = pd.concat(frames, ignore_index=True)
    reps_total = p.clients * (p.warmup + p.reps)   # 지연 모드: 왕복 수(워밍업 포함)
    p["cli_stime_us_per_op"] = p.cli_stime_ms * 1000 / reps_total
    p["cli_nvcsw_per_op"] = p.cli_nvcsw / reps_total
    p["srv_nvcsw_per_op"] = p.srv_nvcsw / reps_total
    p["mb_per_s_median"] = p.bytes_per_sec_median / 2 ** 20
    p.sort_values(["mode", "msg_size", "clients", "ipc"]).to_csv(
        os.path.join(tab, "phase1_summary.csv"), index=False)
    sizes = sorted(p.msg_size.unique())
    lat = p[p["mode"] == "latency"]
    thr = p[p["mode"] == "throughput"]

    # 그림 2-1: 크기별 왕복 지연 (클라이언트 수별 패널)
    fig, axs = plt.subplots(1, 4, figsize=(18, 4.4), sharey=True)
    for ax, n in zip(axs, CLIENTS):
        for ipc in ("pipe", "shm"):
            d = lat[(lat.clients == n) & (lat.ipc == ipc)].sort_values("msg_size")
            if len(d):
                errplot(ax, d.msg_size, d.median_us, d.p99_us, d.mean_us, ipc)
        set_size_axis(ax, sizes)
        ax.set_yscale("log")
        ax.set_title(f"Phase 1 ping-pong, clients = {n}")
        ax.set_xlabel("Message size (bytes, log2)")
        ax.grid(alpha=.3, which="both")
    axs[0].set_ylabel("Round-trip latency (µs, log)")
    axs[0].legend(fontsize=7)
    save(fig, out, "p1_latency_vs_size.png", made)

    # 그림 2-2: 크기별 처리량 (msg/s, MB/s) + memcpy 기준선
    mref = None
    mf = os.path.join(p1dir, "memcpy_ref.csv")
    if os.path.exists(mf):
        mref = pd.read_csv(mf)
    fig, axs = plt.subplots(2, 4, figsize=(18, 8), sharex=True)
    for j, n in enumerate(CLIENTS):
        for ipc in ("pipe", "shm"):
            d = thr[(thr.clients == n) & (thr.ipc == ipc)].sort_values("msg_size")
            st = IPC_STYLE[ipc]
            axs[0, j].plot(d.msg_size, d.msgs_per_sec_median, label=ipc + " median", **st)
            axs[1, j].plot(d.msg_size, d.mb_per_s_median, label=ipc + " median", **st)
        if mref is not None:
            r = mref[mref.threads == n].sort_values("size")
            # shm은 메시지마다 복사 2번(클라 -> shm -> 서버) -> memcpy 속도의 1/2이 상한
            axs[1, j].plot(r["size"], r.copy_GBps * 1e9 / 2 / 2 ** 20, "k--", lw=1,
                           label="memcpy_ref / 2 (same threads)")
        for i in range(2):
            set_size_axis(axs[i, j], sizes)
            axs[i, j].set_yscale("log")
            axs[i, j].grid(alpha=.3, which="both")
        axs[0, j].set_title(f"Phase 1 throughput, clients = {n}")
        axs[1, j].set_xlabel("Message size (bytes, log2)")
    axs[0, 0].set_ylabel("Throughput (messages/s, log)")
    axs[1, 0].set_ylabel("Throughput (MB/s, payload, log)")
    axs[0, 0].legend(fontsize=7)
    axs[1, 0].legend(fontsize=7)
    save(fig, out, "p1_throughput_vs_size.png", made)

    # 그림 2-3: 클라이언트 수별 지연과 처리량 (크기별 패널)
    fig, axs = plt.subplots(2, len(sizes), figsize=(4 * len(sizes), 8))
    for j, s in enumerate(sizes):
        for ipc in ("pipe", "shm"):
            d = lat[(lat.msg_size == s) & (lat.ipc == ipc)].sort_values("clients")
            errplot(axs[0, j], d.clients, d.median_us, d.p99_us, d.mean_us, ipc)
            d = thr[(thr.msg_size == s) & (thr.ipc == ipc)].sort_values("clients")
            axs[1, j].plot(d.clients, d.mb_per_s_median, label=ipc, **IPC_STYLE[ipc])
        for i in range(2):
            set_client_axis(axs[i, j])
            axs[i, j].grid(alpha=.3)
            axs[i, j].set_xlabel("Concurrent clients")
        axs[0, j].set_title(f"Phase 1, message {size_label(s)}")
        axs[0, j].set_ylabel("Round-trip latency (µs)")
        axs[1, j].set_ylabel("Throughput (MB/s, median)")
    axs[0, 0].legend(fontsize=7)
    axs[1, 0].legend(fontsize=7)
    save(fig, out, "p1_vs_clients.png", made)

    # 그림 2-4: 자원 사용량 (지연 모드, 클라이언트 1개)
    d1 = lat[lat.clients == 1]
    fig, axs = plt.subplots(1, 3, figsize=(16, 4.4))
    for ipc in ("pipe", "shm"):
        d = d1[d1.ipc == ipc].sort_values("msg_size")
        st = IPC_STYLE[ipc]
        axs[0].plot(d.msg_size, d.cli_stime_us_per_op, label=ipc, **st)
        axs[1].plot(d.msg_size, d.cli_nvcsw_per_op, label=ipc + " client", **st)
        axs[1].plot(d.msg_size, d.srv_nvcsw_per_op, ls="--", label=ipc + " server",
                    color=st["color"], marker=st["marker"], mfc="none")
    vm = lat[lat.clients == 1].groupby("ipc")[["cli_vmrss_kb", "srv_vmrss_kb"]].max() / 1024
    vm.plot.bar(ax=axs[2], rot=0)
    axs[2].set_ylabel("VmRSS at end of condition (MB)")
    axs[2].set_xlabel("IPC")
    axs[2].set_title("Phase 1 memory (clients = 1)")
    for ax in axs[:2]:
        set_size_axis(ax, sizes)
        ax.set_xlabel("Message size (bytes, log2)")
        ax.grid(alpha=.3)
        ax.legend(fontsize=8)
    axs[0].set_yscale("log")
    axs[0].set_ylabel("Client system CPU time (µs / round trip)")
    axs[0].set_title("Phase 1 kernel CPU per round trip (clients = 1)")
    axs[1].set_ylabel("Voluntary context switches / round trip")
    axs[1].set_title("Phase 1 context switches (clients = 1)")
    save(fig, out, "p1_resource.png", made)

    # 그림 2-5: 왕복 지연 분포 (원자료, 클라이언트 1개)
    raws = []
    for ipc in ("pipe", "shm"):
        f = os.path.join(p1dir, f"{ipc}_latency_raw.csv")
        if os.path.exists(f):
            raws.append(pd.read_csv(f))
    if raws:
        r = pd.concat(raws)
        r = r[r.clients == 1]
        fig, axs = plt.subplots(1, len(sizes), figsize=(4 * len(sizes), 3.8))
        for ax, s in zip(axs, sizes):
            for ipc in ("pipe", "shm"):
                v = r[(r.msg_size == s) & (r.ipc == ipc)].rtt_ns / 1000
                if len(v):
                    ax.hist(v, bins=40, alpha=.55, color=IPC_STYLE[ipc]["color"], label=ipc)
            ax.set_title(f"Phase 1 RTT distribution, {size_label(s)}, clients = 1")
            ax.set_xlabel("Round-trip latency (µs)")
            ax.set_ylabel("Count (samples)")
            ax.legend(fontsize=8)
        save(fig, out, "p1_latency_hist.png", made)

    # 그림 2-6: 1단계 추가 실험 E1 (shm 링 칸 수)
    extra = os.path.join(os.path.dirname(p1dir.rstrip("/")), "extra")
    rows = []
    for tag, label in (("e1_q16", "shm ring 16"), ("e1_q256", "shm ring 256")):
        f = os.path.join(extra, tag, "shm_summary.csv")
        if os.path.exists(f):
            e = pd.read_csv(f)
            for _, x in e.iterrows():
                rows.append((label, x.msg_size, x.clients, x.msgs_per_sec_median))
    if rows:
        for ipc, label in (("pipe", "pipe"), ("shm", "shm ring 4 (main run)")):
            d = thr[(thr.ipc == ipc) & (thr.clients == 1) & (thr.msg_size.isin([64, 1024]))]
            for _, x in d.iterrows():
                rows.append((label, x.msg_size, 1, x.msgs_per_sec_median))
        e1 = pd.DataFrame(rows, columns=["variant", "msg_size", "clients", "msgs_per_sec_median"])
        e1.to_csv(os.path.join(tab, "phase1_e1_ring.csv"), index=False)
        pv = e1.pivot_table(index="msg_size", columns="variant", values="msgs_per_sec_median")
        order = [c for c in ["pipe", "shm ring 4 (main run)", "shm ring 16", "shm ring 256"] if c in pv]
        pv = pv[order]
        pv.index = [size_label(i) for i in pv.index]
        fig, ax = plt.subplots(figsize=(8, 4.2))
        pv.plot.bar(ax=ax, rot=0)
        ax.set_ylabel("Throughput (messages/s, median of 30)")
        ax.set_xlabel("Message size (clients = 1)")
        ax.set_title("Phase 1 extra E1: shm ring cells vs pipe")
        ax.grid(alpha=.3, axis="y")
        save(fig, out, "p1_e1_ring.png", made)

    # 그림 2-7: 1단계 추가 실험 E5 (같은 측정 5회 반복, 회차별 변동)
    rows = []
    for k in range(1, 10):
        f = os.path.join(extra, f"e5_{k}", "shm_summary.csv")
        if os.path.exists(f):
            e = pd.read_csv(f)
            e["rep"] = k
            rows.append(e)
    if rows:
        e5 = pd.concat(rows)
        e5[["rep", "ipc", "mode", "msg_size", "clients", "msgs_per_sec_median", "median_us"]].to_csv(
            os.path.join(tab, "phase1_e5_repeat.csv"), index=False)
        fig, ax = plt.subplots(figsize=(8, 4.2))
        for (s, n), d in e5.groupby(["msg_size", "clients"]):
            ax.plot(d.rep, d.msgs_per_sec_median, marker="o", label=f"{size_label(s)}, clients {n}")
        ax.set_xlabel("Repetition (same command, run back-to-back)")
        ax.set_ylabel("shm throughput (messages/s, median)")
        ax.set_title("Phase 1 extra E5: run-to-run drift")
        ax.legend(fontsize=7)
        ax.grid(alpha=.3)
        save(fig, out, "p1_e5_repeat.png", made)


# --------------------------------------------------------------------------
# 응용 (보고서 6장)
# --------------------------------------------------------------------------

def app_grid(a, out, name, title, ycol, ylabel, made, latency=True):
    fig, axs = plt.subplots(2, 4, figsize=(18, 8))
    for i, mode in enumerate(("stats", "norm")):
        for j, s in enumerate(SIZES_APP):
            ax = axs[i, j]
            for ipc in ("pipe", "shm"):
                d = a[(a["mode"] == mode) & (a["size"] == s) & (a.ipc == ipc)].sort_values("clients")
                if not len(d):
                    continue
                if latency:
                    errplot(ax, d.clients, d.median_us, d.p99_us, d.mean_us, ipc)
                else:
                    ax.plot(d.clients, d[ycol], label=ipc, **IPC_STYLE[ipc])
            set_client_axis(ax)
            ax.set_title(f"{title}: {mode} mode, chunk {size_label(s)}")
            ax.set_xlabel("Concurrent clients")
            ax.set_ylabel(ylabel)
            ax.set_ylim(bottom=0)
            ax.grid(alpha=.3)
    axs[0, 0].legend(fontsize=7)
    save(fig, out, name, made)


def app_e0(e0, out, tab, made, p1dir):
    a = e0["agg"]
    a.to_csv(os.path.join(tab, "e0_conditions.csv"), index=False)
    app_grid(a, out, "app_e0_latency.png", "E0 request latency", None, "Request latency (µs)", made)
    app_grid(a, out, "app_e0_throughput_chunks.png", "E0 throughput", "chunks_per_s",
             "Throughput (chunks/s, all clients)", made, latency=False)
    app_grid(a, out, "app_e0_throughput_mbs.png", "E0 throughput", "mb_per_s",
             "Throughput (MB/s, request payload)", made, latency=False)

    # pipe 대비 shm 배수 (예측 P1~P4 판정용)
    w = a.set_index(KEY).unstack("ipc")
    r = pd.DataFrame({
        "median_pipe_us": w["median_us"]["pipe"], "median_shm_us": w["median_us"]["shm"],
        "median_ratio_pipe_over_shm": w["median_us"]["pipe"] / w["median_us"]["shm"],
        "p99_pipe_us": w["p99_us"]["pipe"], "p99_shm_us": w["p99_us"]["shm"],
        "p99_ratio_pipe_over_shm": w["p99_us"]["pipe"] / w["p99_us"]["shm"],
        "mean_ratio_pipe_over_shm": w["mean_us"]["pipe"] / w["mean_us"]["shm"],
        "chunks_per_s_pipe": w["chunks_per_s"]["pipe"], "chunks_per_s_shm": w["chunks_per_s"]["shm"],
        "throughput_ratio_shm_over_pipe": w["chunks_per_s"]["shm"] / w["chunks_per_s"]["pipe"],
    }).reset_index()
    r.to_csv(os.path.join(tab, "e0_ratio.csv"), index=False)
    fig, axs = plt.subplots(1, 2, figsize=(15, 4.6), sharey=True)
    for ax, mode in zip(axs, ("stats", "norm")):
        d = r[r["mode"] == mode]
        pv = d.pivot_table(index="size", columns="clients", values="median_ratio_pipe_over_shm")
        pv.index = [size_label(i) for i in pv.index]
        pv.plot.bar(ax=ax, rot=0)
        ax.axhline(1, color="k", lw=1)
        ax.set_title(f"E0 median latency ratio pipe / shm ({mode} mode)")
        ax.set_xlabel("Chunk size")
        ax.set_ylabel("pipe median / shm median (×, >1: shm faster)")
        ax.legend(title="clients", fontsize=8)
        ax.grid(alpha=.3, axis="y")
    save(fig, out, "app_e0_ratio_latency.png", made)
    fig, axs = plt.subplots(1, 2, figsize=(15, 4.6), sharey=True)
    for ax, mode in zip(axs, ("stats", "norm")):
        d = r[r["mode"] == mode]
        pv = d.pivot_table(index="size", columns="clients", values="throughput_ratio_shm_over_pipe")
        pv.index = [size_label(i) for i in pv.index]
        pv.plot.bar(ax=ax, rot=0)
        ax.axhline(1, color="k", lw=1)
        ax.set_title(f"E0 throughput ratio shm / pipe ({mode} mode)")
        ax.set_xlabel("Chunk size")
        ax.set_ylabel("shm chunks/s ÷ pipe chunks/s (×)")
        ax.legend(title="clients", fontsize=8)
        ax.grid(alpha=.3, axis="y")
    save(fig, out, "app_e0_ratio_throughput.png", made)

    # 자원: 클라이언트와 서버의 청크당 CPU(user/sys), 자발적 문맥 교환, VmHWM
    for who, pre in (("client", "cli"), ("server", "srv")):
        fig, axs = plt.subplots(2, 3, figsize=(18, 8))
        for i, mode in enumerate(("stats", "norm")):
            for ipc in ("pipe", "shm"):
                st = IPC_STYLE[ipc]
                d = a[(a["mode"] == mode) & (a.ipc == ipc) & (a.clients == 1)].sort_values("size")
                axs[i, 0].plot(d["size"], d[f"{pre}_utime_us_per_chunk"], label=ipc + " user", **st)
                axs[i, 0].plot(d["size"], d[f"{pre}_stime_us_per_chunk"], ls="--", mfc="none",
                               label=ipc + " system", color=st["color"], marker=st["marker"])
                for n, ls in ((1, "-"), (8, ":")):
                    d = a[(a["mode"] == mode) & (a.ipc == ipc) & (a.clients == n)].sort_values("size")
                    axs[i, 1].plot(d["size"], d[f"{pre}_nvcsw_per_chunk"], ls=ls,
                                   label=f"{ipc} c={n}", color=st["color"], marker=st["marker"])
                    axs[i, 2].plot(d["size"], d[f"{pre}_vmhwm_kb"] / 1024, ls=ls,
                                   label=f"{ipc} c={n}", color=st["color"], marker=st["marker"])
            axs[i, 0].set_ylabel(f"{who} CPU time (µs / chunk)")
            axs[i, 0].set_title(f"E0 {who} CPU per chunk, clients = 1 ({mode})")
            axs[i, 1].set_ylabel("Voluntary ctx switches / chunk")
            axs[i, 1].set_title(f"E0 {who} voluntary ctx switches ({mode})")
            axs[i, 2].set_ylabel(f"{who} VmHWM (MB)")
            axs[i, 2].set_title(f"E0 {who} peak memory ({mode})")
            for ax in axs[i]:
                set_size_axis(ax, SIZES_APP)
                ax.set_xlabel("Chunk size")
                ax.grid(alpha=.3)
                ax.legend(fontsize=7)
                ax.set_ylim(bottom=0)
        save(fig, out, f"app_e0_resource_{who}.png", made)

    # 서버 비자발적 문맥 교환 (CPU 경쟁의 근거)
    fig, axs = plt.subplots(1, 2, figsize=(14, 4.4), sharey=True)
    for ax, mode in zip(axs, ("stats", "norm")):
        for s, ls in zip(SIZES_APP, ("-", "--", "-.", ":")):
            for ipc in ("pipe", "shm"):
                d = a[(a["mode"] == mode) & (a["size"] == s) & (a.ipc == ipc)].sort_values("clients")
                st = IPC_STYLE[ipc]
                ax.plot(d.clients, d.srv_nivcsw_per_chunk, ls=ls, color=st["color"], marker=st["marker"],
                        label=f"{ipc} {size_label(s)}")
        set_client_axis(ax)
        ax.set_xlabel("Concurrent clients")
        ax.set_ylabel("Server involuntary ctx switches / chunk")
        ax.set_title(f"E0 server involuntary ctx switches ({mode})")
        ax.grid(alpha=.3)
        ax.legend(fontsize=7, ncol=2)
    save(fig, out, "app_e0_server_nivcsw.png", made)

    # 1단계와 응용 비교 (클라이언트 1개, 겹치는 크기)
    rows = []
    for ipc in ("pipe", "shm"):
        f = os.path.join(p1dir, f"{ipc}_summary.csv")
        if os.path.exists(f):
            p = pd.read_csv(f)
            p = p[(p["mode"] == "latency") & (p.clients == 1)]
            for _, x in p.iterrows():
                rows.append(("phase1 ping-pong", ipc, x.msg_size, x.median_us))
    for _, x in a[a.clients == 1].iterrows():
        rows.append((f"app {x['mode']}", x.ipc, x["size"], x.median_us))
    cmp_ = pd.DataFrame(rows, columns=["source", "ipc", "size", "median_us"])
    cmp_ = cmp_[cmp_["size"].isin(SIZES_APP)]
    cmp_.to_csv(os.path.join(tab, "phase1_vs_app_c1.csv"), index=False)
    if len(cmp_):
        fig, ax = plt.subplots(figsize=(11, 4.6))
        pv = cmp_.pivot_table(index="size", columns=["source", "ipc"], values="median_us")
        pv.index = [size_label(i) for i in pv.index]
        pv.plot.bar(ax=ax, rot=0, logy=True)
        ax.set_ylabel("Median latency (µs, log)")
        ax.set_xlabel("Message / chunk size (clients = 1)")
        ax.set_title("Phase 1 round trip vs application request latency (empty = not measured)")
        ax.legend(fontsize=7, ncol=3)
        ax.grid(alpha=.3, axis="y")
        save(fig, out, "app_phase1_vs_app.png", made)

    # 서버 구간 측정(있을 때만: *_chunks.csv.gz는 git에 없음)
    chunk_breakdown(e0["dir"], out, tab, made, "e0")
    client_cdf(e0["dir"], out, made)


def chunk_breakdown(d, out, tab, made, tag):
    files = glob.glob(os.path.join(d, "server_*_chunks.csv*"))
    if not files:
        log(f"  [{tag}] server_*_chunks.csv(.gz) 없음 -> 서버 구간 그래프 건너뜀 (tar 원자료 폴더에서 다시 실행)")
        return
    rows = []
    for f in sorted(files):
        m = re.search(r"server_(pipe|shm)_(.+?)_chunks\.csv", os.path.basename(f))
        if not m:
            continue
        mode, size, n = parse_label(m.group(2))
        c = read_csv_maybe_gz(f)
        c["lock_ns"] = c.lock_cli_ns + c.lock_grd_ns + c.lock_gwr_ns
        row = {"ipc": m.group(1), "mode": mode, "size": size, "clients": n, "samples": len(c)}
        for col in ("qwait_ns", "proc_ns", "lock_cli_ns", "lock_grd_ns", "lock_gwr_ns", "lock_ns", "send_ns"):
            row[col.replace("_ns", "_med_us")] = c[col].median() / 1000
            row[col.replace("_ns", "_p99_us")] = c[col].quantile(.99) / 1000
            row[col.replace("_ns", "_mean_us")] = c[col].mean() / 1000
        rows.append(row)
    s = pd.DataFrame(rows)
    s.to_csv(os.path.join(tab, f"{tag}_server_segments.csv"), index=False)
    for n in sorted(s.clients.unique()):
        d2 = s[s.clients == n].sort_values(["mode", "size", "ipc"])
        if not len(d2):
            continue
        lab = [f"{r['mode']}\n{size_label(r['size'])}\n{r['ipc']}" for _, r in d2.iterrows()]
        fig, ax = plt.subplots(figsize=(max(8, len(lab) * 0.8), 4.8))
        bottom = [0] * len(d2)
        for col, name in (("qwait_mean_us", "queue wait"), ("proc_mean_us", "processing"),
                          ("lock_mean_us", "lock wait (3 locks)"), ("send_mean_us", "response send")):
            v = list(d2[col])
            ax.bar(range(len(v)), v, bottom=bottom, label=name)
            bottom = [b + x for b, x in zip(bottom, v)]
        ax.set_xticks(range(len(lab)))
        ax.set_xticklabels(lab, fontsize=7)
        ax.set_ylabel("Server time per chunk (µs, mean)")
        ax.set_title(f"{tag}: server-side segments per chunk, clients = {n}")
        ax.legend(fontsize=8)
        ax.grid(alpha=.3, axis="y")
        save(fig, out, f"{tag}_server_segments_c{n}.png", made)


def client_cdf(d, out, made):
    files = glob.glob(os.path.join(d, "client_*_raw.csv*"))
    if not files:
        log("  client_*_raw.csv(.gz) 없음 -> 지연 분포(CDF) 그래프 건너뜀")
        return
    want = {}
    for f in files:
        m = re.search(r"client_(pipe|shm)_(stats|norm)_(\d+)_c(\d+)_r(\d+)_id\d+_raw", os.path.basename(f))
        if m and int(m.group(5)) == 1:
            want.setdefault((m.group(2), int(m.group(3)), int(m.group(4)), m.group(1)), []).append(f)
    if not want:
        return
    fig, axs = plt.subplots(2, 4, figsize=(18, 8))
    for i, mode in enumerate(("stats", "norm")):
        for j, s in enumerate(SIZES_APP):
            ax = axs[i, j]
            for ipc in ("pipe", "shm"):
                for n, ls in ((1, "-"), (8, "--")):
                    fs = want.get((mode, s, n, ipc))
                    if not fs:
                        continue
                    v = pd.concat(read_csv_maybe_gz(f) for f in fs).rtt_us.sort_values()
                    ax.plot(v.values, [k / len(v) for k in range(1, len(v) + 1)], ls=ls,
                            color=IPC_STYLE[ipc]["color"], label=f"{ipc} c={n}")
            ax.set_xscale("log")
            ax.set_title(f"Latency CDF (run 1): {mode}, {size_label(s)}")
            ax.set_xlabel("Request latency (µs, log)")
            ax.set_ylabel("Cumulative fraction")
            ax.grid(alpha=.3)
            ax.legend(fontsize=7)
    save(fig, out, "app_e0_latency_cdf.png", made)


def is_default(x):
    extra = str(x.get("server_extra", "") or "")
    return ("-X" not in extra and x.get("K") == 8 and x.get("client_lock") == "per"
            and x.get("global_lock") == "rwlock")


def session_variance(exps, out, tab, made):
    """같은 기본 설정(per, rwlock, K=8, -X 없음)을 다른 시각에 잰 실험끼리 비교 -> 측정 간 변동 폭"""
    rows = []
    for name, e in exps.items():
        if e is None:
            continue
        a = e["agg"]
        a = a[a.apply(is_default, axis=1)]
        rows.append(a)
    if not rows:
        return
    a = pd.concat(rows)
    cnt = a.groupby(KEY).exp.nunique()
    keep = cnt[cnt >= 2].index
    a = a.set_index(KEY).loc[keep].reset_index()
    if not len(a):
        return
    cols = KEY + ["exp", "dir", "median_us", "p99_us", "mean_us", "chunks_per_s"]
    a[cols].sort_values(KEY + ["exp"]).to_csv(os.path.join(tab, "session_variance_default.csv"), index=False)
    # 그림은 여러 세션에 걸친 클라이언트 8개 조건만 (표에는 전부 남김)
    a = a[a.clients == 8]
    conds = a[["mode", "size", "clients"]].drop_duplicates().sort_values(["mode", "size", "clients"])
    if not len(conds):
        return
    fig, axs = plt.subplots(2, len(conds), figsize=(4.6 * len(conds), 8), squeeze=False)
    for j, (_, cnd) in enumerate(conds.iterrows()):
        d = a[(a["mode"] == cnd["mode"]) & (a["size"] == cnd["size"]) & (a.clients == cnd.clients)]
        for i, col, yl in ((0, "median_us", "Median request latency (µs)"),
                           (1, "chunks_per_s", "Throughput (chunks/s)")):
            pv = d.pivot_table(index="exp", columns="ipc", values=col)
            pv.plot.bar(ax=axs[i, j], rot=30, color=[IPC_STYLE[c]["color"] for c in pv.columns])
            axs[i, j].set_ylabel(yl)
            axs[i, j].set_xlabel("Session (same default settings)")
            axs[i, j].grid(alpha=.3, axis="y")
        axs[0, j].set_title(f"Same settings, other sessions:\n{cnd['mode']} {size_label(cnd['size'])} c={cnd.clients}")
    save(fig, out, "app_session_variance.png", made)


def app_e1(per, table, out, tab, made):
    if per is None or table is None:
        return
    a = pd.concat([per["agg"].assign(lock="per-client mutex"),
                   table["agg"].assign(lock="one table mutex")])
    a.to_csv(os.path.join(tab, "e1_lock_scope.csv"), index=False)
    conds = a[["mode", "size", "clients"]].drop_duplicates().sort_values(["mode", "size"])
    fig, axs = plt.subplots(2, len(conds), figsize=(4.4 * len(conds), 8), squeeze=False)
    for j, (_, c) in enumerate(conds.iterrows()):
        d = a[(a["mode"] == c["mode"]) & (a["size"] == c["size"]) & (a.clients == c.clients)]
        for k, ipc in enumerate(("pipe", "shm")):
            for lk, ls, off in (("per-client mutex", "-", -0.2), ("one table mutex", "--", 0.2)):
                x = d[(d.ipc == ipc) & (d.lock == lk)]
                if not len(x):
                    continue
                st = IPC_STYLE[ipc]
                pos = k + off
                axs[0, j].errorbar([pos], x.median_us, yerr=[[0], x.p99_us - x.median_us],
                                   marker=st["marker"], color=st["color"], mfc="none" if ls == "--" else st["color"],
                                   capsize=3, ls="none", label=f"{ipc}, {lk}")
                axs[1, j].bar([pos], x.chunks_per_s, width=0.38, color=st["color"],
                              alpha=1 if ls == "-" else .45, label=f"{ipc}, {lk}")
        for i in range(2):
            axs[i, j].set_xticks([0, 1])
            axs[i, j].set_xticklabels(["pipe", "shm"])
            axs[i, j].set_xlabel("IPC (left bar/solid: per-client, right/hollow: table)")
            axs[i, j].grid(alpha=.3, axis="y")
        axs[0, j].set_ylabel("Request latency (µs, median, bar: p99)")
        axs[1, j].set_ylabel("Throughput (chunks/s)")
        axs[0, j].set_title(f"E1 lock scope: {c['mode']} {size_label(c['size'])} c={c.clients}")
    axs[0, 0].legend(fontsize=7)
    save(fig, out, "app_e1_lock_scope.png", made)
    for e, t in ((per, "e1_per"), (table, "e1_table")):
        chunk_breakdown(e["dir"], out, tab, made, t)


def app_e2(exps, base, out, tab, made):
    rows, lag = [], []
    for lk in ("rw", "mx"):
        for K in (1, 8, 64):
            e = exps.get(f"e2_{lk}_k{K}")
            if e is None:
                continue
            a = e["agg"].assign(lock="rwlock" if lk == "rw" else "mutex", Kval=K)
            rows.append(a)
            for f in glob.glob(os.path.join(e["dir"], "server_*_report.csv")):
                m = re.search(r"server_(pipe|shm)_(.+?)_report\.csv", os.path.basename(f))
                r = pd.read_csv(f)
                if not len(r):
                    continue
                gap = r.client_chunks - r.global_chunks
                mode, size, n = parse_label(m.group(2))
                lag.append(dict(lock=a["lock"].iloc[0], K=K, ipc=m.group(1), mode=mode, size=size, clients=n,
                                lag_median=gap.median(), lag_max=gap.max(), reports=len(r)))
            chunk_breakdown(e["dir"], out, tab, made, f"e2_{lk}_k{K}")
    if not rows:
        return
    a = pd.concat(rows)
    a.to_csv(os.path.join(tab, "e2_global_lock.csv"), index=False)
    sizes = sorted(a["size"].unique())
    fig, axs = plt.subplots(2, len(sizes), figsize=(7 * len(sizes), 8), squeeze=False)
    for j, s in enumerate(sizes):
        for ipc in ("pipe", "shm"):
            for lk, ls in (("rwlock", "-"), ("mutex", "--")):
                d = a[(a["size"] == s) & (a.ipc == ipc) & (a["lock"] == lk)].sort_values("Kval")
                if not len(d):
                    continue
                st = IPC_STYLE[ipc]
                up = d.p99_us - d.median_us
                axs[0, j].errorbar(d.Kval * (0.93 if lk == "rwlock" else 1.07), d.median_us,
                                   yerr=[[0] * len(d), up], ls=ls, color=st["color"], marker=st["marker"],
                                   capsize=3, label=f"{ipc} {lk}")
                axs[1, j].plot(d.Kval, d.chunks_per_s, ls=ls, color=st["color"], marker=st["marker"],
                               label=f"{ipc} {lk}")
        for i in range(2):
            axs[i, j].set_xscale("log", base=2)
            axs[i, j].set_xticks([1, 8, 64])
            axs[i, j].set_xticklabels(["1", "8", "64"])
            axs[i, j].minorticks_off()
            axs[i, j].set_xlabel("K (global stats write every K chunks)")
            axs[i, j].grid(alpha=.3)
            axs[i, j].legend(fontsize=7)
        axs[0, j].set_ylabel("Request latency (µs, median, bar: p99)")
        axs[1, j].set_ylabel("Throughput (chunks/s)")
        axs[0, j].set_title(f"E2 global stats lock: stats {size_label(s)}, clients = 8")
    save(fig, out, "app_e2_global_lock.png", made)
    if lag:
        L = pd.DataFrame(lag)
        L.to_csv(os.path.join(tab, "e2_report_lag.csv"), index=False)
        fig, ax = plt.subplots(figsize=(8, 4.2))
        for (ipc, lk, s), d in L.groupby(["ipc", "lock", "size"]):
            d = d.sort_values("K")
            st = IPC_STYLE[ipc]
            ax.plot(d.K, d.lag_median, ls="-" if lk == "rwlock" else "--", color=st["color"],
                    marker=st["marker"], label=f"{ipc} {lk} {size_label(s)}")
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 8, 64])
        ax.set_xticklabels(["1", "8", "64"])
        ax.minorticks_off()
        ax.set_xlabel("K (global stats write every K chunks)")
        ax.set_ylabel("Chunks not yet in global stats (median)")
        ax.set_title("E2: global stats lag seen by report thread (client sum − global)")
        ax.grid(alpha=.3)
        ax.legend(fontsize=7)
        save(fig, out, "app_e2_report_lag.png", made)


def app_e5(b, ip, out, tab, made):
    if b is None or ip is None:
        return
    a = pd.concat([b["agg"].assign(variant="base (copy to queue)"),
                   ip["agg"].assign(variant="inplace (-X inplace)")])
    a.to_csv(os.path.join(tab, "e5_inplace.csv"), index=False)
    w = a.set_index(["size", "clients", "variant"])
    cmp_ = pd.DataFrame({
        "median_base": w.xs("base (copy to queue)", level="variant").median_us,
        "median_inplace": w.xs("inplace (-X inplace)", level="variant").median_us,
        "p99_base": w.xs("base (copy to queue)", level="variant").p99_us,
        "p99_inplace": w.xs("inplace (-X inplace)", level="variant").p99_us,
        "cps_base": w.xs("base (copy to queue)", level="variant").chunks_per_s,
        "cps_inplace": w.xs("inplace (-X inplace)", level="variant").chunks_per_s,
        "srv_hwm_base_kb": w.xs("base (copy to queue)", level="variant").srv_vmhwm_kb,
        "srv_hwm_inplace_kb": w.xs("inplace (-X inplace)", level="variant").srv_vmhwm_kb,
        "srv_cpu_base_us": w.xs("base (copy to queue)", level="variant").srv_cpu_us_per_chunk,
        "srv_cpu_inplace_us": w.xs("inplace (-X inplace)", level="variant").srv_cpu_us_per_chunk,
    })
    cmp_["median_change_pct"] = (cmp_.median_inplace / cmp_.median_base - 1) * 100
    cmp_["cps_change_pct"] = (cmp_.cps_inplace / cmp_.cps_base - 1) * 100
    cmp_["srv_hwm_change_kb"] = cmp_.srv_hwm_inplace_kb - cmp_.srv_hwm_base_kb
    cmp_.reset_index().to_csv(os.path.join(tab, "e5_compare.csv"), index=False)

    vs = {"base (copy to queue)": dict(color="#7f7f7f", marker="o"),
          "inplace (-X inplace)": dict(color="#2ca02c", marker="D")}
    fig, axs = plt.subplots(4, 4, figsize=(18, 15))
    for j, s in enumerate(SIZES_APP):
        for v, st in vs.items():
            d = a[(a["size"] == s) & (a.variant == v)].sort_values("clients")
            up = d.p99_us - d.median_us
            axs[0, j].errorbar(d.clients, d.median_us, yerr=[[0] * len(d), up], capsize=3, label=v, **st)
            axs[1, j].plot(d.clients, d.chunks_per_s, label=v, **st)
            axs[2, j].plot(d.clients, d.srv_vmhwm_kb / 1024, label=v, **st)
            axs[3, j].plot(d.clients, d.srv_cpu_us_per_chunk, label=v, **st)
        axs[0, j].set_title(f"E5 shm norm, chunk {size_label(s)}")
        axs[0, j].set_ylabel("Request latency (µs, median, bar: p99)")
        axs[1, j].set_ylabel("Throughput (chunks/s)")
        axs[2, j].set_ylabel("Server VmHWM (MB)")
        axs[3, j].set_ylabel("Server CPU (µs / chunk)")
        for i in range(4):
            set_client_axis(axs[i, j])
            axs[i, j].set_xlabel("Concurrent clients")
            axs[i, j].grid(alpha=.3)
            axs[i, j].set_ylim(bottom=0)
    axs[0, 0].legend(fontsize=7)
    save(fig, out, "app_e5_inplace.png", made)
    for e, t in ((b, "e5_base"), (ip, "e5_inplace")):
        chunk_breakdown(e["dir"], out, tab, made, t)


def checks_table(exps, tab):
    rows = []
    for name, e in exps.items():
        if e is None:
            continue
        f = os.path.join(e["dir"], "checks.csv")
        if os.path.exists(f):
            c = pd.read_csv(f)
            rows.append(dict(exp=name, dir=os.path.basename(e["dir"]), conditions=len(c),
                             client_fail=int(c.client_fail.sum()), match_all=bool((c.match == 1).all()),
                             leftover_ipc=int(c.leftover_ipc.sum()),
                             bad_rsp=int(e["agg"].bad_rsp.sum()), seconds=int(c.seconds.sum())))
    if rows:
        pd.DataFrame(rows).to_csv(os.path.join(tab, "checks_all.csv"), index=False)


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="7조 IPC 설계과제 그래프 생성")
    ap.add_argument("-o", "--out", default=os.path.join(ROOT, "figures"))
    ap.add_argument("--phase1", default=None, help="1단계 결과 폴더 (기본: phase1/results/ 중 최신 날짜 폴더)")
    ap.add_argument("--app", default=os.path.join(ROOT, "data", "app"))
    args = ap.parse_args()

    out = args.out
    tab = os.path.join(out, "tables")
    os.makedirs(tab, exist_ok=True)
    made = []

    p1 = args.phase1
    if p1 is None:
        c = sorted(d for d in glob.glob(os.path.join(ROOT, "phase1", "results", "2*")) if os.path.isdir(d))
        p1 = c[-1] if c else None
    log(f"1단계: {p1}")
    if p1:
        phase1(p1, out, tab, made)

    log("응용 결과 폴더:")
    names = ["e0", "e1_per", "e1_table", "e2_rw_k1", "e2_rw_k8", "e2_rw_k64",
             "e2_mx_k1", "e2_mx_k8", "e2_mx_k64", "e5_base", "e5_inplace"]
    exps = {n: load_exp(args.app, n) for n in names}
    if exps["e0"] is not None:
        app_e0(exps["e0"], out, tab, made, p1 or "")
    session_variance(exps, out, tab, made)
    app_e1(exps["e1_per"], exps["e1_table"], out, tab, made)
    app_e2(exps, args.app, out, tab, made)
    app_e5(exps["e5_base"], exps["e5_inplace"], out, tab, made)
    checks_table(exps, tab)

    log(f"\n그림 {len(made)}개 -> {os.path.relpath(out, ROOT) if out.startswith(ROOT) else out}")
    for p in made:
        log("  " + os.path.basename(p))
    log(f"요약표 -> {os.path.join(out, 'tables')}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 FINS 自采集的 trace CSV 转成标准（新）格式。

FINS 的 CSV 只有 wake/release/execute/complete/finished/sleep，没有 `working` 行，
所以 `execute → complete` 中间那段执行时间无法被下游分析区分「占核」与「被抢占」。
pipeline JSON 里每个节点给出了**明确的执行用时**（`parameters[0].value`，单位 us；
缺省时用 `wcet * 1000`），本脚本据此在 job 内插入 `working` 行：

    execute ────────────── working ───── complete
    └── JSON 执行用时 ────┘└── 余下开销 ──┘

working 行格式与 ROS 导出一致：`n7 [CPU 2, 22793 us]`，位置在 `execute + 用时`，
`cpu` 列取该 job 结束时的核（FINS 绑核，等于 execute 的核；若不等则视为迁移点）。
顺带把 `seq` 重排成标准格式要求的**全局时间序**，并把 release/finished 的
`nX:port` 标签清空（标准格式里这两个事件不带 tag）。

用法：只需要给定三个目录（原始 trace / pipeline JSON / 标准化输出）

    python3 std.py fins pipeline fins_std          # 逐份配对 <stem>.csv + <stem>.json

    import std
    std.standardize("fins", "pipeline", "fins_std")   # 目录里所有同名 csv/json 配对转换

没有匹配 JSON 的 trace 会被跳过（不会退化成整段算一个核）。
"""
import argparse
import csv
import glob
import json
import os
import re
import sys

TAG_RE = re.compile(r"^(?P<node>n\d+)(?::\d+)?$")


# --------------------------------------------------------------------------- JSON

def load_durations(json_path):
    """{node_id: 执行用时 us}。优先 parameters[0].value，其次 wcet*1000。"""
    with open(json_path, encoding="utf-8") as f:
        cfg = json.load(f)
    nodes = cfg["nodes"] if isinstance(cfg, dict) and "nodes" in cfg else cfg
    dur = {}
    for n in nodes:
        nid = n.get("id")
        if not nid:
            continue
        params = n.get("parameters") or []
        if params and params[0].get("value") is not None:
            dur[nid] = int(round(float(params[0]["value"])))
        elif n.get("wcet") is not None:
            dur[nid] = int(round(float(n["wcet"]) * 1000.0))
    return dur


def node_of(tag):
    m = TAG_RE.match(str(tag).strip()) if tag is not None else None
    return m["node"] if m else None


# --------------------------------------------------------------------------- 转换

def convert(csv_path, json_path, keep_tags=False):
    """读 FINS CSV + pipeline JSON，返回标准格式的 DataFrame（列 tid,seq,kind,t_us,cpu,tag）。"""
    import pandas as pd

    dur = load_durations(json_path)
    raw = pd.read_csv(csv_path)
    for c in ["tid", "seq", "t_us", "cpu"]:
        raw[c] = pd.to_numeric(raw[c], errors="coerce")
    raw = raw.dropna(subset=["tid", "seq", "t_us", "cpu"]).sort_values(["tid", "seq"])
    raw["kind"] = raw["kind"].astype(str).str.strip().str.lower()

    rows_out, n_ins, n_skip, clamped = [], 0, 0, 0
    for tid, g in raw.groupby("tid", sort=False):
        ev = list(g.itertuples(index=False))
        # 先配对 execute↔complete，拿到该 job 的结束时刻与核
        jobs, open_exc = [], {}
        for i, r in enumerate(ev):
            nid = node_of(r.tag)
            if r.kind == "execute" and nid:
                open_exc[nid] = i
            elif r.kind == "complete" and nid and nid in open_exc:
                jobs.append((open_exc.pop(nid), i, nid))
        ins_after = {}                                   # execute 下标 -> 待插入的 working 行
        for i_exc, i_com, nid in jobs:
            if nid not in dur:
                n_skip += 1
                continue
            exc_t, exc_cpu = ev[i_exc].t_us, int(ev[i_exc].cpu)
            com_t, com_cpu = ev[i_com].t_us, int(ev[i_com].cpu)
            d = dur[nid]
            t = exc_t + d
            if t > com_t:                                # JSON 用时超出实测跨度：夹到 complete
                t, clamped = com_t, clamped + 1
            ins_after[i_exc] = dict(tid=int(tid), kind="working", t_us=t, cpu=com_cpu,
                                    tag=f"{nid} [CPU {com_cpu}, {int(d)} us]")
            n_ins += 1

        for i, r in enumerate(ev):
            tag = r.tag if keep_tags else (r.tag if r.kind in ("execute", "complete") else None)
            rows_out.append(dict(tid=int(tid), kind=r.kind, t_us=r.t_us, cpu=int(r.cpu),
                                 tag=tag if isinstance(tag, str) else None, _pos=i))
            w = ins_after.get(i)
            if w is not None:
                w = dict(w); w["_pos"] = i + 0.5
                rows_out.append(w)

    df = pd.DataFrame(rows_out).sort_values(["t_us", "tid", "_pos"], kind="stable")
    df = df.reset_index(drop=True)
    df.insert(1, "seq", range(len(df)))                   # 全局时间序
    df["cpu"] = df["cpu"].astype(int)
    info = dict(events=len(df), inserted=n_ins, skipped=n_skip, clamped=clamped,
                nodes=len(dur), nodes_used=len({node_of(r) for r in df[df.kind == "execute"].tag}))
    return df[["tid", "seq", "kind", "t_us", "cpu", "tag"]], info


def write_csv(df, out_path):
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tid", "seq", "kind", "t_us", "cpu", "tag"])
        for r in df.itertuples(index=False):
            t = r.t_us
            t = int(t) if float(t).is_integer() else round(float(t), 3)
            w.writerow([r.tid, r.seq, r.kind, t, r.cpu, "" if r.tag is None else r.tag])


def standardize(trace_dir, json_dir, out_dir, keep_tags=False):
    """批量标准化：只用给三个目录。

      trace_dir  原始 FINS trace（`<name>.csv`）
      json_dir   对应的 pipeline JSON（`<name>.json`，同名配对）
      out_dir    输出目录，写 `<name>_timeline.csv`

    返回 [(name, info), ...]；没有匹配 JSON 的 trace 跳过并打印。
    """
    os.makedirs(out_dir, exist_ok=True)
    done, skipped = [], []
    for csv_path in sorted(glob.glob(os.path.join(trace_dir, "*.csv"))):
        name = os.path.basename(csv_path)[:-4]
        json_path = os.path.join(json_dir, name + ".json")
        if not os.path.exists(json_path):
            skipped.append(name)
            print(f"  [跳过] {name}: 没有匹配的 {json_path}")
            continue
        df, info = convert(csv_path, json_path, keep_tags=keep_tags)
        out_path = os.path.join(out_dir, name + "_timeline.csv")
        write_csv(df, out_path)
        done.append((name, info))
        print(f"  [成功] {name} -> {out_path}  {info}")
    print(f"完成 {len(done)} 份，跳过 {len(skipped)} 份 -> {out_dir}/")
    return done


# --------------------------------------------------------------------------- CLI

def main():
    ap = argparse.ArgumentParser(
        description="FINS trace -> 标准格式（插入 JSON 执行用时为 working 行）；只需三个目录")
    ap.add_argument("trace_dir", help="原始 FINS trace 目录（*.csv）")
    ap.add_argument("json_dir", help="pipeline JSON 目录（*.json，与 trace 同名配对）")
    ap.add_argument("out_dir", help="标准化输出目录（<name>_timeline.csv）")
    ap.add_argument("--keep-tags", action="store_true", help="保留 release/finished 的 nX:port 标签")
    args = ap.parse_args()

    for d, what in ((args.trace_dir, "trace"), (args.json_dir, "JSON")):
        if not os.path.isdir(d):
            sys.exit(f"{what} 目录不存在: {d}")
    print(f"标准化 {args.trace_dir}/ + {args.json_dir}/ -> {args.out_dir}/")
    standardize(args.trace_dir, args.json_dir, args.out_dir, keep_tags=args.keep_tags)


if __name__ == "__main__":
    main()

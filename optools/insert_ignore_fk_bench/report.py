#!/usr/bin/env python3
"""从完整原始 JSONL 生成可复核的设计测量附件，不授予资格/PASS。"""
import argparse
import hashlib
import json
import statistics
from pathlib import Path


def read(path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    if not records or records[-1].get("kind") != "cleanup" or not records[-1].get("database_absent"):
        raise ValueError(f"{path}: 缺少成功清理 receipt")
    return records


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def key(sample):
    return sample["shape"], sample["parents"], sample["children"], sample["mode"]


def median(samples, name, counter=False):
    return statistics.median((s["counters"] if counter else s)[name] for s in samples)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("base", "head", "base_metadata", "head_metadata", "output"):
        parser.add_argument(f"--{name.replace('_', '-')}", type=Path, required=True)
    a = parser.parse_args()
    records = {"base": read(a.base), "head": read(a.head)}
    samples = {v: [s for s in rs if s["kind"] == "sample"] for v, rs in records.items()}
    groups = {}
    for v, ss in samples.items():
        groups[v] = {k: [s for s in ss if key(s) == k] for k in {key(s) for s in ss}}
    if groups["base"].keys() != groups["head"].keys():
        raise ValueError("两端矩阵不一致")
    if any(len(g) < 3 for gs in groups.values() for g in gs.values()):
        raise ValueError("每个组合至少三个样本")
    lines = ["# #30107：有序 FK 有界 SQL 测量原始附件", "",
             "此附件由 `optools/insert_ignore_fk_bench/report.py` 从成功结束的 JSONL 生成。它不是设计 PASS，接受限制见主设计。", "",
             "## 来源与计量限定", ""]
    for v, path, meta_path in (("base", a.base, a.base_metadata), ("head", a.head, a.head_metadata)):
        meta = json.loads(meta_path.read_text())
        env = next(r for r in records[v] if r["kind"] == "environment")
        # 不发布本机绝对路径、password 或完整配置；保留可复核身份与选项。
        lines += [f"### {v}", "", f"- 源 revision：`{meta['revision']}`；二进制 SHA-256：`{meta['binary_sha256']}`。",
                  f"- 原始 JSONL SHA-256：`{sha(path)}`；成功 samples={len(samples[v])}；数据库清理已确认。",
                  f"- 平台：`{env['platform']}`；GOMAXPROCS={meta['env']['GOMAXPROCS']}，GOGC={meta['env']['GOGC']}；启动负载：`{meta['load'].strip()}`。",
                  f"- 工具链/编译：", "", "```text", meta["go_build_info"].replace(meta["source"], "<worktree>").strip(), "```", "",
                  "- 对称插桩源指纹：", "", "```json", json.dumps({k: meta["instrumentation"][k] for k in ("before", "after")}, indent=2), "```", ""]
    lines += ["计量包含公开 SQL frontend 和事务；DDL/seed/flush/oracle 与计量 HTTP 排除，commit 独立记录。父表已 flush，但未清空任何缓存，不能声称冷盘结果。两端采用相同观察点；ctx 附加块 CounterSet 强制覆盖无 PK filter 的 reader，避免旧全局 counter 仅在 filter.Valid 时生效而把缺失 I/O 错记为零。",
              "", "`R/B` 是 reader 返回给执行层的逻辑行数/batch Size，不是底层全行 predicate 次数或磁盘字节；`selected block rows` 是读块原始行数，不能等同完全解码行。FS bytes 是读取 storage layer（含缓存）的进程计数窗口差，不是父表独占磁盘/S3 字节。peak 是 10ms 进程采样绝对峰值，非语句独占或精确高水位。少量样本只报告范围/中位数，不报告 p99。", "",
              "## 各组合中位数", "",
              "| shape | P | N | mode | base exec ms | head exec ms | head/base | base R/B | head R/B | head reader calls | allocator calls base/head |",
              "| --- | ---: | ---: | --- | ---: | ---: | ---: | --- | --- | ---: | --- |"]
    for k in sorted(groups["base"]):
        b, h = groups["base"][k], groups["head"][k]
        bm, hm = median(b, "execute_ms"), median(h, "execute_ms")
        br, hr = (int(median(g, "reader_returned_rows", True)) for g in (b, h))
        bb, hb = (int(median(g, "reader_materialized_bytes", True)) for g in (b, h))
        ac = [int(median(g, "allocator_store_calls", True)) for g in (b, h)]
        lines.append(f"| {k[0]} | {k[1]} | {k[2]} | {k[3]} | {bm:.3f} | {hm:.3f} | {hm/bm:.2f} | {br}/{bb} | {hr}/{hb} | {int(median(h, 'reader_calls', True))} | {ac[0]}/{ac[1]} |")
    lines += ["", "## 逐样本原始计量", "",
              "表中时间保留至微秒精度（ms 三位小数），原始 JSONL 保留完整浮点值；执行吞吐可由 N/(exec ms/1000) 复算，含 commit 吞吐由 N/((exec+commit)/1000) 复算。", "",
              "| variant | shape | P | N | mode | trial | exec ms | commit ms | rows | bytes | block rows | FS bytes | allocator calls/values | heap peak bytes | RSS peak bytes | memory samples |",
              "| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |"]
    for v, ss in samples.items():
        for s in ss:
            c = s["counters"]
            lines.append(f"| {v} | {s['shape']} | {s['parents']} | {s['children']} | {s['mode']} | {s['iteration']} | {s['execute_ms']:.3f} | {s['commit_ms']:.3f} | {c['reader_returned_rows']} | {c['reader_materialized_bytes']} | {c['selected_persisted_block_rows']} | {c.get('process_fileservice_read_bytes', '未计量')} | {c['allocator_store_calls']}/{c['allocator_requested_values']} | {c['sampled_peak_heap_objects_bytes']} | {c['sampled_peak_rss_bytes']} | {c['memory_samples']} |")
    lines += ["", "## 锁等待与取消原始样本", "", "只有公共 mo_locks() 的实际 waiter 才算 barrier；未等待时不伪造取消延迟。writer 从 SQL 入口开始计时，trigger→completion 从 holder commit / KILL QUERY 发出前开始；不包含无意义的人工持锁 sleep。", "",
              "| variant | shape | action | trial | observed wait | outcome | writer total ms | trigger→completion ms | error |",
              "| --- | --- | --- | ---: | --- | --- | ---: | --- | --- |"]
    for v, rs in records.items():
        for s in rs:
            if s["kind"] != "contention":
                continue
            error = s.get("writer_error", "").replace("|", "\\|").replace("\n", " ")
            delay = s.get("trigger_to_completion_ms")
            delay = "无等待，不适用" if delay is None else f"{delay:.3f}"
            lines.append(f"| {v} | {s['shape']} | {s['action']} | {s['iteration']} | {s['observed_lock_wait']} | {s['writer_outcome']} | {s['writer_total_ms']:.3f} | {delay} | {error} |")
    text = "\n".join(lines)
    a.output.write_text("\n".join(line.rstrip() for line in text.splitlines()) + "\n")


if __name__ == "__main__":
    main()

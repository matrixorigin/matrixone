#!/usr/bin/env python3
"""仅为 disposable benchmark build 安装对称计量；绝不用于发布产物。

此脚本不改算法：记录已有 reader 输出/选中持久块行数与 store Allocate
调用；逻辑 reader bytes 不是物理 I/O，in-memory PK 过滤前行数不可见。
install/remove 都要求完整匹配锚点；manifest 保存前后 SHA-256，remove
不使用 git checkout，不覆盖安装后人为改动。
"""
import argparse
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
MANIFEST = "CLAUDE_fkbench_instrumentation.json"
READER = "pkg/vm/engine/readutil/reader.go"
ALLOCATOR = "pkg/incrservice/allocator.go"
PROBE = "pkg/perfcounter/CLAUDE_fkbench.go"
ATTACH = "pkg/vm/engine/readutil/CLAUDE_fkbench.go"


def digest(data):
    return hashlib.sha256(data.encode()).hexdigest()


def once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"计量锚点不是唯一匹配：{old!r}")
    return text.replace(old, new)


def install(root):
    manifest = root / MANIFEST
    if manifest.exists() or (root / PROBE).exists() or (root / ATTACH).exists():
        raise ValueError("已有计量文件，拒绝覆盖")
    originals = {p: (root / p).read_text() for p in (READER, ALLOCATOR)}
    replacements = dict(originals)
    replacements[READER] = once(
        originals[READER],
        "\tvar blkInfo *objectio.BlockInfo\n\n\tstart := time.Now()",
        "\tvar blkInfo *objectio.BlockInfo\n"
        "\tctx = perfcounter.WithCounterSet(ctx, objectio.BlkReadStats.CounterSet)\n"
        "\tdefer func() {\n"
        "\t\tvar blockRows uint64\n"
        "\t\tif blkInfo != nil { blockRows = uint64(blkInfo.MetaLocation().Rows()) }\n"
        "\t\tperfcounter.CLAUDEObserveReader(r.tableDef.Name, uint64(outBatch.RowCount()), uint64(outBatch.Size()), blockRows)\n"
        "\t}()\n\n\tstart := time.Now()",
    )
    a = once(originals[ALLOCATOR], 'import (\n',
             'import (\n "github.com/matrixorigin/matrixone/pkg/perfcounter"\n')
    replacements[ALLOCATOR] = once(a,
        "\tfrom, to, lastAllocateAt, err := a.store.Allocate(",
        "\tperfcounter.CLAUDEObserveAllocate(act.tableID, act.count)\n"
        "\tfrom, to, lastAllocateAt, err := a.store.Allocate(")
    replacements[PROBE] = (HERE / "probe.go.in").read_text()
    replacements[ATTACH] = '''package readutil
import (
 "github.com/matrixorigin/matrixone/pkg/perfcounter"
 "github.com/matrixorigin/matrixone/pkg/objectio"
)
func init() { perfcounter.CLAUDEAttachBlockCounters(objectio.BlkReadStats.CounterSet) }
'''
    # 所有前置条件通过后再落盘。hash 对原始字节（UTF-8/LF）可复核。
    data = {"originals": originals,
            "before": {p: digest(t) for p, t in originals.items()},
            "after": {p: digest(t) for p, t in replacements.items()}}
    manifest.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    for p, text in replacements.items():
        (root / p).write_text(text)
    print(json.dumps({"before": data["before"], "after": data["after"]}, indent=2))


def remove(root):
    manifest = root / MANIFEST
    data = json.loads(manifest.read_text())
    for p, expected in data["after"].items():
        if digest((root / p).read_text()) != expected:
            raise ValueError(f"{p} 安装后发生改动；拒绝自动恢复")
    for p, text in data["originals"].items():
        (root / p).write_text(text)
    for p in (PROBE, ATTACH):
        (root / p).unlink()
    manifest.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("install", "remove"))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    {"install": install, "remove": remove}[args.action](args.root.resolve())


if __name__ == "__main__":
    main()

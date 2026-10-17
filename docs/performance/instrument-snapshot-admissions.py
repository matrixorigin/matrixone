#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Add identical count-only observations to two plain benchmark source exports.

Only the exports are instrumented. This refuses to modify a Git worktree, and
no observer or change to the TxnClient interface is shipped in production.
"""
import pathlib
import subprocess
import sys

OBSERVER = """package client
import("context"; "sync/atomic")
var benchmarkAdmissionAttempts atomic.Uint64
var benchmarkAdmissionSuccesses atomic.Uint64
func(c *txnClient) observeBenchmarkAdmission(ctx,closeCtx context.Context)error{
 benchmarkAdmissionAttempts.Add(1)
 err:=c.limiter.Wait(ctx,closeCtx)
 if err==nil { benchmarkAdmissionSuccesses.Add(1) }
 return err
}
func(c *txnClient) SnapshotAdmissionCounters()(uint64,uint64){return benchmarkAdmissionAttempts.Load(),benchmarkAdmissionSuccesses.Load()}
"""

root = pathlib.Path(sys.argv[1]).resolve()
if (root / ".git").exists():
    raise SystemExit("use a plain git archive source export, not a Git worktree")
client = root / "pkg/txn/client/client.go"
observer = root / "pkg/txn/client/admission_benchmark_observation.go"
source = client.read_text()
needle = "client.limiter.Wait(ctx, client.closeCtx)"
if source.count(needle) != 2 or observer.exists():
    raise SystemExit("expected the two uninstrumented limiter call sites")
client.write_text(source.replace(needle, "client.observeBenchmarkAdmission(ctx, client.closeCtx)"))
observer.write_text(OBSERVER)
subprocess.run(["gofmt", "-w", str(client), str(observer)], check=True)

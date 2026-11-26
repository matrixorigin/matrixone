// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"runtime"
	"runtime/trace"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sync/errgroup"
)

// cloneIndependentDatabaseTables borrows one admitted statement and transaction.
// Each worker owns its mutable frontend, planner and metadata buffers. CREATE
// catalog pairs are published atomically by the transaction workspace owner.
func cloneIndependentDatabaseTables(
	ctx context.Context, owner *backExec, proc *process.Process, tables []*tableInfo,
	clone func(context.Context, BackgroundExec, *tableInfo) (cloneReceipt, error),
) ([]cloneReceipt, error) {
	defer trace.StartRegion(ctx, "clone independent tables").End()
	runCtx, cancel := context.WithCancel(ctx)
	group, workCtx := errgroup.WithContext(runCtx)
	type worker struct {
		exec    *backExec
		proc    *process.Process
		indexes *compile.CloneIndexCatalogBatch
		ctx     context.Context
	}
	workerCount := min(4, runtime.GOMAXPROCS(0), len(tables))
	workers := make([]worker, 0, workerCount)
	defer func() {
		cancel()
		// No rollback or buffer release may race a still-running producer.
		_ = group.Wait()
		for _, worker := range workers {
			worker.indexes.Close()
			worker.exec.Close()
			worker.proc.Free()
		}
	}()
	for range workerCount {
		child := owner.backSes.InitBackExec(proc.GetTxnOperator(), "", backSesOutputCallback).(*backExec)
		child.backSes.respr = &NullResp{database: owner.backSes.GetDatabaseName()}
		child.backSes.ReplaceDerivedStmt(true)
		workerCtx := workCtx
		bufferProc := newCloneCatalogProcess(workerCtx, child.backSes)
		bufferProc.Session = child.backSes
		workers = append(workers, worker{exec: child, proc: bufferProc, ctx: workerCtx,
			indexes: compile.NewCloneIndexCatalogBatch(workerCtx, bufferProc)})
	}
	results := make([]cloneReceipt, len(tables))
	var next atomic.Uint64
	for i := range workers {
		worker := &workers[i]
		group.Go(func() (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = moerr.ConvertPanicError(workCtx, p)
				}
			}()
			for {
				if err := workCtx.Err(); err != nil {
					return err
				}
				index := int(next.Add(1) - 1)
				if index >= len(tables) {
					return worker.indexes.Flush()
				}
				receipt, err := clone(worker.indexes.TableContext(worker.ctx), worker.exec, tables[index])
				if err != nil {
					return err
				}
				if err = worker.indexes.FinishTable(); err != nil {
					return err
				}
				results[index] = receipt
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// Catalog buffers have their own process lifetime; a child SQL statement may
// release its process as soon as ExecRestore returns.
func newCloneCatalogProcess(ctx context.Context, back *backSession) *process.Process {
	runtime := getPu(back.GetService())
	proc := process.NewTopProcess(ctx, back.pool, runtime.TxnClient, back.GetTxnHandler().GetTxn(), runtime.FileService,
		runtime.LockService, runtime.QueryClient, runtime.HAKeeperClient, runtime.UdfService, getAicm(back.GetService()), runtime.GetTaskService())
	proc.Session = back
	proc.Base.SessionInfo.StorageEngine = runtime.StorageEngine
	proc.Base.SessionInfo.TimeZone = back.GetTimeZone()
	return proc
}

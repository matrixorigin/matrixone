// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package adaptivetop

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "adaptive_top"

func (a *AdaptiveTop) String(buf *bytes.Buffer) { buf.WriteString(opName) }
func (a *AdaptiveTop) OpType() vm.OpType        { return vm.AdaptiveTop }

func (a *AdaptiveTop) Prepare(proc *process.Process) error {
	if a.Branches < 2 || a.Branches > 3 || len(a.Children) != 1 || len(proc.Reg.MergeReceivers) != a.Branches {
		return moerr.NewInternalErrorNoCtx("invalid adaptive top topology")
	}
	child, ok := a.GetChildren(0).(*merge.Merge)
	if !ok || !child.Partial || child.StartIDX != 0 || child.EndIDX != 0 || child.MaterializedSource != nil {
		return moerr.NewInternalErrorNoCtx("adaptive top requires an initially inactive merge")
	}
	if a.ctr.source != nil || a.ctr.output != nil || a.ctr.selected || a.ctr.done {
		return moerr.NewInternalErrorNoCtx("adaptive top reused without reset")
	}
	if a.OpAnalyzer == nil {
		a.OpAnalyzer = process.NewAnalyzer(a.GetIdx(), a.IsFirst, a.IsLast, opName)
	} else {
		a.OpAnalyzer.Reset()
	}
	if a.LimitExpr == nil {
		return moerr.NewInternalErrorNoCtx("adaptive top limit is missing")
	}
	var err error
	if a.ctr.limitExecutor == nil {
		a.ctr.limitExecutor, err = colexec.NewExpressionExecutor(proc, a.LimitExpr)
		if err != nil {
			return err
		}
	}
	value, err := a.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	if value == nil || value.Length() != 1 || value.GetType().Oid != types.T_uint64 || value.IsNull(0) {
		return moerr.NewInternalErrorNoCtx("invalid adaptive top limit value")
	}
	a.ctr.limit = vector.GetFixedAtNoTypeCheck[uint64](value, 0)
	return nil
}

func (a *AdaptiveTop) Call(proc *process.Process) (vm.CallResult, error) {
	if err := context.Cause(proc.Ctx); err != nil {
		return a.fail(proc, err)
	}
	result := vm.NewCallResult()
	if a.ctr.done || a.ctr.limit == 0 {
		if a.ctr.limit == 0 && !a.ctr.done {
			a.disableInactiveReceiver(proc)
		}
		a.ctr.done = true
		result.Status = vm.ExecStop
		return result, nil
	}
	if !a.ctr.selected {
		step, err := a.collectStep(proc)
		if err != nil {
			return a.fail(proc, err)
		}
		if step.Status == vm.ExecWaiting {
			if err := context.Cause(proc.Ctx); err != nil {
				return a.fail(proc, err)
			}
			return step, nil
		}
		if !a.ctr.selected {
			// The collector advanced one bounded quantum without producing a
			// replay batch. Keep the continuation live; ExecHasMore is distinct
			// from ExecNext(nil), which terminates a pipeline.
			step.Status = vm.ExecHasMore
			return step, nil
		}
	}
	if a.ctr.output != nil {
		a.ctr.output.Clean(proc.Mp())
		a.ctr.output = nil
	}
	// collect 已确认 EOF 和候选最终成功；不能在此之前调用 Source.Next。
	bat, end, err := a.ctr.source.Next(proc.Ctx, 0, a.ctr.position)
	if err != nil {
		return a.fail(proc, err)
	}
	if end {
		a.discard(proc)
		a.ctr.done = true
		result.Status = vm.ExecStop
		return result, nil
	}
	a.ctr.position++
	a.ctr.output = bat
	result.Batch = bat
	return result, nil
}

// collectStep advances exactly one child/branch event. It deliberately never
// loops on a child or completion barrier: a merge receiver can be empty for
// an arbitrary amount of time, so every wait must be returned to the VM
// continuation as ExecWaiting.
func (a *AdaptiveTop) collectStep(proc *process.Process) (vm.CallResult, error) {
	result := vm.NewCallResult()
	if a.startBranch == nil || a.waitBranch == nil {
		return result, moerr.NewInternalErrorNoCtx("adaptive top branch lifecycle is not installed")
	}
	if a.account == nil {
		return result, mpool.ErrAllocationAccountInvalid
	}
	config := a.SpillConfig
	config.AllocationAccount = a.account
	config.AllocationOwner = mpool.AllocationOwnerTop
	if a.ctr.source == nil {
		a.ctr.source = materialized.NewSource(1)
	}
	child := a.GetChildren(0).(*merge.Merge)
	if a.ctr.branchWaiting {
		if !a.ctr.branchDone {
			result.Status = vm.ExecWaiting
			result.OnReady = func(ready func()) error {
				if ready == nil {
					return moerr.NewInternalErrorNoCtx("adaptive top branch readiness callback is nil")
				}
				a.ctr.branchWake = ready
				if a.ctr.branchDone {
					ready()
				}
				return nil
			}
			return result, nil
		}
		if a.ctr.branchErr != nil {
			return result, a.ctr.branchErr
		}
		a.ctr.branchWaiting = false
		a.ctr.branchDone = false
		a.ctr.branchWake = nil
		a.ctr.source.Finish(nil)
		if a.ctr.branchRows == a.ctr.limit || a.ctr.branch == a.Branches-1 {
			a.ctr.selected = true
			return result, nil
		}
		a.ctr.source.ReleaseReader(0)
		a.ctr.branch++
		a.ctr.branchRows = 0
		a.ctr.branchStarted = false
	}

	if err := context.Cause(proc.Ctx); err != nil {
		return result, err
	}
	if !a.ctr.branchStarted {
		if err := a.ctr.source.Begin(proc.Mp(), config); err != nil {
			return result, err
		}
		// 先装 receiver，再启动对应 producer；前一 receiver 已消费到 EOF。
		if err := child.ActivateReceiverRange(proc, int32(a.ctr.branch), int32(a.ctr.branch+1)); err != nil {
			return result, err
		}
		if err := a.startBranch(a.ctr.branch); err != nil {
			return result, err
		}
		a.ctr.branchStarted = true
	}

	childResult, err := vm.ChildrenCall(child, proc, a.OpAnalyzer)
	if err != nil {
		if yielded, ok := vm.AsYieldError(err); ok {
			result.Status = vm.ExecWaiting
			result.OnReady = yielded.OnReady
			return result, nil
		}
		return result, err
	}
	if childResult.Batch == nil {
		// EOF only proves that the connector emitted its terminal signal. The
		// producer completion event is a separate asynchronous barrier.
		a.ctr.branchWaiting = true
		a.ctr.branchDone = false
		a.ctr.branchErr = nil
		if err := a.waitBranch(a.ctr.branch, func(waitErr error) {
			a.ctr.branchErr = waitErr
			a.ctr.branchDone = true
			if a.ctr.branchWake != nil {
				a.ctr.branchWake()
			}
		}); err != nil {
			return result, err
		}
		result.Status = vm.ExecWaiting
		result.OnReady = func(ready func()) error {
			if ready == nil {
				return moerr.NewInternalErrorNoCtx("adaptive top branch readiness callback is nil")
			}
			a.ctr.branchWake = ready
			if a.ctr.branchDone {
				ready()
			}
			return nil
		}
		return result, nil
	}
	if childResult.Batch.Last() {
		return result, moerr.NewInternalErrorNoCtx("adaptive top received an unfinished recursive batch")
	}
	if childResult.Batch.IsEmpty() {
		result.Status = vm.ExecHasMore
		return result, nil
	}
	count := uint64(childResult.Batch.RowCount())
	if count > a.ctr.limit-a.ctr.branchRows {
		return result, moerr.NewInternalErrorNoCtx("adaptive top candidate exceeded its final page limit")
	}
	stats, err := a.ctr.source.AppendWithStats(childResult.Batch)
	a.OpAnalyzer.SetMemUsed(stats.RetainedBytes)
	if stats.SpilledBytes > 0 {
		a.OpAnalyzer.Spill(stats.SpilledBytes)
		a.OpAnalyzer.SpillRows(stats.SpilledRows)
	}
	if err != nil {
		return result, err
	}
	a.ctr.branchRows += count
	result.Status = vm.ExecHasMore
	return result, nil
}

func (a *AdaptiveTop) fail(proc *process.Process, err error) (vm.CallResult, error) {
	a.disableInactiveReceiver(proc)
	a.discard(proc)
	a.ctr.done = true
	return vm.CancelResult, err
}

func (a *AdaptiveTop) disableInactiveReceiver(proc *process.Process) {
	if len(a.Children) != 1 {
		return
	}
	if child, ok := a.GetChildren(0).(*merge.Merge); ok {
		child.DisableReceiverWaitForStartFailure(proc)
	}
}

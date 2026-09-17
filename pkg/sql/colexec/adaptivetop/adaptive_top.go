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
		a.ctr.done = true
		result.Status = vm.ExecStop
		return result, nil
	}
	if !a.ctr.selected {
		if err := a.collect(proc); err != nil {
			return a.fail(proc, err)
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

func (a *AdaptiveTop) collect(proc *process.Process) error {
	if a.startBranch == nil || a.waitBranch == nil {
		return moerr.NewInternalErrorNoCtx("adaptive top branch lifecycle is not installed")
	}
	if a.account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	config := a.SpillConfig
	config.AllocationAccount = a.account
	config.AllocationOwner = mpool.AllocationOwnerTop
	a.ctr.source = materialized.NewSource(1)
	child := a.GetChildren(0).(*merge.Merge)
	for branch := 0; branch < a.Branches; branch++ {
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
		if err := a.ctr.source.Begin(proc.Mp(), config); err != nil {
			return err
		}
		// 先装 receiver，再启动对应 producer；前一 receiver 已消费到 EOF。
		if err := child.ActivateReceiverRange(proc, int32(branch), int32(branch+1)); err != nil {
			return err
		}
		if err := a.startBranch(branch); err != nil {
			return err
		}
		var rows uint64
		for {
			if err := context.Cause(proc.Ctx); err != nil {
				return err
			}
			result, err := vm.ChildrenCall(child, proc, a.OpAnalyzer)
			if err != nil {
				return err
			}
			if result.Batch == nil {
				break
			}
			if result.Batch.Last() {
				return moerr.NewInternalErrorNoCtx("adaptive top received an unfinished recursive batch")
			}
			if result.Batch.IsEmpty() {
				continue
			}
			count := uint64(result.Batch.RowCount())
			if count > a.ctr.limit-rows {
				return moerr.NewInternalErrorNoCtx("adaptive top candidate exceeded its final page limit")
			}
			if err := a.ctr.source.Append(result.Batch); err != nil {
				return err
			}
			rows += count
		}
		// EOF 只证明连接器发出终态，不证明 Run/RemoteRun 的 defer 已完成。
		if err := a.waitBranch(branch); err != nil {
			return err
		}
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
		a.ctr.source.Finish(nil)
		if rows == a.ctr.limit || branch == a.Branches-1 {
			a.ctr.selected = true
			return nil
		}
		a.ctr.source.ReleaseReader(0)
	}
	return moerr.NewInternalErrorNoCtx("adaptive top has no terminal candidate")
}

func (a *AdaptiveTop) fail(proc *process.Process, err error) (vm.CallResult, error) {
	a.discard(proc)
	a.ctr.done = true
	return vm.CancelResult, err
}

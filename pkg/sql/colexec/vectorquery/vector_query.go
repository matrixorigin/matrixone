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

package vectorquery

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// VectorQuery consumes a scalar provider before starting either result branch.
// Source is Compile-owned: Begin/Close and reader cleanup belong to that owner.
// The provider is the only producer; branch 1 uses reader 0 (ANN), branch 2 uses
// reader 1 (the unmodified scalar relation for NULL/empty providers).
// This operator and all three lazy branches must remain on the coordinator.
type VectorQuery struct {
	vm.OperatorBase
	LimitExpr     *plan.Expr
	Source        *materialized.Source
	startBranch   func(int) error
	waitBranch    func(int) error
	limitExecutor colexec.ExpressionExecutor
	limit         uint64
	selected      int
	done          bool
}

var _ vm.Operator = (*VectorQuery)(nil)

func init() {
	reuse.CreatePool[VectorQuery](func() *VectorQuery { return &VectorQuery{} },
		func(a *VectorQuery) { *a = VectorQuery{} }, reuse.DefaultOptions[VectorQuery]().WithEnableChecker())
}

func NewArgument() *VectorQuery                          { return reuse.Alloc[VectorQuery](nil) }
func (a *VectorQuery) String(buf *bytes.Buffer)          { buf.WriteString("vector_query") }
func (a *VectorQuery) TypeName() string                  { return "vector_query" }
func (a *VectorQuery) OpType() vm.OpType                 { return vm.VectorQuery }
func (a *VectorQuery) GetOperatorBase() *vm.OperatorBase { return &a.OperatorBase }
func (a *VectorQuery) ExecProjection(_ *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
func (a *VectorQuery) SetBranchStarter(f func(int) error) { a.startBranch = f }
func (a *VectorQuery) ClearBranchStarter()                { a.startBranch = nil }
func (a *VectorQuery) SetBranchWaiter(f func(int) error)  { a.waitBranch = f }
func (a *VectorQuery) ClearBranchWaiter()                 { a.waitBranch = nil }
func (a *VectorQuery) DeferFirstBranch() bool             { return true }

func (a *VectorQuery) Prepare(proc *process.Process) error {
	if len(a.Children) != 1 || len(proc.Reg.MergeReceivers) != 3 || a.Source == nil || a.LimitExpr == nil || a.selected != 0 || a.done {
		return moerr.NewInternalErrorNoCtx("invalid vector query topology or generation")
	}
	child, ok := a.GetChildren(0).(*merge.Merge)
	if !ok || !child.Partial || child.StartIDX != 0 || child.EndIDX != 0 || child.MaterializedSource != nil {
		return moerr.NewInternalErrorNoCtx("vector query requires an inactive merge")
	}
	if a.OpAnalyzer == nil {
		a.OpAnalyzer = process.NewAnalyzer(a.GetIdx(), a.IsFirst, a.IsLast, "vector_query")
	} else {
		a.OpAnalyzer.Reset()
	}
	var err error
	if a.limitExecutor == nil {
		a.limitExecutor, err = colexec.NewExpressionExecutor(proc, a.LimitExpr)
		if err != nil {
			return err
		}
	}
	value, err := a.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	if value == nil || value.Length() != 1 || value.GetType().Oid != types.T_uint64 || value.IsNull(0) {
		return moerr.NewInternalErrorNoCtx("invalid vector query limit")
	}
	a.limit = vector.GetFixedAtNoTypeCheck[uint64](value, 0)
	return nil
}

func (a *VectorQuery) Call(proc *process.Process) (vm.CallResult, error) {
	result := vm.NewCallResult()
	if err := context.Cause(proc.Ctx); err != nil {
		return a.fail(err)
	}
	if a.done || a.limit == 0 {
		a.done = true
		a.Source.Finish(nil)
		result.Status = vm.ExecStop
		return result, nil
	}
	if a.selected == 0 {
		if err := a.selectBranch(proc); err != nil {
			return a.fail(err)
		}
	}
	var err error
	for {
		result, err = vm.ChildrenCall(a.GetChildren(0), proc, a.OpAnalyzer)
		if err != nil {
			return a.fail(err)
		}
		// Ordered producers can emit control batches after the final page.
		// They carry transport metadata, not the SQL result schema.
		if result.Batch == nil || !result.Batch.Last() {
			break
		}
	}
	if result.Batch == nil {
		if err = a.waitBranch(a.selected); err != nil {
			return a.fail(err)
		}
		a.done = true
		result.Status = vm.ExecStop
	}
	return result, nil
}

func (a *VectorQuery) selectBranch(proc *process.Process) error {
	if a.startBranch == nil || a.waitBranch == nil {
		return moerr.NewInternalErrorNoCtx("vector query branch lifecycle is missing")
	}
	child := a.GetChildren(0).(*merge.Merge)
	if err := child.ActivateReceiverRange(proc, 0, 1); err != nil {
		return err
	}
	if err := a.startBranch(0); err != nil {
		return err
	}
	rows := 0
	nonNull := false
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
		bat := result.Batch
		if bat.Last() || bat.RowCount() == 0 {
			continue
		}
		rows += bat.RowCount()
		if rows > 1 {
			return moerr.NewInternalErrorNoCtx("scalar vector provider exceeded its single-row proof")
		}
		if len(bat.Vecs) != 1 || bat.Vecs[0] == nil || bat.Vecs[0].Length() != 1 {
			return moerr.NewInternalErrorNoCtx("invalid scalar vector provider batch")
		}
		nonNull = !bat.Vecs[0].IsNull(0)
		if err := a.Source.Append(bat); err != nil {
			return err
		}
	}
	// EOF is not sufficient: a producer can fail during final cleanup.
	if err := a.waitBranch(0); err != nil {
		return err
	}
	a.Source.Finish(nil)
	a.selected = 2
	if rows == 1 && nonNull {
		a.selected = 1
	}
	if err := child.ActivateReceiverRange(proc, int32(a.selected), int32(a.selected+1)); err != nil {
		return err
	}
	return a.startBranch(a.selected)
}

func (a *VectorQuery) fail(err error) (vm.CallResult, error) {
	a.done = true
	a.Source.Finish(err)
	result := vm.NewCallResult()
	result.Status = vm.ExecStop
	return result, err
}

func (a *VectorQuery) Reset(_ *process.Process, _ bool, err error) {
	a.Source.Finish(err)
	if a.limitExecutor != nil {
		a.limitExecutor.ResetForNextQuery()
	}
	a.limit, a.selected, a.done = 0, 0, false
	a.ClearBranchStarter()
	a.ClearBranchWaiter()
	if len(a.Children) == 1 {
		if child, ok := a.GetChildren(0).(*merge.Merge); ok {
			child.WithPartial(0, 0)
		}
	}
}
func (a *VectorQuery) Free(proc *process.Process, failed bool, err error) {
	a.Reset(proc, failed, err)
	if a.limitExecutor != nil {
		a.limitExecutor.Free()
		a.limitExecutor = nil
	}
}
func (a *VectorQuery) Release() {
	if a != nil {
		reuse.Free[VectorQuery](a, nil)
	}
}

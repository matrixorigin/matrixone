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

package window

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Every specification must be streamable. A partitioned or ordered window
// still uses the general window engine and its frame validation.
func (window *Window) streamingRowNumber() bool {
	if len(window.WinSpecList) == 0 {
		return false
	}
	for _, e := range window.WinSpecList {
		s := e.GetW()
		if s == nil || s.Name != "row_number" || s.WindowFunc.GetF() == nil || s.WindowFunc.GetF().Func.ObjName != "row_number" || len(s.WindowFunc.GetF().Args) != 0 || len(s.PartitionBy) != 0 || len(s.OrderBy) != 0 {
			return false
		}
	}
	return true
}
func (window *Window) ActivatesAllocationAccountLifecycle() bool { return window.streamingRowNumber() }
func (window *Window) SetAllocationAccount(a *mpool.AllocationAccount) error {
	if a == nil || a.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if window.ordinalAccount != nil && window.ordinalAccount != a {
		return mpool.ErrAllocationAccountMismatch
	}
	if window.ctr.ordinal != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	s, e := vector.NewAllocationAccountSelection(a, mpool.AllocationOwnerMin, 1, 2, 3, 4)
	if e != nil {
		return e
	}
	window.ordinalAccount = a
	window.ordinalAllocation = s
	return nil
}
func (window *Window) ClearAllocationAccount(a *mpool.AllocationAccount) error {
	if window.ordinalAccount == nil {
		return nil
	}
	if window.ordinalAccount != a {
		return mpool.ErrAllocationAccountMismatch
	}
	if window.ctr.ordinal != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	window.ordinalAccount = nil
	window.ordinalAllocation = nil
	return nil
}
func (ctr *container) clearRowNumber(proc *process.Process) {
	if ctr.ordinalOutput != nil {
		clear(ctr.ordinalOutput.Vecs)
		ctr.ordinalOutput = nil
	}
	if ctr.ordinal != nil {
		ctr.ordinal.Free(proc.Mp())
		ctr.ordinal = nil
	}
	for _, v := range ctr.ordinalExtras {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	ctr.ordinalExtras = nil
	ctr.ordinalCount = 0
}
func (window *Window) callRowNumber(proc *process.Process) (vm.CallResult, error) {
	c := &window.ctr
	if err, canceled := vm.CancelCheck(proc); canceled {
		return vm.CancelResult, err
	}
	// The wrapper does not own the child columns. Detach before advancing it.
	if c.ordinalOutput != nil {
		clear(c.ordinalOutput.Vecs)
	}
	if c.ordinal != nil {
		c.ordinal.CleanOnlyData()
	}
	for _, v := range c.ordinalExtras {
		if v != nil {
			v.CleanOnlyData()
		}
	}
	result, err := vm.ChildrenCall(window.GetChildren(0), proc, window.OpAnalyzer)
	if err != nil || result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, err
	}
	input := result.Batch
	n := input.RowCount()
	if int64(n) > math.MaxInt64-c.ordinalCount {
		return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "ROW_NUMBER exceeds INT64")
	}
	if c.ordinal == nil {
		if window.ordinalAllocation != nil {
			c.ordinal, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), window.ordinalAllocation)
		} else {
			c.ordinal = vector.NewVec(types.T_int64.ToType())
		}
		if err != nil {
			return vm.CancelResult, err
		}
	}
	for i := range n {
		if err := checkCanceled(proc, i); err != nil {
			return vm.CancelResult, err
		}
		if err := vector.AppendFixed(c.ordinal, c.ordinalCount+int64(i)+1, false, proc.Mp()); err != nil {
			return vm.CancelResult, err
		}
	}
	if c.ordinalOutput == nil {
		c.ordinalOutput = batch.NewWithSize(len(input.Vecs) + len(window.WinSpecList))
	}
	copy(c.ordinalOutput.Vecs, input.Vecs)
	c.ordinalOutput.Vecs[len(input.Vecs)] = c.ordinal
	for i := 1; i < len(window.WinSpecList); i++ {
		if len(c.ordinalExtras) < i {
			var v *vector.Vector
			if window.ordinalAllocation != nil {
				v, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), window.ordinalAllocation)
			} else {
				v = vector.NewVec(types.T_int64.ToType())
			}
			if err != nil {
				return vm.CancelResult, err
			}
			c.ordinalExtras = append(c.ordinalExtras, v)
		}
		v := c.ordinalExtras[i-1]
		if err := v.UnionBatch(c.ordinal, 0, n, nil, proc.Mp()); err != nil {
			return vm.CancelResult, err
		}
		c.ordinalOutput.Vecs[len(input.Vecs)+i] = v
	}
	c.ordinalOutput.SetRowCount(n)
	c.ordinalCount += int64(n)
	result.Batch = c.ordinalOutput
	return result, nil
}

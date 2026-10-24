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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type scalarIdentityState struct {
	account    *mpool.AllocationAccount
	allocation *vector.AllocationAccountSelection
	input      *batch.Batch
	retained   *mpool.RetainedMemoryReservation
	offset     int
	counter    uint64
	done       bool
}

func (s *scalarIdentityState) releaseInput() {
	s.retained.Release()
	s.retained = nil
	s.input = nil
	s.offset = 0
}

func (window *Window) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if !window.ScalarSetReaggregation {
		return window.ctr.allocation.Set(account)
	}
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	s := &window.scalarIdentity
	if s.account == account {
		return nil
	}
	if s.account != nil {
		return mpool.ErrAllocationAccountMismatch
	}
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerWindow, 1, 2, 3, 4)
	if err != nil {
		return err
	}
	s.account, s.allocation = account, selection
	return nil
}

func (window *Window) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if !window.ScalarSetReaggregation {
		return window.ctr.allocation.Clear(account)
	}
	s := &window.scalarIdentity
	if s.account == nil {
		return nil
	}
	if s.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if s.input != nil || s.retained != nil || window.ctr.rBat != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	s.account, s.allocation = nil, nil
	return nil
}

func (window *Window) prepareScalarIdentity() error {
	if len(window.WinSpecList) != 1 || window.PartitionTopN || window.WinSpecList[0].Typ.Id != int32(types.T_uint64) {
		return moerr.NewInvalidInputNoCtx("invalid scalar set identity window")
	}
	spec := window.WinSpecList[0].GetW()
	if spec == nil || spec.Name != "row_number" || len(spec.PartitionBy) != 0 || len(spec.OrderBy) != 0 {
		return moerr.NewInvalidInputNoCtx("scalar set identity requires unpartitioned unordered row_number")
	}
	f := spec.WindowFunc.GetF()
	if f == nil || f.Func == nil || int32(f.Func.Obj>>32) != function.ROW_NUMBER || len(f.Args) != 0 {
		return moerr.NewInvalidInputNoCtx("invalid scalar set identity function")
	}
	if window.scalarIdentity.account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	window.scalarIdentity.releaseInput()
	window.scalarIdentity.counter, window.scalarIdentity.done = 0, false
	return nil
}

func (window *Window) callScalarIdentity(proc *process.Process) (vm.CallResult, error) {
	s := &window.scalarIdentity
	if err, canceled := vm.CancelCheck(proc); canceled {
		return vm.CancelResult, err
	}
	if s.done {
		return vm.CancelResult, nil
	}
	for s.input == nil || s.offset == s.input.RowCount() {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return vm.CancelResult, err
		}
		// Call's common prefix has already released the preceding output views.
		// Only now may the child reuse the batch we borrowed from it.
		s.releaseInput()
		result, err := vm.ChildrenCall(window.GetChildren(0), proc, window.OpAnalyzer)
		if err != nil {
			return result, err
		}
		if result.Status == vm.ExecYield {
			return result, nil
		}
		if result.Batch == nil {
			s.done = true
			return result, nil
		}
		if result.Batch.IsEmpty() {
			continue
		}
		var retained uint64
		seen := make(map[*vector.Vector]struct{}, len(result.Batch.Vecs))
		for _, v := range result.Batch.Vecs {
			if v == nil {
				return vm.CancelResult, moerr.NewInvalidInputNoCtx("nil scalar set outer vector")
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			size := v.RetainedBytes()
			if size > math.MaxUint64-retained {
				return vm.CancelResult, mpool.ErrAllocationAllocatorLimit
			}
			retained += size
		}
		s.retained, err = s.account.ReserveRetainedMemory(retained, mpool.AllocationOwnerWindow)
		if err != nil {
			return vm.CancelResult, err
		}
		s.input = result.Batch
	}
	count := min(colexec.DefaultBatchSize, s.input.RowCount()-s.offset)
	if uint64(count) > math.MaxUint64-s.counter {
		return vm.CancelResult, moerr.NewScalarSetResourceBudgetExceeded(proc.Ctx, "occurrence-identity", s.counter, uint64(count), math.MaxUint64)
	}
	identity := vector.NewOffHeapVecWithType(types.T_uint64.ToType())
	if err := identity.SetAllocationAccount(s.allocation); err != nil {
		identity.Free(proc.Mp())
		return vm.CancelResult, err
	}
	if err := identity.PreExtend(count, proc.Mp()); err != nil {
		identity.Free(proc.Mp())
		return vm.CancelResult, err
	}
	identity.SetLength(count)
	values := vector.MustFixedColWithTypeCheck[uint64](identity)
	for i := range values {
		values[i] = s.counter + uint64(i) + 1
	}
	output, err := s.input.WindowWithAllocation(s.offset, s.offset+count, proc.Mp(), s.allocation)
	if err != nil {
		identity.Free(proc.Mp())
		return vm.CancelResult, err
	}
	output.Vecs = append(output.Vecs, identity)
	output.SetRowCount(count)
	window.ctr.rBat = output
	s.counter += uint64(count)
	s.offset += count
	return vm.CallResult{Batch: output, Status: vm.ExecNext}, nil
}

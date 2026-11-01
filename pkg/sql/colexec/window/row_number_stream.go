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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (w *Window) canStreamRowNumber() bool {
	if len(w.WinSpecList) != 1 || len(w.Aggs) != 1 || w.PartitionTopN {
		return false
	}
	spec := w.WinSpecList[0].GetW()
	return spec != nil && spec.Name == "row_number" &&
		len(spec.PartitionBy) == 0 && len(spec.OrderBy) == 0 &&
		w.Aggs[0].GetAggID() == aggexec.WinIdOfRowNumber &&
		!w.Aggs[0].IsDistinct() && len(w.Aggs[0].GetArgExpressions()) == 0
}

func (ctr *container) resetRowNumber() {
	ctr.streamRowNumber = false
	ctr.rowNumberInput = nil
	ctr.rowNumberCounter = 0
	ctr.rowNumberChildDone = false
	ctr.emitOffset = 0
}

// compileWin retains a single coordinator owner. Numbering needs no future
// input when neither partitioning nor ordering is requested. Borrowed child
// vectors stay valid until the next child Call. ODKU lookup joins still require
// the subsequent global SORT to restore this order before arbitration.
func (w *Window) callRowNumber(proc *process.Process) (vm.CallResult, error) {
	ctr := &w.ctr
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return vm.CancelResult, err
		}
		if ctr.rowNumberInput != nil {
			start := ctr.emitOffset
			end := min(start+colexec.DefaultBatchSize, ctr.rowNumberInput.RowCount())
			count := end - start
			if uint64(count) > math.MaxUint64-ctr.rowNumberCounter {
				return vm.NewCallResult(), moerr.NewInternalErrorNoCtx("ROW_NUMBER overflow")
			}
			vec, err := ctr.allocation.NewVector(types.T_uint64.ToType())
			if err != nil {
				return vm.NewCallResult(), err
			}
			if err = vec.PreExtend(count, proc.Mp()); err != nil {
				vec.Free(proc.Mp())
				return vm.NewCallResult(), err
			}
			vec.SetLength(count)
			values := vector.MustFixedColNoTypeCheck[uint64](vec)
			for i := range values {
				if err = checkCanceled(proc, i); err != nil {
					vec.Free(proc.Mp())
					return vm.CancelResult, err
				}
				values[i] = ctr.rowNumberCounter + uint64(i) + 1
			}
			var output *batch.Batch
			if ctr.allocation.Vector != nil {
				output, err = ctr.rowNumberInput.WindowWithAllocation(start, end, proc.Mp(), ctr.allocation.Vector)
			} else {
				output, err = ctr.rowNumberInput.Window(start, end)
			}
			if err != nil {
				vec.Free(proc.Mp())
				return vm.NewCallResult(), err
			}
			vec.SetPrepareParamKind(ctr.prepareParamKind.Get(0))
			output.Vecs = append(output.Vecs, vec)
			w.OpAnalyzer.Alloc(int64(vec.Size()))
			ctr.rBat = output
			ctr.rowNumberCounter += uint64(count)
			ctr.emitOffset = end
			if end == ctr.rowNumberInput.RowCount() {
				ctr.rowNumberInput = nil
				ctr.emitOffset = 0
			}
			return vm.CallResult{Batch: output, Status: vm.ExecNext}, nil
		}
		if ctr.rowNumberChildDone {
			return vm.CallResult{Status: vm.ExecStop}, nil
		}
		result, err := vm.ChildrenCall(w.GetChildren(0), proc, w.OpAnalyzer)
		if err != nil {
			return result, err
		}
		if result.Status == vm.ExecYield {
			return result, nil
		}
		ctr.rowNumberChildDone = result.Status == vm.ExecStop || result.Batch == nil
		if result.Batch == nil || result.Batch.IsEmpty() {
			continue
		}
		ctr.rowNumberInput = result.Batch
	}
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashbuild

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const recoveryCapacityQuantum = uint64(64 << 10)

func recoveryCheckedAdd(left, right uint64) (uint64, error) {
	if left > math.MaxUint64-right {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return left + right, nil
}

func recoveryCheckedMul(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return left * right, nil
}

func roundRecoveryCapacity(size uint64) (uint64, error) {
	if size == 0 {
		return 0, nil
	}
	if size > math.MaxUint64-(recoveryCapacityQuantum-1) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return (size + recoveryCapacityQuantum - 1) &^ (recoveryCapacityQuantum - 1), nil
}

// recoveryBatchProjection is the maximum logical batch materialization that
// one CopyIntoBatches call can add, plus the new logical size of its partial
// tail. Physical copy allocation is admitted independently by AllocationAccount.
type recoveryBatchProjection struct {
	maxRows          int
	maxSelected      uint64
	nextTailSelected uint64
	columns          int
}

func (ctr *container) ensureRecoveryCapacity(
	proc *process.Process,
	exprs []*plan.Expr,
	projection recoveryBatchProjection,
	analyzer process.Analyzer,
) error {
	if ctr.recoveryCapacity == nil || projection.maxRows <= 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	expression := ctr.expressionRecoveryPeak
	if projection.maxRows > ctr.expressionRecoveryRows {
		var err error
		expression, err = expressionRecoveryBytes(
			proc,
			exprs,
			projection.maxRows,
			ctr.hashmapBuilder.needDupVec,
		)
		if err != nil {
			return err
		}
	}
	spill, err := spillRecoveryPeak(projection)
	if err != nil {
		return err
	}
	expression = max(expression, ctr.expressionRecoveryPeak)
	spill = max(spill, ctr.spillRecoveryPeak)
	target, err := recoveryCheckedAdd(expression, spill)
	if err != nil {
		return err
	}
	target, err = roundRecoveryCapacity(target)
	if err != nil {
		return err
	}
	if err = ctr.recoveryCapacity.EnsureCapacity(target); err != nil {
		if analyzer != nil {
			analyzer.GetOpStats().AddExtraStat(
				"HashBuildSpillRecoveryReserveRejects", 1)
		}
		return err
	}
	ctr.expressionRecoveryPeak = expression
	ctr.expressionRecoveryRows = max(
		ctr.expressionRecoveryRows, projection.maxRows)
	ctr.spillRecoveryPeak = spill
	if analyzer != nil {
		analyzer.GetOpStats().SetMaxExtraStat(
			"HashBuildSpillRecoveryReservedBytes", hashBuildStatInt64(target))
	}
	return nil
}

func (hb *HashmapBuilder) projectRetainedRecovery(
	src *batch.Batch,
) (recoveryBatchProjection, error) {
	if src == nil || src.RowCount() <= 0 {
		return recoveryBatchProjection{}, process.ErrHashBuildBudgetInvalid
	}
	projection := recoveryBatchProjection{columns: len(src.Vecs)}
	last := len(hb.Batches.Buf) - 1
	hasTail := last >= 0 && hb.Batches.Buf[last] != nil &&
		hb.Batches.Buf[last].RowCount() != colexec.DefaultBatchSize

	// A full source is copied as its own destination and swapped ahead of an
	// existing partial tail, so that tail remains unchanged.
	if src.RowCount() == colexec.DefaultBatchSize {
		selected, err := projectedSelectedRange(src, 0, src.RowCount())
		if err != nil {
			return recoveryBatchProjection{}, err
		}
		projection.maxRows = src.RowCount()
		projection.maxSelected = selected
		if hasTail {
			projection.nextTailSelected = hb.retainedSpillTailSelected
		}
		return projection, nil
	}

	offset := 0
	if hasTail {
		tailRows := hb.Batches.Buf[last].RowCount()
		appendRows := min(colexec.DefaultBatchSize-tailRows, src.RowCount())
		appended, err := projectedSelectedRange(src, 0, appendRows)
		if err != nil {
			return recoveryBatchProjection{}, err
		}
		combined, err := recoveryCheckedAdd(hb.retainedSpillTailSelected, appended)
		if err != nil {
			return recoveryBatchProjection{}, err
		}
		projection.maxRows = tailRows + appendRows
		projection.maxSelected = combined
		if projection.maxRows < colexec.DefaultBatchSize {
			projection.nextTailSelected = combined
		}
		offset = appendRows
	}

	for offset < src.RowCount() {
		rows := min(colexec.DefaultBatchSize, src.RowCount()-offset)
		selected, err := projectedSelectedRange(src, offset, rows)
		if err != nil {
			return recoveryBatchProjection{}, err
		}
		if rows > projection.maxRows {
			projection.maxRows = rows
		}
		if selected > projection.maxSelected {
			projection.maxSelected = selected
		}
		if rows < colexec.DefaultBatchSize {
			projection.nextTailSelected = selected
		} else if offset+rows == src.RowCount() {
			projection.nextTailSelected = 0
		}
		offset += rows
	}
	return projection, nil
}

func projectedSelectedRange(src *batch.Batch, start, rows int) (uint64, error) {
	if src == nil || start < 0 || rows < 0 || start > src.RowCount() ||
		rows > src.RowCount()-start {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	var total uint64
	for _, vec := range src.Vecs {
		if vec == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		typeSize := vec.GetType().TypeSize()
		if typeSize < 0 {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		descriptors, err := recoveryCheckedMul(uint64(rows), uint64(typeSize))
		if err != nil {
			return 0, err
		}
		total, err = recoveryCheckedAdd(total, descriptors)
		if err != nil {
			return 0, err
		}
		_, payload, err := unionBatchAreaProjection(vec, start, rows)
		if err != nil {
			return 0, err
		}
		total, err = recoveryCheckedAdd(total, payload)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// projectedUnionArea mirrors the logical payload that UnionBatch retains and
// UnionInt32 later materializes. The physical result is retained for focused
// allocation tests and documents the const/full-vector distinction.
func unionBatchAreaProjection(
	src *vector.Vector,
	start, rows int,
) (physical int, selected uint64, err error) {
	if src == nil || !src.GetType().IsVarlen() {
		return 0, 0, nil
	}
	if start < 0 || rows < 0 || start > src.Length() || rows > src.Length()-start {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	if rows == 0 || len(src.GetArea()) == 0 {
		return 0, 0, nil
	}
	if src.IsConst() {
		payload, err := selectedVarlenaPayload(src, 0, 1)
		if err != nil || payload > math.MaxInt {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		selected, err = recoveryCheckedMul(payload, uint64(rows))
		return int(payload), selected, err
	}
	if start == 0 && rows == src.Length() && src.VarlenaAreaIsDisjoint() {
		return len(src.GetArea()), uint64(len(src.GetArea())), nil
	}
	payload, err := selectedVarlenaPayload(src, start, rows)
	if err != nil || payload > math.MaxInt {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	if start == 0 && rows == src.Length() {
		return len(src.GetArea()), payload, nil
	}
	return int(payload), payload, nil
}

func selectedVarlenaPayload(src *vector.Vector, start, rows int) (uint64, error) {
	values, _ := vector.MustVarlenaRawData(src)
	end := start + rows
	if start < 0 || rows < 0 || end > len(values) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	var payload uint64
	for row := start; row < end; row++ {
		index := row
		if src.IsConst() {
			index = 0
		}
		if src.GetNulls().Contains(uint64(index)) || values[index].IsSmall() {
			continue
		}
		_, length := values[index].OffsetLen()
		var err error
		payload, err = recoveryCheckedAdd(payload, uint64(length))
		if err != nil {
			return 0, err
		}
	}
	return payload, nil
}

func spillRecoveryPeak(projection recoveryBatchProjection) (uint64, error) {
	if projection.maxRows <= 0 || projection.columns < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	rowScratch, err := recoveryCheckedMul(uint64(projection.maxRows), 12)
	if err != nil {
		return 0, err
	}
	// Null/grouping bitmaps, vector growth rounding, and record framing have a
	// bounded per-column allowance. The selected batch and marshal buffer are
	// simultaneously live, hence two copies of the logical materialization.
	columnSlack, err := recoveryCheckedMul(uint64(projection.columns), 16<<10)
	if err != nil {
		return 0, err
	}
	slack, err := recoveryCheckedAdd(64<<10, columnSlack)
	if err != nil {
		return 0, err
	}
	selected, err := recoveryCheckedAdd(projection.maxSelected, slack)
	if err != nil {
		return 0, err
	}
	total, err := recoveryCheckedAdd(rowScratch, selected)
	if err != nil {
		return 0, err
	}
	columnMarshalSlack, err := recoveryCheckedMul(uint64(projection.columns), 128)
	if err != nil {
		return 0, err
	}
	marshalSlack, err := recoveryCheckedAdd(64<<10, columnMarshalSlack)
	if err != nil {
		return 0, err
	}
	marshal, err := recoveryCheckedAdd(selected, marshalSlack)
	if err != nil {
		return 0, err
	}
	return recoveryCheckedAdd(total, marshal)
}

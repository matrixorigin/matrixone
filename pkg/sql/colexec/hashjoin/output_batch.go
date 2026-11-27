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

package hashjoin

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// outputRow holds source positions until a selected row has been published.
// A nil side supplies NULLs for an outer join.
type outputRow struct {
	left, right       *batch.Batch
	leftRow, rightRow int64
	markValue         bool
	markNull          bool
}

func (row outputRow) source(relation int32) (*batch.Batch, int64) {
	if relation == 0 {
		return row.left, row.leftRow
	}
	return row.right, row.rightRow
}

func (ctr *container) maxOutputRows() int {
	if ctr.outputRowLimit > 0 {
		return ctr.outputRowLimit
	}
	return colexec.DefaultBatchSize
}

func (ctr *container) retainedOutputBatch(index int64) *batch.Batch {
	// A join with no build projection can publish a hash map without payload.
	if len(ctr.rightBats) == 0 {
		return nil
	}
	return ctr.rightBats[index]
}

// needsOutputAreaCheck only chooses whether projection needs to count bytes.
// Even a build value that never matches can enable the check, but only payloads
// selected for output use up the result batch's physical area.
func needsOutputAreaCheck(columns []colexec.ResultPos, source *batch.Batch, relation int32, copyFlatPayloadOnce bool) bool {
	limit := mpool.MaxAllocationSize()
	threshold := limit / colexec.DefaultBatchSize
	for _, column := range columns {
		if column.Rel != relation || column.Pos < 0 || int(column.Pos) >= len(source.Vecs) {
			continue
		}
		vec := source.Vecs[column.Pos]
		// One copy per disjoint input payload fits in the source area.
		if copyFlatPayloadOnce && vec.VarlenaAreaIsDisjoint() && int64(len(vec.GetArea())) <= limit {
			continue
		}
		if vector.VarlenaPayloadExceeds(vec, threshold) {
			return true
		}
	}
	return false
}

func (hashJoin *HashJoin) prepareBuildOutputAreaCheck() {
	ctr := &hashJoin.ctr
	if ctr.buildAreaChecked || hashJoin.IsShuffle || ctr.mp == nil || ctr.spillEngine != nil {
		return
	}
	ctr.buildAreaChecked = true
	buildTypes := hashJoin.RightTypes
	threshold := mpool.MaxAllocationSize() / colexec.DefaultBatchSize
	for _, column := range hashJoin.ResultCols {
		if column.Rel == 1 && column.Pos >= 0 && int(column.Pos) < len(buildTypes) &&
			buildTypes[column.Pos].IsVarlen() && ctr.mp.BuildPayloadExceeds(int(column.Pos), threshold) {
			ctr.checkBuildArea = true
			return
		}
	}
}

type outputArea struct {
	column    int
	remaining int64
}

type outputAreas []outputArea

func (ctr *container) outputAreas(areas outputAreas, check bool) outputAreas {
	if check {
		limit := mpool.MaxAllocationSize()
		for i, dst := range ctr.resBat.Vecs {
			if dst.GetType().IsVarlen() {
				areas = append(areas, outputArea{column: i, remaining: limit})
			}
		}
	}
	return areas
}

// reserve accounts only for the selected row. In a bulk copy, previous names
// the immediately preceding selection on sides whose flat payload can be
// shared. On rejection the caller publishes the accepted prefix and discards
// this budget, so no rollback of its scratch counters is needed.
func (areas outputAreas) reserve(columns []colexec.ResultPos, row, previous outputRow, haveRows bool) bool {
	for i := range areas {
		area := &areas[i]
		column := columns[area.column]
		source, sel := row.source(column.Rel)
		if source == nil {
			continue
		}
		vec := source.Vecs[column.Pos]
		prevSource, prevSel := previous.source(column.Rel)
		if column.Rel == 0 && prevSource == source && sel == prevSel {
			continue
		}
		area.remaining -= vector.VarlenaPayloadSize(vec, int(sel))
		if area.remaining < 0 && haveRows {
			return false
		}
		// An oversized first value has no smaller batch to move to. Let the
		// vector append report the allocator error through the usual path.
	}
	return true
}

func (ctr *container) fitGroupOutput(
	hashJoin *HashJoin, areas outputAreas, probeRow int64, count, outputRows int,
) (int, bool) {
	var previous outputRow
	for i, ordinal := range ctr.sels[:count] {
		batchIdx, rowIdx := ctr.locateBuildRow(int64(ordinal))
		row := outputRow{left: ctr.leftBat, leftRow: probeRow, right: ctr.retainedOutputBatch(batchIdx), rightRow: rowIdx}
		if !areas.reserve(hashJoin.ResultCols, row, previous, outputRows > 0) {
			return i, true
		}
		outputRows++
		// UnionMulti shares a flat probe value across the group. Build values
		// use UnionOne and each selected row contributes its own payload.
		previous.left, previous.leftRow = row.left, row.leftRow
	}
	return count, false
}

// A unique left join with no residual emits one row per probe row. Start the
// bulk projection only when the entire input fits one output batch.
func (ctr *container) copyUniqueLeftProbeColumns(hashJoin *HashJoin, proc *process.Process, outputLimit int) (bool, error) {
	count := ctr.leftBat.RowCount()
	if hashJoin.JoinType != plan.Node_LEFT || hashJoin.IsRightJoin || !ctr.probeHashOnPK ||
		hashJoin.NonEqCond != nil || ctr.spillEngine != nil || ctr.outputTarget > 0 ||
		ctr.checkBuildArea || ctr.checkProbeArea || ctr.probeState != psNextBatch ||
		ctr.lastIdx != 0 || ctr.pendingOutput.left != nil || count == 0 || count > outputLimit {
		return false, nil
	}
	for i, column := range hashJoin.ResultCols {
		if column.Rel != 0 {
			continue
		}
		if column.Pos < 0 || int(column.Pos) >= len(ctr.leftBat.Vecs) {
			return false, nil
		}
		src, dst := ctr.leftBat.Vecs[column.Pos], ctr.resBat.Vecs[i]
		if src == nil || dst == nil || dst.Length() != 0 || *src.GetType() != *dst.GetType() ||
			(!src.IsConst() && src.Length() != count) {
			return false, nil
		}
	}
	for i, column := range hashJoin.ResultCols {
		if column.Rel != 0 {
			continue
		}
		if err := context.Cause(proc.Ctx); err != nil {
			return false, err
		}
		src, dst := ctr.leftBat.Vecs[column.Pos], ctr.resBat.Vecs[i]
		if src.CanCopyWholeVarlenaArea() && len(src.GetArea()) > 0 {
			// A whole-vector copy includes dead payload left by overwrites.
			// Copy only live values when the physical area exceeds that payload.
			live := src.WindowSize(0, count) - int64(count)*int64(src.GetType().TypeSize())
			if int64(len(src.GetArea())) > live {
				for row := range count {
					if err := dst.UnionOne(src, int64(row), proc.Mp()); err != nil {
						return false, err
					}
				}
				continue
			}
		}
		if err := dst.UnionBatch(src, 0, count, nil, proc.Mp()); err != nil {
			return false, err
		}
	}
	return true, nil
}

// The hash iterator returns matches in probe order. Gather adjacent matches
// from the same build batch together, keeping misses in their original slots.
func (ctr *container) copyUniqueLeftBuildColumns(hashJoin *HashJoin, proc *process.Process) error {
	if err := context.Cause(proc.Ctx); err != nil {
		return err
	}
	var positions [hashmap.UnitLimit]int32
	var build *batch.Batch
	start := 0
	for i, match := range ctr.vs {
		var next *batch.Batch
		if match != 0 && ctr.zvs[i] != 0 && len(ctr.rightBats) != 0 {
			batchIndex, row := ctr.locateBuildRow(int64(match) - 1)
			next = ctr.rightBats[batchIndex]
			positions[i] = int32(row)
		}
		if next != build {
			if i > start {
				if err := ctr.appendUniqueLeftBuildRun(hashJoin, proc, build, positions[start:i]); err != nil {
					return err
				}
			}
			build, start = next, i
		}
	}
	return ctr.appendUniqueLeftBuildRun(hashJoin, proc, build, positions[start:len(ctr.vs)])
}

func (ctr *container) appendUniqueLeftBuildRun(hashJoin *HashJoin, proc *process.Process, build *batch.Batch, positions []int32) error {
	mp := proc.Mp()
	for i, column := range hashJoin.ResultCols {
		if column.Rel == 0 {
			continue
		}
		dst := ctr.resBat.Vecs[i]
		if build == nil {
			if err := vector.AppendMultiFixed(dst, 0, true, len(positions), mp); err != nil {
				return err
			}
			continue
		}
		if column.Pos < 0 || int(column.Pos) >= len(build.Vecs) {
			return moerr.NewInternalErrorNoCtx("hash join output column is out of range")
		}
		src := build.Vecs[column.Pos]
		if len(positions) == 1 || (src.IsConst() && src.HasGrouping()) {
			// A constant can still carry row-specific GROUPING bits.
			for _, row := range positions {
				if err := dst.UnionOne(src, int64(row), mp); err != nil {
					return err
				}
			}
		} else if err := dst.UnionInt32(src, positions, mp); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) appendOutputRow(hashJoin *HashJoin, proc *process.Process, row outputRow) error {
	for i, column := range hashJoin.ResultCols {
		dst := ctr.resBat.Vecs[i]
		if column.Rel == -1 {
			if err := vector.AppendFixed(dst, row.markValue, row.markNull, proc.Mp()); err != nil {
				return err
			}
			continue
		}
		source, sel := row.source(column.Rel)
		if source == nil {
			if ctr.outputTarget > 0 {
				ctr.outputBytes += int64(dst.GetType().TypeSize())
			}
			if err := dst.UnionNull(proc.Mp()); err != nil {
				return err
			}
			continue
		}
		if column.Pos < 0 || int(column.Pos) >= len(source.Vecs) {
			return moerr.NewInternalErrorNoCtx("hash join output column is out of range")
		}
		if ctr.outputTarget > 0 {
			ctr.outputBytes += source.Vecs[column.Pos].WindowSize(int(sel), int(sel)+1)
		}
		if err := dst.UnionOne(source.Vecs[column.Pos], sel, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
		if copyFlatPayloadOnce && vec.VarlenaAreaIsDisjoint() && !vec.IsDictionary() && int64(len(vec.GetArea())) <= limit {
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
	relation, buildTypes := int32(1), hashJoin.RightTypes
	if hashJoin.AsofBuildLeft {
		relation, buildTypes = 0, hashJoin.LeftTypes
	}
	threshold := mpool.MaxAllocationSize() / colexec.DefaultBatchSize
	for _, column := range hashJoin.ResultCols {
		if column.Rel == relation && column.Pos >= 0 && int(column.Pos) < len(buildTypes) &&
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
		if prevSource == source && !vec.IsDictionary() &&
			(vec.IsConst() || (column.Rel == 0 && sel == prevSel)) {
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

// fitFindOutput preserves the Find window and its gather when the selected
// rows fit. Only the examined prefix can be consumed when an output row does
// not fit.
func (ctr *container) fitFindOutput(
	hashJoin *HashJoin, areas outputAreas, count, outputRows int, membership bool,
) (int, bool) {
	var previous outputRow
	for i := range count {
		pos := ctr.vsIdx + i
		matched := ctr.zvs[pos] != 0 && ctr.vs[pos] != 0
		if membership {
			if matched != ctr.probeLeftSemi {
				continue
			}
		} else if !matched {
			continue
		}
		row := outputRow{left: ctr.leftBat, leftRow: int64(ctr.lastIdx + i)}
		if !membership {
			batchIdx, rowIdx := ctr.locateBuildRow(int64(ctr.vs[pos] - 1))
			row.right, row.rightRow = ctr.retainedOutputBatch(batchIdx), rowIdx
		}
		if !areas.reserve(hashJoin.ResultCols, row, previous, outputRows > 0) {
			return i, true
		}
		outputRows++
		previous = row
	}
	return count, false
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
			if err := dst.UnionNull(proc.Mp()); err != nil {
				return err
			}
			continue
		}
		if column.Pos < 0 || int(column.Pos) >= len(source.Vecs) {
			return moerr.NewInternalErrorNoCtx("hash join output column is out of range")
		}
		if err := dst.UnionOne(source.Vecs[column.Pos], sel, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

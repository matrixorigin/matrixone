// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashjoin

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

// OutputEnvelope includes the target payload plus one crossing projected row,
// vector data/area allocator rounding, old and complete new storage, null and
// grouping bitmap growth. maxRowBytes is the admitted bound of either input row.
func (hashJoin *HashJoin) OutputEnvelope(unit, maxRowBytes int64) (int64, bool) {
	row := int64(0)
	multiplicities := [2]map[int32]int64{make(map[int32]int64), make(map[int32]int64)}
	maxMultiplicity := [2]int64{}
	for _, column := range hashJoin.ResultCols {
		var typ types.Type
		if column.Rel == 0 {
			typ = hashJoin.LeftTypes[column.Pos]
		} else {
			typ = hashJoin.RightTypes[column.Pos]
		}
		width := int64(typ.TypeSize())
		if row > math.MaxInt64-width {
			return 0, false
		}
		row += width
		if typ.IsVarlen() {
			side := 0
			if column.Rel != 0 {
				side = 1
			}
			multiplicities[side][column.Pos]++
			maxMultiplicity[side] = max(maxMultiplicity[side], multiplicities[side][column.Pos])
		}
	}
	copies := maxMultiplicity[0] + maxMultiplicity[1]
	if maxRowBytes < 0 || copies > 0 && maxRowBytes > (math.MaxInt64-row)/copies {
		return 0, false
	}
	row += copies * maxRowBytes

	metadata := int64(len(hashJoin.ResultCols)) * (16384 + 4096)
	if unit > math.MaxInt64-row || (unit+row) > (math.MaxInt64-metadata)/4 {
		return 0, false
	}
	return 4*(unit+row) + metadata, true
}

func (ctr *container) outputFull() bool {
	return ctr.outputBytes >= ctr.outputTarget && ctr.outputTarget > 0
}

func (ctr *container) outputRowBytes(join *HashJoin, leftRow, rightBatch, rightRow int64, match bool) int64 {
	total := int64(0)
	for _, column := range join.ResultCols {
		if column.Rel == 0 {
			total += ctr.leftBat.Vecs[column.Pos].WindowSize(int(leftRow), int(leftRow)+1)
		} else if match {
			total += ctr.rightBats[rightBatch].Vecs[column.Pos].WindowSize(int(rightRow), int(rightRow)+1)
		} else {
			total += int64(join.RightTypes[column.Pos].TypeSize())
		}
	}
	return total
}

// fixedOutputRowWidth is computed once per controlled Prepare. A zero width
// falls back to the general path (including zero-column projections).
func (join *HashJoin) fixedOutputRowWidth() (int64, bool) {
	width := int64(0)
	for _, column := range join.ResultCols {
		var typ types.Type
		if column.Rel == 0 {
			typ = join.LeftTypes[column.Pos]
		} else {
			typ = join.RightTypes[column.Pos]
		}
		if typ.IsVarlen() || int64(typ.TypeSize()) > math.MaxInt64-width {
			return 0, false
		}
		width += int64(typ.TypeSize())
	}
	return width, true
}

// Return the logical bytes along with the selected count so bulk append does
// not traverse the same varlen projected values a second time.
func (ctr *container) boundedMatchCount(join *HashJoin, leftRow int64, limit int) (int, int64) {
	if ctr.outputRowWidth > 0 {
		count := min(limit, int(max(int64(1), (ctr.outputTarget-ctr.outputBytes)/ctr.outputRowWidth)))
		return count, int64(count) * ctr.outputRowWidth
	}
	bytes := int64(0)
	count := 0
	for count < limit {
		selected := ctr.sels[count]
		next := ctr.outputRowBytes(join, leftRow, int64(selected/8192), int64(selected%8192), true)
		if count > 0 && next > ctr.outputTarget-ctr.outputBytes-bytes {
			break
		}
		bytes += next
		count++
		if bytes >= ctr.outputTarget-ctr.outputBytes {
			break
		}
	}
	return count, bytes
}

func preExtendJoinValue(dst, src *vector.Vector, row int64, proc *process.Process) (int64, error) {
	bytes := src.WindowSize(int(row), int(row)+1)
	area := bytes - int64(src.GetType().TypeSize())
	return bytes, dst.PreExtendWithArea(1, int(area), proc.Mp())
}

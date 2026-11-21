// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Selected dense loops keep the same accumulation/publication order while
// leaving the existing flat dense loops free of per-row selection branches.

func (exec *sumDecimal64FastExec) batchFillDenseDictionary(offset int, groups []uint64, vec *vector.Vector) error {
	if vec.IsConstNull() || len(groups) == 0 {
		return nil
	}
	view := vector.ResolveFixedReadView[types.Decimal64](vec)
	vals := view.Values
	hasNull := vec.HasNull()
	nulls := vec.GetNulls()
	var localSums [256]types.Decimal128
	var counts [256]int64
	var touched [256]uint8
	n := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if hasNull && nulls.Contains(uint64(row)) {
			continue
		}
		physical := int(view.Selection[row])
		if view.Constant {
			physical = 0
		}
		g := group - 1
		raw := vals[physical]
		val := types.Decimal128{B0_63: uint64(raw), B64_127: uint64(int64(raw) >> 63)}
		if counts[g] == 0 {
			touched[n] = uint8(g)
			n++
			localSums[g] = val
		} else {
			localSums[g] = localSums[g].Add128Unchecked(val)
		}
		counts[g]++
	}
	sums := chunkArr[types.Decimal128](exec.state[0].vecs[0])
	cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])
	for _, g := range touched[:n] {
		sums[g] = sums[g].Add128Unchecked(localSums[g])
		cnts[g] += counts[g]
	}
	return nil
}

func (exec *sumAvgDecExec[A, S]) batchFillDenseDictionary(offset int, groups []uint64, vec *vector.Vector) error {
	if vec.IsConstNull() || len(groups) == 0 {
		return nil
	}
	view := vector.ResolveFixedReadView[A](vec)
	vals := view.Values
	scale := exec.aggInfo.argTypes[0].Scale
	hasNull := vec.HasNull()
	nulls := vec.GetNulls()
	var localSums [256]S
	var counts [256]int64
	var touched [256]uint8
	n := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if hasNull && nulls.Contains(uint64(row)) {
			continue
		}
		physical := int(view.Selection[row])
		if view.Constant {
			physical = 0
		}
		g := group - 1
		val := decimalStateFromArg[A, S](vals[physical], scale)
		if counts[g] == 0 {
			touched[n] = uint8(g)
			n++
			localSums[g] = val
		} else if exec.localAddSafe {
			localSums[g] = decimalStateAddUnchecked(localSums[g], val)
		} else {
			var err error
			if localSums[g], err = decimalStateAdd(localSums[g], val); err != nil {
				return err
			}
		}
		counts[g]++
	}
	sums := chunkArr[S](exec.state[0].vecs[0])
	sumVec := exec.state[0].vecs[0]
	if exec.isSum {
		for _, g := range touched[:n] {
			if sumVec.IsNull(uint64(g)) {
				sumVec.UnsetNull(uint64(g))
				sums[g] = localSums[g]
			} else {
				var err error
				if sums[g], err = decimalStateAdd(sums[g], localSums[g]); err != nil {
					return err
				}
			}
		}
	} else {
		cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])
		for _, g := range touched[:n] {
			var err error
			if sums[g], err = decimalStateAdd(sums[g], localSums[g]); err != nil {
				return err
			}
			cnts[g] += counts[g]
		}
	}
	return nil
}

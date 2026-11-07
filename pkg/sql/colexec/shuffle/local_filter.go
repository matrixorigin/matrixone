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

package shuffle

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Filtering feeds the original batch's row indices into the existing exchange.
// It never materializes an intermediate batch or changes its borrowed input.
func (shuffle *Shuffle) routeWithLocalFilter(bat *batch.Batch, proc *process.Process) (bool, error) {
	vec := bat.Vecs[shuffle.ShuffleColIdx]
	if bat.RowCount() > len(shuffle.ctr.filterScratch) || !shuffle.ctr.localBloom.TestFixedVectorInto(vec, shuffle.ctr.filterScratch) {
		shuffle.ctr.filterBypassed = true
		return false, nil
	}
	mask := shuffle.ctr.filterScratch[:bat.RowCount()]
	survivors := 0
	for row, present := range mask {
		if vec.IsNull(uint64(row)) {
			mask[row] = 0
			continue
		}
		if present != 0 {
			survivors++
		}
	}
	shuffle.ctr.filterTested += int64(len(mask))
	shuffle.ctr.filterRejected += int64(len(mask) - survivors)
	if shuffle.ctr.filterTested >= 8192 && shuffle.ctr.filterRejected*10 < shuffle.ctr.filterTested {
		shuffle.ctr.filterBypassed = true
	}
	if survivors == len(mask) {
		return false, nil
	}
	if survivors == 0 {
		return true, nil
	}
	sels := shuffle.clearSels()
	switch vec.GetType().Oid {
	case types.T_int16:
		routeFilteredIntegers[int16](shuffle, vec, mask, sels, true)
	case types.T_int32:
		routeFilteredIntegers[int32](shuffle, vec, mask, sels, true)
	case types.T_int64:
		routeFilteredIntegers[int64](shuffle, vec, mask, sels, true)
	case types.T_uint16:
		routeFilteredIntegers[uint16](shuffle, vec, mask, sels, false)
	case types.T_uint32:
		routeFilteredIntegers[uint32](shuffle, vec, mask, sels, false)
	case types.T_uint64:
		routeFilteredIntegers[uint64](shuffle, vec, mask, sels, false)
	default:
		shuffle.ctr.filterBypassed = true
		return false, nil
	}
	return true, shuffle.enqueueBySels(bat, proc)
}

func routeFilteredIntegers[T interface {
	~int16 | ~int32 | ~int64 | ~uint16 | ~uint32 | ~uint64
}](s *Shuffle, vec *vector.Vector, mask []uint8, sels [][]int32, signed bool) {
	values := vector.MustFixedColNoTypeCheck[T](vec)
	constMask := -min(1, len(values)-1)
	for row, present := range mask {
		if present == 0 {
			continue
		}
		value := values[row&constMask]
		var bucket uint64
		if s.ShuffleType == int32(plan.ShuffleType_Hash) {
			bucket = plan2.SimpleInt64HashToRange(uint64(value), uint64(s.BucketNum))
		} else if signed {
			if len(s.ShuffleRangeInt64) > 0 {
				bucket = plan2.GetRangeShuffleIndexSignedSlice(s.ShuffleRangeInt64, int64(value))
			} else {
				bucket = plan2.GetRangeShuffleIndexSignedMinMax(s.ShuffleColMin, s.ShuffleColMax, int64(value), uint64(s.BucketNum))
			}
		} else {
			if len(s.ShuffleRangeUint64) > 0 {
				bucket = plan2.GetRangeShuffleIndexUnsignedSlice(s.ShuffleRangeUint64, uint64(value))
			} else {
				bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(s.ShuffleColMin), uint64(s.ShuffleColMax), uint64(value), uint64(s.BucketNum))
			}
		}
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

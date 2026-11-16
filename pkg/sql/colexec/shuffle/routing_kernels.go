// Copyright 2021 Matrix Origin
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

package shuffle

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type shuffleInteger interface {
	~int16 | ~int32 | ~int64 | ~uint16 | ~uint32 | ~uint64
}

// partitionNullableIntegerHash evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionNullableIntegerHash[T shuffleInteger](values []T, buckets uint64, nullVec *vector.Vector, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = 0
	if !nullVec.IsNull(uint64(row)) {
		bucket = plan2.SimpleInt64HashToRange(uint64(values[row]), buckets)
	}
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.SimpleInt64HashToRange(uint64(values[row]), buckets)
		}
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.SimpleInt64HashToRange(uint64(values[row]), buckets)
		}
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionSignedRangeMinMax evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionSignedRangeMinMax[T shuffleInteger](values []T, minimum, maximum int64, buckets uint64, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = plan2.GetRangeShuffleIndexSignedMinMax(minimum, maximum, int64(values[row]), buckets)
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexSignedMinMax(minimum, maximum, int64(values[row]), buckets)
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexSignedMinMax(minimum, maximum, int64(values[row]), buckets)
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionSignedRangeMinMaxNullable evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionSignedRangeMinMaxNullable[T shuffleInteger](values []T, minimum, maximum int64, buckets uint64, nullVec *vector.Vector, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = 0
	if !nullVec.IsNull(uint64(row)) {
		bucket = plan2.GetRangeShuffleIndexSignedMinMax(minimum, maximum, int64(values[row]), buckets)
	}
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexSignedMinMax(minimum, maximum, int64(values[row]), buckets)
		}
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexSignedMinMax(minimum, maximum, int64(values[row]), buckets)
		}
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionSignedRangeSlice evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionSignedRangeSlice[T shuffleInteger](values []T, boundaries []int64, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = plan2.GetRangeShuffleIndexSignedSlice(boundaries, int64(values[row]))
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexSignedSlice(boundaries, int64(values[row]))
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexSignedSlice(boundaries, int64(values[row]))
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionSignedRangeSliceNullable evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionSignedRangeSliceNullable[T shuffleInteger](values []T, boundaries []int64, nullVec *vector.Vector, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = 0
	if !nullVec.IsNull(uint64(row)) {
		bucket = plan2.GetRangeShuffleIndexSignedSlice(boundaries, int64(values[row]))
	}
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexSignedSlice(boundaries, int64(values[row]))
		}
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexSignedSlice(boundaries, int64(values[row]))
		}
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionUnsignedRangeMinMax evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionUnsignedRangeMinMax[T shuffleInteger](values []T, minimum, maximum uint64, buckets uint64, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(minimum, maximum, uint64(values[row]), buckets)
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(minimum, maximum, uint64(values[row]), buckets)
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(minimum, maximum, uint64(values[row]), buckets)
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionUnsignedRangeMinMaxNullable evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionUnsignedRangeMinMaxNullable[T shuffleInteger](values []T, minimum, maximum uint64, buckets uint64, nullVec *vector.Vector, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = 0
	if !nullVec.IsNull(uint64(row)) {
		bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(minimum, maximum, uint64(values[row]), buckets)
	}
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(minimum, maximum, uint64(values[row]), buckets)
		}
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(minimum, maximum, uint64(values[row]), buckets)
		}
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionUnsignedRangeSlice evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionUnsignedRangeSlice[T shuffleInteger](values []T, boundaries []uint64, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = plan2.GetRangeShuffleIndexUnsignedSlice(boundaries, uint64(values[row]))
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexUnsignedSlice(boundaries, uint64(values[row]))
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = plan2.GetRangeShuffleIndexUnsignedSlice(boundaries, uint64(values[row]))
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

// partitionUnsignedRangeSliceNullable evaluates each logical key once, then switches to ordinary
// selection building at the first differing bucket.
func partitionUnsignedRangeSliceNullable[T shuffleInteger](values []T, boundaries []uint64, nullVec *vector.Vector, partition *bucketSelections) {
	if len(values) == 0 {
		return
	}
	var bucket uint64
	row := 0
	bucket = 0
	if !nullVec.IsNull(uint64(row)) {
		bucket = plan2.GetRangeShuffleIndexUnsignedSlice(boundaries, uint64(values[row]))
	}
	first := bucket
	partition.first = first
	row = 1
	for ; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexUnsignedSlice(boundaries, uint64(values[row]))
		}
		if bucket != first {
			break
		}
	}
	if row == len(values) {
		return
	}
	partition.startMixed(row)
	sels := partition.sels
	sels[bucket] = append(sels[bucket], int32(row))
	for row++; row < len(values); row++ {
		bucket = 0
		if !nullVec.IsNull(uint64(row)) {
			bucket = plan2.GetRangeShuffleIndexUnsignedSlice(boundaries, uint64(values[row]))
		}
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

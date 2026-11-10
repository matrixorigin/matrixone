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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type dictionaryShuffleInteger interface {
	~int16 | ~int32 | ~int64 | ~uint16 | ~uint32 | ~uint64
}

// Only key lookup changes. Partition hash/range functions, destination buffers,
// pending-write ownership and queue capacity remain the existing shuffle's.
func dictionaryIntegerSels[T dictionaryShuffleInteger](ap *Shuffle, key *vector.Vector, signed bool) {
	view := vector.ResolveFixedReadView[T](key)
	sels := ap.ctr.sels
	count := uint64(ap.BucketNum)
	ranged := ap.ShuffleType == int32(plan.ShuffleType_Range)
	for row := 0; row < key.Length(); row++ {
		var bucket uint64
		if !view.AllNull && !view.Nulls.Contains(uint64(row)) {
			physical := int(view.Selection[row])
			if view.Constant {
				physical = 0
			}
			value := view.Values[physical]
			if !ranged {
				bucket = plan2.SimpleInt64HashToRange(uint64(value), count)
			} else if signed {
				if len(ap.ShuffleRangeInt64) > 0 {
					bucket = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(value))
				} else {
					bucket = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(value), count)
				}
			} else {
				if len(ap.ShuffleRangeUint64) > 0 {
					bucket = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(value))
				} else {
					bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(value), count)
				}
			}
		}
		sels[bucket] = append(sels[bucket], int32(row))
	}
}

func (shuffle *Shuffle) shuffleDictionaryKey(bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	key := bat.Vecs[shuffle.ShuffleColIdx]
	shuffle.clearSels()
	// typeswitch:partial Matches the planner-admitted legacy shuffle key domains.
	switch key.GetType().Oid {
	case types.T_int16:
		dictionaryIntegerSels[int16](shuffle, key, true)
	case types.T_int32:
		dictionaryIntegerSels[int32](shuffle, key, true)
	case types.T_int64:
		dictionaryIntegerSels[int64](shuffle, key, true)
	case types.T_uint16:
		dictionaryIntegerSels[uint16](shuffle, key, false)
	case types.T_uint32:
		dictionaryIntegerSels[uint32](shuffle, key, false)
	case types.T_uint64:
		dictionaryIntegerSels[uint64](shuffle, key, false)
	case types.T_decimal64:
		dictionaryIntegerSels[types.Decimal64](shuffle, key, false)
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob, types.T_datalink:
		view := vector.ResolveVarlenaReadView(key)
		for row := 0; row < key.Length(); row++ {
			var bucket uint64
			if !view.AllNull && !view.Nulls.Contains(uint64(row)) {
				physical := int(view.Selection[row])
				if view.Constant {
					physical = 0
				}
				value := &view.Values[physical]
				if shuffle.ShuffleType == int32(plan.ShuffleType_Hash) {
					bucket = stringHashToRangeForType(shuffle, *key.GetType(), value.GetByteSlice(view.Area), uint64(shuffle.BucketNum))
				} else {
					raw := plan2.VarlenaToUint64(value, view.Area)
					if len(shuffle.ShuffleRangeUint64) > 0 {
						bucket = plan2.GetRangeShuffleIndexUnsignedSlice(shuffle.ShuffleRangeUint64, raw)
					} else {
						bucket = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(shuffle.ShuffleColMin), uint64(shuffle.ShuffleColMax), raw, uint64(shuffle.BucketNum))
					}
				}
			}
			shuffle.ctr.sels[bucket] = append(shuffle.ctr.sels[bucket], int32(row))
		}
	case types.T_decimal128:
		view := vector.ResolveFixedReadView[types.Decimal128](key)
		for row := 0; row < key.Length(); row++ {
			var bucket uint64
			if !view.AllNull && !view.Nulls.Contains(uint64(row)) {
				physical := int(view.Selection[row])
				if view.Constant {
					physical = 0
				}
				value := view.Values[physical]
				bucket = plan2.SimpleInt64HashToRange(value.B0_63^value.B64_127, uint64(shuffle.BucketNum))
			}
			shuffle.ctr.sels[bucket] = append(shuffle.ctr.sels[bucket], int32(row))
		}
	case types.T_uuid:
		view := vector.ResolveFixedReadView[types.Uuid](key)
		for row := 0; row < key.Length(); row++ {
			var bucket uint64
			if !view.AllNull && !view.Nulls.Contains(uint64(row)) {
				physical := int(view.Selection[row])
				if view.Constant {
					physical = 0
				}
				bucket = plan2.SimpleInt64HashToRange(uuidHash(view.Values[physical]), uint64(shuffle.BucketNum))
			}
			shuffle.ctr.sels[bucket] = append(shuffle.ctr.sels[bucket], int32(row))
		}
	default:
		panic("unsupported shuffle type, wrong plan!")
	}
	for bucket, rows := range shuffle.ctr.sels {
		if len(rows) == bat.RowCount() && bat.RowCount() > 0 {
			return shuffle.routeSingleBucket(bat, int32(bucket), proc)
		}
	}
	// Existing pool buffers own the flat gather before the child can be reused.
	return nil, shuffle.enqueueBySels(bat, proc)
}

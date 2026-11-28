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

package hashmap

// fillNullableStr before its per-row loop was split by type, kept verbatim
// as the oracle for TestFillNullableStrMatchesLegacy. Delete this file once
// the new loop has been the only implementation for a release.

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func legacyFillNullableStr(
	itr *strHashmapIterator,
	vec *vector.Vector,
	n int,
	start int,
) {
	keys := itr.keys
	if isFlatRawStringHashKey(vec) {
		fillFlatRawStringNullableStr(itr, vec, n, start)
		return
	}
	// Nullable maps use a domain byte before every ordinary
	// value. Most batches contain no NULL rows, though. Keep
	// that common case on flat vector data so the hot path does not probe two
	// bitmaps and redispatch the vector representation for every row.
	if vec.GetType().IsFixedLen() && !vec.IsConstNull() && !vec.GetNulls().Any() {
		fillFlatFixedNullableStr(keys, vec, n, start)
		return
	}
	if vec.IsConstNull() {
		for i := 0; i < n; i++ {
			if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				itr.zValues[i] = 0
			}
		}
		return
	}
	float32Codec := keycodec.NewFloat32Codec(vec.GetType().Scale)
	for i := 0; i < n; i++ {
		row := start + i
		if vec.GetNulls().Contains(uint64(row)) {
			if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				itr.zValues[i] = 0
			}
			continue
		}
		keys[i] = append(keys[i], byte(0))
		valueRow := row
		if vec.IsConst() {
			valueRow = 0
		}
		switch vec.GetType().Oid {
		case types.T_float32:
			values := vector.MustFixedColNoTypeCheck[float32](vec)
			value := float32Codec.CanonicalBytes(values[valueRow])
			keys[i] = append(keys[i], value[:]...)
			continue
		case types.T_float64:
			values := vector.MustFixedColNoTypeCheck[float64](vec)
			value := keycodec.CanonicalFloat64Bytes(values[valueRow])
			keys[i] = append(keys[i], value[:]...)
			continue
		}
		if vec.GetType().IsFixedLen() {
			size := vec.GetType().TypeSize()
			data := vec.GetData()
			value := data[valueRow*size : (valueRow+1)*size]
			keys[i] = append(keys[i], value...)
			continue
		}
		value := canonicalVarlenaHashValueForType(*vec.GetType(), vec.GetBytesAt(valueRow))
		length := uint32(len(value))
		keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
		keys[i] = append(keys[i], value...)
	}
}

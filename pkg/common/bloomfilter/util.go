// Copyright 2021 - 2023 Matrix Origin
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

package bloomfilter

import (
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func fillStringGroupStr(keys [][]byte, vec *vector.Vector, n int, start int) {
	area := vec.GetArea()
	vs := vector.MustFixedCol[types.Varlena](vec)
	if !vec.GetNulls().Any() {
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(0))
			keys[i] = append(keys[i], vs[i+start].GetByteSlice(area)...)
		}
	} else {
		nsp := vec.GetNulls()
		for i := 0; i < n; i++ {
			hasNull := nsp.Contains(uint64(i + start))
			if hasNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				keys[i] = append(keys[i], byte(0))
				keys[i] = append(keys[i], vs[i+start].GetByteSlice(area)...)
			}
		}
	}
}

func fillGroupStr(keys [][]byte, vec *vector.Vector, n int, sz int, start int) {
	data := unsafe.Slice(vector.GetPtrAt[byte](vec, 0), (n+start)*sz)
	if !vec.GetNulls().Any() {
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(0))
			keys[i] = append(keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
		}
	} else {
		nsp := vec.GetNulls()
		for i := 0; i < n; i++ {
			isNull := nsp.Contains(uint64(i + start))
			if isNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				keys[i] = append(keys[i], byte(0))
				keys[i] = append(keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
			}
		}
	}
}

func encodeHashKeys(keys [][]byte, vec *vector.Vector, start, count int) {
	if vec.GetType().IsFixedLen() {
		fillGroupStr(keys, vec, count, vec.GetType().TypeSize(), start)
	} else {
		fillStringGroupStr(keys, vec, count, start)
	}

	for i := 0; i < count; i++ {
		if l := len(keys[i]); l < 16 {
			keys[i] = append(keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
}

func computeMemAndHashCount(rowCount int64, probability float64) (int64, int) {
	k := 1
	if rowCount < 10001 {
		k = 1
	} else if rowCount < 100001 {
		k = 1
	} else if rowCount < 1000001 {
		k = 1
	} else if rowCount < 10000001 {
		k = 2
	} else if rowCount < 100000001 {
		k = 3
	} else if rowCount < 1000000001 {
		k = 3
	} else if rowCount < 10000000001 {
		k = 3
	} else {
		panic("unsupport rowCount")
	}
	hashCount := k * 3
	m := -float64(hashCount) * float64(rowCount) / math.Log(1-math.Pow(probability, 1.0/float64(hashCount)))
	return int64(m), k
}

// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Hash(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(types.New(types.T_int64, 0, 0, 0))
	count := vecs[0].Length()
	keys := make([][]byte, hashmap.UnitLimit)
	states := make([][3]uint64, hashmap.UnitLimit)
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		encodeHashKeys(keys, vecs, i, n)
		hashtable.BytesBatchGenHashStates(&keys[0], &states[0], n)
		for j := 0; j < n; j++ {
			if err := vec.Append(int64(states[j][0]), false, proc.GetMheap()); err != nil {
				vec.Free(proc.GetMheap())
				return nil, err
			}
		}
	}
	return vec, nil
}

func encodeHashKeys(keys [][]byte, vecs []*vector.Vector, start, count int) {
	for _, vec := range vecs {
		if vec.GetType().IsFixedLen() {
			fillGroupStr(keys, vec, count, vec.GetType().TypeSize(), start)
		} else {
			fillStringGroupStr(keys, vec, count, start)
		}
	}
	for i := 0; i < count; i++ {
		if l := len(keys[i]); l < 16 {
			keys[i] = append(keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
}

func fillStringGroupStr(keys [][]byte, vec *vector.Vector, n int, start int) {
	area := vec.GetArea()
	vs := vector.MustTCols[types.Varlena](vec)
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
	data := unsafe.Slice((*byte)(vector.GetPtrAt(vec, 0)), (n+start)*sz)
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

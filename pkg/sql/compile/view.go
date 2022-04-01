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

package compile

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
)

const (
	UnitLimit = 256
)

func constructViews(bats []*batch.Batch, vars [][]string) {
	for i := range vars {
		constructView(bats[i], vars[i])
	}
}

func constructView(bat *batch.Batch, vars []string) {
	var rows uint64

	ht := &join.HashTable{
		StrHashMap: &hashtable.StringHashMap{},
	}
	ht.StrHashMap.Init()
	keys := make([][]byte, UnitLimit)
	values := make([]uint64, UnitLimit)
	strHashStates := make([][3]uint64, UnitLimit)
	batch.Reorder(bat, vars)
	vecs := make([]*vector.Vector, len(vars))
	{ // fill vectors
		for i := range vars {
			vecs[i] = batch.GetVector(bat, vars[i])
		}
	}
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
						}
					}
				}
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
						}
					}
				}
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
						}
					}
				}
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
						}
					}
				}
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], byte(0))
						keys[k] = append(keys[k], vs.Get(i+k)...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							keys[k] = append(keys[k], byte(1))
						} else {
							keys[k] = append(keys[k], byte(0))
							keys[k] = append(keys[k], vs.Get(i+k)...)
						}
					}
				}
			}
		}
		for k := int64(0); k < n; k++ {
			if l := len(keys[k]); l < 16 {
				keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
			}
		}
		ht.StrHashMap.InsertStringBatch(strHashStates, keys[:n], values)
		{
			for k, v := range values[:n] {
				if v > rows {
					ht.Sels = append(ht.Sels, make([]int64, 0, 8))
				}
				ai := int64(v) - 1
				ht.Sels[ai] = append(ht.Sels[ai], i+int64(k))
			}
		}
	}
	bat.Ht = ht
}

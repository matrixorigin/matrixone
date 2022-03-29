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

package variance

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

// VarRing is the ring structure to compute the Overall variance
type VarRing struct {
	// Typ is vector's value type
	Typ types.Type

	// attributes for computing the variance
	Data []byte    // store all the sums' bytes
	Sums []float64 // sums of each group, its memory address is same to Dates

	Values     [][]float64 // values of each group
	NullCounts []int64     // group to record number of the null value
}

func NewVarRing(typ types.Type) *VarRing {
	return &VarRing{Typ: typ}
}

func (v *VarRing) Free(m *mheap.Mheap) {
	if v.Data != nil {
		mheap.Free(m, v.Data)
		v.Data = nil
		v.Sums = nil
		v.NullCounts = nil
		v.Values = nil
	}
	return
}

// Count return group number of this ring.
func (v *VarRing) Count() int {
	return len(v.Sums)
}

// Size return how much memory space allocated in memory pool by this ring.
// TODO: it's not exactly now
func (v *VarRing) Size() int {
	size := cap(v.Data)
	for _, value := range v.Values {
		size += cap(value) * 8
	}
	return size
}

// Dup will make a new VarRing with the same type.
func (v *VarRing) Dup() ring.Ring {
	return NewVarRing(v.Typ)
}

func (v *VarRing) Type() types.Type {
	return v.Typ
}

func (v *VarRing) SetLength(n int) {
	// first n group will be kept.
	v.Sums = v.Sums[:n]
	v.Values = v.Values[:n]
	v.NullCounts = v.NullCounts[:n]
}

func (v *VarRing) Shrink(selectIndexes []int64) {
	// move the data according to the list of index numbers.
	for i, index := range selectIndexes {
		v.Sums[i] = v.Sums[index]
		v.Values[i] = v.Values[index]
		v.NullCounts[i] = v.NullCounts[index]
	}
	v.Sums = v.Sums[:len(selectIndexes)]
	v.Values = v.Values[:len(selectIndexes)]
	v.NullCounts = v.NullCounts[:len(selectIndexes)]
}

func (v *VarRing) Grow(m *mheap.Mheap) error {
	n := len(v.Sums)

	if n == 0 {
		// The first time memory is allocated,
		// more space is allocated to avoid the performance loss caused by multiple allocations.
		data, err := mheap.Alloc(m, 64)
		if err != nil {
			return err
		}
		v.Data = data
		v.Sums = encoding.DecodeFloat64Slice(data)

		v.NullCounts = make([]int64, 0, 8)
		v.Values = make([][]float64, 0, 8)
	} else if n+1 >= cap(v.Sums) {
		v.Data = v.Data[:n*8]
		data, err := mheap.Grow(m, v.Data, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, v.Data)
		v.Data = data
		v.Sums = encoding.DecodeFloat64Slice(data)
	}

	v.Sums = v.Sums[:n+1]
	v.Sums[n] = 0
	v.NullCounts = append(v.NullCounts, 0)
	v.Values = append(v.Values, make([]float64, 0, 16))
	return nil
}

func (v *VarRing) Grows(size int, m *mheap.Mheap) error {
	n := len(v.Sums)

	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		v.Data = data
		v.Sums = encoding.DecodeFloat64Slice(data)

		v.Values = make([][]float64, 0, size)
		v.NullCounts = make([]int64, 0, size)

	} else if n+size >= cap(v.Sums) {
		v.Data = v.Data[:n*8]
		data, err := mheap.Grow(m, v.Data, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, v.Data)
		v.Data = data
		v.Sums = encoding.DecodeFloat64Slice(data)
	}

	v.Sums = v.Sums[:n+size]
	for i := 0; i < size; i++ {
		v.NullCounts = append(v.NullCounts, 0)
		v.Values = append(v.Values, make([]float64, 0, 16))
	}
	return nil
}

func (v *VarRing) Fill(i, j int64, z int64, vec *vector.Vector) {
	var value float64 = 0

	switch vec.Typ.Oid {
	case types.T_int8:
		value = float64(vec.Col.([]int8)[j])
	case types.T_int16:
		value = float64(vec.Col.([]int16)[j])
	case types.T_int32:
		value = float64(vec.Col.([]int32)[j])
	case types.T_int64:
		value = float64(vec.Col.([]int64)[j])
	case types.T_uint8:
		value = float64(vec.Col.([]uint8)[j])
	case types.T_uint16:
		value = float64(vec.Col.([]uint16)[j])
	case types.T_uint32:
		value = float64(vec.Col.([]uint32)[j])
	case types.T_uint64:
		value = float64(vec.Col.([]uint64)[j])
	case types.T_float32:
		value = float64(vec.Col.([]float32)[j])
	case types.T_float64:
		value = vec.Col.([]float64)[j]
	}
	for k := z; k > 0; k-- {
		v.Values[i] = append(v.Values[i], value)
	}

	v.Sums[i] += value * float64(z)

	if nulls.Contains(vec.Nsp, uint64(z)) {
		v.NullCounts[i] += z
	}
}

func (v *VarRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		for i := range os {
			v.Sums[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], float64(vs[int64(i)+start]))
			}
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for i := range os {
			v.Sums[vps[i]-1] += vs[int64(i)+start] * float64(zs[int64(i)+start])

			for k := zs[int64(i)+start]; k > 0; k-- {
				v.Values[vps[i]-1] = append(v.Values[vps[i]-1], vs[int64(i)+start])
			}
		}
	}
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				v.NullCounts[vps[i]-1] += zs[int64(i)+start]
			}
		}
	}
}

func (v *VarRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch v.Typ.Oid {
	case types.T_int8:
		values := vec.Col.([]int8)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_int16:
		values := vec.Col.([]int16)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_int32:
		values := vec.Col.([]int32)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_int64:
		values := vec.Col.([]int64)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_uint8:
		values := vec.Col.([]uint8)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_uint16:
		values := vec.Col.([]uint16)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_uint32:
		values := vec.Col.([]uint32)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_uint64:
		values := vec.Col.([]uint64)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_float32:
		values := vec.Col.([]float32)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], float64(value))
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += float64(value) * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], float64(value))
				}
			}
		}
	case types.T_float64:
		values := vec.Col.([]float64)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					for k := zs[j]; k > 0; k-- {
						v.Values[i] = append(v.Values[i], value)
					}
				}
			}
		} else {
			for j, value := range values {
				v.Sums[i] += value * float64(zs[j])
				for k := zs[j]; k > 0; k-- {
					v.Values[i] = append(v.Values[i], value)
				}
			}
		}
	}
}

// Add merge ring2 group Y into ring1's group X
func (v *VarRing) Add(a interface{}, x, y int64) {
	v2 := a.(*VarRing)
	v.Sums[x] += v2.Sums[y]
	v.NullCounts[x] += v2.NullCounts[y]
	v.Values[x] = append(v.Values[x], v2.Values[y]...)
}

func (v *VarRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	v2 := a.(*VarRing)
	for i := range os {
		v.Sums[vps[os[i]]-1] += v2.Sums[start+int64(i)]
		v.NullCounts[vps[os[i]]-1] += v2.NullCounts[start+int64(i)]
		v.Values[vps[os[i]]-1] = append(v.Values[vps[os[i]]-1], v2.Values[start+int64(i)]...)
	}
}

func (v *VarRing) Mul(a interface{}, x, y, z int64) {
	v2 := a.(*VarRing)
	{
		v.Sums[x] += v2.Sums[y] * float64(z)
		v.NullCounts[x] += v2.NullCounts[y] * z
		for k := z; k > 0; k-- {
			v.Values[x] = append(v.Values[x], v2.Values[y]...)
		}
	}
}

func (v *VarRing) Eval(zs []int64) *vector.Vector {
	defer func() {
		v.Values = nil
		v.Sums = nil
		v.NullCounts = nil
		v.Data = nil
	}()

	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if n := z - v.NullCounts[i]; n == 0 {
			nulls.Add(nsp, uint64(i))
		} else {
			v.Sums[i] /= float64(n)

			var variance float64 = 0
			avg := v.Sums[i]
			for _, value := range v.Values[i] {
				variance += math.Pow(value-avg, 2.0) / float64(n)
			}
			v.Sums[i] = variance
		}
	}

	return &vector.Vector{
		Nsp:  nsp,
		Data: v.Data,
		Col:  v.Sums,
		Or:   false,
		Typ:  types.Type{Oid: types.T_float64, Size: 8},
	}
}

// useless functions for VarRing and just implement it
func (v *VarRing) String() string {
	return "var-ring"
}

func (v *VarRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

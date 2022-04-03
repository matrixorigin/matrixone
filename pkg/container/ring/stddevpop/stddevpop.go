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

package stddevpop

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type StdDevPopRing struct {
	// Typ is vector's value type
	Typ types.Type
	// attributes for computing the variance
	Data  []byte
	SumX  []float64 // sum of x, its memory address is same to Data, because we will use it to store result finally.
	SumX2 []float64 // sum of x^2

	NullCounts []int64 // group to record number of the null value
}

func NewStdDevPopRing(typ types.Type) *StdDevPopRing {
	return &StdDevPopRing{Typ: typ}
}
func (v *StdDevPopRing) Count() int {
	return len(v.SumX)
}

func (v *StdDevPopRing) Size() int {
	return cap(v.Data)
}

func (v *StdDevPopRing) Dup() ring.Ring {
	return NewStdDevPopRing(v.Typ)
}

func (v *StdDevPopRing) Type() types.Type {
	return v.Typ
}

func (v *StdDevPopRing) String() string {
	return "stddev_pop ring"
}

func (v *StdDevPopRing) Free(m *mheap.Mheap) {
	if v.Data != nil {
		mheap.Free(m, v.Data)
		v.Data = nil
		v.SumX = nil
		v.SumX2 = nil
		v.NullCounts = nil
	}
	return
}

func (v *StdDevPopRing) Grow(m *mheap.Mheap) error {
	n := len(v.SumX)

	if n == 0 {
		// The first time memory is allocated,
		// more space is required to avoid the performance loss caused by multiple allocations.
		data, err := mheap.Alloc(m, 64)
		if err != nil {
			return err
		}
		v.Data = data
		v.SumX = encoding.DecodeFloat64Slice(data)

		v.NullCounts = make([]int64, 0, 8)
		v.SumX2 = make([]float64, 0, 8)
	} else if n+1 >= cap(v.SumX) {
		v.Data = v.Data[:n*8]
		data, err := mheap.Grow(m, v.Data, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, v.Data)
		v.Data = data
		v.SumX2 = encoding.DecodeFloat64Slice(data)
	}

	v.SumX = v.SumX[:n+1]
	v.SumX[n] = 0
	v.SumX2 = append(v.SumX2, 0)
	v.NullCounts = append(v.NullCounts, 0)
	return nil
}

func (v *StdDevPopRing) Grows(N int, m *mheap.Mheap) error {
	n := len(v.SumX)

	if n == 0 {
		data, err := mheap.Alloc(m, int64(N*8))
		if err != nil {
			return err
		}
		v.Data = data
		v.SumX = encoding.DecodeFloat64Slice(data)

		v.SumX2 = make([]float64, 0, N)
		v.NullCounts = make([]int64, 0, N)

	} else if n+N >= cap(v.SumX) {
		v.Data = v.Data[:n*8]
		data, err := mheap.Grow(m, v.Data, int64(n+N)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, v.Data)
		v.Data = data
		v.SumX = encoding.DecodeFloat64Slice(data)
	}

	v.SumX = v.SumX[:n+N]
	for i := 0; i < N; i++ {
		v.NullCounts = append(v.NullCounts, 0)
		v.SumX2 = append(v.SumX2, 0)
	}
	return nil
}

func (v *StdDevPopRing) SetLength(n int) {
	v.SumX = v.SumX[:n]
	v.SumX2 = v.SumX2[:n]
	v.NullCounts = v.NullCounts[:n]
}

func (v *StdDevPopRing) Shrink(selectIndexes []int64) {
	for i, index := range selectIndexes {
		v.SumX[i] = v.SumX[index]
		v.SumX2[i] = v.SumX2[index]
		v.NullCounts[i] = v.NullCounts[index]
	}
	v.SumX = v.SumX[:len(selectIndexes)]
	v.SumX2 = v.SumX2[:len(selectIndexes)]
	v.NullCounts = v.NullCounts[:len(selectIndexes)]
}

func (v *StdDevPopRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (v *StdDevPopRing) Eval(zs []int64) *vector.Vector {
	defer func() {
		v.SumX = nil
		v.SumX2 = nil
		v.NullCounts = nil
		v.Data = nil
	}()

	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if n := z - v.NullCounts[i]; n == 0 {
			nulls.Add(nsp, uint64(i))
		} else {
			v.SumX[i] /= float64(n)  // compute E(x)
			v.SumX2[i] /= float64(n) // compute E(x^2)

			variance := v.SumX2[i] - math.Pow(v.SumX[i], 2)

			v.SumX[i] = math.Sqrt(variance) // using v.SumX to record the result and return.
		}
	}

	return &vector.Vector{
		Nsp:  nsp,
		Data: v.Data,
		Col:  v.SumX,
		Or:   false,
		Typ:  types.Type{Oid: types.T_float64, Size: 8},
	}

}

func (v *StdDevPopRing) Fill(i, j int64, z int64, vec *vector.Vector) {
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

	v.SumX[i] += value * float64(z)
	v.SumX2[i] += math.Pow(value, 2) * float64(z)

	if nulls.Contains(vec.Nsp, uint64(z)) {
		v.NullCounts[i] += z
	}
}

func (v *StdDevPopRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch v.Typ.Oid {
	case types.T_int8:
		values := vec.Col.([]int8)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_int16:
		values := vec.Col.([]int16)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_int32:
		values := vec.Col.([]int32)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_int64:
		values := vec.Col.([]int64)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_uint8:
		values := vec.Col.([]uint8)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_uint16:
		values := vec.Col.([]uint16)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_uint32:
		values := vec.Col.([]uint32)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_uint64:
		values := vec.Col.([]uint64)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_float32:
		values := vec.Col.([]float32)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += float64(value) * float64(zs[j])
					v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += float64(value) * float64(zs[j])
				v.SumX2[i] += math.Pow(float64(value), 2) * float64(zs[j])
			}
		}
	case types.T_float64:
		values := vec.Col.([]float64)
		if nulls.Any(vec.Nsp) {
			for j, value := range values {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					v.NullCounts[i] += zs[j]
				} else {
					v.SumX[i] += value * float64(zs[j])
					v.SumX2[i] += math.Pow(value, 2) * float64(zs[j])
				}
			}
		} else {
			for j, value := range values {
				v.SumX[i] += value * float64(zs[j])
				v.SumX2[i] += math.Pow(value, 2) * float64(zs[j])
			}
		}
	}
}

func (v *StdDevPopRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		for i := range os {
			v.SumX[vps[i]-1] += float64(vs[int64(i)+start]) * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(float64(vs[int64(i)+start]), 2) * float64(zs[int64(i)+start])
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for i := range os {
			v.SumX[vps[i]-1] += vs[int64(i)+start] * float64(zs[int64(i)+start])
			v.SumX2[vps[i]-1] += math.Pow(vs[int64(i)+start], 2) * float64(zs[int64(i)+start])
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

func (v *StdDevPopRing) Add(a interface{}, x, y int64) {
	v2 := a.(*StdDevPopRing)
	v.SumX[x] += v2.SumX[y]
	v.SumX2[x] += v2.SumX2[y]
	v.NullCounts[x] += v2.NullCounts[y]
}

func (v *StdDevPopRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	v2 := a.(*StdDevPopRing)
	for i := range os {
		v.SumX[vps[i]-1] += v2.SumX[start+int64(i)]
		v.SumX2[vps[i]-1] += v2.SumX2[start+int64(i)]
		v.NullCounts[vps[i]-1] += v2.NullCounts[start+int64(i)]
	}
}

func (v *StdDevPopRing) Mul(a interface{}, x, y, z int64) {
	v2 := a.(*StdDevPopRing)
	{
		v.SumX[x] += v2.SumX[y] * float64(z)
		v.SumX2[x] += v2.SumX2[y] * float64(z)
		v.NullCounts[x] += v2.NullCounts[y] * z
	}

}

package bit_or

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewFloat(typ types.Type) *FloatRing {
	return &FloatRing{Typ: typ}
}

func (r *FloatRing) String() string {
	return fmt.Sprintf("%v-%v", r.Values, r.NullCounts)
}

func (r *FloatRing) Free(m *mheap.Mheap) {
	if r.Data != nil {
		mheap.Free(m, r.Data)
		r.Data = nil
		r.Values = nil
		r.NullCounts = nil
	}
}

func (r *FloatRing) Count() int {
	return len(r.Values)
}

func (r *FloatRing) Size() int {
	return cap(r.Data)
}

func (r *FloatRing) Dup() ring.Ring {
	return &FloatRing{
		Typ: r.Typ,
	}
}

func (r *FloatRing) Type() types.Type {
	return r.Typ
}

func (r *FloatRing) SetLength(n int) {
	r.Values = r.Values[:n]
	r.NullCounts = r.NullCounts[:n]
}

func (r *FloatRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Values[i] = r.Values[sel]
		r.NullCounts[i] = r.NullCounts[sel]
	}
	r.Values = r.Values[:len(sels)]
	r.NullCounts = r.NullCounts[:len(sels)]
}

func (r *FloatRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *FloatRing) Grow(m *mheap.Mheap) error {
	n := len(r.Values)
	if n == 0 {
		data, err := mheap.Alloc(m, 64)
		if err != nil {
			return err
		}
		r.Data = data
		r.NullCounts = make([]int64, 0, 8)
		r.Values = encoding.DecodeUint64Slice(data)
	} else if n+1 >= cap(r.Values) {
		r.Data = r.Data[:n*8]
		data, err := mheap.Grow(m, r.Data, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.Values = encoding.DecodeUint64Slice(data)
	}
	r.Values = r.Values[:n+1]
	r.Values[n] = 0
	r.NullCounts = append(r.NullCounts, 0)
	return nil
}

func (r *FloatRing) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Values)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Data = data
		r.NullCounts = make([]int64, 0, size)
		r.Values = encoding.DecodeUint64Slice(data)
	} else if n+size >= cap(r.Values) {
		r.Data = r.Data[:n*8]
		data, err := mheap.Grow(m, r.Data, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.Values = encoding.DecodeUint64Slice(data)
	}
	r.Values = r.Values[:n+size]
	for i := 0; i < size; i++ {
		r.NullCounts = append(r.NullCounts, 0)
	}
	return nil
}

func (r *FloatRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		r.Values[i] |= uint64(vec.Col.([]float32)[sel]) 
	case types.T_float64:
		r.Values[i] |= uint64(vec.Col.([]float64)[sel])
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.NullCounts[i] += z
	}
}

func (r *FloatRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		vs := vec.Col.([]float32)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vs[int64(i)+start])
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vs[int64(i)+start]) 
		}
	}
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.NullCounts[vps[i]-1] += zs[int64(i)+start]
			}
		}
	}
}

func (r *FloatRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		vs := vec.Col.([]float32)
		for _, v := range vs {
			r.Values[i] |= uint64(v)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for _, v := range vs {
			r.Values[i] |= uint64(v )
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	}
}

func (r *FloatRing) Add(a interface{}, x, y int64) {
	ar := a.(*FloatRing)
	r.Values[x] |= ar.Values[y]
	r.NullCounts[x] += ar.NullCounts[y]
}

func (r *FloatRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*FloatRing)
	for i := range os {
		r.Values[vps[i]-1] |= ar.Values[int64(i)+start]
		r.NullCounts[vps[i]-1] += ar.NullCounts[int64(i)+start]
	}
}

// r[x] += a[y] * z
func (r *FloatRing) Mul(a interface{}, x, y, z int64) {
	ar := a.(*FloatRing)
	r.NullCounts[x] += ar.NullCounts[y] * z
	r.Values[x] |= ar.Values[y]
}

func (r *FloatRing) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Data = nil
		r.Values = nil
		r.NullCounts = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if z-r.NullCounts[i] == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Data,
		Col:  r.Values,
		Or:   false,
		Typ:  types.Type{Oid: types.T_uint64, Size: 8},
	}
}

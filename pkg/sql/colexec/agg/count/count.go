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

package count

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func ReturnType(_ types.Type) types.Type {
	return types.New(types.T_int64, 0, 0, 0)
}

func New(in, typ types.Type) agg.Agg[*IntAgg] {
	return &IntAgg{
		In:  in,
		Typ: typ,
	}
}

func (r *IntAgg) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *IntAgg) Free(m *mheap.Mheap) {
	if r.Da != nil {
		m.Free(r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *IntAgg) Dup() agg.Agg[any] {
	return &IntAgg{
		In:  r.In,
		Typ: r.Typ,
	}
}

func (r *IntAgg) Type() types.Type {
	return r.Typ
}

func (r *IntAgg) InputType() types.Type {
	return r.In
}

func (r *IntAgg) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := m.Alloc(int64(size * 8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
		r.Vs = encoding.DecodeSlice[types.Int64](r.Da, 8)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := m.Grow(r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		m.Free(r.Da)
		r.Da = data
		r.Vs = encoding.DecodeSlice[types.Int64](r.Da, 8)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
	}
	return nil
}

func (r *IntAgg) Fill(i int64, sel, z int64, vec vector.AnyVector) {
	nsp := vec.Nulls()
	if nsp.Contains(uint64(sel)) {
		r.Ns[i] += z
	} else {
		r.Vs[i] += types.Int64(z)
	}
}

func (r *IntAgg) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec vector.AnyVector) {
	nsp := vec.Nulls()
	if nsp.IsEmpty() {
		for i := range os {
			r.Vs[vps[i]-1] += types.Int64(zs[int64(i)+start])
		}
		return
	}
	for i := range os {
		if nsp.Contains(uint64(i) + uint64(start)) {
			r.Ns[vps[i]-1] += zs[int64(i)+start]
		} else {
			r.Vs[vps[i]-1] += types.Int64(zs[int64(i)+start])
		}
	}
}

func (r *IntAgg) BulkFill(i int64, zs []int64, vec vector.AnyVector) {
	nsp := vec.Nulls()
	if nsp.IsEmpty() {
		for _, z := range zs {
			r.Vs[i] += types.Int64(z)
		}
		return
	}
	for j, z := range zs {
		if nsp.Contains(uint64(j)) {
			r.Ns[i] += z
		} else {
			r.Vs[i] += types.Int64(z)
		}
	}
}

// r[x] += a[y]
func (r *IntAgg) Merge(a agg.Agg[any], x, y int64) {
	ar := a.(*IntAgg)
	r.Vs[x] += ar.Vs[y]
	r.Ns[x] += ar.Ns[y]
}

func (r *IntAgg) BatchMerge(a agg.Agg[any], start int64, os []uint8, vps []uint64) {
	ar := a.(*IntAgg)
	for i := range os {
		r.Vs[vps[i]-1] += ar.Vs[int64(i)+start]
		r.Ns[vps[i]-1] += ar.Ns[int64(i)+start]
	}
}

func (r *IntAgg) Eval(zs []int64, _ *mheap.Mheap) vector.AnyVector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}()
	nsp := bitmap.New(len(zs))
	return &vector.Vector[types.Int64]{
		Nsp:  nsp,
		Data: r.Da,
		Col:  r.Vs,
		Typ:  r.Typ,
	}
}

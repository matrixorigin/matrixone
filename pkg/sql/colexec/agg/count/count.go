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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func ReturnType(_ types.Type) types.Type {
	return types.New(types.T_int64, 0, 0, 0)
}

func New(ityp, otyp types.Type) agg.Agg[*Count] {
	return &Count{
		Ityp: ityp,
		Otyp: otyp,
	}
}

func (r *Count) String() string {
	return fmt.Sprintf("%v", r.Vs)
}

func (r *Count) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
	}
}

func (r *Count) Dup() agg.Agg[any] {
	return &Count{
		Ityp: r.Ityp,
		Otyp: r.Otyp,
	}
}

func (r *Count) Type() types.Type {
	return r.Otyp
}

func (r *Count) InputType() types.Type {
	return r.Ityp
}

func (r *Count) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Vs = encoding.DecodeSlice[int64](r.Da, 8)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeSlice[int64](r.Da, 8)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	return nil
}

func (r *Count) Fill(i int64, sel, z int64, vec *vector.Vector) {
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Vs[i] += z
	}
}

func (r *Count) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if !nulls.Contains(vec.Nsp, uint64(i)+uint64(start)) {
				r.Vs[vps[i]-1] += zs[int64(i)+start]
			}
		}
		return
	}
	for i := range os {
		r.Vs[vps[i]-1] += zs[int64(i)+start]
	}
}

func (r *Count) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		for j, z := range zs {
			if !nulls.Contains(vec.Nsp, uint64(j)) {
				r.Vs[i] += z
			}
		}
	}
	for _, z := range zs {
		r.Vs[i] += z
	}
}

// r[x] += a[y]
func (r *Count) Merge(a agg.Agg[any], x, y int64) {
	ar := a.(*Count)
	r.Vs[x] += ar.Vs[y]
}

func (r *Count) BatchMerge(a agg.Agg[any], start int64, os []uint8, vps []uint64) {
	ar := a.(*Count)
	for i := range os {
		r.Vs[vps[i]-1] += ar.Vs[int64(i)+start]
	}
}

func (r *Count) Eval(zs []int64, _ *mheap.Mheap) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
	}()
	return &vector.Vector{
		Typ:  r.Otyp,
		Col:  r.Vs,
		Data: r.Da,
		Nsp:  &nulls.Nulls{},
	}
}

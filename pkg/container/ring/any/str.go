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

package any

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewStr(typ types.Type) *StrRing {
	return &StrRing{
		Typ: typ,
	}
}

func (r *StrRing) String() string {
	return fmt.Sprintf("%v-%v", r.Values, r.NullCounts)
}

func (r *StrRing) Free(m *mheap.Mheap) {
	r.Values = nil
	r.NullCounts = nil
}

func (r *StrRing) Count() int {
	return len(r.Values)
}

func (r *StrRing) Size() int {
	return 0
}

func (r *StrRing) Dup() ring.Ring {
	return &StrRing{
		Typ: r.Typ,
	}
}

func (r *StrRing) Type() types.Type {
	return r.Typ
}

func (r *StrRing) SetLength(n int) {
	r.Values = r.Values[:n]
	r.NullCounts = r.NullCounts[:n]
	r.Empty = r.Empty[:n]
}

func (r *StrRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Values[i] = r.Values[sel]
		r.NullCounts[i] = r.NullCounts[sel]
		r.Empty[i] = r.Empty[sel]
	}
	r.Values = r.Values[:len(sels)]
	r.NullCounts = r.NullCounts[:len(sels)]
	r.Empty = r.Empty[:len(sels)]
}

func (r *StrRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *StrRing) Grow(m *mheap.Mheap) error {
	if r.Mp == nil {
		r.Mp = m
	}
	if len(r.Values) == 0 {
		r.Empty = make([]bool, 0, 8)
		r.NullCounts = make([]int64, 0, 8)
		r.Values = make([][]byte, 0, 8)
	}
	r.NullCounts = append(r.NullCounts, 0)
	r.Empty = append(r.Empty, true)
	r.Values = append(r.Values, make([]byte, 0, 4))
	return nil
}

func (r *StrRing) Grows(size int, m *mheap.Mheap) error {
	if r.Mp == nil {
		r.Mp = m
	}
	if len(r.Values) == 0 {
		r.Empty = make([]bool, 0, size)
		r.NullCounts = make([]int64, 0, size)
		r.Values = make([][]byte, 0, size)
	}
	for i := 0; i < size; i++ {
		r.NullCounts = append(r.NullCounts, 0)
		r.Empty = append(r.Empty, true)
		r.Values = append(r.Values, make([]byte, 0, 4))
	}
	return nil
}

func (r *StrRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	v := vec.Col.(*types.Bytes).Get(sel)
	if r.Empty[i] {
		r.Empty[i] = false
		r.Values[i] = append(r.Values[i][:0], v...)
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.NullCounts[i] += z
	}
}

func (r *StrRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.(*types.Bytes)
	for i := range os {
		j := vps[i] - 1
		v := vs.Get(int64(i) + start)
		if r.Empty[j] {
			r.Empty[j] = false
			r.Values[j] = append(r.Values[j][:0], v...)
		}
		break
	}
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.NullCounts[vps[i]-1] += zs[int64(i)+start]
			}
		}
	}
}

func (r *StrRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.(*types.Bytes)
	for j := range zs {
		v := vs.Get(int64(j))
		if r.Empty[i] {
			r.Empty[i] = false
			r.Values[i] = append(r.Values[i][:0], v...)
		}
		break
	}
	if nulls.Any(vec.Nsp) {
		for j := range zs {
			if nulls.Contains(vec.Nsp, uint64(j)) {
				r.NullCounts[i] += zs[j]
			}
		}
	}
}

func (r *StrRing) Add(a interface{}, x, y int64) {
	ar := a.(*StrRing)
	if r.Empty[x] {
		r.Empty[x] = false
		r.Values[x] = ar.Values[y]
	}
	r.NullCounts[x] += ar.NullCounts[y]
}

func (r *StrRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*StrRing)
	for i := range os {
		j := vps[i] - 1
		if r.Empty[j] {
			r.Empty[j] = false
			r.Values[j] = ar.Values[int64(i)+start]
		}
		r.NullCounts[j] += ar.NullCounts[int64(i)+start]
	}
}

func (r *StrRing) Mul(a interface{}, x, y, z int64) {
	ar := a.(*StrRing)
	if r.Empty[x] {
		r.Empty[x] = false
		r.Values[x] = ar.Values[y]
	}
	r.NullCounts[x] += ar.NullCounts[y] * z
}

func (r *StrRing) Eval(zs []int64) *vector.Vector {
	var data []byte
	var os, ns []uint32

	defer func() {
		r.Values = nil
		r.NullCounts = nil
	}()
	{
		o := uint32(0)
		for _, v := range r.Values {
			os = append(os, o)
			data = append(data, v...)
			o += uint32(len(v))
			ns = append(ns, uint32(len(v)))
		}
		if err := r.Mp.Gm.Alloc(int64(cap(data))); err != nil {
			return nil
		}
	}
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if z-r.NullCounts[i] == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}
	return &vector.Vector{
		Nsp: nsp,
		Or:  false,
		Typ: r.Typ,
		Col: &types.Bytes{
			Offsets: os,
			Lengths: ns,
			Data:    data,
		},
	}
}

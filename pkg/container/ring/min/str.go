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

package min

import (
	"bytes"
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
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *StrRing) Free(m *mheap.Mheap) {
	r.Vs = nil
	r.Ns = nil
}

func (r *StrRing) Count() int {
	return len(r.Vs)
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
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
	r.Es = r.Es[:n]
}

func (r *StrRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
		r.Es[i] = r.Es[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
	r.Es = r.Es[:len(sels)]
}

func (r *StrRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *StrRing) Grow(m *mheap.Mheap) error {
	if r.Mp == nil {
		r.Mp = m
	}
	if len(r.Vs) == 0 {
		r.Es = make([]bool, 0, 8)
		r.Ns = make([]int64, 0, 8)
		r.Vs = make([][]byte, 0, 8)
	}
	r.Ns = append(r.Ns, 0)
	r.Es = append(r.Es, true)
	r.Vs = append(r.Vs, make([]byte, 0, 4))
	return nil
}

func (r *StrRing) Fill(i int64, sel, _ int64, vec *vector.Vector) {
	if v := vec.Col.(*types.Bytes).Get(sel); r.Es[i] || bytes.Compare(v, r.Vs[i]) < 0 {
		r.Es[i] = false
		r.Vs[i] = append(r.Vs[i][:0], v...)
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i]++
	}
}

func (r *StrRing) BulkFill(i int64, _ []int64, vec *vector.Vector) {
	vs := vec.Col.(*types.Bytes)
	for j, o := range vs.Offsets {
		v := vs.Data[o : o+vs.Lengths[j]]
		if r.Es[i] || bytes.Compare(v, r.Vs[i]) < 0 {
			r.Es[i] = false
			r.Vs[i] = append(r.Vs[i][:0], v...)
		}
	}
	r.Ns[i] += int64(nulls.Length(vec.Nsp))
}

func (r *StrRing) Add(a interface{}, x, y int64) {
	ar := a.(*StrRing)
	if r.Es[x] || bytes.Compare(ar.Vs[y], r.Vs[x]) < 0 {
		r.Es[x] = false
		r.Vs[x] = append(r.Vs[x][:0], ar.Vs[y]...)
	}
	r.Ns[x] += ar.Ns[y]
}

func (r *StrRing) Mul(_, _ int64) {
}

func (r *StrRing) Eval(zs []int64) *vector.Vector {
	var data []byte
	var os, ns []uint32

	defer func() {
		r.Vs = nil
		r.Ns = nil
	}()
	{
		o := uint32(0)
		for _, v := range r.Vs {
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
		if z-r.Ns[i] == 0 {
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

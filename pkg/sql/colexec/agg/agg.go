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

package agg

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewUnaryAgg[T1, T2 any](priv any, isCount bool, ityp, otyp types.Type, grows func(int),
	eval func([]T2) []T2, merge func(int64, int64, T2, T2, bool, bool, any) (T2, bool),
	fill func(int64, T1, T2, int64, bool, bool) (T2, bool)) Agg[*UnaryAgg[T1, T2]] {
	return &UnaryAgg[T1, T2]{
		priv:    priv,
		otyp:    otyp,
		eval:    eval,
		fill:    fill,
		merge:   merge,
		grows:   grows,
		isCount: isCount,
		ityps:   []types.Type{ityp},
	}
}

func (a *UnaryAgg[T1, T2]) String() string {
	return fmt.Sprintf("%v", a.vs)
}

func (a *UnaryAgg[T1, T2]) Free(m *mheap.Mheap) {
	if a.da != nil {
		m.Free(a.da)
		a.da = nil
		a.vs = nil
	}
}

func (a *UnaryAgg[T1, T2]) Dup() Agg[any] {
	return &UnaryAgg[T1, T2]{
		otyp:  a.otyp,
		ityps: a.ityps,
		fill:  a.fill,
		merge: a.merge,
		grows: a.grows,
		eval:  a.eval,
	}
}

func (a *UnaryAgg[T1, T2]) OutputType() types.Type {
	return a.otyp
}

func (a *UnaryAgg[T1, T2]) InputTypes() []types.Type {
	return a.ityps
}

func (a *UnaryAgg[T1, T2]) Grows(size int, m *mheap.Mheap) error {
	if a.otyp.IsString() {
		if len(a.vs) == 0 {
			a.es = make([]bool, 0, size)
			a.vs = make([]T2, 0, size)
			a.vs = a.vs[:size]
			for i := 0; i < size; i++ {
				a.es = append(a.es, true)
			}
		} else {
			var v T2
			a.es = append(a.es, true)
			a.vs = append(a.vs, v)
		}
		a.grows(size)
		return nil
	}
	sz := a.otyp.TypeSize()
	n := len(a.vs)
	if n == 0 {
		data, err := m.Alloc(int64(size * sz))
		if err != nil {
			return err
		}
		a.da = data
		a.es = make([]bool, 0, size)
		a.vs = encoding.DecodeSlice[T2](a.da, sz)
	} else if n+size >= cap(a.vs) {
		a.da = a.da[:n*8]
		data, err := m.Grow(a.da, int64(n+size)*int64(sz))
		if err != nil {
			return err
		}
		m.Free(a.da)
		a.da = data
		a.vs = encoding.DecodeSlice[T2](a.da, sz)
	}
	a.vs = a.vs[:n+size]
	a.da = a.da[:(n+size)*sz]
	for i := 0; i < size; i++ {
		a.es = append(a.es, true)
	}
	a.grows(size)
	return nil
}

func (a *UnaryAgg[T1, T2]) Fill(i int64, sel, z int64, vecs []*vector.Vector) {
	vec := vecs[0]
	hasNull := vec.GetNulls().Contains(uint64(sel))
	if vec.Typ.IsString() {
		a.vs[i], a.es[i] = a.fill(i, (any)(vec.GetString(sel)).(T1), a.vs[i], z, a.es[i], hasNull)
	} else {
		a.vs[i], a.es[i] = a.fill(i, vector.GetColumn[T1](vec)[sel], a.vs[i], z, a.es[i], hasNull)
	}
}

func (a *UnaryAgg[T1, T2]) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vecs []*vector.Vector) {
	vec := vecs[0]
	if vec.GetType().IsString() {
		for i := range os {
			hasNull := vec.GetNulls().Contains(uint64(i) + uint64(start))
			j := vps[i] - 1
			a.vs[j], a.es[j] = a.fill(int64(j), (any)(vec.GetString(int64(i)+start)).(T1), a.vs[j], zs[int64(i)+start], a.es[j], hasNull)
		}
		return
	}
	vs := vector.GetColumn[T1](vec)
	for i := range os {
		hasNull := vec.GetNulls().Contains(uint64(i) + uint64(start))
		j := vps[i] - 1
		a.vs[j], a.es[j] = a.fill(int64(j), vs[int64(i)+start], a.vs[j], zs[int64(i)+start], a.es[j], hasNull)
	}
}

func (a *UnaryAgg[T1, T2]) BulkFill(i int64, zs []int64, vecs []*vector.Vector) {
	vec := vecs[0]
	if vec.GetType().IsString() {
		len := vec.Count()
		for j := 0; j < len; j++ {
			hasNull := vec.GetNulls().Contains(uint64(j))
			a.vs[i], a.es[i] = a.fill(i, (any)(vec.GetString(int64(j))).(T1), a.vs[i], zs[j], a.es[i], hasNull)
		}
		return
	}
	vs := vector.GetColumn[T1](vec)
	for j, v := range vs {
		hasNull := vec.GetNulls().Contains(uint64(j))
		a.vs[i], a.es[i] = a.fill(i, v, a.vs[i], zs[j], a.es[i], hasNull)
	}
}

// Merge a[x] += b[y]
func (a *UnaryAgg[T1, T2]) Merge(b Agg[any], x, y int64) {
	b0 := b.(*UnaryAgg[T1, T2])
	a.vs[x], a.es[x] = a.merge(x, y, a.vs[x], b0.vs[y], a.es[x], b0.es[y], b0.priv)
}

func (a *UnaryAgg[T1, T2]) BatchMerge(b Agg[any], start int64, os []uint8, vps []uint64) {
	b0 := b.(*UnaryAgg[T1, T2])
	for i := range os {
		j := vps[i] - 1
		a.vs[j], a.es[j] = a.merge(int64(j), int64(i)+start, a.vs[j], b0.vs[int64(i)+start], a.es[j], b0.es[int64(i)+start], b0.priv)
	}
}

func (a *UnaryAgg[T1, T2]) Eval(m *mheap.Mheap) (*vector.Vector, error) {
	defer func() {
		a.da = nil
		a.vs = nil
		a.es = nil
	}()
	nsp := nulls.NewWithSize(len(a.es))
	if !a.isCount {
		for i, e := range a.es {
			if e {
				nsp.Set(uint64(i))
			}
		}
	}
	if a.otyp.IsString() {
		vec := vector.New(a.otyp)
		a.vs = a.eval(a.vs)
		vs := (any)(a.vs).([][]byte)
		for _, v := range vs {
			if err := vec.Append(v, m); err != nil {
				vec.Free(m)
				return nil, err
			}
		}
		return vec, nil
	}
	return vector.NewWithData(a.otyp, a.da, a.eval(a.vs), nsp), nil
}

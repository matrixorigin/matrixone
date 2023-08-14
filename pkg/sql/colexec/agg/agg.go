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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func NewUnaryAgg[T1, T2 any](op int, priv AggStruct, isCount bool, ityp, otyp types.Type, grows func(int),
	eval func([]T2, error) ([]T2, error), merge func(int64, int64, T2, T2, bool, bool, any) (T2, bool, error),
	fill func(int64, T1, T2, int64, bool, bool) (T2, bool, error),
	batchFill func(any, any, int64, int64, []uint64, *nulls.Nulls) error) Agg[*UnaryAgg[T1, T2]] {
	return &UnaryAgg[T1, T2]{
		op:        op,
		priv:      priv,
		otyp:      otyp,
		eval:      eval,
		fill:      fill,
		merge:     merge,
		grows:     grows,
		batchFill: batchFill,
		isCount:   isCount,
		ityps:     []types.Type{ityp},
		err:       nil,
	}
}

func (a *UnaryAgg[T1, T2]) String() string {
	return fmt.Sprintf("%v", a.vs)
}

func (a *UnaryAgg[T1, T2]) Free(m *mpool.MPool) {
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

func (a *UnaryAgg[T1, T2]) Grows(size int, m *mpool.MPool) error {
	if a.otyp.IsVarlen() {
		if len(a.vs) == 0 {
			a.es = make([]bool, 0, size)
			a.vs = make([]T2, 0, size)
			a.vs = a.vs[:size]
			for i := 0; i < size; i++ {
				a.es = append(a.es, true)
			}
		} else {
			var v T2
			for i := 0; i < size; i++ {
				a.es = append(a.es, true)
				a.vs = append(a.vs, v)
			}
		}
		a.grows(size)
		return nil
	}
	sz := a.otyp.TypeSize()
	n := len(a.vs)
	if n == 0 {
		data, err := m.Alloc(size * sz)
		if err != nil {
			return err
		}
		a.da = data
		a.es = make([]bool, 0, size)
		a.vs = types.DecodeSlice[T2](a.da)
	} else if n+size >= cap(a.vs) {
		a.da = a.da[:n*sz]
		data, err := m.Grow(a.da, (n+size)*sz)
		if err != nil {
			return err
		}
		a.da = data
		a.vs = types.DecodeSlice[T2](a.da)
	}
	a.vs = a.vs[:n+size]
	a.da = a.da[:(n+size)*sz]
	for i := 0; i < size; i++ {
		a.es = append(a.es, true)
	}
	a.grows(size)
	return nil
}

func (a *UnaryAgg[T1, T2]) Fill(i int64, sel int64, vecs []*vector.Vector) error {
	var err error
	vec := vecs[0]

	if vec.IsConstNull() {
		var v T1
		a.vs[i], a.es[i], err = a.fill(i, v, a.vs[i], 1, a.es[i], true)
		if a.err == nil {
			a.err = err
		}
		return nil
	}

	hasNull := vec.GetNulls().Contains(uint64(sel))
	if vec.IsConst() {
		sel = 0
	}
	if vec.IsConstNull() {
		var v T1
		a.vs[i], a.es[i], err = a.fill(i, v, a.vs[i], 1, a.es[i], true)
		if a.err == nil {
			a.err = err
		}
	} else if vec.GetType().IsVarlen() {
		a.vs[i], a.es[i], err = a.fill(i, (any)(vec.GetBytesAt(int(sel))).(T1), a.vs[i], 1, a.es[i], hasNull)
		if a.err == nil {
			a.err = err
		}
	} else {
		a.vs[i], a.es[i], err = a.fill(i, vector.MustFixedCol[T1](vec)[sel], a.vs[i], 1, a.es[i], hasNull)
		if a.err == nil {
			a.err = err
		}
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) BatchFill(start int64, os []uint8, vps []uint64, vecs []*vector.Vector) error {
	var err error
	vec := vecs[0]
	constNull := vec.IsConstNull()
	if vec.GetType().IsVarlen() {
		for i := range os {
			hasNull := constNull || vec.GetNulls().Contains(uint64(i)+uint64(start))
			if vps[i] == 0 {
				continue
			}
			j := vps[i] - 1
			if !vec.IsConst() {
				a.vs[j], a.es[j], err = a.fill(int64(j), (any)(vec.GetBytesAt(i+int(start))).(T1), a.vs[j], 1, a.es[j], hasNull)
				if a.err == nil {
					a.err = err
				}
			} else if vec.IsConstNull() {
				var v T1
				a.vs[j], a.es[j], err = a.fill(int64(j), v, a.vs[j], 1, a.es[j], hasNull)
				if a.err == nil {
					a.err = err
				}
			} else {
				a.vs[j], a.es[j], err = a.fill(int64(j), (any)(vec.GetBytesAt(0)).(T1), a.vs[j], 1, a.es[j], hasNull)
				if a.err == nil {
					a.err = err
				}
			}

		}
		return nil
	}
	vs := vector.MustFixedCol[T1](vec)
	// I do these bad hack here because the batchFill method can't know the vector is const null or not.
	if a.batchFill != nil && !constNull {
		if a.err = a.batchFill(a.vs, vs, start, int64(len(os)), vps, vec.GetNulls()); a.err != nil {
			return a.err
		}

		nsp := vec.GetNulls()
		if nsp.Any() {
			for i := range os {
				hasNull := nsp.Contains(uint64(i) + uint64(start))
				if !hasNull {
					if vps[i] == 0 {
						continue
					}
					a.es[vps[i]-1] = false
				}
			}
		} else {
			for i := range os {
				if vps[i] == 0 {
					continue
				}
				a.es[vps[i]-1] = false
			}
		}
		return nil
	}

	inc := 1
	vi := start
	if vec.IsConst() {
		inc = 0
		vi = 0

		if vec.IsConstNull() {
			var v T1
			for i := range os {
				if vps[i] == 0 {
					continue
				}
				j := vps[i] - 1
				a.vs[j], a.es[j], err = a.fill(int64(j), v, a.vs[j], 1, a.es[j], true)
				if a.err == nil {
					a.err = err
				}
			}
			return nil
		}
	}

	for i := range os {
		hasNull := vec.GetNulls().Contains(uint64(i) + uint64(start))
		if vps[i] == 0 {
			continue
		}
		j := vps[i] - 1
		if a.err != nil {
			return nil
		}
		a.vs[j], a.es[j], err = a.fill(int64(j), vs[vi], a.vs[j], 1, a.es[j], hasNull)
		if a.err == nil {
			a.err = err
		}
		vi += int64(inc)
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) BulkFill(i int64, rowCount int, vecs []*vector.Vector) error {
	var err error
	vec := vecs[0]
	if vec.IsConst() {
		if vec.IsConstNull() {
			var v T1
			a.vs[i], a.es[i], err = a.fill(i, v, a.vs[i], int64(rowCount), a.es[i], true)
			if a.err == nil {
				a.err = err
			}
		} else if vec.GetType().IsVarlen() {
			a.vs[i], a.es[i], err = a.fill(i, (any)(vec.GetBytesAt(0)).(T1), a.vs[i], int64(rowCount), a.es[i], false)
			if a.err == nil {
				a.err = err
			}
		} else {
			a.vs[i], a.es[i], err = a.fill(i, vector.GetFixedAt[T1](vec, 0), a.vs[i], int64(rowCount), a.es[i], false)
			if a.err == nil {
				a.err = err
			}
		}
		return nil
	} else if vec.GetType().IsVarlen() {
		length := vec.Length()
		for j := 0; j < length; j++ {
			hasNull := vec.GetNulls().Contains(uint64(j))
			a.vs[i], a.es[i], err = a.fill(i, (any)(vec.GetBytesAt(j)).(T1), a.vs[i], 1, a.es[i], hasNull)
			if a.err == nil {
				a.err = err
			}
		}

		return nil
	} else {
		vs := vector.MustFixedCol[T1](vec)
		for j, v := range vs {
			hasNull := vec.GetNulls().Contains(uint64(j))
			a.vs[i], a.es[i], err = a.fill(i, v, a.vs[i], 1, a.es[i], hasNull)
			if a.err == nil {
				a.err = err
			}
		}
		return nil
	}
}

// Merge a[x] += b[y]
func (a *UnaryAgg[T1, T2]) Merge(b Agg[any], x, y int64) error {
	var err error
	b0 := b.(*UnaryAgg[T1, T2])
	if a.es[x] && !b0.es[y] {
		a.otyp = b0.otyp
	}
	a.vs[x], a.es[x], err = a.merge(x, y, a.vs[x], b0.vs[y], a.es[x], b0.es[y], b0.priv)
	if a.err == nil {
		a.err = err
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) BatchMerge(b Agg[any], start int64, os []uint8, vps []uint64) error {
	var err error
	b0 := b.(*UnaryAgg[T1, T2])
	for i := range os {
		if vps[i] == 0 {
			continue
		}
		j := vps[i] - 1
		if a.es[j] && !b0.es[int64(i)+start] {
			a.otyp = b0.otyp
		}
		a.vs[j], a.es[j], err = a.merge(int64(j), int64(i)+start, a.vs[j], b0.vs[int64(i)+start], a.es[j], b0.es[int64(i)+start], b0.priv)
		if a.err == nil {
			a.err = err
		}
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) Eval(m *mpool.MPool) (*vector.Vector, error) {
	defer func() {
		a.Free(m)
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
	if a.otyp.IsVarlen() {
		vec := vector.NewVec(a.otyp)
		a.vs, a.err = a.eval(a.vs, a.err)
		if a.err != nil {
			return nil, a.err
		}
		vs := (any)(a.vs).([][]byte)
		if err := vector.AppendBytesList(vec, vs, nil, m); err != nil {
			vec.Free(m)
			return nil, err
		}
		vec.SetNulls(nsp)
		return vec, nil
	}
	vec := vector.NewVec(a.otyp)
	a.vs, a.err = a.eval(a.vs, a.err)
	if a.err != nil {
		return nil, a.err
	}
	if err := vector.AppendFixedList(vec, a.vs, nil, m); err != nil {
		vec.Free(m)
		return nil, err
	}
	vec.SetNulls(nsp)
	return vec, nil
}

func (a *UnaryAgg[T1, T2]) WildAggReAlloc(m *mpool.MPool) error {
	d, err := m.Alloc(len(a.da))
	if err != nil {
		return err
	}
	copy(d, a.da)
	a.da = d
	setAggValues[T1, T2](a, a.otyp)
	return nil
}

func (a *UnaryAgg[T1, T2]) IsDistinct() bool {
	return false
}

func (a *UnaryAgg[T1, T2]) GetOperatorId() int {
	return a.op
}

func (a *UnaryAgg[T1, T2]) GetInputTypes() []types.Type {
	return a.ityps
}

func (a *UnaryAgg[T1, T2]) MarshalBinary() ([]byte, error) {
	pData, err := a.priv.MarshalBinary()
	if err != nil {
		return nil, err
	}
	// encode the input types.
	source := &EncodeAgg{
		Op:         a.op,
		Private:    pData,
		Es:         a.es,
		InputTypes: types.EncodeSlice(a.ityps),
		OutputType: types.EncodeType(&a.otyp),
		IsCount:    a.isCount,
	}
	switch {
	case a.otyp.Oid.IsMySQLString():
		source.Da = types.EncodeStringSlice(getUnaryAggStrVs(a))
	default:
		source.Da = a.da
	}

	return source.Marshal()
}

func getUnaryAggStrVs(strUnaryAgg any) []string {
	agg := strUnaryAgg.(*UnaryAgg[[]byte, []byte])
	result := make([]string, len(agg.vs))
	for i := range result {
		result[i] = string(agg.vs[i])
	}
	return result
}

func (a *UnaryAgg[T1, T2]) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	decoded := new(EncodeAgg)
	if err := decoded.Unmarshal(copyData); err != nil {
		return err
	}

	// Recover data
	a.ityps = types.DecodeSlice[types.Type](decoded.InputTypes)
	a.otyp = types.DecodeType(decoded.OutputType)
	a.isCount = decoded.IsCount
	a.es = decoded.Es
	data = make([]byte, len(decoded.Da))
	copy(data, decoded.Da)
	a.da = data

	setAggValues[T1, T2](a, a.otyp)

	return a.priv.UnmarshalBinary(decoded.Private)
}

func setAggValues[T1, T2 any](agg any, typ types.Type) {
	switch {
	case typ.Oid.IsMySQLString():
		a := agg.(*UnaryAgg[[]byte, []byte])
		values := types.DecodeStringSlice(a.da)
		a.vs = make([][]byte, len(values))
		for i := range a.vs {
			a.vs[i] = []byte(values[i])
		}
	default:
		a := agg.(*UnaryAgg[T1, T2])
		a.vs = types.DecodeSlice[T2](a.da)
	}
}

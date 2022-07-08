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

package vector

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func New[T types.Element](typ types.Type) *Vector[T] {
	return &Vector[T]{
		Typ:  typ,
		Col:  []T{},
		Data: []byte{},
		Nsp:  &nulls.Nulls{},
	}
}

func (v *Vector[T]) Reset() {
	v.Col = v.Col[:0]
	if !v.IsConst {
		v.Data = v.Data[:0]
	}
	if _, ok := (any)(v).(*Vector[types.String]); ok {
		v.Array.Offsets = v.Array.Offsets[:0]
		v.Array.Lengths = v.Array.Lengths[:0]
	}
}

func (v *Vector[T]) SetConst() {
	v.IsConst = true
}

func (v *Vector[T]) SetData(data []byte) {
	v.Data = data
}

func (v *Vector[T]) Length() int {
	if v.IsConst {
		return v.Const.Size
	}
	return len(v.Col)
}

func (v *Vector[T]) SetLength(n int) {
	if v.IsConst {
		v.Const.Size = n
		return
	}
	switch (any)(v).(type) {
	case *Vector[types.String]:
		v.Array.Offsets = v.Array.Offsets[:n]
		v.Array.Lengths = v.Array.Lengths[:n]
		v.Data = v.Data[:v.Array.Offsets[n-1]+v.Array.Lengths[n-1]]
	default:
		v.Data = v.Data[:n*v.Typ.TypeSize()]
	}
	v.Col = v.Col[:n]
}

func (v *Vector[T]) Type() types.Type {
	return v.Typ
}

func (v *Vector[T]) Nulls() *nulls.Nulls {
	return v.Nsp
}

func (v *Vector[T]) Free(m *mheap.Mheap) {
	if v.Data != nil {
		mheap.Free(m, v.Data)
	}
}

func (v *Vector[T]) ReallocForFixedSlice(n int, m *mheap.Mheap) ([]T, error) {
	var w T

	len := len(v.Col)
	if n+len >= cap(v.Col) {
		if err := v.Realloc(w.Size()*(cap(v.Col)-n-len+1), m); err != nil {
			return nil, err
		}
	}
	v.Col = v.Col[:n+len]
	return v.Col, nil
}

func (v *Vector[T]) Realloc(size int, m *mheap.Mheap) error {
	oldLen := len(v.Data)
	data, err := mheap.Grow(m, v.Data, int64(cap(v.Data)+size))
	if err != nil {
		return err
	}
	mheap.Free(m, v.Data)
	v.Data = data[:oldLen]
	switch vec := (any)(v).(type) {
	case *Vector[types.String]:
		vec.Col = vec.Col[:0]
		for i, off := range vec.Array.Offsets {
			vec.Col = append(vec.Col, v.Data[off:off+vec.Array.Lengths[i]])
		}
	default:
		v.Col = encoding.DecodeSlice[T](v.Data[:len(data)], size)[:oldLen/size]
	}
	return nil
}

func (v *Vector[T]) Append(w T, m *mheap.Mheap) error {
	switch vec := (any)(v).(type) {
	case *Vector[types.String]:
		wv, _ := (any)(w).(types.String)
		n := len(v.Data)
		if n+w.Size() >= cap(v.Data) {
			if err := v.Realloc(n+w.Size()-cap(v.Data)+1, m); err != nil {
				return err
			}
		}
		vec.Array.Lengths = append(vec.Array.Lengths, uint64(len(wv)))
		vec.Array.Offsets = append(vec.Array.Offsets, uint64(len(v.Data)))
		size := len(v.Data)
		v.Data = append(v.Data, wv...)
		vec.Col = append(vec.Col, v.Data[size:size+len(wv)])
	default:
		n := len(v.Col)
		if n+1 >= cap(v.Col) {
			if err := v.Realloc(w.Size(), m); err != nil {
				return err
			}
		}
		v.Col = append(v.Col, w)
		v.Data = v.Data[:(n+1)*w.Size()]
	}
	return nil
}

func (v *Vector[T]) Dup(m *mheap.Mheap) (*Vector[T], error) {
	w := New[T](v.Typ)
	if v.IsConst || len(v.Col) == 0 {
		return w, nil
	}
	if err := w.Realloc(len(v.Data), m); err != nil {
		return nil, err
	}
	w.Data = w.Data[:len(v.Data)]
	copy(w.Data, v.Data)
	switch vec := (any)(v).(type) {
	case *Vector[types.String]:
		wv := (any)(w).(*Vector[types.String])
		wv.Col = make([]types.String, len(vec.Col))
		w.Array.Lengths = make([]uint64, len(v.Array.Lengths))
		w.Array.Offsets = make([]uint64, len(v.Array.Offsets))
		copy(w.Array.Lengths, v.Array.Lengths)
		copy(w.Array.Offsets, v.Array.Offsets)
		for i, o := range w.Array.Offsets {
			wv.Col[i] = w.Data[o : o+w.Array.Lengths[i]]
		}
	default:
		size := v.Col[0].Size()
		w.Col = encoding.DecodeSlice[T](w.Data, size)[:len(w.Data)/size]
	}
	return w, nil
}

func (v *Vector[T]) Shrink(sels []int64) {
	if v.IsConst || len(v.Col) == 0 {
		return
	}
	size := v.Col[0].Size()
	switch (any)(v).(type) {
	case *Vector[types.String]:
		for i, sel := range sels {
			v.Array.Offsets[i] = v.Array.Offsets[sel]
			v.Array.Lengths[i] = v.Array.Lengths[sel]
		}
		v.Array.Offsets = v.Array.Offsets[:len(sels)]
		v.Array.Lengths = v.Array.Lengths[:len(sels)]
	default:
		v.Data = v.Data[:len(sels)*size]
	}
	for i, sel := range sels {
		v.Col[i] = v.Col[sel]
	}
	v.Col = v.Col[:len(sels)]
	v.Nsp = nulls.Filter(v.Nsp, sels)
}

func (v *Vector[T]) Shuffle(sels []int64, m *mheap.Mheap) error {
	size := v.Col[0].Size()
	switch vec := (any)(v).(type) {
	case *Vector[types.String]:
		maxSize := uint64(0)
		for _, sel := range sels {
			if size := v.Array.Offsets[sel] + v.Array.Lengths[sel]; size > maxSize {
				maxSize = size
			}
		}
		data, err := mheap.Alloc(m, int64(maxSize))
		if err != nil {
			return err
		}
		o := uint64(0)
		data = data[:0]
		os := make([]uint64, len(sels))
		ns := make([]uint64, len(sels))
		for i, sel := range sels {
			data = append(data, vec.Col[sel]...)
			os[i] = o
			ns[i] = uint64(len(vec.Col[sel]))
			o += ns[i]
		}
		mheap.Free(m, v.Data)
		v.Data = data
		vec.Col = vec.Col[:len(sels)]
		for i, o := range os {
			vec.Col[i] = v.Data[o : o+ns[i]]
		}
		copy(v.Array.Offsets, os)
		copy(v.Array.Lengths, ns)
	default:
		ws := make([]T, len(v.Col))
		for i, sel := range sels {
			ws[i] = v.Col[sel]
		}
		v.Col = v.Col[:len(sels)]
		v.Data = v.Data[:len(sels)*size]
		copy(v.Col, ws)
	}
	v.Nsp = nulls.Filter(v.Nsp, sels)
	return nil
}

func (v *Vector[T]) UnionOne(w *Vector[T], sel int64, m *mheap.Mheap) error {
	return v.Append(w.Col[sel], m)
}

func (v *Vector[T]) UnionNull(_ *Vector[T], _ int64, m *mheap.Mheap) error {
	var val T

	if err := v.Append(val, m); err != nil {
		return err
	}
	return nil
}

func (v *Vector[T]) UnionBatch(w *Vector[T], offset int64, cnt int, flags []uint8, m *mheap.Mheap) error {
	switch vec := (any)(v).(type) {
	case *Vector[types.String]:
		wv := (any)(w).(*Vector[types.String])
		incSize := 0
		for i, flg := range flags {
			if flg > 0 {
				incSize += int(vec.Array.Lengths[int(offset)+i])
			}
		}
		n := len(v.Data)
		if n+incSize >= cap(v.Data) {
			if err := v.Realloc(n+incSize-cap(v.Data), m); err != nil {
				return err
			}
		}
		o := uint64(len(v.Data))
		for i, flg := range flags {
			if flg > 0 {
				from := wv.Col[int(offset)+i]
				v.Array.Offsets = append(v.Array.Offsets, o)
				v.Array.Lengths = append(v.Array.Lengths, uint64(len(from)))
				v.Data = append(v.Data, from...)
				vec.Col = append(vec.Col, v.Data[o:o+uint64(len(from))])
				o += uint64(len(from))
			}
		}
	default:
		n := len(v.Col)
		if n+cnt >= cap(v.Col) {
			if err := v.Realloc(n+cnt-cap(v.Col), m); err != nil {
				return err
			}
		}
		v.Col = v.Col[:n+cnt]
		for i, j := 0, n; i < len(flags); i++ {
			if flags[i] > 0 {
				v.Col[j] = w.Col[int(offset)+i]
				j++
			}
		}
	}
	return nil
}

func (v *Vector[T]) String() string {
	return fmt.Sprintf("%v-%v", v.Col, v.Nsp)
}

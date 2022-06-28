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

package anyvalue

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"golang.org/x/exp/constraints"
)

type ts1 interface {
	constraints.Integer | bool | types.Date | types.Datetime |
		constraints.Float | types.Decimal64 | types.Decimal128 | types.Timestamp
}

// AnyVRing1 for bool / int / uint / float / date / datetime
// and decimal64 / decimal128
type AnyVRing1[T ts1] struct {
	Typ types.Type

	Da  []byte
	Vs  []T
	Ns  []int64
	Set []bool // if Set[i] is false, the Vs[i] hasn't been assign any value
}

// AnyVRing2 for char / varchar
type AnyVRing2 struct {
	Typ types.Type
	Mp  *mheap.Mheap
	Vs  [][]byte
	Ns  []int64
	Set []bool // if Set[i] is false, the Vs[i] hasn't been assign any value
}

func EncodeAnyValueRing1[T ts1](ring *AnyVRing1[T], buf *bytes.Buffer) {
	// Ns
	n := len(ring.Ns)
	buf.Write(encoding.EncodeUint32(uint32(n)))
	if n > 0 {
		buf.Write(encoding.EncodeInt64Slice(ring.Ns))
	}
	// Vs
	da := encoding.EncodeFixedSlice[T](ring.Vs, ring.Typ.Oid.TypeLen())
	n = len(da)
	buf.Write(encoding.EncodeUint32(uint32(n)))
	if n > 0 {
		buf.Write(da)
	}
	// Set
	n = len(ring.Set)
	buf.Write(encoding.EncodeUint32(uint32(n)))
	if n > 0 {
		buf.Write(encoding.EncodeBoolSlice(ring.Set))
	}
	// Typ
	buf.Write(encoding.EncodeType(ring.Typ))
}

func DecodeAnyValueRing1[T ts1](data []byte, typ types.Type) (*AnyVRing1[T], []byte, error) {
	r := new(AnyVRing1[T])
	l := typ.Oid.TypeLen()
	// Ns
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		r.Ns = encoding.DecodeInt64Slice(data[:n*8])
		data = data[n*8:]
	}
	// Da
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		r.Da = data[:n]
		data = data[n:]
	}
	// Vs
	r.Vs = encoding.DecodeFixedSlice[T](r.Da, l)
	// Set
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		r.Set = encoding.DecodeBoolSlice(data[:n])
	}
	// Typ
	r.Typ = encoding.DecodeType(data[:encoding.TypeSize])
	data = data[encoding.TypeSize:]
	return r, data, nil
}

func EncodeAnyRing2(ring *AnyVRing2, buf *bytes.Buffer) {
	// Ns
	n := len(ring.Ns)
	buf.Write(encoding.EncodeUint32(uint32(n)))
	if n > 0 {
		buf.Write(encoding.EncodeInt64Slice(ring.Ns))
	}
	// Vs
	n = len(ring.Vs)
	buf.Write(encoding.EncodeUint32(uint32(n)))
	if n > 0 {
		for i := 0; i < n; i++ {
			m := len(ring.Vs[i])
			buf.Write(encoding.EncodeUint32(uint32(m)))
			if m > 0 {
				buf.Write(ring.Vs[i])
			}
		}
	}
	// Set
	n = len(ring.Set)
	buf.Write(encoding.EncodeUint32(uint32(n)))
	if n > 0 {
		buf.Write(encoding.EncodeBoolSlice(ring.Set))
	}
	// Typ
	buf.Write(encoding.EncodeType(ring.Typ))
}

func DecodeAnyRing2(data []byte) (*AnyVRing2, []byte, error) {
	r := new(AnyVRing2)
	// Ns
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		r.Ns = encoding.DecodeInt64Slice(data[:n*8])
		data = data[n*8:]
	}
	// Vs
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		r.Vs = make([][]byte, n)
		for i := uint32(0); i < n; i++ {
			m := encoding.DecodeUint32(data[:4])
			data = data[4:]
			if m > 0 {
				r.Vs[i] = data[:m]
				data = data[m:]
			}
		}
	}
	// Set
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		r.Set = encoding.DecodeBoolSlice(data[:n])
		data = data[n:]
	}
	// Typ
	r.Typ = encoding.DecodeType(data[:encoding.TypeSize])
	data = data[encoding.TypeSize:]
	return r, data, nil
}

func NewAnyValueRingWithTypeCheck(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_bool:
		return &AnyVRing1[bool]{Typ: typ}, nil
	case types.T_uint8:
		return &AnyVRing1[uint8]{Typ: typ}, nil
	case types.T_uint16:
		return &AnyVRing1[uint16]{Typ: typ}, nil
	case types.T_uint32:
		return &AnyVRing1[uint32]{Typ: typ}, nil
	case types.T_uint64:
		return &AnyVRing1[uint64]{Typ: typ}, nil
	case types.T_int8:
		return &AnyVRing1[int8]{Typ: typ}, nil
	case types.T_int16:
		return &AnyVRing1[int16]{Typ: typ}, nil
	case types.T_int32:
		return &AnyVRing1[int32]{Typ: typ}, nil
	case types.T_int64:
		return &AnyVRing1[int64]{Typ: typ}, nil
	case types.T_float32:
		return &AnyVRing1[float32]{Typ: typ}, nil
	case types.T_float64:
		return &AnyVRing1[float64]{Typ: typ}, nil
	case types.T_char:
		return &AnyVRing2{Typ: typ}, nil
	case types.T_varchar:
		return &AnyVRing2{Typ: typ}, nil
	case types.T_date:
		return &AnyVRing1[types.Date]{Typ: typ}, nil
	case types.T_datetime:
		return &AnyVRing1[types.Datetime]{Typ: typ}, nil
	case types.T_decimal64:
		return &AnyVRing1[types.Decimal64]{Typ: typ}, nil
	case types.T_decimal128:
		return &AnyVRing1[types.Decimal128]{Typ: typ}, nil
	case types.T_timestamp:
		return &AnyVRing1[types.Timestamp]{Typ: typ}, nil
	}
	return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("Any_Value do not support '%v'", typ.Oid))
}

// shouldSet returns true means we should assign the value
func shouldSet(set bool) bool {
	return !set
}

func (r *AnyVRing1[T]) String() string {
	return fmt.Sprintf("any_value(%s)", r.Typ)
}

func (r *AnyVRing2) String() string {
	return fmt.Sprintf("any_value(%s)", r.Typ)
}

func (r *AnyVRing1[T]) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
		r.Set = nil
	}
}

func (r *AnyVRing2) Free(_ *mheap.Mheap) {
	r.Vs = nil
	r.Ns = nil
	r.Set = nil
}

func (r *AnyVRing1[T]) Count() int {
	return len(r.Vs)
}

func (r *AnyVRing2) Count() int {
	return len(r.Vs)
}

func (r *AnyVRing1[T]) Size() int {
	return len(r.Da)
}

func (r *AnyVRing2) Size() int {
	return 0
}

func (r *AnyVRing1[T]) Dup() ring.Ring {
	return &AnyVRing1[T]{
		Typ: r.Typ,
	}
}

func (r *AnyVRing2) Dup() ring.Ring {
	return &AnyVRing2{
		Typ: r.Typ,
	}
}

func (r *AnyVRing1[T]) Type() types.Type {
	return r.Typ
}

func (r *AnyVRing2) Type() types.Type {
	return r.Typ
}

func (r *AnyVRing1[T]) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
	r.Set = r.Set[:n]
}

func (r *AnyVRing2) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
	r.Set = r.Set[:n]
}

func (r *AnyVRing1[T]) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
		r.Set[i] = r.Set[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
	r.Set = r.Set[:len(sels)]
}

func (r *AnyVRing2) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
		r.Set[i] = r.Set[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
	r.Set = r.Set[:len(sels)]
}

func (r *AnyVRing1[T]) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *AnyVRing2) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *AnyVRing1[T]) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	itemSize := r.Typ.Oid.TypeLen()
	if n == 0 {
		data, err := mheap.Alloc(m, int64(8*itemSize))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
		r.Set = make([]bool, 0, 8)
		r.Vs = encoding.DecodeFixedSlice[T](data, itemSize)
	} else if n+1 >= cap(r.Vs) {
		r.Da = r.Da[:n*itemSize]
		data, err := mheap.Grow(m, r.Da, int64(n+1)*int64(itemSize))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeFixedSlice[T](data, itemSize)
	}
	r.Vs = r.Vs[:n+1]
	r.Da = r.Da[:(n+1)*itemSize]
	r.Set = append(r.Set, false)
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *AnyVRing2) Grow(m *mheap.Mheap) error {
	if r.Mp == nil {
		r.Mp = m
	}
	if len(r.Vs) == 0 {
		r.Ns = make([]int64, 0, 8)
		r.Vs = make([][]byte, 0, 8)
		r.Set = make([]bool, 0, 8)
	}
	r.Ns = append(r.Ns, 0)
	r.Vs = append(r.Vs, make([]byte, 0, 4))
	r.Set = append(r.Set, false)
	return nil
}

func (r *AnyVRing1[T]) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	itemSize := r.Typ.Oid.TypeLen()
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*itemSize))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
		r.Set = make([]bool, 0, size)
		r.Vs = encoding.DecodeFixedSlice[T](data, itemSize)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*itemSize]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*int64(itemSize))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeFixedSlice[T](data, itemSize)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*itemSize]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
		r.Set = append(r.Set, false)
	}
	return nil
}

func (r *AnyVRing2) Grows(size int, m *mheap.Mheap) error {
	if r.Mp == nil {
		r.Mp = m
	}
	if len(r.Vs) == 0 {
		r.Ns = make([]int64, 0, size)
		r.Vs = make([][]byte, 0, size)
		r.Set = make([]bool, 0, size)
	}
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
		r.Vs = append(r.Vs, make([]byte, 0, 4))
		r.Set = append(r.Set, false)
	}
	return nil
}

func (r *AnyVRing1[T]) Fill(i int64, sel, z int64, vec *vector.Vector) {
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
		return
	}
	if v := vec.Col.([]T)[sel]; shouldSet(r.Set[i]) {
		r.Set[i] = true
		r.Vs[i] = v
	}
}

func (r *AnyVRing2) Fill(i int64, sel, z int64, vec *vector.Vector) {
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
		return
	}
	if v := vec.Col.(*types.Bytes).Get(sel); shouldSet(r.Set[i]) {
		r.Set[i] = true
		r.Vs[i] = append(r.Vs[i][:0], v...)
	}
}

func (r *AnyVRing1[T]) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]T)
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.Ns[vps[i]-1] += zs[int64(i)+start]
			} else {
				j := vps[i] - 1
				if shouldSet(r.Set[j]) {
					r.Vs[j] = vs[int64(i)+start]
					r.Set[j] = true
				}
			}
		}
	} else {
		for i := range os {
			j := vps[i] - 1
			if shouldSet(r.Set[j]) {
				r.Vs[j] = vs[int64(i)+start]
				r.Set[j] = true
			}
		}
	}
}

func (r *AnyVRing2) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.(*types.Bytes)
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.Ns[vps[i]-1] += zs[int64(i)+start]
			} else {
				j := vps[i] - 1
				if v := vs.Get(int64(i) + start); shouldSet(r.Set[j]) {
					r.Vs[j] = append(r.Vs[j][:0], v...)
					r.Set[j] = true
				}
			}
		}
	} else {
		for i := range os {
			j := vps[i] - 1
			if v := vs.Get(int64(i) + start); shouldSet(r.Set[j]) {
				r.Vs[j] = append(r.Vs[j][:0], v...)
				r.Set[j] = true
			}
		}
	}
}

func (r *AnyVRing1[T]) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]T)
	if nulls.Any(vec.Nsp) {
		for j, v := range vs {
			if nulls.Contains(vec.Nsp, uint64(j)) {
				r.Ns[i] += zs[j]
			} else {
				if shouldSet(r.Set[i]) {
					r.Vs[i] = v
					r.Set[i] = true
				}
			}
		}
	} else {
		for _, v := range vs {
			if shouldSet(r.Set[i]) {
				r.Vs[i] = v
				r.Set[i] = true
			}
		}
	}
}

func (r *AnyVRing2) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.(*types.Bytes)
	if nulls.Any(vec.Nsp) {
		for j := range zs {
			if nulls.Contains(vec.Nsp, uint64(j)) {
				r.Ns[i] += zs[j]
			} else {
				if v := vs.Get(int64(j)); shouldSet(r.Set[i]) {
					r.Vs[i] = append(r.Vs[i][:0], v...)
					r.Set[i] = true
				}
			}
		}
	} else {
		for j := range zs {
			if v := vs.Get(int64(j)); shouldSet(r.Set[i]) {
				r.Vs[i] = append(r.Vs[i][:0], v...)
				r.Set[i] = true
			}
		}
	}
}

func (r *AnyVRing1[T]) Add(a interface{}, x, y int64) {
	ar := a.(*AnyVRing1[T])
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	if ar.Set[y] && shouldSet(r.Set[x]) {
		r.Vs[x] = ar.Vs[y]
		r.Set[x] = true
	}
	r.Ns[x] += ar.Ns[y]
}

func (r *AnyVRing2) Add(a interface{}, x, y int64) {
	ar := a.(*AnyVRing2)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	if ar.Set[y] && shouldSet(r.Set[x]) {
		r.Vs[x] = ar.Vs[y]
		r.Set[x] = true
	}
	r.Ns[x] += ar.Ns[y]
}

func (r *AnyVRing1[T]) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*AnyVRing1[T])
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	for i := range os {
		j := vps[i] - 1
		if ar.Set[int64(i)+start] && shouldSet(r.Set[j]) {
			r.Vs[j] = ar.Vs[int64(i)+start]
			r.Set[j] = true
		}
		r.Ns[j] += ar.Ns[int64(i)+start]
	}
}

func (r *AnyVRing2) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*AnyVRing2)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	for i := range os {
		j := vps[i] - 1
		if ar.Set[int64(i)+start] && shouldSet(r.Set[j]) {
			r.Vs[j] = ar.Vs[int64(i)+start]
		}
		r.Ns[j] += ar.Ns[int64(i)+start]
	}
}

func (r *AnyVRing1[T]) Mul(a interface{}, x, y, z int64) {
	ar := a.(*AnyVRing1[T])
	if ar.Set[y] && shouldSet(r.Set[x]) {
		r.Vs[x] = ar.Vs[y]
		r.Set[x] = true
	}
	r.Ns[x] += ar.Ns[y] * z
}

func (r *AnyVRing2) Mul(a interface{}, x, y, z int64) {
	ar := a.(*AnyVRing2)
	if ar.Set[y] && shouldSet(r.Set[x]) {
		r.Vs[x] = ar.Vs[y]
	}
	r.Ns[x] += ar.Ns[y] * z
}

func (r *AnyVRing1[T]) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
		r.Set = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if z-r.Ns[i] == 0 || !r.Set[i] {
			nulls.Add(nsp, uint64(i))
		}
	}
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Da,
		Col:  r.Vs,
		Or:   false,
		Typ:  r.Typ,
	}
}

func (r *AnyVRing2) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Vs = nil
		r.Ns = nil
		r.Set = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if z-r.Ns[i] == 0 || !r.Set[i] {
			nulls.Add(nsp, uint64(i))
		}
	}
	var data []byte
	os, ls := []uint32(nil), []uint32(nil) // offsets and lengths
	o := uint32(0)
	for _, v := range r.Vs {
		os = append(os, o)
		data = append(data, v...)
		o += uint32(len(v))
		ls = append(ls, uint32(len(v)))
	}
	return &vector.Vector{
		Nsp: nsp,
		Col: &types.Bytes{
			Data:    data,
			Offsets: os,
			Lengths: ls,
		},
		Or:  false,
		Typ: r.Typ,
	}
}

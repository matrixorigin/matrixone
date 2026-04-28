// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"bytes"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// vectorUnmarshal is instead of vector.UnmarshalBinary.
// it will check if mp is nil first.
func vectorUnmarshal(v *vector.Vector, data []byte, mp *mpool.MPool) error {
	if mp == nil {
		return v.UnmarshalBinary(data)
	}
	return v.UnmarshalBinaryWithCopy(data, mp)
}

func FromD64ToD128(v types.Decimal64) types.Decimal128 {
	k := types.Decimal128{
		B0_63:   uint64(v),
		B64_127: 0,
	}
	if v.Sign() {
		k.B64_127 = ^k.B64_127
	}
	return k
}

func modifyChunkSizeOfAggregator(a AggFuncExec, n int) {
	r := a.GetOptResult()
	if r != nil {
		r.modifyChunkSize(n)
	}
}

func SyncAggregatorsToChunkSize(as []AggFuncExec, syncLimit int) {
	for _, a := range as {
		modifyChunkSizeOfAggregator(a, syncLimit)
	}
}

type Vectors[T numeric | types.Decimal64 | types.Decimal128] struct {
	vecs []*vector.Vector
}

const (
	MaxVectorLength = 262144
)

func NewVectors[T numeric | types.Decimal64 | types.Decimal128](typ types.Type) *Vectors[T] {
	vec := vector.NewOffHeapVecWithType(typ)
	return &Vectors[T]{vecs: []*vector.Vector{vec}}
}

func (vs *Vectors[T]) MarshalBinary() ([]byte, error) {
	var bbuf bytes.Buffer
	length := int64(len(vs.vecs))
	if _, err := bbuf.Write(types.EncodeInt64(&length)); err != nil {
		return nil, err
	}
	for _, v := range vs.vecs {
		var buf []byte
		var err error
		if buf, err = v.MarshalBinary(); err != nil {
			return nil, err
		}
		if _, err := WriteBytes(buf, &bbuf); err != nil {
			return nil, err
		}
	}
	return bbuf.Bytes(), nil
}

func (vs *Vectors[T]) Unmarshal(data []byte, typ types.Type, mp *mpool.MPool) error {
	bbuf := bytes.NewBuffer(data)
	length := int64(0)
	if _, err := bbuf.Read(types.EncodeInt64(&length)); err != nil {
		return err
	}
	for i := int64(0); i < length; i++ {
		var buf []byte
		var err error
		if buf, _, err = ReadBytes(bbuf); err != nil {
			return err
		}
		vec := vector.NewOffHeapVecWithType(typ)
		if err := vectorUnmarshal(vec, buf, mp); err != nil {
			return err
		}
		vs.vecs = append(vs.vecs, vec)
	}
	return nil
}

func (vs *Vectors[T]) UnmarshalFromReader(r io.Reader, typ types.Type, mp *mpool.MPool) error {
	length := int64(0)
	if _, err := io.ReadFull(r, types.EncodeInt64(&length)); err != nil {
		return err
	}
	for i := int64(0); i < length; i++ {
		sz, err := types.ReadUint32(r)
		if err != nil {
			return err
		}
		lr := io.LimitReader(r, int64(sz))
		vec := vector.NewOffHeapVecWithType(typ)
		if err := vec.UnmarshalWithReader(lr, mp); err != nil {
			return err
		}
		if _, err := io.Copy(io.Discard, lr); err != nil {
			return err
		}
		vs.vecs = append(vs.vecs, vec)
	}
	return nil
}

func (vs *Vectors[T]) Length() int {
	length := 0
	for _, v := range vs.vecs {
		length += v.Length()
	}
	return length
}

func (vs *Vectors[T]) getAppendableVector() *vector.Vector {
	vec := vs.vecs[len(vs.vecs)-1]
	if vec.Length() >= MaxVectorLength {
		vec = vector.NewOffHeapVecWithType(*vec.GetType())
		vs.vecs = append(vs.vecs, vec)
	}
	return vec
}

// clone other
func (vs *Vectors[T]) Union(other *Vectors[T], mp *mpool.MPool) error {
	if len(other.vecs) == 0 {
		return nil
	}
	for _, vec := range other.vecs {
		vals := vector.MustFixedColWithTypeCheck[T](vec)
		left := len(vals)
		for {
			vec := vs.getAppendableVector()
			appendableCount := MaxVectorLength - vec.Length()
			if appendableCount > left {
				appendableCount = left
			}

			if err := vector.AppendFixedList(vec, vals[:appendableCount], nil, mp); err != nil {
				return err
			}
			left -= appendableCount
			vals = vals[appendableCount:]
			if left == 0 {
				break
			}
		}
	}

	return nil
}

func (vs *Vectors[T]) Free(mp *mpool.MPool) {
	for _, vec := range vs.vecs {
		vec.Free(mp)
	}
}

func (vs *Vectors[T]) Size() int64 {
	var size int64
	for _, vec := range vs.vecs {
		size += int64(vec.Allocated())
	}
	// 8 is the size of a pointer.
	size += int64(cap(vs.vecs)) * 8
	return size
}

func AppendMultiFixed[T numeric | types.Decimal64 | types.Decimal128](vecs *Vectors[T], vals T, isNull bool, cnt int, mp *mpool.MPool) error {
	leftRow := cnt
	for {
		vec := vecs.getAppendableVector()
		appendCnt := MaxVectorLength - vec.Length()
		if appendCnt > leftRow {
			appendCnt = leftRow
		}
		if err := vector.AppendMultiFixed(vec, vals, isNull, appendCnt, mp); err != nil {
			return err
		}
		leftRow -= appendCnt
		if leftRow == 0 {
			break
		}
	}
	return nil
}

func WriteBytes(b []byte, w io.Writer) (n int64, err error) {
	size := uint32(len(b))
	if _, err = w.Write(types.EncodeUint32(&size)); err != nil {
		return
	}
	wn, err := w.Write(b)
	return int64(wn + 4), err
}
func ReadBytes(r io.Reader) (buf []byte, n int64, err error) {
	strLen := uint32(0)
	if _, err = io.ReadFull(r, types.EncodeUint32(&strLen)); err != nil {
		return
	}
	buf = make([]byte, strLen)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n = 4 + int64(strLen)
	return
}

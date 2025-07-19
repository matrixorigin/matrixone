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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// vectorAppendWildly is a more efficient version of vector.AppendFixed.
// It ignores the const and null flags check, and uses a wilder way to append (avoiding the overhead of appending one by one).
func vectorAppendWildly[T numeric | types.Decimal64 | types.Decimal128](v *vector.Vector, mp *mpool.MPool, value T) error {
	oldLen := v.Length()
	if oldLen == v.Capacity() {
		if err := v.PreExtend(10, mp); err != nil {
			return err
		}
	}
	v.SetLength(oldLen + 1)

	var vs []T
	vector.ToSlice(v, &vs)
	vs[oldLen] = value
	return nil
}

/*
func vectorAppendBytesWildly(v *vector.Vector, mp *mpool.MPool, value []byte) error {
	var va types.Varlena
	if err := vector.BuildVarlenaFromByteSlice(v, &va, &value, mp); err != nil {
		return err
	}

	oldLen := v.Length()
	if oldLen == v.Capacity() {
		if err := v.PreExtend(10, mp); err != nil {
			return err
		}
	}
	v.SetLength(oldLen + 1)

	var vs []types.Varlena
	vector.ToSliceNoTypeCheck(v, &vs)
	vs[oldLen] = va
	return nil
}
*/

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

func getChunkSizeOfAggregator(a AggFuncExec) int {
	r := a.GetOptResult()
	if r != nil {
		return r.getChunkSize()
	}
	return math.MaxInt64
}

func GetMinAggregatorsChunkSize(outer []*vector.Vector, as []AggFuncExec) (minLimit int) {
	minLimit = math.MaxInt64
	for _, o := range outer {
		if s := GetChunkSizeFromType(*o.GetType()); s < minLimit {
			minLimit = s
		}
	}
	for _, a := range as {
		if s := getChunkSizeOfAggregator(a); s < minLimit {
			minLimit = s
		}
	}
	return minLimit
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
	vec := vector.NewVec(typ)
	return &Vectors[T]{vecs: []*vector.Vector{vec}}
}

func NewEmptyVectors[T numeric | types.Decimal64 | types.Decimal128]() *Vectors[T] {
	return &Vectors[T]{vecs: make([]*vector.Vector, 0)}
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
		vec := vector.NewVec(typ)
		if err := vectorUnmarshal(vec, buf, mp); err != nil {
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
		vec = vector.NewVec(*vec.GetType())
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

func MedianDecimal64[T numeric | types.Decimal64 | types.Decimal128](vs *Vectors[T]) (types.Decimal128, error) {
	vals := make([]types.Decimal64, 0)
	for _, vec := range vs.vecs {
		vals = append(vals, vector.MustFixedColWithTypeCheck[types.Decimal64](vec)...)
	}
	lessFnFactory := func(nums []types.Decimal64) func(a, b int) bool {
		return func(i, j int) bool {
			return nums[i].Compare(nums[j]) < 0
		}
	}
	rows := len(vals)
	if rows&1 == 1 {
		val := quickSelect(
			vals,
			lessFnFactory,
			rows>>1,
		)
		return FromD64ToD128(val).Scale(1)
	} else {
		decimal1 := quickSelect(
			vals,
			lessFnFactory,
			rows>>1-1,
		)
		decimal2 := quickSelect(
			vals,
			lessFnFactory,
			rows>>1,
		)

		v1, v2 := FromD64ToD128(decimal1), FromD64ToD128(decimal2)
		var ret types.Decimal128
		var err error
		if ret, err = v1.Add128(v2); err != nil {
			return types.Decimal128{}, err
		}
		if ret.Sign() {
			// scale(1) here because we set the result scale to be arg.Scale+1
			if ret, err = ret.Minus().Scale(1); err != nil {
				return types.Decimal128{}, err
			}
			ret = ret.Right(1).Minus()
		} else {
			if ret, err = ret.Scale(1); err != nil {
				return types.Decimal128{}, err
			}
			ret = ret.Right(1)
		}
		return ret, nil
	}
}

func MedianDecimal128[T numeric | types.Decimal64 | types.Decimal128](vs *Vectors[T]) (types.Decimal128, error) {
	vals := make([]types.Decimal128, 0)
	for _, vec := range vs.vecs {
		vals = append(vals, vector.MustFixedColWithTypeCheck[types.Decimal128](vec)...)
	}

	lessFnFactory := func(nums []types.Decimal128) func(a, b int) bool {
		return func(i, j int) bool {
			return nums[i].Compare(nums[j]) < 0
		}
	}
	rows := len(vals)
	if rows&1 == 1 {
		ret := quickSelect(
			vals,
			lessFnFactory,
			rows>>1,
		)
		var err error
		if ret, err = ret.Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret, nil
	} else {
		v1 := quickSelect(
			vals,
			lessFnFactory,
			rows>>1-1,
		)
		v2 := quickSelect(
			vals,
			lessFnFactory,
			rows>>1,
		)
		var ret types.Decimal128
		var err error
		if ret, err = v1.Add128(v2); err != nil {
			return types.Decimal128{}, err
		}
		if ret.Sign() {
			// scale(1) here because we set the result scale to be arg.Scale+1
			if ret, err = ret.Minus().Scale(1); err != nil {
				return types.Decimal128{}, err
			}
			ret = ret.Right(1).Minus()
		} else {
			if ret, err = ret.Scale(1); err != nil {
				return types.Decimal128{}, err
			}
			ret = ret.Right(1)
		}
		return ret, nil
	}
}

func MedianNumeric[T numeric](vs *Vectors[T]) (float64, error) {
	vals := make([]T, 0)
	for _, vec := range vs.vecs {
		vals = append(vals, vector.MustFixedColWithTypeCheck[T](vec)...)
	}
	lessFnFactory := func(nums []T) func(a, b int) bool {
		return func(i, j int) bool {
			return nums[i] < nums[j]
		}
	}
	rows := len(vals)
	if rows&1 == 1 {
		return float64(quickSelect(
			vals,
			lessFnFactory,
			rows>>1,
		)), nil
	} else {
		v1 := quickSelect(
			vals,
			lessFnFactory,
			rows>>1-1,
		)
		v2 := quickSelect(
			vals,
			lessFnFactory,
			rows>>1,
		)
		return float64(v1+v2) / 2, nil
	}
}

func quickSelect[T numeric | types.Decimal64 | types.Decimal128](nums []T, lessFnFactory func([]T) func(a, b int) bool, k int) T {
	if len(nums) == 1 {
		return nums[0]
	}
	pivotIndex := len(nums) / 2
	lows := []T{}
	highs := []T{}
	pivots := []T{}
	lessFn := lessFnFactory(nums)
	for i, v := range nums {
		switch {
		case lessFn(i, pivotIndex):
			lows = append(lows, v)
		case lessFn(pivotIndex, i):
			highs = append(highs, v)
		default:
			pivots = append(pivots, v)
		}
	}
	switch {
	case k < len(lows):
		return quickSelect[T](lows, lessFnFactory, k)
	case k < len(lows)+len(pivots):
		return pivots[0]
	default:
		return quickSelect(highs, lessFnFactory, k-len(lows)-len(pivots))
	}
}

// vectorAppendWildly is a more efficient version of vector.AppendFixed.
// It ignores the const and null flags check, and uses a wilder way to append (avoiding the overhead of appending one by one).
func vectorsAppendWildly[T numeric | types.Decimal64 | types.Decimal128](v *Vectors[T], mp *mpool.MPool, value T) error {
	vec := v.getAppendableVector()
	return vectorAppendWildly(vec, mp, value)
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

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

package approxcd

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

type bytesIter interface {
	// NextBytes is reserved for outer loop case, not used for now
	NextBytes() ([]byte, bool)
	// For each []byte, call the passed function
	Foreach(func([]byte))
	// zero-based index, return nil if null
	Nth(int64) []byte
}

type fixedBytes struct {
	data               []byte
	nsp                *nulls.Nulls
	dataLen, i, stride int
}

func newFixedBytes(data []byte, nsp *nulls.Nulls, stride int) *fixedBytes {
	return &fixedBytes{
		data:    data,
		dataLen: len(data),
		nsp:     nsp,
		i:       0,
		stride:  stride,
	}
}

func (b *fixedBytes) NextBytes() (data []byte, hasNext bool) {
	if b.i >= b.dataLen {
		return nil, false
	}
	if nulls.Contains(b.nsp, uint64(b.i)) {
		data, hasNext = nil, true
	} else {
		data, hasNext = b.data[b.i:b.i+b.stride], true
	}
	b.i += b.stride
	return
}

func (b *fixedBytes) Foreach(f func([]byte)) {
	if nulls.Any(b.nsp) {
		for ; b.i < b.dataLen; b.i += b.stride {
			if !nulls.Contains(b.nsp, uint64(b.i)) {
				f(b.data[b.i : b.i+b.stride])
			}
		}
	} else {
		for ; b.i < b.dataLen; b.i += b.stride {
			f(b.data[b.i : b.i+b.stride])
		}
	}
}

func (b *fixedBytes) Nth(i int64) []byte {
	if nulls.Contains(b.nsp, uint64(i)) {
		return nil
	}
	s, e := i*int64(b.stride), (i+1)*int64(b.stride)
	return b.data[s:e]
}

type varBytes struct {
	data       *types.Bytes
	nsp        *nulls.Nulls
	dataLen, i int
}

func newVarBytes(vec *vector.Vector) *varBytes {
	data := vec.Col.(*types.Bytes)
	return &varBytes{
		data:    data,
		dataLen: len(data.Lengths),
		nsp:     vec.Nsp,
		i:       0,
	}
}

func (b *varBytes) NextBytes() (data []byte, hasNext bool) {
	if b.i >= b.dataLen {
		return nil, false
	}
	if nulls.Contains(b.nsp, uint64(b.i)) {
		data, hasNext = nil, true
	} else {
		data, hasNext = b.data.Get(int64(b.i)), true
	}
	b.i += 1
	return
}

func (b *varBytes) Foreach(f func([]byte)) {
	if nulls.Any(b.nsp) {
		for ; b.i < b.dataLen; b.i += 1 {
			if !nulls.Contains(b.nsp, uint64(b.i)) {
				f(b.data.Get(int64(b.i)))
			}
		}
	} else {
		for ; b.i < b.dataLen; b.i += 1 {
			f(b.data.Get(int64(b.i)))
		}
	}
}

func (b *varBytes) Nth(i int64) []byte {
	if nulls.Contains(b.nsp, uint64(i)) {
		return nil
	}
	return b.data.Get(i)
}

func newBytesIterSolo(vec *vector.Vector) bytesIter {
	var data []byte
	var stride int
	switch vec.Typ.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		return newVarBytes(vec)
	case types.T_int8:
		data, stride = encoding.EncodeInt8Slice(vec.Col.([]int8)), 1
	case types.T_uint8:
		data, stride = encoding.EncodeUint8Slice(vec.Col.([]uint8)), 1
	case types.T_int16:
		data, stride = encoding.EncodeInt16Slice(vec.Col.([]int16)), 2
	case types.T_uint16:
		data, stride = encoding.EncodeUint16Slice(vec.Col.([]uint16)), 2
	case types.T_int32:
		data, stride = encoding.EncodeInt32Slice(vec.Col.([]int32)), 4
	case types.T_uint32:
		data, stride = encoding.EncodeUint32Slice(vec.Col.([]uint32)), 4
	case types.T_float32:
		data, stride = encoding.EncodeFloat32Slice(vec.Col.([]float32)), 4
	case types.T_int64:
		data, stride = encoding.EncodeInt64Slice(vec.Col.([]int64)), 8
	case types.T_uint64:
		data, stride = encoding.EncodeUint64Slice(vec.Col.([]uint64)), 8
	case types.T_float64:
		data, stride = encoding.EncodeFloat64Slice(vec.Col.([]float64)), 8
	case types.T_date:
		data, stride = encoding.EncodeDateSlice(vec.Col.([]types.Date)), encoding.DateSize
	case types.T_datetime:
		data, stride = encoding.EncodeDatetimeSlice(vec.Col.([]types.Datetime)), encoding.DatetimeSize
	case types.T_decimal128:
		data, stride = encoding.EncodeDecimal128Slice(vec.Col.([]types.Decimal128)), encoding.Decimal128Size
	}
	if data == nil {
		panic(fmt.Sprintf("not support for type %s", vec.Typ.Oid))
	}
	return newFixedBytes(data, vec.Nsp, stride)
}

// A combinator of multiple bytes_iters.
// Might be used in approx_count_distinct(col1, col2)
//
// func newBytesIter(vecs []*vector.Vector) bytesIter {
// 	iters := make([]bytesIter, len(vecs))
// 	for i, v := range vecs {
// 		iters[i] = newBytesIterSolo(v)
// 	}
// 	if len(vecs) == 1 {
// 		return iters[0]
// 	}

// 	return &multiBytes{iters}
// }

// type multiBytes struct {
// 	iters []bytesIter
// }

// func (b *multiBytes) NextBytes() (data []byte, hasNext bool) {
// 	panic("use foreach instead")
// }

// func (b *multiBytes) Foreach(f func([]byte)) {
// 	var buf bytes.Buffer
// 	var d []byte
// 	var has_next bool
// 	for {
// 		for _, iter := range b.iters {
// 			if d, has_next = iter.NextBytes(); !has_next {
// 				return
// 			}
// 			if d != nil {
// 				buf.Write(d)
// 			}
// 		}
// 		f(buf.Bytes())
// 		buf.Reset()
// 	}
// }

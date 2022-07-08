// Copyright 2021 - 2022 Matrix Origin
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

package compare

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func New(typ types.Type, desc bool) Compare {
	switch typ.Oid {
	case types.T_bool:
		if desc {
			return newCompare(boolDescCompare[types.Bool], boolCopy[types.Bool])
		}
		return newCompare(boolCompare[types.Bool], boolCopy[types.Bool])
	case types.T_int8:
		if desc {
			return newCompare(genericDescCompare[types.Int8], genericCopy[types.Int8])
		}
		return newCompare(genericCompare[types.Int8], genericCopy[types.Int8])
	case types.T_int16:
		if desc {
			return newCompare(genericDescCompare[types.Int16], genericCopy[types.Int16])
		}
		return newCompare(genericCompare[types.Int16], genericCopy[types.Int16])
	case types.T_int32:
		if desc {
			return newCompare(genericDescCompare[types.Int32], genericCopy[types.Int32])
		}
		return newCompare(genericCompare[types.Int32], genericCopy[types.Int32])
	case types.T_int64:
		if desc {
			return newCompare(genericDescCompare[types.Int64], genericCopy[types.Int64])
		}
		return newCompare(genericCompare[types.Int64], genericCopy[types.Int64])
	case types.T_uint8:
		if desc {
			return newCompare(genericDescCompare[types.UInt8], genericCopy[types.UInt8])
		}
		return newCompare(genericCompare[types.UInt8], genericCopy[types.UInt8])
	case types.T_uint16:
		if desc {
			return newCompare(genericDescCompare[types.UInt16], genericCopy[types.UInt16])
		}
		return newCompare(genericCompare[types.UInt16], genericCopy[types.UInt16])
	case types.T_uint32:
		if desc {
			return newCompare(genericDescCompare[types.UInt32], genericCopy[types.UInt32])
		}
		return newCompare(genericCompare[types.UInt32], genericCopy[types.UInt32])
	case types.T_uint64:
		if desc {
			return newCompare(genericDescCompare[types.UInt64], genericCopy[types.UInt64])
		}
		return newCompare(genericCompare[types.UInt64], genericCopy[types.UInt64])
	case types.T_float32:
		if desc {
			return newCompare(genericDescCompare[types.Float32], genericCopy[types.Float32])
		}
		return newCompare(genericCompare[types.Float32], genericCopy[types.Float32])
	case types.T_float64:
		if desc {
			return newCompare(genericDescCompare[types.Float64], genericCopy[types.Float64])
		}
		return newCompare(genericCompare[types.Float64], genericCopy[types.Float64])
	case types.T_date:
		if desc {
			return newCompare(genericDescCompare[types.Date], genericCopy[types.Date])
		}
		return newCompare(genericCompare[types.Date], genericCopy[types.Date])
	case types.T_datetime:
		if desc {
			return newCompare(genericDescCompare[types.Datetime], genericCopy[types.Datetime])
		}
		return newCompare(genericCompare[types.Datetime], genericCopy[types.Datetime])
	case types.T_timestamp:
		if desc {
			return newCompare(genericDescCompare[types.Timestamp], genericCopy[types.Timestamp])
		}
		return newCompare(genericCompare[types.Timestamp], genericCopy[types.Timestamp])
	case types.T_decimal64:
		if desc {
			return newCompare(genericDescCompare[types.Decimal64], genericCopy[types.Decimal64])
		}
		return newCompare(genericCompare[types.Decimal64], genericCopy[types.Decimal64])
	case types.T_decimal128:
		if desc {
			return newCompare(decimal128DescCompare[types.Decimal128], decimal128Copy[types.Decimal128])
		}
		return newCompare(decimal128Compare[types.Decimal128], decimal128Copy[types.Decimal128])
	case types.T_char, types.T_varchar:
		if desc {
			return newCompare(stringDescCompare[types.String], stringCopy[types.String])
		}
		return newCompare(stringCompare[types.String], stringCopy[types.String])
	}
	return nil
}

func boolCompare[T types.Bool](x, y T) int {
	if x == y {
		return 0
	}
	if !x && y {
		return -1
	}
	return 1
}

func stringCompare[T types.String](x, y T) int {
	return bytes.Compare(x, y)
}

func decimal128Compare[T types.Decimal128](x, y T) int {
	return int(types.CompareDecimal128Decimal128Aligned(types.Decimal128(x), types.Decimal128(y)))
}

func genericCompare[T types.Generic](x, y T) int {
	if x == y {
		return 0
	}
	if x < y {
		return -1
	}
	return 1
}

func boolDescCompare[T types.Bool](x, y T) int {
	if x == y {
		return 0
	}
	if !x && y {
		return 1
	}
	return -1
}

func stringDescCompare[T types.String](x, y T) int {
	return bytes.Compare(x, y) * -1
}

func decimal128DescCompare[T types.Decimal128](x, y T) int {
	return int(types.CompareDecimal128Decimal128Aligned(types.Decimal128(x), types.Decimal128(y))) * -1
}

func genericDescCompare[T types.Generic](x, y T) int {
	if x == y {
		return 0
	}
	if x < y {
		return 1
	}
	return -1
}

func boolCopy[T types.Bool](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func stringCopy[T types.String](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = append(vecDst[dst][:0], vecSrc[src]...)
}

func decimal128Copy[T types.Decimal128](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func genericCopy[T types.Generic](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func newCompare[T types.All](cmp func(T, T) int, cpy func([]T, []T, int64, int64)) *compare[T] {
	return &compare[T]{
		cmp: cmp,
		cpy: cpy,
		xs:  make([][]T, 2),
		ns:  make([]*nulls.Nulls, 2),
		vs:  make([]vector.AnyVector, 2),
	}
}

func (c *compare[T]) Vector() vector.AnyVector {
	return c.vs[0]
}

func (c *compare[T]) Set(idx int, vec vector.AnyVector) {
	switch vec.Type().Oid {
	case types.T_bool:
		v := (any)(vec).(*vector.Vector[types.Bool])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_int8:
		v := (any)(vec).(*vector.Vector[types.Int8])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_int16:
		v := (any)(vec).(*vector.Vector[types.Int16])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_int32:
		v := (any)(vec).(*vector.Vector[types.Int32])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_int64:
		v := (any)(vec).(*vector.Vector[types.Int64])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_uint8:
		v := (any)(vec).(*vector.Vector[types.UInt8])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_uint16:
		v := (any)(vec).(*vector.Vector[types.UInt16])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_uint32:
		v := (any)(vec).(*vector.Vector[types.UInt32])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_uint64:
		v := (any)(vec).(*vector.Vector[types.UInt64])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_float32:
		v := (any)(vec).(*vector.Vector[types.Float32])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_float64:
		v := (any)(vec).(*vector.Vector[types.Float64])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_date:
		v := (any)(vec).(*vector.Vector[types.Date])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_datetime:
		v := (any)(vec).(*vector.Vector[types.Datetime])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_timestamp:
		v := (any)(vec).(*vector.Vector[types.Timestamp])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_decimal64:
		v := (any)(vec).(*vector.Vector[types.Decimal64])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_decimal128:
		v := (any)(vec).(*vector.Vector[types.Decimal128])
		c.xs[idx] = (any)(v.Col).([]T)
	case types.T_char, types.T_varchar:
		v := (any)(vec).(*vector.Vector[types.String])
		c.xs[idx] = (any)(v.Col).([]T)
	}
	c.vs[idx] = vec
	c.ns[idx] = vec.Nulls()
}

func (c *compare[T]) Compare(veci, vecj int, vi, vj int64) int {
	return c.cmp(c.xs[veci][vi], c.xs[vecj][vj])
}

func (c *compare[T]) Copy(vecSrc, vecDst int, src, dst int64) {
	if nulls.Contains(c.ns[vecSrc], uint64(src)) {
		nulls.Add(c.ns[vecDst], uint64(dst))
	} else {
		nulls.Del(c.ns[vecDst], uint64(dst))
		c.cpy(c.xs[vecDst], c.xs[vecSrc], dst, src)
	}
}

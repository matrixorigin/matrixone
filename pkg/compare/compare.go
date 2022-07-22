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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(typ types.Type, desc bool) Compare {
	switch typ.Oid {
	case types.T_bool:
		if desc {
			return newCompare(boolDescCompare[bool], boolCopy[bool])
		}
		return newCompare(boolCompare[bool], boolCopy[bool])
	case types.T_int8:
		if desc {
			return newCompare(genericDescCompare[int8], genericCopy[int8])
		}
		return newCompare(genericCompare[int8], genericCopy[int8])
	case types.T_int16:
		if desc {
			return newCompare(genericDescCompare[int16], genericCopy[int16])
		}
		return newCompare(genericCompare[int16], genericCopy[int16])
	case types.T_int32:
		if desc {
			return newCompare(genericDescCompare[int32], genericCopy[int32])
		}
		return newCompare(genericCompare[int32], genericCopy[int32])
	case types.T_int64:
		if desc {
			return newCompare(genericDescCompare[int64], genericCopy[int64])
		}
		return newCompare(genericCompare[int64], genericCopy[int64])
	case types.T_uint8:
		if desc {
			return newCompare(genericDescCompare[uint8], genericCopy[uint8])
		}
		return newCompare(genericCompare[uint8], genericCopy[uint8])
	case types.T_uint16:
		if desc {
			return newCompare(genericDescCompare[uint16], genericCopy[uint16])
		}
		return newCompare(genericCompare[uint16], genericCopy[uint16])
	case types.T_uint32:
		if desc {
			return newCompare(genericDescCompare[uint32], genericCopy[uint32])
		}
		return newCompare(genericCompare[uint32], genericCopy[uint32])
	case types.T_uint64:
		if desc {
			return newCompare(genericDescCompare[uint64], genericCopy[uint64])
		}
		return newCompare(genericCompare[uint64], genericCopy[uint64])
	case types.T_float32:
		if desc {
			return newCompare(genericDescCompare[float32], genericCopy[float32])
		}
		return newCompare(genericCompare[float32], genericCopy[float32])
	case types.T_float64:
		if desc {
			return newCompare(genericDescCompare[float64], genericCopy[float64])
		}
		return newCompare(genericCompare[float64], genericCopy[float64])
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
			return newCompare(decimal64DescCompare, decimal64Copy)
		}
		return newCompare(decimal64Compare, decimal64Copy)
	case types.T_decimal128:
		if desc {
			return newCompare(decimal128DescCompare, decimal128Copy)
		}
		return newCompare(decimal128Compare, decimal128Copy)
	case types.T_char, types.T_varchar:
		return &strCompare{
			desc: desc,
			vs:   make([]*vector.Vector, 2),
		}
	}
	return nil
}

func boolCompare[T bool](x, y T) int {
	if x == y {
		return 0
	}
	if !x && y {
		return -1
	}
	return 1
}

func decimal64Compare(x, y types.Decimal64) int {
	return x.Compare(y)
}

func decimal128Compare(x, y types.Decimal128) int {
	return x.Compare(y)
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

func boolDescCompare[T bool](x, y T) int {
	if x == y {
		return 0
	}
	if !x && y {
		return 1
	}
	return -1
}

func decimal64DescCompare(x, y types.Decimal64) int {
	return -x.Compare(y)
}
func decimal128DescCompare(x, y types.Decimal128) int {
	return -x.Compare(y)
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

func boolCopy[T bool](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func decimal64Copy(vecDst, vecSrc []types.Decimal64, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func decimal128Copy(vecDst, vecSrc []types.Decimal128, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func genericCopy[T types.Generic](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func newCompare[T any](cmp func(T, T) int, cpy func([]T, []T, int64, int64)) *compare[T] {
	return &compare[T]{
		cmp: cmp,
		cpy: cpy,
		xs:  make([][]T, 2),
		ns:  make([]*nulls.Nulls, 2),
		vs:  make([]*vector.Vector, 2),
	}
}

func (c *compare[T]) Vector() *vector.Vector {
	return c.vs[0]
}

func (c *compare[T]) Set(idx int, vec *vector.Vector) {
	c.vs[idx] = vec
	c.ns[idx] = vec.Nsp
	c.xs[idx] = vec.Col.([]T)
}

func (c *compare[T]) Compare(veci, vecj int, vi, vj int64) int {
	return c.cmp(c.xs[veci][vi], c.xs[vecj][vj])
}

func (c *compare[T]) Copy(vecSrc, vecDst int, src, dst int64, _ *process.Process) error {
	if nulls.Contains(c.ns[vecSrc], uint64(src)) {
		nulls.Add(c.ns[vecDst], uint64(dst))
	} else {
		nulls.Del(c.ns[vecDst], uint64(dst))
		c.cpy(c.xs[vecDst], c.xs[vecSrc], dst, src)
	}
	return nil
}

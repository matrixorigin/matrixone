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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(typ types.Type, desc, nullsLast bool) Compare {
	switch typ.Oid {
	case types.T_bool:
		if desc {
			return newCompare(boolDescCompare, boolCopy[bool], nullsLast)
		}
		return newCompare(boolAscCompare, boolCopy[bool], nullsLast)
	case types.T_int8:
		if desc {
			return newCompare(genericDescCompare[int8], genericCopy[int8], nullsLast)
		}
		return newCompare(genericAscCompare[int8], genericCopy[int8], nullsLast)
	case types.T_int16:
		if desc {
			return newCompare(genericDescCompare[int16], genericCopy[int16], nullsLast)
		}
		return newCompare(genericAscCompare[int16], genericCopy[int16], nullsLast)
	case types.T_int32:
		if desc {
			return newCompare(genericDescCompare[int32], genericCopy[int32], nullsLast)
		}
		return newCompare(genericAscCompare[int32], genericCopy[int32], nullsLast)
	case types.T_int64:
		if desc {
			return newCompare(genericDescCompare[int64], genericCopy[int64], nullsLast)
		}
		return newCompare(genericAscCompare[int64], genericCopy[int64], nullsLast)
	case types.T_uint8:
		if desc {
			return newCompare(genericDescCompare[uint8], genericCopy[uint8], nullsLast)
		}
		return newCompare(genericAscCompare[uint8], genericCopy[uint8], nullsLast)
	case types.T_uint16:
		if desc {
			return newCompare(genericDescCompare[uint16], genericCopy[uint16], nullsLast)
		}
		return newCompare(genericAscCompare[uint16], genericCopy[uint16], nullsLast)
	case types.T_uint32:
		if desc {
			return newCompare(genericDescCompare[uint32], genericCopy[uint32], nullsLast)
		}
		return newCompare(genericAscCompare[uint32], genericCopy[uint32], nullsLast)
	case types.T_uint64:
		if desc {
			return newCompare(genericDescCompare[uint64], genericCopy[uint64], nullsLast)
		}
		return newCompare(genericAscCompare[uint64], genericCopy[uint64], nullsLast)
	case types.T_float32:
		if desc {
			return newCompare(genericDescCompare[float32], genericCopy[float32], nullsLast)
		}
		return newCompare(genericAscCompare[float32], genericCopy[float32], nullsLast)
	case types.T_float64:
		if desc {
			return newCompare(genericDescCompare[float64], genericCopy[float64], nullsLast)
		}
		return newCompare(genericAscCompare[float64], genericCopy[float64], nullsLast)
	case types.T_date:
		if desc {
			return newCompare(genericDescCompare[types.Date], genericCopy[types.Date], nullsLast)
		}
		return newCompare(genericAscCompare[types.Date], genericCopy[types.Date], nullsLast)
	case types.T_datetime:
		if desc {
			return newCompare(genericDescCompare[types.Datetime], genericCopy[types.Datetime], nullsLast)
		}
		return newCompare(genericAscCompare[types.Datetime], genericCopy[types.Datetime], nullsLast)
	case types.T_timestamp:
		if desc {
			return newCompare(genericDescCompare[types.Timestamp], genericCopy[types.Timestamp], nullsLast)
		}
		return newCompare(genericAscCompare[types.Timestamp], genericCopy[types.Timestamp], nullsLast)
	case types.T_decimal64:
		if desc {
			return newCompare(decimal64DescCompare, decimal64Copy, nullsLast)
		}
		return newCompare(decimal64AscCompare, decimal64Copy, nullsLast)
	case types.T_decimal128:
		if desc {
			return newCompare(decimal128DescCompare, decimal128Copy, nullsLast)
		}
		return newCompare(decimal128AscCompare, decimal128Copy, nullsLast)
	case types.T_TS:
		if desc {
			return newCompare(txntsDescCompare, txntsCopy, nullsLast)
		}
		return newCompare(txntsAscCompare, txntsCopy, nullsLast)
	case types.T_Rowid:
		if desc {
			return newCompare(rowidDescCompare, rowidCopy, nullsLast)
		}
		return newCompare(rowidAscCompare, rowidCopy, nullsLast)

	case types.T_char, types.T_varchar, types.T_blob, types.T_json:
		return &strCompare{
			desc:      desc,
			nullsLast: nullsLast,
			vs:        make([]*vector.Vector, 2),
		}
	}
	return nil
}

func boolAscCompare(x, y bool) int {
	if x == y {
		return 0
	}
	if !x && y {
		return -1
	}
	return 1
}

func decimal64AscCompare(x, y types.Decimal64) int {
	return x.Compare(y)
}
func decimal128AscCompare(x, y types.Decimal128) int {
	return x.Compare(y)
}
func txntsAscCompare(x, y types.TS) int {
	return bytes.Compare(x[:], y[:])
}
func rowidAscCompare(x, y types.Rowid) int {
	return bytes.Compare(x[:], x[:])
}

func genericAscCompare[T types.OrderedT](x, y T) int {
	if x == y {
		return 0
	}
	if x < y {
		return -1
	}
	return 1
}

func boolDescCompare(x, y bool) int {
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

func txntsDescCompare(x, y types.TS) int {
	return bytes.Compare(y[:], x[:])
}
func rowidDescCompare(x, y types.Rowid) int {
	return bytes.Compare(y[:], x[:])
}

func genericDescCompare[T types.OrderedT](x, y T) int {
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

func txntsCopy(vecDst, vecSrc []types.TS, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}
func rowidCopy(vecDst, vecSrc []types.Rowid, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func genericCopy[T types.OrderedT](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func newCompare[T any](cmp func(T, T) int, cpy func([]T, []T, int64, int64), nullsLast bool) *compare[T] {
	return &compare[T]{
		cmp:       cmp,
		cpy:       cpy,
		xs:        make([][]T, 2),
		ns:        make([]*nulls.Nulls, 2),
		vs:        make([]*vector.Vector, 2),
		nullsLast: nullsLast,
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
	cmp := nullsCompare(c.ns[veci], c.ns[vecj], vi, vj, c.nullsLast)
	if cmp != 0 {
		return cmp
	}
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

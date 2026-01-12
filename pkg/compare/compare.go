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

func New(typ types.Type, desc, nullsLast bool) Compare {
	switch typ.Oid {
	case types.T_bool:
		if desc {
			return newCompare(types.BoolDescCompare, boolCopy[bool], nullsLast)
		}
		return newCompare(types.BoolAscCompare, boolCopy[bool], nullsLast)
	case types.T_bit:
		if desc {
			return newCompare(types.GenericDescCompare[uint64], genericCopy[uint64], nullsLast)
		}
		return newCompare(types.GenericAscCompare[uint64], genericCopy[uint64], nullsLast)
	case types.T_int8:
		if desc {
			return newCompare(types.GenericDescCompare[int8], genericCopy[int8], nullsLast)
		}
		return newCompare(types.GenericAscCompare[int8], genericCopy[int8], nullsLast)
	case types.T_int16:
		if desc {
			return newCompare(types.GenericDescCompare[int16], genericCopy[int16], nullsLast)
		}
		return newCompare(types.GenericAscCompare[int16], genericCopy[int16], nullsLast)
	case types.T_int32:
		if desc {
			return newCompare(types.GenericDescCompare[int32], genericCopy[int32], nullsLast)
		}
		return newCompare(types.GenericAscCompare[int32], genericCopy[int32], nullsLast)
	case types.T_int64:
		if desc {
			return newCompare(types.GenericDescCompare[int64], genericCopy[int64], nullsLast)
		}
		return newCompare(types.GenericAscCompare[int64], genericCopy[int64], nullsLast)
	case types.T_uint8:
		if desc {
			return newCompare(types.GenericDescCompare[uint8], genericCopy[uint8], nullsLast)
		}
		return newCompare(types.GenericAscCompare[uint8], genericCopy[uint8], nullsLast)
	case types.T_uint16:
		if desc {
			return newCompare(types.GenericDescCompare[uint16], genericCopy[uint16], nullsLast)
		}
		return newCompare(types.GenericAscCompare[uint16], genericCopy[uint16], nullsLast)
	case types.T_uint32:
		if desc {
			return newCompare(types.GenericDescCompare[uint32], genericCopy[uint32], nullsLast)
		}
		return newCompare(types.GenericAscCompare[uint32], genericCopy[uint32], nullsLast)
	case types.T_uint64:
		if desc {
			return newCompare(types.GenericDescCompare[uint64], genericCopy[uint64], nullsLast)
		}
		return newCompare(types.GenericAscCompare[uint64], genericCopy[uint64], nullsLast)
	case types.T_float32:
		if desc {
			return newCompare(types.GenericDescCompare[float32], genericCopy[float32], nullsLast)
		}
		return newCompare(types.GenericAscCompare[float32], genericCopy[float32], nullsLast)
	case types.T_float64:
		if desc {
			return newCompare(types.GenericDescCompare[float64], genericCopy[float64], nullsLast)
		}
		return newCompare(types.GenericAscCompare[float64], genericCopy[float64], nullsLast)
	case types.T_date:
		if desc {
			return newCompare(types.GenericDescCompare[types.Date], genericCopy[types.Date], nullsLast)
		}
		return newCompare(types.GenericAscCompare[types.Date], genericCopy[types.Date], nullsLast)
	case types.T_datetime:
		if desc {
			return newCompare(types.GenericDescCompare[types.Datetime], genericCopy[types.Datetime], nullsLast)
		}
		return newCompare(types.GenericAscCompare[types.Datetime], genericCopy[types.Datetime], nullsLast)
	case types.T_time:
		if desc {
			return newCompare(types.GenericDescCompare[types.Time], genericCopy[types.Time], nullsLast)
		}
		return newCompare(types.GenericAscCompare[types.Time], genericCopy[types.Time], nullsLast)
	case types.T_timestamp:
		if desc {
			return newCompare(types.GenericDescCompare[types.Timestamp], genericCopy[types.Timestamp], nullsLast)
		}
		return newCompare(types.GenericAscCompare[types.Timestamp], genericCopy[types.Timestamp], nullsLast)
	case types.T_decimal64:
		if desc {
			return newCompare(types.Decimal64DescCompare, decimal64Copy, nullsLast)
		}
		return newCompare(types.Decimal64AscCompare, decimal64Copy, nullsLast)
	case types.T_decimal128:
		if desc {
			return newCompare(types.Decimal128DescCompare, decimal128Copy, nullsLast)
		}
		return newCompare(types.Decimal128AscCompare, decimal128Copy, nullsLast)
	case types.T_TS:
		if desc {
			return newCompare(types.TxntsDescCompare, txntsCopy, nullsLast)
		}
		return newCompare(types.TxntsAscCompare, txntsCopy, nullsLast)
	case types.T_Rowid:
		if desc {
			return newCompare(types.RowidDescCompare, rowidCopy, nullsLast)
		}
		return newCompare(types.RowidAscCompare, rowidCopy, nullsLast)
	case types.T_Blockid:
		if desc {
			return newCompare(types.BlockidDescCompare, blockidCopy, nullsLast)
		}
		return newCompare(types.BlockidAscCompare, blockidCopy, nullsLast)
	case types.T_uuid:
		if desc {
			return newCompare(types.UuidDescCompare, uuidCopy, nullsLast)
		}
		return newCompare(types.UuidAscCompare, uuidCopy, nullsLast)
	case types.T_enum:
		if desc {
			return newCompare(types.GenericDescCompare[types.Enum], genericCopy[types.Enum], nullsLast)
		}
		return newCompare(types.GenericAscCompare[types.Enum], genericCopy[types.Enum], nullsLast)
	case types.T_year:
		if desc {
			return newCompare(types.GenericDescCompare[types.MoYear], genericCopy[types.MoYear], nullsLast)
		}
		return newCompare(types.GenericAscCompare[types.MoYear], genericCopy[types.MoYear], nullsLast)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_json, types.T_text, types.T_datalink:
		return &strCompare{
			desc:        desc,
			nullsLast:   nullsLast,
			vs:          make([]*vector.Vector, 2),
			isConstNull: make([]bool, 2),
		}
	case types.T_array_float32, types.T_array_float64:
		//NOTE: Used by merge_order, merge_top, top agg operators.
		return &arrayCompare{
			desc:        desc,
			nullsLast:   nullsLast,
			vs:          make([]*vector.Vector, 2),
			isConstNull: make([]bool, 2),
		}
	}
	return nil
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

func uuidCopy(vecDst, vecSrc []types.Uuid, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func txntsCopy(vecDst, vecSrc []types.TS, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}
func rowidCopy(vecDst, vecSrc []types.Rowid, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func blockidCopy(vecDst, vecSrc []types.Blockid, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func genericCopy[T types.OrderedT](vecDst, vecSrc []T, dst, src int64) {
	vecDst[dst] = vecSrc[src]
}

func newCompare[T any](cmp func(T, T) int, cpy func([]T, []T, int64, int64), nullsLast bool) *compare[T] {
	return &compare[T]{
		cmp:         cmp,
		cpy:         cpy,
		xs:          make([][]T, 2),
		ns:          make([]*nulls.Nulls, 2),
		gs:          make([]*nulls.Nulls, 2),
		vs:          make([]*vector.Vector, 2),
		isConstNull: make([]bool, 2),
		nullsLast:   nullsLast,
	}
}

func (c *compare[T]) Vector() *vector.Vector {
	return c.vs[0]
}

func (c *compare[T]) Set(idx int, vec *vector.Vector) {
	c.vs[idx] = vec
	c.ns[idx] = vec.GetNulls()
	c.xs[idx] = vector.ExpandFixedCol[T](vec)
	c.isConstNull[idx] = vec.IsConstNull()
	c.gs[idx] = vec.GetGrouping()
}

func (c *compare[T]) Compare(veci, vecj int, vi, vj int64) int {
	n0 := c.isConstNull[veci] || c.ns[veci].Contains(uint64(vi))
	n1 := c.isConstNull[vecj] || c.ns[vecj].Contains(uint64(vj))
	cmp := nullsCompare(n0, n1, c.nullsLast)
	if cmp != 0 {
		return cmp - nullsCompareFlag
	}
	return c.cmp(c.xs[veci][vi], c.xs[vecj][vj])
}

func (c *compare[T]) Copy(vecSrc, vecDst int, src, dst int64, _ *process.Process) error {
	if c.isConstNull[vecSrc] || c.ns[vecSrc].Contains(uint64(src)) {
		nulls.Add(c.ns[vecDst], uint64(dst))
	} else {
		nulls.Del(c.ns[vecDst], uint64(dst))
		c.cpy(c.xs[vecDst], c.xs[vecSrc], dst, src)
	}
	return nil
}

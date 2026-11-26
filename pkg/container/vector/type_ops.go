// Copyright 2026 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Physical dispatch for vectors: which Go element type stores each type id,
// and the operations instantiated for it. One row per type id in
// typeOpsRows replaces a switch over the type id in each operation. Every
// T_ constant must be either a row or listed in typeOpsUnsupported;
// TestTypeOpsCoverEveryOid fails otherwise. See
// docs/design/20261126-type-vft.md.

// typeOps are the operations on a vector of one type id. Every field is set
// for a supported type.
type typeOps struct {
	supported bool
	// varlen reports that elements are Varlena descriptors.
	varlen bool

	getAny         func(v *Vector, i int, deepCopy bool) any
	appendAny      func(v *Vector, val any, mp *mpool.MPool) error // a non-null value
	shrink         func(v *Vector, sels []int64, negate bool)
	shrinkByMask   func(v *Vector, sels *bitmap.Bitmap, negate bool, offset uint64)
	shuffle        func(v *Vector, sels []int64, mp *mpool.MPool) error
	shuffleWithBuf func(v *Vector, sels []int64, buf *[]byte) error
}

// fixedOps are the operations for a type id whose elements are T.
func fixedOps[T types.FixedSizeT]() typeOps {
	return typeOps{
		supported: true,
		getAny: func(v *Vector, i int, _ bool) any {
			return GetFixedAtNoTypeCheck[T](v, i)
		},
		appendAny: func(v *Vector, val any, mp *mpool.MPool) error {
			return appendOneFixed(v, val.(T), false, mp)
		},
		shrink:         shrinkFixed[T],
		shrinkByMask:   shrinkFixedByMask[T],
		shuffle:        shuffleFixedNoTypeCheck[T],
		shuffleWithBuf: shuffleFixedNoTypeCheckWithBuf[T],
	}
}

// varlenOps are the operations for a type id whose elements are Varlena
// descriptors: rows move as descriptors, values are read and appended as
// []byte.
func varlenOps() typeOps {
	ops := fixedOps[types.Varlena]()
	ops.varlen = true
	ops.getAny = func(v *Vector, i int, deepCopy bool) any {
		ret := v.GetBytesAt(i)
		if deepCopy {
			copied := make([]byte, len(ret))
			copy(copied, ret)
			ret = copied
		}
		return ret
	}
	ops.appendAny = func(v *Vector, val any, mp *mpool.MPool) error {
		return appendOneBytes(v, val.([]byte), false, mp)
	}
	return ops
}

// typeOpsRow is one row of the table.
type typeOpsRow struct {
	oid types.T
	ops typeOps
}

// typeOpsRows has one row per supported type id. Adding a type means adding
// its row here (or to typeOpsUnsupported).
var typeOpsRows = []typeOpsRow{
	{types.T_bool, fixedOps[bool]()},
	{types.T_bit, fixedOps[uint64]()},

	{types.T_int8, fixedOps[int8]()},
	{types.T_int16, fixedOps[int16]()},
	{types.T_int32, fixedOps[int32]()},
	{types.T_int64, fixedOps[int64]()},
	{types.T_uint8, fixedOps[uint8]()},
	{types.T_uint16, fixedOps[uint16]()},
	{types.T_uint32, fixedOps[uint32]()},
	{types.T_uint64, fixedOps[uint64]()},

	{types.T_float32, fixedOps[float32]()},
	{types.T_float64, fixedOps[float64]()},
	{types.T_bf16, fixedOps[types.BF16]()},
	{types.T_float16, fixedOps[types.Float16]()},
	{types.T_float8, fixedOps[types.Float8]()},
	{types.T_float4, fixedOps[types.Float4]()},

	{types.T_decimal64, fixedOps[types.Decimal64]()},
	{types.T_decimal128, fixedOps[types.Decimal128]()},
	{types.T_decimal256, fixedOps[types.Decimal256]()},

	{types.T_date, fixedOps[types.Date]()},
	{types.T_time, fixedOps[types.Time]()},
	{types.T_datetime, fixedOps[types.Datetime]()},
	{types.T_timestamp, fixedOps[types.Timestamp]()},
	{types.T_year, fixedOps[types.MoYear]()},

	{types.T_enum, fixedOps[types.Enum]()},
	{types.T_uuid, fixedOps[types.Uuid]()},
	{types.T_TS, fixedOps[types.TS]()},
	{types.T_Rowid, fixedOps[types.Rowid]()},
	{types.T_Blockid, fixedOps[types.Blockid]()},

	{types.T_char, varlenOps()},
	{types.T_varchar, varlenOps()},
	{types.T_binary, varlenOps()},
	{types.T_varbinary, varlenOps()},
	{types.T_json, varlenOps()},
	{types.T_blob, varlenOps()},
	{types.T_text, varlenOps()},
	{types.T_datalink, varlenOps()},
	{types.T_geometry, varlenOps()},
	{types.T_geometry32, varlenOps()},
	{types.T_array_float32, varlenOps()},
	{types.T_array_float64, varlenOps()},
	{types.T_array_bf16, varlenOps()},
	{types.T_array_float16, varlenOps()},
	{types.T_array_int8, varlenOps()},
	{types.T_array_uint8, varlenOps()},
	{types.T_array_float8, varlenOps()},
	{types.T_array_float4, varlenOps()},
}

// typeOpsUnsupported are the type ids no vector operation here supports:
// GetAny returns nil, AppendAny of a non-null value does nothing, and
// Shrink, ShrinkByMask and Shuffle panic (Shrink, ShrinkByMask and Shuffle
// first handle an all-NULL T_any vector themselves).
var typeOpsUnsupported = []types.T{
	types.T_any,
	types.T_star,
	types.T_int128,
	types.T_uint128,
	types.T_interval,
	types.T_Objectid,
	types.T_tuple,
}

// vectorTypeOps holds a row for every type id; types.T is uint8, so indexing
// is never out of range. A type id with no row reads the zero row
// (supported is false).
var vectorTypeOps = buildTypeOps(typeOpsRows)

func buildTypeOps(rows []typeOpsRow) (tbl [256]typeOps) {
	for _, r := range rows {
		if tbl[r.oid].supported {
			panic(fmt.Sprintf("vector: two type-ops rows for %s", r.oid.String()))
		}
		tbl[r.oid] = r.ops
	}
	return
}

// opsOf returns the operations for v's type id; ok is false when it has none.
func opsOf(v *Vector) (*typeOps, bool) {
	ops := &vectorTypeOps[v.typ.Oid]
	return ops, ops.supported
}

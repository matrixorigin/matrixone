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
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	appendAny      func(v *Vector, val any, mp *mpool.MPool) error        // a non-null value
	setAny         func(v *Vector, i int, val any, mp *mpool.MPool) error // a non-null value
	appendRaw      func(v *Vector, raw []byte, isNull bool, mp *mpool.MPool) error
	shrink         func(v *Vector, sels []int64, negate bool)
	shrinkByMask   func(v *Vector, sels *bitmap.Bitmap, negate bool, offset uint64)
	shuffle        func(v *Vector, sels []int64, mp *mpool.MPool) error
	shuffleWithBuf func(v *Vector, sels []int64, buf *[]byte) error

	// rowKey is row i as a comparable map key: equal keys mean equal
	// values. A fixed-size value is itself, a varlena one a string, a copy
	// unless view is set.
	rowKey func(v *Vector, i int, view bool) any
	// valueFromRaw is the value whose raw bytes are raw (a copy for a
	// varlena type); rawFromValue is the raw bytes of a value.
	valueFromRaw func(raw []byte) any
	rawFromValue func(val any) []byte
	// exportFromRaw is the value whose raw bytes are raw in the form code
	// outside the engine works with (see ExportValue).
	exportFromRaw func(raw []byte) any
	// setConst makes v a constant vector of length rows of val.
	setConst func(v *Vector, val any, length int, mp *mpool.MPool) error
	// textAppender resolves, for type t, the function that parses a value's
	// text and appends it (see TextAppender).
	textAppender func(t types.Type, opts types.TextParseOptions) (TextAppendFunc, error)

	// constructors of per-type functions
	unionAll    func(mp *mpool.MPool) func(v, w *Vector) error              // getUnionAllFunction
	appendBytes func(vec *Vector) func([]byte, bool, *mpool.MPool) error    // MakeAppendBytesFunc
	newResult   func(typ types.Type, mp *mpool.MPool) FunctionResultWrapper // NewFunctionResultWrapper
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
		setAny: func(v *Vector, i int, val any, _ *mpool.MPool) error {
			return SetFixedAtNoTypeCheck(v, i, val.(T))
		},
		appendRaw: func(v *Vector, raw []byte, isNull bool, mp *mpool.MPool) error {
			var val T
			if !isNull {
				// a NULL carries no payload: raw may be nil
				val = types.DecodeFixed[T](raw)
			}
			return AppendFixed(v, val, isNull, mp)
		},
		shrink:         shrinkFixed[T],
		shrinkByMask:   shrinkFixedByMask[T],
		shuffle:        shuffleFixedNoTypeCheck[T],
		shuffleWithBuf: shuffleFixedNoTypeCheckWithBuf[T],
		rowKey: func(v *Vector, i int, _ bool) any {
			return GetFixedAtNoTypeCheck[T](v, i)
		},
		valueFromRaw: func(raw []byte) any {
			return types.DecodeFixed[T](raw)
		},
		rawFromValue: func(val any) []byte {
			return types.EncodeFixed(val.(T))
		},
		exportFromRaw: func(raw []byte) any {
			return types.DecodeFixed[T](raw)
		},
		setConst: func(v *Vector, val any, length int, mp *mpool.MPool) error {
			return SetConstFixed(v, val.(T), length, mp)
		},
		textAppender: func(t types.Type, opts types.TextParseOptions) (TextAppendFunc, error) {
			parse, err := types.ParseFunc[T](t, opts)
			if err != nil {
				return nil, err
			}
			return func(v *Vector, text string, mp *mpool.MPool) error {
				val, err := parse(text)
				if err != nil {
					return err
				}
				return AppendFixed(v, val, false, mp)
			}, nil
		},
		unionAll:    unionAllFixed[T],
		appendBytes: appendBytesToFixSized[T],
		newResult: func(typ types.Type, mp *mpool.MPool) FunctionResultWrapper {
			return newResultFunc[T](typ, mp)
		},
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
	ops.setAny = func(v *Vector, i int, val any, mp *mpool.MPool) error {
		return SetBytesAt(v, i, val.([]byte), mp)
	}
	ops.appendRaw = func(v *Vector, raw []byte, isNull bool, mp *mpool.MPool) error {
		return AppendBytes(v, raw, isNull, mp)
	}
	ops.rowKey = func(v *Vector, i int, view bool) any {
		if view {
			return v.UnsafeGetStringAt(i)
		}
		return v.GetStringAt(i)
	}
	ops.valueFromRaw = func(raw []byte) any {
		return slices.Clone(raw)
	}
	ops.rawFromValue = func(val any) []byte {
		return val.([]byte)
	}
	ops.exportFromRaw = func(raw []byte) any {
		return slices.Clone(raw)
	}
	ops.setConst = func(v *Vector, val any, length int, mp *mpool.MPool) error {
		return SetConstBytes(v, val.([]byte), length, mp)
	}
	ops.textAppender = func(t types.Type, opts types.TextParseOptions) (TextAppendFunc, error) {
		parse, err := types.ParseFunc[[]byte](t, opts)
		if err != nil {
			return nil, err
		}
		return func(v *Vector, text string, mp *mpool.MPool) error {
			val, err := parse(text)
			if err != nil {
				return err
			}
			return AppendBytes(v, val, false, mp)
		}, nil
	}
	ops.unionAll = unionAllVarlen
	ops.appendBytes = func(vec *Vector) func([]byte, bool, *mpool.MPool) error {
		return func(v []byte, isNull bool, mp *mpool.MPool) error {
			return AppendBytes(vec, v, isNull, mp)
		}
	}
	// newResult stays newResultFunc[types.Varlena]
	return ops
}

// jsonOps are varlenOps whose exported value is the decoded document.
func jsonOps() typeOps {
	ops := varlenOps()
	ops.exportFromRaw = func(raw []byte) any {
		return types.DecodeJson(slices.Clone(raw))
	}
	return ops
}

// arrayOps are varlenOps for a vector of T elements, whose exported value is
// the []T of the row.
func arrayOps[T types.ArrayElement]() typeOps {
	ops := varlenOps()
	ops.exportFromRaw = func(raw []byte) any {
		return types.BytesToArray[T](slices.Clone(raw))
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
	{types.T_json, jsonOps()},
	{types.T_blob, varlenOps()},
	{types.T_text, varlenOps()},
	{types.T_datalink, varlenOps()},
	{types.T_geometry, varlenOps()},
	{types.T_geometry32, varlenOps()},
	{types.T_array_float32, arrayOps[float32]()},
	{types.T_array_float64, arrayOps[float64]()},
	{types.T_array_bf16, arrayOps[types.BF16]()},
	{types.T_array_float16, arrayOps[types.Float16]()},
	{types.T_array_int8, arrayOps[int8]()},
	{types.T_array_uint8, arrayOps[uint8]()},
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

// SupportsAny reports whether GetAny, AppendAny and SetAny handle values of
// type oid: every type id in the table above.
func SupportsAny(oid types.T) bool {
	return vectorTypeOps[oid].supported
}

// SetAny sets row i of vec to val, which must be the Go type that stores
// vec's type ([]byte for a varlena type). It does not change the row's NULL
// bit. It returns an error for a type SupportsAny rejects, or when vec cannot
// be written.
func SetAny(vec *Vector, i int, val any, mp *mpool.MPool) error {
	ops, ok := opsOf(vec)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("SetAny: unsupported type %s", vec.typ.String())
	}
	return ops.setAny(vec, i, val, mp)
}

// AppendRaw appends a value given as its raw bytes: the value's memory for a
// fixed-size type (types.EncodeFixed), the bytes themselves for a varlena
// type. A NULL needs no payload: raw is ignored when isNull is set, and may be
// nil. It returns an error for a type SupportsAny rejects.
func AppendRaw(vec *Vector, raw []byte, isNull bool, mp *mpool.MPool) error {
	ops, ok := opsOf(vec)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("AppendRaw: unsupported type %s", vec.typ.String())
	}
	return ops.appendRaw(vec, raw, isNull, mp)
}

// RowKey returns row i of vec as a comparable key for a map or set over the
// rows of one type: keys are equal exactly when the values are. It is the
// value itself for a fixed-size type and a copy of the bytes, as a string,
// for a varlena type. It panics for a type SupportsAny rejects.
func RowKey(vec *Vector, i int) any {
	return rowKey(vec, i, false)
}

// RowKeyView is RowKey without the copy: a varlena key refers to vec's
// memory, so it is only for looking up a map, not for storing in one.
func RowKeyView(vec *Vector, i int) any {
	return rowKey(vec, i, true)
}

// RowKeyValue returns the value of key, a RowKey of a row, as GetAny
// returns it.
func RowKeyValue(key any) any {
	if s, ok := key.(string); ok {
		return []byte(s)
	}
	return key
}

func rowKey(vec *Vector, i int, view bool) any {
	ops, ok := opsOf(vec)
	if !ok {
		panic(moerr.NewInternalErrorNoCtxf("RowKey: unsupported type %s", vec.typ.String()))
	}
	return ops.rowKey(vec, i, view)
}

// ValueFromRaw returns the value of type oid whose raw bytes are raw, as
// GetAny would return it: the Go element type for a fixed-size type, a copy
// of raw for a varlena one. It panics for a type SupportsAny rejects.
func ValueFromRaw(oid types.T, raw []byte) any {
	ops := &vectorTypeOps[oid]
	if !ops.supported {
		panic(moerr.NewInternalErrorNoCtxf("ValueFromRaw: unsupported type %s", oid.String()))
	}
	return ops.valueFromRaw(raw)
}

// RawFromValue returns the raw bytes of val, a value of type oid as GetAny
// returns it; ValueFromRaw reverses it. It panics for a type SupportsAny
// rejects.
func RawFromValue(oid types.T, val any) []byte {
	ops := &vectorTypeOps[oid]
	if !ops.supported {
		panic(moerr.NewInternalErrorNoCtxf("RawFromValue: unsupported type %s", oid.String()))
	}
	return ops.rawFromValue(val)
}

// ExportValue returns row i of vec as the value code outside the engine
// (CDC, index maintenance) works with, or nil for a NULL row: what GetAny
// returns with a copy of varlena bytes, except that a json row is its
// decoded bytejson.ByteJson and a numeric vector row its []T
// ([]float32, []types.BF16, ...). types.ValueText and AppendSQLLiteral
// accept these values. It panics for a type SupportsAny rejects.
func ExportValue(vec *Vector, i int) any {
	if vec.IsNull(uint64(i)) {
		return nil
	}
	return ExportValueFromRaw(vec.typ.Oid, vec.GetRawBytesAt(i))
}

// ExportValueFromRaw returns the value of type oid whose raw bytes are raw
// (a zone map bound, for example) in the form ExportValue returns, copied
// out of raw. It panics for a type SupportsAny rejects.
func ExportValueFromRaw(oid types.T, raw []byte) any {
	ops := &vectorTypeOps[oid]
	if !ops.supported {
		panic(moerr.NewInternalErrorNoCtxf("ExportValue: unsupported type %s", oid.String()))
	}
	return ops.exportFromRaw(raw)
}

// FixedValueOfInt returns a value of the fixed-size type oid made from n, for
// test data: the value whose memory is n in little-endian order, cut or
// zero-padded to the type's size. For integer, decimal, temporal and enum
// types that is the value n (for n >= 0, and for a negative n of a signed
// integer); distinct n give distinct values. It panics for a varlena type, or
// for bool and the float types, whose memory n does not fit.
func FixedValueOfInt(oid types.T, n int64) any {
	typ := oid.ToType()
	if typ.IsVarlen() || oid == types.T_bool || oid.IsFloat() {
		panic(moerr.NewInternalErrorNoCtxf("FixedValueOfInt: %s", oid.String()))
	}
	raw := make([]byte, max(typ.TypeSize(), 8))
	binary.LittleEndian.PutUint64(raw, uint64(n))
	return ValueFromRaw(oid, raw[:typ.TypeSize()])
}

// TextAppendFunc parses text, a value of the vector's type, and appends the
// value to v. A parse error is returned with nothing appended.
type TextAppendFunc func(v *Vector, text string, mp *mpool.MPool) error

// TextAppender returns the TextAppendFunc for vectors of type t, resolved
// once (types.ParseFunc), so that appending a row does no type dispatch. It
// returns an error for a type SupportsAny rejects or that has no text form.
func TextAppender(t types.Type, opts types.TextParseOptions) (TextAppendFunc, error) {
	ops := &vectorTypeOps[t.Oid]
	if !ops.supported {
		return nil, moerr.NewNotSupportedNoCtxf("parsing a %s value from text", t.String())
	}
	return ops.textAppender(t, opts)
}

// SetConstValue makes vec a constant vector of length rows of val, a value
// as GetAny returns it. It returns an error for a type SupportsAny rejects.
func SetConstValue(vec *Vector, val any, length int, mp *mpool.MPool) error {
	ops, ok := opsOf(vec)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("SetConstValue: unsupported type %s", vec.typ.String())
	}
	return ops.setConst(vec, val, length, mp)
}

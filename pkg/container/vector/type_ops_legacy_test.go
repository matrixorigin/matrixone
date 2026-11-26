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

// The switch implementations of the functions that moved onto the type-ops
// table (type_ops.go), kept verbatim as the oracle for
// TestTypeOpsMatchLegacy. Delete this file once the table has been the only
// implementation for a release.

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func legacyGetAny(vec *Vector, i int, deepCopy bool) any {
	switch vec.typ.Oid {
	case types.T_bool:
		return GetFixedAtNoTypeCheck[bool](vec, i)
	case types.T_bit:
		return GetFixedAtNoTypeCheck[uint64](vec, i)
	case types.T_int8:
		return GetFixedAtNoTypeCheck[int8](vec, i)
	case types.T_int16:
		return GetFixedAtNoTypeCheck[int16](vec, i)
	case types.T_int32:
		return GetFixedAtNoTypeCheck[int32](vec, i)
	case types.T_int64:
		return GetFixedAtNoTypeCheck[int64](vec, i)
	case types.T_uint8:
		return GetFixedAtNoTypeCheck[uint8](vec, i)
	case types.T_uint16:
		return GetFixedAtNoTypeCheck[uint16](vec, i)
	case types.T_uint32:
		return GetFixedAtNoTypeCheck[uint32](vec, i)
	case types.T_uint64:
		return GetFixedAtNoTypeCheck[uint64](vec, i)
	case types.T_float32:
		return GetFixedAtNoTypeCheck[float32](vec, i)
	case types.T_float64:
		return GetFixedAtNoTypeCheck[float64](vec, i)
	case types.T_bf16:
		return GetFixedAtNoTypeCheck[types.BF16](vec, i)
	case types.T_float16:
		return GetFixedAtNoTypeCheck[types.Float16](vec, i)
	case types.T_float8:
		return GetFixedAtNoTypeCheck[types.Float8](vec, i)
	case types.T_float4:
		return GetFixedAtNoTypeCheck[types.Float4](vec, i)
	case types.T_date:
		return GetFixedAtNoTypeCheck[types.Date](vec, i)
	case types.T_datetime:
		return GetFixedAtNoTypeCheck[types.Datetime](vec, i)
	case types.T_time:
		return GetFixedAtNoTypeCheck[types.Time](vec, i)
	case types.T_timestamp:
		return GetFixedAtNoTypeCheck[types.Timestamp](vec, i)
	case types.T_year:
		return GetFixedAtNoTypeCheck[types.MoYear](vec, i)
	case types.T_enum:
		return GetFixedAtNoTypeCheck[types.Enum](vec, i)
	case types.T_decimal64:
		return GetFixedAtNoTypeCheck[types.Decimal64](vec, i)
	case types.T_decimal128:
		return GetFixedAtNoTypeCheck[types.Decimal128](vec, i)
	case types.T_decimal256:
		return GetFixedAtNoTypeCheck[types.Decimal256](vec, i)
	case types.T_uuid:
		return GetFixedAtNoTypeCheck[types.Uuid](vec, i)
	case types.T_TS:
		return GetFixedAtNoTypeCheck[types.TS](vec, i)
	case types.T_Rowid:
		return GetFixedAtNoTypeCheck[types.Rowid](vec, i)
	case types.T_Blockid:
		return GetFixedAtNoTypeCheck[types.Blockid](vec, i)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4, types.T_datalink, types.T_geometry, types.T_geometry32:
		ret := vec.GetBytesAt(i)
		if deepCopy {
			copied := make([]byte, len(ret))
			copy(copied, ret)
			ret = copied
		}
		return ret
	}
	return nil
}

func legacyAppendAny(vec *Vector, val any, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}

	if isNull {
		return appendOneFixed(vec, 0, true, mp)
	}

	switch vec.typ.Oid {
	case types.T_bool:
		return appendOneFixed(vec, val.(bool), false, mp)
	case types.T_bit:
		return appendOneFixed(vec, val.(uint64), false, mp)
	case types.T_int8:
		return appendOneFixed(vec, val.(int8), false, mp)
	case types.T_int16:
		return appendOneFixed(vec, val.(int16), false, mp)
	case types.T_int32:
		return appendOneFixed(vec, val.(int32), false, mp)
	case types.T_int64:
		return appendOneFixed(vec, val.(int64), false, mp)
	case types.T_uint8:
		return appendOneFixed(vec, val.(uint8), false, mp)
	case types.T_uint16:
		return appendOneFixed(vec, val.(uint16), false, mp)
	case types.T_uint32:
		return appendOneFixed(vec, val.(uint32), false, mp)
	case types.T_uint64:
		return appendOneFixed(vec, val.(uint64), false, mp)
	case types.T_float32:
		return appendOneFixed(vec, val.(float32), false, mp)
	case types.T_float64:
		return appendOneFixed(vec, val.(float64), false, mp)
	case types.T_bf16:
		return appendOneFixed(vec, val.(types.BF16), false, mp)
	case types.T_float16:
		return appendOneFixed(vec, val.(types.Float16), false, mp)
	case types.T_float8:
		return appendOneFixed(vec, val.(types.Float8), false, mp)
	case types.T_float4:
		return appendOneFixed(vec, val.(types.Float4), false, mp)
	case types.T_date:
		return appendOneFixed(vec, val.(types.Date), false, mp)
	case types.T_year:
		return appendOneFixed(vec, val.(types.MoYear), false, mp)
	case types.T_datetime:
		return appendOneFixed(vec, val.(types.Datetime), false, mp)
	case types.T_time:
		return appendOneFixed(vec, val.(types.Time), false, mp)
	case types.T_timestamp:
		return appendOneFixed(vec, val.(types.Timestamp), false, mp)
	case types.T_enum:
		return appendOneFixed(vec, val.(types.Enum), false, mp)
	case types.T_decimal64:
		return appendOneFixed(vec, val.(types.Decimal64), false, mp)
	case types.T_decimal128:
		return appendOneFixed(vec, val.(types.Decimal128), false, mp)
	case types.T_decimal256:
		return appendOneFixed(vec, val.(types.Decimal256), false, mp)
	case types.T_uuid:
		return appendOneFixed(vec, val.(types.Uuid), false, mp)
	case types.T_TS:
		return appendOneFixed(vec, val.(types.TS), false, mp)
	case types.T_Rowid:
		return appendOneFixed(vec, val.(types.Rowid), false, mp)
	case types.T_Blockid:
		return appendOneFixed(vec, val.(types.Blockid), false, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4, types.T_datalink, types.T_geometry, types.T_geometry32:
		return appendOneBytes(vec, val.([]byte), false, mp)
	}
	return nil
}

func legacyShrink(v *Vector, sels []int64, negate bool, mp *mpool.MPool) error {

	if v.IsConst() {
		if v.typ.Oid == types.T_any {
			if err := v.checkMP(mp); err != nil {
				return err
			}
		}
		if negate {
			v.length -= len(sels)
		} else {
			v.length = len(sels)
		}
		return nil
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if v.typ.Oid == types.T_any && (v.length == 0 || v.AllNull()) {
		newLength := len(sels)
		if negate {
			newLength = v.length - len(sels)
		}
		if !negate || len(sels) > 0 {
			nulls.FilterInPlaceOrdered(&v.nsp, sels, negate)
		}
		v.length = newLength
		trimVectorBitmap(&v.nsp, newLength)
		return nil
	}
	if v.typ.Oid == types.T_any {
		return moerr.NewNotSupportedNoCtx("non-NULL ANY selection")
	}
	switch v.typ.Oid {
	case types.T_bool:
		shrinkFixed[bool](v, sels, negate)
	case types.T_bit:
		shrinkFixed[uint64](v, sels, negate)
	case types.T_int8:
		shrinkFixed[int8](v, sels, negate)
	case types.T_int16:
		shrinkFixed[int16](v, sels, negate)
	case types.T_int32:
		shrinkFixed[int32](v, sels, negate)
	case types.T_int64:
		shrinkFixed[int64](v, sels, negate)
	case types.T_uint8:
		shrinkFixed[uint8](v, sels, negate)
	case types.T_uint16:
		shrinkFixed[uint16](v, sels, negate)
	case types.T_uint32:
		shrinkFixed[uint32](v, sels, negate)
	case types.T_uint64:
		shrinkFixed[uint64](v, sels, negate)
	case types.T_float32:
		shrinkFixed[float32](v, sels, negate)
	case types.T_float64:
		shrinkFixed[float64](v, sels, negate)
	case types.T_bf16:
		shrinkFixed[types.BF16](v, sels, negate)
	case types.T_float16:
		shrinkFixed[types.Float16](v, sels, negate)
	case types.T_float8:
		shrinkFixed[types.Float8](v, sels, negate)
	case types.T_float4:
		shrinkFixed[types.Float4](v, sels, negate)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4, types.T_datalink, types.T_geometry, types.T_geometry32:
		// XXX shrink varlena, but did not shrink area.  For our vector, this
		// may well be the right thing.  If want to shrink area as well, we
		// have to copy each varlena value and swizzle pointer.
		shrinkFixed[types.Varlena](v, sels, negate)
	case types.T_date:
		shrinkFixed[types.Date](v, sels, negate)
	case types.T_datetime:
		shrinkFixed[types.Datetime](v, sels, negate)
	case types.T_time:
		shrinkFixed[types.Time](v, sels, negate)
	case types.T_timestamp:
		shrinkFixed[types.Timestamp](v, sels, negate)
	case types.T_year:
		shrinkFixed[types.MoYear](v, sels, negate)
	case types.T_enum:
		shrinkFixed[types.Enum](v, sels, negate)
	case types.T_decimal64:
		shrinkFixed[types.Decimal64](v, sels, negate)
	case types.T_decimal128:
		shrinkFixed[types.Decimal128](v, sels, negate)
	case types.T_decimal256:
		shrinkFixed[types.Decimal256](v, sels, negate)
	case types.T_uuid:
		shrinkFixed[types.Uuid](v, sels, negate)
	case types.T_TS:
		shrinkFixed[types.TS](v, sels, negate)
	case types.T_Rowid:
		shrinkFixed[types.Rowid](v, sels, negate)
	case types.T_Blockid:
		shrinkFixed[types.Blockid](v, sels, negate)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}

	return nil
}

func legacyShrinkByMask(v *Vector, sels *bitmap.Bitmap, negate bool, offset uint64, mp *mpool.MPool) error {
	if negate && sels.Count() == 0 {
		return nil
	}
	if v.IsConst() {
		if v.typ.Oid == types.T_any {
			if err := v.checkMP(mp); err != nil {
				return err
			}
		}
		if negate {
			v.length -= sels.Count()
		} else {
			v.length = sels.Count()
		}
		return nil
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if v.typ.Oid == types.T_any && (v.length == 0 || v.AllNull()) {
		newLength := sels.Count()
		if negate {
			newLength = v.length - newLength
		}
		nulls.FilterByMaskInPlaceWithOffset(&v.nsp, sels, negate, offset)
		v.length = newLength
		trimVectorBitmap(&v.nsp, newLength)
		return nil
	}
	if v.typ.Oid == types.T_any {
		return moerr.NewNotSupportedNoCtx("non-NULL ANY selection")
	}
	switch v.typ.Oid {
	case types.T_bool:
		shrinkFixedByMask[bool](v, sels, negate, offset)
	case types.T_bit:
		shrinkFixedByMask[uint64](v, sels, negate, offset)
	case types.T_int8:
		shrinkFixedByMask[int8](v, sels, negate, offset)
	case types.T_int16:
		shrinkFixedByMask[int16](v, sels, negate, offset)
	case types.T_int32:
		shrinkFixedByMask[int32](v, sels, negate, offset)
	case types.T_int64:
		shrinkFixedByMask[int64](v, sels, negate, offset)
	case types.T_uint8:
		shrinkFixedByMask[uint8](v, sels, negate, offset)
	case types.T_uint16:
		shrinkFixedByMask[uint16](v, sels, negate, offset)
	case types.T_uint32:
		shrinkFixedByMask[uint32](v, sels, negate, offset)
	case types.T_uint64:
		shrinkFixedByMask[uint64](v, sels, negate, offset)
	case types.T_float32:
		shrinkFixedByMask[float32](v, sels, negate, offset)
	case types.T_float64:
		shrinkFixedByMask[float64](v, sels, negate, offset)
	case types.T_bf16:
		shrinkFixedByMask[types.BF16](v, sels, negate, offset)
	case types.T_float16:
		shrinkFixedByMask[types.Float16](v, sels, negate, offset)
	case types.T_float8:
		shrinkFixedByMask[types.Float8](v, sels, negate, offset)
	case types.T_float4:
		shrinkFixedByMask[types.Float4](v, sels, negate, offset)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4, types.T_datalink, types.T_geometry, types.T_geometry32:
		// XXX shrink varlena, but did not shrink area.  For our vector, this
		// may well be the right thing.  If want to shrink area as well, we
		// have to copy each varlena value and swizzle pointer.
		shrinkFixedByMask[types.Varlena](v, sels, negate, offset)
	case types.T_date:
		shrinkFixedByMask[types.Date](v, sels, negate, offset)
	case types.T_datetime:
		shrinkFixedByMask[types.Datetime](v, sels, negate, offset)
	case types.T_time:
		shrinkFixedByMask[types.Time](v, sels, negate, offset)
	case types.T_timestamp:
		shrinkFixedByMask[types.Timestamp](v, sels, negate, offset)
	case types.T_year:
		shrinkFixedByMask[types.MoYear](v, sels, negate, offset)
	case types.T_enum:
		shrinkFixedByMask[types.Enum](v, sels, negate, offset)
	case types.T_decimal64:
		shrinkFixedByMask[types.Decimal64](v, sels, negate, offset)
	case types.T_decimal128:
		shrinkFixedByMask[types.Decimal128](v, sels, negate, offset)
	case types.T_decimal256:
		shrinkFixedByMask[types.Decimal256](v, sels, negate, offset)
	case types.T_uuid:
		shrinkFixedByMask[types.Uuid](v, sels, negate, offset)
	case types.T_TS:
		shrinkFixedByMask[types.TS](v, sels, negate, offset)
	case types.T_Rowid:
		shrinkFixedByMask[types.Rowid](v, sels, negate, offset)
	case types.T_Blockid:
		shrinkFixedByMask[types.Blockid](v, sels, negate, offset)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
	return nil
}

func legacyShuffle(v *Vector, sels []int64, mp *mpool.MPool) (err error) {
	if v.IsConst() {
		if v.typ.Oid == types.T_any {
			if err := v.checkMP(mp); err != nil {
				return err
			}
			v.length = len(sels)
		}
		return nil
	}
	// ANY has no value payload; its row metadata still follows the selection.
	if v.typ.Oid == types.T_any && (v.length == 0 || v.AllNull()) {
		if err := v.checkMP(mp); err != nil {
			return err
		}
		if err := v.remapShuffleBitmaps(sels, mp); err != nil {
			return err
		}
		v.length = len(sels)
		return nil
	}
	if v.typ.Oid == types.T_any {
		return moerr.NewNotSupportedNoCtx("non-NULL ANY selection")
	}
	switch v.typ.Oid {
	case types.T_bool:
		err = shuffleFixedNoTypeCheck[bool](v, sels, mp)
	case types.T_bit:
		err = shuffleFixedNoTypeCheck[uint64](v, sels, mp)
	case types.T_int8:
		err = shuffleFixedNoTypeCheck[int8](v, sels, mp)
	case types.T_int16:
		err = shuffleFixedNoTypeCheck[int16](v, sels, mp)
	case types.T_int32:
		err = shuffleFixedNoTypeCheck[int32](v, sels, mp)
	case types.T_int64:
		err = shuffleFixedNoTypeCheck[int64](v, sels, mp)
	case types.T_uint8:
		err = shuffleFixedNoTypeCheck[uint8](v, sels, mp)
	case types.T_uint16:
		err = shuffleFixedNoTypeCheck[uint16](v, sels, mp)
	case types.T_uint32:
		err = shuffleFixedNoTypeCheck[uint32](v, sels, mp)
	case types.T_uint64:
		err = shuffleFixedNoTypeCheck[uint64](v, sels, mp)
	case types.T_float32:
		err = shuffleFixedNoTypeCheck[float32](v, sels, mp)
	case types.T_float64:
		err = shuffleFixedNoTypeCheck[float64](v, sels, mp)
	case types.T_bf16:
		err = shuffleFixedNoTypeCheck[types.BF16](v, sels, mp)
	case types.T_float16:
		err = shuffleFixedNoTypeCheck[types.Float16](v, sels, mp)
	case types.T_float8:
		err = shuffleFixedNoTypeCheck[types.Float8](v, sels, mp)
	case types.T_float4:
		err = shuffleFixedNoTypeCheck[types.Float4](v, sels, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4, types.T_datalink, types.T_geometry, types.T_geometry32:
		err = shuffleFixedNoTypeCheck[types.Varlena](v, sels, mp)
	case types.T_date:
		err = shuffleFixedNoTypeCheck[types.Date](v, sels, mp)
	case types.T_datetime:
		err = shuffleFixedNoTypeCheck[types.Datetime](v, sels, mp)
	case types.T_time:
		err = shuffleFixedNoTypeCheck[types.Time](v, sels, mp)
	case types.T_timestamp:
		err = shuffleFixedNoTypeCheck[types.Timestamp](v, sels, mp)
	case types.T_year:
		err = shuffleFixedNoTypeCheck[types.MoYear](v, sels, mp)
	case types.T_enum:
		err = shuffleFixedNoTypeCheck[types.Enum](v, sels, mp)
	case types.T_decimal64:
		err = shuffleFixedNoTypeCheck[types.Decimal64](v, sels, mp)
	case types.T_decimal128:
		err = shuffleFixedNoTypeCheck[types.Decimal128](v, sels, mp)
	case types.T_decimal256:
		err = shuffleFixedNoTypeCheck[types.Decimal256](v, sels, mp)
	case types.T_uuid:
		err = shuffleFixedNoTypeCheck[types.Uuid](v, sels, mp)
	case types.T_TS:
		err = shuffleFixedNoTypeCheck[types.TS](v, sels, mp)
	case types.T_Rowid:
		err = shuffleFixedNoTypeCheck[types.Rowid](v, sels, mp)
	case types.T_Blockid:
		err = shuffleFixedNoTypeCheck[types.Blockid](v, sels, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}

	if err != nil {
		return err
	}
	return err
}

func legacyShuffleWithBufData(v *Vector, sels []int64, buf *[]byte) (err error) {
	switch v.typ.Oid {
	case types.T_bool:
		err = shuffleFixedNoTypeCheckWithBuf[bool](v, sels, buf)
	case types.T_bit:
		err = shuffleFixedNoTypeCheckWithBuf[uint64](v, sels, buf)
	case types.T_int8:
		err = shuffleFixedNoTypeCheckWithBuf[int8](v, sels, buf)
	case types.T_int16:
		err = shuffleFixedNoTypeCheckWithBuf[int16](v, sels, buf)
	case types.T_int32:
		err = shuffleFixedNoTypeCheckWithBuf[int32](v, sels, buf)
	case types.T_int64:
		err = shuffleFixedNoTypeCheckWithBuf[int64](v, sels, buf)
	case types.T_uint8:
		err = shuffleFixedNoTypeCheckWithBuf[uint8](v, sels, buf)
	case types.T_uint16:
		err = shuffleFixedNoTypeCheckWithBuf[uint16](v, sels, buf)
	case types.T_uint32:
		err = shuffleFixedNoTypeCheckWithBuf[uint32](v, sels, buf)
	case types.T_uint64:
		err = shuffleFixedNoTypeCheckWithBuf[uint64](v, sels, buf)
	case types.T_float32:
		err = shuffleFixedNoTypeCheckWithBuf[float32](v, sels, buf)
	case types.T_float64:
		err = shuffleFixedNoTypeCheckWithBuf[float64](v, sels, buf)
	case types.T_bf16:
		err = shuffleFixedNoTypeCheckWithBuf[types.BF16](v, sels, buf)
	case types.T_float16:
		err = shuffleFixedNoTypeCheckWithBuf[types.Float16](v, sels, buf)
	case types.T_float8:
		err = shuffleFixedNoTypeCheckWithBuf[types.Float8](v, sels, buf)
	case types.T_float4:
		err = shuffleFixedNoTypeCheckWithBuf[types.Float4](v, sels, buf)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4, types.T_datalink, types.T_geometry, types.T_geometry32:
		err = shuffleFixedNoTypeCheckWithBuf[types.Varlena](v, sels, buf)
	case types.T_date:
		err = shuffleFixedNoTypeCheckWithBuf[types.Date](v, sels, buf)
	case types.T_datetime:
		err = shuffleFixedNoTypeCheckWithBuf[types.Datetime](v, sels, buf)
	case types.T_time:
		err = shuffleFixedNoTypeCheckWithBuf[types.Time](v, sels, buf)
	case types.T_timestamp:
		err = shuffleFixedNoTypeCheckWithBuf[types.Timestamp](v, sels, buf)
	case types.T_year:
		err = shuffleFixedNoTypeCheckWithBuf[types.MoYear](v, sels, buf)
	case types.T_enum:
		err = shuffleFixedNoTypeCheckWithBuf[types.Enum](v, sels, buf)
	case types.T_decimal64:
		err = shuffleFixedNoTypeCheckWithBuf[types.Decimal64](v, sels, buf)
	case types.T_decimal128:
		err = shuffleFixedNoTypeCheckWithBuf[types.Decimal128](v, sels, buf)
	case types.T_decimal256:
		err = shuffleFixedNoTypeCheckWithBuf[types.Decimal256](v, sels, buf)
	case types.T_uuid:
		err = shuffleFixedNoTypeCheckWithBuf[types.Uuid](v, sels, buf)
	case types.T_TS:
		err = shuffleFixedNoTypeCheckWithBuf[types.TS](v, sels, buf)
	case types.T_Rowid:
		err = shuffleFixedNoTypeCheckWithBuf[types.Rowid](v, sels, buf)
	case types.T_Blockid:
		err = shuffleFixedNoTypeCheckWithBuf[types.Blockid](v, sels, buf)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.ShuffleWithBuf", v.typ))
	}

	return err
}

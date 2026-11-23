// Copyright 2022 Matrix Origin
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

package readutil

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func EncodePrimaryKey(v any, packer *types.Packer) []byte {
	packer.Reset()

	switch v := v.(type) {

	case bool:
		packer.EncodeBool(v)

	case int8:
		packer.EncodeInt8(v)

	case int16:
		packer.EncodeInt16(v)

	case int32:
		packer.EncodeInt32(v)

	case int64:
		packer.EncodeInt64(v)

	case uint8:
		packer.EncodeUint8(v)

	case uint16:
		packer.EncodeUint16(v)

	case uint32:
		packer.EncodeUint32(v)

	case uint64:
		packer.EncodeUint64(v)

	case float32:
		packer.EncodeFloat32(v)

	case float64:
		packer.EncodeFloat64(v)

	case types.Date:
		packer.EncodeDate(v)

	case types.Time:
		packer.EncodeTime(v)

	case types.Datetime:
		packer.EncodeDatetime(v)

	case types.Timestamp:
		packer.EncodeTimestamp(v)

	case types.Decimal64:
		packer.EncodeDecimal64(v)

	case types.Decimal128:
		packer.EncodeDecimal128(v)

	case types.Decimal256:
		packer.EncodeDecimal256(v)

	case types.Uuid:
		packer.EncodeStringType(v[:])

	case types.Enum:
		packer.EncodeEnum(v)

	case types.MoYear:
		packer.EncodeMoYear(v)

	case string:
		packer.EncodeStringType([]byte(v))

	case []byte:
		packer.EncodeStringType(v)

	default:
		panic(fmt.Sprintf("unknown type: %T", v))

	}

	return packer.Bytes()
}

func EncodePrimaryKeyVector(vec *vector.Vector, packer *types.Packer) (ret [][]byte) {
	if vec.Length() > 0 || vec.IsConstNull() {
		ret = make([][]byte, 0, vec.Length())
	}
	visitEncodedPrimaryKeyVector(vec, packer, func(key []byte, repeats int) {
		owned := bytes.Clone(key)
		for i := 0; i < repeats; i++ {
			ret = append(ret, owned)
		}
	})
	if len(ret) != vec.Length() {
		panic(fmt.Sprintf("bad result, expecting %v, got %v", vec.Length(), len(ret)))
	}
	return
}
func visitEncodedPrimaryKeyVector(vec *vector.Vector, packer *types.Packer, visit func([]byte, int)) {
	encodePKRowsForTest.Add(uint64(vec.Length()))
	packer.Reset()

	if vec.IsConstNull() {
		visit(nil, vec.Length())
		return
	}
	repeats := 1
	if vec.IsConst() {
		repeats = max(1, vec.Length())
	}

	// typeswitch:partial bf16/float16/float8/float4 and VECTOR columns cannot be in a primary key (rejected at DDL)
	switch vec.GetType().Oid {

	case types.T_bool:
		s := vector.MustFixedColNoTypeCheck[bool](vec)
		for _, v := range s {
			packer.EncodeBool(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_bit:
		s := vector.MustFixedColNoTypeCheck[uint64](vec)
		for _, v := range s {
			packer.EncodeUint64(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_int8:
		s := vector.MustFixedColNoTypeCheck[int8](vec)
		for _, v := range s {
			packer.EncodeInt8(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_int16:
		s := vector.MustFixedColNoTypeCheck[int16](vec)
		for _, v := range s {
			packer.EncodeInt16(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_int32:
		s := vector.MustFixedColNoTypeCheck[int32](vec)
		for _, v := range s {
			packer.EncodeInt32(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_int64:
		s := vector.MustFixedColNoTypeCheck[int64](vec)
		for _, v := range s {
			packer.EncodeInt64(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_uint8:
		s := vector.MustFixedColNoTypeCheck[uint8](vec)
		for _, v := range s {
			packer.EncodeUint8(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_uint16:
		s := vector.MustFixedColNoTypeCheck[uint16](vec)
		for _, v := range s {
			packer.EncodeUint16(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_uint32:
		s := vector.MustFixedColNoTypeCheck[uint32](vec)
		for _, v := range s {
			packer.EncodeUint32(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_uint64:
		s := vector.MustFixedColNoTypeCheck[uint64](vec)
		for _, v := range s {
			packer.EncodeUint64(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_float32:
		s := vector.MustFixedColNoTypeCheck[float32](vec)
		for _, v := range s {
			packer.EncodeFloat32(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_float64:
		s := vector.MustFixedColNoTypeCheck[float64](vec)
		for _, v := range s {
			packer.EncodeFloat64(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_date:
		s := vector.MustFixedColNoTypeCheck[types.Date](vec)
		for _, v := range s {
			packer.EncodeDate(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_time:
		s := vector.MustFixedColNoTypeCheck[types.Time](vec)
		for _, v := range s {
			packer.EncodeTime(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_datetime:
		s := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		for _, v := range s {
			packer.EncodeDatetime(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_timestamp:
		s := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		for _, v := range s {
			packer.EncodeTimestamp(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_enum:
		s := vector.MustFixedColNoTypeCheck[types.Enum](vec)
		for _, v := range s {
			packer.EncodeEnum(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_year:
		s := vector.MustFixedColNoTypeCheck[types.MoYear](vec)
		for _, v := range s {
			packer.EncodeMoYear(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_decimal64:
		s := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		for _, v := range s {
			packer.EncodeDecimal64(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_decimal128:
		s := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		for _, v := range s {
			packer.EncodeDecimal128(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_decimal256:
		s := vector.MustFixedColNoTypeCheck[types.Decimal256](vec)
		for _, v := range s {
			packer.EncodeDecimal256(v)
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_uuid:
		s := vector.MustFixedColNoTypeCheck[types.Uuid](vec)
		for _, v := range s {
			packer.EncodeStringType(v[:])
			visit(packer.GetBuf(), repeats)
			packer.Reset()
		}

	case types.T_json, types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_Rowid,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		for i := 0; i < vec.Length(); i++ {
			packer.EncodeStringType(vec.GetBytesAt(i))
			visit(packer.GetBuf(), 1)
			packer.Reset()
		}
	default:
		panic(fmt.Sprintf("unknown type: %v", vec.GetType().String()))

	}

}

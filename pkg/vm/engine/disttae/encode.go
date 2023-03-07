// Copyright 2023 Matrix Origin
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

package disttae

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func encodePrimaryKeyVector(vec *vector.Vector, pool *mpool.MPool) (ret [][]byte) {

	if vec.IsScalarNull() {
		return make([][]byte, vec.Length())
	}

	packer := types.NewPacker(pool)
	defer packer.FreeMem()

	switch vec.Typ.Oid {

	case types.T_bool:
		s := vector.MustTCols[bool](vec)
		for _, v := range s {
			packer.EncodeBool(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int8:
		s := vector.MustTCols[int8](vec)
		for _, v := range s {
			packer.EncodeInt8(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int16:
		s := vector.MustTCols[int16](vec)
		for _, v := range s {
			packer.EncodeInt16(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int32:
		s := vector.MustTCols[int32](vec)
		for _, v := range s {
			packer.EncodeInt32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int64:
		s := vector.MustTCols[int64](vec)
		for _, v := range s {
			packer.EncodeInt64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint8:
		s := vector.MustTCols[uint8](vec)
		for _, v := range s {
			packer.EncodeUint8(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint16:
		s := vector.MustTCols[uint16](vec)
		for _, v := range s {
			packer.EncodeUint16(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint32:
		s := vector.MustTCols[uint32](vec)
		for _, v := range s {
			packer.EncodeUint32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint64:
		s := vector.MustTCols[uint64](vec)
		for _, v := range s {
			packer.EncodeUint64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_float32:
		s := vector.MustTCols[float32](vec)
		for _, v := range s {
			packer.EncodeFloat32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_float64:
		s := vector.MustTCols[float64](vec)
		for _, v := range s {
			packer.EncodeFloat64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_date:
		s := vector.MustTCols[types.Date](vec)
		for _, v := range s {
			packer.EncodeDate(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_time:
		s := vector.MustTCols[types.Time](vec)
		for _, v := range s {
			packer.EncodeTime(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_datetime:
		s := vector.MustTCols[types.Datetime](vec)
		for _, v := range s {
			packer.EncodeDatetime(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_timestamp:
		s := vector.MustTCols[types.Timestamp](vec)
		for _, v := range s {
			packer.EncodeTimestamp(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_decimal64:
		s := vector.MustTCols[types.Decimal64](vec)
		for _, v := range s {
			packer.EncodeDecimal64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_decimal128:
		s := vector.MustTCols[types.Decimal128](vec)
		for _, v := range s {
			packer.EncodeDecimal128(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uuid:
		s := vector.MustTCols[types.Uuid](vec)
		for _, v := range s {
			packer.EncodeStringType(v[:])
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_json, types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		s := vector.GetStrVectorValues(vec)
		for _, v := range s {
			packer.EncodeStringType([]byte(v))
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	default:
		panic(fmt.Sprintf("unknown type: %v", vec.Typ.String()))

	}

	l := vec.Length()

	if vec.IsConst() {
		for len(ret) < l {
			ret = append(ret, ret[0])
		}
	}

	if len(ret) != l {
		panic(fmt.Sprintf("bad result, expecting %v, got %v", l, len(ret)))
	}

	return
}

func encodePrimaryKey(v any, pool *mpool.MPool) []byte {
	packer := types.NewPacker(pool)
	defer packer.FreeMem()

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

	case types.Uuid:
		packer.EncodeStringType(v[:])

	case string:
		packer.EncodeStringType([]byte(v))

	case []byte:
		packer.EncodeStringType(v)

	default:
		panic(fmt.Sprintf("unknown type: %T", v))

	}

	return packer.Bytes()
}

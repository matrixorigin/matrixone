// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func appendPayloadField(dst []byte, data []byte, isNull bool) []byte {
	if isNull {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	sz := uint32(len(data))
	dst = append(dst, types.EncodeUint32(&sz)...)
	return append(dst, data...)
}

func payloadFieldIterator(payload []byte, fieldCount int, fn func(i int, isNull bool, data []byte) error) error {
	offset := 0
	for i := 0; i < fieldCount; i++ {
		if offset >= len(payload) {
			return moerr.NewInternalErrorNoCtx("invalid agg payload: truncated null flag")
		}
		flag := payload[offset]
		offset++
		if flag == 0 {
			if err := fn(i, true, nil); err != nil {
				return err
			}
			continue
		}
		if offset+4 > len(payload) {
			return moerr.NewInternalErrorNoCtx("invalid agg payload: truncated size")
		}
		sz := int(types.DecodeUint32(payload[offset : offset+4]))
		offset += 4
		if offset+sz > len(payload) {
			return moerr.NewInternalErrorNoCtx("invalid agg payload: truncated field bytes")
		}
		if err := fn(i, false, payload[offset:offset+sz]); err != nil {
			return err
		}
		offset += sz
	}
	if offset != len(payload) {
		return moerr.NewInternalErrorNoCtx("invalid agg payload: trailing bytes")
	}
	return nil
}

func encodeGroupConcatPayload(vectors []*vector.Vector, row int, argTypes []types.Type) ([]byte, error) {
	payload := make([]byte, 0, len(vectors)*8)
	for i, vec := range vectors {
		r := row
		if vec.IsConst() {
			r = 0
		}
		if vec.IsNull(uint64(r)) {
			return nil, nil
		}
		payload = appendPayloadField(payload, groupConcatFieldBytes(vec, r, argTypes[i]), false)
	}
	return payload, nil
}

func groupConcatFieldBytes(vec *vector.Vector, row int, typ types.Type) []byte {
	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink,
		types.T_varbinary, types.T_binary, types.T_json, types.T_enum,
		types.T_array_float32, types.T_array_float64:
		return vec.GetBytesAt(row)
	default:
		return vec.GetRawBytesAt(row)
	}
}

func appendGroupConcatData(dst []byte, typ types.Type, data []byte) ([]byte, error) {
	switch typ.Oid {
	case types.T_bit, types.T_uint64:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[uint64](data)), nil
	case types.T_bool:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[bool](data)), nil
	case types.T_int8:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[int8](data)), nil
	case types.T_int16:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[int16](data)), nil
	case types.T_int32:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[int32](data)), nil
	case types.T_int64:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[int64](data)), nil
	case types.T_uint8:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[uint8](data)), nil
	case types.T_uint16:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[uint16](data)), nil
	case types.T_uint32:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[uint32](data)), nil
	case types.T_float32:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[float32](data)), nil
	case types.T_float64:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[float64](data)), nil
	case types.T_decimal64:
		return fmt.Appendf(dst, "%v", types.DecodeDecimal64(data).Format(typ.Scale)), nil
	case types.T_decimal128:
		return fmt.Appendf(dst, "%v", types.DecodeDecimal128(data).Format(typ.Scale)), nil
	case types.T_date:
		return fmt.Appendf(dst, "%v", util.UnsafeFromBytes[types.Date](data).String()), nil
	case types.T_datetime:
		return fmt.Appendf(dst, "%v", util.UnsafeFromBytes[types.Datetime](data).String()), nil
	case types.T_timestamp:
		return fmt.Appendf(dst, "%v", util.UnsafeFromBytes[types.Timestamp](data).String()), nil
	case types.T_time:
		return fmt.Appendf(dst, "%v", util.UnsafeFromBytes[types.Time](data).String()), nil
	case types.T_blob, types.T_text, types.T_datalink, types.T_varbinary, types.T_binary,
		types.T_char, types.T_varchar, types.T_enum, types.T_array_float32, types.T_array_float64:
		if err := isValidGroupConcatUnit(data); err != nil {
			return nil, err
		}
		return append(dst, data...), nil
	case types.T_json:
		if err := isValidGroupConcatUnit(data); err != nil {
			return nil, err
		}
		return append(dst, types.DecodeJson(data).String()...), nil
	case types.T_interval:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[types.IntervalType](data)), nil
	case types.T_TS:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[types.TS](data)), nil
	case types.T_Rowid:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[types.Rowid](data)), nil
	case types.T_Blockid:
		return fmt.Appendf(dst, "%v", *util.UnsafeFromBytes[types.Blockid](data)), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported type for group_concat payload: %s", typ.String())
	}
}

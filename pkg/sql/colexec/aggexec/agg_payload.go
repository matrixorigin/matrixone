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
	"io"

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
		if flag != 1 {
			return moerr.NewInternalErrorNoCtx("invalid agg payload: invalid null flag")
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

func encodeGroupConcatPayloadWithNulls(
	vectors []*vector.Vector,
	row int,
	argTypes []types.Type,
) ([]byte, error) {
	payload := make([]byte, 0, len(vectors)*8)
	for i, vec := range vectors {
		r := row
		if vec.IsConst() {
			r = 0
		}
		if vec.IsNull(uint64(r)) {
			payload = appendPayloadField(payload, nil, true)
			continue
		}
		payload = appendPayloadField(payload, groupConcatFieldBytes(vec, r, argTypes[i]), false)
	}
	return payload, nil
}

func groupConcatFieldBytes(vec *vector.Vector, row int, typ types.Type) []byte {
	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink,
		types.T_varbinary, types.T_binary, types.T_json,
		types.T_array_float32, types.T_array_float64:
		return vec.GetBytesAt(row)
	default:
		return vec.GetRawBytesAt(row)
	}
}

type appendSliceWriter struct {
	data []byte
}

func (w *appendSliceWriter) Write(value []byte) (int, error) {
	w.data = append(w.data, value...)
	return len(value), nil
}

func appendGroupConcatData(dst []byte, typ types.Type, data []byte) ([]byte, error) {
	writer := appendSliceWriter{data: dst}
	if err := writeGroupConcatData(&writer, typ, data); err != nil {
		return nil, err
	}
	return writer.data, nil
}

func writeGroupConcatData(writer io.Writer, typ types.Type, data []byte) error {
	switch typ.Oid {
	case types.T_bit, types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_TS, types.T_Rowid, types.T_Blockid:
		if len(data) != typ.TypeSize() {
			return moerr.NewInternalErrorNoCtx(
				"invalid group_concat fixed payload size")
		}
	case types.T_interval:
		if len(data) != 1 {
			return moerr.NewInternalErrorNoCtx(
				"invalid group_concat fixed payload size")
		}
	}
	writeValue := func(value any) error {
		_, err := fmt.Fprint(writer, value)
		return err
	}
	writeBytes := func(value []byte) error {
		n, err := writer.Write(value)
		if err == nil && n != len(value) {
			err = io.ErrShortWrite
		}
		return err
	}
	switch typ.Oid {
	case types.T_bit, types.T_uint64:
		return writeValue(*util.UnsafeFromBytes[uint64](data))
	case types.T_bool:
		return writeValue(*util.UnsafeFromBytes[bool](data))
	case types.T_int8:
		return writeValue(*util.UnsafeFromBytes[int8](data))
	case types.T_int16:
		return writeValue(*util.UnsafeFromBytes[int16](data))
	case types.T_int32:
		return writeValue(*util.UnsafeFromBytes[int32](data))
	case types.T_int64:
		return writeValue(*util.UnsafeFromBytes[int64](data))
	case types.T_uint8:
		return writeValue(*util.UnsafeFromBytes[uint8](data))
	case types.T_uint16:
		return writeValue(*util.UnsafeFromBytes[uint16](data))
	case types.T_uint32:
		return writeValue(*util.UnsafeFromBytes[uint32](data))
	case types.T_float32:
		return writeValue(*util.UnsafeFromBytes[float32](data))
	case types.T_float64:
		return writeValue(*util.UnsafeFromBytes[float64](data))
	case types.T_decimal64:
		return writeValue(types.DecodeDecimal64(data).Format(typ.Scale))
	case types.T_decimal128:
		return writeValue(types.DecodeDecimal128(data).Format(typ.Scale))
	case types.T_date:
		return writeValue(util.UnsafeFromBytes[types.Date](data).String())
	case types.T_datetime:
		return writeValue(util.UnsafeFromBytes[types.Datetime](data).String())
	case types.T_timestamp:
		return writeValue(util.UnsafeFromBytes[types.Timestamp](data).String())
	case types.T_time:
		return writeValue(util.UnsafeFromBytes[types.Time](data).String())
	case types.T_blob, types.T_text, types.T_datalink, types.T_varbinary, types.T_binary,
		types.T_char, types.T_varchar, types.T_enum, types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		if err := isValidGroupConcatUnit(data); err != nil {
			return err
		}
		return writeBytes(data)
	case types.T_json:
		if err := isValidGroupConcatUnit(data); err != nil {
			return err
		}
		return writeValue(types.DecodeJson(data).String())
	case types.T_interval:
		return writeValue(*util.UnsafeFromBytes[types.IntervalType](data))
	case types.T_TS:
		return writeValue(*util.UnsafeFromBytes[types.TS](data))
	case types.T_Rowid:
		return writeValue(*util.UnsafeFromBytes[types.Rowid](data))
	case types.T_Blockid:
		return writeValue(*util.UnsafeFromBytes[types.Blockid](data))
	default:
		return moerr.NewInternalErrorNoCtxf(
			"unsupported type for group_concat payload: %s", typ.String())
	}
}

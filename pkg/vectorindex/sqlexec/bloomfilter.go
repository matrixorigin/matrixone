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

package sqlexec

import (
	"context"
	"encoding/hex"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// WaitBloomFilter blocks until it receives a RuntimeFilter_BLOOMFILTER message
// that matches sqlproc.RuntimeFilterSpecs (if any). It returns the raw serialized
// unique join key bytes from the build side.
//
// The caller is responsible for deserializing the bytes and deciding how to use
// them (e.g. exact pk IN filter vs bloom filter based on its own threshold).
func WaitBloomFilter(sqlproc *SqlProcess) ([]byte, error) {
	if sqlproc.Proc == nil {
		return nil, nil
	}

	if len(sqlproc.RuntimeFilterSpecs) == 0 {
		return nil, nil
	}
	spec := sqlproc.RuntimeFilterSpecs[0]
	if !spec.UseBloomFilter {
		return nil, nil
	}

	msgReceiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		sqlproc.Proc.GetMessageBoard(),
	)
	msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, sqlproc.GetContext())
	if err != nil || ctxDone {
		return nil, err
	}

	for i := range msgs {
		m, ok := msgs[i].(message.RuntimeFilterMessage)
		if !ok {
			continue
		}
		if m.Typ != message.RuntimeFilter_BLOOMFILTER {
			continue
		}
		return m.Data, nil
	}

	return nil, nil
}

// BuildExactPkFilter converts a vector of primary key values into a comma-separated
// SQL literal string suitable for use in an IN (...) clause.
func BuildExactPkFilter(ctx context.Context, vec *vector.Vector) (string, error) {
	var buf []byte
	rowCount := vec.Length()
	for i := 0; i < rowCount; i++ {
		if vec.IsNull(uint64(i)) {
			continue
		}
		if len(buf) > 0 {
			buf = append(buf, ',')
		}
		var err error
		buf, err = AppendVectorSQLLiteral(ctx, vec, i, buf)
		if err != nil {
			return "", err
		}
	}
	return string(buf), nil
}

// AppendVectorSQLLiteral appends the SQL literal representation of vec[row] to buf.
func AppendVectorSQLLiteral(ctx context.Context, vec *vector.Vector, row int, buf []byte) ([]byte, error) {
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_bool:
		if vector.GetFixedAtWithTypeCheck[bool](vec, row) {
			buf = append(buf, "true"...)
		} else {
			buf = append(buf, "false"...)
		}
	case types.T_bit:
		value := vector.GetFixedAtWithTypeCheck[uint64](vec, row)
		bitLength := typ.Width
		byteLength := (bitLength + 7) / 8
		b := types.EncodeUint64(&value)[:byteLength]
		slices.Reverse(b)
		buf = append(buf, '\'')
		buf = append(buf, b...)
		buf = append(buf, '\'')
	case types.T_int8:
		value := vector.GetFixedAtWithTypeCheck[int8](vec, row)
		buf = AppendInt64(buf, int64(value))
	case types.T_uint8:
		value := vector.GetFixedAtWithTypeCheck[uint8](vec, row)
		buf = AppendUint64(buf, uint64(value))
	case types.T_int16:
		value := vector.GetFixedAtWithTypeCheck[int16](vec, row)
		buf = AppendInt64(buf, int64(value))
	case types.T_uint16:
		value := vector.GetFixedAtWithTypeCheck[uint16](vec, row)
		buf = AppendUint64(buf, uint64(value))
	case types.T_int32:
		value := vector.GetFixedAtWithTypeCheck[int32](vec, row)
		buf = AppendInt64(buf, int64(value))
	case types.T_uint32:
		value := vector.GetFixedAtWithTypeCheck[uint32](vec, row)
		buf = AppendUint64(buf, uint64(value))
	case types.T_int64:
		value := vector.GetFixedAtWithTypeCheck[int64](vec, row)
		buf = AppendInt64(buf, value)
	case types.T_uint64:
		value := vector.GetFixedAtWithTypeCheck[uint64](vec, row)
		buf = AppendUint64(buf, value)
	case types.T_float32:
		value := vector.GetFixedAtWithTypeCheck[float32](vec, row)
		buf = AppendFloat64(buf, float64(value), 32)
	case types.T_float64:
		value := vector.GetFixedAtWithTypeCheck[float64](vec, row)
		buf = AppendFloat64(buf, value, 64)
	case types.T_binary, types.T_varbinary, types.T_blob:
		buf = AppendHex(buf, vec.GetBytesAt(row))
	case types.T_char,
		types.T_varchar,
		types.T_text,
		types.T_datalink:
		value := string(vec.GetBytesAt(row))
		value = strings.ReplaceAll(value, "\\", "\\\\")
		value = strings.ReplaceAll(value, "'", "\\'")
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_date:
		value := vector.GetFixedAtWithTypeCheck[types.Date](vec, row)
		buf = append(buf, '\'')
		buf = append(buf, value.String()...)
		buf = append(buf, '\'')
	case types.T_datetime:
		scale := typ.Scale
		value := vector.GetFixedAtWithTypeCheck[types.Datetime](vec, row).String2(scale)
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_time:
		scale := typ.Scale
		value := vector.GetFixedAtWithTypeCheck[types.Time](vec, row).String2(scale)
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_timestamp:
		scale := typ.Scale
		value := vector.GetFixedAtWithTypeCheck[types.Timestamp](vec, row).String2(time.UTC, scale)
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_decimal64:
		scale := typ.Scale
		value := vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, row).Format(scale)
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_decimal128:
		scale := typ.Scale
		value := vector.GetFixedAtWithTypeCheck[types.Decimal128](vec, row).Format(scale)
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_uuid:
		value := vector.GetFixedAtWithTypeCheck[types.Uuid](vec, row).String()
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_Rowid:
		value := vector.GetFixedAtWithTypeCheck[types.Rowid](vec, row).String()
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_Blockid:
		value := vector.GetFixedAtWithTypeCheck[types.Blockid](vec, row)
		buf = append(buf, '\'')
		buf = append(buf, (&value).String()...)
		buf = append(buf, '\'')
	case types.T_TS:
		value := vector.GetFixedAtWithTypeCheck[types.TS](vec, row).ToString()
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	case types.T_enum:
		value := vector.GetFixedAtWithTypeCheck[types.Enum](vec, row).String()
		buf = append(buf, '\'')
		buf = append(buf, value...)
		buf = append(buf, '\'')
	default:
		return nil, moerr.NewInternalErrorf(ctx, "ivf_search: unsupported pk type %d", typ.Oid)
	}

	return buf, nil
}

// AppendHex appends hex-encoded binary literal to buf.
func AppendHex(dst []byte, src []byte) []byte {
	dst = append(dst, "x'"...)
	dst = hex.AppendEncode(dst, src)
	dst = append(dst, '\'')
	return dst
}

// AppendInt64 appends an int64 value as decimal text.
func AppendInt64(buf []byte, value int64) []byte {
	return strconv.AppendInt(buf, value, 10)
}

// AppendUint64 appends a uint64 value as decimal text.
func AppendUint64(buf []byte, value uint64) []byte {
	return strconv.AppendUint(buf, value, 10)
}

// AppendFloat64 appends a float64 value, handling infinities.
func AppendFloat64(buf []byte, value float64, bitSize int) []byte {
	if !math.IsInf(value, 0) {
		return strconv.AppendFloat(buf, value, 'f', -1, bitSize)
	}
	if math.IsInf(value, 1) {
		return append(buf, "+Infinity"...)
	}
	return append(buf, "-Infinity"...)
}

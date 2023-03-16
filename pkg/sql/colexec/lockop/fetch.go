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

package lockop

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
)

func getFetchRowsFunc(t types.Type) fetchRowsFunc {
	switch t.Oid {
	case types.T_bool:
		return fetchBoolRows
	case types.T_int8:
		return fetchInt8Rows
	case types.T_int16:
		return fetchInt16Rows
	case types.T_int32:
		return fetchInt32Rows
	case types.T_int64:
		return fetchInt64Rows
	case types.T_uint8:
		return fetchUint8Rows
	case types.T_uint16:
		return fetchUint16Rows
	case types.T_uint32:
		return fetchUint32Rows
	case types.T_uint64:
		return fetchUint64Rows
	case types.T_float32:
		return fetchFloat32Rows
	case types.T_float64:
		return fetchFloat64Rows
	case types.T_date:
		return fetchDateRows
	case types.T_time:
		return fetchTimeRows
	case types.T_datetime:
		return fetchDateTimeRows
	case types.T_timestamp:
		return fetchTimestampRows
	case types.T_decimal64:
		return fetchDecimal64Rows
	case types.T_decimal128:
		return fetchDecimal128Rows
	case types.T_uuid:
		return fetchUUIDRows
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return fetchVarlenaRows
	default:
		panic(fmt.Sprintf("not support for %s", t.String()))
	}
}

func fetchBoolRows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return [][]byte{{0}, {1}},
		lock.Granularity_Range
}

func fetchInt8Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		1,
		func(v int8) []byte {
			parker.Reset()
			parker.EncodeInt8(v)
			return parker.Bytes()
		})
}

func fetchInt16Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		2,
		func(v int16) []byte {
			parker.Reset()
			parker.EncodeInt16(v)
			return parker.Bytes()
		})
}

func fetchInt32Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		4,
		func(v int32) []byte {
			parker.Reset()
			parker.EncodeInt32(v)
			return parker.Bytes()
		})
}

func fetchInt64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		8,
		func(v int64) []byte {
			parker.Reset()
			parker.EncodeInt64(v)
			return parker.Bytes()
		})
}

func fetchUint8Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		1,
		func(v uint8) []byte {
			parker.Reset()
			parker.EncodeUint8(v)
			return parker.Bytes()
		})
}

func fetchUint16Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		2,
		func(v uint16) []byte {
			parker.Reset()
			parker.EncodeUint16(v)
			return parker.Bytes()
		})
}

func fetchUint32Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		4,
		func(v uint32) []byte {
			parker.Reset()
			parker.EncodeUint32(v)
			return parker.Bytes()
		})
}

func fetchUint64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		8,
		func(v uint64) []byte {
			parker.Reset()
			parker.EncodeUint64(v)
			return parker.Bytes()
		})
}

func fetchFloat32Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		4,
		func(v float32) []byte {
			parker.Reset()
			parker.EncodeFloat32(v)
			return parker.Bytes()
		})
}

func fetchFloat64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		8,
		func(v float64) []byte {
			parker.Reset()
			parker.EncodeFloat64(v)
			return parker.Bytes()
		})
}

func fetchDateRows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		4,
		func(v types.Date) []byte {
			parker.Reset()
			parker.EncodeDate(v)
			return parker.Bytes()
		})
}

func fetchTimeRows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		8,
		func(v types.Time) []byte {
			parker.Reset()
			parker.EncodeTime(v)
			return parker.Bytes()
		})
}

func fetchDateTimeRows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		8,
		func(v types.Datetime) []byte {
			parker.Reset()
			parker.EncodeDatetime(v)
			return parker.Bytes()
		})
}

func fetchTimestampRows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		8,
		func(v types.Timestamp) []byte {
			parker.Reset()
			parker.EncodeTimestamp(v)
			return parker.Bytes()
		})
}

func fetchDecimal64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		8,
		func(v types.Decimal64) []byte {
			parker.Reset()
			parker.EncodeDecimal64(v)
			return parker.Bytes()
		})
}

func fetchDecimal128Rows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		16,
		func(v types.Decimal128) []byte {
			parker.Reset()
			parker.EncodeDecimal128(v)
			return parker.Bytes()
		})
}

func fetchUUIDRows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	return fetchFixedRows(
		vec,
		max,
		16,
		func(v types.Uuid) []byte {
			parker.Reset()
			parker.EncodeStringType(v[:])
			return parker.Bytes()
		})
}

func fetchVarlenaRows(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity) {
	n := vec.Length()
	data, area := vector.MustVarlenaRawData(vec)
	get := func(v []byte) []byte {
		parker.Reset()
		parker.EncodeStringType(v)
		return parker.Bytes()
	}

	if n == 1 {
		return [][]byte{
				get(data[0].GetByteSlice(area))},
			lock.Granularity_Row
	}
	size := n * 64
	if size > max {
		return [][]byte{
				get(data[0].GetByteSlice(area)),
				get(data[n-1].GetByteSlice(area))},
			lock.Granularity_Range
	}
	rows := make([][]byte, 0, n)
	for _, v := range data {
		rows = append(rows, get(v.GetByteSlice(area)))
	}
	return rows, lock.Granularity_Row
}

func fetchFixedRows[T any](
	vec *vector.Vector,
	max int,
	typeSize int,
	fn func(v T) []byte) ([][]byte, lock.Granularity) {
	n := vec.Length()
	values := vector.MustFixedCol[T](vec)
	if n == 1 {
		return [][]byte{
				fn(values[0])},
			lock.Granularity_Row
	}
	size := n * typeSize
	if size > max {
		return [][]byte{
				fn(values[0]),
				fn(values[n-1])},
			lock.Granularity_Range
	}
	rows := make([][]byte, 0, n)
	for _, v := range values {
		rows = append(rows, fn(v))
	}
	return rows, lock.Granularity_Row
}

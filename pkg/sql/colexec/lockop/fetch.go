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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
)

var (
	minUUID = [16]byte{}
	maxUUID = [16]byte{
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8,
		math.MaxInt8}
)

// GetFetchRowsFunc get FetchLockRowsFunc based on primary key type
func GetFetchRowsFunc(t types.Type) FetchLockRowsFunc {
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
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	return [][]byte{{0}, {1}},
		lock.Granularity_Range
}

func fetchInt8Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v int8) []byte {
		parker.Reset()
		parker.EncodeInt8(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt8)
		max := fn(math.MaxInt8)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		1,
		fn)
}

func fetchInt16Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v int16) []byte {
		parker.Reset()
		parker.EncodeInt16(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt16)
		max := fn(math.MaxInt16)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		2,
		fn)
}

func fetchInt32Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v int32) []byte {
		parker.Reset()
		parker.EncodeInt32(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt32)
		max := fn(math.MaxInt32)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		4,
		fn)
}

func fetchInt64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v int64) []byte {
		parker.Reset()
		parker.EncodeInt64(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt64)
		max := fn(math.MaxInt64)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		8,
		fn)
}

func fetchUint8Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v uint8) []byte {
		parker.Reset()
		parker.EncodeUint8(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(0)
		max := fn(math.MaxUint8)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		1,
		fn)
}

func fetchUint16Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v uint16) []byte {
		parker.Reset()
		parker.EncodeUint16(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(0)
		max := fn(math.MaxUint16)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		2,
		fn)
}

func fetchUint32Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v uint32) []byte {
		parker.Reset()
		parker.EncodeUint32(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(0)
		max := fn(math.MaxUint32)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		4,
		fn)
}

func fetchUint64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v uint64) []byte {
		parker.Reset()
		parker.EncodeUint64(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(0)
		max := fn(math.MaxUint64)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		8,
		fn)
}

func fetchFloat32Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v float32) []byte {
		parker.Reset()
		parker.EncodeFloat32(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.SmallestNonzeroFloat32)
		max := fn(math.MaxFloat32)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		4,
		fn)
}

func fetchFloat64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v float64) []byte {
		parker.Reset()
		parker.EncodeFloat64(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.SmallestNonzeroFloat64)
		max := fn(math.MaxFloat64)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		8,
		fn)
}

func fetchDateRows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v types.Date) []byte {
		parker.Reset()
		parker.EncodeDate(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt32)
		max := fn(math.MaxInt32)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		4,
		fn)
}

func fetchTimeRows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v types.Time) []byte {
		parker.Reset()
		parker.EncodeTime(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt64)
		max := fn(math.MaxInt64)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		8,
		fn)
}

func fetchDateTimeRows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v types.Datetime) []byte {
		parker.Reset()
		parker.EncodeDatetime(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt64)
		max := fn(math.MaxInt64)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		8,
		fn)
}

func fetchTimestampRows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v types.Timestamp) []byte {
		parker.Reset()
		parker.EncodeTimestamp(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(math.MinInt64)
		max := fn(math.MaxInt64)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		8,
		fn)
}

func fetchDecimal64Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v types.Decimal64) []byte {
		parker.Reset()
		parker.EncodeDecimal64(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(0)
		max := fn(math.MaxUint64)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		8,
		fn)
}

func fetchDecimal128Rows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v types.Decimal128) []byte {
		parker.Reset()
		parker.EncodeDecimal128(v)
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(types.Decimal128{})
		max := fn(types.Decimal128{B0_63: math.MaxUint64, B64_127: math.MaxUint64})
		return [][]byte{min, max},
			lock.Granularity_Range
	}
	return fetchFixedRows(
		vec,
		max,
		16,
		fn)
}

func fetchUUIDRows(
	vec *vector.Vector,
	parker *types.Packer,
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v types.Uuid) []byte {
		parker.Reset()
		parker.EncodeStringType(v[:])
		return parker.Bytes()
	}
	if lockTabel {
		min := fn(minUUID)
		max := fn(maxUUID)
		return [][]byte{min, max},
			lock.Granularity_Range
	}
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
	tp types.Type,
	max int,
	lockTabel bool) ([][]byte, lock.Granularity) {
	fn := func(v []byte) []byte {
		parker.Reset()
		parker.EncodeStringType(v[:])
		return parker.Bytes()
	}
	if lockTabel {
		min := fn([]byte{0})
		max := fn(getMax(int(tp.Width)))
		return [][]byte{min, max},
			lock.Granularity_Range
	}

	n := vec.Length()
	data, area := vector.MustVarlenaRawData(vec)
	if n == 1 {
		return [][]byte{fn(data[0].GetByteSlice(area))},
			lock.Granularity_Row
	}
	size := n * int(tp.Width)
	if size > max {
		return [][]byte{
				fn(data[0].GetByteSlice(area)),
				fn(data[n-1].GetByteSlice(area))},
			lock.Granularity_Range
	}
	rows := make([][]byte, 0, n)
	for _, v := range data {
		rows = append(rows, fn(v.GetByteSlice(area)))
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

func getMax(size int) []byte {
	v := make([]byte, size)
	for idx := range v {
		v[idx] = math.MaxUint8
	}
	return v
}

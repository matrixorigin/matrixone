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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestFetchBoolRows(t *testing.T) {
	values := []bool{false, true}
	runFetchRowsTest(
		t,
		types.New(types.T_bool, 0, 0),
		0,
		values,
		lock.Granularity_Range,
		values,
		values,
		values,
		func(packer *types.Packer, v bool) {
			packer.EncodeBool(v)
		},
	)
}

func TestFetchInt8Rows(t *testing.T) {
	values := []int8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int8, 0, 0),
		2,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]int8{math.MinInt8, math.MaxInt8},
		func(packer *types.Packer, v int8) {
			packer.EncodeInt8(v)
		},
	)
}

func TestFetchInt16Rows(t *testing.T) {
	values := []int16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int16, 0, 0),
		4,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]int16{math.MinInt16, math.MaxInt16},
		func(packer *types.Packer, v int16) {
			packer.EncodeInt16(v)
		},
	)
}

func TestFetchInt32Rows(t *testing.T) {
	values := []int32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int32, 0, 0),
		8,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]int32{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v int32) {
			packer.EncodeInt32(v)
		},
	)
}

func TestFetchInt64Rows(t *testing.T) {
	values := []int64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int64, 0, 0),
		16,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]int64{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v int64) {
			packer.EncodeInt64(v)
		},
	)
}

func TestFetchUint8Rows(t *testing.T) {
	values := []uint8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint8, 0, 0),
		2,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]uint8{0, math.MaxUint8},
		func(packer *types.Packer, v uint8) {
			packer.EncodeUint8(v)
		},
	)
}

func TestFetchUint16Rows(t *testing.T) {
	values := []uint16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint16, 0, 0),
		4,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]uint16{0, math.MaxUint16},
		func(packer *types.Packer, v uint16) {
			packer.EncodeUint16(v)
		},
	)
}

func TestFetchUint32Rows(t *testing.T) {
	values := []uint32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint32, 0, 0),
		8,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]uint32{0, math.MaxUint32},
		func(packer *types.Packer, v uint32) {
			packer.EncodeUint32(v)
		},
	)
}

func TestFetchUint64Rows(t *testing.T) {
	values := []uint64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint64, 0, 0),
		16,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]uint64{0, math.MaxUint64},
		func(packer *types.Packer, v uint64) {
			packer.EncodeUint64(v)
		},
	)
}

func TestFetchFloat32Rows(t *testing.T) {
	values := []float32{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float32, 0, 0),
		8,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]float32{math.SmallestNonzeroFloat32, math.MaxFloat32},
		func(packer *types.Packer, v float32) {
			packer.EncodeFloat32(v)
		},
	)
}

func TestFetchFloat64Rows(t *testing.T) {
	values := []float64{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float64, 0, 0),
		16,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]float64{math.SmallestNonzeroFloat64, math.MaxFloat64},
		func(packer *types.Packer, v float64) {
			packer.EncodeFloat64(v)
		},
	)
}

func TestFetchDateRows(t *testing.T) {
	values := []types.Date{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_date, 0, 0),
		8,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]types.Date{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v types.Date) {
			packer.EncodeDate(v)
		},
	)
}

func TestFetchTimeRows(t *testing.T) {
	values := []types.Time{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_time, 0, 0),
		16,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]types.Time{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Time) {
			packer.EncodeTime(v)
		},
	)
}

func TestFetchDateTimeRows(t *testing.T) {
	values := []types.Datetime{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_datetime, 0, 0),
		16,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]types.Datetime{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Datetime) {
			packer.EncodeDatetime(v)
		},
	)
}

func TestFetchTimestampRows(t *testing.T) {
	values := []types.Timestamp{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_timestamp, 0, 0),
		16,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]types.Timestamp{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Timestamp) {
			packer.EncodeTimestamp(v)
		},
	)
}

func TestFetchDecimal64Rows(t *testing.T) {
	values := []types.Decimal64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal64, 0, 0),
		16,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]types.Decimal64{0, math.MaxUint64},
		func(packer *types.Packer, v types.Decimal64) {
			packer.EncodeDecimal64(v)
		},
	)
}

func TestFetchDecimal128Rows(t *testing.T) {
	values := []types.Decimal128{{B0_63: 0, B64_127: 0}, {B0_63: 1, B64_127: 1}}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal128, 0, 0),
		32,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]types.Decimal128{{B0_63: 0, B64_127: 0}, {B0_63: math.MaxUint64, B64_127: math.MaxUint64}},
		func(packer *types.Packer, v types.Decimal128) {
			packer.EncodeDecimal128(v)
		},
	)
}

func TestFetchUUIDRows(t *testing.T) {
	values := []types.Uuid{[16]byte{}, [16]byte{1}}
	runFetchRowsTest(
		t,
		types.New(types.T_uuid, 0, 0),
		32,
		values,
		lock.Granularity_Row,
		values,
		values,
		[]types.Uuid{minUUID, maxUUID},
		func(packer *types.Packer, v types.Uuid) {
			packer.EncodeStringType(v[:])
		},
	)
}

func TestFetchCharRows(t *testing.T) {
	values := [][]byte{{0}, {1}}
	runFetchBytesRowsTest(
		t,
		types.New(types.T_char, 2, 0),
		4,
		values,
		lock.Granularity_Row,
		values,
		values,
		[][]byte{{0}, {math.MaxUint8, math.MaxUint8}},
		func(packer *types.Packer, v []byte) {
			packer.EncodeStringType(v)
		},
	)
}

func TestFetchVarcharRows(t *testing.T) {
	values := [][]byte{{0}, {1}}
	runFetchBytesRowsTest(
		t,
		types.New(types.T_varchar, 2, 0),
		4,
		values,
		lock.Granularity_Row,
		values,
		values,
		[][]byte{{0}, {math.MaxUint8, math.MaxUint8}},
		func(packer *types.Packer, v []byte) {
			packer.EncodeStringType(v)
		},
	)
}

func runFetchRowsTest[T any](
	t *testing.T,
	tp types.Type,
	max int,
	values []T,
	expectG lock.Granularity,
	expectValues []T,
	expectRangeValues []T,
	expectLockTableValues []T,
	fn func(*types.Packer, T)) {
	runFetchRowsTestWithAppendFunc(
		t,
		tp,
		max,
		values,
		expectG,
		expectValues,
		expectRangeValues,
		expectLockTableValues,
		fn,
		func(vec *vector.Vector, mp *mpool.MPool) {
			vector.AppendFixedList(vec, values, nil, mp)
		},
	)
}

func runFetchBytesRowsTest(
	t *testing.T,
	tp types.Type,
	max int,
	values [][]byte,
	expectG lock.Granularity,
	expectValues [][]byte,
	expectRangeValues [][]byte,
	expectLockTableValues [][]byte,
	fn func(*types.Packer, []byte)) {
	runFetchRowsTestWithAppendFunc(
		t,
		tp,
		max,
		values,
		expectG,
		expectValues,
		expectRangeValues,
		expectLockTableValues,
		fn,
		func(vec *vector.Vector, mp *mpool.MPool) {
			for _, v := range values {
				vector.AppendBytes(vec, v[:], false, mp)
			}
		},
	)
}

func runFetchRowsTestWithAppendFunc[T any](
	t *testing.T,
	tp types.Type,
	max int,
	values []T,
	expectG lock.Granularity,
	expectValues []T,
	expectRangeValues []T,
	expectLockTableValues []T,
	fn func(*types.Packer, T),
	appendFunc func(vec *vector.Vector, mp *mpool.MPool)) {
	mp := mpool.MustNew("test")
	vec := vector.NewVec(tp)
	appendFunc(vec, mp)

	packer := types.NewPacker(mpool.MustNew("test"))
	fetcher := GetFetchRowsFunc(tp)
	assertFN := func(values []T, rows [][]byte) {
		for idx, v := range values {
			packer.Reset()
			fn(packer, v)
			assert.Equal(t, packer.Bytes(), rows[idx])
		}
	}

	// many rows
	rows, g := fetcher(vec, packer, tp, max, false)
	assert.Equal(t, expectG, g)
	assert.Equal(t, len(expectValues), len(rows))
	assertFN(expectValues, rows)

	// many rows => range row
	rows, g = fetcher(vec, packer, tp, max-1, false)
	assert.Equal(t, lock.Granularity_Range, g)
	assert.Equal(t, 2, len(rows))
	assertFN(expectRangeValues, rows)

	// lock table
	rows, g = fetcher(vec, packer, tp, max, true)
	assert.Equal(t, lock.Granularity_Range, g)
	assert.Equal(t, 2, len(rows))
	assertFN(expectLockTableValues, rows)
}

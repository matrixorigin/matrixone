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
	"bytes"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchBoolRows(t *testing.T) {
	values := []bool{false, true}
	runFetchRowsTest(
		t,
		types.New(types.T_bool, 0, 0),
		values,
		lock.Granularity_Range,
		values,
		values,
		values,
		func(packer *types.Packer, v bool) {
			packer.EncodeBool(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchBitRows(t *testing.T) {
	values := []uint64{1, 0}
	expectRangeValues := []uint64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_bit, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]uint64{0, math.MaxUint64},
		func(packer *types.Packer, v uint64) {
			packer.EncodeUint64(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchBitRowsWithFilter(t *testing.T) {
	values := []uint64{1, 0, 2}
	expectRangeValues := []uint64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_bit, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint64{0, math.MaxUint64},
		func(packer *types.Packer, v uint64) {
			packer.EncodeUint64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchBitRowsWithFilterAll(t *testing.T) {
	values := []uint64{1, 0, 2}
	expectRangeValues := []uint64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_bit, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint64{0, math.MaxUint64},
		func(packer *types.Packer, v uint64) {
			packer.EncodeUint64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchInt8Rows(t *testing.T) {
	values := []int8{1, 0}
	expectRangeValues := []int8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int8, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]int8{math.MinInt8, math.MaxInt8},
		func(packer *types.Packer, v int8) {
			packer.EncodeInt8(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchInt8RowsWithFilter(t *testing.T) {
	values := []int8{1, 0, 2}
	expectRangeValues := []int8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int8, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int8{math.MinInt8, math.MaxInt8},
		func(packer *types.Packer, v int8) {
			packer.EncodeInt8(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchInt8RowsWithFilterAll(t *testing.T) {
	values := []int8{1, 0, 2}
	expectRangeValues := []int8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int8, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int8{math.MinInt8, math.MaxInt8},
		func(packer *types.Packer, v int8) {
			packer.EncodeInt8(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchInt16Rows(t *testing.T) {
	values := []int16{1, 0}
	expectRangeValues := []int16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int16, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]int16{math.MinInt16, math.MaxInt16},
		func(packer *types.Packer, v int16) {
			packer.EncodeInt16(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchInt16RowsWithFilter(t *testing.T) {
	values := []int16{1, 0, 2}
	expectRangeValues := []int16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int16, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int16{math.MinInt16, math.MaxInt16},
		func(packer *types.Packer, v int16) {
			packer.EncodeInt16(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchInt16RowsWithFilterAll(t *testing.T) {
	values := []int16{1, 0, 2}
	expectRangeValues := []int16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int16, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int16{math.MinInt16, math.MaxInt16},
		func(packer *types.Packer, v int16) {
			packer.EncodeInt16(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchInt32Rows(t *testing.T) {
	values := []int32{1, 0}
	expectRangeValues := []int32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int32, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]int32{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v int32) {
			packer.EncodeInt32(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchInt32RowsWithFilter(t *testing.T) {
	values := []int32{1, 0, 2}
	expectRangeValues := []int32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int32, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int32{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v int32) {
			packer.EncodeInt32(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchInt32RowsWithFilterAll(t *testing.T) {
	values := []int32{1, 0, 2}
	expectRangeValues := []int32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int32, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int32{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v int32) {
			packer.EncodeInt32(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchInt64Rows(t *testing.T) {
	values := []int64{1, 0}
	expectRangeValues := []int64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int64, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]int64{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v int64) {
			packer.EncodeInt64(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchInt64RowsWithFilter(t *testing.T) {
	values := []int64{1, 0, 2}
	expectRangeValues := []int64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int64{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v int64) {
			packer.EncodeInt64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchInt64RowsWithFilterAll(t *testing.T) {
	values := []int64{1, 0, 2}
	expectRangeValues := []int64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]int64{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v int64) {
			packer.EncodeInt64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchUint8Rows(t *testing.T) {
	values := []uint8{1, 0}
	expectRangeValues := []uint8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint8, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]uint8{0, math.MaxUint8},
		func(packer *types.Packer, v uint8) {
			packer.EncodeUint8(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchUint8RowsWithFilter(t *testing.T) {
	values := []uint8{1, 0, 2}
	expectRangeValues := []uint8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint8, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint8{0, math.MaxUint8},
		func(packer *types.Packer, v uint8) {
			packer.EncodeUint8(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchUint8RowsWithFilterAll(t *testing.T) {
	values := []uint8{1, 0, 2}
	expectRangeValues := []uint8{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint8, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint8{0, math.MaxUint8},
		func(packer *types.Packer, v uint8) {
			packer.EncodeUint8(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchUint16Rows(t *testing.T) {
	values := []uint16{1, 0}
	expectRangeValues := []uint16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint16, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]uint16{0, math.MaxUint16},
		func(packer *types.Packer, v uint16) {
			packer.EncodeUint16(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchUint16RowsWithFilter(t *testing.T) {
	values := []uint16{1, 0, 2}
	expectRangeValues := []uint16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint16, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint16{0, math.MaxUint16},
		func(packer *types.Packer, v uint16) {
			packer.EncodeUint16(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchUint16RowsWithFilterAll(t *testing.T) {
	values := []uint16{1, 0, 2}
	expectRangeValues := []uint16{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint16, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint16{0, math.MaxUint16},
		func(packer *types.Packer, v uint16) {
			packer.EncodeUint16(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchUint32Rows(t *testing.T) {
	values := []uint32{1, 0}
	expectRangeValues := []uint32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint32, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]uint32{0, math.MaxUint32},
		func(packer *types.Packer, v uint32) {
			packer.EncodeUint32(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchUint32RowsWithFilter(t *testing.T) {
	values := []uint32{1, 0, 2}
	expectRangeValues := []uint32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint32, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint32{0, math.MaxUint32},
		func(packer *types.Packer, v uint32) {
			packer.EncodeUint32(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchUint32RowsWithFilterAll(t *testing.T) {
	values := []uint32{1, 0, 2}
	expectRangeValues := []uint32{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint32, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint32{0, math.MaxUint32},
		func(packer *types.Packer, v uint32) {
			packer.EncodeUint32(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchUint64Rows(t *testing.T) {
	values := []uint64{1, 0}
	expectRangeValues := []uint64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint64, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]uint64{0, math.MaxUint64},
		func(packer *types.Packer, v uint64) {
			packer.EncodeUint64(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchUint64RowsWithFilter(t *testing.T) {
	values := []uint64{1, 0, 2}
	expectRangeValues := []uint64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint64{0, math.MaxUint64},
		func(packer *types.Packer, v uint64) {
			packer.EncodeUint64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchUint64RowsWithFilterAll(t *testing.T) {
	values := []uint64{1, 0, 2}
	expectRangeValues := []uint64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_uint64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]uint64{0, math.MaxUint64},
		func(packer *types.Packer, v uint64) {
			packer.EncodeUint64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchFloat32Rows(t *testing.T) {
	values := []float32{0.1, 0.0}
	expectRangeValues := []float32{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float32, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]float32{math.SmallestNonzeroFloat32, math.MaxFloat32},
		func(packer *types.Packer, v float32) {
			packer.EncodeFloat32(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchFloat32RowsWithFilter(t *testing.T) {
	values := []float32{0.1, 0.0, 0.2}
	expectRangeValues := []float32{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float32, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]float32{math.SmallestNonzeroFloat32, math.MaxFloat32},
		func(packer *types.Packer, v float32) {
			packer.EncodeFloat32(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchFloat32RowsWithFilterAll(t *testing.T) {
	values := []float32{0.1, 0.0, 0.2}
	expectRangeValues := []float32{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float32, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]float32{math.SmallestNonzeroFloat32, math.MaxFloat32},
		func(packer *types.Packer, v float32) {
			packer.EncodeFloat32(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchFloat64Rows(t *testing.T) {
	values := []float64{0.1, 0.0}
	expectRangeValues := []float64{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float64, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]float64{math.SmallestNonzeroFloat64, math.MaxFloat64},
		func(packer *types.Packer, v float64) {
			packer.EncodeFloat64(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchFloat64RowsWithFilter(t *testing.T) {
	values := []float64{0.1, 0.0, 0.2}
	expectRangeValues := []float64{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]float64{math.SmallestNonzeroFloat64, math.MaxFloat64},
		func(packer *types.Packer, v float64) {
			packer.EncodeFloat64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchFloat64RowsWithFilterAll(t *testing.T) {
	values := []float64{0.1, 0.0, 0.2}
	expectRangeValues := []float64{0.0, 0.1}
	runFetchRowsTest(
		t,
		types.New(types.T_float64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]float64{math.SmallestNonzeroFloat64, math.MaxFloat64},
		func(packer *types.Packer, v float64) {
			packer.EncodeFloat64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchDateRows(t *testing.T) {
	values := []types.Date{1, 0}
	expectRangeValues := []types.Date{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_date, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]types.Date{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v types.Date) {
			packer.EncodeDate(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchDateRowsWithFilter(t *testing.T) {
	values := []types.Date{1, 0, 2}
	expectRangeValues := []types.Date{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_date, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Date{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v types.Date) {
			packer.EncodeDate(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchDateRowsWithFilterAll(t *testing.T) {
	values := []types.Date{1, 0, 2}
	expectRangeValues := []types.Date{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_date, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Date{math.MinInt32, math.MaxInt32},
		func(packer *types.Packer, v types.Date) {
			packer.EncodeDate(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchTimeRows(t *testing.T) {
	values := []types.Time{1, 0}
	expectRangeValues := []types.Time{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_time, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]types.Time{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Time) {
			packer.EncodeTime(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchTimeRowsWithFilter(t *testing.T) {
	values := []types.Time{1, 0, 2}
	expectRangeValues := []types.Time{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_time, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Time{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Time) {
			packer.EncodeTime(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchTimeRowsWithFilterAll(t *testing.T) {
	values := []types.Time{1, 0, 2}
	expectRangeValues := []types.Time{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_time, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Time{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Time) {
			packer.EncodeTime(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchDateTimeRows(t *testing.T) {
	values := []types.Datetime{1, 0}
	expectRangeValues := []types.Datetime{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_datetime, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]types.Datetime{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Datetime) {
			packer.EncodeDatetime(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchDateTimeRowsWithFilter(t *testing.T) {
	values := []types.Datetime{1, 0, 2}
	expectRangeValues := []types.Datetime{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_datetime, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Datetime{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Datetime) {
			packer.EncodeDatetime(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchDateTimeRowsWithFilterAll(t *testing.T) {
	values := []types.Datetime{1, 0, 2}
	expectRangeValues := []types.Datetime{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_datetime, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Datetime{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Datetime) {
			packer.EncodeDatetime(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchTimestampRows(t *testing.T) {
	values := []types.Timestamp{1, 0}
	expectRangeValues := []types.Timestamp{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_timestamp, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]types.Timestamp{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Timestamp) {
			packer.EncodeTimestamp(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchTimestampRowsWithFilter(t *testing.T) {
	values := []types.Timestamp{1, 0, 2}
	expectRangeValues := []types.Timestamp{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_timestamp, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Timestamp{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Timestamp) {
			packer.EncodeTimestamp(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchTimestampRowsWithFilterAll(t *testing.T) {
	values := []types.Timestamp{1, 0, 2}
	expectRangeValues := []types.Timestamp{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_timestamp, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Timestamp{math.MinInt64, math.MaxInt64},
		func(packer *types.Packer, v types.Timestamp) {
			packer.EncodeTimestamp(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchDecimal64Rows(t *testing.T) {
	max := types.Decimal64(999999999999999999)
	min := max.Minus()
	values := []types.Decimal64{1, 0}
	expectRangeValues := []types.Decimal64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal64, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]types.Decimal64{min, max},
		func(packer *types.Packer, v types.Decimal64) {
			packer.EncodeDecimal64(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchDecimal64RowsWithFilter(t *testing.T) {
	max := types.Decimal64(999999999999999999)
	min := max.Minus()
	values := []types.Decimal64{1, 0, 2}
	expectRangeValues := []types.Decimal64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Decimal64{min, max},
		func(packer *types.Packer, v types.Decimal64) {
			packer.EncodeDecimal64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchDecimal64RowsWithFilterAll(t *testing.T) {
	max := types.Decimal64(999999999999999999)
	min := max.Minus()
	values := []types.Decimal64{1, 0, 2}
	expectRangeValues := []types.Decimal64{0, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal64, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Decimal64{min, max},
		func(packer *types.Packer, v types.Decimal64) {
			packer.EncodeDecimal64(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchDecimal128Rows(t *testing.T) {
	max, _, _ := types.Parse128("99999999999999999999999999999999999999")
	min := max.Minus()
	values := []types.Decimal128{{B0_63: 1, B64_127: 1}, {B0_63: 0, B64_127: 0}}
	expectRangeValues := []types.Decimal128{{B0_63: 0, B64_127: 0}, {B0_63: 1, B64_127: 1}}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal128, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]types.Decimal128{min, max},
		func(packer *types.Packer, v types.Decimal128) {
			packer.EncodeDecimal128(v)
		},
		nil,
		nil,
		false,
	)
}

func TestFetchDecimal128RowsWithFilter(t *testing.T) {
	max, _, _ := types.Parse128("99999999999999999999999999999999999999")
	min := max.Minus()
	values := []types.Decimal128{{B0_63: 1, B64_127: 1}, {B0_63: 0, B64_127: 0}, {B0_63: 2, B64_127: 2}}
	expectRangeValues := []types.Decimal128{{B0_63: 0, B64_127: 0}, {B0_63: 1, B64_127: 1}}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal128, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Decimal128{min, max},
		func(packer *types.Packer, v types.Decimal128) {
			packer.EncodeDecimal128(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchDecimal128RowsWithFilterAll(t *testing.T) {
	max, _, _ := types.Parse128("99999999999999999999999999999999999999")
	min := max.Minus()
	values := []types.Decimal128{{B0_63: 1, B64_127: 1}, {B0_63: 0, B64_127: 0}, {B0_63: 2, B64_127: 2}}
	expectRangeValues := []types.Decimal128{{B0_63: 0, B64_127: 0}, {B0_63: 1, B64_127: 1}}
	runFetchRowsTest(
		t,
		types.New(types.T_decimal128, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Decimal128{min, max},
		func(packer *types.Packer, v types.Decimal128) {
			packer.EncodeDecimal128(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchUUIDRows(t *testing.T) {
	values := []types.Uuid{[16]byte{1}, [16]byte{}}
	expectRangeValues := []types.Uuid{[16]byte{}, [16]byte{1}}
	runFetchRowsTest(
		t,
		types.New(types.T_uuid, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[]types.Uuid{minUUID, maxUUID},
		func(packer *types.Packer, v types.Uuid) {
			packer.EncodeStringType(v[:])
		},
		nil,
		nil,
		false,
	)
}

func TestFetchUUIDRowsWithFilter(t *testing.T) {
	values := []types.Uuid{[16]byte{1}, [16]byte{}, [16]byte{2}}
	expectRangeValues := []types.Uuid{[16]byte{}, [16]byte{1}}
	runFetchRowsTest(
		t,
		types.New(types.T_uuid, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Uuid{minUUID, maxUUID},
		func(packer *types.Packer, v types.Uuid) {
			packer.EncodeStringType(v[:])
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
		false,
	)
}

func TestFetchUUIDRowsWithFilterAll(t *testing.T) {
	values := []types.Uuid{[16]byte{1}, [16]byte{}, [16]byte{2}}
	expectRangeValues := []types.Uuid{[16]byte{}, [16]byte{1}}
	runFetchRowsTest(
		t,
		types.New(types.T_uuid, 0, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[]types.Uuid{minUUID, maxUUID},
		func(packer *types.Packer, v types.Uuid) {
			packer.EncodeStringType(v[:])
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{1, 1, 1},
		true,
	)
}

func TestFetchCharRows(t *testing.T) {
	values := [][]byte{{1}, {0}}
	expectRangeValues := [][]byte{{0}, {1}}
	runFetchBytesRowsTest(
		t,
		types.New(types.T_char, 2, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[][]byte{{0}, {math.MaxUint8, math.MaxUint8}},
		func(packer *types.Packer, v []byte) {
			packer.EncodeStringType(v)
		},
		nil,
		nil,
	)
}

func TestFetchCharRowsWithFilter(t *testing.T) {
	values := [][]byte{{1}, {0}, {2}}
	expectRangeValues := [][]byte{{0}, {1}}
	runFetchBytesRowsTest(
		t,
		types.New(types.T_char, 2, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[][]byte{{0}, {math.MaxUint8, math.MaxUint8}},
		func(packer *types.Packer, v []byte) {
			packer.EncodeStringType(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
	)
}

func TestFetchVarcharRows(t *testing.T) {
	values := [][]byte{{1}, {0}}
	expectRangeValues := [][]byte{{0}, {1}}
	runFetchBytesRowsTest(
		t,
		types.New(types.T_varchar, 2, 0),
		values,
		lock.Granularity_Row,
		values,
		expectRangeValues,
		[][]byte{{0}, {math.MaxUint8, math.MaxUint8}},
		func(packer *types.Packer, v []byte) {
			packer.EncodeStringType(v)
		},
		nil,
		nil,
	)
}

func TestFetchVarcharRowsWithFilter(t *testing.T) {
	values := [][]byte{{1}, {0}, {2}}
	expectRangeValues := [][]byte{{0}, {1}}
	runFetchBytesRowsTest(
		t,
		types.New(types.T_varchar, 2, 0),
		values,
		lock.Granularity_Row,
		values[:2],
		expectRangeValues,
		[][]byte{{0}, {math.MaxUint8, math.MaxUint8}},
		func(packer *types.Packer, v []byte) {
			packer.EncodeStringType(v)
		},
		getRowsFilter(1, []uint64{1, 2}),
		[]int32{0, 0, 1},
	)
}

func TestFetchRangeWithSameMinAndMax(t *testing.T) {
	values := []int16{1, 1}
	runFetchRowsTest(
		t,
		types.New(types.T_int16, 0, 0),
		values,
		lock.Granularity_Row,
		values,
		values[:1],
		[]int16{math.MinInt16, math.MaxInt16},
		func(packer *types.Packer, v int16) {
			packer.EncodeInt16(v)
		},
		nil,
		nil,
		false,
	)
}

func runFetchRowsTest[T any](
	t *testing.T,
	tp types.Type,
	values []T,
	expectG lock.Granularity,
	expectValues []T,
	expectRangeValues []T,
	expectLockTableValues []T,
	fn func(*types.Packer, T),
	filter RowsFilter,
	filterCols []int32,
	filterAll bool) {
	runFetchRowsTestWithAppendFunc(
		t,
		tp,
		values,
		expectG,
		expectValues,
		expectRangeValues,
		expectLockTableValues,
		fn,
		func(vec *vector.Vector, mp *mpool.MPool) {
			vector.AppendFixedList(vec, values, nil, mp)
		},
		filter,
		filterCols,
		filterAll,
	)
}

func runFetchBytesRowsTest(
	t *testing.T,
	tp types.Type,
	values [][]byte,
	expectG lock.Granularity,
	expectValues [][]byte,
	expectRangeValues [][]byte,
	expectLockTableValues [][]byte,
	fn func(*types.Packer, []byte),
	filter RowsFilter,
	filterCols []int32) {
	runFetchRowsTestWithAppendFunc(
		t,
		tp,
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
		filter,
		filterCols,
		false,
	)
}

func runFetchRowsTestWithAppendFunc[T any](
	t *testing.T,
	tp types.Type,
	values []T,
	expectG lock.Granularity,
	expectValues []T,
	expectRangeValues []T,
	expectLockTableValues []T,
	fn func(*types.Packer, T),
	appendFunc func(vec *vector.Vector, mp *mpool.MPool),
	filter RowsFilter,
	filterCols []int32,
	filterAll bool) {
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
	ok, rows, g := fetcher(vec, packer, tp, len(values), false, filter, filterCols)
	if !ok {
		require.Equal(t, filterAll, !ok)
		return
	} else {
		assert.Equal(t, expectG, g)
		assert.Equal(t, len(expectValues), len(rows))
		assertFN(expectValues, rows)
	}

	// many rows => range row
	ok, rows, g = fetcher(vec, packer, tp, len(values)-1, false, filter, filterCols)
	if !ok {
		require.Equal(t, filterAll, !ok)
		return
	} else {
		if len(expectRangeValues) > 1 {
			assert.Equal(t, lock.Granularity_Range, g)
			assert.Equal(t, 2, len(rows))
			assertFN(expectRangeValues, rows)
		} else {
			assert.Equal(t, lock.Granularity_Row, g)
			assert.Equal(t, 1, len(rows))
			assertFN(expectRangeValues, rows)
		}
	}

	// lock table
	ok, rows, g = fetcher(vec, packer, tp, len(values), true, filter, filterCols)
	require.True(t, ok)
	assert.Equal(t, lock.Granularity_Range, g)
	assert.Equal(t, 2, len(rows))
	assertFN(expectLockTableValues, rows)
}

func TestDecimal128(t *testing.T) {
	packer := types.NewPacker(mpool.MustNew("test"))
	decimal128Fn := func(v types.Decimal128) []byte {
		packer.Reset()
		packer.EncodeDecimal128(v)
		return packer.Bytes()
	}
	max128, _, _ := types.Parse128("99999999999999999999999999999999999999")
	minDecimal128 := decimal128Fn(max128.Minus())
	maxDecimal128 := decimal128Fn(max128)
	assert.True(t, bytes.Compare(minDecimal128, maxDecimal128) < 0)
}

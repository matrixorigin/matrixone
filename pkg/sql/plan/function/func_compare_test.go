// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOperatorOpBitAndInt64Fn(t *testing.T) {
	// 1 & 2 = 0
	// -1 & 2 = 2
	// null & 2 = null
	tc := tcTemp{
		info: "& test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{0, 2, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitAndInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitOrInt64Fn(t *testing.T) {
	// 1 | 2 = 3
	// -1 | 2 = -1
	// null | 2 = null
	tc := tcTemp{
		info: "| test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, -1, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitOrInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitXorInt64Fn(t *testing.T) {
	// 1 ^ 2 = 3
	// -1 ^ 2 = -3
	// null ^ 2 = null
	tc := tcTemp{
		info: "^ test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, -3, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitXorInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitRightShiftInt64Fn(t *testing.T) {
	// 1024 >> 2 = 256
	// -5 >> 2 = -2
	// 2 >> -2 = 0
	// null >> 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1024, -5, 2, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, -2, 2}, []bool{false, false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{256, -2, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftRightInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitLeftShiftInt64Fn(t *testing.T) {
	// -1 << 2 = 4
	// -1 << 2 = -4
	// 2 << -2 = 0
	// null << 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 2, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, -2, 2}, []bool{false, false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{4, -4, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftLeftInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFn(t *testing.T) {
	// 1 <=> 1 = true
	// 1 <=> 0 = false
	// 1 <=> null = false
	// null <=> 1 = false
	// null <=> null = true
	tcInt64 := tcTemp{
		info: "<=> int64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTCInt64 := NewFunctionTestCase(proc,
		tcInt64.inputs, tcInt64.expect, nullSafeEqualFn)
	s, info := fcTCInt64.Run()
	require.True(t, s, info, tcInt64.info)

	// Float64 Test
	tcFloat := tcTemp{
		info: "<=> float64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1.1, 1.1, 1.1, 0.0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1.1, 0.0, 0.0, 0.0}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCFloat := NewFunctionTestCase(proc,
		tcFloat.inputs, tcFloat.expect, nullSafeEqualFn)
	s, info = fcTCFloat.Run()
	require.True(t, s, info, tcFloat.info)

	// Varchar Test
	tcStr := tcTemp{
		info: "<=> varchar test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "a", "a", ""}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "b", "", ""}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCStr := NewFunctionTestCase(proc,
		tcStr.inputs, tcStr.expect, nullSafeEqualFn)
	s, info = fcTCStr.Run()
	require.True(t, s, info, tcStr.info)

	// Bool Test
	tcBool := tcTemp{
		info: "<=> bool test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, true, true, false}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, false, false, false}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCBool := NewFunctionTestCase(proc,
		tcBool.inputs, tcBool.expect, nullSafeEqualFn)
	s, info = fcTCBool.Run()
	require.True(t, s, info, tcBool.info)

	// Date Test
	d1, _ := types.ParseDateCast("2022-01-01")
	d2, _ := types.ParseDateCast("2022-01-02")
	tcDate := tcTemp{
		info: "<=> date test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{d1, d1, d1, d2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{d1, d2, d2, d2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDate := NewFunctionTestCase(proc,
		tcDate.inputs, tcDate.expect, nullSafeEqualFn)
	s, info = fcTCDate.Run()
	require.True(t, s, info, tcDate.info)

	// Time Test
	t1, _ := types.ParseTime("12:00:00", 0)
	t2, _ := types.ParseTime("13:00:00", 0)
	tcTime := tcTemp{
		info: "<=> time test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_time.ToType(), []types.Time{t1, t1, t1, t2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_time.ToType(), []types.Time{t1, t2, t2, t2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCTime := NewFunctionTestCase(proc,
		tcTime.inputs, tcTime.expect, nullSafeEqualFn)
	s, info = fcTCTime.Run()
	require.True(t, s, info, tcTime.info)

	// Timestamp Test
	ts1, _ := types.ParseTimestamp(time.UTC, "2022-01-01 12:00:00", 6)
	ts2, _ := types.ParseTimestamp(time.UTC, "2022-01-01 13:00:00", 6)
	tcTimestamp := tcTemp{
		info: "<=> timestamp test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{ts1, ts1, ts1, ts2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{ts1, ts2, ts2, ts2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCTimestamp := NewFunctionTestCase(proc,
		tcTimestamp.inputs, tcTimestamp.expect, nullSafeEqualFn)
	s, info = fcTCTimestamp.Run()
	require.True(t, s, info, tcTimestamp.info)

	// Decimal64 Test
	dec64_1, _ := types.ParseDecimal64("1.1", 10, 2)
	dec64_2, _ := types.ParseDecimal64("2.2", 10, 2)
	tcDecimal64 := tcTemp{
		info: "<=> decimal64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_decimal64.ToType(), []types.Decimal64{dec64_1, dec64_1, dec64_1, dec64_2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_decimal64.ToType(), []types.Decimal64{dec64_1, dec64_2, dec64_2, dec64_2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDecimal64 := NewFunctionTestCase(proc,
		tcDecimal64.inputs, tcDecimal64.expect, nullSafeEqualFn)
	s, info = fcTCDecimal64.Run()
	require.True(t, s, info, tcDecimal64.info)

	// Decimal128 Test
	dec128_1, _ := types.ParseDecimal128("1.1", 30, 2)
	dec128_2, _ := types.ParseDecimal128("2.2", 30, 2)
	tcDecimal128 := tcTemp{
		info: "<=> decimal128 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{dec128_1, dec128_1, dec128_1, dec128_2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{dec128_1, dec128_2, dec128_2, dec128_2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDecimal128 := NewFunctionTestCase(proc,
		tcDecimal128.inputs, tcDecimal128.expect, nullSafeEqualFn)
	s, info = fcTCDecimal128.Run()
	require.True(t, s, info, tcDecimal128.info)

	// UUID Test
	uuid1, _ := types.ParseUuid("00000000-0000-0000-0000-000000000001")
	uuid2, _ := types.ParseUuid("00000000-0000-0000-0000-000000000002")
	tcUuid := tcTemp{
		info: "<=> uuid test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uuid.ToType(), []types.Uuid{uuid1, uuid1, uuid1, uuid2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_uuid.ToType(), []types.Uuid{uuid1, uuid2, uuid2, uuid2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCUuid := NewFunctionTestCase(proc,
		tcUuid.inputs, tcUuid.expect, nullSafeEqualFn)
	s, info = fcTCUuid.Run()
	require.True(t, s, info, tcUuid.info)

	// Int8 Test
	tcInt8 := tcTemp{
		info: "<=> int8 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int8.ToType(), []int8{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int8.ToType(), []int8{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCInt8 := NewFunctionTestCase(proc, tcInt8.inputs, tcInt8.expect, nullSafeEqualFn)
	s, info = fcTCInt8.Run()
	require.True(t, s, info, tcInt8.info)

	// Int16 Test
	tcInt16 := tcTemp{
		info: "<=> int16 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int16.ToType(), []int16{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int16.ToType(), []int16{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCInt16 := NewFunctionTestCase(proc, tcInt16.inputs, tcInt16.expect, nullSafeEqualFn)
	s, info = fcTCInt16.Run()
	require.True(t, s, info, tcInt16.info)

	// Int32 Test
	tcInt32 := tcTemp{
		info: "<=> int32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int32.ToType(), []int32{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int32.ToType(), []int32{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCInt32 := NewFunctionTestCase(proc, tcInt32.inputs, tcInt32.expect, nullSafeEqualFn)
	s, info = fcTCInt32.Run()
	require.True(t, s, info, tcInt32.info)

	// Uint8 Test
	tcUint8 := tcTemp{
		info: "<=> uint8 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint8.ToType(), []uint8{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint8.ToType(), []uint8{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint8 := NewFunctionTestCase(proc, tcUint8.inputs, tcUint8.expect, nullSafeEqualFn)
	s, info = fcTCUint8.Run()
	require.True(t, s, info, tcUint8.info)

	// Uint16 Test
	tcUint16 := tcTemp{
		info: "<=> uint16 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint16.ToType(), []uint16{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint16.ToType(), []uint16{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint16 := NewFunctionTestCase(proc, tcUint16.inputs, tcUint16.expect, nullSafeEqualFn)
	s, info = fcTCUint16.Run()
	require.True(t, s, info, tcUint16.info)

	// Uint32 Test
	tcUint32 := tcTemp{
		info: "<=> uint32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint32 := NewFunctionTestCase(proc, tcUint32.inputs, tcUint32.expect, nullSafeEqualFn)
	s, info = fcTCUint32.Run()
	require.True(t, s, info, tcUint32.info)

	// Uint64 Test
	tcUint64 := tcTemp{
		info: "<=> uint64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint64 := NewFunctionTestCase(proc, tcUint64.inputs, tcUint64.expect, nullSafeEqualFn)
	s, info = fcTCUint64.Run()
	require.True(t, s, info, tcUint64.info)

	// Float32 Test
	tcFloat32 := tcTemp{
		info: "<=> float32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float32.ToType(), []float32{1.1, 1.1, 1.1, 0.0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_float32.ToType(), []float32{1.1, 0.0, 0.0, 0.0}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCFloat32 := NewFunctionTestCase(proc, tcFloat32.inputs, tcFloat32.expect, nullSafeEqualFn)
	s, info = fcTCFloat32.Run()
	require.True(t, s, info, tcFloat32.info)

	// Enum Test
	tcEnum := tcTemp{
		info: "<=> enum test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCEnum := NewFunctionTestCase(proc, tcEnum.inputs, tcEnum.expect, nullSafeEqualFn)
	s, info = fcTCEnum.Run()
	require.True(t, s, info, tcEnum.info)

	// Datetime Test
	dt1, _ := types.ParseDatetime("2022-01-01 12:00:00", 6)
	dt2, _ := types.ParseDatetime("2022-01-01 13:00:00", 6)
	tcDatetime := tcTemp{
		info: "<=> datetime test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{dt1, dt1, dt1, dt2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{dt1, dt2, dt2, dt2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDatetime := NewFunctionTestCase(proc, tcDatetime.inputs, tcDatetime.expect, nullSafeEqualFn)
	s, info = fcTCDatetime.Run()
	require.True(t, s, info, tcDatetime.info)

	// Year Test
	y1 := types.MoYear(2022)
	y2 := types.MoYear(2023)
	tcYear := tcTemp{
		info: "<=> year test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{y1, y1, y1, y2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{y1, y2, y2, y2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCYear := NewFunctionTestCase(proc, tcYear.inputs, tcYear.expect, nullSafeEqualFn)
	s, info = fcTCYear.Run()
	require.True(t, s, info, tcYear.info)

	// Float32 with Scale Test
	f32Type := types.T_float32.ToType()
	f32Type.Scale = 2
	tcFloat32Scale := tcTemp{
		info: "<=> float32 scale test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(f32Type, []float32{1.111, 1.114, 1.115}, []bool{false, false, false}),
			NewFunctionTestInput(f32Type, []float32{1.11, 1.11, 1.12}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true, true}, []bool{false, false, false}),
	}
	fcTCFloat32Scale := NewFunctionTestCase(proc, tcFloat32Scale.inputs, tcFloat32Scale.expect, nullSafeEqualFn)
	s, info = fcTCFloat32Scale.Run()
	require.True(t, s, info, tcFloat32Scale.info)

	// JSON Test
	tcJson := tcTemp{
		info: "<=> json test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{`{"a":1}`, `{"a":1}`}, []bool{false, true}),
			NewFunctionTestInput(types.T_json.ToType(), []string{`{"a":1}`, `{"a":1}`}, []bool{false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true}, []bool{false, false}),
	}
	fcTCJson := NewFunctionTestCase(proc, tcJson.inputs, tcJson.expect, nullSafeEqualFn)
	s, info = fcTCJson.Run()
	require.True(t, s, info, tcJson.info)

	// Bit Test
	tcBit := tcTemp{
		info: "<=> bit test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bit.ToType(), []uint64{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_bit.ToType(), []uint64{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCBit := NewFunctionTestCase(proc, tcBit.inputs, tcBit.expect, nullSafeEqualFn)
	s, info = fcTCBit.Run()
	require.True(t, s, info, tcBit.info)

	// Rowid Test
	rid1 := types.Rowid([24]byte{1})
	rid2 := types.Rowid([24]byte{2})
	tcRowid := tcTemp{
		info: "<=> rowid test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_Rowid.ToType(), []types.Rowid{rid1, rid1, rid1, rid2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_Rowid.ToType(), []types.Rowid{rid1, rid2, rid2, rid2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCRowid := NewFunctionTestCase(proc, tcRowid.inputs, tcRowid.expect, nullSafeEqualFn)
	s, info = fcTCRowid.Run()
	require.True(t, s, info, tcRowid.info)

	// Array Float32 Test
	arr1 := []float32{1.0, 2.0}
	arr2 := []float32{3.0, 4.0}
	tcArrF32 := tcTemp{
		info: "<=> array float32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{arr1, arr1, arr1, arr2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{arr1, arr2, arr2, arr2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCArrF32 := NewFunctionTestCase(proc, tcArrF32.inputs, tcArrF32.expect, nullSafeEqualFn)
	s, info = fcTCArrF32.Run()
	require.True(t, s, info, tcArrF32.info)

	// Array Float64 Test
	arrD1 := []float64{1.0, 2.0}
	arrD2 := []float64{3.0, 4.0}
	tcArrF64 := tcTemp{
		info: "<=> array float64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{arrD1, arrD1, arrD1, arrD2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{arrD1, arrD2, arrD2, arrD2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCArrF64 := NewFunctionTestCase(proc, tcArrF64.inputs, tcArrF64.expect, nullSafeEqualFn)
	s, info = fcTCArrF64.Run()
	require.True(t, s, info, tcArrF64.info)
}

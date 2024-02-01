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

package function

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func initCastTestCase() []tcTemp {
	var testCases []tcTemp

	// used on case.
	f1251ToDec128, _ := types.Decimal128FromFloat64(125.1, 38, 1)
	s01date, _ := types.ParseDateCast("2004-04-03")
	s02date, _ := types.ParseDateCast("2021-10-03")
	s01ts, _ := types.ParseTimestamp(time.Local, "2020-08-23 11:52:21", 6)

	castToSameTypeCases := []tcTemp{
		// cast to same type.
		{
			info: "int8 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{-23}, []bool{false}),
		},
		{
			info: "int16 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{-23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{-23}, []bool{false}),
		},
		{
			info: "int32 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{-23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{-23}, []bool{false}),
		},
		{
			info: "int64 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-23}, []bool{false}),
		},
		{
			info: "uint8 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{23}, []bool{false}),
		},
		{
			info: "uint16 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{23}, []bool{false}),
		},
		{
			info: "uint32 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{23}, []bool{false}),
		},
		{
			info: "uint64 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{23}, []bool{false}),
		},
		{
			info: "float32 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{23.5}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{23.5}, []bool{false}),
		},
		{
			info: "float64 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{23.5}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{23.5}, []bool{false}),
		},
		{
			info: "date to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{729848}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{729848}, []bool{false}),
		},
		{
			info: "datetime to datetime",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{66122056321728512}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{66122056321728512}, []bool{false}),
		},
		{
			info: "timestamp to timestamp",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_timestamp.ToType(),
					[]types.Timestamp{66122026122739712}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_timestamp.ToType(), false,
				[]types.Timestamp{66122026122739712}, []bool{false}),
		},
		{
			info: "time to time",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{661220261227}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_time.ToType(), []types.Time{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{661220261227}, []bool{false}),
		},
		{
			info: "vecf32 to vecf32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{}}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}}, []bool{false}),
		},
		{
			info: "vecf64 to vecf64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{}}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}}, []bool{false}),
		},
		{
			info: "bit to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{23}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false, []uint64{23}, []bool{false}),
		},
	}
	castInt8ToOthers := []tcTemp{
		// test cast int8 to others.
		{
			info: "int8 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int8 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt16ToOthers := []tcTemp{
		// test cast int16 to others.
		{
			info: "int16 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int16 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt32ToOthers := []tcTemp{
		// test cast int32 to others.
		{
			info: "int32 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int32 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt64ToOthers := []tcTemp{
		// test cast int64 to others.
		{
			info: "int64 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int64 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint8ToOthers := []tcTemp{
		// test cast uint8 to others.
		{
			info: "uint8 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint8 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint16ToOthers := []tcTemp{
		// test cast uint16 to others.
		{
			info: "uint16 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint16 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint32ToOthers := []tcTemp{
		// test cast uint32 to others.
		{
			info: "uint32 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint32 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint64ToOthers := []tcTemp{
		// test cast uint64 to others.
		{
			info: "uint64 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint64 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.New(types.T_bit, 5, 0), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), true, []uint64{}, []bool{}),
		},
	}
	castFloat32ToOthers := []tcTemp{
		// test cast float32 to others.
		{
			info: "float32 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to str type",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{23.56, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_char.ToType(), []string{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_char.ToType(), false,
				[]string{"23.56", "126", "0"}, []bool{false, false, true}),
		},
		{
			info: "float32 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125.1, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.New(types.T_decimal128, 8, 1), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.New(types.T_decimal128, 8, 1), false,
				[]types.Decimal128{f1251ToDec128, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "float32 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{125.4999, 125.55555, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castFloat64ToOthers := []tcTemp{
		// test cast float64 to others.
		{
			info: "float64 to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to str type",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{23.56, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_char.ToType(), []string{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_char.ToType(), false,
				[]string{"23.56", "126", "0"}, []bool{false, false, true}),
		},
		{
			info: "float64 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125.1, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.New(types.T_decimal128, 8, 1), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.New(types.T_decimal128, 8, 1), false,
				[]types.Decimal128{f1251ToDec128, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "float64 to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{125.4999, 125.55555, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castStrToOthers := []tcTemp{
		{
			info: "str type to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501", "16", ""}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1501, 16, 0}, []bool{false, false, true}),
		},
		{
			info: "str type to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501", "16", ""}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1501, 16, 0}, []bool{false, false, true}),
		},
		{
			info: "str type to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501.12", "16", ""}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1501.12, 16, 0}, []bool{false, false, true}),
		},
		{
			info: "str type to str type",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501.12", "16", ""}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_text.ToType(), []string{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_text.ToType(), false,
				[]string{"1501.12", "16", ""}, []bool{false, false, true}),
		},
		{
			info: "str type to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2004-04-03", "2004-04-03", "2021-10-03"},
					[]bool{false, false, false}),
				testutil.NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(
				types.T_date.ToType(), false,
				[]types.Date{
					s01date, s01date, s02date,
				},
				[]bool{false, false, false}),
		},
		{
			info: "str type to vecf32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]", "[4,5,6]"}, nil),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
		{
			info: "str type to vecf64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]", "[4,5,6]"}, nil),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
		{
			info: "str type to bit",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"15", "ab"}, nil),
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{12597, 24930}, []bool{false, false}),
		},
	}
	castDecToOthers := []tcTemp{
		{
			info: "decimal64 to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 0),
					[]types.Decimal64{types.Decimal64(333333000)}, nil),
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 0),
					[]types.Decimal128{}, nil),
			},
			expect: testutil.NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 0), false,
				[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
		},
		{
			info: "decimal64(10,5) to decimal64(10, 4)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(33333300)}, nil),
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 4),
					[]types.Decimal64{0}, nil),
			},
			expect: testutil.NewFunctionTestResult(
				types.New(types.T_decimal64, 10, 4), false,
				[]types.Decimal64{types.Decimal64(3333330)}, nil),
		},
		{
			info: "decimal64(10,5) to decimal128(20, 5)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(333333000)}, nil),
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 5),
					[]types.Decimal128{}, nil),
			},
			expect: testutil.NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 5), false,
				[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
		},
		{
			info: "decimal128(20,5) to decimal128(20, 4)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 5),
					[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 4),
					[]types.Decimal128{}, nil),
			},
			expect: testutil.NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 4), false,
				[]types.Decimal128{{B0_63: 33333300, B64_127: 0}}, nil),
		},
		{
			info: "decimal64 to str type",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(1234)}, nil),
				testutil.NewFunctionTestInput(
					types.T_varchar.ToType(),
					[]string{}, nil),
			},
			expect: testutil.NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"0.01234"}, nil),
		},
		{
			info: "decimal128 to str type",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 2),
					[]types.Decimal128{{B0_63: 1234, B64_127: 0}}, nil),
				testutil.NewFunctionTestInput(
					types.T_varchar.ToType(),
					[]string{}, nil),
			},
			expect: testutil.NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"12.34"}, nil),
		},
	}
	castTimestampToOthers := []tcTemp{
		{
			info: "timestamp to str type",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(
					types.T_timestamp.ToType(),
					[]types.Timestamp{s01ts}, nil),
				testutil.NewFunctionTestInput(
					types.T_varchar.ToType(), []string{}, nil),
			},
			expect: testutil.NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"2020-08-23 11:52:21"}, nil),
		},
	}

	castArrayFloat32ToOthers := []tcTemp{
		{
			info: "vecf32 type to vecf64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, nil),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
	}

	castArrayFloat64ToOthers := []tcTemp{
		{
			info: "vecf64 type to vecf32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, nil),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
	}

	castBitToOthers := []tcTemp{
		// test cast bit to others.
		{
			info: "bit to int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 256, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), true, []int8{}, []bool{}),
		},
		{
			info: "bit to int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to float32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to decimal128",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 0}, []bool{false, true}),
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
	}

	// init the testCases
	testCases = append(testCases, castFloat64ToOthers...)
	testCases = append(testCases, castFloat32ToOthers...)
	testCases = append(testCases, castStrToOthers...)
	testCases = append(testCases, castDecToOthers...)
	testCases = append(testCases, castTimestampToOthers...)
	testCases = append(testCases, castArrayFloat32ToOthers...)
	testCases = append(testCases, castArrayFloat64ToOthers...)
	testCases = append(testCases, castBitToOthers...)
	testCases = append(testCases, castToSameTypeCases...)
	testCases = append(testCases, castInt8ToOthers...)
	testCases = append(testCases, castInt16ToOthers...)
	testCases = append(testCases, castInt32ToOthers...)
	testCases = append(testCases, castInt64ToOthers...)
	testCases = append(testCases, castUint8ToOthers...)
	testCases = append(testCases, castUint16ToOthers...)
	testCases = append(testCases, castUint32ToOthers...)
	testCases = append(testCases, castUint64ToOthers...)

	return testCases
}

func TestCast(t *testing.T) {
	testCases := initCastTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, NewCast)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func BenchmarkCast(b *testing.B) {
	testCases := initCastTestCase()
	proc := testutil.NewProcess()

	b.StartTimer()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, NewCast)
		_ = fcTC.BenchMarkRun()
	}
	b.StopTimer()
}

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
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{-23}, []bool{false}),
		},
		{
			info: "int16 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{-23}, []bool{false}),
		},
		{
			info: "int32 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{-23}, []bool{false}),
		},
		{
			info: "int64 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-23}, []bool{false}),
		},
		{
			info: "uint8 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{23}, []bool{false}),
		},
		{
			info: "uint16 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{23}, []bool{false}),
		},
		{
			info: "uint32 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{23}, []bool{false}),
		},
		{
			info: "uint64 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{23}, []bool{false}),
		},
		{
			info: "float32 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{23.5}, []bool{false}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{23.5}, []bool{false}),
		},
		{
			info: "float64 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{23.5}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{23.5}, []bool{false}),
		},
		{
			info: "date to date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{729848}, []bool{false}),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{729848}, []bool{false}),
		},
		{
			info: "datetime to datetime",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{66122056321728512}, []bool{false}),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{}, []bool{}),
			},
			// When casting to datetime(0), .728512 microseconds should round to next second
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{66122056322000000}, []bool{false}),
		},
		{
			info: "timestamp to timestamp",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(),
					[]types.Timestamp{66122026122739712}, []bool{false}),
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{}, []bool{}),
			},
			// When casting to timestamp(0), .739712 microseconds should round to next second
			expect: NewFunctionTestResult(types.T_timestamp.ToType(), false,
				[]types.Timestamp{66122026123000000}, []bool{false}),
		},
		{
			info: "time to time",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{661220261227}, []bool{false}),
				NewFunctionTestInput(types.T_time.ToType(), []types.Time{}, []bool{}),
			},
			// When casting to time(0), .261227 microseconds should be truncated (2 < 5, no rounding)
			expect: NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{661220000000}, []bool{false}),
		},
		{
			info: "vecf32 to vecf32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{}}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}}, []bool{false}),
		},
		{
			info: "vecf64 to vecf64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}}, []bool{false}),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{}}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}}, []bool{false}),
		},
		{
			info: "bit to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{23}, []bool{false}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false, []uint64{23}, []bool{false}),
		},
	}
	castInt8ToOthers := []tcTemp{
		// test cast int8 to others.
		{
			info: "int8 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int8 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(), []int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt16ToOthers := []tcTemp{
		// test cast int16 to others.
		{
			info: "int16 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int16 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(), []int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt32ToOthers := []tcTemp{
		// test cast int32 to others.
		{
			info: "int32 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int32 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(), []int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt64ToOthers := []tcTemp{
		// test cast int64 to others.
		{
			info: "int64 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int64 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint8ToOthers := []tcTemp{
		// test cast uint8 to others.
		{
			info: "uint8 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint8 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint16ToOthers := []tcTemp{
		// test cast uint16 to others.
		{
			info: "uint16 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint16 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint32ToOthers := []tcTemp{
		// test cast uint32 to others.
		{
			info: "uint32 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint32 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint64ToOthers := []tcTemp{
		// test cast uint64 to others.
		{
			info: "uint64 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint64 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.New(types.T_bit, 5, 0), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), true, []uint64{}, []bool{}),
		},
	}
	castFloat32ToOthers := []tcTemp{
		// test cast float32 to others.
		{
			info: "float32 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{23.56, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_char.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_char.ToType(), false,
				[]string{"23.56", "126", "0"}, []bool{false, false, true}),
		},
		{
			info: "float32 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125.1, 0}, []bool{false, true}),
				NewFunctionTestInput(types.New(types.T_decimal128, 8, 1), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 8, 1), false,
				[]types.Decimal128{f1251ToDec128, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "float32 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(), []float32{125.4999, 125.55555, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castFloat64ToOthers := []tcTemp{
		// test cast float64 to others.
		{
			info: "float64 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{23.56, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_char.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_char.ToType(), false,
				[]string{"23.56", "126", "0"}, []bool{false, false, true}),
		},
		{
			info: "float64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125.1, 0}, []bool{false, true}),
				NewFunctionTestInput(types.New(types.T_decimal128, 8, 1), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 8, 1), false,
				[]types.Decimal128{f1251ToDec128, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "float64 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{125.4999, 125.55555, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castStrToOthers := []tcTemp{
		{
			info: "str type to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501", "16", ""}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1501, 16, 0}, []bool{false, false, true}),
		},
		{
			info: "str type to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501", "16", ""}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1501, 16, 0}, []bool{false, false, true}),
		},
		{
			info: "str type to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501.12", "16", ""}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1501.12, 16, 0}, []bool{false, false, true}),
		},
		{
			info: "str type to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501.12", "16", ""}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_text.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_text.ToType(), false,
				[]string{"1501.12", "16", ""}, []bool{false, false, true}),
		},
		{
			info: "str type to date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2004-04-03", "2004-04-03", "2021-10-03"},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, []bool{}),
			},
			expect: NewFunctionTestResult(
				types.T_date.ToType(), false,
				[]types.Date{
					s01date, s01date, s02date,
				},
				[]bool{false, false, false}),
		},
		{
			info: "str type to vecf32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]", "[4,5,6]"}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
		{
			info: "str type to vecf64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]", "[4,5,6]"}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
		{
			info: "str type to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"15", "ab"}, nil),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{12597, 24930}, []bool{false, false}),
		},
	}
	castDecToOthers := []tcTemp{
		{
			info: "decimal64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 0),
					[]types.Decimal64{types.Decimal64(333333000)}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 0),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 0), false,
				[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
		},
		{
			info: "decimal64(10,5) to decimal64(10, 4)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(33333300)}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 4),
					[]types.Decimal64{0}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal64, 10, 4), false,
				[]types.Decimal64{types.Decimal64(3333330)}, nil),
		},
		{
			info: "decimal64(10,5) to decimal128(20, 5)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(333333000)}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 5),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 5), false,
				[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
		},
		{
			info: "decimal128(20,5) to decimal128(20, 4)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 5),
					[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 4),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 4), false,
				[]types.Decimal128{{B0_63: 33333300, B64_127: 0}}, nil),
		},
		{
			info: "decimal64 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(1234)}, nil),
				NewFunctionTestInput(
					types.T_varchar.ToType(),
					[]string{}, nil),
			},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"0.01234"}, nil),
		},
		{
			info: "decimal128 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 2),
					[]types.Decimal128{{B0_63: 1234, B64_127: 0}}, nil),
				NewFunctionTestInput(
					types.T_varchar.ToType(),
					[]string{}, nil),
			},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"12.34"}, nil),
		},
	}
	castTimestampToOthers := []tcTemp{
		{
			info: "timestamp to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.T_timestamp.ToType(),
					[]types.Timestamp{s01ts}, nil),
				NewFunctionTestInput(
					types.T_varchar.ToType(), []string{}, nil),
			},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"2020-08-23 11:52:21"}, nil),
		},
	}

	castArrayFloat32ToOthers := []tcTemp{
		{
			info: "vecf32 type to vecf64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
	}

	castArrayFloat64ToOthers := []tcTemp{
		{
			info: "vecf64 type to vecf32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
	}

	castBitToOthers := []tcTemp{
		// test cast bit to others.
		{
			info: "bit to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 256, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), true, []int8{}, []bool{}),
		},
		{
			info: "bit to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
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
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, NewCast)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func BenchmarkCast(b *testing.B) {
	testCases := initCastTestCase()
	proc := testutil.NewProcess(b)

	b.StartTimer()
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, NewCast)
		_ = fcTC.BenchMarkRun()
	}
	b.StopTimer()
}

func Test_strToSigned_Binary(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	tests := []struct {
		name      string
		inputs    [][]byte
		nulls     []uint64
		bitSize   int
		want      []int64
		wantNulls []uint64
		wantErr   bool
		errMsg    string
	}{
		{
			name:    "empty slice",
			inputs:  [][]byte{{}},
			bitSize: 64,
			wantErr: true,
			errMsg:  "invalid arg",
		},
		{
			name:    "out of range length",
			inputs:  [][]byte{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}},
			bitSize: 64,
			wantErr: true,
			errMsg:  "out of range",
		},
		{
			name:    "max int64",
			inputs:  [][]byte{{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
			bitSize: 64,
			want:    []int64{math.MaxInt64},
		},
		{
			name:    "valid binary",
			inputs:  [][]byte{{0x12, 0x34}},
			bitSize: 64,
			want:    []int64{0x1234},
		},
		{
			name:      "null value",
			inputs:    [][]byte{nil, {0x12}},
			nulls:     []uint64{0},
			bitSize:   64,
			want:      []int64{0, 0x12},
			wantNulls: []uint64{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputVec := testutil.MakeVarlenaVector(tt.inputs, tt.nulls, mp)
			defer inputVec.Free(mp)
			inputVec.SetIsBin(true)

			from := vector.GenerateFunctionStrParameter(inputVec)

			resultType := types.T_int64.ToType()
			to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[int64])
			defer to.Free()
			err := to.PreExtendAndReset(len(tt.inputs))
			require.NoError(t, err)

			err = strToSigned(ctx, from, to, tt.bitSize, len(tt.inputs), nil)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
				return
			}
			require.NoError(t, err)

			resultVec := to.GetResultVector()

			r := vector.GenerateFunctionFixedTypeParameter[int64](resultVec)

			for i := 0; i < len(tt.want); i++ {
				want := tt.want[i]
				get, null := r.GetValue(uint64(i))

				if contains(tt.wantNulls, uint64(i)) {
					require.True(t, null, "row %d should be null", i)
				} else {
					require.False(t, null, "row %d should not be null", i)
					require.Equal(t, want, get, "row %d value not match", i)
				}
			}

			resultNulls := to.GetResultVector().GetNulls()
			if len(tt.wantNulls) > 0 {
				for _, pos := range tt.wantNulls {
					require.True(t, resultNulls.Contains(pos))
				}
			} else {
				require.True(t, resultNulls.IsEmpty())
			}
		})
	}
}

func contains(slice []uint64, item uint64) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func Benchmark_strToSigned_Binary(b *testing.B) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	benchCases := []struct {
		name    string
		inputs  [][]byte
		nulls   []uint64
		bitSize int
	}{
		{
			name:    "small_binary",
			inputs:  [][]byte{{0x12, 0x34}, {0x56, 0x78}},
			bitSize: 64,
		},
		{
			name:    "max_int64",
			inputs:  [][]byte{{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
			bitSize: 64,
		},
		{
			name:    "mixed_with_nulls",
			inputs:  [][]byte{nil, {0x12}, {0x34}, nil, {0x56}},
			nulls:   []uint64{0, 3},
			bitSize: 64,
		},
		{
			name: "large_input_set",
			inputs: func() [][]byte {
				data := make([][]byte, 1000)
				for i := range data {
					data[i] = []byte{byte(i % 256), byte((i + 1) % 256)}
				}
				return data
			}(),
			bitSize: 64,
		},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			inputVec := testutil.MakeVarlenaVector(bc.inputs, bc.nulls, mp)
			defer inputVec.Free(mp)
			inputVec.SetIsBin(true)

			resultType := types.T_int64.ToType()

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[int64])
				err := to.PreExtendAndReset(len(bc.inputs))
				if err != nil {
					b.Fatalf("PreExtendAndReset failed: %v", err)
				}

				from := vector.GenerateFunctionStrParameter(inputVec)

				err = strToSigned(ctx, from, to, bc.bitSize, len(bc.inputs), nil)
				if err != nil {
					b.Fatalf("strToSigned failed: %v", err)
				}

			to.Free()
		}
	})
	}
}

// Test_strToStr_TextToCharVarchar tests that TEXT type can be cast to CHAR/VARCHAR
// without length validation errors, even when the string length exceeds the target length.
// This is important for UPDATE operations on TEXT columns with CONCAT operations.
func Test_strToStr_TextToCharVarchar(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	// Helper function to create long strings
	longString260 := strings.Repeat("a", 260)  // 260 characters
	longString100 := strings.Repeat("b", 100)  // 100 characters

	tests := []struct {
		name      string
		inputs    []string
		nulls     []uint64
		fromType  types.Type
		toType    types.Type
		want      []string
		wantNulls []uint64
		wantErr   bool
		errMsg    string
	}{
		{
			name:     "TEXT to CHAR(255) with length 260 - should succeed",
			inputs:   []string{longString260},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 255, 0),
			want:     []string{longString260}, // Should keep original length
			wantErr:  false,
		},
		{
			name:     "TEXT to VARCHAR(255) with length 260 - should succeed",
			inputs:   []string{longString260},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_varchar, 255, 0),
			want:     []string{longString260}, // Should keep original length
			wantErr:  false,
		},
		{
			name:     "TEXT to CHAR(10) with length 100 - should succeed",
			inputs:   []string{longString100},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 10, 0),
			want:     []string{longString100}, // Should keep original length
			wantErr:  false,
		},
		{
			name:     "TEXT to VARCHAR(10) with length 100 - should succeed",
			inputs:   []string{longString100},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_varchar, 10, 0),
			want:     []string{longString100}, // Should keep original length
			wantErr:  false,
		},
		{
			name:      "TEXT to CHAR(255) with NULL - should handle NULL",
			inputs:    []string{"", "test"},
			nulls:     []uint64{0},
			fromType:  types.T_text.ToType(),
			toType:    types.New(types.T_char, 255, 0),
			want:      []string{"", "test"},
			wantNulls: []uint64{0},
			wantErr:   false,
		},
		{
			name:     "VARCHAR to CHAR(10) with length 100 - should fail (normal behavior)",
			inputs:   []string{longString100},
			fromType: types.New(types.T_varchar, 100, 0),
			toType:   types.New(types.T_char, 10, 0),
			wantErr:  true,
			errMsg:   "larger than Dest length",
		},
		{
			name:     "TEXT to TEXT - should succeed",
			inputs:   []string{"test text"},
			fromType: types.T_text.ToType(),
			toType:   types.T_text.ToType(),
			want:     []string{"test text"},
			wantErr:  false,
		},
		{
			name:     "TEXT to CHAR(255) with multiple values",
			inputs:   []string{"short", longString260, "medium length string"},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 255, 0),
			want:     []string{"short", longString260, "medium length string"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create input vector based on source type
			var inputVec *vector.Vector
			if tt.fromType.Oid == types.T_text {
				inputVec = testutil.MakeTextVector(tt.inputs, tt.nulls)
			} else {
				inputVec = testutil.MakeVarcharVector(tt.inputs, tt.nulls)
				// Set the type explicitly for non-TEXT types
				inputVec.SetType(tt.fromType)
			}
			defer inputVec.Free(mp)

			from := vector.GenerateFunctionStrParameter(inputVec)

			resultType := tt.toType
			to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[types.Varlena])
			defer to.Free()
			err := to.PreExtendAndReset(len(tt.inputs))
			require.NoError(t, err)

			err = strToStr(ctx, from, to, len(tt.inputs), tt.toType)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					require.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}
			require.NoError(t, err)

			resultVec := to.GetResultVector()
			r := vector.GenerateFunctionStrParameter(resultVec)

			for i := 0; i < len(tt.want); i++ {
				want := tt.want[i]
				get, null := r.GetStrValue(uint64(i))

				if contains(tt.wantNulls, uint64(i)) {
					require.True(t, null, "row %d should be null", i)
				} else {
					require.False(t, null, "row %d should not be null", i)
					require.Equal(t, want, string(get), "row %d value not match", i)
				}
			}

			resultNulls := to.GetResultVector().GetNulls()
			if len(tt.wantNulls) > 0 {
				for _, pos := range tt.wantNulls {
					require.True(t, resultNulls.Contains(pos))
				}
			} else {
				require.True(t, resultNulls.IsEmpty())
			}
		})
	}
}

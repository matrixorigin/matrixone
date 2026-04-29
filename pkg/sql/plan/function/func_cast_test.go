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
	"strconv"
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
	longString260 := strings.Repeat("a", 260) // 260 characters
	longString100 := strings.Repeat("b", 100) // 100 characters

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
			name:     "TEXT to CHAR(1) with length > 1 - should fail (explicit CAST)",
			inputs:   []string{"ab"},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 1, 0),
			wantErr:  true,
			errMsg:   "larger than Dest length",
		},
		{
			name:     "TEXT to CHAR(10) with length 100 - should fail (explicit CAST to small width)",
			inputs:   []string{longString100},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 10, 0),
			wantErr:  true,
			errMsg:   "larger than Dest length",
		},
		{
			name:     "TEXT to VARCHAR(10) with length 100 - should fail (explicit CAST to small width)",
			inputs:   []string{longString100},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_varchar, 10, 0),
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

func Test_strToDecimal256(t *testing.T) {
	mp := mpool.MustNewZero()
	inputVec := testutil.MakeVarcharVector(
		[]string{"123456789012345678901234567890.1234", "", "1e+06"},
		[]uint64{1},
	)
	defer inputVec.Free(mp)

	from := vector.GenerateFunctionStrParameter(inputVec)
	resultType := types.New(types.T_decimal256, 65, 4)
	to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[types.Decimal256])
	defer to.Free()

	require.NoError(t, to.PreExtendAndReset(3))
	require.NoError(t, strToDecimal256(from, to, 3, nil))

	resultVec := to.GetResultVector()
	require.True(t, resultVec.GetNulls().Contains(1))
	values := vector.MustFixedColNoTypeCheck[types.Decimal256](resultVec)
	require.Equal(t, "123456789012345678901234567890.1234", values[0].Format(4))
	require.Equal(t, "1000000.0000", values[2].Format(4))
}

func TestDecimal256ToFloat64Cast(t *testing.T) {
	ctx := context.Background()
	fromType := types.New(types.T_decimal256, 65, 16)
	toType := types.T_float64.ToType()
	_, err := GetFunctionByName(ctx, "cast", []types.Type{fromType, toType})
	require.NoError(t, err)

	mp := mpool.MustNewZero()
	inputVec := vector.NewVec(fromType)
	defer inputVec.Free(mp)

	value, err := types.ParseDecimal256(
		"12421512141241241241241241849912840129402.1241124124241241",
		65,
		16,
	)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixedList(inputVec, []types.Decimal256{value}, nil, mp))

	targetVec := vector.NewVec(toType)
	defer targetVec.Free(mp)

	result := vector.NewFunctionResultWrapper(toType, mp).(*vector.FunctionResult[float64])
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))

	proc := testutil.NewProcess(t)
	require.NoError(t, NewCast([]*vector.Vector{inputVec, targetVec}, result, proc, 1, nil))

	resultValues := vector.MustFixedColNoTypeCheck[float64](result.GetResultVector())
	require.InDelta(t, 1.2421512141241241e40, resultValues[0], 1e27)
}

func TestYearCastHelpers(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	inputVec := testutil.MakeVarcharVector([]string{"69", "1901", "0"}, nil)
	defer inputVec.Free(mp)
	from := vector.GenerateFunctionStrParameter(inputVec)
	to := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer to.Free()

	require.NoError(t, to.PreExtendAndReset(3))
	require.NoError(t, strToYear(ctx, from, to, 3, nil, types.T_varchar.ToType()))

	yearVec := to.GetResultVector()
	years := vector.MustFixedColNoTypeCheck[types.MoYear](yearVec)
	require.Equal(t, types.MoYear(2069), years[0])
	require.Equal(t, types.MoYear(1901), years[1])
	require.Equal(t, types.MoYear(0), years[2])

	strResult := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp).(*vector.FunctionResult[types.Varlena])
	defer strResult.Free()
	require.NoError(t, strResult.PreExtendAndReset(3))
	require.NoError(t, yearToStr(ctx, vector.GenerateFunctionFixedTypeParameter[types.MoYear](yearVec), strResult, 3, types.T_varchar.ToType()))
	strParam := vector.GenerateFunctionStrParameter(strResult.GetResultVector())
	got, null := strParam.GetStrValue(0)
	require.False(t, null)
	require.Equal(t, "2069", string(got))
}

func TestYearCastRejectsInvalidValues(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	inputVec := testutil.MakeVarcharVector([]string{"2156", "abcd"}, nil)
	defer inputVec.Free(mp)
	strResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer strResult.Free()
	require.NoError(t, strResult.PreExtendAndReset(2))
	require.Error(t, strToYear(ctx, vector.GenerateFunctionStrParameter(inputVec), strResult, 2, nil, types.T_varchar.ToType()))

	// VARBINARY / BLOB payloads must not be NUL-trimmed: ['0', 0x00] must not
	// be silently accepted as YEAR 0.
	varBinPayload := []byte{'0', 0x00}
	varBinVec := testutil.MakeVarcharVector([]string{string(varBinPayload)}, nil)
	defer varBinVec.Free(mp)
	varBinResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer varBinResult.Free()
	require.NoError(t, varBinResult.PreExtendAndReset(1))
	require.Error(t, strToYear(ctx, vector.GenerateFunctionStrParameter(varBinVec), varBinResult, 1, nil, types.T_varbinary.ToType()))

	// Fixed-length BINARY right-pads with NUL; keep that behavior and still
	// parse the logical value. '0' padded with NUL should become YEAR 0.
	binVec := testutil.MakeVarcharVector([]string{string(varBinPayload)}, nil)
	defer binVec.Free(mp)
	binResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer binResult.Free()
	require.NoError(t, binResult.PreExtendAndReset(1))
	require.NoError(t, strToYear(ctx, vector.GenerateFunctionStrParameter(binVec), binResult, 1, nil, types.T_binary.ToType()))

	intVec := vector.NewVec(types.T_int64.ToType())
	defer intVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(intVec, []int64{2156}, nil, mp))
	intResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer intResult.Free()
	require.NoError(t, intResult.PreExtendAndReset(1))
	require.Error(t, integerToYear(ctx, vector.GenerateFunctionFixedTypeParameter[int64](intVec), intResult, 1, nil))
}

func TestFloatToYearRoundsBeforeRangeCheck(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	// 2155.6 should round to 2156 and be rejected as out-of-range (not silently
	// truncated to 2155 as the old int64(v) cast did).
	floatVec := vector.NewVec(types.T_float64.ToType())
	defer floatVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(floatVec, []float64{2155.6}, nil, mp))
	floatResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer floatResult.Free()
	require.NoError(t, floatResult.PreExtendAndReset(1))
	require.Error(t, floatToYear(ctx, vector.GenerateFunctionFixedTypeParameter[float64](floatVec), floatResult, 1, nil))

	// 2069.4 rounds to 2069, which maps to year 2069 via the century window.
	okVec := vector.NewVec(types.T_float64.ToType())
	defer okVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(okVec, []float64{69.4}, nil, mp))
	okResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer okResult.Free()
	require.NoError(t, okResult.PreExtendAndReset(1))
	require.NoError(t, floatToYear(ctx, vector.GenerateFunctionFixedTypeParameter[float64](okVec), okResult, 1, nil))
	years := vector.MustFixedColNoTypeCheck[types.MoYear](okResult.GetResultVector())
	require.Equal(t, types.MoYear(2069), years[0])

	// NaN / Inf are rejected up front.
	nanVec := vector.NewVec(types.T_float64.ToType())
	defer nanVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(nanVec, []float64{math.NaN()}, nil, mp))
	nanResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer nanResult.Free()
	require.NoError(t, nanResult.PreExtendAndReset(1))
	require.Error(t, floatToYear(ctx, vector.GenerateFunctionFixedTypeParameter[float64](nanVec), nanResult, 1, nil))

	// Null float propagates as null-year without error.
	nullVec := vector.NewVec(types.T_float64.ToType())
	defer nullVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(nullVec, []float64{0}, []bool{true}, mp))
	nullResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer nullResult.Free()
	require.NoError(t, nullResult.PreExtendAndReset(1))
	require.NoError(t, floatToYear(ctx, vector.GenerateFunctionFixedTypeParameter[float64](nullVec), nullResult, 1, nil))
	nullYears := vector.MustFixedColNoTypeCheck[types.MoYear](nullResult.GetResultVector())
	require.Equal(t, types.MoYear(0), nullYears[0])
}

// TestDecimalToFloatSinglePrecisionParse asserts decimal128ToFloat and
// decimal256ToFloat parse the textual form exactly once at the target float
// precision. The previous implementation parsed at float32, then re-parsed
// the same string at float64 and widened — defeating the float32 quantization
// the caller asked for. We verify by matching the result against a direct
// strconv.ParseFloat at the matching bitSize.
func TestDecimalToFloatSinglePrecisionParse(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	// Use a string whose float32 vs float64 parses differ at the double
	// level: at float32 precision 3.14159265 rounds to approximately
	// 3.1415927, at float64 it stays at 3.14159265. Widening a float32
	// parse back to float64 gives 3.1415927... instead of 3.14159265.
	str := "3.14159265"
	wantParsed32, err := strconv.ParseFloat(str, 32)
	require.NoError(t, err)
	wantParsed64, err := strconv.ParseFloat(str, 64)
	require.NoError(t, err)
	// Sanity: the float32 parse (held in a float64) is not equal to the
	// float64 parse. Without this the test can't distinguish the old and
	// new behaviors.
	require.NotEqual(t, wantParsed32, wantParsed64,
		"test input should parse to different float64 values at the two precisions")

	// decimal128 -> float32
	d128Vec := vector.NewVec(types.T_decimal128.ToType())
	defer d128Vec.Free(mp)
	d128, err := types.ParseDecimal128(str, 38, 8)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixedList(d128Vec, []types.Decimal128{d128}, nil, mp))
	// Override the scale so Format picks up the right number of fractional
	// digits; the vec Oid must match what the helper expects.
	d128Vec.SetType(types.Type{Oid: types.T_decimal128, Width: 38, Scale: 8})

	f32Result := vector.NewFunctionResultWrapper(types.T_float32.ToType(), mp).(*vector.FunctionResult[float32])
	defer f32Result.Free()
	require.NoError(t, f32Result.PreExtendAndReset(1))
	require.NoError(t, decimal128ToFloat[float32](
		ctx,
		vector.GenerateFunctionFixedTypeParameter[types.Decimal128](d128Vec),
		f32Result, 1, 32))
	got := vector.MustFixedColNoTypeCheck[float32](f32Result.GetResultVector())[0]
	require.Equal(t, float32(wantParsed32), got,
		"decimal128ToFloat(bitSize=32) should match ParseFloat(str, 32); got %v, wantParsed32=%v, wantParsed64=%v",
		got, wantParsed32, wantParsed64)

	// decimal256 -> float32
	d256Vec := vector.NewVec(types.T_decimal256.ToType())
	defer d256Vec.Free(mp)
	d256, err := types.ParseDecimal256(str, 65, 8)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixedList(d256Vec, []types.Decimal256{d256}, nil, mp))
	d256Vec.SetType(types.Type{Oid: types.T_decimal256, Width: 65, Scale: 8})

	f32Result2 := vector.NewFunctionResultWrapper(types.T_float32.ToType(), mp).(*vector.FunctionResult[float32])
	defer f32Result2.Free()
	require.NoError(t, f32Result2.PreExtendAndReset(1))
	require.NoError(t, decimal256ToFloat[float32](
		ctx,
		vector.GenerateFunctionFixedTypeParameter[types.Decimal256](d256Vec),
		f32Result2, 1, 32))
	got2 := vector.MustFixedColNoTypeCheck[float32](f32Result2.GetResultVector())[0]
	require.Equal(t, float32(wantParsed32), got2,
		"decimal256ToFloat(bitSize=32) should match ParseFloat(str, 32)")
}

// TestDecimal256ToOthersRouting sanity-checks the cast-target matrix wired
// through decimal256ToOthers by invoking each helper with a small input.
// Having these helpers in place means CAST(decimal256_col AS VARCHAR) and
// similar statements no longer hit "unsupported cast".
func TestDecimal256ToOthersRouting(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	d256Typ := types.T_decimal256.ToType()
	d256, err := types.ParseDecimal256("42", 65, 0)
	require.NoError(t, err)
	srcVec := vector.NewVec(d256Typ)
	defer srcVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(srcVec, []types.Decimal256{d256}, nil, mp))
	src := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](srcVec)

	// int64
	intRes := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp).(*vector.FunctionResult[int64])
	defer intRes.Free()
	require.NoError(t, intRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToSigned[int64](ctx, src, intRes, 64, 1))
	require.Equal(t, int64(42), vector.MustFixedColNoTypeCheck[int64](intRes.GetResultVector())[0])

	// uint32
	uRes := vector.NewFunctionResultWrapper(types.T_uint32.ToType(), mp).(*vector.FunctionResult[uint32])
	defer uRes.Free()
	require.NoError(t, uRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToUnsigned[uint32](ctx, src, uRes, 32, 1))
	require.Equal(t, uint32(42), vector.MustFixedColNoTypeCheck[uint32](uRes.GetResultVector())[0])

	// bit
	bitRes := vector.NewFunctionResultWrapper(types.T_bit.ToType(), mp).(*vector.FunctionResult[uint64])
	defer bitRes.Free()
	require.NoError(t, bitRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToBit(ctx, src, bitRes, 64, 1))
	require.Equal(t, uint64(42), vector.MustFixedColNoTypeCheck[uint64](bitRes.GetResultVector())[0])

	// decimal64
	d64Res := vector.NewFunctionResultWrapper(types.T_decimal64.ToType(), mp).(*vector.FunctionResult[types.Decimal64])
	defer d64Res.Free()
	require.NoError(t, d64Res.PreExtendAndReset(1))
	require.NoError(t, decimal256ToDecimal64(ctx, src, d64Res, 1))

	// decimal128
	d128Res := vector.NewFunctionResultWrapper(types.T_decimal128.ToType(), mp).(*vector.FunctionResult[types.Decimal128])
	defer d128Res.Free()
	require.NoError(t, d128Res.PreExtendAndReset(1))
	require.NoError(t, decimal256ToDecimal128(ctx, src, d128Res, 1))

	// decimal256 -> decimal256 narrow
	d256bRes := vector.NewFunctionResultWrapper(types.T_decimal256.ToType(), mp).(*vector.FunctionResult[types.Decimal256])
	defer d256bRes.Free()
	require.NoError(t, d256bRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToDecimal256(src, d256bRes, 1))

	// varchar
	vcTyp := types.T_varchar.ToType()
	vcTyp.Width = 64
	vcRes := vector.NewFunctionResultWrapper(vcTyp, mp).(*vector.FunctionResult[types.Varlena])
	defer vcRes.Free()
	require.NoError(t, vcRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToStr(ctx, src, vcRes, 1, vcTyp))
	strParam := vector.GenerateFunctionStrParameter(vcRes.GetResultVector())
	got, null := strParam.GetStrValue(0)
	require.False(t, null)
	require.Equal(t, "42", string(got))
}

func TestIntegerToYearAcrossWidths(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	// all integer widths feeding a valid and an invalid YEAR value
	cases := []struct {
		name    string
		valid   int64
		valOK   types.MoYear
		invalid int64
	}{
		{"int8", 69, 2069, -1},
		{"int16", 2155, 2155, 2156},
		{"int32", 1901, 1901, 1900},
		{"int64", 69, 2069, 3000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			okVec := vector.NewVec(types.T_int64.ToType())
			defer okVec.Free(mp)
			require.NoError(t, vector.AppendFixedList(okVec, []int64{c.valid}, nil, mp))
			okResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
			defer okResult.Free()
			require.NoError(t, okResult.PreExtendAndReset(1))
			require.NoError(t, integerToYear(ctx, vector.GenerateFunctionFixedTypeParameter[int64](okVec), okResult, 1, nil))
			years := vector.MustFixedColNoTypeCheck[types.MoYear](okResult.GetResultVector())
			require.Equal(t, c.valOK, years[0])

			badVec := vector.NewVec(types.T_int64.ToType())
			defer badVec.Free(mp)
			require.NoError(t, vector.AppendFixedList(badVec, []int64{c.invalid}, nil, mp))
			badResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
			defer badResult.Free()
			require.NoError(t, badResult.PreExtendAndReset(1))
			require.Error(t, integerToYear(ctx, vector.GenerateFunctionFixedTypeParameter[int64](badVec), badResult, 1, nil))
		})
	}

	// null integer propagates as null-year.
	nullVec := vector.NewVec(types.T_int64.ToType())
	defer nullVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(nullVec, []int64{0}, []bool{true}, mp))
	nullResult := vector.NewFunctionResultWrapper(types.T_year.ToType(), mp).(*vector.FunctionResult[types.MoYear])
	defer nullResult.Free()
	require.NoError(t, nullResult.PreExtendAndReset(1))
	require.NoError(t, integerToYear(ctx, vector.GenerateFunctionFixedTypeParameter[int64](nullVec), nullResult, 1, nil))
	nullYears := vector.MustFixedColNoTypeCheck[types.MoYear](nullResult.GetResultVector())
	require.Equal(t, types.MoYear(0), nullYears[0])
}

func TestYearToStringPath(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	// year 2024 -> "2024"
	yearVec := vector.NewVec(types.T_year.ToType())
	defer yearVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(yearVec, []types.MoYear{2024}, nil, mp))
	strResult := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp).(*vector.FunctionResult[types.Varlena])
	defer strResult.Free()
	require.NoError(t, strResult.PreExtendAndReset(1))
	require.NoError(t, yearToStr(ctx, vector.GenerateFunctionFixedTypeParameter[types.MoYear](yearVec), strResult, 1, types.T_varchar.ToType()))
	strParam := vector.GenerateFunctionStrParameter(strResult.GetResultVector())
	got, null := strParam.GetStrValue(0)
	require.False(t, null)
	require.Equal(t, "2024", string(got))

	// null year -> null string
	nullYear := vector.NewVec(types.T_year.ToType())
	defer nullYear.Free(mp)
	require.NoError(t, vector.AppendFixedList(nullYear, []types.MoYear{0}, []bool{true}, mp))
	nullStrResult := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp).(*vector.FunctionResult[types.Varlena])
	defer nullStrResult.Free()
	require.NoError(t, nullStrResult.PreExtendAndReset(1))
	require.NoError(t, yearToStr(ctx, vector.GenerateFunctionFixedTypeParameter[types.MoYear](nullYear), nullStrResult, 1, types.T_varchar.ToType()))
}

func TestYearToOthersCoversSupportedNumericMatrix(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	yearVec := vector.NewVec(types.T_year.ToType())
	defer yearVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(yearVec, []types.MoYear{0}, nil, mp))
	from := vector.GenerateFunctionFixedTypeParameter[types.MoYear](yearVec)

	targets := []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint8.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_blob.ToType(),
		types.T_text.ToType(),
		types.T_binary.ToType(),
		types.T_varbinary.ToType(),
	}

	for _, target := range targets {
		t.Run(target.Oid.String(), func(t *testing.T) {
			result := vector.NewFunctionResultWrapper(target, mp)
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))
			require.NoError(t, yearToOthers(ctx, from, target, result, 1, nil))
			require.False(t, result.GetResultVector().IsNull(0))
		})
	}
}

func TestYearToIntegerRejectsOverflow(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	yearVec := vector.NewVec(types.T_year.ToType())
	defer yearVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(yearVec, []types.MoYear{2024}, nil, mp))
	from := vector.GenerateFunctionFixedTypeParameter[types.MoYear](yearVec)

	int8Result := vector.NewFunctionResultWrapper(types.T_int8.ToType(), mp).(*vector.FunctionResult[int8])
	defer int8Result.Free()
	require.NoError(t, int8Result.PreExtendAndReset(1))
	require.Error(t, yearToOthers(ctx, from, types.T_int8.ToType(), int8Result, 1, nil))

	uint8Result := vector.NewFunctionResultWrapper(types.T_uint8.ToType(), mp).(*vector.FunctionResult[uint8])
	defer uint8Result.Free()
	require.NoError(t, uint8Result.PreExtendAndReset(1))
	require.Error(t, yearToOthers(ctx, from, types.T_uint8.ToType(), uint8Result, 1, nil))

	int16Result := vector.NewFunctionResultWrapper(types.T_int16.ToType(), mp).(*vector.FunctionResult[int16])
	defer int16Result.Free()
	require.NoError(t, int16Result.PreExtendAndReset(1))
	require.NoError(t, yearToOthers(ctx, from, types.T_int16.ToType(), int16Result, 1, nil))
}

func TestYearToStringRespectsTargetWidthAndBinaryPadding(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	yearVec := vector.NewVec(types.T_year.ToType())
	defer yearVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(yearVec, []types.MoYear{2024}, nil, mp))
	from := vector.GenerateFunctionFixedTypeParameter[types.MoYear](yearVec)

	charType := types.New(types.T_char, 2, 0)
	charResult := vector.NewFunctionResultWrapper(charType, mp).(*vector.FunctionResult[types.Varlena])
	defer charResult.Free()
	require.NoError(t, charResult.PreExtendAndReset(1))
	require.Error(t, yearToOthers(ctx, from, charType, charResult, 1, nil))

	binaryType := types.New(types.T_binary, 6, -1)
	binaryResult := vector.NewFunctionResultWrapper(binaryType, mp).(*vector.FunctionResult[types.Varlena])
	defer binaryResult.Free()
	require.NoError(t, binaryResult.PreExtendAndReset(1))
	require.NoError(t, yearToOthers(ctx, from, binaryType, binaryResult, 1, nil))
	binaryParam := vector.GenerateFunctionStrParameter(binaryResult.GetResultVector())
	got, null := binaryParam.GetStrValue(0)
	require.False(t, null)
	require.Equal(t, []byte{'2', '0', '2', '4', 0, 0}, got)

	varbinaryType := types.New(types.T_varbinary, 4, 0)
	varbinaryResult := vector.NewFunctionResultWrapper(varbinaryType, mp).(*vector.FunctionResult[types.Varlena])
	defer varbinaryResult.Free()
	require.NoError(t, varbinaryResult.PreExtendAndReset(1))
	require.NoError(t, yearToOthers(ctx, from, varbinaryType, varbinaryResult, 1, nil))
	varbinaryParam := vector.GenerateFunctionStrParameter(varbinaryResult.GetResultVector())
	got, null = varbinaryParam.GetStrValue(0)
	require.False(t, null)
	require.Equal(t, []byte("2024"), got)
}

func TestScalarNullToDecimal256(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	resultType := types.New(types.T_decimal256, 65, 4)
	result := vector.NewFunctionResultWrapper(resultType, mp)
	defer result.Free()

	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, scalarNullToOthers(ctx, resultType, result, 1, nil))
	require.True(t, result.GetResultVector().IsNull(0))
}

func TestAnyCastMatrixIncludesYearAndDecimal256(t *testing.T) {
	ctx := context.Background()
	_, err := GetFunctionByName(ctx, "cast", []types.Type{types.T_any.ToType(), types.T_year.ToType()})
	require.NoError(t, err)

	decimal256Type := types.New(types.T_decimal256, 65, 30)
	_, err = GetFunctionByName(ctx, "cast", []types.Type{types.T_any.ToType(), decimal256Type})
	require.NoError(t, err)
}

// Test_strToArray_DimensionCheck tests that strToArray correctly validates
// vector dimension against the target type's width (vecf32(N) / vecf64(N)).
// This is the regression test for https://github.com/matrixorigin/matrixone/issues/23872
func Test_strToArray_DimensionCheck(t *testing.T) {
	proc := testutil.NewProcess(t)

	// vecf32(3): dimension match should succeed
	tc1 := tcTemp{
		info: "str to vecf32(3) - dimension match",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]"}, nil),
			NewFunctionTestInput(types.New(types.T_array_float32, 3, 0), [][]float32{}, nil),
		},
		expect: NewFunctionTestResult(types.New(types.T_array_float32, 3, 0), false,
			[][]float32{{1, 2, 3}}, []bool{false}),
	}
	fcTC := NewFunctionTestCase(proc, tc1.inputs, tc1.expect, NewCast)
	s, info := fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc1.info, info))

	// vecf32(3): dimension mismatch (5-dim into vecf32(3)) should fail
	tc2 := tcTemp{
		info: "str to vecf32(3) - dimension mismatch should error",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3,4,5]"}, nil),
			NewFunctionTestInput(types.New(types.T_array_float32, 3, 0), [][]float32{}, nil),
		},
		expect: NewFunctionTestResult(types.New(types.T_array_float32, 3, 0), true, nil, nil),
	}
	fcTC = NewFunctionTestCase(proc, tc2.inputs, tc2.expect, NewCast)
	s, info = fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc2.info, info))

	// vecf64(3): dimension match should succeed
	tc3 := tcTemp{
		info: "str to vecf64(3) - dimension match",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]"}, nil),
			NewFunctionTestInput(types.New(types.T_array_float64, 3, 0), [][]float64{}, nil),
		},
		expect: NewFunctionTestResult(types.New(types.T_array_float64, 3, 0), false,
			[][]float64{{1, 2, 3}}, []bool{false}),
	}
	fcTC = NewFunctionTestCase(proc, tc3.inputs, tc3.expect, NewCast)
	s, info = fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc3.info, info))

	// vecf64(3): dimension mismatch (5-dim into vecf64(3)) should fail
	tc4 := tcTemp{
		info: "str to vecf64(3) - dimension mismatch should error",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3,4,5]"}, nil),
			NewFunctionTestInput(types.New(types.T_array_float64, 3, 0), [][]float64{}, nil),
		},
		expect: NewFunctionTestResult(types.New(types.T_array_float64, 3, 0), true, nil, nil),
	}
	fcTC = NewFunctionTestCase(proc, tc4.inputs, tc4.expect, NewCast)
	s, info = fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc4.info, info))

	// vecf32(MaxArrayDimension): should bypass dimension check (any dimension accepted)
	tc5 := tcTemp{
		info: "str to vecf32(MaxDim) - bypass dimension check",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3,4,5]"}, nil),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{}, nil),
		},
		expect: NewFunctionTestResult(types.T_array_float32.ToType(), false,
			[][]float32{{1, 2, 3, 4, 5}}, []bool{false}),
	}
	fcTC = NewFunctionTestCase(proc, tc5.inputs, tc5.expect, NewCast)
	s, info = fcTC.Run()
	require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc5.info, info))
}

func TestCastJsonLargeIntegerPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)

	intResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer intResult.Free()
	require.NoError(t, intResult.PreExtendAndReset(1))
	require.NoError(t, NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{"9007199254740993"}, nil),
		vector.NewVec(types.T_int64.ToType()),
	}, intResult, proc, 1, nil))
	gotInt := vector.MustFixedColNoTypeCheck[int64](intResult.GetResultVector())
	require.Equal(t, int64(9007199254740993), gotInt[0])

	uintResult := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
	defer uintResult.Free()
	require.NoError(t, uintResult.PreExtendAndReset(1))
	require.NoError(t, NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{"18446744073709551615"}, nil),
		vector.NewVec(types.T_uint64.ToType()),
	}, uintResult, proc, 1, nil))
	gotUint := vector.MustFixedColNoTypeCheck[uint64](uintResult.GetResultVector())
	require.Equal(t, uint64(18446744073709551615), gotUint[0])
}

func TestCastJsonIntegerStringOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)

	intResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer intResult.Free()
	require.NoError(t, intResult.PreExtendAndReset(1))
	err := NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{`"9223372036854775808"`}, nil),
		vector.NewVec(types.T_int64.ToType()),
	}, intResult, proc, 1, nil)
	require.Error(t, err)

	uintResult := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
	defer uintResult.Free()
	require.NoError(t, uintResult.PreExtendAndReset(1))
	err = NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{`"18446744073709551616"`}, nil),
		vector.NewVec(types.T_uint64.ToType()),
	}, uintResult, proc, 1, nil)
	require.Error(t, err)

	require.NoError(t, intResult.PreExtendAndReset(1))
	err = NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{"9223372036854775808.0"}, nil),
		vector.NewVec(types.T_int64.ToType()),
	}, intResult, proc, 1, nil)
	require.Error(t, err)

	require.NoError(t, uintResult.PreExtendAndReset(1))
	err = NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{"18446744073709551616.0"}, nil),
		vector.NewVec(types.T_uint64.ToType()),
	}, uintResult, proc, 1, nil)
	require.Error(t, err)
}

func TestCastJsonContainerRejected(t *testing.T) {
	// Casting a JSON object or array to a numeric type must fail via jsonCastErr,
	// not panic or silently succeed.
	proc := testutil.NewProcess(t)

	for _, payload := range []string{`{"a":1}`, `[1,2,3]`} {
		intResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
		require.NoError(t, intResult.PreExtendAndReset(1))
		err := NewCast([]*vector.Vector{
			testutil.MakeJsonVector([]string{payload}, nil),
			vector.NewVec(types.T_int64.ToType()),
		}, intResult, proc, 1, nil)
		require.Error(t, err, "payload=%s", payload)
		intResult.Free()
	}
}

func TestCastJsonNullPropagates(t *testing.T) {
	// JSON literal null and an actual SQL NULL input must both produce a SQL NULL
	// on the output vector, not 0.
	proc := testutil.NewProcess(t)

	intResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer intResult.Free()
	require.NoError(t, intResult.PreExtendAndReset(2))
	require.NoError(t, NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{"null", "1"}, []uint64{1}),
		vector.NewVec(types.T_int64.ToType()),
	}, intResult, proc, 2, nil))

	resVec := intResult.GetResultVector()
	require.True(t, resVec.GetNulls().Contains(0), "JSON literal null should become SQL NULL")
	require.True(t, resVec.GetNulls().Contains(1), "SQL NULL input should propagate")
}

func TestCastJsonFloatOverflow(t *testing.T) {
	// jsonAppendFloatValue must reject overflow when targeting narrower numeric types
	// and reject negative floats for unsigned targets.
	proc := testutil.NewProcess(t)

	cases := []struct {
		input  string
		target types.Type
	}{
		{`1.0e39`, types.T_float32.ToType()}, // |f| > math.MaxFloat32
		{`-1.0`, types.T_uint32.ToType()},    // negative to unsigned
		{`1.0e19`, types.T_int32.ToType()},   // overflow int32 via float path
		{`1.0e20`, types.T_uint32.ToType()},  // overflow uint32 via float path
		{`1.0e10`, types.T_int8.ToType()},    // overflow int8 bounds
	}
	for _, tc := range cases {
		res := vector.NewFunctionResultWrapper(tc.target, proc.Mp())
		require.NoError(t, res.PreExtendAndReset(1))
		err := NewCast([]*vector.Vector{
			testutil.MakeJsonVector([]string{tc.input}, nil),
			vector.NewVec(tc.target),
		}, res, proc, 1, nil)
		require.Error(t, err, "input=%s target=%s", tc.input, tc.target.Oid.String())
		res.Free()
	}
}

func TestCastJsonFloatToIntegerRounds(t *testing.T) {
	proc := testutil.NewProcess(t)

	intResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer intResult.Free()
	require.NoError(t, intResult.PreExtendAndReset(3))
	require.NoError(t, NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{"1.6", "-1.6", "1.4"}, nil),
		vector.NewVec(types.T_int64.ToType()),
	}, intResult, proc, 3, nil))
	gotInt := vector.MustFixedColNoTypeCheck[int64](intResult.GetResultVector())
	require.Equal(t, []int64{2, -2, 1}, gotInt)

	uintResult := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
	defer uintResult.Free()
	require.NoError(t, uintResult.PreExtendAndReset(2))
	require.NoError(t, NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{"1.6", "1.4"}, nil),
		vector.NewVec(types.T_uint64.ToType()),
	}, uintResult, proc, 2, nil))
	gotUint := vector.MustFixedColNoTypeCheck[uint64](uintResult.GetResultVector())
	require.Equal(t, []uint64{2, 1}, gotUint)
}

func TestCastJsonStringNumeric(t *testing.T) {
	// A JSON string containing digits should parse through jsonAppendStringValue
	// and land as an int64. A JSON string that cannot parse must error.
	proc := testutil.NewProcess(t)

	ok := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer ok.Free()
	require.NoError(t, ok.PreExtendAndReset(1))
	require.NoError(t, NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{`"42"`}, nil),
		vector.NewVec(types.T_int64.ToType()),
	}, ok, proc, 1, nil))
	got := vector.MustFixedColNoTypeCheck[int64](ok.GetResultVector())
	require.Equal(t, int64(42), got[0])

	bad := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer bad.Free()
	require.NoError(t, bad.PreExtendAndReset(1))
	err := NewCast([]*vector.Vector{
		testutil.MakeJsonVector([]string{`"not a number"`}, nil),
		vector.NewVec(types.T_int64.ToType()),
	}, bad, proc, 1, nil)
	require.Error(t, err)
}

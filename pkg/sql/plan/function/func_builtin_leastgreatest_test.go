// Copyright 2021 - 2025 Matrix Origin
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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func initLeastTestCase() []tcTemp {
	return []tcTemp{
		// Test int8
		{
			info: "test least int8",
			typ:  types.T_int8,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{1, 5, -3, 10, -5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{2, 3, -1, 5, -10},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{3, 1, 0, 8, -2},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{1, 1, -3, 5, -10},
				[]bool{false, false, false, false, false}),
		},
		// Test int64
		{
			info: "test least int64",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0, 200, -100},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{50, -30, -10, 150, -200},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{50, -50, -10, 150, -200},
				[]bool{false, false, false, false, false}),
		},
		// Test uint64
		{
			info: "test least uint64",
			typ:  types.T_uint64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{100, 50, 200, 10, 5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{50, 30, 150, 20, 15},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{50, 30, 150, 10, 5},
				[]bool{false, false, false, false, false}),
		},
		// Test float64
		{
			info: "test least float64",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, -2.3, 0.0, 10.5, -5.7},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.5, -1.3, 0.5, 8.5, -10.7},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.5, -2.3, 0.0, 8.5, -10.7},
				[]bool{false, false, false, false, false}),
		},
		// Test bool
		{
			info: "test least bool",
			typ:  types.T_bool,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{false, true, false, true},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false, false},
				[]bool{false, false, false, false}),
		},
		// Test varchar
		{
			info: "test least varchar",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"apple", "banana", "cherry", "date"},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zebra", "ant", "dog", "cat"},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"apple", "ant", "cherry", "cat"},
				[]bool{false, false, false, false}),
		},
		// Test date
		{
			info: "test least date",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1000), types.Date(2000), types.Date(1500)},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1200), types.Date(1800), types.Date(1400)},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(1000), types.Date(1800), types.Date(1400)},
				[]bool{false, false, false}),
		},
		// Test with nulls
		{
			info: "test least with nulls",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 15, 25},
				[]bool{false, true, true}),
		},
		// Test single argument
		{
			info: "test least single argument",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{42, 100, -50},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{42, 100, -50},
				[]bool{false, false, false}),
		},
		// Test three arguments
		{
			info: "test least three arguments",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{8, 12, 35},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 12, 25},
				[]bool{false, false, false}),
		},
	}
}

func TestLeast(t *testing.T) {
	testCases := initLeastTestCase()
	proc := testutil.NewProcess(t)

	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_int8:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_bool:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_date:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		default:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info '%s'", tc.info, info))
	}
}

func initGreatestTestCase() []tcTemp {
	return []tcTemp{
		// Test int8
		{
			info: "test greatest int8",
			typ:  types.T_int8,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{1, 5, -3, 10, -5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{2, 3, -1, 5, -10},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{3, 1, 0, 8, -2},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{3, 5, 0, 10, -2},
				[]bool{false, false, false, false, false}),
		},
		// Test int64
		{
			info: "test greatest int64",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0, 200, -100},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{50, -30, -10, 150, -200},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{100, -30, 0, 200, -100},
				[]bool{false, false, false, false, false}),
		},
		// Test uint64
		{
			info: "test greatest uint64",
			typ:  types.T_uint64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{100, 50, 200, 10, 5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{50, 30, 150, 20, 15},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{100, 50, 200, 20, 15},
				[]bool{false, false, false, false, false}),
		},
		// Test float64
		{
			info: "test greatest float64",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, -2.3, 0.0, 10.5, -5.7},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.5, -1.3, 0.5, 8.5, -10.7},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{2.5, -1.3, 0.5, 10.5, -5.7},
				[]bool{false, false, false, false, false}),
		},
		// Test bool
		{
			info: "test greatest bool",
			typ:  types.T_bool,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{false, true, false, true},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, true},
				[]bool{false, false, false, false}),
		},
		// Test varchar
		{
			info: "test greatest varchar",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"apple", "banana", "cherry", "date"},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zebra", "ant", "dog", "cat"},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"zebra", "banana", "dog", "date"},
				[]bool{false, false, false, false}),
		},
		// Test date
		{
			info: "test greatest date",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1000), types.Date(2000), types.Date(1500)},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1200), types.Date(1800), types.Date(1400)},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(1200), types.Date(2000), types.Date(1500)},
				[]bool{false, false, false}),
		},
		// Test with nulls
		{
			info: "test greatest with nulls",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 15, 25},
				[]bool{false, true, true}),
		},
		// Test single argument
		{
			info: "test greatest single argument",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{42, 100, -50},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{42, 100, -50},
				[]bool{false, false, false}),
		},
		// Test three arguments
		{
			info: "test greatest three arguments",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{8, 12, 35},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 20, 35},
				[]bool{false, false, false}),
		},
	}
}

func TestGreatest(t *testing.T) {
	testCases := initGreatestTestCase()
	proc := testutil.NewProcess(t)

	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_int8:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_bool:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_date:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		default:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TestLeastGreatestCheck verifies the type-promotion behavior of the
// LEAST/GREATEST type checker (issue #25145): mixed numeric arguments are
// promoted to a common type via an implicit cast, identical types succeed
// directly, and non-promotable mixes are rejected.
func TestLeastGreatestCheck(t *testing.T) {
	dec := func(oid types.T, width, scale int32) types.Type {
		typ := oid.ToType()
		typ.Width = width
		typ.Scale = scale
		return typ
	}

	type tc struct {
		name     string
		inputs   []types.Type
		wantOk   bool    // expect a successful check (no failure)
		wantCast bool    // expect an implicit cast to be requested
		target   types.T // expected common Oid when wantCast is true
	}

	cases := []tc{
		{
			name:   "no input",
			inputs: []types.Type{},
			wantOk: false,
		},
		{
			name:   "all same int64",
			inputs: []types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
			wantOk: true,
		},
		{
			name:   "all NULL literals",
			inputs: []types.Type{types.T_any.ToType(), types.T_any.ToType()},
			wantOk: true,
		},
		{
			name:     "bigint + double -> double",
			inputs:   []types.Type{types.T_int64.ToType(), types.T_float64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_float64,
		},
		{
			name:     "bigint + decimal -> decimal",
			inputs:   []types.Type{types.T_int64.ToType(), dec(types.T_decimal64, 10, 2)},
			wantOk:   true,
			wantCast: true,
			target:   types.T_decimal128,
		},
		{
			name:     "int + float32 -> double",
			inputs:   []types.Type{types.T_int32.ToType(), types.T_float32.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_float64,
		},
		{
			name:     "int8 + int32 -> int32",
			inputs:   []types.Type{types.T_int8.ToType(), types.T_int32.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_int32,
		},
		{
			name:     "int64 + uint64 -> decimal128",
			inputs:   []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_decimal128,
		},
		{
			name:     "int32 + uint16 -> int64",
			inputs:   []types.Type{types.T_int32.ToType(), types.T_uint16.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_int64,
		},
		{
			name:     "double + NULL literal -> double",
			inputs:   []types.Type{types.T_float64.ToType(), types.T_any.ToType(), types.T_int64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_float64,
		},
		{
			name:     "uint8 + uint16 -> uint16",
			inputs:   []types.Type{types.T_uint8.ToType(), types.T_uint16.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_uint16,
		},
		{
			name:     "uint8 + uint32 -> uint32",
			inputs:   []types.Type{types.T_uint8.ToType(), types.T_uint32.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_uint32,
		},
		{
			name:     "uint16 + uint64 -> uint64",
			inputs:   []types.Type{types.T_uint16.ToType(), types.T_uint64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_uint64,
		},
		{
			name:     "bit + bigint -> decimal128",
			inputs:   []types.Type{types.T_bit.ToType(), types.T_int64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_decimal128,
		},
		{
			name:     "bit + uint32 -> uint64",
			inputs:   []types.Type{types.T_bit.ToType(), types.T_uint32.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_uint64,
		},
		{
			name:     "decimal256 + bigint -> decimal256",
			inputs:   []types.Type{dec(types.T_decimal256, 40, 2), types.T_int64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_decimal256,
		},
		{
			name:     "decimal128 + decimal64 -> decimal128",
			inputs:   []types.Type{dec(types.T_decimal128, 20, 4), dec(types.T_decimal64, 10, 2)},
			wantOk:   true,
			wantCast: true,
			target:   types.T_decimal128,
		},
		{
			name:     "numeric + varchar -> varchar",
			inputs:   []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_varchar,
		},
		{
			name:     "varchar + int -> varchar",
			inputs:   []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_varchar,
		},
		{
			name:     "varchar + bigint + double -> varchar",
			inputs:   []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_float64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_varchar,
		},
		{
			name:     "text + numeric -> text",
			inputs:   []types.Type{types.T_text.ToType(), types.T_int64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_text,
		},
		{
			name:     "varchar + varbinary -> varchar",
			inputs:   []types.Type{types.T_varchar.ToType(), types.T_varbinary.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_varchar,
		},
		{
			name:     "blob + numeric -> blob",
			inputs:   []types.Type{types.T_blob.ToType(), types.T_int64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_blob,
		},
		{
			name:     "binary + numeric -> varbinary",
			inputs:   []types.Type{types.T_binary.ToType(), types.T_int64.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_varbinary,
		},
		{
			name:     "json + varchar -> varchar",
			inputs:   []types.Type{types.T_json.ToType(), types.T_varchar.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_varchar,
		},
		{
			name:     "json + text -> text",
			inputs:   []types.Type{types.T_json.ToType(), types.T_text.ToType()},
			wantOk:   true,
			wantCast: true,
			target:   types.T_text,
		},
		{
			name:   "json + blob rejected",
			inputs: []types.Type{types.T_json.ToType(), types.T_blob.ToType()},
			wantOk: false,
		},
		{
			name:   "json + binary rejected",
			inputs: []types.Type{types.T_json.ToType(), types.T_binary.ToType()},
			wantOk: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := leastGreatestCheck(nil, c.inputs)
			if !c.wantOk {
				require.Equal(t, failedFunctionParametersWrong, res.status,
					"case %q should fail the type check", c.name)
				return
			}
			if c.wantCast {
				require.Equal(t, succeedWithCast, res.status, "case %q should request a cast", c.name)
				require.Len(t, res.finalType, len(c.inputs))
				for i := range res.finalType {
					require.Equal(t, c.target, res.finalType[i].Oid,
						"case %q arg %d cast target", c.name, i)
				}
			} else {
				require.Equal(t, succeedMatched, res.status, "case %q should match directly", c.name)
			}
		})
	}
}

func TestLeastGreatestFunctionResolution(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name        string
		args        []types.Type
		wantReturn  types.T
		wantCast    bool
		wantTargets []types.T
	}{
		{
			name:        "greatest varchar bigint returns varchar",
			args:        []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
			wantReturn:  types.T_varchar,
			wantCast:    true,
			wantTargets: []types.T{types.T_varchar, types.T_varchar},
		},
		{
			name:        "least text bigint returns text",
			args:        []types.Type{types.T_text.ToType(), types.T_int64.ToType()},
			wantReturn:  types.T_text,
			wantCast:    true,
			wantTargets: []types.T{types.T_text, types.T_text},
		},
		{
			name:        "pure json returns varchar",
			args:        []types.Type{types.T_json.ToType(), types.T_json.ToType()},
			wantReturn:  types.T_varchar,
			wantCast:    true,
			wantTargets: []types.T{types.T_varchar, types.T_varchar},
		},
		{
			name:        "binary bigint returns varbinary",
			args:        []types.Type{types.T_binary.ToType(), types.T_int64.ToType()},
			wantReturn:  types.T_varbinary,
			wantCast:    true,
			wantTargets: []types.T{types.T_varbinary, types.T_varbinary},
		},
		{
			name:        "enum varchar returns varchar",
			args:        []types.Type{types.T_enum.ToType(), types.T_varchar.ToType()},
			wantReturn:  types.T_varchar,
			wantCast:    true,
			wantTargets: []types.T{types.T_varchar, types.T_varchar},
		},
		{
			name:        "enum enum returns varchar",
			args:        []types.Type{types.T_enum.ToType(), types.T_enum.ToType()},
			wantReturn:  types.T_varchar,
			wantCast:    true,
			wantTargets: []types.T{types.T_varchar, types.T_varchar},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, fnName := range []string{"greatest", "least"} {
				fn, err := GetFunctionByName(ctx, fnName, c.args)
				require.NoError(t, err, fnName)
				require.Equal(t, c.wantReturn, fn.GetReturnType().Oid, fnName)
				targets, shouldCast := fn.ShouldDoImplicitTypeCast()
				require.Equal(t, c.wantCast, shouldCast, fnName)
				require.Len(t, targets, len(c.wantTargets), fnName)
				for i := range c.wantTargets {
					require.Equal(t, c.wantTargets[i], targets[i].Oid, "%s target %d", fnName, i)
				}
			}
		})
	}
}

func TestLeastGreatestTemporalResolution(t *testing.T) {
	cases := []struct {
		name         string
		inputs       []types.Type
		wantOK       bool
		wantReturn   types.T
		wantOverload int
		wantMode     leastGreatestComparisonMode
		wantTargets  []types.T
	}{
		{
			name:         "date varchar uses packed date and returns varchar",
			inputs:       []types.Type{types.T_date.ToType(), types.T_varchar.ToType()},
			wantOK:       true,
			wantReturn:   types.T_varchar,
			wantOverload: leastGreatestTemporalOverload,
			wantMode:     leastGreatestComparePackedDate,
			wantTargets:  []types.T{types.T_date, types.T_varchar},
		},
		{
			name:         "date datetime uses packed date and returns datetime",
			inputs:       []types.Type{types.T_date.ToType(), types.T_datetime.ToType()},
			wantOK:       true,
			wantReturn:   types.T_datetime,
			wantOverload: leastGreatestTemporalOverload,
			wantMode:     leastGreatestComparePackedDate,
			wantTargets:  []types.T{types.T_date, types.T_datetime},
		},
		{
			name:         "json date bigint casts non temporal peers to varchar",
			inputs:       []types.Type{types.T_json.ToType(), types.T_date.ToType(), types.T_int64.ToType()},
			wantOK:       true,
			wantReturn:   types.T_varchar,
			wantOverload: 2,
			wantMode:     leastGreatestComparePackedDate,
			wantTargets:  []types.T{types.T_varchar, types.T_date, types.T_varchar},
		},
		{
			name:         "json date time keeps temporal inputs for json temporal overload",
			inputs:       []types.Type{types.T_json.ToType(), types.T_date.ToType(), types.T_time.ToType()},
			wantOK:       true,
			wantReturn:   types.T_varchar,
			wantOverload: 2,
			wantMode:     leastGreatestComparePackedDate,
			wantTargets:  []types.T{types.T_varchar, types.T_date, types.T_time},
		},
		{
			name:         "year date bigint keeps temporal peers",
			inputs:       []types.Type{types.T_year.ToType(), types.T_date.ToType(), types.T_int64.ToType()},
			wantOK:       true,
			wantReturn:   types.T_varchar,
			wantOverload: leastGreatestTemporalOverload,
			wantMode:     leastGreatestComparePackedDate,
			wantTargets:  []types.T{types.T_year, types.T_date, types.T_varchar},
		},
		{
			name:   "json date text rejected",
			inputs: []types.Type{types.T_json.ToType(), types.T_date.ToType(), types.T_text.ToType()},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resolution, ok := resolveLeastGreatestType(c.inputs)
			require.Equal(t, c.wantOK, ok)
			if !ok {
				return
			}
			require.Equal(t, c.wantReturn, resolution.resultType.Oid)
			require.Equal(t, c.wantOverload, resolution.overloadID)
			require.Equal(t, c.wantMode, resolution.comparisonMode)
			require.Len(t, resolution.castTypes, len(c.wantTargets))
			for i := range c.wantTargets {
				require.Equal(t, c.wantTargets[i], resolution.castTypes[i].Oid, "target %d", i)
			}
		})
	}
}

func TestLeastGreatestHighPriorityResolution(t *testing.T) {
	decimalScale1 := types.New(types.T_decimal64, 10, 1)
	decimalScale2 := types.New(types.T_decimal64, 12, 2)

	t.Run("year numeric keeps year for specialized executor", func(t *testing.T) {
		resolution, ok := resolveLeastGreatestType([]types.Type{
			types.T_year.ToType(),
			types.T_decimal128.ToType(),
		})
		require.True(t, ok)
		require.Equal(t, types.T_decimal128, resolution.resultType.Oid)
		require.Equal(t, leastGreatestYearNumericOverload, resolution.overloadID)
		require.Equal(t, []types.T{types.T_year, types.T_decimal128}, []types.T{
			resolution.castTypes[0].Oid,
			resolution.castTypes[1].Oid,
		})
	})

	t.Run("same oid decimal aligns metadata", func(t *testing.T) {
		resolution, ok := resolveLeastGreatestType([]types.Type{decimalScale1, decimalScale2})
		require.True(t, ok)
		require.Equal(t, types.T_decimal64, resolution.resultType.Oid)
		require.Equal(t, int32(12), resolution.resultType.Width)
		require.Equal(t, int32(2), resolution.resultType.Scale)
		require.Len(t, resolution.castTypes, 2)
		for _, target := range resolution.castTypes {
			require.Equal(t, resolution.resultType, target)
		}
	})

	t.Run("same oid decimal256 precision overflow falls back to float64", func(t *testing.T) {
		decimalScale75 := types.New(types.T_decimal256, 76, 75)
		decimalScale0 := types.New(types.T_decimal256, 76, 0)
		resolution, ok := resolveLeastGreatestType([]types.Type{decimalScale75, decimalScale0})
		require.True(t, ok)
		require.Equal(t, types.T_float64, resolution.resultType.Oid)
		require.Equal(t, []types.T{types.T_float64, types.T_float64}, []types.T{
			resolution.castTypes[0].Oid,
			resolution.castTypes[1].Oid,
		})

		for _, name := range []string{"greatest", "least"} {
			fn, err := GetFunctionByName(context.Background(), name, []types.Type{decimalScale75, decimalScale0})
			require.NoError(t, err)
			require.Equal(t, types.T_float64, fn.GetReturnType().Oid)
			targets, shouldCast := fn.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, []types.T{types.T_float64, types.T_float64}, []types.T{targets[0].Oid, targets[1].Oid})
		}
	})

	for _, oid := range []types.T{types.T_time, types.T_datetime, types.T_timestamp} {
		t.Run(oid.String()+" same oid aligns scale", func(t *testing.T) {
			scale1 := oid.ToType()
			scale1.Scale = 1
			scale2 := oid.ToType()
			scale2.Scale = 4

			resolution, ok := resolveLeastGreatestType([]types.Type{scale1, scale2})
			require.True(t, ok)
			require.Equal(t, oid, resolution.resultType.Oid)
			require.Equal(t, int32(4), resolution.resultType.Scale)
			require.Len(t, resolution.castTypes, 2)
			for _, target := range resolution.castTypes {
				require.Equal(t, resolution.resultType, target)
			}
		})
	}

	t.Run("unsupported same oid is rejected before executor", func(t *testing.T) {
		intervalType := types.Type{Oid: types.T_interval}
		_, ok := resolveLeastGreatestType([]types.Type{intervalType, intervalType})
		require.False(t, ok)
	})
}

func TestLeastGreatestPackedDateMaterializesNullRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	d1, err := types.ParseDateCast("2020-01-01")
	require.NoError(t, err)
	dateTyp := types.T_date.ToType()
	varcharTyp := types.T_varchar.ToType()

	t.Run("ignore all rows", func(t *testing.T) {
		fcTC := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(dateTyp, []types.Date{d1, d1, d1}, nil),
				NewFunctionTestInput(varcharTyp, []string{"2020-01-01", "2020-01-01", "2020-01-01"}, nil),
			},
			NewFunctionTestResult(varcharTyp, false, nil, nil),
			greatestTemporalFn)
		require.NoError(t, fcTC.result.PreExtendAndReset(fcTC.fnLength))
		require.NoError(t, fcTC.fn(fcTC.parameters, fcTC.result, fcTC.proc, fcTC.fnLength, &FunctionSelectList{AllNull: true}))

		resultVec := fcTC.result.GetResultVector()
		require.Equal(t, fcTC.fnLength, resultVec.Length())
		for i := 0; i < fcTC.fnLength; i++ {
			require.True(t, resultVec.IsNull(uint64(i)))
		}
	})

	t.Run("constant null argument", func(t *testing.T) {
		fcTC := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(dateTyp, []types.Date{d1, d1, d1}, nil),
				NewFunctionTestConstInput(varcharTyp, []string{"2020-01-01"}, []bool{true}),
			},
			NewFunctionTestResult(varcharTyp, false, nil, nil),
			greatestTemporalFn)
		require.NoError(t, fcTC.result.PreExtendAndReset(fcTC.fnLength))
		require.NoError(t, fcTC.fn(fcTC.parameters, fcTC.result, fcTC.proc, fcTC.fnLength, nil))

		resultVec := fcTC.result.GetResultVector()
		require.Equal(t, fcTC.fnLength, resultVec.Length())
		for i := 0; i < fcTC.fnLength; i++ {
			require.True(t, resultVec.IsNull(uint64(i)))
		}
	})
}

func TestLeastGreatestYearNumericExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimalType := types.New(types.T_decimal128, 10, 1)
	parseDecimal := func(values ...string) []types.Decimal128 {
		result := make([]types.Decimal128, len(values))
		for i, value := range values {
			parsed, err := types.ParseDecimal128(value, decimalType.Width, decimalType.Scale)
			require.NoError(t, err)
			result[i] = parsed
		}
		return result
	}

	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{2020, 2022}, nil),
		NewFunctionTestInput(decimalType, parseDecimal("2019.5", "2022.5"), nil),
	}

	tcGreatest := NewFunctionTestCase(proc,
		inputs,
		NewFunctionTestResult(decimalType, false, parseDecimal("2020.0", "2022.5"), nil),
		greatestYearNumericFn)
	ok, info := tcGreatest.Run()
	require.True(t, ok, info)

	tcLeast := NewFunctionTestCase(proc,
		inputs,
		NewFunctionTestResult(decimalType, false, parseDecimal("2019.5", "2022.0"), nil),
		leastYearNumericFn)
	ok, info = tcLeast.Run()
	require.True(t, ok, info)
}

func TestLeastGreatestYearNumericFunctionResolution(t *testing.T) {
	for _, name := range []string{"greatest", "least"} {
		fn, err := GetFunctionByName(context.Background(), name, []types.Type{
			types.T_year.ToType(),
			types.New(types.T_decimal128, 10, 1),
		})
		require.NoError(t, err)
		require.Equal(t, int32(leastGreatestYearNumericOverload), fn.overloadId)
		require.Equal(t, types.T_decimal128, fn.GetReturnType().Oid)

		targets, shouldCast := fn.ShouldDoImplicitTypeCast()
		require.True(t, shouldCast)
		require.Equal(t, []types.T{types.T_year, types.T_decimal128}, []types.T{targets[0].Oid, targets[1].Oid})
	}
}

func TestLeastGreatestJSONTemporalFunctionResolution(t *testing.T) {
	for _, name := range []string{"greatest", "least"} {
		fn, err := GetFunctionByName(context.Background(), name, []types.Type{
			types.T_json.ToType(),
			types.T_date.ToType(),
			types.T_time.ToType(),
		})
		require.NoError(t, err)
		require.Equal(t, int32(2), fn.overloadId)
		require.Equal(t, types.T_varchar, fn.GetReturnType().Oid)

		targets, shouldCast := fn.ShouldDoImplicitTypeCast()
		require.True(t, shouldCast)
		require.Equal(t, []types.T{types.T_varchar, types.T_date, types.T_time}, []types.T{
			targets[0].Oid,
			targets[1].Oid,
			targets[2].Oid,
		})
	}
}

func TestLeastGreatestTemporalExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)
	d1, err := types.ParseDateCast("2020-01-01")
	require.NoError(t, err)
	d2, err := types.ParseDateCast("2020-01-03")
	require.NoError(t, err)
	dateTyp := types.T_date.ToType()
	varcharTyp := types.T_varchar.ToType()

	tcGreatest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dateTyp, []types.Date{d1, d2}, nil),
			NewFunctionTestInput(varcharTyp, []string{"2020-01-02", "2020-01-01"}, nil),
		},
		NewFunctionTestResult(varcharTyp, false, []string{"2020-01-02", "2020-01-03"}, nil),
		greatestTemporalFn)
	ok, info := tcGreatest.Run()
	require.True(t, ok, info)

	tcLeast := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dateTyp, []types.Date{d1, d2}, nil),
			NewFunctionTestInput(varcharTyp, []string{"2020-01-02", "2020-01-01"}, nil),
		},
		NewFunctionTestResult(varcharTyp, false, []string{"2020-01-01", "2020-01-01"}, nil),
		leastTemporalFn)
	ok, info = tcLeast.Run()
	require.True(t, ok, info)

	tcInvalidDatePeer := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dateTyp, []types.Date{d1}, nil),
			NewFunctionTestInput(varcharTyp, []string{"1"}, nil),
		},
		NewFunctionTestResult(varcharTyp, true, []string{""}, nil),
		greatestTemporalFn)
	ok, info = tcInvalidDatePeer.Run()
	require.True(t, ok, info)

	// JSON-temporal parameters reach this executor after JSON is cast to
	// VARCHAR. The specialized overload keeps the VARCHAR return contract.
	tcJSONGreatest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dateTyp, []types.Date{d1, d2}, nil),
			NewFunctionTestInput(varcharTyp, []string{"2020-01-02", "2020-01-01"}, nil),
		},
		NewFunctionTestResult(varcharTyp, false, []string{"2020-01-02", "2020-01-03"}, nil),
		greatestJSONTemporalFn)
	ok, info = tcJSONGreatest.Run()
	require.True(t, ok, info)
}

func TestLeastGreatestPackedDateRejectsTimeFallback(t *testing.T) {
	_, err := leastGreatestParsePackedDateBytes([]byte("1"), types.T_date.ToType(), nil, time.Local)
	require.Error(t, err)
}

func TestLeastGreatestPackedDateFallsBackToLocalTimezone(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = nil

	date, err := types.ParseDateCast("2020-01-01")
	require.NoError(t, err)
	timestamp, err := types.ParseTimestamp(time.Local, "2020-01-02 12:34:56", 0)
	require.NoError(t, err)

	t.Run("stored timestamp", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{date}, nil),
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{timestamp}, nil),
			},
			NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{timestamp.ToDatetime(time.Local)}, nil),
			greatestTemporalFn)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})

	t.Run("timestamp string peer", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{timestamp}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"2020-01-03 00:00:00"}, nil),
			},
			NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"2020-01-03 00:00:00"}, nil),
			greatestTemporalFn)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})
}

func TestLeastGreatestPackedDateUsesStatementStartForTime(t *testing.T) {
	proc := testutil.NewProcess(t)
	loc := time.FixedZone("UTC+8", 8*60*60)
	proc.GetSessionInfo().TimeZone = loc
	stmtProfile := &process.StmtProfile{}
	stmtProfile.SetQueryStart(time.Date(2024, 5, 6, 1, 2, 3, 0, loc))
	proc.SetStmtProfile(stmtProfile)

	date, err := types.ParseDateCast("2020-01-01")
	require.NoError(t, err)
	timeValue, err := types.ParseTime("12:34:56", 0)
	require.NoError(t, err)

	resultType := types.T_datetime.ToType()
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{date}, nil),
			NewFunctionTestInput(types.T_time.ToType(), []types.Time{timeValue}, nil),
		},
		NewFunctionTestResult(resultType, false,
			[]types.Datetime{types.DatetimeFromClock(2024, 5, 6, 12, 34, 56, 0)}, nil),
		greatestTemporalFn)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestLeastGreatestNormalExecutorRestoresTemporalScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	timeScale1 := types.New(types.T_time, 64, 1)
	timeScale2 := types.New(types.T_time, 64, 2)
	first, err := types.ParseTime("10:00:00.1", timeScale1.Scale)
	require.NoError(t, err)
	second, err := types.ParseTime("10:00:00.99", timeScale2.Scale)
	require.NoError(t, err)

	// Simulate an execution path whose result wrapper was initialized before
	// the resolver's aligned temporal metadata reached it.
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(timeScale1, []types.Time{first}, nil),
			NewFunctionTestInput(timeScale2, []types.Time{second}, nil),
		},
		NewFunctionTestResult(types.New(types.T_time, 64, 0), false, []types.Time{second}, nil),
		greatestFn)
	ok, info := tc.Run()
	require.True(t, ok, info)
	require.Equal(t, int32(2), tc.GetResultVectorDirectly().GetType().Scale)
}

// TestLeastGreatestWidthHelpers covers every branch of the integer-width →
// type helpers used by the LEAST/GREATEST promotion logic.
func TestLeastGreatestWidthHelpers(t *testing.T) {
	require.Equal(t, types.T_int8, signedTypeForWidth(1).Oid)
	require.Equal(t, types.T_int8, signedTypeForWidth(3).Oid)
	require.Equal(t, types.T_int16, signedTypeForWidth(5).Oid)
	require.Equal(t, types.T_int32, signedTypeForWidth(10).Oid)
	require.Equal(t, types.T_int64, signedTypeForWidth(19).Oid)

	require.Equal(t, types.T_uint8, unsignedTypeForWidth(2).Oid)
	require.Equal(t, types.T_uint8, unsignedTypeForWidth(3).Oid)
	require.Equal(t, types.T_uint16, unsignedTypeForWidth(5).Oid)
	require.Equal(t, types.T_uint32, unsignedTypeForWidth(10).Oid)
	require.Equal(t, types.T_uint64, unsignedTypeForWidth(20).Oid)
}

// TestLeastGreatestCommonNumericType drives leastGreatestCommonNumericType
// directly to cover the branches that are awkward to reach through the public
// type checker: the all-unsigned/all-signed paths, the BIT operand, the
// DECIMAL256 seed, and the DECIMAL-precision-overflow fallback to DOUBLE.
func TestLeastGreatestCommonNumericType(t *testing.T) {
	dec := func(oid types.T, width, scale int32) types.Type {
		typ := oid.ToType()
		typ.Width = width
		typ.Scale = scale
		return typ
	}

	cases := []struct {
		name   string
		inputs []types.Type
		wantOk bool
		oid    types.T
	}{
		{"all signed widens to widest", []types.Type{types.T_int16.ToType(), types.T_int8.ToType()}, true, types.T_int16},
		{"all unsigned widens to widest", []types.Type{types.T_uint8.ToType(), types.T_uint32.ToType()}, true, types.T_uint32},
		{"bit treated as unsigned 64-bit", []types.Type{types.T_bit.ToType(), types.T_uint16.ToType()}, true, types.T_uint64},
		{"bit plus signed -> decimal128", []types.Type{types.T_bit.ToType(), types.T_int32.ToType()}, true, types.T_decimal128},
		{"float beats everything", []types.Type{types.T_float32.ToType(), dec(types.T_decimal64, 10, 2)}, true, types.T_float64},
		{"decimal256 source is kept", []types.Type{dec(types.T_decimal256, 50, 4), types.T_int64.ToType()}, true, types.T_decimal256},
		{"decimal precision overflow falls back to double",
			[]types.Type{dec(types.T_decimal128, 38, 38), dec(types.T_decimal256, 76, 0)}, true, types.T_float64},
		{"non-numeric rejected", []types.Type{types.T_int64.ToType(), types.T_date.ToType()}, false, types.T_any},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, ok := leastGreatestCommonNumericType(c.inputs)
			require.Equal(t, c.wantOk, ok, c.name)
			if ok {
				require.Equal(t, c.oid, target.Oid, c.name)
			}
		})
	}
}

// TestLeastGreatestDecimal256 exercises the DECIMAL256 branch of the leastFn /
// greatestFn executors (added so a promoted DECIMAL256 compares instead of
// hitting the unreachable-code panic).
func TestLeastGreatestDecimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal256, 40, 2)
	mk := func(ss []string) []types.Decimal256 {
		out := make([]types.Decimal256, len(ss))
		for i, s := range ss {
			v, err := types.ParseDecimal256(s, typ.Width, typ.Scale)
			require.NoError(t, err)
			out[i] = v
		}
		return out
	}
	a := []string{"10.00", "20.00", "-5.00"}
	b := []string{"3.00", "25.00", "-1.00"}

	tcLeast := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, mk(a), nil),
			NewFunctionTestInput(typ, mk(b), nil),
		},
		NewFunctionTestResult(typ, false, mk([]string{"3.00", "20.00", "-5.00"}), nil),
		leastFn)
	ok, info := tcLeast.Run()
	require.True(t, ok, info)

	tcGreatest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, mk(a), nil),
			NewFunctionTestInput(typ, mk(b), nil),
		},
		NewFunctionTestResult(typ, false, mk([]string{"10.00", "25.00", "-1.00"}), nil),
		greatestFn)
	ok, info = tcGreatest.Run()
	require.True(t, ok, info)
}

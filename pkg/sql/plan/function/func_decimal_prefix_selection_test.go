// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestDecimalPrefixSelectionContracts(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		target := types.New(oid, 2, 0)
		target.Charset = 255
		for _, constant := range []bool{false, true} {
			shape := "flat"
			if constant {
				shape = "constant"
			}
			for _, state := range []string{"active", "partial", "all masked", "all bitmap", "empty", "NULL"} {
				t.Run(oid.String()+"/"+shape+"/"+state, func(t *testing.T) {
					input := NewFunctionTestInput(types.T_varchar.ToType(), []string{"12tail", "12tail"}, nil)
					if constant {
						input = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"12tail"}, nil)
					}
					length := 2
					nulls := []bool(nil)
					selection := (*FunctionSelectList)(nil)
					if state == "NULL" {
						input.nullList = []bool{true, true}
						nulls = []bool{true, true}
					}
					if state == "partial" {
						selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}}
						nulls = []bool{true, false}
					}
					if state == "all masked" {
						selection = &FunctionSelectList{AllNull: true}
						nulls = []bool{true, true}
					}
					if state == "all bitmap" {
						selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false}}
						nulls = []bool{true, true}
					}
					if state == "empty" {
						length = 0
					}
					var wanted any
					switch oid {
					case types.T_decimal64:
						wanted = []types.Decimal64{12, 12}
					case types.T_decimal128:
						wanted = []types.Decimal128{{B0_63: 12}, {B0_63: 12}}
					case types.T_decimal256:
						wanted = []types.Decimal256{{B0_63: 12}, {B0_63: 12}}
					}
					if length == 0 {
						wanted = emptyCastTargetValues(target)
					}
					fc := NewFunctionTestCase(proc, []FunctionTestInput{input, NewFunctionTestInput(target, emptyCastTargetValues(target), nil)}, NewFunctionTestResult(target, false, wanted, nulls), NewCast).WithSelectList(selection)
					t.Cleanup(fc.Free)
					fc.fnLength = length
					ok, info := fc.Run()
					require.True(t, ok, info)
				})
			}
		}
	}
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, constant := range []bool{false, true} {
			shape := "flat"
			if constant {
				shape = "constant"
			}
			t.Run(oid.String()+"/binary/"+shape, func(t *testing.T) {
				target := types.New(oid, 5, 0)
				target.Charset = 255
				input := NewFunctionTestInput(types.T_binary.ToType(), []string{"12", "12"}, nil)
				if constant {
					input = NewFunctionTestConstInput(types.T_binary.ToType(), []string{"12"}, nil)
				}
				value := uint64(12594)
				var wanted any
				switch oid {
				case types.T_decimal64:
					wanted = []types.Decimal64{types.Decimal64(value), types.Decimal64(value)}
				case types.T_decimal128:
					wanted = []types.Decimal128{{B0_63: value}, {B0_63: value}}
				case types.T_decimal256:
					wanted = []types.Decimal256{{B0_63: value}, {B0_63: value}}
				}
				fc := NewFunctionTestCase(proc, []FunctionTestInput{input, NewFunctionTestInput(target, emptyCastTargetValues(target), nil)}, NewFunctionTestResult(target, false, wanted, nil), NewCast)
				t.Cleanup(fc.Free)
				fc.parameters[0].SetIsBin(true)
				fc.fnLength = 2
				ok, info := fc.Run()
				require.True(t, ok, info)
			})
		}
	}
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		target := types.New(oid, 5, 0)
		target.Charset = 255
		var wanted any
		switch oid {
		case types.T_decimal64:
			wanted = []types.Decimal64{12, 49}
		case types.T_decimal128:
			wanted = []types.Decimal128{{B0_63: 12}, {B0_63: 49}}
		case types.T_decimal256:
			wanted = []types.Decimal256{{B0_63: 12}, {B0_63: 49}}
		}
		t.Run(oid.String()+"/mixed-numeric-literal-rows", func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"12tail", "1"}, nil),
				NewFunctionTestInput(target, emptyCastTargetValues(target), nil),
			}, NewFunctionTestResult(target, false, wanted, nil), NewCast)
			t.Cleanup(fc.Free)
			require.NoError(t, fc.parameters[0].SetIsBinRowsWithMP([]bool{false, true}, proc.Mp()))
			fc.fnLength = 2
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
	t.Run("mixed-numeric-literal-selection", func(t *testing.T) {
		target := types.NewWithCharset(types.T_decimal128, 5, 0, 255)
		selection := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
		fc := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"12tail", "1"}, nil),
			NewFunctionTestInput(target, emptyCastTargetValues(target), nil),
		}, NewFunctionTestResult(target, false,
			[]types.Decimal128{{B0_63: 12}, {}}, []bool{false, true}), NewCast).WithSelectList(selection)
		t.Cleanup(fc.Free)
		require.NoError(t, fc.parameters[0].SetIsBinRowsWithMP([]bool{false, true}, proc.Mp()))
		fc.fnLength = 2
		ok, info := fc.Run()
		require.True(t, ok, info)
	})
	t.Run("cached parameter shape and payload reuse", func(t *testing.T) {
		target := types.New(types.T_decimal128, 5, 0)
		target.Charset = 255
		fc := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"12tail", "56tail"}, nil),
			NewFunctionTestInput(target, []types.Decimal128{}, nil),
		}, NewFunctionTestResult(target, false, []types.Decimal128{{B0_63: 12}, {B0_63: 56}}, nil), NewCast)
		t.Cleanup(fc.Free)
		source := fc.parameters[0]
		for _, state := range []string{"flat", "constant", "NULL", "empty", "new flat payload"} {
			switch state {
			case "constant":
				source.SetClass(vector.CONSTANT)
				fc.expected.wanted = []types.Decimal128{{B0_63: 12}, {B0_63: 12}}
			case "NULL":
				source.GetNulls().Add(0)
				fc.expected.nullList = []bool{true, true}
			case "empty":
				fc.fnLength = 0
				fc.expected.wanted = []types.Decimal128{}
				fc.expected.nullList = nil
			case "new flat payload":
				source.SetClass(vector.FLAT)
				source.GetNulls().Reset()
				fc.fnLength = 2
				require.NoError(t, vector.SetStringAt(source, 0, "34tail", proc.Mp()))
				require.NoError(t, vector.SetStringAt(source, 1, "78tail", proc.Mp()))
				fc.expected.wanted = []types.Decimal128{{B0_63: 34}, {B0_63: 78}}
			}
			ok, info := fc.Run()
			require.True(t, ok, "state=%s: %s", state, info)
		}
	})

}

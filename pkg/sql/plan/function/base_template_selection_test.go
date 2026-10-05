// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnaryInactiveExecutionContracts(t *testing.T) {
	proc := testutil.NewProcess(nil)
	t.Cleanup(func() { proc.Base.FileService.Close(proc.Ctx); proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	cases := []struct {
		name    string
		input   FunctionTestInput
		target  types.Type
		wanted  any
		execute func(*int) executeLogicOfOverload
	}{
		{"opUnaryFixedToFixed", NewFunctionTestConstInput(types.T_int64.ToType(), []int64{7}, nil), types.T_int64.ToType(), []int64{9, 9}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryFixedToFixed(parameters, result, proc, length, func(v int64) int64 { (*calls)++; return int64(9) }, selection)
			}
		}},
		{"opUnaryBytesToFixed", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_int64.ToType(), []int64{9, 9}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryBytesToFixed(parameters, result, proc, length, func(v []byte) int64 { (*calls)++; return int64(9) }, selection)
			}
		}},
		{"opUnaryStrToFixed", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_int64.ToType(), []int64{9, 9}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryStrToFixed(parameters, result, proc, length, func(v string) int64 { (*calls)++; return int64(9) }, selection)
			}
		}},
		{"opUnaryBytesToBytes", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryBytesToBytes(parameters, result, proc, length, func(v []byte) []byte { (*calls)++; return []byte("result") }, selection)
			}
		}},
		{"opUnaryBytesToStr", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryBytesToStr(parameters, result, proc, length, func(v []byte) string { (*calls)++; return "result" }, selection)
			}
		}},
		{"opUnaryStrToStr", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryStrToStr(parameters, result, proc, length, func(v string) string { (*calls)++; return "result" }, selection)
			}
		}},
		{"opUnaryFixedToStr", NewFunctionTestConstInput(types.T_int64.ToType(), []int64{7}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryFixedToStr(parameters, result, proc, length, func(v int64) string { (*calls)++; return "result" }, selection)
			}
		}},
		{"opUnaryFixedToStrWithNullOnError", NewFunctionTestConstInput(types.T_int64.ToType(), []int64{7}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryFixedToStrWithNullOnError(parameters, result, proc, length, func(v int64) (string, error) { (*calls)++; return "result", nil }, selection)
			}
		}},
		{"opUnaryFixedToStrWithErrorCheck", NewFunctionTestConstInput(types.T_int64.ToType(), []int64{7}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryFixedToStrWithErrorCheck(parameters, result, proc, length, func(v int64) (string, error) { (*calls)++; return "result", nil }, selection)
			}
		}},
		{"opUnaryStrToBytesWithRowErrorCheck", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryStrToBytesWithRowErrorCheck(parameters, result, length, func(v string, row int) ([]byte, error) { (*calls)++; return []byte("result"), nil }, selection)
			}
		}},
		{"opUnaryBytesToBytesWithErrorCheck", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryBytesToBytesWithErrorCheck(parameters, result, proc, length, func(v []byte) ([]byte, error) { (*calls)++; return []byte("result"), nil }, selection)
			}
		}},
		{"opUnaryBytesToBytesWithResultNull", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryBytesToBytesWithResultNull(parameters, result, proc, length, func(v []byte) ([]byte, bool, error) { (*calls)++; return []byte("result"), false, nil }, selection)
			}
		}},
		{"opUnaryBytesToBytesWithNullOnError", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length, func(v []byte) ([]byte, error) { (*calls)++; return []byte("result"), nil }, selection)
			}
		}},
		{"opUnaryBytesToStrWithRowErrorCheck", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"input"}, nil), types.T_varchar.ToType(), []string{"result", "result"}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryBytesToStrWithRowErrorCheck(parameters, result, length, func(v []byte, row int) (string, error) { (*calls)++; return "result", nil }, selection)
			}
		}},
		{"opUnaryFixedToFixedWithErrorCheck", NewFunctionTestConstInput(types.T_int64.ToType(), []int64{7}, nil), types.T_int64.ToType(), []int64{9, 9}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryFixedToFixedWithErrorCheck(parameters, result, proc, length, func(v int64) (int64, error) { (*calls)++; return int64(9), nil }, selection)
			}
		}},
		{"opUnaryFixedToFixedWithNullCheck", NewFunctionTestConstInput(types.T_int64.ToType(), []int64{7}, nil), types.T_int64.ToType(), []int64{9, 9}, func(calls *int) executeLogicOfOverload {
			return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
				return opUnaryFixedToFixedWithNullCheck(parameters, result, length, func(v int64) (int64, bool) { (*calls)++; return int64(9), false }, selection)
			}
		}},
	}
	// Every distinct owner must suppress constant work for empty and fully masked batches.
	// Shape and metadata controls are concentrated on the fixed owner and the special string owner.
	for _, tc := range cases {
		shapes := []bool{true}
		if tc.name == "opUnaryFixedToFixed" || tc.name == "opUnaryFixedToStrWithNullOnError" {
			shapes = []bool{false, true}
		}
		for _, constant := range shapes {
			shape := "flat"
			if constant {
				shape = "constant"
			}
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				states := []string{"empty", "all bitmap"}
				if tc.name == "opUnaryFixedToFixed" || tc.name == "opUnaryFixedToStrWithNullOnError" {
					states = []string{"empty", "all flag", "all bitmap", "partial", "active", "NULL", "stale bitmap"}
				}
				for _, state := range states {
					t.Run(state, func(t *testing.T) {
						length := 2
						selection := (*FunctionSelectList)(nil)
						nullList := []bool(nil)
						input := tc.input
						wanted := tc.wanted
						expectCalls := 1
						if !constant {
							input.isConst = false
							expectCalls = 2
							if input.typ.Oid == types.T_int64 {
								input.values = []int64{7, 7}
							} else {
								input.values = []string{"input", "input"}
							}
						}
						switch state {
						case "empty":
							length = 0
							expectCalls = 0
							if tc.target.Oid == types.T_int64 {
								wanted = []int64{}
							} else {
								wanted = []string{}
							}
						case "all flag":
							selection = &FunctionSelectList{AllNull: true}
							nullList = []bool{true, true}
							expectCalls = 0
						case "all bitmap":
							selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, false}}
							nullList = []bool{true, true}
							expectCalls = 0
						case "partial":
							selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false}}
							nullList = []bool{true, false}
							expectCalls = 1
						case "stale bitmap":
							selection = &FunctionSelectList{AnyNull: false, SelectList: []bool{false, false}}
						case "NULL":
							input.nullList = []bool{true, true}
							nullList = []bool{true, true}
							expectCalls = 0
						}
						calls := 0
						fc := NewFunctionTestCase(proc, []FunctionTestInput{input}, NewFunctionTestResult(tc.target, false, wanted, nullList), tc.execute(&calls)).WithSelectList(selection)
						t.Cleanup(fc.Free)
						fc.fnLength = length
						ok, info := fc.Run()
						t.Logf("calls=%d expected=%d", calls, expectCalls)
						require.True(t, ok, info)
						require.Equal(t, expectCalls, calls)
						if state == "all bitmap" {
							require.False(t, fc.result.GetResultVector().GetNulls().Contains(uint64(length)), "mask tail must not escape the batch")
						}
					})
				}
			})
		}
	}

	for _, stringsResult := range []bool{false, true} {
		name := "fixed reuse"
		target := types.T_int64.ToType()
		var wanted any = []int64{8, 8}
		if stringsResult {
			name = "string reuse"
			target = types.T_varchar.ToType()
			wanted = []string{"first", "first"}
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			fn := func(parameters []*vector.Vector, result vector.FunctionResultWrapper, p *process.Process, n int, selection *FunctionSelectList) error {
				if stringsResult {
					return opUnaryFixedToStrWithNullOnError(parameters, result, p, n, func(v int64) (string, error) {
						calls++
						if v < 0 {
							return "", moerr.NewInvalidInputNoCtx("negative input")
						}
						if v == 7 {
							return "first", nil
						}
						return "second", nil
					}, selection)
				}
				return opUnaryFixedToFixed(parameters, result, p, n, func(v int64) int64 { calls++; return v + 1 }, selection)
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestConstInput(types.T_int64.ToType(), []int64{7}, nil)}, NewFunctionTestResult(target, false, wanted, []bool{true, true}), fn)
			t.Cleanup(fc.Free)
			fc.fnLength = 2
			steps := 3
			if stringsResult {
				steps = 5
			}
			for step := 0; step < steps; step++ {
				if step == 0 {
					fc.selectList = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, false}}
				} else {
					fc.selectList = nil
					fc.expected.nullList = nil
					if step == 2 {
						vector.MustFixedColNoTypeCheck[int64](fc.parameters[0])[0] = 11
						if stringsResult {
							fc.expected.wanted = []string{"second", "second"}
						} else {
							fc.expected.wanted = []int64{12, 12}
						}
					}
				}
				if step == 3 {
					vector.MustFixedColNoTypeCheck[int64](fc.parameters[0])[0] = -1
					fc.expected.nullList = []bool{true, true}
				}
				if step == 4 {
					vector.MustFixedColNoTypeCheck[int64](fc.parameters[0])[0] = 11
				}
				ok, info := fc.Run()
				require.True(t, ok, "step=%d: %s", step, info)
				require.Equal(t, step, calls)
			}
		})
	}

	t.Run("string conversion error admission", func(t *testing.T) {
		for _, bytesInput := range []bool{false, true} {
			name := "string"
			if bytesInput {
				name = "bytes"
			}
			for _, state := range []string{"empty", "all flag", "all bitmap", "active error"} {
				t.Run(name+"/"+state, func(t *testing.T) {
					calls := 0
					length := 2
					selection := (*FunctionSelectList)(nil)
					if state == "empty" {
						length = 0
					}
					if state == "all flag" {
						selection = &FunctionSelectList{AllNull: true}
					}
					if state == "all bitmap" {
						selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, false}}
					}
					if state == "active error" {
						selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false}}
					}
					fn := func(parameters []*vector.Vector, result vector.FunctionResultWrapper, p *process.Process, n int, s *FunctionSelectList) error {
						if bytesInput {
							return opUnaryBytesToFixedWithErrorCheck(parameters, result, p, n, func(v []byte) (int64, error) { calls++; return 0, moerr.NewInvalidInputNoCtx("inactive row evaluated") }, s)
						}
						return opUnaryStrToFixedWithErrorCheck(parameters, result, p, n, func(v string) (int64, error) { calls++; return 0, moerr.NewInvalidInputNoCtx("inactive row evaluated") }, s)
					}
					fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"bad"}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0}, nil), fn).WithSelectList(selection)
					t.Cleanup(fc.Free)
					fc.fnLength = length
					actual, err := fc.DebugRun()
					t.Logf("calls=%d error=%v", calls, err)
					if state == "active error" {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
						require.Equal(t, 1, calls)
						return
					}
					require.NoError(t, err)
					require.Zero(t, calls)
					require.Equal(t, types.T_int64.ToType(), *actual.GetType())
					require.Equal(t, length, actual.Length())
					for i := 0; i < length; i++ {
						require.True(t, actual.GetNulls().Contains(uint64(i)))
					}
				})
			}
		}

	})
}

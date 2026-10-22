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
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestEnumSetAssignmentModes(t *testing.T) {
	for _, mode := range []string{"strict", "nonstrict", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.Base.IsFrontend = true
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
				if mode == "nonstrict" {
					return "", nil
				}
				return "STRICT_TRANS_TABLES", nil
			})
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", mode == "ignore")
			for _, tc := range []struct {
				name, def string
				source    types.Type
				values    any
				target    types.Type
				want      any
				warnings  uint64
				fn        executeLogicOfOverload
			}{
				{"enum labels", "a,b", types.T_varchar.ToType(), []string{"bad", "a", "0"}, types.T_enum.ToType(), []types.Enum{0, 1, 0}, 1, CastValueToIndex},
				{"enum wide signed", "a,b", types.T_int64.ToType(), []int64{-1, 0, 2}, types.T_enum.ToType(), []types.Enum{0, 0, 2}, 2, castIntegerToEnum[int64]},
				{"enum wide unsigned", "a,b", types.T_uint64.ToType(), []uint64{65537, ^uint64(0), 2}, types.T_enum.ToType(), []types.Enum{0, 0, 2}, 2, castIntegerToEnum[uint64]},
				{"set partial names", "a,b", types.T_varchar.ToType(), []string{"a,x,y", "a,2", "b"}, types.T_uint64.ToType(), []uint64{1, 1, 2}, 2, CastSetValueToIndex},
				{"set quoted bitmap", "a,b", types.T_varchar.ToType(), []string{"9", "+2", "2 "}, types.T_uint64.ToType(), []uint64{0, 2, 0}, 2, CastSetValueToIndex},
				{"set signed bitmap", "a,b", types.T_int64.ToType(), []int64{-1, 9, 2}, types.T_uint64.ToType(), []uint64{3, 1, 2}, 2, castIntegerToSet[int64]},
				{"set unsigned bitmap", "a,b", types.T_uint64.ToType(), []uint64{^uint64(0), 9, 2}, types.T_uint64.ToType(), []uint64{3, 1, 2}, 2, castIntegerToSet[uint64]},
			} {
				t.Run(tc.name, func(t *testing.T) {
					sink := &arithmeticWarningSink{limit: 3}
					proc.WarningSink = sink
					budget := process.WarningDiagnosticBudgetForProcess(proc)
					fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.def, tc.def, tc.def}, nil), NewFunctionTestInput(tc.source, tc.values, nil)}, NewFunctionTestResult(tc.target, mode == "strict", tc.want, nil), tc.fn)
					defer fc.Free()
					if mode == "strict" {
						require.NoError(t, fc.result.PreExtendAndReset(3))
						err := fc.fn(fc.parameters, fc.result, proc, 3, nil)
						var failure *moerr.Error
						require.ErrorAs(t, err, &failure)
						require.True(t, failure.IsRealError())
						require.Equal(t, moerr.ErrEnumSetValueTruncated, failure.ErrorCode())
						require.Equal(t, uint16(1265), failure.MySQLCode())
						require.Equal(t, "01000", failure.SqlState())
						require.Zero(t, sink.total)
					} else {
						ok, info := fc.Run()
						require.True(t, ok, info)
						require.Equal(t, tc.warnings, sink.total)
						for _, r := range sink.records {
							require.Equal(t, uint16(1265), r.code)
						}
					}
					require.Zero(t, budget.Used(), "Flush/Reset must release the local accumulator's budget")
				})
			}
		})
	}
}

func TestEnumSetAssignmentPolicySources(t *testing.T) {
	for _, tc := range []struct {
		name, snapshot, live             string
		resolver, fail, frontend, adjust bool
	}{
		{name: "default strict"},
		{name: "strict snapshot", snapshot: "STRICT_ALL_TABLES"},
		{name: "explicit empty snapshot", snapshot: process.EmptySqlModeSentinel, adjust: true},
		{name: "nonstrict snapshot", snapshot: "NO_ENGINE_SUBSTITUTION", adjust: true},
		{name: "frontend empty overrides snapshot", snapshot: "TRADITIONAL", resolver: true, frontend: true, adjust: true},
		{name: "live strict overrides snapshot", snapshot: process.EmptySqlModeSentinel, live: "STRICT_TRANS_TABLES", resolver: true},
		{name: "remote empty preserves strict snapshot", snapshot: "TRADITIONAL", resolver: true},
		{name: "remote explicit empty snapshot", snapshot: process.EmptySqlModeSentinel, resolver: true, adjust: true},
		{name: "resolver failure uses snapshot", snapshot: process.EmptySqlModeSentinel, resolver: true, fail: true, adjust: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.Base.IsFrontend = tc.frontend
			proc.GetSessionInfo().SqlMode = tc.snapshot
			if tc.resolver {
				proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
					if tc.fail {
						return nil, errors.New("unavailable")
					}
					return tc.live, nil
				})
			}
			sink := &arithmeticWarningSink{limit: 1}
			proc.WarningSink = sink
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b"}, nil), NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad"}, nil)}, NewFunctionTestResult(types.T_enum.ToType(), !tc.adjust, []types.Enum{0}, nil), CastValueToIndex)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
			if tc.adjust {
				require.Equal(t, uint64(1), sink.total)
			} else {
				require.Zero(t, sink.total)
			}
		})
	}
	for _, callProc := range []*process.Process{nil, {}} {
		proc := testutil.NewProcess(t)
		func() {
			defer proc.Free()
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, nil), NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad"}, nil)}, NewFunctionTestResult(types.T_enum.ToType(), true, nil, nil), CastValueToIndex)
			defer fc.Free()
			require.NoError(t, fc.result.PreExtendAndReset(1))
			err := fc.fn(fc.parameters, fc.result, callProc, 1, nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrEnumSetValueTruncated))
		}()
	}
}

func TestEnumSetAssignmentMasksAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name           string
		source, target types.Type
		good, bad      any
		fn             executeLogicOfOverload
		def            string
	}{
		{"enum text", types.T_varchar.ToType(), types.T_enum.ToType(), []string{"a", "bad", "b"}, []string{"bad", "a", "bad"}, CastValueToIndex, "a,b"},
		{"enum integer", types.T_int64.ToType(), types.T_enum.ToType(), []int64{1, 9, 2}, []int64{9, 1, 9}, castIntegerToEnum[int64], "a,b"},
		{"set text", types.T_varchar.ToType(), types.T_uint64.ToType(), []string{"a", "bad", "b"}, []string{"bad", "a", "bad"}, CastSetValueToIndex, "a,b"},
		{"set integer", types.T_int64.ToType(), types.T_uint64.ToType(), []int64{1, 9, 2}, []int64{9, 1, 9}, castIntegerToSet[int64], "a,b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			sink := &arithmeticWarningSink{limit: 1}
			proc.WarningSink = sink
			inputs := func(values any) []FunctionTestInput {
				return []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.def, tc.def, tc.def}, nil), NewFunctionTestInput(tc.source, values, nil)}
			}
			selected := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
			want := any([]uint64{1, 0, 2})
			if tc.target.Oid == types.T_enum {
				want = []types.Enum{1, 0, 2}
			}
			fc := NewFunctionTestCase(proc, inputs(tc.good), NewFunctionTestResult(tc.target, false, want, []bool{false, true, false}), tc.fn).WithSelectList(selected)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
			require.Zero(t, sink.total)
			proc.GetSessionInfo().SqlMode = process.EmptySqlModeSentinel
			zero := any([]uint64{0, 0, 0})
			if tc.target.Oid == types.T_enum {
				zero = []types.Enum{0, 0, 0}
			}
			// Invalid selected rows adjust and warn twice, even when only one record fits.
			fc = NewFunctionTestCase(proc, inputs(tc.bad), NewFunctionTestResult(tc.target, false, zero, []bool{false, true, false}), tc.fn).WithSelectList(selected)
			// Numeric SET 9 retains the a bit, unlike the invalid text member.
			if tc.name == "set integer" {
				fc.expected.wanted = []uint64{1, 0, 1}
			}
			ok, info = fc.RunAndFree()
			require.True(t, ok, info)
			require.Equal(t, uint64(2), sink.total)
			require.Len(t, sink.records, 1)
			sink.total = 0
			sink.records = nil
			// NULL and all-skipped rows cannot parse even malformed metadata.
			badDef := ""
			if tc.target.Oid == types.T_enum {
				badDef = strings.Repeat("x,", types.MaxEnumLen)
			}
			for _, nullInput := range []bool{true, false} {
				in := inputs(tc.bad)
				in[0] = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{badDef}, nil)
				if nullInput {
					in[1].nullList = []bool{true, true, true}
				}
				fc = NewFunctionTestCase(proc, in, NewFunctionTestResult(tc.target, false, zero, []bool{true, true, true}), tc.fn)
				fc.fnLength = 3
				if !nullInput {
					fc.selectList = &FunctionSelectList{AllNull: true}
				}
				ok, info = fc.RunAndFree()
				require.True(t, ok, info)
				require.Zero(t, sink.total)
			}
			for _, ignore := range []bool{false, true} {
				proc.SetStmtProfile(&process.StmtProfile{})
				proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", ignore)
				invalidInputs := inputs(tc.bad)
				invalidInputs[0] = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{badDef}, nil)
				invalid := NewFunctionTestCase(proc, invalidInputs, NewFunctionTestResult(tc.target, true, nil, nil), tc.fn)
				func() {
					defer invalid.Free()
					require.NoError(t, invalid.result.PreExtendAndReset(3))
					err := invalid.fn(invalid.parameters, invalid.result, proc, 3, nil)
					require.Error(t, err)
					require.False(t, moerr.IsMoErrCode(err, moerr.ErrEnumSetValueTruncated))
					require.Zero(t, sink.total, "bad metadata never becomes an assignment warning")
				}()
			}

			fc = NewFunctionTestCase(proc, inputs(tc.bad), NewFunctionTestResult(tc.target, false, zero, nil), tc.fn)
			fc.fnLength = 0
			fc.expected.wanted = zero // Run verifies zero cardinality independently of source length.
			defer fc.Free()
			require.NoError(t, fc.result.PreExtendAndReset(0))
			require.NoError(t, fc.fn(fc.parameters, fc.result, proc, 0, nil))
			require.Zero(t, fc.result.GetResultVector().Length())
			require.Zero(t, sink.total)
		})
	}
	// Exercise lazy metadata/index construction above the 64-row threshold.
	for _, allSkipped := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		func() {
			defer proc.Free()
			invalid := strings.Repeat("x,", types.MaxEnumLen)
			nulls := make([]bool, 64)
			for i := range nulls {
				nulls[i] = true
			}
			input := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"bad"}, []bool{!allSkipped})
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{invalid}, nil), input}, NewFunctionTestResult(types.T_enum.ToType(), false, make([]types.Enum, 64), nulls), CastValueToIndex)
			fc.fnLength = 64
			if allSkipped {
				fc.selectList = &FunctionSelectList{AllNull: true}
			}
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		}()
	}

	// An adjusted row followed by bad metadata must discard pending local warnings.
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().SqlMode = process.EmptySqlModeSentinel
	sink := &arithmeticWarningSink{limit: 2}
	proc.WarningSink = sink
	budget := process.WarningDiagnosticBudgetForProcess(proc)
	fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b", ""}, nil), NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,x", "a"}, nil)}, NewFunctionTestResult(types.T_uint64.ToType(), true, nil, nil), CastSetValueToIndex)
	defer fc.Free()
	require.NoError(t, fc.result.PreExtendAndReset(2))
	err := fc.fn(fc.parameters, fc.result, proc, 2, nil)
	require.Error(t, err)
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrEnumSetValueTruncated))
	require.Zero(t, sink.total)
	require.Zero(t, budget.Used())
}

func TestEnumSetWarningRetentionAndFullBitmap(t *testing.T) {
	for _, limit := range []int{0, 1, 3} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.GetSessionInfo().SqlMode = process.EmptySqlModeSentinel
			sink := &arithmeticWarningSink{limit: limit}
			proc.WarningSink = sink
			bad := strings.Repeat("界", 100)
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a,b"}, nil), NewFunctionTestConstInput(types.T_varchar.ToType(), []string{bad}, nil)}, NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{0, 0, 0}, nil), CastValueToIndex)
			fc.fnLength = 3
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
			require.Equal(t, uint64(3), sink.total)
			require.Len(t, sink.records, limit)
			for _, r := range sink.records {
				require.True(t, utf8.ValidString(r.message))
				require.LessOrEqual(t, len(r.message), 128+len("Data truncated for ENUM value: ''"))
				require.Contains(t, r.message, "truncated")
			}
		})
	}
	proc := testutil.NewProcess(t)
	defer proc.Free()
	labels := make([]string, 64)
	for i := range labels {
		labels[i] = fmt.Sprintf("l%02d", i+1)
	}
	fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{strings.Join(labels, ",")}, nil), NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, -2, -9223372036854775808}, nil)}, NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{^uint64(0), ^uint64(0) - 1, 1 << 63}, nil), castIntegerToSet[int64])
	fc.fnLength = 3
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
}

func TestEnumValueIndexAssignmentBoundaries(t *testing.T) {
	for _, labelCount := range []int{7, 8} {
		labels := []string{"a ", "a", "02", "2", "e", "f", "g", "h"}[:labelCount]
		def := strings.Join(labels, ",")
		for _, rows := range []int{63, 64} {
			t.Run(fmt.Sprintf("%d-labels/%d-rows", labelCount, rows), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				defer proc.Free()
				proc.GetSessionInfo().SqlMode = process.EmptySqlModeSentinel
				inputs := []string{"a ", "a  ", "A ", "02", "2 ", "+2", "0", "0x2", "000002"}
				wantCycle := []types.Enum{1, 2, 1, 3, 4, 2, 0, 0, 0}
				values := make([]string, rows)
				want := make([]types.Enum, rows)
				for i := range rows {
					values[i] = inputs[i%len(inputs)]
					want[i] = wantCycle[i%len(inputs)]
				}
				fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{def}, nil), NewFunctionTestInput(types.T_varchar.ToType(), values, nil)}, NewFunctionTestResult(types.T_enum.ToType(), false, want, nil), CastValueToIndex)
				fc.fnLength = rows
				ok, info := fc.RunAndFree()
				require.True(t, ok, info)
			})
		}
	}
}

// Typed-source conversion shares the ordinary assignment policy. The stored
// error member remains distinct from a declared empty member in every mode.
func TestEnumSetTypedSourceAssignmentModes(t *testing.T) {
	for _, mode := range []string{"strict", "nonstrict", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.Base.IsFrontend = true
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
				if mode == "nonstrict" {
					return "", nil
				}
				return "STRICT_TRANS_TABLES", nil
			})
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Update", "DML", mode == "ignore")
			for _, constant := range []bool{false, true} {
				t.Run(fmt.Sprintf("constant=%v", constant), func(t *testing.T) {
					sink := &arithmeticWarningSink{limit: 1}
					proc.WarningSink = sink
					source := NewFunctionTestInput(types.T_varchar.ToType(), []string{",bad,blue", ",bad,blue", ",bad,blue", ",bad,blue"}, nil)
					if constant {
						source = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{",bad,blue"}, nil)
					}
					fc := NewFunctionTestCase(proc, []FunctionTestInput{
						NewFunctionTestConstInput(types.T_varchar.ToType(), []string{",blue"}, nil), source,
						NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{0, 2, 3, 1}, nil),
					}, NewFunctionTestResult(types.T_enum.ToType(), mode == "strict", []types.Enum{0, 0, 2, 1}, nil), CastValueToIndex)
					defer fc.Free()
					fc.fnLength = 4
					ok, info := fc.Run()
					require.True(t, ok, info)
					if mode == "strict" {
						require.Zero(t, sink.total)
					} else {
						require.Equal(t, uint64(1), sink.total)
						require.Equal(t, uint16(1265), sink.records[0].code)
					}
					require.Zero(t, process.WarningDiagnosticBudgetForProcess(proc).Used())
				})
			}
		})
	}
}

func TestEnumSetTypedSourceLazyDecode(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		source                  []string
		ordinals                []types.Enum
		sourceNull, ordinalNull []bool
		mask                    *FunctionSelectList
		want                    []types.Enum
		nulls                   []bool
		fail                    bool
	}{
		{name: "constant repeated empty", source: []string{",red,blue"}, ordinals: []types.Enum{3, 1, 3, 0}, want: []types.Enum{2, 1, 2, 0}},
		{name: "row definitions", source: []string{"red,blue", "blue,red"}, ordinals: []types.Enum{1, 1}, want: []types.Enum{3, 2}},
		{name: "masked invalid", source: []string{"red,blue"}, ordinals: []types.Enum{1, 99, 2}, mask: &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}, want: []types.Enum{3, 0, 2}, nulls: []bool{false, true, false}},
		{name: "selected invalid", source: []string{"red,blue"}, ordinals: []types.Enum{1, 99}, fail: true},
		{name: "null ordinal", source: []string{"red,blue"}, ordinals: []types.Enum{99, 2}, ordinalNull: []bool{true, false}, want: []types.Enum{0, 2}, nulls: []bool{true, false}},
		{name: "null source", source: []string{"bad"}, sourceNull: []bool{true}, ordinals: []types.Enum{99, 0}, want: []types.Enum{0, 0}, nulls: []bool{true, true}},
		{name: "all masked", source: []string{"bad"}, ordinals: []types.Enum{99, 99}, mask: &FunctionSelectList{AllNull: true}, want: []types.Enum{0, 0}, nulls: []bool{true, true}},
		{name: "warning then corrupt ordinal", source: []string{"missing,blue"}, ordinals: []types.Enum{1, 99}, fail: true},
		{name: "empty batch", source: []string{"bad"}, ordinals: []types.Enum{}, want: []types.Enum{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.GetSessionInfo().SqlMode = process.EmptySqlModeSentinel
			sink := &arithmeticWarningSink{limit: 1}
			proc.WarningSink = sink
			source := NewFunctionTestInput(types.T_varchar.ToType(), tc.source, tc.sourceNull)
			if len(tc.source) == 1 {
				source = NewFunctionTestConstInput(types.T_varchar.ToType(), tc.source, tc.sourceNull)
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{",blue,red"}, nil), source,
				NewFunctionTestInput(types.T_enum.ToType(), tc.ordinals, tc.ordinalNull),
			}, NewFunctionTestResult(types.T_enum.ToType(), tc.fail, tc.want, tc.nulls), CastValueToIndex).WithSelectList(tc.mask)
			defer fc.Free()
			fc.fnLength = len(tc.ordinals)
			ok, info := fc.Run()
			require.True(t, ok, info)
			require.Zero(t, sink.total, "masked/NULL rows and aborted calls must not publish warnings")
			require.Zero(t, process.WarningDiagnosticBudgetForProcess(proc).Used())
		})
	}
}

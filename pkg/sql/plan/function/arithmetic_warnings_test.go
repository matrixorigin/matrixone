// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type arithmeticWarningSink struct {
	uncompressWarningSink
	limit int
}

func (s *arithmeticWarningSink) GetWarningRetentionLimit() int { return s.limit }

func TestDivisionWarningModeSourcesAndScope(t *testing.T) {
	for _, tc := range []struct {
		name, statement, query, snapshot, local string
		resolver, failed                        bool
		want                                    uint64
	}{
		{name: "remote snapshot", statement: "Select", query: "DQL", snapshot: "TRADITIONAL", want: 1},
		{name: "local enabled", statement: "Select", query: "DQL", local: "ERROR_FOR_DIVISION_BY_ZERO", resolver: true, want: 1},
		{name: "local empty overrides snapshot", statement: "Select", query: "DQL", snapshot: "TRADITIONAL", resolver: true},
		{name: "resolver failure preserves NULL", statement: "Select", query: "DQL", snapshot: "TRADITIONAL", resolver: true, failed: true},
		{name: "strict only", statement: "Select", query: "DQL", snapshot: "STRICT_ALL_TABLES"},
		{name: "DML", statement: "Insert", query: "DML", snapshot: "TRADITIONAL"},
		{name: "CTAS", statement: "Create Table", query: "DDL", snapshot: "TRADITIONAL"},
		{name: "explain", statement: "Explain", query: "DQL", snapshot: "TRADITIONAL"},
		{name: "unknown", snapshot: "TRADITIONAL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile(tc.statement, tc.query, false)
			proc.GetSessionInfo().SqlMode = tc.snapshot
			if tc.resolver {
				proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
					if tc.failed {
						return nil, errors.New("unavailable")
					}
					return tc.local, nil
				})
			}
			sink := &arithmeticWarningSink{limit: 2}
			proc.WarningSink = sink
			typ := types.T_int64.ToType()
			test := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(typ, []int64{1}, nil),
				NewFunctionTestInput(typ, []int64{0}, nil),
			}, NewFunctionTestResult(typ, false, nil, nil), integerDivFn)
			defer test.Free()
			require.Equal(t, tc.want > 0, ScalarDivisionNeedsRowEvaluation(test.parameters, proc))
			require.NoError(t, test.result.PreExtendAndReset(1))
			require.NoError(t, test.fn(test.parameters, test.result, proc, 1, nil))
			require.True(t, test.GetResultVectorDirectly().IsNull(0))
			require.Equal(t, tc.want, sink.total)
		})
	}
}

func TestDivisionWarningBoundariesAndRetention(t *testing.T) {
	for _, limit := range []int{0, 1, 3} {
		t.Run(string(rune('0'+limit)), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
			proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
			sink := &arithmeticWarningSink{limit: limit}
			proc.WarningSink = sink
			typ := types.T_float64.ToType()
			test := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(typ, []float64{0, 1, 1, 1, 4}, nil),
				NewFunctionTestInput(typ, []float64{0, math.Copysign(0, -1), 0, 0, 2}, []bool{false, false, true, false, false}),
			}, NewFunctionTestResult(typ, false, nil, nil), divFn)
			defer test.Free()
			require.NoError(t, test.result.PreExtendAndReset(5))
			// Row 3 is masked, row 2 has a NULL divisor, row 4 is a control.
			selection := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false, true, false}}
			require.NoError(t, test.fn(test.parameters, test.result, proc, 5, selection))
			require.Equal(t, uint64(2), sink.total)
			require.Len(t, sink.records, min(limit, 2))
			require.True(t, test.GetResultVectorDirectly().IsNull(0))
			require.True(t, test.GetResultVectorDirectly().IsNull(1))
			require.False(t, test.GetResultVectorDirectly().IsNull(4))
			// Reuse the invocation with no rows and an all-masked batch.
			require.NoError(t, test.result.PreExtendAndReset(0))
			require.NoError(t, test.fn(test.parameters, test.result, proc, 0, nil))
			require.NoError(t, test.result.PreExtendAndReset(5))
			require.NoError(t, test.fn(test.parameters, test.result, proc, 5, &FunctionSelectList{AnyNull: true, AllNull: true}))
			require.Equal(t, uint64(2), sink.total)
		})
	}
}

func TestDivisionWarningsMixedIntegerDomains(t *testing.T) {
	for _, inputs := range [][]FunctionTestInput{
		{NewFunctionTestInput(types.New(types.T_bit, 64, 0), []uint64{1}, nil), NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil)},
		{NewFunctionTestInput(types.New(types.T_bit, 64, 0), []uint64{1}, nil), NewFunctionTestConstInput(types.T_uint64.ToType(), []uint64{0}, nil)},
		{NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1}, nil), NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil)},
		{NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1}, nil), NewFunctionTestConstInput(types.New(types.T_decimal256, 65, 2), []types.Decimal256{{}}, nil)},
	} {
		proc := testutil.NewProcess(t)
		func() {
			defer proc.Free()
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
			proc.GetSessionInfo().SqlMode = "TRADITIONAL"
			sink := &arithmeticWarningSink{limit: 1}
			proc.WarningSink = sink
			resultType := types.T_int64.ToType()
			if inputs[0].typ.Oid == types.T_bit {
				resultType = types.T_uint64.ToType()
			}
			test := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(resultType, false, nil, nil), integerDivFn)
			defer test.Free()
			require.True(t, ScalarDivisionNeedsRowEvaluation(test.parameters, proc))
			require.NoError(t, test.result.PreExtendAndReset(1))
			require.NoError(t, test.fn(test.parameters, test.result, proc, 1, nil))
			require.Equal(t, uint64(1), sink.total)
			require.True(t, test.GetResultVectorDirectly().IsNull(0))
		}()
	}
}

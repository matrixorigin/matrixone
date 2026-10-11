// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestPreparedCaseTypedNullOwnsReturnDomain(t *testing.T) {
	for _, declaration := range []string{"signed", "char", "binary"} {
		for _, source := range []types.T{types.T_any, types.T_varchar, types.T_varbinary, types.T_binary} {
			t.Run(declaration+"/"+source.String(), func(t *testing.T) {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
					"select case when ? then cast(null as "+declaration+") else ? end", 1)
				require.NoError(t, err)
				t.Cleanup(stmt.Free)
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				var value any = "A"
				if source == types.T_any {
					value = nil
				}
				sourceType := source.ToType()
				if source == types.T_binary {
					sourceType.Width = -1
				}
				values := []any{
					ParamValue{Value: int64(0), SourceType: types.T_int64.ToType(), HasSourceType: true},
					ParamValue{Value: value, SourceType: sourceType, HasSourceType: true, EnableNumericPrefix: true},
				}
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: sourceType}}
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
				require.NoError(t, err)
				filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), bound.Plan, values)
				require.NoError(t, err)
				query := filled.GetQuery()
				expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
				executor, err := colexec.NewExpressionExecutor(mock.ctxt.GetProcess(), expr)
				require.NoError(t, err)
				defer executor.Free()
				out, err := executor.Eval(mock.ctxt.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				if declaration == "signed" {
					if source == types.T_any {
						require.Equal(t, types.T_int64, out.GetType().Oid)
					} else {
						require.True(t, out.GetType().Oid.IsMySQLString(), "a first real string binding remains a native CASE value")
						require.Equal(t, "A", string(out.GetBytesAt(0)))
					}
				} else {
					domain := types.StringDomainText
					if declaration == "binary" {
						domain = types.StringDomainBinary
					}
					require.Equal(t, domain, types.StaticStringDomain(*out.GetType()))
					require.Equal(t, types.T_varchar, out.GetType().Oid, "unknown marker schema must not expand to TEXT/LONGBLOB")
					if source != types.T_any {
						require.Equal(t, "A", string(out.GetBytesAt(0)), expr.String())
					}
				}
				if source == types.T_any {
					require.True(t, out.IsConstNull() || out.GetNulls().Contains(0))
				}
			})
		}
	}
}

func TestPreparedCaseNumericNullNativeResolution(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select case when ? then cast(null as signed) else ? end", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	var domains map[int32]types.Type
	for _, value := range []any{nil, "A", "9007199254740993", "9007199254740993x", float64(1.555), "1.555x"} {
		source := types.T_any.ToType()
		if value != nil {
			source = types.T_varchar.ToType()
		}
		if _, ok := value.(float64); ok {
			source = types.T_float64.ToType()
		}
		bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: source}}
		values := []any{ParamValue{Value: int64(0), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: source, HasSourceType: true, EnableNumericPrefix: true}}
		mock.ctxt.SetContext(WithPreparedFieldConsumers(mock.ctxt.GetContext(), domains, false))
		bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
		require.NoError(t, err)
		require.Contains(t, bound.FieldDomains, int32(1))
		expected := types.T_int64
		if _, ok := value.(float64); ok || value == "1.555x" {
			expected = types.T_any
		}
		require.Equal(t, expected, bound.FieldDomains[1].Oid)
		proc := mock.CurrentContext().GetProcess()
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(mock.ctxt.GetContext(), bound.Plan, values)
		require.NoError(t, err)
		query := filled.GetQuery()
		expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
		executor, err := colexec.NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		if value == "A" {
			require.Equal(t, int64(0), vector.GetFixedAtWithTypeCheck[int64](out, 0))
		}
		if value == "9007199254740993" || value == "9007199254740993x" {
			require.Equal(t, int64(9007199254740993), vector.GetFixedAtWithTypeCheck[int64](out, 0), "the numeric NULL default retains exact integers above 2^53")
		}
		if value == "1.555x" {
			require.Equal(t, "1.555x", string(out.GetBytesAt(0)))
		}
		executor.Free()
		domains = bound.FieldDomains
	}
}

func TestPreparedCaseDecimalNullDefaultKeepsNativeStrings(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select case when ? then cast(null as decimal(20,3)) else ? end", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	for _, binary := range []bool{false, true} {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		var domains map[int32]types.Type
		for index, value := range []any{nil, "A", "1.555x"} {
			source := types.T_any.ToType()
			if value != nil {
				source = types.T_varchar.ToType()
			}
			bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: source}}
			values := []any{ParamValue{Value: int64(0), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: source, HasSourceType: true, EnableNumericPrefix: true}}
			mock.ctxt.SetContext(WithPreparedFieldConsumers(mock.ctxt.GetContext(), domains, binary))
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			domains = bound.FieldDomains
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(mock.ctxt.GetContext(), bound.Plan, values)
			require.NoError(t, err)
			query := filled.GetQuery()
			executor, err := colexec.NewExpressionExecutor(mock.ctxt.GetProcess(), query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0])
			require.NoError(t, err)
			out, err := executor.Eval(mock.ctxt.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			if index > 0 {
				require.Equal(t, types.T_varchar, out.GetType().Oid)
				require.Equal(t, value, string(out.GetBytesAt(0)), "SQL and COM preserve native DECIMAL-marker strings")
			} else {
				require.Equal(t, types.T_decimal128, out.GetType().Oid)

			}
			executor.Free()
		}
	}
}

func TestPreparedCaseTypedNullPreservesUnboundedValues(t *testing.T) {
	for _, declaration := range []string{"char", "binary"} {
		t.Run(declaration, func(t *testing.T) {
			value := strings.Repeat("A", types.MaxStringSize+1)
			source := types.T_varchar.ToType()
			if declaration == "binary" {
				value = strings.Repeat("\xff\x00A", types.MaxStringSize/3+1)
				source = types.T_varbinary.ToType()
			}
			source.Width = -1
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select case when ? then cast(null as "+declaration+") else ? end", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: source}}
			values := []any{ParamValue{Value: int64(0), SourceType: bindings[0].Type, HasSourceType: true, IsBinaryProtocol: true},
				ParamValue{Value: value, SourceType: source, HasSourceType: true, IsBinaryProtocol: true}}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), bound.Plan, values)
			require.NoError(t, err)
			query := filled.GetQuery()
			expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(mock.ctxt.GetProcess(), expr)
			require.NoError(t, err)
			defer executor.Free()
			out, err := executor.Eval(mock.ctxt.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, types.T_varchar, out.GetType().Oid)
			before := expr.Typ
			columns := GetResultColumnsFromPlan(filled)
			require.Len(t, columns, 1)
			require.Greater(t, columns[0].Typ.Width, int32(0))
			require.LessOrEqual(t, columns[0].Typ.Width, int32(types.MaxStringSize))
			require.Equal(t, before, expr.Typ, "wire presentation does not constrain or mutate the runtime envelope")
			require.Len(t, out.GetBytesAt(0), len(value), "bound=%v filled=%v", bound.Plan.GetQuery().Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0].Typ, expr.Typ)
			require.Equal(t, value, string(out.GetBytesAt(0)), "unbounded internal casts preserve length and raw bytes")
		})
	}
}

func TestPreparedCaseTypedNullDoesNotOverrideValueBoundary(t *testing.T) {
	for _, operand := range []string{"cast(? as char)", "'A'"} {
		t.Run(operand, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select case when ? then cast(null as signed) else "+operand+" end", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			values := []any{ParamValue{Value: int64(0), SourceType: types.T_int64.ToType(), HasSourceType: true}}
			bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}}
			if operand == "cast(? as char)" {
				values = append(values, ParamValue{Value: "A", SourceType: types.T_varbinary.ToType(), HasSourceType: true})
				bindings = append(bindings, PreparedSourceBinding{Position: 1, Type: types.T_varbinary.ToType()})
			}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), bound.Plan, values)
			require.NoError(t, err)
			query := filled.GetQuery()
			expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(mock.ctxt.GetProcess(), expr)
			require.NoError(t, err)
			defer executor.Free()
			out, err := executor.Eval(mock.ctxt.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, "A", string(out.GetBytesAt(0)))
		})
	}
}

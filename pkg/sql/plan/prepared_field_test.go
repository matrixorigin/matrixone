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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldResolvedStringDomain(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		binary    bool
	}{
		{"bare", "select field(?, 'A', 'a')", false},
		{"binary peer", "select field(?, _binary 'A', _binary 'a')", true},
		{"explicit binary", "select field(cast(? as binary), 'A', 'a')", true},
		{"explicit text", "select field(cast(? as char), 'A', 'a')", false},
		{"coalesce null", "select field(coalesce(?, null), 'A', 'a')", true},
		{"coalesce binary", "select field(coalesce(?, _binary 'x'), 'A', 'a')", true},
		{"coalesce text", "select field(coalesce(?, 'x'), 'A', 'a')", false},
		{"if", "select field(if(true, ?, null), 'A', 'a')", true},
		{"case", "select field(case when true then ? else null end, 'A', 'a')", true},
		{"nullif return", "select field(nullif(?, 'x'), 'A', 'a')", false},
		{"nullif condition", "select field(nullif('a', ?), 'A', 'a')", false},
		{"derived", "select field(x, 'A', 'a') from (select ? x limit 1) d", false},
		{"scalar", "select field((select ? limit 1), 'A', 'a')", false},
		{"max", "select field(max(?), 'A', 'a')", false},
		{"window", "select field(x, 'A', 'a') from (select max(?) over () x) d", false},
		{"derived case", "select field(x, 'A', 'a') from (select case when true then ? else null end x limit 1) d", true},
		{"union text", "select field(x, 'A', 'a') from (select ? x union all select 'a') d", false},
		{"union binary", "select field(x, 'A', 'a') from (select ? x union all select _binary 'a') d", true},
	} {
		for _, sourceOID := range []types.T{types.T_varchar, types.T_varbinary} {
			t.Run(tc.name+"/"+sourceOID.String(), func(t *testing.T) {
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				proc := mock.ctxt.GetProcess()
				params := vector.NewVec(types.T_text.ToType())
				t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
				require.NoError(t, vector.AppendBytes(params, []byte("a"), false, proc.Mp()))
				proc.SetPrepareParams(params)
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				source := sourceOID.ToType()
				bindings := []PreparedSourceBinding{{Position: 0, Type: source}}
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings,
					[]any{ParamValue{Value: "a", SourceType: source, HasSourceType: true}})
				require.NoError(t, err)
				field := findPlanFunctionExpr(bound.Plan, "field")
				require.NotNil(t, field)
				got := types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])) == types.StringDomainBinary
				require.Equal(t, tc.binary, got, field.String())
				require.Equal(t, source, bindings[0].Type, "consumer changed the marker's source domain")
			})
		}
	}
}

func TestFieldResolvedSelectorStringDomain(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		binary bool
	}{
		{"select field(lag(NULL, 1, 'a') over (), 'A', 'a')", false},
		{"select field(lag(NULL, 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lag('x', 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lag(_binary 'x', 1, 'a') over (), 'A', 'a')", true},
		{"select field(lead(NULL, 1, 'a') over (), 'A', 'a')", false},
		{"select field(lead(NULL, 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lead('x', 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lead(_binary 'x', 1, 'a') over (), 'A', 'a')", true},
		{"select field(coalesce('a', _binary 'x'), 'A', 'a')", true},
		{"select field(coalesce(_binary 'a', 'x'), 'A', 'a')", true},
		{"select field(coalesce('a', 'x'), 'A', 'a')", false},
		{"select field(nullif('a', _binary 'x'), 'A', 'a')", false},
		{"select field(x, 'A', 'a') from (select coalesce('a', _binary 'x') x limit 1) d", true},
		{"select field(x,'A','a') from (select _binary 'a' x union all select '') d where x<>''", true},
		{"select field(x,'A','a') from (select 'z' x union all select _binary 'a') d where x<>'z'", true},
		{"select field(x,'A','a') from (select _binary 'a' x union select '') d where x<>''", true},
		{"select field(x,'A','a') from (select 'a' x union all select '') d where x<>''", false},
		{"select field(x,'A','a') from (select cast(_binary 'a' as char) x union all select '') d where x<>''", false},
		{"select field(x,'A','a') from (select cast('a' as binary(1)) x union all select '') d where x<>''", true},
		{"select field(cast(x as char),'A','a') from (select _binary 'a' x union all select '') d where x<>''", false},
		{"select field(cast(x as binary(1)),'A','a') from (select 'a' x union all select '') d where x<>''", true},
		{"select field(concat_ws('',elt(1,x)),'A','a') from (select _binary 'a' x union all select '') d where x<>''", true},
		{"select field(concat_ws('',elt(1,cast(x as char))),'A','a') from (select _binary 'a' x union all select '') d where x<>''", false},
		{"select field(lower(x),'A','a') from (select _binary 'A' x union all select '') d where x<>''", false},
		{"select field(concat_ws('',lower(x)),'A','a') from (select _binary 'A' x union all select '') d where x<>''", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			field := findPlanFunctionExpr(bound, "field")
			require.NotNil(t, field)
			got := types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])) == types.StringDomainBinary
			require.Equal(t, tc.binary, got, field.String())
		})
	}
}

func TestPreparedFieldSetConversionSource(t *testing.T) {
	for _, tc := range []struct {
		name             string
		explicit, folded bool
	}{
		{"set conversion", false, false},
		{"folded set conversion", false, true},
		{"SQL declaration", true, false},
		{"folded SQL declaration", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			target := types.T_text.ToType()
			converted, err := appendSetOperationCastBeforeExpr(ctx, makePlan2StringConstExprWithType("a", true), makePlan2Type(&target))
			require.NoError(t, err)
			require.True(t, isCastOverload(converted, 3))
			converted.GetF().SyntaxExplicitCast = tc.explicit
			require.True(t, isExplicitPreparedCast(converted), "do not change the shared numeric-specialization predicate")
			if tc.folded {
				folded := makePlan2StringConstExprWithType("a")
				folded.GetLit().Src = converted
				converted = folded
			}
			r := newPreparedFieldStringResolver(&baseBinder{sysCtx: ctx})
			require.Equal(t, !tc.explicit, r.expr(converted).kind == fieldStringBinary)
		})
	}
}

func TestPreparedFieldTypeHistory(t *testing.T) {
	for _, tc := range []struct {
		name           string
		binaryProtocol bool
		sequence       []types.T
		want           []types.T
	}{
		{"SQL text first", false, []types.T{types.T_varchar, types.T_any, types.T_blob, types.T_varchar}, []types.T{types.T_text, types.T_text, types.T_text, types.T_text}},
		{"SQL binary first", false, []types.T{types.T_blob, types.T_varchar, types.T_any, types.T_blob}, []types.T{types.T_blob, types.T_blob, types.T_blob, types.T_blob}},
		{"SQL null first", false, []types.T{types.T_any, types.T_varchar, types.T_blob}, []types.T{types.T_any, types.T_any, types.T_any}},
		{"COM binary first", true, []types.T{types.T_blob, types.T_varchar, types.T_any}, []types.T{types.T_text, types.T_text, types.T_text}},
		{"numeric transition", false, []types.T{types.T_blob, types.T_int64, types.T_varchar, types.T_blob}, []types.T{types.T_blob, types.T_int64, types.T_int64, types.T_int64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select field(case when ? then ? else null end, 'Z', 'A', 'a')", 1)
			require.NoError(t, err)
			defer stmt.Free()
			var state *PreparedFieldTypes
			for i, oid := range tc.sequence {
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
				var value any = "a"
				if oid == types.T_any {
					value = nil
				}
				if oid == types.T_int64 {
					value = int64(123)
				}
				values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true, IsBinaryProtocol: tc.binaryProtocol},
					ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true, IsBinaryProtocol: tc.binaryProtocol}}
				ctx.SetContext(WithPreparedFieldTypes(context.Background(), state))
				var priorType types.Type
				if state != nil {
					priorType = state.cases[0].result
				}
				bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
				require.NoError(t, err)
				require.NotNil(t, bound.FieldTypes)
				if state != nil {
					require.Equal(t, priorType, state.cases[0].result, "reprepare must not mutate the prior snapshot")
				}
				require.Equal(t, tc.want[i], bound.FieldTypes.cases[0].result.Oid, "execution %d", i)
				if state != nil && tc.want[i] == tc.want[i-1] {
					require.Same(t, state, bound.FieldTypes, "compatible execution must retain immutable snapshot")
				}
				state = bound.FieldTypes
				require.Equal(t, oid, bindings[1].Type.Oid, "consumer must not rewrite source")
			}
		})
	}
}

func TestPreparedFieldHistoryRelations(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		binary bool
	}{
		{"select field(x, 'A', 'a') from (select case when ? then ? else null end x limit 1) d", true},
		{"select field((select case when ? then ? else null end limit 1), 'A', 'a')", true},
		{"select field(max(case when ? then ? else null end), 'A', 'a')", true},
		{"select field(x, 'A', 'a') from (select max(case when ? then ? else null end) over () x) d", true},
		{"select field(coalesce(case when ? then ? else null end, 'x'), 'A', 'a')", false},
		{"select field(case when ? then reverse(?) else null end, 'A', 'a')", false},
		{"select field(x,'A','a') from (select case when ? then ? else null end x union all select 'a') d", false},
		{"with d as (select case when ? then ? else null end x) select field(x,'A','a'),field(x,'a','A') from d", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			var state *PreparedFieldTypes
			for _, oid := range []types.T{types.T_any, types.T_varchar, types.T_blob, types.T_varchar} {
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
				var value any = "a"
				if oid == types.T_any {
					value = nil
				}
				values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}}
				ctx.SetContext(WithPreparedFieldTypes(context.Background(), state))
				var priorType types.Type
				if state != nil {
					priorType = state.cases[0].result
				}
				bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
				require.NoError(t, err)
				if state != nil {
					require.Same(t, state, bound.FieldTypes, "compatible string/NULL changes cannot reset history")
					require.Equal(t, priorType, state.cases[0].result, "published snapshots must remain immutable")
				}
				state = bound.FieldTypes
				require.Len(t, state.cases, 1, "shared relational consumers own one fact")
				field := findPlanFunctionExpr(bound.Plan, "field")
				require.NotNil(t, field)
				if oid != types.T_any {
					require.Equal(t, tc.binary, types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])) == types.StringDomainBinary)
				}
			}
		})
	}
}

func TestPreparedFieldCaseIdentity(t *testing.T) {
	sql := "with c as (select case when ? then ? else null end x) select case when true then (select case when ? then ? else null end) else x end from c where case when ? then true else false end"
	first, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer first.Free()
	second, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer second.Free()
	a, b := tree.CaseExpressions(first), tree.CaseExpressions(second)
	require.Len(t, a, 4)
	require.Len(t, b, 4)
	for i := range a {
		require.NotSame(t, a[i], b[i])
		require.Equal(t, tree.String(a[i], dialect.MYSQL), tree.String(b[i], dialect.MYSQL))
	}
	require.Equal(t, a, tree.CaseExpressions(first), "enumeration must not mutate the AST")
}

func TestPreparedFieldOverflowAdmission(t *testing.T) {
	signed := types.T_int64.ToType()
	snapshot := &PreparedFieldTypes{revision: 1, params: map[int32]preparedFieldParameter{0: {typ: signed}}}
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"9223372036854775807", false}, {"-9223372036854775808", false},
		{"9223372036854775808", true}, {"18446744073709551615", true},
		{"a", false}, {"18446744073709551615tail", false}, {"1e100000", true}, {"1e-100000", false},
	} {
		bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}}
		require.Equal(t, tc.want, snapshot.NeedsReprepare(bindings, []any{ParamValue{Value: tc.value}}), tc.value)
	}
	require.Equal(t, signed, snapshot.params[0].typ)
	require.False(t, (&PreparedFieldTypes{params: map[int32]preparedFieldParameter{0: {typ: signed, inherited: true}}}).NeedsReprepare(
		[]PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}}, []any{"18446744073709551615"}))
}

func TestPreparedFieldUnrelatedStatementsKeepNoHistory(t *testing.T) {
	for _, sql := range []string{"select ?", "select case when ? then ? else null end", "select field(cast(case when ? then ? else null end as char),'A','a')", "select field(case when true then 'a' else 'b' end,'A','a')"} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			ctx.SetContext(WithPreparedFieldTypes(context.Background(), nil))
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bindings := make([]PreparedSourceBinding, tree.ParameterCount(stmt))
			values := make([]any, len(bindings))
			for i := range bindings {
				bindings[i] = PreparedSourceBinding{Position: int32(i), Type: types.T_text.ToType()}
				values[i] = ParamValue{Value: "1", SourceType: bindings[i].Type, HasSourceType: true}
			}
			bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
			require.NoError(t, err)
			require.Nil(t, bound.FieldTypes)
		})
	}
}

func TestPreparedFieldWrappedTypeHistory(t *testing.T) {
	for _, wrapper := range []string{"concat(?)", "concat_ws('', ?)", "(select ? limit 1)"} {
		for _, first := range []types.T{types.T_any, types.T_varchar, types.T_blob, types.T_int64} {
			t.Run(wrapper+"/"+first.String(), func(t *testing.T) {
				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select field(case when ? then "+wrapper+" else null end,'A','a')", 1)
				require.NoError(t, err)
				defer stmt.Free()
				var state *PreparedFieldTypes
				want := types.T_text
				if first == types.T_blob {
					want = types.T_blob
				} else if first == types.T_int64 && wrapper == "(select ? limit 1)" {
					want = types.T_int64
				}
				for _, oid := range []types.T{first, types.T_varchar, types.T_blob} {
					bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
					var value any = "a"
					if oid == types.T_any {
						value = nil
					} else if oid == types.T_int64 {
						value = int64(123)
					}
					values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}}
					ctx.SetContext(WithPreparedFieldTypes(context.Background(), state))
					bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
					require.NoError(t, err)
					require.NotNil(t, bound.FieldTypes, "the wrapper must retain its parameter dependency")
					require.Equal(t, want, bound.FieldTypes.cases[0].result.Oid)
					if state != nil {
						require.Same(t, state, bound.FieldTypes, "string source changes must preserve resolved history")
					}
					state = bound.FieldTypes
					require.Equal(t, oid, bindings[1].Type.Oid, "return types must not rewrite marker sources")
				}
			})
		}
	}
}

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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldScalarComparisonLifetime(t *testing.T) {
	for _, tc := range []struct {
		name, sql                     string
		condition, derived, nullFirst bool
		key                           int32
	}{
		{"scalar_marker", "select field(case when ? then null else (select ?) end,?)", true, false, true, 1},
		{"scalar_parens", "select field(case when ? then null else (select (?)) end,?)", true, false, true, 1},
		{"scalar_union", "select field(case when ? then null else (select ? union all select null limit 1) end,?)", true, false, true, 1},
		{"scalar_char", "select field((select case when ? then cast(null as char) else ? end),?)", true, false, true, 0},
		{"computed_case", "select field(case when ?+0 then null else ? end,?)", true, false, false, 1},
		{"computed_case_parens", "select field((case when ?+0 then null else ? end),?)", true, false, false, 1},
		{"computed_scalar", "select field((select case when ?+0 then null else ? end),?)", true, false, false, 1},
		{"computed_coalesce", "select field(coalesce(case when ?+0 then null else ? end,null),?)", true, false, false, 1},
		{"computed_nullif", "select field(nullif(case when ?+0 then null else ? end,null),?)", true, false, false, 1},
		{"computed_derived", "select field(v,?) from (select case when ?+0 then null else ? end v)d", true, true, false, 1},
		{"nullif_peer_control", "select field(nullif(?,cast('B' as binary)),?)", false, false, false, 0},
		{"binary_literal", "select field(coalesce(?,_binary 'A'),?)", false, false, false, -1},
		{"hex_literal", "select field(coalesce(?,X'41'),?)", false, false, false, -1},
		{"hexnum_literal", "select field(coalesce(?,0x41),?)", false, false, false, -1},
		{"bit_literal", "select field(coalesce(?,b'01000001'),?)", false, false, false, -1},
		{"least_binary", "select field(least(?,cast('A' as binary)),?)", false, false, false, -1},
		{"greatest_binary", "select field(greatest(?,cast('A' as binary)),?)", false, false, false, -1},
		{"nullif_wrapper", "select field(nullif(coalesce(?,cast('A' as binary)),null),?)", false, false, false, -1},
		{"scalar_binary", "select field((select coalesce(?,cast('A' as binary))),?)", false, false, false, -1},
		{"derived_binary", "select field(v,?) from (select coalesce(?,cast('A' as binary)) v)d", false, true, false, -1},
		{"double_derived_binary", "select field(v,?) from (select v from (select coalesce(?,cast('A' as binary)) v)d0)d1", false, true, false, -1},
		{"double_derived_case", "select field(v,?) from (select v from (select case when ?+0 then null else ? end v)d0)d1", true, true, false, 1},
		{"double_derived_hex", "select field(v,?) from (select v from (select coalesce(?,X'41') v)d0)d1", false, true, false, -1},
		{"derived_case_control", "select field(v,?) from (select case when ? then null else ? end v from (select 1 union all select 2) seed limit 1)d", true, true, true, 1},
		{"window_case_control", "select distinct field(v,?) from (select max(case when ? then null else ? end) over () v from (select 1 union all select 2) seed)d", true, true, true, 1},
	} {
		for _, binary := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "SQL", true: "COM"}[binary], func(t *testing.T) {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
				require.NoError(t, err)
				t.Cleanup(stmt.Free)
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				var domains map[int32]types.Type
				for step := 0; step < 3; step++ {
					subject, candidate := 0, 1
					count := 2
					if tc.condition && tc.derived {
						subject, candidate, count = 2, 0, 3
					} else if tc.condition {
						subject, candidate, count = 1, 2, 3
					} else if tc.derived {
						subject, candidate = 1, 0
					}
					bindings := make([]PreparedSourceBinding, count)
					values := make([]any, count)
					for i := range bindings {
						bindings[i] = PreparedSourceBinding{Position: int32(i), Type: types.T_int64.ToType()}
						values[i] = ParamValue{Value: int64(0), SourceType: bindings[i].Type, HasSourceType: true, IsBinaryProtocol: binary}
					}
					var value any = int64(42)
					source := types.T_int64.ToType()
					var match any = int64(42)
					if step == 0 && tc.name == "least_binary" {
						value, match = int64(0), int64(0)
					}
					matchType := source
					if step == 0 && tc.nullFirst {
						value = nil
						source = types.T_any.ToType()
						match = "a"
						matchType = types.T_varchar.ToType()
					}
					if step > 0 {
						value = "A"
						source = types.T_varchar.ToType()
						match = "a"
						matchType = source
					}
					if step == 2 {
						value = "A"
						source = types.T_binary.ToType()
						if binary {
							// Connector/J setBytes is wire VAR_STRING (253), not BLOB.
							source = types.T_varchar.ToType()
						}
						source.Width = 1
						if !binary {
							source.Width = -1
						}
					}
					bindings[subject].Type = source
					bindings[candidate].Type = matchType
					values[subject] = ParamValue{Value: value, SourceType: source, HasSourceType: true, IsBinaryProtocol: binary, IsBinaryString: step == 2 && !binary, EnableNumericPrefix: true}
					if step == 2 && !binary {
						parameter := values[subject].(ParamValue)
						parameter.RuntimeStringDomain = types.RuntimeStringBinary
						values[subject] = parameter
					}
					values[candidate] = ParamValue{Value: match, SourceType: matchType, HasSourceType: true, IsBinaryProtocol: binary}
					mock.ctxt.SetContext(WithPreparedFieldConsumers(mock.ctxt.GetContext(), domains, binary))
					bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
					require.NoError(t, err, "step %d", step)
					domains = bound.FieldDomains
					filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), bound.Plan, values)
					require.NoError(t, err, "fill step %d", step)
					require.NotNil(t, filled)
					if !tc.derived && tc.name != "scalar_union" {
						actual := func() uint64 {
							proc := mock.ctxt.GetProcess()
							parameters := vector.NewVec(types.T_text.ToType())
							defer parameters.Free(proc.Mp())
							for _, wrapped := range values {
								value := wrapped.(ParamValue).Value
								require.NoError(t, vector.AppendBytes(parameters, []byte(fmt.Sprint(value)), value == nil, proc.Mp()))
							}
							proc.SetPrepareParams(parameters)
							defer proc.SetPrepareParams(nil)
							query := bound.Plan.GetQuery()
							expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
							executor, err := colexec.NewExpressionExecutor(mock.ctxt.GetProcess(), expr)
							require.NoError(t, err)
							defer executor.Free()
							out, err := executor.Eval(mock.ctxt.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
							require.NoError(t, err)
							return vector.GetFixedAtWithTypeCheck[uint64](out, 0)
						}()
						want := uint64(1)
						if step == 0 && (tc.nullFirst || tc.name == "greatest_binary") {
							want = 0
						}
						if tc.name == "nullif_peer_control" && step > 0 {
							want = 0
						}
						require.Equal(t, want, actual, "step %d", step)
					}
					if tc.name == "nullif_peer_control" {
						require.NotContains(t, domains, int32(-1), "a binary equality peer does not own NULLIF's return domain")
						continue
					}
					if tc.key == 0 {
						field := findPlanFunctionExpr(bound.Plan, "field")
						require.NotNil(t, field)
						require.Equal(t, types.StringDomainText, types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])), "typed CHAR NULL keeps its text comparison through scalar materialization; step %d", step)
						continue
					}
					require.Contains(t, domains, tc.key, "step %d", step)
					if tc.nullFirst {
						want := types.StringDomainText
						if tc.derived {
							want = types.StringDomainBinary
						}
						if tc.derived {
							require.NotContains(t, domains, int32(-1), "private numeric NULL probes do not resolve a numeric value lifetime")
						}
						require.Equal(t, want, types.StaticStringDomain(domains[tc.key]))
					} else {
						require.True(t, domains[tc.key].IsNumeric())
					}
				}
			})
		}
	}
}

func TestPreparedFieldScalarMarkerWitnessBoundaries(t *testing.T) {
	for _, operation := range []tree.UnionType{tree.EXCEPT, tree.INTERSECT, tree.UT_MINUS} {
		require.Nil(t, preparedFieldScalarResultsAST(&tree.UnionClause{Type: operation}), "set-operation peers must not become returned-value owners")
	}
	binding := PreparedSourceBinding{Position: 7, Type: types.T_any.ToType()}
	ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{binding})
	marker := tree.NewParamExpr(1)
	null := tree.NewNumVal("", "", false, tree.P_null)
	witness, err := preparedFieldScalarMarkerWitness(ctx, []tree.Expr{marker, null, marker, null}, makePlan2Type(&binding.Type))
	require.NoError(t, err)
	require.Len(t, witness.GetF().Args, 2, "duplicate branches retain one marker and one NULL, not a UNION graph")
	require.Equal(t, int32(7), witness.GetF().Args[0].GetP().Pos, "parser ordinals are not execution slots")
	require.True(t, witness.GetF().Args[1].GetLit().Isnull)
	for _, branches := range [][]tree.Expr{
		{null, null},
		{marker, tree.NewNumVal("A", "A", false, tree.P_char)},
		{marker, tree.NewCastExpr(marker, tree.TYPE_BOOL)},
	} {
		witness, err = preparedFieldScalarMarkerWitness(ctx, branches, makePlan2Type(&binding.Type))
		require.NoError(t, err)
		require.Nil(t, witness, "all-NULL, fixed values and authored CASTs do not declare a raw projected marker")
	}
	witness, err = preparedFieldScalarMarkerWitness(ctx, []tree.Expr{tree.NewParamExpr(2), null}, makePlan2Type(&binding.Type))
	require.Error(t, err)
	require.Nil(t, witness)
	require.Equal(t, binding, preparedSourceBindings(ctx)[0], "failed witness binding leaves its source snapshot immutable")
}

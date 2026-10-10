// Copyright 2021 - 2026 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldCaseNullFirstAdmission(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select field(case when ? then null else ? end,?)", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: types.T_any.ToType()}, {Position: 2, Type: types.T_varchar.ToType()}}
	values := []any{ParamValue{Value: int64(0), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: nil, SourceType: bindings[1].Type, HasSourceType: true}, ParamValue{Value: "a", SourceType: bindings[2].Type, HasSourceType: true}}
	bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
	require.NoError(t, err)
	for _, plainNull := range []bool{false, true} {
		candidate := append([]any(nil), values...)
		if plainNull {
			candidate[1] = nil
		}
		for _, bind := range []func(context.Context, *Plan, map[int32]types.Type, []PreparedSourceBinding, []any) (map[int32]types.Type, bool, error){BindPreparedFieldCaseDomains, BindPreparedFieldNullFirstCaseDomains} {
			domains, dependent, err := bind(ctx, DeepCopyPlan(bound.Plan), nil, bindings, candidate)
			require.NoError(t, err)
			require.False(t, dependent)
			require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(domains[1]))
			domains, _, err = bind(ctx, DeepCopyPlan(bound.Plan), nil, bindings, nil)
			require.NoError(t, err)
			require.Empty(t, domains, "missing values are not evidence of a NULL execution")
		}
	}
	for _, typ := range []types.T{types.T_varchar, types.T_int64} {
		bindings[1].Type = typ.ToType()
		values[1] = ParamValue{Value: nil, SourceType: bindings[1].Type, HasSourceType: true}
		bound, err = BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
		require.NoError(t, err)
		domains, _, err := BindPreparedFieldNullFirstCaseDomains(ctx, bound.Plan, nil, bindings, values)
		require.NoError(t, err)
		require.Equal(t, types.T_any, domains[1].Oid, "typed NULL records native resolution, not a consumer override")
		missing, _, err := BindPreparedFieldNullFirstCaseDomains(ctx, DeepCopyPlan(bound.Plan), nil, bindings, nil)
		require.NoError(t, err)
		require.Empty(t, missing, "native resolution also requires an actual execution value")
		published := domains[1]
		for _, source := range []types.T{types.T_any, types.T_varchar} {
			bindings[1].Type = source.ToType()
			var value any = "A"
			if source == types.T_any {
				value = nil
			}
			values[1] = ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}
			bound, err = BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			next, _, err := BindPreparedFieldNullFirstCaseDomains(ctx, bound.Plan, domains, bindings, values)
			require.NoError(t, err)
			require.Equal(t, published, domains[1], "reuse must not mutate the published resolution fact")
			require.Equal(t, published, next[1], "a later NULL must not become a first-NULL override")
			if source == types.T_varchar {
				consumer := findPlanFunctionExpr(bound.Plan, "field")
				require.NotNil(t, consumer)
				require.Equal(t, types.StringDomainText, types.StaticStringDomain(makeTypeByPlan2Expr(consumer.GetF().Args[0])))
			}
			domains = next
		}
	}
}

func TestPreparedFieldCaseDecimalExactPrefix(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select field(case when ? then null else ? end,?)", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	decimal := types.New(types.T_decimal128, 20, 0)
	for _, source := range []types.T{types.T_varchar, types.T_int64} {
		t.Run(source.String(), func(t *testing.T) {
			bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: source.ToType()}, {Position: 2, Type: decimal}}
			var subject any = "9007199254740993"
			if source == types.T_int64 {
				subject = int64(9007199254740993)
			}
			values := []any{
				ParamValue{Value: int64(0), SourceType: types.T_int64.ToType(), HasSourceType: true},
				ParamValue{Value: subject, SourceType: source.ToType(), HasSourceType: true},
				ParamValue{Value: "9007199254740992", SourceType: decimal, HasSourceType: true, EnableNumericPrefix: true},
			}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			published := map[int32]types.Type{1: types.New(types.T_decimal64, 8, 2)}
			if source == types.T_varchar {
				_, _, admissionErr := BindPreparedFieldCaseDomains(ctx, DeepCopyPlan(bound.Plan), published, bindings, nil)
				require.Error(t, admissionErr)
				require.Equal(t, types.New(types.T_decimal64, 8, 2), published[1])
			}
			_, dependent, err := BindPreparedFieldCaseDomains(ctx, bound.Plan, published, bindings, values)
			require.NoError(t, err)
			require.Equal(t, source == types.T_varchar, dependent)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, bound.Plan, values)
			require.NoError(t, err)
			consumer := findPlanFunctionExpr(filled, "field")
			proc := testutil.NewProcess(t)
			executor, err := colexec.NewExpressionExecutor(proc, consumer)
			require.NoError(t, err)
			defer executor.Free()
			out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, int64(0), vector.GetFixedAtWithTypeCheck[int64](out, 0), consumer.String())
		})
	}
}

func TestPreparedFieldCaseDomainAdmission(t *testing.T) {
	ctx := context.Background()
	for _, operand := range []string{
		"case when ? then null else ? end",
		"cast(case when ? then null else ? end as char)",
		"case when ? then cast(null as signed) else ? end",
	} {
		t.Run(operand, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select field("+operand+",?)", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			for _, source := range []types.T{types.T_varchar, types.T_any} {
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: source.ToType()}, {Position: 2, Type: types.T_varchar.ToType()}}
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, nil)
				require.NoError(t, err)
				candidate, _, err := BindPreparedFieldCaseDomains(ctx, bound.Plan, nil, bindings, nil)
				require.NoError(t, err)
				eligible := operand == "case when ? then null else ? end" && source == types.T_varchar
				if eligible {
					require.Len(t, candidate, 1)
				} else {
					require.Empty(t, candidate)
				}
			}
		})
	}
	// An invalid existing target fails on the new candidate plan without
	// modifying the previously published snapshot.
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select field(case when ? then null else ? end,?)", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: types.T_varchar.ToType()}, {Position: 2, Type: types.T_varchar.ToType()}}
	bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, nil)
	require.NoError(t, err)
	published := map[int32]types.Type{1: {Oid: types.T_tuple}}
	_, _, err = BindPreparedFieldCaseDomains(ctx, bound.Plan, published, bindings, nil)
	require.Error(t, err)
	require.Equal(t, map[int32]types.Type{1: {Oid: types.T_tuple}}, published)

	// Non-FIELD and absent-query owners do not allocate a snapshot.
	for _, p := range []*Plan{nil, {Plan: &planpb.Plan_Query{Query: &planpb.Query{}}}} {
		next, _, err := BindPreparedFieldCaseDomains(ctx, p, nil, nil, nil)
		require.NoError(t, err)
		require.Nil(t, next)
	}
}

func TestPreparedFieldResolvedCaseDomainReuse(t *testing.T) {
	for _, operand := range []string{"case when ? then null else ? end", "coalesce(case when ? then null else ? end,null)"} {
		for _, initial := range []types.T{types.T_varchar, types.T_varbinary} {
			t.Run(operand+"/"+initial.String(), func(t *testing.T) {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select field("+operand+",?)", 1)
				require.NoError(t, err)
				t.Cleanup(stmt.Free)
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				valuesFor := func(source types.T, condition int64) []any {
					return []any{
						ParamValue{Value: condition, SourceType: types.T_int64.ToType(), HasSourceType: true},
						ParamValue{Value: "A", SourceType: source.ToType(), HasSourceType: true},
						ParamValue{Value: "a", SourceType: source.ToType(), HasSourceType: true},
					}
				}
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{
					{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: initial.ToType()}, {Position: 2, Type: initial.ToType()},
				}, valuesFor(initial, 0))
				require.NoError(t, err)
				template := bound.Plan
				before := template.String()
				var domains map[int32]types.Type
				opposite := types.T_varbinary
				if initial == types.T_varbinary {
					opposite = types.T_varchar
				}
				for _, source := range []types.T{initial, opposite, initial} {
					for _, condition := range []int64{0, 1, 0} {
						func() {
							// Frontend cache miss: a fresh source-bound plan, but the
							// same SQL handle's immutable first-domain snapshot.
							fresh, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{
								{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: source.ToType()}, {Position: 2, Type: source.ToType()},
							}, valuesFor(source, condition))
							require.NoError(t, err)
							prior := make(map[int32]types.Type, len(domains))
							for pos, typ := range domains {
								prior[pos] = typ
							}
							candidate, _, err := BindPreparedFieldCaseDomains(context.Background(), fresh.Plan, domains, []PreparedSourceBinding{
								{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: source.ToType()}, {Position: 2, Type: source.ToType()},
							}, valuesFor(source, condition))
							require.NoError(t, err)
							if domains != nil {
								require.Equal(t, prior, domains)
							}
							require.Len(t, candidate, 1)
							domains = candidate
							filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), fresh.Plan, valuesFor(source, condition))
							require.NoError(t, err)
							consumer := findPlanFunctionExpr(filled, "field")
							require.NotNil(t, consumer)
							proc := testutil.NewProcess(t)
							executor, err := colexec.NewExpressionExecutor(proc, consumer)
							require.NoError(t, err)
							defer executor.Free()
							out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
							require.NoError(t, err)
							want := int64(1 - condition)
							if initial == types.T_varbinary {
								want = 0
							}
							require.Equal(t, want, vector.GetFixedAtWithTypeCheck[int64](out, 0), consumer.String())
						}()
						require.Equal(t, before, template.String())
					}
				}
			})
		}
	}
}

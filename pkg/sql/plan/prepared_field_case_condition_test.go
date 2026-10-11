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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/stretchr/testify/require"
)

func TestFoldedNullRetainsDeclaredCastBoundary(t *testing.T) {
	for _, target := range []types.Type{types.T_int64.ToType(), types.New(types.T_decimal128, 20, 4), types.New(types.T_varchar, 16, 0), types.New(types.T_varbinary, 16, 0)} {
		t.Run(target.Oid.String(), func(t *testing.T) {
			proc := newPlanTestProcess(t)
			for _, authored := range []bool{false, true} {
				for _, optimizer := range []bool{false, true} {
					input := makePlan2NullConstExprWithType()
					var expression *Expr
					var err error
					if authored {
						expression, err = appendSyntaxExplicitCastBeforeExpr(proc.Ctx, input, makePlan2Type(&target))
					} else {
						expression, err = appendCastBeforeExpr(proc.Ctx, input, makePlan2Type(&target))
					}
					require.NoError(t, err)
					if optimizer {
						node := &planpb.Node{ProjectList: []*Expr{expression}}
						rule.NewConstantFold(false).Apply(node, nil, proc)
						expression = node.ProjectList[0]
					} else {
						expression, err = ConstantFold(batch.EmptyForConstFoldBatch, expression, proc, false, true)
						require.NoError(t, err)
					}
					require.True(t, expression.GetLit().GetIsnull())
					payload, err := proto.Marshal(expression)
					require.NoError(t, err)
					decoded := new(Expr)
					require.NoError(t, proto.Unmarshal(payload, decoded))
					collector := stringDomainWitnessCollector{seen: make(map[string]struct{})}
					_, untyped, other := preparedFieldBoundCaseValues(decoded, &collector)
					require.Equal(t, !authored, untyped, "authored=%v optimizer=%v", authored, optimizer)
					require.Equal(t, authored, other)
					text := types.T_text.ToType()
					text.Width = -1
					promoted, err := appendCastBeforeExpr(proc.Ctx, decoded, makePlan2Type(&text))
					require.NoError(t, err)
					promoted, err = ConstantFold(batch.EmptyForConstFoldBatch, promoted, proc, false, true)
					require.NoError(t, err)
					_, untyped, other = preparedFieldBoundCaseValues(promoted, &collector)
					require.Equal(t, !authored, untyped, "implicit promotion must not erase the declaration")
					require.Equal(t, authored, other)
				}
			}
		})
	}
}

func TestPreparedFieldCaseRelationalConditionIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, sql                     string
		condition, subject, candidate int
		unresolved                    bool
	}{
		{"scalar_condition", "select field(case when (select ?) then null else ? end,?)", 0, 1, 2, true},
		{"derived_condition", "select field(case when cond then null else ? end,?) from (select ? as cond) s", 2, 0, 1, true},
		{"scalar_condition_limit", "select field(case when (select ? from (select 1 union all select 2) seed limit 1) then null else ? end,?)", 0, 1, 2, true},
		{"derived_condition_limit", "select field(case when cond then null else ? end,?) from (select ? as cond from (select 1 union all select 2) seed limit 1) s", 2, 0, 1, true},
		{"derived_cast_condition", "select field(case when cond then null else ? end,?) from (select cast(? as signed) as cond from (select 1 union all select 2) seed limit 1) s", 2, 0, 1, false},
		{"derived_computed_condition", "select field(case when cond then null else ? end,?) from (select ?+0 as cond from (select 1 union all select 2) seed limit 1) s", 2, 0, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			bindings := make([]PreparedSourceBinding, 3)
			values := make([]any, 3)
			for position := range bindings {
				bindings[position] = PreparedSourceBinding{Position: int32(position), Type: types.T_varchar.ToType()}
			}
			bindings[tc.condition].Type = types.T_int64.ToType()
			values[tc.condition] = ParamValue{Value: int64(0), SourceType: bindings[tc.condition].Type, HasSourceType: true}
			values[tc.subject] = ParamValue{Value: "A", SourceType: bindings[tc.subject].Type, HasSourceType: true}
			values[tc.candidate] = ParamValue{Value: "a", SourceType: bindings[tc.candidate].Type, HasSourceType: true}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			domains, _, err := BindPreparedFieldCaseDomains(ctx, bound.Plan, nil, bindings, values)
			require.NoError(t, err)
			if tc.unresolved {
				require.Contains(t, domains, int32(1), "the first FIELD consumer owns resolution, not the returned marker ordinal")
				require.Equal(t, types.StringDomainText, types.StaticStringDomain(domains[1]))
			} else {
				require.Empty(t, domains, "a computed/declared predicate domain is not an unresolved marker")
			}
		})
	}
	t.Run("static_relational_condition", func(t *testing.T) {
		ctx := context.Background()
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select field(case when (select 0 from (select 1 union all select 2) seed limit 1) then null else ? end,?)", 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_varchar.ToType()}, {Position: 1, Type: types.T_varchar.ToType()}}
		values := []any{ParamValue{Value: "A", SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: "a", SourceType: bindings[1].Type, HasSourceType: true}}
		bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
		require.NoError(t, err)
		domains, _, err := BindPreparedFieldCaseDomains(ctx, bound.Plan, nil, bindings, values)
		require.NoError(t, err)
		require.Empty(t, domains, "relational shape alone is not a dynamic predicate")
	})
}

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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldCaseRelationalNullFirst(t *testing.T) {
	for _, tc := range []struct {
		name, sql                     string
		condition, subject, candidate int
	}{
		{"scalar", "select field((select case when ? then null else ? end),?)", 0, 1, 2},
		{"derived", "select field(v,?) from (select case when ? then null else ? end as v) s", 1, 2, 0},
		{"max", "select field(max(case when ? then null else ? end),?)", 0, 1, 2},
		{"window", "select field(max(case when ? then null else ? end) over (),?)", 0, 1, 2},
		{"scalar_limit", "select field((select case when ? then null else ? end from (select 1 union all select 2) seed limit 1),?)", 0, 1, 2},
		{"derived_limit", "select field(v,?) from (select case when ? then null else ? end as v from (select 1 union all select 2) seed limit 1) s", 1, 2, 0},
		{"max_rows", "select field(max(case when ? then null else ? end),?) from (select 1 union all select 2) seed", 0, 1, 2},
		{"window_derived", "select field(v,?) from (select max(case when ? then null else ? end) over () as v from (select 1 union all select 2) seed) s", 1, 2, 0},
		{"window_multiple", "select field(v,?) from (select max(case when ? then null else ? end) over () as v,max(cast('A' as binary)) over () as fixed,row_number() over (order by n) as rn from (select 1 n union all select 2) seed) s where rn=1 and fixed=cast('A' as binary)", 1, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			var domains map[int32]types.Type
			for _, source := range []types.T{types.T_any, types.T_varchar, types.T_varbinary} {
				bindings := make([]PreparedSourceBinding, 3)
				values := make([]any, 3)
				bindings[tc.condition] = PreparedSourceBinding{Position: int32(tc.condition), Type: types.T_int64.ToType()}
				bindings[tc.subject] = PreparedSourceBinding{Position: int32(tc.subject), Type: source.ToType()}
				bindings[tc.candidate] = PreparedSourceBinding{Position: int32(tc.candidate), Type: types.T_varchar.ToType()}
				var subject any = "A"
				if source == types.T_any {
					subject = nil
				}
				values[tc.condition] = ParamValue{Value: int64(0), SourceType: bindings[tc.condition].Type, HasSourceType: true}
				values[tc.subject] = ParamValue{Value: subject, SourceType: bindings[tc.subject].Type, HasSourceType: true}
				values[tc.candidate] = ParamValue{Value: "a", SourceType: bindings[tc.candidate].Type, HasSourceType: true}
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
				require.NoError(t, err)
				domains, _, err = BindPreparedFieldCaseDomains(ctx, bound.Plan, domains, bindings, values)
				require.NoError(t, err)
				require.Contains(t, domains, int32(tc.subject), "relational return must retain CASE resolution ownership")
				require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(domains[int32(tc.subject)]))
				filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, bound.Plan, values)
				require.NoError(t, err)
				if source != types.T_any {
					consumer := findPlanFunctionExpr(filled, "field")
					require.NotNil(t, consumer)
					require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(makeTypeByPlan2Expr(consumer.GetF().Args[0])), "consumer must receive the owned CASE domain: %s", consumer.String())
				}
			}
		})
	}
}

func TestPreparedFieldCaseContractWitnessBudget(t *testing.T) {
	// A lightweight binder fixture and at most fourteen parameters, not a
	// cluster or elapsed-time oracle. Nested predicates must not be retained.
	for _, depth := range []int{4, 8, 12} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			ctx := context.Background()
			expression := strings.Repeat("case when ? then null else ", depth) + "?" + strings.Repeat(" end", depth)
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select field(max("+expression+"),?)", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			bindings := make([]PreparedSourceBinding, depth+2)
			values := make([]any, len(bindings))
			for position := range bindings {
				typ, value := types.T_int64.ToType(), any(int64(0))
				if position == depth {
					typ, value = types.T_any.ToType(), nil
				}
				if position == depth+1 {
					typ, value = types.T_varchar.ToType(), "a"
				}
				bindings[position] = PreparedSourceBinding{Position: int32(position), Type: typ}
				values[position] = ParamValue{Value: value, SourceType: typ, HasSourceType: true}
			}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			consumer := findPlanFunctionExpr(bound.Plan, "field")
			require.NotNil(t, consumer)
			before := bound.Plan.String()
			witness := preparedFieldBoundCaseContractWitness(consumer.GetF().Args[0])
			require.NotNil(t, witness)
			nodes := 0
			markers := make(map[int32]struct{})
			require.NoError(t, planpb.VisitExprTree(witness, func(expr *Expr) error {
				nodes++
				if marker := expr.GetP(); marker != nil {
					markers[marker.Pos] = struct{}{}
				}
				return nil
			}))
			require.LessOrEqual(t, nodes, 8)
			require.Len(t, markers, 2)
			require.Contains(t, markers, int32(depth))
			require.Equal(t, before, bound.Plan.String(), "witness construction must not mutate its producer")
		})
	}
}

// Scalar/aggregate expressions require their relational producer at runtime.
// These typed checks are paired with actual public SQL execution in func_field.
func TestPreparedFieldNullifRelationalReturnDomain(t *testing.T) {
	for _, tc := range []struct {
		operand string
		binary  bool
	}{
		{"nullif((select ? limit 1),null)", false},
		{"nullif(max(coalesce(?,_binary'B')),null)", true},
		{"nullif(min(coalesce(?,_binary'B')),null)", true},
		{"nullif(any_value(coalesce(?,_binary'B')),null)", true},
		{"nullif(max(coalesce(?,_binary'B')) over (),null)", true},
		{"nullif(max(coalesce(?,cast('B' as binary(1)))),null)", true},
		{"nullif(max(coalesce(?,cast('B' as binary(1)))) over (),null)", true},
		{"nullif((select coalesce(?,_binary'B') limit 1),null)", true},
		{"nullif((select max(coalesce(?,_binary'B'))),null)", true},
		{"nullif((select v from (select coalesce(?,_binary'B') as v) s limit 1),null)", true},
		{"nullif((select v from (select max(coalesce(?,_binary'B')) over () as v) s limit 1),null)", true},
	} {
		t.Run(tc.operand, func(t *testing.T) {
			query := "select field(" + tc.operand + ",?)"
			prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "prepare p from '"+strings.ReplaceAll(query, "'", "''")+"'")
			require.NoError(t, err)
			template := prepared.GetDcl().GetPrepare().Plan
			before := template.String()
			for _, source := range []types.T{types.T_varchar, types.T_varbinary, types.T_varchar} {
				values := []any{ParamValue{Value: "A", SourceType: source.ToType(), HasSourceType: true}, ParamValue{Value: "a", SourceType: source.ToType(), HasSourceType: true}}
				filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), template, values)
				require.NoError(t, err)
				consumer := findPlanFunctionExpr(filled, "field")
				require.NotNil(t, consumer)
				want := types.StringDomainText
				if tc.binary {
					want = types.StringDomainBinary
				}
				require.Equal(t, want, types.StaticStringDomain(makeTypeByPlan2Expr(consumer.GetF().Args[0])), consumer.String())
				require.Equal(t, before, template.String())
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, query, 1)
				require.NoError(t, err)
				t.Cleanup(stmt.Free)
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{
					{Position: 0, Type: source.ToType()}, {Position: 1, Type: source.ToType()},
				}, values)
				require.NoError(t, err)
				boundPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), bound.Plan, values)
				require.NoError(t, err)
				boundConsumer := findPlanFunctionExpr(boundPlan, "field")
				require.NotNil(t, boundConsumer)
				require.Equal(t, want, types.StaticStringDomain(makeTypeByPlan2Expr(boundConsumer.GetF().Args[0])), boundConsumer.String())
			}
		})
	}
}

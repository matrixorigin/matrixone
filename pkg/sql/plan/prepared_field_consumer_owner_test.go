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
	"maps"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldNativeNullSelectorComparison(t *testing.T) {
	for _, condition := range []string{"cast(? as signed)", "?+0"} {
		t.Run(condition, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select field(case when "+condition+" then null else ? end,?)", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()},
				{Position: 1, Type: types.T_varchar.ToType()}, {Position: 2, Type: types.T_varchar.ToType()}}
			values := []any{ParamValue{Value: int64(0), SourceType: bindings[0].Type, HasSourceType: true, IsBinaryProtocol: true},
				ParamValue{Value: "A", SourceType: bindings[1].Type, HasSourceType: true, IsBinaryProtocol: true},
				ParamValue{Value: "a", SourceType: bindings[2].Type, HasSourceType: true, IsBinaryProtocol: true}}
			mock.ctxt.SetContext(WithPreparedFieldConsumers(mock.ctxt.GetContext(), nil, true))
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			require.Empty(t, bound.FieldDomains, "a declared/computed predicate never admits a CASE handle domain")
			field := findPlanFunctionExpr(bound.Plan, "field")
			require.NotNil(t, field)
			require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])))
		})
	}
}

func TestPreparedFieldASTConsumersOwnSharedProjection(t *testing.T) {
	sql := "select field(case when ? then null else s end,?),field(case when ? then null else coalesce(s,x) end,?) from (select ? s,? x limit 1)d"
	for _, binary := range []bool{false, true} {
		t.Run(map[bool]string{false: "SQL", true: "COM"}[binary], func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			var domains map[int32]types.Type
			for _, subject := range []any{nil, "A", "C"} {
				source := types.T_varchar.ToType()
				if subject == nil {
					source = types.T_any.ToType()
				}
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: types.T_varchar.ToType()},
					{Position: 2, Type: types.T_int64.ToType()}, {Position: 3, Type: types.T_varchar.ToType()},
					{Position: 4, Type: source}, {Position: 5, Type: types.T_varchar.ToType()}}
				values := []any{ParamValue{Value: int64(0), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: "a", SourceType: bindings[1].Type, HasSourceType: true},
					ParamValue{Value: int64(0), SourceType: bindings[2].Type, HasSourceType: true}, ParamValue{Value: "a", SourceType: bindings[3].Type, HasSourceType: true},
					ParamValue{Value: subject, SourceType: source, HasSourceType: true}, ParamValue{Value: "A", SourceType: types.T_varchar.ToType(), HasSourceType: true}}
				before := maps.Clone(domains)
				previous := mock.ctxt.GetContext()
				mock.ctxt.SetContext(WithPreparedFieldConsumers(previous, domains, binary))
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
				mock.ctxt.SetContext(previous)
				require.NoError(t, err)
				require.Equal(t, before, domains, "binding never mutates the published handle snapshot")
				domains = bound.FieldDomains
				require.Len(t, domains, 2, "consumer ids do not collapse to source marker 4; subject=%v binary=%v", subject, binary)
				for _, id := range []int32{1, 2} {
					require.Contains(t, domains, id)
				}
				if !binary || subject == nil {
					require.Equal(t, types.StringDomainText, types.StaticStringDomain(domains[1]), "projected NULL retains SELECT's default declaration")
				}
			}
		})
	}
}

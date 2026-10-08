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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Scalar/aggregate expressions require their relational producer at runtime.
// These typed checks are paired with actual public SQL execution in func_field.
func TestPreparedFieldNullifRelationalReturnDomain(t *testing.T) {
	for _, tc := range []struct {
		operand string
		binary  bool
	}{
		{"nullif((select ? limit 1),null)", false},
		{"nullif((select coalesce(?,_binary'B') limit 1),null)", true},
		{"nullif((select max(coalesce(?,_binary'B'))),null)", true},
		{"nullif((select v from (select coalesce(?,_binary'B') as v) s limit 1),null)", true},
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

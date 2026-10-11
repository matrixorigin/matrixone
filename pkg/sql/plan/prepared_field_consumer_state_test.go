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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldCaseConsumersDoNotShareParameterState(t *testing.T) {
	one := "field(case when c then null else x end,'a')"
	two := "field(case when d then null else x end,'A')"
	for _, projection := range []string{one, two, one + "," + two, two + "," + one} {
		t.Run(projection, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select "+projection+" from (select ? x,? c,? d) s", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_any.ToType()},
				{Position: 1, Type: types.T_int64.ToType()}, {Position: 2, Type: types.T_int64.ToType()}}
			values := []any{ParamValue{Value: nil, SourceType: types.T_any.ToType(), HasSourceType: true},
				ParamValue{Value: int64(0), SourceType: types.T_int64.ToType(), HasSourceType: true},
				ParamValue{Value: int64(0), SourceType: types.T_int64.ToType(), HasSourceType: true}}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			before := bound.Plan.String()
			domains, _, err := BindPreparedFieldCaseDomains(context.Background(), bound.Plan, nil, bindings, values)
			require.NoError(t, err)
			if projection == one || projection == two {
				require.Len(t, domains, 1, "prove each consumer is individually admitted before testing isolation")
				return
			}
			require.Len(t, domains, 2, "two consumers may share source ordinal 0, not a resolution record")
			for _, typ := range domains {
				require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(typ))
			}
			require.NotEqual(t, before, bound.Plan.String(), "only the newly owned execution plan is specialized")
		})
	}
}

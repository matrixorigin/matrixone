// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSetOperationDateTimeCommonType(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		kind      plan.Node_NodeType
		want      types.T
		scale     int32
	}{
		{"date-time", "select cast('2024-01-01' as date) union all select cast('10:30:00' as time)", plan.Node_UNION_ALL, types.T_datetime, 0},
		{"time-date", "select cast('10:30:00' as time) union all select cast('2024-01-01' as date)", plan.Node_UNION_ALL, types.T_datetime, 0},
		{"fractional-time", "select cast('2024-01-01' as date) union all select cast('10:30:00.123456' as time(6))", plan.Node_UNION_ALL, types.T_datetime, 6},
		{"null-branch", "select cast('2024-01-01' as date) union all select null union all select cast('10:30:00' as time)", plan.Node_UNION_ALL, types.T_datetime, 0},
		{"distinct", "select cast('10:30:00' as time) union select cast('2024-01-01' as date)", plan.Node_UNION, types.T_datetime, 0},
		{"intersect", "select cast('2024-01-01' as date) intersect select cast('10:30:00' as time)", plan.Node_INTERSECT, types.T_datetime, 0},
		{"except", "select cast('2024-01-01' as date) except select cast('10:30:00' as time)", plan.Node_MINUS, types.T_datetime, 0},
		{"time-control", "select cast('10:30:00.12' as time(2)) union all select cast('11:30:00' as time)", plan.Node_UNION_ALL, types.T_time, 2},
		{"date-control", "select cast('2024-01-01' as date) union all select cast('2025-01-01' as date)", plan.Node_UNION_ALL, types.T_date, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()
			var setNode *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType == tc.kind {
					setNode = node
				}
			}
			require.NotNil(t, setNode)
			require.Len(t, setNode.ProjectList, 1)
			require.Equal(t, int32(tc.want), setNode.ProjectList[0].Typ.Id)
			require.Equal(t, tc.scale, setNode.ProjectList[0].Typ.Scale)
			for _, child := range setNode.Children {
				require.Equal(t, int32(tc.want), query.Nodes[child].ProjectList[0].Typ.Id)
				require.Equal(t, tc.scale, query.Nodes[child].ProjectList[0].Typ.Scale)
			}
		})
	}
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

// Issue #31913 regression. A set-operation column that combines DATE and
// TIME must keep both domains: MySQL types it as DATETIME so the time-of-day
// survives, and coalesce-style scoring alone would pick DATE and silently
// discard it. The tests go through the real planner from public SQL and
// assert the column type materialized on the set-operation's result node,
// including the widest fractional-second precision. Combinations without the
// DATE+TIME mixture keep their existing resolution and act as controls.
func TestSetOperationDateAndTimeCommonType(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		wantType  types.T
		wantScale int32
	}{
		// A DATE+TIME mixture resolves to DATETIME in both branch orders.
		{"date-time", "select cast('2024-01-01' as date) union all select cast('10:30:00' as time)", types.T_datetime, 0},
		{"time-date", "select cast('10:30:00' as time) union all select cast('2024-01-01' as date)", types.T_datetime, 0},
		{"date-time-distinct", "select cast('10:30:00' as time) union select cast('2024-01-01' as date)", types.T_datetime, 0},
		// INTERSECT and EXCEPT share the same materialized column contract.
		{"intersect-date-time", "select cast('2024-01-01' as date) intersect select cast('10:30:00' as time)", types.T_datetime, 0},
		{"except-date-time", "select cast('2024-01-01' as date) except select cast('10:30:00' as time)", types.T_datetime, 0},
		// The widest fractional-second precision wins.
		{"date-time-fsp", "select cast('2024-01-01' as date) union all select cast('10:30:00.123456' as time(6))", types.T_datetime, 6},
		{"date-time-fsp3", "select cast('2024-01-01' as date) union all select cast('10:30:00.123' as time(3))", types.T_datetime, 3},
		// A pure-NULL branch contributes no type of its own.
		{"date-time-null-branch", "select cast('2024-01-01' as date) union all select null union all select cast('10:30:00' as time)", types.T_datetime, 0},
		// A datetime branch bears both domains already; the mixture keeps DATETIME.
		{"-date-time-datetime", "select cast('2024-01-01' as date) union all select cast('10:30:00' as time) union all select cast('2024-01-02 10:11:12' as datetime)", types.T_datetime, 0},
		{"date-time-datetime-fsp", "select cast('2024-01-01' as date) union all select cast('10:30:00.123' as time(3)) union all select cast('2024-01-02 10:11:12.123' as datetime(3))", types.T_datetime, 3},
		// A real TIMESTAMP column joins the mixture without flattening the
		// FSP: the widest fractional-second precision still comes from
		// TIME(6) (CAST AS TIMESTAMP is unsupported; mo_catalog.mo_pitr's
		// create_time is a mock-bound TIMESTAMP column).
		{"date-time-timestamp-column", "select cast('2024-01-01' as date) as x union all select cast('10:30:00.123456' as time(6)) as x union all select create_time from mo_catalog.mo_pitr as x", types.T_datetime, 6},
		// Other temporal combinations keep their existing resolution: controls.
		{"date-date-control", "select cast('2024-01-01' as date) union all select cast('2024-01-02' as date)", types.T_date, 0}, {"time-time-control", "select cast('10:30:00.12' as time(2)) union all select cast('11:30:00' as time)", types.T_time, 2},
		{"date-datetime-control", "select cast('2024-01-01' as date) union all select cast('2024-01-02 10:11:12' as datetime)", types.T_datetime, 0},
		{"time-datetime-control", "select cast('10:30:00' as time) union all select cast('2024-01-02 10:11:12' as datetime)", types.T_datetime, 0},
		{"null-date-control", "select null union all select cast('2024-01-01' as date)", types.T_date, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()
			node := lastSetOperationNode(query)
			require.NotNil(t, node, "the statement must lower to a set-operation node")
			require.NotEmpty(t, node.ProjectList)
			require.Equal(t, int32(tc.wantType), node.ProjectList[0].Typ.Id)
			require.Equal(t, tc.wantScale, node.ProjectList[0].Typ.Scale)
		})
	}
}

// lastSetOperationNode returns the latest set-operation node of the query.
// The outermost materialized set operation is appended last, so it carries
// the user-visible column type of the statement.
func lastSetOperationNode(query *plan.Query) *plan.Node {
	var found *plan.Node
	for i := range query.Nodes {
		switch query.Nodes[i].NodeType {
		case plan.Node_UNION, plan.Node_UNION_ALL, plan.Node_INTERSECT, plan.Node_MINUS:
			found = query.Nodes[i]
		}
	}
	return found
}

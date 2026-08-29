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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestLeftJoinNullFilterRewritesToAnti(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicalPlan, err := runOneStmt(mock, t, `
		select n.n_nationkey
		from nation n
		left join region r on n.n_regionkey = r.r_regionkey
		where r.r_regionkey is null`)
	require.NoError(t, err)

	query := logicalPlan.GetQuery()
	require.True(t, reachablePlanHasJoinType(query, planpb.Node_ANTI))
	require.False(t, reachablePlanHasJoinType(query, planpb.Node_LEFT))
}

func TestLeftJoinNullFilterAntiRewriteFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "nullable right marker",
			sql: `select n.n_nationkey
				from nation n
				left join region r on n.n_regionkey = r.r_regionkey
				where r.r_comment is null`,
		},
		{
			name: "right payload remains observable",
			sql: `select r.r_name
				from nation n
				left join region r on n.n_regionkey = r.r_regionkey
				where r.r_regionkey is null`,
		},
		{
			name: "another right predicate remains observable",
			sql: `select n.n_nationkey
				from nation n
				left join region r on n.n_regionkey = r.r_regionkey
				where r.r_regionkey is null and r.r_comment is null`,
		},
		{
			name: "null marker under or",
			sql: `select n.n_nationkey
				from nation n
				left join region r on n.n_regionkey = r.r_regionkey
				where r.r_regionkey is null or n.n_nationkey = 1`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			logicalPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			query := logicalPlan.GetQuery()
			require.True(t, reachablePlanHasJoinType(query, planpb.Node_LEFT))
			require.False(t, reachablePlanHasJoinType(query, planpb.Node_ANTI))
		})
	}
}

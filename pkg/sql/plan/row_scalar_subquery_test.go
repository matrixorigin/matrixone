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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRowConstructorScalarSubqueryComparisonBuilds(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "equal", sql: "select (1, 'AFRICA') = (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "not equal", sql: "select (1, 'AFRICA') <> (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "less", sql: "select (1, 'AFRICA') < (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "less equal", sql: "select (1, 'AFRICA') <= (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "greater", sql: "select (1, 'AFRICA') > (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "greater equal", sql: "select (1, 'AFRICA') >= (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "null safe equal", sql: "select (1, 'AFRICA') <=> (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0)"},
		{name: "subquery on left", sql: "select (select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = 0) < (1, 'AFRICA')"},
		{name: "constant subquery", sql: "select (1, 'AFRICA') = (select 1, 'AFRICA')"},
		{name: "correlated", sql: `select N_NATIONKEY from NATION
			where (N_REGIONKEY, N_NAME) =
				(select R_REGIONKEY, R_NAME from REGION where R_REGIONKEY = N_REGIONKEY)`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			require.NotNil(t, logicPlan.GetQuery())
			require.False(t, queryContainsSubqueryRef(logicPlan.GetQuery()))
		})
	}
}

func TestRowConstructorScalarSubqueryComparisonRejectsArityMismatch(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(false), t,
		"select (1, 2) = (select R_REGIONKEY, R_NAME, R_COMMENT from REGION where R_REGIONKEY = 0)")
	require.ErrorContains(t, err, "subquery should return 2 columns")
}

func queryContainsSubqueryRef(query *planpb.Query) bool {
	for _, node := range query.Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList,
			node.FilterList,
			node.OnList,
			node.GroupBy,
			node.AggList,
		} {
			for _, expr := range exprs {
				if hasSubquery(expr) {
					return true
				}
			}
		}
	}
	return false
}

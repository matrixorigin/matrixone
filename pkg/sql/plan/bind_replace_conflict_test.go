// Copyright 2021 Matrix Origin
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

func TestReplaceConflictLookupUsesEquiJoins(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantUnion bool
	}{
		{
			name:      "primary and unique keys",
			sql:       "REPLACE INTO dept VALUES (1, 'Sales', 'NY')",
			wantUnion: true,
		},
		{
			name:      "primary and composite unique keys",
			sql:       "REPLACE INTO dept_composite_uk VALUES (1, 'Sales', 'NY')",
			wantUnion: true,
		},
		{
			name:      "primary key only",
			sql:       "REPLACE INTO self_ref VALUES (1, NULL, 'root')",
			wantUnion: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, test.sql)
			require.NoError(t, err)

			requireReplaceConflictLookupPlan(t, logicPlan.GetQuery(), test.wantUnion)
		})
	}
}

func requireReplaceConflictLookupPlan(t *testing.T, query *planpb.Query, wantBranchMerge bool) {
	t.Helper()
	require.NotNil(t, query)
	hasConflictUnion := false
	hasSourceOrdinal := false
	hasOrdinalSort := false
	joinCount := 0
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_UNION {
			hasConflictUnion = true
		}
		require.NotEqual(t, planpb.Node_UNION_ALL, node.NodeType,
			"conflict candidates must be deduplicated across lookup branches")
		if node.NodeType == planpb.Node_WINDOW {
			for _, spec := range node.WinSpecList {
				if spec.GetW().GetName() == "row_number" {
					hasSourceOrdinal = true
				}
			}
		}
		if node.NodeType == planpb.Node_SORT {
			hasOrdinalSort = true
		}
		if node.NodeType != planpb.Node_JOIN {
			continue
		}
		joinCount++
		for _, condition := range node.OnList {
			require.False(t, expressionContainsFunction(condition, "or"),
				"REPLACE conflict lookup must not contain an OR join predicate")
		}
	}
	require.Equal(t, wantBranchMerge, hasConflictUnion)
	require.Equal(t, wantBranchMerge, hasSourceOrdinal)
	require.Equal(t, wantBranchMerge, hasOrdinalSort)
	require.Positive(t, joinCount)
}

func expressionContainsFunction(expr *planpb.Expr, name string) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.Func != nil && fn.Func.ObjName == name {
		return true
	}
	for _, arg := range fn.Args {
		if expressionContainsFunction(arg, name) {
			return true
		}
	}
	return false
}

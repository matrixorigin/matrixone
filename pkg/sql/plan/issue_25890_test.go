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

	"github.com/stretchr/testify/require"
)

func TestIssue25890DistinctWithOrderByUsesFullTextIndex(t *testing.T) {
	logicPlan, err := runOneStmt(newIssue24822Optimizer(), t, `
		SELECT DISTINCT base_id
		FROM ft
		WHERE MATCH(title, body) AGAINST('+database' IN BOOLEAN MODE)
		ORDER BY base_id`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.True(t, hasReachableSortAboveAggregate(query))
	require.Equal(t, 1, countReachableFullTextScans(query))
	require.Zero(t, countReachableFullTextMatches(query))
}

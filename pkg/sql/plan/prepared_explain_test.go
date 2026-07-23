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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareExplainSelectBuildsUnderlyingQuery(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name       string
		sql        string
		paramCount int
	}{
		{
			name: "literal",
			sql:  "explain select 1",
		},
		{
			name:       "parameterized",
			sql:        "explain select * from nation where n_nationkey >= ?",
			paramCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, fmt.Sprintf("prepare pe from '%s'", test.sql))
			require.NoError(t, err)

			prepare := logicPlan.GetDcl().GetPrepare()
			require.NotNil(t, prepare)
			require.NotNil(t, prepare.GetPlan().GetQuery())
			require.Len(t, prepare.GetParamTypes(), test.paramCount)
		})
	}
}

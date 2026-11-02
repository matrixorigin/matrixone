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

	"github.com/stretchr/testify/require"
)

// A constant BETWEEN whose final fold fails (e.g. a non-integer string
// against integer bounds) must surface the fold error instead of binding a
// nil expression that later panics in projectExprKey or fails in cast.
func TestBetweenConstantFoldErrorIsReturned(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, sql := range []string{
		"select '1.5' between 1 and 20",
		"select 'abc' between 1 and 20",
		"select 1 where '1.5' between 1 and 20",
		"select 1 from (select 1) t where 'x' between 1 and 2",
	} {
		t.Run(sql, func(t *testing.T) {
			var p *Plan
			var err error
			require.NotPanics(t, func() { p, err = runOneStmt(mock, t, sql) }, sql)
			require.Error(t, err, sql)
			require.Nil(t, p)
			require.Contains(t, err.Error(), "cast to int", sql)
		})
	}

	// controls: foldable constant BETWEEN still folds and binds
	for _, sql := range []string{
		"select '12' between 1 and 20",
		"select 5 between 1 and 20",
		"select 1 where 30 between 1 and 20",
	} {
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
	}
}

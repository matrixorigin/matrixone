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

// `time +/- INTERVAL n unit` binds like DATE_ADD/DATE_SUB(time, ...).
func TestIntervalOperatorAcceptsTimeOperand(t *testing.T) {
	for _, tc := range []struct {
		sql string
		fn  string
	}{
		{"select cast('10:00:00' as time) + interval 1 hour", "date_add"},
		{"select interval 90 minute + cast('10:00:00' as time)", "date_add"},
		{"select cast('10:00:00.5' as time(1)) - interval 1 second", "date_sub"},
		{"select cast('10:00:00' as time) + interval '1:30' hour_minute", "date_add"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			require.NotNil(t, findPlanFunctionExpr(p, tc.fn), p.String())
		})
	}
}

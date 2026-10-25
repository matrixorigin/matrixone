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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// SHOW CREATE TABLE must print a string default so that replaying the
// output stores the same default (#31066).
func TestShowCreateTableStringDefaultRoundTrip(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	const sourceSQL = "create table t_default_quote (" +
		"a varchar(10) default 'it''s', " +
		"b varchar(10) default 'x`y', " +
		"c varchar(10) default \"q\"\"r\", " +
		"d varchar(10) default 'a\\\\b', " +
		"e varchar(10) default '')"

	sourceDef, err := buildTestCreateTableStmt(mock, sourceSQL)
	require.NoError(t, err)

	showSQL, _, err := ConstructCreateTableSQL(&mock.ctxt, sourceDef, nil, false, nil)
	require.NoError(t, err)
	for _, want := range []string{
		"`a` varchar(10) DEFAULT 'it''s'",
		"`b` varchar(10) DEFAULT 'x`y'",
		"`c` varchar(10) DEFAULT 'q\"r'",
		"`d` varchar(10) DEFAULT 'a\\\\b'",
		"`e` varchar(10) DEFAULT ''",
	} {
		require.True(t, strings.Contains(showSQL, want), "missing %q in\n%s", want, showSQL)
	}

	replayedDef, err := buildTestCreateTableStmt(mock, showSQL)
	require.NoError(t, err)
	for i, col := range sourceDef.Cols {
		require.Equal(t, col.Default.OriginString, replayedDef.Cols[i].Default.OriginString, col.Name)
	}
}

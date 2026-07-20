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

package process

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStmtProfileStatementRuntimeProfileCompatibility(t *testing.T) {
	sp := &StmtProfile{}

	sp.SetStatementRuntimeProfile("Insert", "DML", true)
	stmtType, queryType, ignore := sp.GetStatementRuntimeProfile()
	require.Equal(t, "Insert", stmtType)
	require.Equal(t, "DML", queryType)
	require.True(t, ignore)
	require.True(t, sp.GetStatementIgnore())

	stmtType, queryType, ignore = sp.GetDivByZeroRuntimeProfile()
	require.Equal(t, "Insert", stmtType)
	require.Equal(t, "DML", queryType)
	require.True(t, ignore)
	require.True(t, sp.GetDivByZeroIgnore())

	sp.SetDivByZeroRuntimeProfile("Update", "DML", false)
	stmtType, queryType, ignore = sp.GetStatementRuntimeProfile()
	require.Equal(t, "Update", stmtType)
	require.Equal(t, "DML", queryType)
	require.False(t, ignore)
	require.False(t, sp.GetStatementIgnore())
}

func TestIsStrictNoZeroDateMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode any
		want bool
	}{
		{name: "strict trans tables", mode: "STRICT_TRANS_TABLES,NO_ZERO_DATE", want: true},
		{name: "strict all tables lowercase", mode: "strict_all_tables,no_zero_date", want: true},
		{name: "traditional", mode: "TRADITIONAL", want: true},
		{name: "missing strict", mode: "NO_ZERO_DATE"},
		{name: "missing no zero date", mode: "STRICT_TRANS_TABLES"},
		{name: "non string", mode: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsStrictNoZeroDateMode(tc.mode))
		})
	}
}

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

package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLForRecordByStatementWithSQLMode(t *testing.T) {
	for _, tc := range []struct {
		name, sql, mode string
		want            []string
	}{
		{
			name: "blank fragments and credentials",
			sql:  "; /* cloud_user */ select 'a;b';; create user test_record identified by 'dummy'; /* tail */",
			want: []string{"select 'a;b'", "create user test_record identified by '******'"},
		},
		{
			name: "ansi quoted semicolon",
			sql:  `select ";"; select 2`,
			mode: "ANSI_QUOTES",
			want: []string{`select ";"`, "select 2"},
		},
		{
			name: "no backslash escapes",
			sql:  `select 'a\'; select 2`,
			mode: "NO_BACKSLASH_ESCAPES",
			want: []string{`select 'a\'`, "select 2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records, err := sqlForRecordByStatementWithSQLMode(context.Background(), tc.sql, tc.mode)
			require.NoError(t, err)
			require.Equal(t, tc.want, records)
		})
	}

	_, err := sqlForRecordByStatementWithSQLMode(context.Background(), "select ('unterminated'", "")
	require.Error(t, err)
}

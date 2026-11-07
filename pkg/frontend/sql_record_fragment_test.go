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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
		{
			name: "single comment",
			sql:  " /* benign */ ",
			want: []string{"/* benign */"},
		},
		{
			name: "only blank fragments",
			sql:  "; ; /* benign */",
			want: []string{""},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records, err := sqlForRecordByStatementWithSQLMode(context.Background(), tc.sql, tc.mode)
			require.NoError(t, err)
			require.Equal(t, tc.want, records)

			_, _, fragments, err := schedulingSQLByStatementWithSQLMode(context.Background(), tc.sql, tc.mode, false)
			require.NoError(t, err)
			input := &UserInput{sql: tc.sql}
			input.setRecordFragments(fragments, tc.mode)
			records, err = input.sqlForRecord(context.Background(), tc.mode, len(tc.want))
			require.NoError(t, err)
			require.Equal(t, tc.want, records)
			require.Nil(t, input.recordFragments)
		})
	}

	_, err := sqlForRecordByStatementWithSQLMode(context.Background(), "select ('unterminated'", "")
	require.Error(t, err)
}

func TestSQLForRecordRejectsStaleFragments(t *testing.T) {
	ctx := context.Background()
	_, _, oldFragments, err := schedulingSQLByStatementWithSQLMode(ctx, "select 1; select 2", "", false)
	require.NoError(t, err)

	t.Run("different SQL", func(t *testing.T) {
		input := &UserInput{sql: "select 1; select 2"}
		input.setRecordFragments(oldFragments, "")
		input.sql = "select 9"
		records, err := input.sqlForRecord(ctx, "", 1)
		require.NoError(t, err)
		require.Equal(t, []string{"select 9"}, records)
	})

	t.Run("different SQL mode", func(t *testing.T) {
		sql := `select 'a\'; select 2`
		_, _, fragments, err := schedulingSQLByStatementWithSQLMode(ctx, sql, "NO_BACKSLASH_ESCAPES", false)
		require.NoError(t, err)
		input := &UserInput{sql: sql}
		input.setRecordFragments(fragments, "NO_BACKSLASH_ESCAPES")
		_, err = input.sqlForRecord(ctx, "", 2)
		require.Error(t, err)
	})

	t.Run("wrong statement count", func(t *testing.T) {
		input := &UserInput{sql: "select 9"}
		input.setRecordFragments(oldFragments, "")
		records, err := input.sqlForRecord(ctx, "", 1)
		require.NoError(t, err)
		require.Equal(t, []string{"select 9"}, records)
	})

	t.Run("AST input", func(t *testing.T) {
		input := &UserInput{sql: "select 9", stmt: &tree.Select{}}
		input.setRecordFragments(oldFragments, "")
		records, err := input.sqlForRecord(ctx, "", 2)
		require.NoError(t, err)
		require.Equal(t, []string{"select 9"}, records)
	})

	t.Run("derived input", func(t *testing.T) {
		input := &UserInput{sql: "select 1; select 2"}
		input.setRecordFragments(oldFragments, "")
		derived := newSQLStatementInput(input, "select 9")
		records, err := derived.sqlForRecord(ctx, "", 1)
		require.NoError(t, err)
		require.Equal(t, []string{"select 9"}, records)
		require.NotNil(t, input.recordFragments)
	})
}

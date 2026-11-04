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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestHasSiriusHint(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"/*+ SIDECAR */ SELECT * FROM t", true},
		{"  /*+ SIDECAR */ SELECT * FROM t", true},
		{"/*+ sidecar */ SELECT * FROM t", true},
		{"/*+ SIDECAR GPU */ SELECT * FROM t", true},
		{"  /*+ sidecar gpu */ SELECT * FROM t", true},
		{"SELECT * FROM t", false},
		{"/*+ HASH_JOIN */ SELECT * FROM t", false},
		{"/*+ SIDECAR CPU */ SELECT * FROM t", false},
		{"SELECT '/*+ SIDECAR */'", false},
		{"", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			require.Equal(t, tc.want, hasSiriusHint(tc.sql))
		})
	}
}

func TestSiriusSelectorIsScopedToOneStatement(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		sql  string
		want []bool
	}{
		{name: "hinted select has unhinted sibling", sql: "/*+ SIDECAR */ SELECT 1; SELECT 2", want: []bool{true, false}},
		{name: "GPU hint has unhinted sibling", sql: "/*+ SIDECAR GPU */ SELECT 1; SELECT 2", want: []bool{true, false}},
		{name: "perform remains local", sql: "/*+ SIDECAR */ PERFORM SELECT 1; SELECT 2", want: []bool{false, false}},
		{name: "GPU hint on perform remains local", sql: "/*+ SIDECAR GPU */ PERFORM SELECT 1; SELECT 2", want: []bool{false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parsers.Parse(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer freeStatements(stmts)
			fragments, _, err := schedulingSQLByStatementWithSQLMode(ctx, tc.sql, "", false)
			require.NoError(t, err)
			require.Len(t, fragments, len(stmts))
			require.Len(t, stmts, len(tc.want))
			for i := range stmts {
				require.Equal(t, tc.want[i], siriusStatementSelected(fragments[i], stmts[i]))
			}
		})
	}
}

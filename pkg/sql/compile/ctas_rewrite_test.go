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

package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestPrependCTASRewriteHint(t *testing.T) {
	hint := `/*+ {"rewrites":{"db1.t1":"select * from db1.t1 where tenant_id = 1"}} */`
	generated := "insert into `db1`.`copy` select * from (`select * from db1.t1`) as __mo_ctas_source"
	rewritten := prependCTASRewriteHint("  "+hint+" create table db1.copy as select * from db1.t1", generated)
	require.Equal(t, hint+" "+generated, rewritten)
	spacedHint := `/*+    {"rewrites":{"db1.t1":"select * from db1.t1 where tenant_id = 1"}} */`
	require.Equal(t, spacedHint+" "+generated,
		prependCTASRewriteHint(spacedHint+" create table db1.copy as select * from db1.t1", generated))
	require.Equal(t, generated, prependCTASRewriteHint("create table db1.copy as select * from db1.t1", generated))
}

func TestLeadingRewriteHintValidation(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
		ok   bool
	}{
		{
			name: "bang hint",
			sql:  `/*!+ {"rewrites":{"db1.t1":"select 1"}} */ select 1`,
			want: `/*!+ {"rewrites":{"db1.t1":"select 1"}} */`,
			ok:   true,
		},
		{name: "non-json hint", sql: "/*+ select 1", ok: false},
		{name: "unterminated hint", sql: "/*+ {\"rewrites\":{}} select 1", ok: false},
		{name: "ordinary sql", sql: "select 1", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := leadingRewriteHint(tt.sql)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestInternalCTASRewriteHintAttachesToSource(t *testing.T) {
	sql := `/*+    {"rewrites":{"db1.t1":"select * from db1.t1 where tenant_id = 1"}} */ insert into db1.copy select * from db1.t1`
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.True(t, hasLeadingRewriteHint(sql))
	require.NoError(t, parsers.AddRewriteHintsWithSQLMode(context.Background(), stmts, sql, ""))
	insert, ok := stmts[0].(*tree.Insert)
	require.True(t, ok)
	require.NotNil(t, insert.Rows)
	require.NotNil(t, insert.Rows.RewriteOption)
	require.Contains(t, insert.Rows.RewriteOption.Rewrites, "db1.t1")
}

// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// applyRemapDbToSQL parses sql, runs applyRemapDb with the given remap, and
// returns the re-stringified statement.
func applyRemapDbToSQL(t *testing.T, sql string, remap map[string]string) string {
	ctx := context.Background()
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	applyRemapDb(stmts, remap)
	return tree.String(stmts[0], dialect.MYSQL)
}

func TestApplyRemapDb(t *testing.T) {
	remap := map[string]string{"dbxxx": "dbyyy"}

	t.Run("qualified ref", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from dbxxx.t", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("join both sides", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from dbxxx.a join dbxxx.b on a.id = b.id", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("from subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from (select * from dbxxx.t) x", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("union", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select id from dbxxx.a union select id from dbxxx.b", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("cte name is not remapped, body is", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "with c as (select * from dbxxx.t) select * from c", remap)
		require.Contains(t, out, "dbyyy.t") // body remapped
		require.Contains(t, out, "from c")  // CTE reference untouched
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("unqualified ref untouched", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from t", remap)
		require.Contains(t, out, "from t")
		require.NotContains(t, out, "dbyyy")
	})

	t.Run("non-mapped db untouched", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from other.t", remap)
		require.Contains(t, out, "other.t")
	})

	t.Run("insert target and select source", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "insert into dbxxx.t select * from dbxxx.u", remap)
		require.Contains(t, out, "dbyyy.t")
		require.Contains(t, out, "dbyyy.u")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("insert values", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "insert into dbxxx.t values (1)", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("update", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "update dbxxx.t set v = 1 where id = 2", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("delete", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "delete from dbxxx.t where id = 2", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})
}

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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28327FulltextScorePreservesBaseRows(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dbName := testutils.GetDatabaseName(t)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+quoteIssue28327Ident(dbName))
		}()

		execIssue28327(t, ctx, conn, `set ft_relevancy_algorithm="TF-IDF"`)
		execIssue28327(t, ctx, conn, "set experimental_fulltext_index = 1")
		execIssue28327(t, ctx, conn, "set experimental_fulltext2_index = 1")
		execIssue28327(t, ctx, conn, "drop database if exists "+quoteIssue28327Ident(dbName))
		execIssue28327(t, ctx, conn, "create database "+quoteIssue28327Ident(dbName))
		execIssue28327(t, ctx, conn, "use "+quoteIssue28327Ident(dbName))
		for _, algorithm := range []string{"fulltext", "fulltext2"} {
			t.Run(algorithm, func(t *testing.T) {
				execIssue28327(t, ctx, conn, "drop table if exists docs")
				execIssue28327(t, ctx, conn, "create table docs(id int primary key, body text)")
				execIssue28327(t, ctx, conn, "insert into docs values (1, 'alpha'), (2, 'beta'), (3, 'alpha beta')")
				execIssue28327(t, ctx, conn, "create "+algorithm+" index ft on docs(body)")

				got := queryIssue28327IDs(t, ctx, conn,
					"select id from docs order by match(body) against('alpha') desc, id")
				t.Logf("ORDER BY-only MATCH result: %v", got)
				require.Equal(t, []int{1, 3, 2}, got)
				require.Equal(t, []int{1, 2, 3}, queryIssue28327IDs(t, ctx, conn,
					"select id from docs order by match(body) against('missing') desc, id"))
				require.Equal(t, []int{3, 2}, queryIssue28327IDs(t, ctx, conn,
					"select id from docs where id > 1 order by match(body) against('alpha') desc, id"))
				// TF-IDF and BM25 rank the two positive scores differently; test zero
				// membership and pagination without pinning their scoring formulas.
				require.Equal(t, []int{2, 1}, queryIssue28327IDs(t, ctx, conn,
					"select id from docs order by (match(body) against('alpha') > 0), id limit 2"))
				require.Equal(t, []int{2}, queryIssue28327IDs(t, ctx, conn,
					"select id from docs where match(body) against('alpha') <= 0 "+
						"order by match(body) against('alpha'), id"))

				require.Equal(t, []issue28327ScoreState{
					{id: 1, hasScore: sql.NullBool{Bool: true, Valid: true}, zeroScore: sql.NullBool{Bool: false, Valid: true}},
					{id: 2, hasScore: sql.NullBool{Bool: false, Valid: true}, zeroScore: sql.NullBool{Bool: true, Valid: true}},
					{id: 3, hasScore: sql.NullBool{Bool: true, Valid: true}, zeroScore: sql.NullBool{Bool: false, Valid: true}},
				}, queryIssue28327ScoreStates(t, ctx, conn,
					"select id, match(body) against('alpha') > 0, match(body) against('alpha') = 0 "+
						"from docs order by id"))
				require.Equal(t, []issue28327ScoreState{
					{id: 1, hasScore: sql.NullBool{Bool: true, Valid: true}, zeroScore: sql.NullBool{Bool: false, Valid: true}},
					{id: 2, hasScore: sql.NullBool{Bool: false, Valid: true}, zeroScore: sql.NullBool{Bool: true, Valid: true}},
					{id: 3, hasScore: sql.NullBool{Bool: true, Valid: true}, zeroScore: sql.NullBool{Bool: false, Valid: true}},
				}, queryIssue28327ScoreStates(t, ctx, conn,
					"select id, round(match(body) against('alpha'), 3) > 0, "+
						"round(match(body) against('alpha'), 3) = 0 from docs order by id"))

				require.Equal(t, []issue28327DualScoreState{
					{id: 1, alpha: sql.NullBool{Bool: true, Valid: true}, beta: sql.NullBool{Bool: false, Valid: true}},
					{id: 2, alpha: sql.NullBool{Bool: false, Valid: true}, beta: sql.NullBool{Bool: true, Valid: true}},
					{id: 3, alpha: sql.NullBool{Bool: true, Valid: true}, beta: sql.NullBool{Bool: true, Valid: true}},
				}, queryIssue28327DualScoreStates(t, ctx, conn,
					"select id, match(body) against('alpha') > 0, match(body) against('beta') > 0 "+
						"from docs order by id"))

				require.Equal(t, []issue28327ScoreState{
					{id: 3, hasScore: sql.NullBool{Bool: true, Valid: true}, zeroScore: sql.NullBool{Bool: false, Valid: true}},
					{id: 1, hasScore: sql.NullBool{Bool: false, Valid: true}, zeroScore: sql.NullBool{Bool: true, Valid: true}},
				}, queryIssue28327ScoreStates(t, ctx, conn,
					"select id, match(body) against('beta') > 0, match(body) against('beta') = 0 "+
						"from docs where match(body) against('alpha') "+
						"order by match(body) against('beta') desc, id"))
				require.Equal(t, []issue28327ScoreState{
					{id: 3, hasScore: sql.NullBool{Bool: true, Valid: true}, zeroScore: sql.NullBool{Bool: false, Valid: true}},
					{id: 1, hasScore: sql.NullBool{Bool: false, Valid: true}, zeroScore: sql.NullBool{Bool: true, Valid: true}},
				}, queryIssue28327ScoreStates(t, ctx, conn,
					"select id, match(body) against('beta') > 0, match(body) against('beta') = 0 "+
						"from docs where match(body) against('alpha') > 0 "+
						"order by match(body) against('beta') desc, id"))
			})
		}
	})
}

type issue28327ScoreState struct {
	id                  int
	hasScore, zeroScore sql.NullBool
}

type issue28327DualScoreState struct {
	id          int
	alpha, beta sql.NullBool
}

func execIssue28327(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}

func queryIssue28327IDs(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) []int {
	t.Helper()
	rows, err := conn.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

func queryIssue28327ScoreStates(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) []issue28327ScoreState {
	t.Helper()
	rows, err := conn.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()

	var result []issue28327ScoreState
	for rows.Next() {
		var row issue28327ScoreState
		require.NoError(t, rows.Scan(&row.id, &row.hasScore, &row.zeroScore))
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	return result
}

func queryIssue28327DualScoreStates(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) []issue28327DualScoreState {
	t.Helper()
	rows, err := conn.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()

	var result []issue28327DualScoreState
	for rows.Next() {
		var row issue28327DualScoreState
		require.NoError(t, rows.Scan(&row.id, &row.alpha, &row.beta))
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	return result
}

func quoteIssue28327Ident(name string) string {
	return "`" + name + "`"
}

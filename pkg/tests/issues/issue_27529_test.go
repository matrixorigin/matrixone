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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue27529JSONStringsDoNotCompareAsBooleans(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_27529_json_string_bool"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()

		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db, "create table "+dbName+`.docs(id int primary key, meta json)`)
		execSQLRequire(t, ctx, db, "insert into "+dbName+`.docs values
			(1, '{\"active\":true}'),
			(2, '{\"active\":false}'),
			(3, '{\"active\":\"true\"}'),
			(4, '{\"active\":\"false\"}'),
			(5, '{\"active\":null}'),
			(6, '{}')`)

		rows, err := db.QueryContext(ctx, "select id, json_extract(meta, '$.active') = true, "+
			"json_extract(meta, '$.active') = false from "+dbName+".docs order by id")
		require.NoError(t, err)
		defer rows.Close()
		expectedComparisons := []struct {
			id         int
			equalTrue  sql.NullBool
			equalFalse sql.NullBool
		}{
			{id: 1, equalTrue: sql.NullBool{Bool: true, Valid: true}, equalFalse: sql.NullBool{Valid: true}},
			{id: 2, equalTrue: sql.NullBool{Valid: true}, equalFalse: sql.NullBool{Bool: true, Valid: true}},
			{id: 3},
			{id: 4},
			{id: 5},
			{id: 6},
		}
		for _, expected := range expectedComparisons {
			require.True(t, rows.Next())
			var id int
			var equalTrue, equalFalse sql.NullBool
			require.NoError(t, rows.Scan(&id, &equalTrue, &equalFalse))
			require.Equal(t, expected.id, id)
			require.Equal(t, expected.equalTrue, equalTrue)
			require.Equal(t, expected.equalFalse, equalFalse)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		assertIDs := func(query string, expected ...int) {
			t.Helper()
			rows, queryErr := db.QueryContext(ctx, query)
			require.NoError(t, queryErr)
			defer rows.Close()
			var actual []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				actual = append(actual, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, expected, actual)
		}

		assertIDs("select id from "+dbName+`.docs where json_extract(meta, '$.active') = true order by id`, 1)
		assertIDs("select id from "+dbName+`.docs where json_extract(meta, '$.active') = false order by id`, 2)

		execSQLRequire(t, ctx, db, `prepare issue_27529_p from "select id from `+dbName+`.docs where json_extract(meta, '$.active') = ? order by id"`)
		defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue_27529_p")
		execSQLRequire(t, ctx, db, "set @issue_27529_b = true")
		assertIDs("execute issue_27529_p using @issue_27529_b", 1)
		execSQLRequire(t, ctx, db, "set @issue_27529_b = false")
		assertIDs("execute issue_27529_p using @issue_27529_b", 2)
	})
}

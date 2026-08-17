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

func TestIssue27187JSONExtractBooleanComparison(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_27187_json_bool"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db, "create table "+dbName+`.t(id int primary key, j json)`)
		execSQLRequire(t, ctx, db, "insert into "+dbName+`.t values
			(1, '{"enabled":true}'),
			(2, '{"enabled":false}'),
			(3, '{}'),
			(4, '{"enabled":null}')`)

		rows, err := db.QueryContext(ctx,
			"select id, json_extract(j, '$.enabled') = true from "+dbName+".t order by id")
		require.NoError(t, err)
		defer rows.Close()

		for _, expected := range []struct {
			id    int
			value sql.NullBool
		}{
			{id: 1, value: sql.NullBool{Bool: true, Valid: true}},
			{id: 2, value: sql.NullBool{Bool: false, Valid: true}},
			{id: 3, value: sql.NullBool{}},
			{id: 4, value: sql.NullBool{}},
		} {
			require.True(t, rows.Next())
			var id int
			var value sql.NullBool
			require.NoError(t, rows.Scan(&id, &value))
			require.Equal(t, expected.id, id)
			require.Equal(t, expected.value, value)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		rows, err = db.QueryContext(ctx,
			"select id from "+dbName+".t where json_extract(j, '$.enabled') = false order by id")
		require.NoError(t, err)
		require.True(t, rows.Next())
		var id int
		require.NoError(t, rows.Scan(&id))
		require.Equal(t, 2, id)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		var decimalZero, decimalNonZero bool
		require.NoError(t, db.QueryRowContext(ctx, `select
			json_extract(json_array(cast(0.00 as decimal(10,2))), '$[0]') = true,
			json_extract(json_array(cast(1.20 as decimal(10,2))), '$[0]') = true`).
			Scan(&decimalZero, &decimalNonZero))
		require.False(t, decimalZero)
		require.True(t, decimalNonZero)

		_, err = db.ExecContext(ctx,
			`select json_extract(json_object('v', '"true"'), '$.v') = true`)
		require.Error(t, err)
	})
}

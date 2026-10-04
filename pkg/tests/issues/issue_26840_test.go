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

func TestIssue26840PreparedJSONModifyPreservesValueTypes(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		defer db.Close()

		const dbName = "issue_26840_prepared_json_modify"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db,
			"create table "+dbName+".t(id int primary key, j json)")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t values "+
				"(1,'{}'),(2,'{}'),(3,'{}'),(4,'{}'),(5,'{}'),(6,'{}'),(7,'{}'),(8,'{\"x\":0}')")

		setStmt, err := db.PrepareContext(ctx,
			"update "+dbName+".t set j=json_set(j,?,?) where id=?")
		require.NoError(t, err)
		defer setStmt.Close()
		for _, binding := range []struct {
			value any
			id    int64
		}{
			{value: int64(7), id: 1},
			{value: float64(1.5), id: 2},
			{value: true, id: 3},
			{value: false, id: 4},
			{value: "7", id: 5},
			{value: nil, id: 6},
		} {
			_, err = setStmt.ExecContext(ctx, "$.x", binding.value, binding.id)
			require.NoError(t, err)
		}

		insertStmt, err := db.PrepareContext(ctx,
			"update "+dbName+".t set j=json_insert(j,?,?) where id=?")
		require.NoError(t, err)
		defer insertStmt.Close()
		_, err = insertStmt.ExecContext(ctx, "$.x", int64(8), int64(7))
		require.NoError(t, err)

		replaceStmt, err := db.PrepareContext(ctx,
			"update "+dbName+".t set j=json_replace(j,?,?) where id=?")
		require.NoError(t, err)
		defer replaceStmt.Close()
		_, err = replaceStmt.ExecContext(ctx, "$.x", float64(2.5), int64(8))
		require.NoError(t, err)

		rows, err := db.QueryContext(ctx,
			"select json_type(json_extract(j,'$.x')) from "+dbName+".t order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{
			"INTEGER", "DOUBLE", "BOOLEAN", "BOOLEAN", "STRING", "NULL", "INTEGER", "DOUBLE",
		} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}

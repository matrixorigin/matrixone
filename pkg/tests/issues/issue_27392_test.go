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

func TestIssue27392JSONArrayAppend(t *testing.T) {
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

		var result string
		require.NoError(t, db.QueryRowContext(ctx,
			`select json_array_append('{"arr":[1,2]}', '$.arr', 3)`).Scan(&result))
		require.JSONEq(t, `{"arr":[1,2,3]}`, result)

		require.NoError(t, db.QueryRowContext(ctx,
			`select json_array_append('{"value":1}', '$.value', 2)`).Scan(&result))
		require.JSONEq(t, `{"value":[1,2]}`, result)

		require.NoError(t, db.QueryRowContext(ctx,
			`select json_array_append('{"arr":[1]}', '$.missing', 2)`).Scan(&result))
		require.JSONEq(t, `{"arr":[1]}`, result)

		var nullResult sql.NullString
		require.NoError(t, db.QueryRowContext(ctx,
			`select json_array_append('{"arr":[]}', '$.arr', null)`).Scan(&nullResult))
		require.False(t, nullResult.Valid)

		_, err = db.ExecContext(ctx,
			`select json_array_append('{"arr":[]}', '$.*', 1)`)
		require.ErrorContains(t, err, "invalid argument json_array_append")

		const dbName = "issue_27392_json_array_append"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db,
			"create table "+dbName+".t(id int primary key, doc json)")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+`.t values (1, '{"arr":[1,2]}')`)

		stmt, err := db.PrepareContext(ctx,
			"update "+dbName+".t set doc=json_array_append(doc,?,?) where id=?")
		require.NoError(t, err)
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, "$.arr", int64(3), int64(1))
		require.NoError(t, err)

		require.NoError(t, db.QueryRowContext(ctx,
			"select doc from "+dbName+".t where id=1").Scan(&result))
		require.JSONEq(t, `{"arr":[1,2,3]}`, result)
	})
}

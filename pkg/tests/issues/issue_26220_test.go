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
	"github.com/stretchr/testify/require"
)

func TestIssue26220ViewExplicitColumnList(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		execSQLRequire(t, ctx, db, "set role moadmin")

		const dbName = "issue_26220"
		execSQLRequire(t, ctx, db, "drop database if exists "+dbName)
		execSQLRequire(t, ctx, db, "create database "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create table "+dbName+".base_t (id int primary key, code varchar(20))")
		execSQLRequire(t, ctx, db, "insert into "+dbName+".base_t values (1, 'one')")

		execSQLRequire(t, ctx, db, "create view "+dbName+".v_alias (view_id, view_code) as select id, code from "+dbName+".base_t")
		var id int
		var code string
		require.NoError(t, db.QueryRowContext(ctx,
			"select view_id, view_code from "+dbName+".v_alias").Scan(&id, &code))
		require.Equal(t, 1, id)
		require.Equal(t, "one", code)
		rows, err := db.QueryContext(ctx, "select * from "+dbName+".v_alias")
		require.NoError(t, err)
		defer rows.Close()
		columns, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, []string{"view_id", "view_code"}, columns)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&id, &code))
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		err = db.QueryRowContext(ctx, "select id, code from "+dbName+".v_alias").Scan(&id, &code)
		require.Error(t, err)

		_, err = db.ExecContext(ctx,
			"create view "+dbName+".v_too_few (only_one) as select id, code from "+dbName+".base_t")
		require.Error(t, err)
		_, err = db.ExecContext(ctx,
			"create view "+dbName+".v_too_many (one_name, two_name, three_name) as select id, code from "+dbName+".base_t")
		require.Error(t, err)

		execSQLRequire(t, ctx, db, "use "+dbName)
		execSQLRequire(t, ctx, db, "create view "+dbName+".v_alter as select id, code from "+dbName+".base_t")
		execSQLRequire(t, ctx, db, "alter view v_alter (alter_id, alter_code) as select id, code from "+dbName+".base_t")
		require.NoError(t, db.QueryRowContext(ctx,
			"select alter_id, alter_code from "+dbName+".v_alter").Scan(&id, &code))
		require.Equal(t, 1, id)
		require.Equal(t, "one", code)
		_, err = db.ExecContext(ctx,
			"alter view v_alter (only_one) as select id, code from "+dbName+".base_t")
		require.Error(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select alter_id, alter_code from "+dbName+".v_alter").Scan(&id, &code))
	})
}

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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// #29903: the OK-packet insert_id reports only the value the statement generated,
// LAST_INSERT_ID() keeps the session value, and neither ever surfaces the hidden
// fake primary key, including after DELETE is optimized to a truncate.
func TestIssue29903InsertIDOkPacket(t *testing.T) {
	embed.RunBaseClusterTests(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			port := cn.GetServiceConfig().CN.Frontend.Port
			sqlDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, err)
			defer sqlDB.Close()
			conn, err := sqlDB.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			dbName := strings.ToLower(testutils.GetDatabaseName(t))
			exec := func(stmt string) sql.Result {
				res, err := conn.ExecContext(ctx, stmt)
				require.NoError(t, err, stmt)
				return res
			}
			insertID := func(stmt string) int64 {
				id, err := exec(stmt).LastInsertId()
				require.NoError(t, err)
				return id
			}
			lastInsertID := func() int64 {
				var id int64
				require.NoError(t, conn.QueryRowContext(ctx, "select last_insert_id()").Scan(&id))
				return id
			}

			exec("create database " + dbName)
			defer func() { _, _ = sqlDB.ExecContext(context.Background(), "drop database if exists "+dbName) }()
			exec("use " + dbName)

			// explicit id after a generated one: insert_id 0, LAST_INSERT_ID() kept
			exec("create table t_pk(id bigint auto_increment primary key, v int)")
			require.Equal(t, int64(1), insertID("insert into t_pk(v) values (1)"))
			require.Equal(t, int64(0), insertID("insert into t_pk values (100, 2)"))
			require.Equal(t, int64(1), lastInsertID())

			// a hidden-index fake primary key does not surface
			exec("create table t_ft(id bigint auto_increment primary key, body varchar(40), fulltext(body))")
			require.Equal(t, int64(1), insertID("insert into t_ft(body) values ('alpha')"))
			require.Equal(t, int64(2), insertID("insert into t_ft(body) values ('beta')"))
			require.Equal(t, int64(0), insertID("insert into t_ft values (50, 'gamma')"))
			require.Equal(t, int64(2), lastInsertID())

			// PK-less table: the hidden fake primary key stays internal after
			// DELETE is optimized to a truncate
			exec("set delete_opt_to_truncate = 1")
			exec("create table t_uk(id bigint auto_increment, v int, unique key(id))")
			require.Equal(t, int64(1), insertID("insert into t_uk(v) values (1)"))
			exec("delete from t_uk")
			require.Equal(t, int64(0), insertID("insert into t_uk values (100, 2)"))
			require.Equal(t, int64(1), lastInsertID())
		})
}

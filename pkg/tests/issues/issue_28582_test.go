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
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue28582PreparedNumericCastKeepsRuntimeKind(t *testing.T) {
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

		dbName := testutils.GetDatabaseName(t)
		execSQLRequire(t, ctx, db, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, db, "drop database if exists `"+dbName+"`")

		execSQLRequire(t, ctx, db,
			"prepare issue28582_cast from 'select cast(? as signed), cast(? as unsigned)'")
		defer execSQLMaybe(t, ctx, db, "deallocate prepare issue28582_cast")

		assertSQLExecute := func(name, value, wantSigned, wantUnsigned string) {
			t.Helper()
			t.Run(name, func(t *testing.T) {
				execSQLRequire(t, ctx, db, "set @issue28582_a = "+value)
				execSQLRequire(t, ctx, db, "set @issue28582_b = "+value)
				var signed, unsigned string
				require.NoError(t, db.QueryRowContext(ctx,
					"execute issue28582_cast using @issue28582_a, @issue28582_b").Scan(&signed, &unsigned))
				require.Equal(t, wantSigned, signed)
				require.Equal(t, wantUnsigned, unsigned)
			})
		}
		assertSQLExecute("decimal positive half", "1.5", "2", "2")
		assertSQLExecute("decimal negative half", "-1.5", "-2", "18446744073709551614")
		assertSQLExecute("decimal positive non-half", "1.6", "2", "2")
		assertSQLExecute("decimal negative non-half", "-1.6", "-2", "18446744073709551614")

		binaryStmt, err := db.PrepareContext(ctx, "select cast(? as signed), cast(? as unsigned)")
		require.NoError(t, err)
		defer binaryStmt.Close()
		for _, test := range []struct {
			name         string
			value        float64
			wantSigned   string
			wantUnsigned string
		}{
			{name: "binary positive half", value: 1.5, wantSigned: "2", wantUnsigned: "2"},
			{name: "binary negative half", value: -1.5, wantSigned: "-2", wantUnsigned: "18446744073709551614"},
			{name: "binary positive non-half", value: 1.6, wantSigned: "2", wantUnsigned: "2"},
			{name: "binary negative non-half", value: -1.6, wantSigned: "-2", wantUnsigned: "18446744073709551614"},
		} {
			t.Run(test.name, func(t *testing.T) {
				var signed, unsigned string
				require.NoError(t, binaryStmt.QueryRowContext(ctx, test.value, test.value).Scan(&signed, &unsigned))
				require.Equal(t, test.wantSigned, signed)
				require.Equal(t, test.wantUnsigned, unsigned)
			})
		}

		execSQLRequire(t, ctx, db,
			"create table `"+dbName+"`.dst (id int primary key, d decimal(10,1))")
		execSQLRequire(t, ctx, db,
			"prepare issue28582_insert from 'insert into `"+dbName+"`.dst values (?, cast(? as signed))'")
		defer execSQLMaybe(t, ctx, db, "deallocate prepare issue28582_insert")
		execSQLRequire(t, ctx, db, "set @issue28582_id = 1")
		execSQLRequire(t, ctx, db, "set @issue28582_d = 1.5")
		execSQLRequire(t, ctx, db, "execute issue28582_insert using @issue28582_id, @issue28582_d")
		execSQLRequire(t, ctx, db, "set @issue28582_id = 2")
		execSQLRequire(t, ctx, db, "set @issue28582_d = -1.5")
		execSQLRequire(t, ctx, db, "execute issue28582_insert using @issue28582_id, @issue28582_d")

		rows, err := db.QueryContext(ctx, "select id, d from `"+dbName+"`.dst order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []struct {
			id int64
			d  string
		}{
			{id: 1, d: "2.0"},
			{id: 2, d: "-2.0"},
		} {
			require.True(t, rows.Next())
			var id int64
			var d string
			require.NoError(t, rows.Scan(&id, &d))
			require.Equal(t, expected.id, id)
			require.Equal(t, expected.d, d)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}

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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPreparedInsertSelectGeneratedColumn(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		db := openRetestSQLDB(t, c)
		defer db.Close()

		dbName := testutils.GetDatabaseName(t)
		defer cleanupTestDatabases(t, db, dbName)
		execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
		execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
		execSQLDB(t, ctx, db, "create table src (id int primary key, v int)")
		execSQLDB(t, ctx, db, "insert into src values (1,10),(2,20),(3,30)")
		for _, tc := range []struct {
			name      string
			prepared  bool
			secondary bool
		}{
			{name: "direct_with_secondary", secondary: true},
			{name: "prepared_without_secondary", prepared: true},
			{name: "prepared_with_secondary", prepared: true, secondary: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				table := "gen_" + tc.name
				secondary := ""
				if tc.secondary {
					secondary = ", index iv(v)"
				}
				execSQLDB(t, ctx, db, fmt.Sprintf(
					"create table %s (id int primary key, v int, g int as (v + 1) stored%s)",
					table, secondary))
				var stmt *sql.Stmt
				if tc.prepared {
					// BVT covers SQL PREPARE; this test owns binary protocol reuse.
					var err error
					stmt, err = db.PrepareContext(ctx, fmt.Sprintf(
						"insert into %s(id,v) select id,v+? from src where id<=?", table))
					require.NoError(t, err)
					defer stmt.Close()
					_, err = stmt.ExecContext(ctx, int64(7), int64(3))
					require.NoError(t, err)
				} else {
					execSQLDB(t, ctx, db, fmt.Sprintf(
						"insert into %s(id,v) select id,v+7 from src where id<=3", table))
				}

				query := "select id,v,g,v+1 from " + table + " order by id"
				require.Equal(t, [][]string{{"1", "17", "18", "18"}, {"2", "27", "28", "28"}, {"3", "37", "38", "38"}},
					queryStringRows(t, ctx, db, query))
				if tc.prepared {
					// DELETE preserves the table identity so this exercises rebinding,
					// not the DDL-triggered rebuild covered by the BVT's TRUNCATE.
					execSQLDB(t, ctx, db, "delete from "+table)
					_, err := stmt.ExecContext(ctx, int64(-3), int64(2))
					require.NoError(t, err)
					require.Equal(t, [][]string{{"1", "7", "8", "8"}, {"2", "17", "18", "18"}},
						queryStringRows(t, ctx, db, query))
				}
			})
		}
	})
}

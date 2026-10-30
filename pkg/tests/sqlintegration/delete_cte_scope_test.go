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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDeleteCTETargetScope(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists delete_cte_scope", "drop database if exists delete_cte_other")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, statement := range []string{
			"create database delete_cte_scope", "use delete_cte_scope",
			"create table t(id int primary key)", "insert into t values (1),(2),(3)",
			"create table s(id int primary key)", "insert into s values (1),(2),(3)",
		} {
			execSQLRequire(t, ctx, db, statement)
		}
		for _, tc := range []struct {
			name, statement string
			affected        int64
			wantError       string
		}{
			{"alias", "with x as (select 1 as id) delete x from t as x where x.id=1", 1, ""},
			{"apply source", "delete t from t cross apply generate_series(t.id,t.id) as g where t.id=1", 1, ""},
			{"qualified explicit alias", "with x as (select 99 as id) delete ignored.x from t as x where x.id=1", 1, ""},
			{"qualified explicit alias same spelling", "with t as (select 99 as id) delete ignored.t from delete_cte_scope.t as t where t.id=1", 1, ""},
			{"using", "with x as (select 1 as id) delete from x using t as x where x.id=1", 1, ""},
			{"qualified source", "with t as (select 99 as id) delete x from delete_cte_scope.t as x where x.id=1", 1, ""},
			{"qualified unaliased source", "with t as (select 99 as id) delete t from delete_cte_scope.t where t.id=1", 1, ""},
			{"qualified target", "with t as (select 99 as id) delete from delete_cte_scope.t where id=1", 1, ""},
			{"CTE read source", "with q as (select 1 as id) delete x from t as x join q on x.id=q.id", 1, ""},
			{"multiple aliases", "with x as (select 99 as id), y as (select 99 as id) delete x,y from t as x join s as y on x.id=y.id where x.id=1", 2, ""},
			{"CTE target", "with t as (select 1 as id) delete from t where id=1", 0, "cannot delete from CTE"},
			{"aliased CTE", "with t as (select 1 as id) delete x from t as x where x.id=1", 0, "cannot delete from CTE"},
			{"mixed targets", "with s as (select 1 as id) delete x,y from t as x join s as y on x.id=y.id", 0, "cannot delete from CTE"},
			{"qualified target reads CTE", "with t as (select 1 as id) delete delete_cte_scope.t from t where t.id=1", 0, "cannot delete from CTE"},
			{"qualified target mismatch", "with q as (select 1) delete other.t from delete_cte_scope.t where t.id=1", 0, "does not match its source"},
			{"target missing from USING", "delete from t using s where s.id=1", 0, "missing FROM-clause entry for table 't'"},
			{"derived source", "delete t from (select 1 as id) as t", 0, "missing FROM-clause entry for table 't'"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				result, err := tx.ExecContext(ctx, tc.statement)
				if tc.wantError != "" {
					var sqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &sqlErr)
					require.Contains(t, sqlErr.Message, tc.wantError)
				} else {
					require.NoError(t, err)
					affected, err := result.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, tc.affected, affected)
				}
				// Both tables retain their exact original contents on error. Valid
				// DELETEs remove only id=1, including both targets in the fallback.
				for _, table := range []string{"t", "s"} {
					wantCount, wantSum := 3, 6
					if tc.wantError == "" && (table == "t" || tc.affected == 2) {
						wantCount, wantSum = 2, 5
					}
					var count, sum int
					require.NoError(t, tx.QueryRowContext(ctx, "select count(*),sum(id) from "+table).Scan(&count, &sum))
					require.Equal(t, wantCount, count)
					require.Equal(t, wantSum, sum)
				}
				require.NoError(t, tx.Rollback())
				var count int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from t where id in (1,2,3)").Scan(&count))
				require.Equal(t, 3, count)
			})
		}
		t.Run("read only resolves the actual source", func(t *testing.T) {
			for _, statement := range []string{
				"create database delete_cte_other", "create table delete_cte_other.t(id int primary key)",
				"insert into delete_cte_other.t values (1)",
				"create temporary table t(id int primary key)", "insert into t values (1)",
			} {
				execSQLRequire(t, ctx, db, statement)
			}
			tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			require.NoError(t, err)
			defer tx.Rollback()
			for _, statement := range []string{
				"delete t from delete_cte_other.t where t.id=1",
				"delete x from (delete_cte_other.t as x) where x.id=1",
			} {
				_, err := tx.ExecContext(ctx, statement)
				var sqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &sqlErr)
				require.Equal(t, uint16(1792), sqlErr.Number)
			}
			var count int
			require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from delete_cte_other.t where id=1").Scan(&count))
			require.Equal(t, 1, count)
			result, err := tx.ExecContext(ctx, "delete x from (t as x) where x.id=1")
			require.NoError(t, err)
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), affected)
			require.NoError(t, tx.Rollback())
		})

	})
}

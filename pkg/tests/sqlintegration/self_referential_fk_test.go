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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestSelfReferentialForeignKeyCatalog(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		const dbName = "self_referential_fk_catalog"
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+dbName)

		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "exec failed: %s", statement)
		}
		count := func(statement string, want int) {
			t.Helper()
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, statement).Scan(&got), statement)
			require.Equal(t, want, got, statement)
		}

		exec("create database " + dbName)
		exec("use " + dbName)

		// The default ALTER algorithm is INPLACE. The self-referential catalog
		// rows must be persisted after the final table definition is validated.
		exec("create table self_fk (id int primary key, parent_id int)")
		exec("insert into self_fk values (1, null), (2, 1)")
		exec("alter table self_fk add constraint fk_self foreign key (parent_id) references self_fk(id)")
		count("select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+dbName+"' and table_name = 'self_fk' and constraint_name = 'fk_self'", 1)

		var tableName, createSQL string
		require.NoError(t, conn.QueryRowContext(ctx, "show create table self_fk").Scan(&tableName, &createSQL))
		require.Equal(t, "self_fk", tableName)
		require.Contains(t, createSQL, "CONSTRAINT `fk_self`")
		count("select count(*) from information_schema.table_constraints where constraint_schema = '"+dbName+"' and table_name = 'self_fk' and constraint_name = 'fk_self' and constraint_type = 'FOREIGN KEY'", 1)

		// Composite self-references require one catalog row per column while
		// preserving declaration order through constraint_id.
		exec("create table self_composite (a int, b int, c int, d int, primary key (a, b))")
		exec("insert into self_composite values (1, 10, null, null), (2, 20, 1, 10)")
		exec("alter table self_composite add constraint fk_composite foreign key (c, d) references self_composite(a, b)")
		count("select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+dbName+"' and table_name = 'self_composite' and constraint_name = 'fk_composite'", 2)

		rows, err := conn.QueryContext(ctx, "select constraint_id, column_name, refer_column_name from mo_catalog.mo_foreign_keys where db_name = '"+dbName+"' and table_name = 'self_composite' and constraint_name = 'fk_composite' order by constraint_id")
		require.NoError(t, err)
		defer rows.Close()
		want := []struct {
			id       int
			column   string
			referred string
		}{{1, "c", "a"}, {2, "d", "b"}}
		for _, expected := range want {
			require.True(t, rows.Next())
			var got struct {
				id       int
				column   string
				referred string
			}
			require.NoError(t, rows.Scan(&got.id, &got.column, &got.referred))
			require.Equal(t, expected, got)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		// A failed self-reference check must roll back both the table metadata
		// mutation and the catalog insert executed by the ALTER.
		exec("create table self_rollback (id int primary key, parent_id int)")
		exec("insert into self_rollback values (1, 999)")
		_, err = conn.ExecContext(ctx, "alter table self_rollback add constraint fk_rollback foreign key (parent_id) references self_rollback(id)")
		require.Error(t, err)
		count("select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+dbName+"' and table_name = 'self_rollback' and constraint_name = 'fk_rollback'", 0)
		require.NoError(t, conn.QueryRowContext(ctx, "show create table self_rollback").Scan(&tableName, &createSQL))
		require.NotContains(t, createSQL, "fk_rollback")
	})
}

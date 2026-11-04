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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlterRenameForeignKey(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const database = "alter_rename_fk"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+database)
		_, err = conn.ExecContext(ctx, "create database "+database)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "use "+database)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create table p(id int primary key)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into p values(1),(2)")
		require.NoError(t, err)

		for _, tc := range []struct {
			name, oldName, newName, newSQL, parent, alter, invalidInsert string
			setup                                                        []string
			rows, columns                                                int
		}{
			{
				name:    "rename first preserves incoming and outgoing references",
				oldName: "child_before", newName: "renamed_before", newSQL: "renamed_before", parent: "p", rows: 1, columns: 1,
				setup: []string{
					"create table child_before(id int primary key, v int, constraint fk_existing foreign key(id) references p(id))",
					"insert into child_before values(1,1)",
					"create table grandchild(id int, constraint fk_incoming foreign key(id) references child_before(id))",
					"insert into grandchild values(1)",
				},
				alter:         "alter table child_before rename to renamed_before, add constraint fk_new foreign key(v) references p(id)",
				invalidInsert: "insert into renamed_before values(2,999)",
			},
			{
				name: "rename last", oldName: "child_after", newName: "renamed_after", newSQL: "renamed_after", parent: "p", rows: 1, columns: 1,
				setup:         []string{"create table child_after(id int primary key, v int)", "insert into child_after values(1,1)"},
				alter:         "alter table child_after add constraint fk_new foreign key(v) references p(id), rename to renamed_after",
				invalidInsert: "insert into renamed_after values(2,999)",
			},
			{
				name: "quoted composite self reference", oldName: "self`old", newName: "self`new", newSQL: "`self``new`", parent: "self`new", rows: 2, columns: 2,
				setup: []string{
					"create table `self``old`(a int, b int, c int, d int, primary key(a,b))",
					"insert into `self``old` values(1,10,null,null),(2,20,1,10)",
				},
				alter:         "alter table `self``old` rename to `self``new`, add constraint fk_new foreign key(c,d) references `self``old`(a,b)",
				invalidInsert: "insert into `self``new` values(3,30,1,999)",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := func(statement string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, statement)
					require.NoError(t, err, statement)
				}
				count := func(statement string, want int, args ...any) {
					t.Helper()
					var got int
					require.NoError(t, conn.QueryRowContext(ctx, statement, args...).Scan(&got), statement)
					require.Equal(t, want, got, statement)
				}
				for _, statement := range tc.setup {
					exec(statement)
				}
				exec(tc.alter)
				count("select count(*) from "+tc.newSQL, tc.rows)
				if tc.columns == 1 {
					count("select count(*) from "+tc.newSQL+" where id=1 and v=1", 1)
				} else {
					count("select count(*) from "+tc.newSQL+" where a=2 and b=20 and c=1 and d=10", 1)
					count("select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name=? and constraint_name='fk_new' and ((constraint_id=1 and column_name='c' and refer_column_name='a') or (constraint_id=2 and column_name='d' and refer_column_name='b'))", 2, database, tc.newName)
				}
				count("select count(*) from information_schema.tables where table_schema=? and table_name=?", 0, database, tc.oldName)
				count("select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name=? and constraint_name='fk_new' and refer_table_name=?", tc.columns, database, tc.newName, tc.parent)
				count("select count(*) from mo_catalog.mo_foreign_keys where db_name=? and (table_name=? or refer_table_name=?)", 0, database, tc.oldName, tc.oldName)
				count("select count(*) from information_schema.key_column_usage where constraint_schema=? and table_name=? and constraint_name='fk_new' and referenced_table_name=?", tc.columns, database, tc.newName, tc.parent)
				var name, definition string
				require.NoError(t, conn.QueryRowContext(ctx, "show create table "+tc.newSQL).Scan(&name, &definition))
				require.Equal(t, tc.newName, name)
				require.Contains(t, definition, "CONSTRAINT `fk_new`")
				_, err := conn.ExecContext(ctx, tc.invalidInsert)
				require.ErrorContains(t, err, "foreign key constraint fails")
				if tc.oldName == "child_before" {
					count("select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name='grandchild' and refer_table_name='renamed_before'", 1, database)
					count("select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name='renamed_before' and constraint_name='fk_existing'", 1, database)
				}
				exec("alter table " + tc.newSQL + " drop foreign key fk_new")
				count("select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name=? and constraint_name='fk_new'", 0, database, tc.newName)
			})
		}

		for _, self := range []bool{false, true} {
			t.Run(fmt.Sprintf("rollback self=%v", self), func(t *testing.T) {
				table := fmt.Sprintf("invalid_%v", self)
				parent := "p"
				if self {
					parent = table
				}
				definition := "create table " + table + "(id int primary key, v int"
				if !self {
					definition += ", constraint fk_kept foreign key(id) references p(id)"
				}
				_, err := conn.ExecContext(ctx, definition+")")
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, "insert into "+table+" values(1,999)")
				require.NoError(t, err)
				var name, before, after string
				require.NoError(t, conn.QueryRowContext(ctx, "show create table "+table).Scan(&name, &before))
				alter := "alter table " + table + " rename to " + table + "_new, "
				if !self {
					alter += "drop foreign key fk_kept, "
				}
				_, err = conn.ExecContext(ctx, alter+"add constraint fk_invalid foreign key(v) references "+parent+"(id)")
				var sqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &sqlErr)
				assert.Equal(t, uint16(1452), sqlErr.Number, "must reject invalid rows, not look up the old name")
				require.NoError(t, conn.QueryRowContext(ctx, "show create table "+table).Scan(&name, &after))
				require.Equal(t, before, after)
				var id, value, count int
				require.NoError(t, conn.QueryRowContext(ctx, "select id,v from "+table).Scan(&id, &value))
				require.Equal(t, 1, id)
				require.Equal(t, 999, value)
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema=? and table_name=?", database, table+"_new").Scan(&count))
				require.Zero(t, count)
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_foreign_keys where db_name=? and constraint_name='fk_invalid'", database).Scan(&count))
				require.Zero(t, count)
				if !self {
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name=? and constraint_name='fk_kept' and column_name='id' and refer_table_name='p'", database, table).Scan(&count))
					require.Equal(t, 1, count, "rollback must restore the dropped foreign key catalog row")
				}
			})
		}
	})
}

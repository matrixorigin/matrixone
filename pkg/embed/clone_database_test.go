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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneDatabaseForeignKeySourceInventory(t *testing.T) {
	defer func() { require.NoError(t, CloseBaseClusterTests()) }()
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		count := func(t *testing.T, query string, expected int) {
			t.Helper()
			var actual int
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&actual), query)
			require.Equal(t, expected, actual, query)
		}
		const source = "clone_fk_inventory_source"
		const target = "clone_fk_inventory_target"
		const snapshot = "clone_fk_inventory_snapshot"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			for _, statement := range []string{"rollback", "drop snapshot if exists " + snapshot, "drop database if exists " + target, "drop database if exists " + source} {
				_, err := conn.ExecContext(cleanupCtx, statement)
				assert.NoError(t, err, statement)
			}
		}()
		exec(t, "create database "+source)
		exec(t, "create table "+source+".parent (id int primary key)")
		exec(t, "create table "+source+".child (id int primary key, parent_id int, foreign key (parent_id) references "+source+".parent(id))")
		exec(t, "insert into "+source+".parent values (1),(2)")
		exec(t, "insert into "+source+".child values (10,1)")
		exec(t, "create snapshot "+snapshot+" for database "+source)
		exec(t, "insert into "+source+".child values (20,2)")
		for _, historical := range []bool{false, true} {
			t.Run(fmt.Sprintf("historical=%t", historical), func(t *testing.T) {
				cloneSQL := "create database " + target + " clone " + source
				expected := 2
				if historical {
					cloneSQL += " {snapshot='" + snapshot + "'}"
					expected = 1
				}
				exec(t, cloneSQL)
				count(t, "select count(*) from "+target+".parent", 2)
				count(t, "select count(*) from "+target+".child", expected)
				count(t, "select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+target+"' and table_name = 'child' and refer_db_name = '"+target+"' and refer_table_name = 'parent'", 1)
				_, err := conn.ExecContext(ctx, "insert into "+target+".child values (30,999)")
				require.Error(t, err)
				exec(t, "insert into "+target+".parent values (3)")
				exec(t, "insert into "+target+".child values (30,3)")
				count(t, "select count(*) from "+target+".child", expected+1)
				count(t, "select count(*) from "+source+".child", 2)
				exec(t, "drop database "+target)
			})
		}
		exec(t, "begin")
		exec(t, "create database "+target+" clone "+source)
		count(t, "select count(*) from "+target+".child", 2)
		exec(t, "rollback")
		count(t, "select count(*) from mo_catalog.mo_database where datname = '"+target+"'", 0)
		count(t, "select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+target+"'", 0)
	})
}

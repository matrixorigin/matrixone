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
		const external = "clone_fk_inventory_external"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			for _, statement := range []string{"rollback", "drop snapshot if exists " + snapshot, "drop database if exists " + external, "drop database if exists " + target, "drop database if exists " + source} {
				_, err := conn.ExecContext(cleanupCtx, statement)
				assert.NoError(t, err, statement)
			}
		}()
		exec(t, "create database "+source)
		exec(t, "create table "+source+".parent (id int auto_increment primary key, key idx_visibility(id) invisible) auto_increment=10")
		exec(t, "create table "+source+".child (id int primary key, parent_id int, key idx_visibility(parent_id), foreign key (parent_id) references "+source+".parent(id))")
		exec(t, "insert into "+source+".parent values (1),(2)")
		exec(t, "insert into "+source+".child values (10,1)")
		exec(t, "create table "+source+".native_index (id int primary key, score int, key ix_score(score) invisible)")
		exec(t, "insert into "+source+".native_index values (1,7)")
		// Independent native tables exercise concurrent CREATE catalog pairs
		// and index buffers in the same atomic database-clone transaction.
		for _, name := range []string{"native_peer_a", "native_peer_b", "native_peer_c"} {
			exec(t, "create table "+source+"."+name+" (id int primary key, marker varchar(50), key ix_marker(marker) invisible)")
			exec(t, "insert into "+source+"."+name+" values (1, '"+name+"')")
		}
		exec(t, "create snapshot "+snapshot+" for database "+source)
		exec(t, "alter table "+source+".parent alter index idx_visibility visible")
		exec(t, "insert into "+source+".parent values (99)")
		exec(t, "insert into "+source+".child values (20,2)")
		insertAutoID := func(t *testing.T, want int64) {
			t.Helper()
			result, err := conn.ExecContext(ctx, "insert into "+target+".parent values (null)")
			require.NoError(t, err)
			id, err := result.LastInsertId()
			require.NoError(t, err)
			require.Equal(t, want, id)
		}

		for _, historical := range []bool{false, true} {
			t.Run(fmt.Sprintf("historical=%t", historical), func(t *testing.T) {
				cloneSQL := "create database " + target + " clone " + source
				expected := 2
				expectedParents := 3
				nextID := int64(100)
				if historical {
					cloneSQL += " {snapshot='" + snapshot + "'}"
					expected = 1
					expectedParents = 2
					nextID = 10
				}
				exec(t, cloneSQL)
				// Compare catalog values written by the clone with the same
				// source generation, including invisible keys and column aliases.
				sourceHint := ""
				if historical {
					var physical int64
					require.NoError(t, conn.QueryRowContext(ctx, "select ts from mo_catalog.mo_snapshots where sname='"+snapshot+"'").Scan(&physical))
					sourceHint = fmt.Sprintf(" {MO_TS=%d}", physical)
				}
				indexMetadata := func(database, hint string) []string {
					query := "select concat_ws('|', t.relname,i.name,i.type,coalesce(i.algo,''),coalesce(i.algo_params,''),cast(i.is_visible as varchar),cast(i.hidden as varchar),i.comment,i.column_name,cast(i.ordinal_position as varchar),coalesce(i.options,'')) " +
						"from mo_catalog.mo_indexes" + hint + " i join mo_catalog.mo_tables" + hint +
						" t on i.table_id=t.rel_id where t.reldatabase='" + database + "' order by t.relname,i.name,i.ordinal_position"
					rows, err := conn.QueryContext(ctx, query)
					require.NoError(t, err)
					defer rows.Close()
					var metadata []string
					for rows.Next() {
						var value string
						require.NoError(t, rows.Scan(&value))
						metadata = append(metadata, value)
					}
					require.NoError(t, rows.Err())
					return metadata
				}
				require.Equal(t, indexMetadata(source, sourceHint), indexMetadata(target, ""))
				var table, ddl string
				require.NoError(t, conn.QueryRowContext(ctx, "show create table "+target+".native_index").Scan(&table, &ddl))
				require.Contains(t, ddl, "INVISIBLE")
				count(t, "select count(*) from "+target+".native_index where score=7", 1)
				for _, name := range []string{"native_peer_a", "native_peer_b", "native_peer_c"} {
					count(t, "select count(*) from "+target+"."+name+" where id=1 and marker='"+name+"'", 1)
				}
				count(t, "select count(*) from "+target+".parent", expectedParents)
				count(t, "select count(*) from "+target+".child", expected)
				insertAutoID(t, nextID)
				for _, name := range []string{"parent", "child"} {
					var table, ddl string
					err := conn.QueryRowContext(ctx, "show create table "+target+"."+name).Scan(&table, &ddl)
					require.NoError(t, err)
					require.Equal(t, name, table)
					if historical && name == "parent" {
						require.Contains(t, ddl, "INVISIBLE")
					} else {
						require.NotContains(t, ddl, "INVISIBLE")
					}
				}
				count(t, "select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+target+"' and table_name = 'child' and refer_db_name = '"+target+"' and refer_table_name = 'parent'", 1)
				_, err := conn.ExecContext(ctx, "insert into "+target+".child values (30,999)")
				require.Error(t, err)
				exec(t, "insert into "+target+".parent values (3)")
				exec(t, "insert into "+target+".child values (30,3)")
				count(t, "select count(*) from "+target+".child", expected+1)
				count(t, "select count(*) from "+source+".child", 2)
				if !historical {
					exec(t, "delete from "+target+".parent where id >= 99")
					exec(t, "create table "+target+".copied_parent clone "+target+".parent")
					res, err := conn.ExecContext(ctx, "insert into "+target+".copied_parent values (null)")
					require.NoError(t, err)
					id, err := res.LastInsertId()
					require.NoError(t, err)
					require.EqualValues(t, 100, id)
				}
				exec(t, "drop database "+target)
			})
		}
		t.Run("target binds a preexisting forward reference", func(t *testing.T) {
			exec(t, "create table "+source+".plain (id int primary key)")
			exec(t, "insert into "+source+".plain values (1)")
			exec(t, "create database "+external)
			exec(t, "set foreign_key_checks = 0")
			exec(t, "create table "+external+".child (id int primary key, parent_id int, "+
				"constraint fk_external_plain foreign key (parent_id) references "+target+".plain(id))")
			exec(t, "set foreign_key_checks = 1")

			exec(t, "create database "+target+" clone "+source)
			exec(t, "insert into "+external+".child values (1,1)")
			_, err := conn.ExecContext(ctx, "insert into "+external+".child values (2,999)")
			require.ErrorContains(t, err, "foreign key constraint fails")
			_, err = conn.ExecContext(ctx, "delete from "+target+".plain where id=1")
			require.ErrorContains(t, err, "foreign key constraint fails")
			exec(t, "drop database "+external)
			exec(t, "drop database "+target)
		})

		t.Run("shared objects and the mutable tail keep the source snapshot", func(t *testing.T) {
			exec(t, "insert into "+source+".native_index values (2,8),(3,9)")
			exec(t, "select mo_ctl('dn','flush','"+source+".native_index')")
			exec(t, "delete from "+source+".native_index where id=3")
			exec(t, "update "+source+".native_index set score=80 where id=2")
			exec(t, "insert into "+source+".native_index values (4,40),(5,50)")
			exec(t, "delete from "+source+".native_index where id=5")

			exec(t, "create database "+target+" clone "+source)
			for _, database := range []string{target, source} {
				count(t, "select count(*) from "+database+".native_index", 3)
				count(t, "select count(*) from "+database+".native_index where (id,score) in ((1,7),(2,80),(4,40))", 3)
				count(t, "select count(*) from "+database+".native_index where score in (8,9,50)", 0)
			}
			exec(t, "alter table "+target+".native_index alter index ix_score visible")
			count(t, "select count(*) from "+target+".native_index force index (ix_score) where score=80", 1)
			count(t, "select count(*) from "+target+".native_index force index (ix_score) where score=9", 0)
			exec(t, "delete from "+target+".native_index where id=4")
			count(t, "select count(*) from "+source+".native_index where id=4 and score=40", 1)
			exec(t, "drop database "+target)
		})

		t.Run("a failed independent table aborts all workers", func(t *testing.T) {
			exec(t, "select enable_fault_injection()")
			exec(t, "select add_fault_point('fj/cn/clone_fails',':::','echo',40,'"+target+".native_peer_b')")
			defer func() {
				_, _ = conn.ExecContext(context.Background(), "select remove_fault_point('fj/cn/clone_fails')")
			}()
			_, err := conn.ExecContext(ctx, "create database "+target+" clone "+source)
			require.ErrorContains(t, err, "injected table clone error")
			exec(t, "select remove_fault_point('fj/cn/clone_fails')")
			// Rollback must keep the source's shared immutable objects.
			count(t, "select count(*) from "+source+".native_index where (id,score) in ((1,7),(2,80),(4,40))", 3)
			count(t, "select count(*) from mo_catalog.mo_database where datname='"+target+"'", 0)
			exec(t, "create database "+target+" clone "+source)
			for _, name := range []string{"native_peer_a", "native_peer_b", "native_peer_c"} {
				count(t, "select count(*) from "+target+"."+name+" where id=1 and marker='"+name+"'", 1)
			}
			exec(t, "drop database "+target)
		})

		t.Run("an orphan rolls back the entire database clone", func(t *testing.T) {
			exec(t, "set foreign_key_checks = 0")
			exec(t, "insert into "+source+".child values (40,999)")
			exec(t, "set foreign_key_checks = 1")
			_, err := conn.ExecContext(ctx, "create database "+target+" clone "+source)
			require.ErrorContains(t, err, "would leave orphan row(s)")
			count(t, "select count(*) from mo_catalog.mo_database where datname = '"+target+"'", 0)
			count(t, "select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+target+"'", 0)
			exec(t, "delete from "+source+".child where id=40")
			exec(t, "create database "+target+" clone "+source)
			count(t, "select count(*) from "+target+".child", 2)
			exec(t, "drop database "+target)
		})

		t.Run("cross database foreign keys and nested views preserve dependency scope", func(t *testing.T) {
			t.Cleanup(func() {
				for _, statement := range []string{
					"drop database if exists " + target,
					"drop view if exists " + source + ".v_nested",
					"drop view if exists " + source + ".v_external",
					"drop view if exists " + source + ".v_qualified",
					"drop view if exists " + source + ".v_local",
					"drop table if exists " + source + ".outgoing",
					"drop database if exists " + external,
				} {
					_, err := conn.ExecContext(ctx, statement)
					assert.NoError(t, err, statement)
				}
			})
			exec(t, "create database "+external)
			exec(t, "create table "+external+".parent (id int primary key, label varchar(40))")
			exec(t, "insert into "+external+".parent values (1,'external original')")
			exec(t, "create table "+external+".incoming (id int primary key, parent_id int, foreign key (parent_id) references "+source+".native_peer_a(id))")
			exec(t, "insert into "+external+".incoming values (1,1)")
			exec(t, "create table "+source+".outgoing (id int primary key, local_id int, external_id int, "+
				"constraint fk_local foreign key (local_id) references "+source+".parent(id), "+
				"constraint fk_external foreign key (external_id) references "+external+".parent(id))")
			exec(t, "insert into "+source+".outgoing values (1,1,1)")
			exec(t, "use "+source)
			exec(t, "create view "+source+".v_local as select id from parent")
			exec(t, "create view "+source+".v_qualified as select id from "+source+".v_local")
			exec(t, "create view "+external+".v_parent as select id,label from "+external+".parent")
			exec(t, "create view "+source+".v_external as select id,label from "+external+".v_parent")
			exec(t, "create view "+source+".v_nested as select l.id,e.label from "+source+".v_qualified l join "+source+".v_external e on l.id=e.id")

			exec(t, "create database "+target+" clone "+source)
			count(t, "select count(*) from "+target+".v_nested where id=1 and label='external original'", 1)
			count(t, "select count(*) from mo_catalog.mo_foreign_keys where db_name='"+target+"' and table_name='outgoing' and constraint_name='fk_local' and refer_db_name='"+target+"' and refer_table_name='parent'", 1)
			count(t, "select count(*) from mo_catalog.mo_foreign_keys where db_name='"+target+"' and table_name='outgoing' and constraint_name='fk_external' and refer_db_name='"+external+"' and refer_table_name='parent'", 1)
			exec(t, "insert into "+target+".parent values (500)")
			exec(t, "insert into "+target+".outgoing values (2,500,1)")
			count(t, "select count(*) from "+target+".v_qualified where id=500", 1)
			count(t, "select count(*) from "+source+".v_qualified where id=500", 0)
			for _, statement := range []string{
				"insert into " + source + ".outgoing values (2,500,1)",
				"insert into " + target + ".outgoing values (3,999,1)",
				"insert into " + target + ".outgoing values (3,500,999)",
				"delete from " + target + ".parent where id=500",
				"delete from " + source + ".native_peer_a where id=1",
				"delete from " + external + ".parent where id=1",
			} {
				_, err := conn.ExecContext(ctx, statement)
				require.ErrorContains(t, err, "foreign key constraint fails", statement)
			}
			// An incoming FK outside the cloned database still belongs to its
			// original parent; the cloned parent must not inherit that reverse link.
			exec(t, "delete from "+target+".native_peer_a where id=1")
			count(t, "select count(*) from "+source+".native_peer_a where id=1 and marker='native_peer_a'", 1)
			exec(t, "update "+external+".parent set label='external updated' where id=1")
			count(t, "select count(*) from "+target+".v_nested where id=1 and label='external updated'", 1)
		})

		exec(t, "begin")
		exec(t, "create database "+target+" clone "+source)
		count(t, "select count(*) from "+target+".child", 2)
		insertAutoID(t, 100)
		var databaseID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select dat_id from mo_catalog.mo_database where datname = '"+target+"'").Scan(&databaseID))
		indexMetadata := fmt.Sprintf("select count(*) from mo_catalog.mo_indexes where database_id = %d and name = 'ix_score' and column_name = 'score'", databaseID)
		count(t, indexMetadata, 1)
		exec(t, "rollback")
		count(t, indexMetadata, 0)
		count(t, "select count(*) from mo_catalog.mo_database where datname = '"+target+"'", 0)
		count(t, "select count(*) from mo_catalog.mo_foreign_keys where db_name = '"+target+"'", 0)
	})
}

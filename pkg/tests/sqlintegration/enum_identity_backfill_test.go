// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestEnumIdentityPopulatedIndexBackfill(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists enum_identity_backfill")
		exec := func(tb *testing.T, s string) { tb.Helper(); execSQLRequire(tb, ctx, db, s) }
		values := func(tb *testing.T, s string) []string {
			tb.Helper()
			rows, err := db.QueryContext(ctx, s)
			require.NoError(tb, err, s)
			defer rows.Close()
			var out []string
			for rows.Next() {
				var v string
				require.NoError(tb, rows.Scan(&v))
				out = append(out, v)
			}
			require.NoError(tb, rows.Err())
			return out
		}
		hidden := func(tb *testing.T, table, index, role string) string {
			tb.Helper()
			var name string
			require.NoError(tb, db.QueryRowContext(ctx, "select i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname=? and i.name=? and (?='' or i.algo_table_type=?) limit 1", table, index, role, role).Scan(&name))
			return "`" + strings.ReplaceAll(name, "`", "``") + "`"
		}
		indexCount := func(tb *testing.T, table string) int {
			tb.Helper()
			var n int
			require.NoError(tb, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname=? and i.name not in ('PRIMARY')", table).Scan(&n))
			return n
		}
		exec(t, "create database enum_identity_backfill")
		exec(t, "use enum_identity_backfill")
		t.Run("unique", func(t *testing.T) {
			exec(t, "create table u(id int primary key,e enum('a','A','') collate utf8mb4_bin)")
			exec(t, "insert into u values(1,2),(2,1),(3,3),(4,NULL)")
			exec(t, "insert ignore into u values(5,'invalid')")
			exec(t, "create unique index ux on u(e)")
			name := hidden(t, "u", "ux", "")
			query := "select concat(__mo_index_pri_col,':',hex(cast(__mo_index_idx_col as char)),':',__mo_index_idx_col+0) from " + name + " order by __mo_index_pri_col"
			want := []string{"1:41:2", "2:61:1", "3::3", "5::0"}
			require.Equal(t, want, values(t, query))
			exec(t, "create table u_inline(id int primary key,e enum('a','A','') collate utf8mb4_bin,unique key ux(e))")
			exec(t, "insert into u_inline values(1,2),(2,1),(3,3),(4,NULL)")
			exec(t, "insert ignore into u_inline values(5,'invalid')")
			require.Equal(t, want, values(t, strings.Replace(query, name, hidden(t, "u_inline", "ux", ""), 1)))
			_, err = db.ExecContext(ctx, "insert into u values(6,2)")
			require.Error(t, err)
			_, err = db.ExecContext(ctx, "update u set e=2 where id=2")
			require.Error(t, err)
			require.Equal(t, want, values(t, query), "failed duplicate operations leave keys intact")
			exec(t, "update u set e=NULL where id=1")
			exec(t, "insert into u values(6,2)")
			exec(t, "delete from u where id=6")
			require.Equal(t, []string{"2:61:1", "3::3", "5::0"}, values(t, query))
			exec(t, "insert into u values(7,2)")
			require.Equal(t, []string{"7:2"}, values(t, "select concat(id,':',e+0) from u where e+0=2 order by id"))
		})
		t.Run("ivfflat_sync_include", func(t *testing.T) {
			exec(t, "create table inc(id int primary key,e enum('a','A','') collate utf8mb4_bin,v vecf32(2))")
			exec(t, "insert into inc values(1,2,'[1,2]'),(2,1,'[3,4]'),(3,3,'[5,6]'),(4,NULL,'[7,8]')")
			exec(t, "insert ignore into inc values(5,'invalid','[9,10]')")
			exec(t, "create index vi using ivfflat on inc(v) lists=1 op_type 'vector_l2_ops' include(e)")
			query := func() string {
				return "select concat(__mo_index_pri_col,':',coalesce(hex(cast(__mo_index_include_e as char)),'NULL'),':',coalesce(cast(__mo_index_include_e+0 as char),'NULL')) from " + hidden(t, "inc", "vi", "entries") + " order by __mo_index_pri_col"
			}
			require.Equal(t, []string{"1:41:2", "2:61:1", "3::3", "4:NULL:NULL", "5::0"}, values(t, query()))
			exec(t, "update inc set e=2 where id=2")
			exec(t, "delete from inc where id=1")
			exec(t, "insert into inc values(6,1,'[11,12]')")
			require.Equal(t, []string{"2:41:2", "3::3", "4:NULL:NULL", "5::0", "6:61:1"}, values(t, query()))
			exec(t, "alter table inc alter reindex vi ivfflat lists=1")
			require.Equal(t, []string{"2:41:2", "3::3", "4:NULL:NULL", "5::0", "6:61:1"}, values(t, query()))
			exec(t, "set experimental_ivf_index=1")
			search := "select concat(id,':',e+0) from inc order by l2_distance(v,'[3,4]') limit 1"
			require.Equal(t, []string{"2:2"}, values(t, search))
			physical := strings.Join(values(t, "explain analyze verbose "+search), "\n")
			require.Contains(t, physical, "Vector Index Scan", "prove the search really executes the vector index")

		})
		t.Run("unsupported_maintenance_is_atomic", func(t *testing.T) {
			exec(t, "create table pk(e enum('a','A') collate utf8mb4_bin primary key,v vecf32(2))")
			exec(t, "insert into pk values(1,'[1,2]'),(2,'[3,4]')")
			_, err = db.ExecContext(ctx, "create index vp using ivfflat on pk(v) lists=1 op_type 'vector_l2_ops'")
			require.ErrorContains(t, err, "collated ENUM primary key")
			require.Zero(t, indexCount(t, "pk"))
			exec(t, "create table ab(id int primary key,e enum('a','A') collate utf8mb4_general_ci,v vecf32(2))")
			exec(t, "insert into ab values(1,2,'[1,2]')")
			_, err = db.ExecContext(ctx, "create index va using ivfflat on ab(v) lists=1 op_type 'vector_l2_ops' async include(e)")
			require.ErrorContains(t, err, "asynchronous maintenance")
			require.Zero(t, indexCount(t, "ab"))
			require.Equal(t, []string{"2"}, values(t, "select cast(e+0 as char) from ab"))
		})

		t.Run("composite_pk_is_opaque", func(t *testing.T) {
			exec(t, "create table cp(e enum('a','A') collate utf8mb4_bin,k int,v vecf32(2),primary key(e,k))")
			exec(t, "insert into cp values(1,1,'[1,2]'),(2,1,'[3,4]')")
			exec(t, "create index vc using ivfflat on cp(v) lists=1 op_type 'vector_l2_ops'")
			check := func() {
				t.Helper()
				base := values(t, "select hex(__mo_cpkey_col) from cp order by __mo_cpkey_col")
				entries := values(t, "select hex(__mo_index_pri_col) from "+hidden(t, "cp", "vc", "entries")+" order by __mo_index_pri_col")
				require.Equal(t, base, entries, "composite keys retain exact bytes")
			}
			check()
			exec(t, "delete from cp where e+0=2 and k=1")
			exec(t, "insert into cp values(2,2,'[5,6]')")
			check()
		})
		t.Run("async_include_final_schema", func(t *testing.T) {
			exec(t, "create table ai(id int primary key,e enum('a','A') collate __mo_enum_legacy_main_v1,v vecf32(2))")
			exec(t, "create index ai_idx using ivfflat on ai(v) lists=1 op_type 'vector_l2_ops' async include(e)")
			for _, action := range []string{
				"modify e enum('a','A') collate utf8mb4_bin",
				"change e renamed enum('a','A') collate utf8mb4_bin, add other int",
			} {
				_, err = db.ExecContext(ctx, "alter table ai "+action)
				require.ErrorContains(t, err, "asynchronous maintenance")
			}
			_, err = db.ExecContext(ctx, "alter table ai modify e enum('a','A') collate utf8mb4_bin, drop index ai_idx")
			require.ErrorContains(t, err, "combined COPY DROP INDEX")
			require.NotZero(t, indexCount(t, "ai"))
			exec(t, "drop index ai_idx on ai")
			exec(t, "alter table ai modify e enum('a','A') collate utf8mb4_bin")
		})
		t.Run("legacy_pk_and_final_schema", func(t *testing.T) {
			exec(t, "create table lp(e enum('a','A') collate __mo_enum_legacy_main_v1 primary key,v vecf32(2))")
			exec(t, "insert into lp values(1,'[1,2]'),(2,'[3,4]')")
			exec(t, "create index vl using ivfflat on lp(v) lists=1 op_type 'vector_l2_ops'")
			q := "select cast(__mo_index_pri_col+0 as char) from " + hidden(t, "lp", "vl", "entries") + " order by __mo_index_pri_col+0"
			require.Equal(t, []string{"1", "2"}, values(t, q))
			for _, action := range []string{
				"modify e enum('a','A') collate utf8mb4_bin",
				"add column other int, modify e enum('a','A') collate utf8mb4_bin",
				"change e renamed enum('a','A') collate utf8mb4_bin",
			} {
				_, err = db.ExecContext(ctx, "alter table lp "+action)
				require.ErrorContains(t, err, "collated ENUM primary key")
				require.Equal(t, []string{"1", "2"}, values(t, q))
			}
			_, err = db.ExecContext(ctx, "alter table lp drop index vl, modify e enum('a','A') collate utf8mb4_bin")
			require.ErrorContains(t, err, "combined COPY DROP INDEX")
			require.Equal(t, []string{"1", "2"}, values(t, q))
			exec(t, "drop index vl on lp")
			exec(t, "alter table lp modify e enum('a','A') collate utf8mb4_bin")
			require.Equal(t, []string{"1", "2"}, values(t, "select cast(e+0 as char) from lp order by e+0"))
		})
	})
}

func TestEnumCollationAlterAdmission(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists enum_alter_admission")
		execute := func(s string) { t.Helper(); execSQLRequire(t, ctx, db, s) }
		ordinals := func() []string {
			t.Helper()
			rows, e := db.QueryContext(ctx, "select concat(id,':',coalesce(cast(e+0 as char),'NULL')) from t order by id")
			require.NoError(t, e)
			defer rows.Close()
			var out []string
			for rows.Next() {
				var v string
				require.NoError(t, rows.Scan(&v))
				out = append(out, v)
			}
			require.NoError(t, rows.Err())
			return out
		}
		execute("create database enum_alter_admission")
		execute("use enum_alter_admission")
		execute("create table t(id int primary key,e enum('A','a','') collate __mo_enum_legacy_main_v1,unique key u(e))")
		execute("insert into t values(1,1),(2,2),(3,3),(5,NULL)")
		execute("insert ignore into t values(4,'invalid')")
		want := []string{"1:1", "2:2", "3:3", "4:0", "5:NULL"}
		require.Equal(t, want, ordinals())
		execute("alter table t modify e enum('A','a','') collate utf8mb4_bin")
		require.Equal(t, want, ordinals(), "safe identity change preserves empty,0,NULL and case-distinct ordinals")
		for _, action := range []string{
			"modify e enum('A','a','') collate utf8mb4_general_ci",
			"add q int, change e renamed enum('A','a','') collate utf8mb4_unicode_ci",
		} {
			_, err = db.ExecContext(ctx, "alter table t "+action)
			require.ErrorContains(t, err, "would remap")
			require.Equal(t, want, ordinals())
			var n int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema=database() and table_name='t' and column_name in ('q','renamed')").Scan(&n))
			require.Zero(t, n)
		}
		execute("delete from t where id=2")
		execute("insert into t values(6,'a')")
		require.Equal(t, []string{"1:1", "3:3", "4:0", "5:NULL", "6:2"}, ordinals(), "new writes use the new binary identity and maintain UNIQUE")
		execute("create table distinct_members(e enum('X','Y') collate utf8mb4_bin)")
		execute("insert into distinct_members values(2)")
		execute("alter table distinct_members change e renamed enum('X','Y') collate utf8mb4_general_ci")
		var ordinal int
		require.NoError(t, db.QueryRowContext(ctx, "select renamed+0 from distinct_members").Scan(&ordinal))
		require.Equal(t, 2, ordinal)
	})
}

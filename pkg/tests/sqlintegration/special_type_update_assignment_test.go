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

func TestSpecialTypeUpdateAssignments(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "special_type_update_assignments"
		exec := func(t *testing.T, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec(t, "create database "+schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec(t, "use "+schema)
		for _, tc := range []struct{ name, definition, initial, source, assignment, want string }{
			{"enum-alias", "enum('','red','中文😀')", "'red'", "'中文😀'", "incoming.v", "中文😀:3"},
			{"set-alias", "set('a','文')", "'a'", "'文'", "incoming.v", "文:2"},
			{"enum-cast", "enum('red','blue')", "'red'", "'blue'", "cast(incoming.v as char)", "blue:2"},
			{"enum-null", "enum('red','blue')", "'red'", "NULL", "incoming.v", "NULL:NULL"},
			{"enum-defined-empty-old", "enum('','red')", "''", "'red'", "incoming.v", "red:2"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec(t, "drop table if exists dst")
				exec(t, "drop table if exists src")
				exec(t, "create table dst(id int primary key,v "+tc.definition+",note int)")
				exec(t, "create table src(id int primary key,v "+tc.definition+")")
				exec(t, "insert into dst values(1,"+tc.initial+",0),(2,NULL,0)")
				exec(t, "insert into src values(1,"+tc.source+")")
				exec(t, "update dst target join src incoming on target.id=incoming.id set target.v="+tc.assignment)
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, "select concat(coalesce(cast(v as char),'NULL'),':',coalesce(cast(cast(v as unsigned) as char),'NULL')) from dst where id=1").Scan(&got))
				require.Equal(t, tc.want, got)
				var untouched bool
				require.NoError(t, conn.QueryRowContext(ctx, "select v is null from dst where id=2").Scan(&untouched))
				require.True(t, untouched)
			})
		}
		for _, tc := range []struct{ name, definition, source, want, boundary string }{
			{"derived-enum-zero", "enum('','red')", "'invalid'", "0", ""},
			{"derived-set-empty-member", "set('','a')", "1", "1", ""},
			{"derived-enum-zero-limit", "enum('','red')", "'invalid'", "0", " limit 1"},
			{"derived-enum-zero-order-limit", "enum('','red')", "'invalid'", "0", " order by id limit 1"},
			{"derived-enum-zero-offset", "enum('','red')", "'invalid'", "0", " order by id limit 1 offset 1"},
			{"derived-set-empty-limit", "set('','a')", "1", "1", " limit 1"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec(t, "drop table if exists dst")
				exec(t, "drop table if exists src")
				exec(t, "create table dst(id int primary key,v "+tc.definition+")")
				exec(t, "create table src(id int primary key,v "+tc.definition+")")
				exec(t, "insert into dst values(1,NULL)")
				exec(t, "insert ignore into src values(1,"+tc.source+")")
				if tc.boundary == " order by id limit 1 offset 1" {
					exec(t, "insert into src values(0,NULL)")
				}
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, "select cast(cast(v as unsigned) as char) from (select v from src"+tc.boundary+") x").Scan(&got))
				require.Equal(t, tc.want, got)
				exec(t, "update dst d join (select id,v from src"+tc.boundary+") incoming on d.id=incoming.id set d.v=incoming.v")
				require.NoError(t, conn.QueryRowContext(ctx, "select cast(cast(v as unsigned) as char) from dst").Scan(&got))
				require.Equal(t, tc.want, got)
			})
		}
		for _, tc := range []struct{ name, definition, want string }{
			{"cross-domain-enum-zero-with-empty", "enum('','blue','red')", "1:0,2:3,3:NULL"},
			{"cross-domain-enum-zero-without-empty", "enum('blue','red')", "1:0,2:2,3:NULL"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec(t, "drop table if exists dst")
				exec(t, "drop table if exists src")
				exec(t, "create table dst(id int primary key,v "+tc.definition+")")
				exec(t, "create table src(id int primary key,v enum('red','blue'))")
				exec(t, "insert into dst values(1,NULL),(2,NULL),(3,NULL)")
				exec(t, "insert ignore into src values(1,'invalid')")
				exec(t, "insert into src values(2,'red'),(3,NULL)")
				exec(t, "update dst d join (select id,v from src) incoming on d.id=incoming.id set d.v=incoming.v")
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(concat(id,':',coalesce(cast(cast(v as unsigned) as char),'NULL')) order by id) from dst").Scan(&got))
				require.Equal(t, tc.want, got)
			})
		}
		t.Run("multi-target-duplicate-stored-values", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table dst(id int primary key,v enum('','red'),note int,index idx_v(v))")
			exec(t, "create table src(id int primary key,owner int,note int)")
			exec(t, "insert into dst values(1,'',0),(2,'red',0),(3,NULL,0)")
			exec(t, "insert ignore into dst values(4,'invalid',0)")
			exec(t, "insert into src values(1,1,0),(2,1,0),(3,4,0),(4,4,0)")
			exec(t, "update dst d join src s on d.id=s.owner set d.v=d.v,d.note=d.note+1,s.note=s.note+1")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(concat(id,':',coalesce(cast(cast(v as unsigned) as char),'NULL'),':',note) order by id) from dst").Scan(&got))
			require.Equal(t, "1:1:1,2:2:0,3:NULL:0,4:0:1", got)
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(note order by id) from src").Scan(&got))
			require.Equal(t, "1,1,1,1", got)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst force index(idx_v) where v='' ").Scan(&count))
			require.Equal(t, 2, count)
		})
		t.Run("domain-index-and-prepared", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table dst(id int primary key,v enum('red','blue'),s set('a','b'),note int,unique index idx_v(v))")
			exec(t, "create table src(id int primary key,v enum('blue','red'),s set('b','a'))")
			exec(t, "insert into dst values(1,'red','a',0)")
			exec(t, "insert into src values(1,'blue','b')")
			exec(t, "update dst d join src incoming on d.id=incoming.id set d.v=incoming.v,d.s=incoming.s")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select concat(cast(v as unsigned),':',cast(s as unsigned)) from dst where id=1").Scan(&got))
			require.Equal(t, "2:2", got)
			exec(t, "insert into dst values(2,'red','a',0)") // OLD unique key was removed
			_, err := conn.ExecContext(ctx, "insert into dst values(3,'blue','a',0)")
			require.Error(t, err) // NEW unique key exists
			stmt, err := conn.PrepareContext(ctx, "update dst d join src incoming on d.id=incoming.id set d.v=?")
			require.NoError(t, err)
			defer stmt.Close()
			_, err = stmt.ExecContext(ctx, "invalid")
			require.Error(t, err)
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(concat(id,':',cast(v as unsigned)) order by id) from dst").Scan(&got))
			require.Equal(t, "1:2,2:1", got)
			exec(t, "delete from dst where id=2")
			_, err = stmt.ExecContext(ctx, "red")
			require.NoError(t, err)
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst where id=1").Scan(&got))
			require.Equal(t, "red", got)
			_, err = stmt.ExecContext(ctx, "blue")
			require.NoError(t, err)
			exec(t, "update dst set note=note+1,s=s where id=1 order by id limit 1")
		})
		t.Run("shared-set-storage-cache", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table dst(id int primary key,s set('','b','a'),note int)")
			exec(t, "create table src(id int primary key,s set('','a','b'))")
			exec(t, "insert into dst values(1,NULL,0),(2,NULL,0),(3,NULL,0)")
			exec(t, "insert into src values(1,'a'),(2,1),(3,NULL)")
			exec(t, "update dst d join (select id,s from src) x on d.id=x.id set d.s=x.s,d.note=find_in_set('a',x.s)")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(concat(id,':',coalesce(cast(cast(s as unsigned) as char),'NULL'),':',coalesce(cast(note as char),'NULL')) order by id) from dst").Scan(&got))
			// Different SET definitions convert through labels; the empty display maps to bitmap 0.
			require.Equal(t, "1:4:2,2:0:0,3:NULL:NULL", got)
		})
		for _, sequential := range []bool{false, true} {
			t.Run(fmt.Sprintf("generated-new-evaluated-once/sequential=%v", sequential), func(t *testing.T) {
				exec(t, "drop table if exists dst")
				exec(t, "drop sequence if exists special_update_seq")
				exec(t, "create sequence special_update_seq start with 1")
				exec(t, "create table dst(id int primary key,v enum('red','blue'),note int default 0,g int generated always as (cast(v as unsigned)) stored)")
				exec(t, "insert into dst(id,v) values(1,'red')")
				prefix := ""
				if sequential {
					prefix = "note=note+1,"
				}
				exec(t, "update dst set "+prefix+"v=if(nextval('special_update_seq')=1,'red','blue') where id=1")
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, "select concat(v,':',g,':',currval('special_update_seq')) from dst").Scan(&got))
				require.Equal(t, "red:1:1", got)
			})

		}

		t.Run("limited-copy-retains-selected-storage", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table src(id int primary key,v enum('','red'))")
			exec(t, "create table dst(id int primary key,v enum('','red'))")
			exec(t, "insert into src values(1,''),(3,'red'),(4,NULL)")
			exec(t, "insert ignore into src values(2,'invalid')")
			exec(t, "insert into dst values(1,'red'),(2,'red'),(3,''),(4,'red')")
			var got string
			check := func(want string) {
				require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(concat(id,':',coalesce(cast(cast(v as unsigned) as char),'NULL')) order by id) from dst").Scan(&got))
				require.Equal(t, want, got)
			}
			exec(t, "update dst d join (select id,v from src order by id limit 0) s on d.id=s.id set d.v=s.v")
			check("1:2,2:2,3:1,4:2")
			exec(t, "update dst d join (select id,v from src order by id limit 2 offset 1) s on d.id=s.id set d.v=s.v")
			check("1:2,2:0,3:2,4:2")
			exec(t, "update dst d join (select id,v from src order by id limit 4) s on d.id=s.id set d.v=s.v")
			check("1:1,2:0,3:2,4:NULL")
		})
		for _, tc := range []struct{ name, definition, assignment, want string }{
			{"earlier-enum", "enum('red','blue')", "v=if(nextval('other_update_seq')=1,'red','blue'),note=note+1", "1:1:1"},
			{"numeric", "int", "note=note+1,v=nextval('other_update_seq')", "1:1:1"},
		} {
			t.Run("generated-sequential-"+tc.name, func(t *testing.T) {
				exec(t, "drop table if exists dst")
				exec(t, "drop sequence if exists other_update_seq")
				exec(t, "create sequence other_update_seq start with 1")
				exec(t, "create table dst(id int primary key,v "+tc.definition+",note int,g int generated always as(cast(v as unsigned)) stored,index idx_g(g))")
				exec(t, "insert into dst(id,v,note) values(1,1,0)")
				exec(t, "update dst set "+tc.assignment+" where id=1")
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, "select concat(cast(v as unsigned),':',g,':',currval('other_update_seq')) from dst force index(idx_g) where g=1").Scan(&got))
				require.Equal(t, tc.want, got)
				exec(t, "update dst set note=note+1,v=NULL where id=1")
				var nulls bool
				require.NoError(t, conn.QueryRowContext(ctx, "select v is null and g is null from dst").Scan(&nulls))
				require.True(t, nulls)
				require.NoError(t, conn.QueryRowContext(ctx, "select cast(currval('other_update_seq') as char)").Scan(&got))
				require.Equal(t, "1", got)
			})
		}
		t.Run("typed-enum-expression-evaluated-once", func(t *testing.T) {
			exec(t, "create sequence typed_update_seq start with 1")
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(cast_value_to_index('blue,red','red,blue',cast_value_to_index('red,blue',if(nextval('typed_update_seq')=1,'red','blue'))) as unsigned)").Scan(&got))
			require.Equal(t, int64(2), got)
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('typed_update_seq')").Scan(&got))
			require.Equal(t, int64(1), got)
		})
		t.Run("generated-if-display", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "create table dst(id int primary key,v enum('red','blue'),s set('a','b'),g varchar(10) generated always as(if(id>0,v,v)) stored,h varchar(10) generated always as(if(id>0,s,s)) stored)")
			exec(t, "insert into dst(id,v,s) values(1,'red','a'),(2,NULL,NULL)")
			exec(t, "update dst set v='blue',s='b' where id=1")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select concat(v,':',g,':',s,':',h) from dst where id=1").Scan(&got))
			require.Equal(t, "blue:blue:b:b", got)
			var nulls bool
			require.NoError(t, conn.QueryRowContext(ctx, "select g is null and h is null from dst where id=2").Scan(&nulls))
			require.True(t, nulls)
			_, err := conn.ExecContext(ctx, "create table invalid_generated(id int,v enum('red','blue'),g varchar(10) generated always as(if(rand()>0.5,v,v)) stored)")
			require.Error(t, err)
		})
		t.Run("outer-join-inactive-invalid-source", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table dst(id int primary key,label varchar(20),note int)")
			exec(t, "create table src(id int primary key,owner int,v enum('red','blue'))")
			exec(t, "insert into dst values(1,'blue',0),(2,'invalid',0)")
			exec(t, "insert into src values(1,1,'red')")
			exec(t, "update dst d left join src s on d.id=s.owner set d.note=d.note+1,s.v=d.label")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(note order by id) from dst").Scan(&got))
			require.Equal(t, "1,1", got)
			require.NoError(t, conn.QueryRowContext(ctx, "select v from src").Scan(&got))
			require.Equal(t, "blue", got)
		})
		t.Run("mixed-batch-rollback-and-indexes", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table dst(id int primary key,v enum('red','blue','green'),unique index idx_v(v))")
			exec(t, "create table src(id int primary key,v varchar(20))")
			exec(t, "insert into dst values(1,'red'),(2,'blue')")
			exec(t, "insert into src values(1,'green'),(2,'invalid')")
			_, err := conn.ExecContext(ctx, "update dst d join src s on d.id=s.id set d.v=s.v")
			require.Error(t, err)
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(concat(id,':',cast(v as unsigned)) order by id) from dst").Scan(&got))
			require.Equal(t, "1:1,2:2", got)
			exec(t, "insert into dst values(3,'green')")
			_, err = conn.ExecContext(ctx, "insert into dst values(4,'red')")
			require.Error(t, err)
		})
		t.Run("sequential-default-display-order", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "create table dst(id int primary key,v enum('z','a') default 'z',note int)")
			exec(t, "insert into dst values(1,'z',0),(2,'a',0)")
			exec(t, "update dst set note=note+1 order by cast(v as char) limit 1")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(note order by id) from dst").Scan(&got))
			require.Equal(t, "0,1", got)
			exec(t, "update dst set v='z',note=cast(v as unsigned) where id=2")
			require.NoError(t, conn.QueryRowContext(ctx, "select concat(v,':',note) from dst where id=2").Scan(&got))
			require.Equal(t, "z:1", got)
			exec(t, "update dst set v='a' where id=2")
			exec(t, "update dst set v=default where id=2")
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst where id=2").Scan(&got))
			require.Equal(t, "z", got)
		})
		t.Run("noop-on-update-and-onduplicate", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table dst(id int primary key,v enum('red','blue'),s set('a','b'),stamp datetime default '2000-01-01 00:00:00' on update current_timestamp)")
			exec(t, "insert into dst(id,v,s) values(1,'red','a')")
			exec(t, "update dst set v=v,s=s where id=1")
			var unchanged bool
			require.NoError(t, conn.QueryRowContext(ctx, "select stamp='2000-01-01 00:00:00' from dst").Scan(&unchanged))
			require.True(t, unchanged)
			exec(t, "create table src(id int primary key,v enum('blue','red'),s set('b','a'))")
			exec(t, "insert into src values(1,'blue','b')")
			exec(t, "insert into dst(id,v,s) select id,v,s from src on duplicate key update v=values(v),s=values(s)")
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "select concat(v,':',cast(v as unsigned),':',s,':',cast(s as unsigned)) from dst").Scan(&got))
			require.Equal(t, "blue:2:b:2", got)
		})
		t.Run("strict-invalid-assignment", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "create table dst(id int primary key,v enum('red','blue'),note int)")
			exec(t, "insert into dst values(1,'red',0)")
			for _, query := range []string{"update dst set v=0", "update dst set v='invalid'", "update dst set v=missing_column"} {
				_, err := conn.ExecContext(ctx, query)
				require.Error(t, err, query)
				var ordinal int
				require.NoError(t, conn.QueryRowContext(ctx, "select cast(v as unsigned) from dst where id=1").Scan(&ordinal))
				require.Equal(t, 1, ordinal)
			}
		})

		for _, mode := range []string{"strict", "nonstrict", "ignore"} {
			t.Run("typed-source-policy/"+mode, func(t *testing.T) {
				var oldMode string
				require.NoError(t, conn.QueryRowContext(ctx, "select @@session.sql_mode").Scan(&oldMode))
				defer exec(t, "set sql_mode='"+oldMode+"'")
				exec(t, "drop table if exists dst")
				exec(t, "drop table if exists src")
				exec(t, "create table dst(id int primary key,v enum('blue'),index idx_v(v))")
				exec(t, "create table src(id int primary key,v enum('red','blue'))")
				exec(t, "insert into dst values(1,'blue')")
				exec(t, "insert into src values(1,'red')")
				sqlMode := "STRICT_TRANS_TABLES"
				if mode == "nonstrict" {
					sqlMode = ""
				}
				exec(t, "set sql_mode='"+sqlMode+"'")
				modifier := ""
				if mode == "ignore" {
					modifier = "ignore "
				}
				_, err := conn.ExecContext(ctx, "update "+modifier+"dst d join src s on d.id=s.id set d.v=s.v")
				if mode == "strict" {
					var failure *mysql.MySQLError
					require.ErrorAs(t, err, &failure)
					require.Equal(t, uint16(1265), failure.Number)
				} else {
					require.NoError(t, err)
					func() {
						rows, err := conn.QueryContext(ctx, "show warnings")
						require.NoError(t, err)
						defer rows.Close()
						require.True(t, rows.Next())
						var level, message string
						var code int
						require.NoError(t, rows.Scan(&level, &code, &message))
						require.Equal(t, 1265, code)
						require.False(t, rows.Next())
						require.NoError(t, rows.Err())
					}()
				}
				var got int
				require.NoError(t, conn.QueryRowContext(ctx, "select cast(v as unsigned) from dst").Scan(&got))
				if mode == "strict" {
					require.Equal(t, 1, got)
				} else {
					require.Zero(t, got)
				}
				var indexed int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst force index(idx_v) where v='blue'").Scan(&indexed))
				if mode == "strict" {
					require.Equal(t, 1, indexed)
				} else {
					require.Zero(t, indexed)
				}
			})
		}
		t.Run("stored-ordinal-identity", func(t *testing.T) {
			exec(t, "drop table if exists dst")
			exec(t, "drop table if exists src")
			exec(t, "create table dst(id int primary key,v enum('','red'),note int)")
			exec(t, "insert into dst values(1,'',0),(2,'red',0)")
			exec(t, "insert ignore into dst values(3,'invalid',0)")
			var before string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(cast(v as unsigned) order by id) from dst").Scan(&before))
			require.Equal(t, "1,2,0", before)
			exec(t, "update dst set note=1")
			rows, err := conn.QueryContext(ctx, "select id,cast(v as unsigned),note from dst order by id")
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var id, ordinal, note int
				require.NoError(t, rows.Scan(&id, &ordinal, &note))
				got = append(got, fmt.Sprintf("%d:%d:%d", id, ordinal, note))
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []string{"1:1:1", "2:2:1", "3:0:1"}, got)
		})
	})
}

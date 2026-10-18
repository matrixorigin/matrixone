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
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// One existing shared fixture covers the SQL boundary and same-connection
// recovery. This is current-head regression evidence, not historical parity
// or an external rollout/QA acceptance record.
func TestInsertRowAliasSQLContracts(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "insert_row_alias_contract"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q, args...)
			require.NoError(t, err, q)
		}
		scalar := func(q, want string) {
			t.Helper()
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&got), q)
			require.Equal(t, want, got, q)
		}
		reject := func(q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			var wire *mysql.MySQLError
			require.ErrorAs(t, err, &wire, q)
			require.Equal(t, uint16(20313), wire.Number, q)
			require.Equal(t, "HY000", string(wire.SQLState[:]), q)
			require.Equal(t, "unsupported DML: row-alias subqueries in on duplicate key update cannot be evaluated before duplicate-key action", wire.Message, q)
		}
		exec("drop database if exists " + schema)
		exec("create database " + schema)
		exec("use " + schema)
		exec("create table keyed(id int primary key, a int not null, b int default 7)")
		exec("insert into keyed values(1,10,20)")
		exec("insert into keyed(b,id,a) values(30,1,40) as n(x,k,y) on duplicate key update a=n.y,b=a+n.x")
		scalar("select concat(a,',',b) from keyed", "40,70")
		exec("insert into keyed set id=1,a=9,b=3 as n on duplicate key update a=n.a,b=n.b")
		scalar("select concat(a,',',b) from keyed", "9,3")
		exec("start transaction")
		exec("insert into keyed values(1,60,70) as n on duplicate key update a=n.a,b=n.b")
		scalar("select concat(a,',',b) from keyed", "60,70")
		exec("rollback")
		scalar("select concat(a,',',b) from keyed", "9,3")
		p, err := conn.PrepareContext(ctx, "insert into keyed(id,a) values(1,?) as n on duplicate key update a=n.a,b=?")
		require.NoError(t, err)
		defer p.Close()
		_, err = p.ExecContext(ctx, int64(11), int64(12))
		require.NoError(t, err)
		scalar("select concat(a,',',b) from keyed", "11,12")
		_, err = p.ExecContext(ctx, nil, int64(99))
		var wire *mysql.MySQLError
		require.ErrorAs(t, err, &wire)
		require.Equal(t, uint16(3819), wire.Number)
		require.Equal(t, "HY000", string(wire.SQLState[:]))
		scalar("select concat(a,',',b) from keyed", "11,12")
		_, err = p.ExecContext(ctx, int64(13), int64(14))
		require.NoError(t, err)
		scalar("select concat(a,',',b) from keyed", "13,14")
		exec("create table materialized(id int primary key default 1,a int default 7,g int generated always as(a+1) stored)")
		exec("insert into materialized(id,a) values(1,99)")
		exec("insert into materialized values() as n(k,x,y) on duplicate key update a=n.y")
		scalar("select concat(a,',',g) from materialized", "8,9")
		exec("create table no_key(a int,b int)")
		exec("insert into no_key values(1,2) as n on duplicate key update b=n.b+1")
		scalar("select concat(a,',',b) from no_key", "1,2")
		exec("create table visible_name(id int primary key,__mo_fake_pk_col int auto_increment,a int)")
		exec("insert into visible_name(id,a) values(1,5)")
		exec("insert into visible_name(id,__mo_fake_pk_col,a) values(1,default,6) as n on duplicate key update a=n.__mo_fake_pk_col")
		scalar("select concat(__mo_fake_pk_col,',',a) from visible_name", "1,2")
		exec("create table multi_auto(id int primary key,a int auto_increment,b int auto_increment,v int)")
		exec("insert into multi_auto(id,v) values(1,1) as n on duplicate key update v=n.v")
		exec("insert into multi_auto values(1,default,default,2) as n on duplicate key update v=n.a+n.b")
		scalar("select concat(a,',',b,',',v) from multi_auto", "1,1,4")
		exec("create table udf_source(y int)")
		exec("insert into udf_source values(10),(20)")
		exec("create function alias_lookup() returns int language sql as 'select y from udf_source'")
		exec("create function alias_nested_lookup() returns int language sql as 'alias_lookup()'")
		exec("create function alias_constant() returns int language sql as '1+1'")
		for _, target := range []string{"keyed(id,a,b) values(1,99,88)", "no_key(a,b) values(3,4)"} {
			for _, rhs := range []string{"(select y from udf_source)", "alias_lookup()", "alias_nested_lookup()"} {
				reject("insert into " + target + " as n on duplicate key update b=" + rhs)
				scalar("select concat(a,',',b) from keyed", "13,14")
				scalar("select count(*) from no_key", "1")
			}
		}
		exec("insert into keyed values(1,99,88) as n on duplicate key update b=alias_constant()")
		scalar("select concat(a,',',b) from keyed", "13,2")
		exec("insert into keyed values(1,99,88) on duplicate key update b=(select max(y) from udf_source)")
		scalar("select concat(a,',',b) from keyed", "13,20")
		exec("insert into keyed values(1,99,88) as n on duplicate key update b=n.b")
		scalar("select concat(a,',',b) from keyed", "13,88")
	})
}

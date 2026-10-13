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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestReorderedJoinPredicateSemantics(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists join_order_semantics")
		exec := func(q string) { _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		for _, q := range []string{
			"create database join_order_semantics", "use join_order_semantics",
			"create table a(x int,y int)", "create table b(x int,y int)", "create table c(x int)",
			"insert into a values(1,2),(1,2),(null,2),(2,3)",
			"insert into b values(1,2),(1,3),(2,3),(null,2)", "insert into c values(2),(4),(null)",
		} {
			exec(q)
		}
		for _, tc := range []struct {
			q     string
			count int
		}{
			{"select count(*) from a,b where a.x=b.x and a.y=b.y", 3},
			{"select count(*) from a,b,c where a.x+b.x=c.x and a.y<=b.y", 5},
			{"select count(*) from a,b,c where a.x=b.x", 15},
			{"select count(*) from a,b where a.x<b.x", 2},
			{"select count(*) from a,b,c where a.x+b.x=c.x and c.x=99", 0},
		} {
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, tc.q).Scan(&count))
			require.Equal(t, tc.count, count, tc.q)
		}
		for _, q := range []string{
			"create table part(p_partkey int primary key,p_name varchar(30))",
			"create table supplier(s_suppkey int primary key,s_nationkey int)",
			"create table nation(n_nationkey int primary key,n_name varchar(30))",
			"create table orders(o_orderkey int primary key,o_orderdate date)",
			"create table partsupp(ps_partkey int,ps_suppkey int,ps_supplycost decimal(10,2),primary key(ps_partkey,ps_suppkey))",
			"create table lineitem(l_orderkey int,l_partkey int,l_suppkey int,l_extendedprice decimal(10,2),l_discount decimal(5,2),l_quantity int)",
			"insert into part values(1,'pink rose'),(2,'blue')",
			"insert into nation values(1,'A'),(2,'B')", "insert into supplier values(1,1),(2,2)",
			"insert into orders values(1,'2020-01-01'),(2,'2021-01-01')",
			"insert into partsupp values(1,1,3),(1,2,5),(2,1,8)",
			"insert into lineitem values(1,1,1,100,.10,2),(1,1,1,100,.10,2),(2,1,2,50,0,1),(1,2,1,999,0,1),(1,null,1,999,0,1)",
		} {
			exec(q)
		}
		query, err := os.ReadFile("../../sql/plan/tpch/q9.sql")
		require.NoError(t, err)
		rows, err := conn.QueryContext(ctx, string(query))
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var nation, profit string
			var year int
			require.NoError(t, rows.Scan(&nation, &year, &profit))
			got = append(got, fmt.Sprintf("%s/%d/%s", nation, year, profit))
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"A/2020/168.0000", "B/2021/45.0000"}, got)
	})
}

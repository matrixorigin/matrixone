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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTPCHAggBench(t *testing.T) {
	RunBaseClusterTests(
		func(c Cluster) {
			cn0, err := c.GetCNService(0)
			require.NoError(t, err)

			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
				cn0.GetServiceConfig().CN.Frontend.Port,
			)

			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer db.Close()

			ctx := context.Background()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			// Create and load TPC-H tables (SF=0.01 inline)
			setupTPCH(t, ctx, conn)

			queries := []struct {
				name string
				sql  string
			}{
				{"Q1", `SELECT l_returnflag, l_linestatus,
					sum(l_quantity), sum(l_extendedprice),
					sum(l_extendedprice * (1 - l_discount)),
					avg(l_quantity), avg(l_extendedprice), avg(l_discount), count(*)
					FROM lineitem
					WHERE l_shipdate <= date '1998-12-01' - interval '90' day
					GROUP BY l_returnflag, l_linestatus`},
				{"Q5-like", `SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
					FROM lineitem, orders, customer, nation, region, supplier
					WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
					AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
					AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
					AND r_name = 'ASIA'
					GROUP BY n_name ORDER BY revenue DESC`},
				{"CountStar", `SELECT count(*) FROM lineitem`},
				{"SumDecimal", `SELECT l_returnflag, sum(l_extendedprice) FROM lineitem GROUP BY l_returnflag`},
			}

			// Warmup
			for _, q := range queries {
				_, err := conn.ExecContext(ctx, q.sql)
				require.NoError(t, err, "query %s failed", q.name)
			}

			// Benchmark
			const iterations = 5
			for _, q := range queries {
				var total time.Duration
				for i := 0; i < iterations; i++ {
					start := time.Now()
					_, err := conn.ExecContext(ctx, q.sql)
					require.NoError(t, err)
					total += time.Since(start)
				}
				t.Logf("%-12s avg=%v", q.name, total/iterations)
			}
		},
	)
}

func setupTPCH(t *testing.T, ctx context.Context, conn *sql.Conn) {
	t.Helper()

	ddls := []string{
		`CREATE DATABASE IF NOT EXISTS tpch`,
		`USE tpch`,
		`DROP TABLE IF EXISTS nation`,
		`DROP TABLE IF EXISTS region`,
		`DROP TABLE IF EXISTS supplier`,
		`DROP TABLE IF EXISTS customer`,
		`DROP TABLE IF EXISTS orders`,
		`DROP TABLE IF EXISTS lineitem`,
		`CREATE TABLE region (r_regionkey INT, r_name CHAR(25), r_comment VARCHAR(152), PRIMARY KEY (r_regionkey))`,
		`CREATE TABLE nation (n_nationkey INT, n_name CHAR(25), n_regionkey INT, n_comment VARCHAR(152), PRIMARY KEY (n_nationkey))`,
		`CREATE TABLE supplier (s_suppkey INT, s_name CHAR(25), s_address VARCHAR(40), s_nationkey INT, s_phone CHAR(15), s_acctbal DECIMAL(15,2), s_comment VARCHAR(101), PRIMARY KEY (s_suppkey))`,
		`CREATE TABLE customer (c_custkey INT, c_name VARCHAR(25), c_address VARCHAR(40), c_nationkey INT, c_phone CHAR(15), c_acctbal DECIMAL(15,2), c_mktsegment CHAR(10), c_comment VARCHAR(117), PRIMARY KEY (c_custkey))`,
		`CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHAR(1), o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority CHAR(15), o_clerk CHAR(15), o_shippriority INT, o_comment VARCHAR(79), PRIMARY KEY (o_orderkey))`,
		`CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag CHAR(1), l_linestatus CHAR(1), l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHAR(25), l_shipmode CHAR(10), l_comment VARCHAR(44), PRIMARY KEY (l_orderkey, l_linenumber))`,
	}
	for _, ddl := range ddls {
		_, err := conn.ExecContext(ctx, ddl)
		require.NoError(t, err, "DDL failed: %s", ddl)
	}

	// Generate synthetic data (100K lineitem rows)
	_, err := conn.ExecContext(ctx, `USE tpch`)
	require.NoError(t, err)

	// Regions
	_, err = conn.ExecContext(ctx, `INSERT INTO region VALUES (0,'AFRICA','a'),(1,'AMERICA','b'),(2,'ASIA','c'),(3,'EUROPE','d'),(4,'MIDDLE EAST','e')`)
	require.NoError(t, err)

	// Nations (25)
	for i := 0; i < 25; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO nation VALUES (%d, 'NATION_%d', %d, 'comment')`, i, i, i%5))
		require.NoError(t, err)
	}

	// Suppliers (100)
	for i := 1; i <= 100; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO supplier VALUES (%d,'Supplier#%05d','addr%d',%d,'phone%d',%.2f,'comment')`, i, i, i, i%25, i, float64(i)*10.5))
		require.NoError(t, err)
	}

	// Customers (1500)
	for batch := 0; batch < 15; batch++ {
		vals := ""
		for i := 0; i < 100; i++ {
			id := batch*100 + i + 1
			if i > 0 {
				vals += ","
			}
			vals += fmt.Sprintf("(%d,'Customer#%09d','addr%d',%d,'phone%d',%.2f,'BUILDING','comment')", id, id, id, id%25, id, float64(id)*5.5)
		}
		_, err = conn.ExecContext(ctx, "INSERT INTO customer VALUES "+vals)
		require.NoError(t, err)
	}

	// Orders (15000)
	for batch := 0; batch < 150; batch++ {
		vals := ""
		for i := 0; i < 100; i++ {
			id := batch*100 + i + 1
			if i > 0 {
				vals += ","
			}
			vals += fmt.Sprintf("(%d,%d,'O',%.2f,'1995-01-01','1-URGENT','Clerk#000001',0,'comment')", id, (id%1500)+1, float64(id)*100.5)
		}
		_, err = conn.ExecContext(ctx, "INSERT INTO orders VALUES "+vals)
		require.NoError(t, err)
	}

	// Lineitem (100K rows in batches of 1000)
	flags := []string{"A", "N", "R"}
	statuses := []string{"F", "O"}
	for batch := 0; batch < 100; batch++ {
		vals := ""
		for i := 0; i < 1000; i++ {
			row := batch*1000 + i
			orderkey := int64(row + 1)
			linenum := 1
			if i > 0 {
				vals += ","
			}
			vals += fmt.Sprintf("(%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,'%s','%s','1995-06-17','1995-06-17','1995-06-17','DELIVER IN PERSON','TRUCK','comment')",
				orderkey, row%200000+1, row%100+1, linenum,
				float64(row%50)+1, float64(row%10000)+100.0, float64(row%11)*0.01, 0.02,
				flags[row%3], statuses[row%2])
		}
		_, err = conn.ExecContext(ctx, "INSERT INTO lineitem VALUES "+vals)
		require.NoError(t, err)
	}
}

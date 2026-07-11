// Copyright 2021 - 2022 Matrix Origin
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

package explain

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestSingleTableQueryPrune(t *testing.T) {
	cases := []struct {
		name         string
		sql          string
		wantTableCol []Entry[string, []string]
	}{
		{
			name: "Test01",
			sql:  "SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
			},
		},

		{
			name: "Test02",
			sql:  "SELECT N_NAME, N_REGIONKEY, 23 as a FROM NATION",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
			},
		},

		{
			name: "Test03",
			sql:  "SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
			},
		},

		{
			name: "Test04",
			sql:  "SELECT NATION.N_NAME FROM NATION",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name"},
				},
			},
		},

		{
			name: "Test05",
			sql:  "SELECT a.* FROM NATION a",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
				},
			},
		},

		{
			name: "Test06",
			sql:  "SELECT count(*) FROM NATION",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey"},
				},
			},
		},

		{
			name: "Test07",
			sql:  "SELECT count(*) FROM NATION group by N_NAME",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name"},
				},
			},
		},

		{
			name: "Test08",
			sql:  "SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
			},
		},

		{
			name: "Test09",
			sql:  "SELECT DISTINCT N_NAME FROM NATION limit 10",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name"},
				},
			},
		},

		{
			name: "Test10",
			sql:  "SELECT DISTINCT N_NAME FROM NATION",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name"},
				},
			},
		},

		{
			name: "Test11",
			sql:  "SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_regionkey"},
				},
			},
		},

		{
			name: "Test12",
			sql:  "SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_name", "n_regionkey"},
				},
			},
		},

		{
			name: "Test13",
			sql:  "SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_regionkey"},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mock := plan2.NewMockOptimizer(false)
			logicPlan, err := buildOneStmt(mock, t, c.sql)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			columns, err := getPrunedTableColumns(logicPlan)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			require.Equal(t, c.wantTableCol, columns)
		})
	}
}

func TestJoinQueryPrune(t *testing.T) {
	cases := []struct {
		name         string
		sql          string
		wantTableCol []Entry[string, []string]
	}{
		{
			name: "Test01",
			sql:  "SELECT NATION.N_NAME, REGION.R_NAME FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND NATION.N_NAME > REGION.R_NAME",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey", "r_name"},
				},
			},
		},
		{
			name: "Test02",
			sql:  "SELECT NATION.N_NAME, REGION.R_NAME FROM NATION left join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND NATION.N_NAME > REGION.R_NAME",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey", "r_name"},
				},
			},
		},
		{
			name: "Test03",
			sql:  "SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test04",
			sql:  "SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation2",
					colNames:  []string{"n_name", "r_regionkey"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test05",
			sql:  "SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE NATION2.R_REGIONKEY > 0",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation2",
					colNames:  []string{"n_name", "r_regionkey"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test06",
			sql:  "SELECT N_NAME FROM NATION NATURAL JOIN REGION",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test07",
			sql:  "SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_name", "n_regionkey"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test08",
			sql:  "SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey"},
				},
				{
					tableName: "orders",
					colNames:  []string{"o_orderkey", "o_custkey"},
				},
				{
					tableName: "lineitem",
					colNames:  []string{"l_orderkey"},
				},
			},
		},
		{
			name: "Test09",
			sql:  "SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"},
				},
				{
					tableName: "orders",
					colNames:  []string{"o_orderkey", "o_custkey"},
				},
				{
					tableName: "lineitem",
					colNames:  []string{"l_orderkey"},
				},
			},
		},
		{
			name: "Test10",
			sql:  "SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"},
				},
				{
					tableName: "orders",
					colNames:  []string{"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"},
				},
				{
					tableName: "lineitem",
					colNames:  []string{"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"},
				},
			},
		},
		{
			name: "Test11",
			sql:  "SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test12",
			sql:  "SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mock := plan2.NewMockOptimizer(false)
			logicPlan, err := buildOneStmt(mock, t, c.sql)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			columns, err := getPrunedTableColumns(logicPlan)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			require.Equal(t, c.wantTableCol, columns)
		})
	}
}

func TestNestedQueryPrune(t *testing.T) {

	cases := []struct {
		name         string
		sql          string
		wantTableCol []Entry[string, []string]
	}{
		{
			name: "Test01",
			sql:  "SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test02",
			sql:  "SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY = N_REGIONKEY)",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
				},
				{
					tableName: "region",
					colNames:  []string{"r_regionkey"},
				},
			},
		},
		{
			name: "Test03",
			sql:  "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#54' and p_container = 'LG BAG' and l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey)",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "lineitem",
					colNames:  []string{"l_partkey", "l_quantity", "l_extendedprice"},
				},
				{
					tableName: "part",
					colNames:  []string{"p_partkey", "p_brand", "p_container"},
				},
				{
					tableName: "lineitem",
					colNames:  []string{"l_partkey", "l_quantity"},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mock := plan2.NewMockOptimizer(false)
			logicPlan, err := buildOneStmt(mock, t, c.sql)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			columns, err := getPrunedTableColumns(logicPlan)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			require.Equal(t, c.wantTableCol, columns)
		})
	}

}

func TestDerivedTableQueryPrune(t *testing.T) {
	cases := []struct {
		name         string
		sql          string
		wantTableCol []Entry[string, []string]
	}{
		{
			name: "Test01",
			sql:  "select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey"},
				},
			},
		},
		{
			name: "Test02",
			sql:  "select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey", "c_nationkey"},
				},
			},
		},
		{
			name: "Test03",
			sql:  "select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey"},
				},
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_regionkey"},
				},
			},
		},
		{
			name: "Test04",
			sql:  "select a.* from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey", "c_nationkey"},
				},
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_regionkey"},
				},
			},
		},
		{
			name: "Test05",
			sql:  "select * from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
			wantTableCol: []Entry[string, []string]{
				{
					tableName: "customer",
					colNames:  []string{"c_custkey", "c_nationkey"},
				},
				{
					tableName: "nation",
					colNames:  []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mock := plan2.NewMockOptimizer(false)
			logicPlan, err := buildOneStmt(mock, t, c.sql)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			columns, err := getPrunedTableColumns(logicPlan)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			require.Equal(t, c.wantTableCol, columns)
		})
	}

}

func TestColumnPruneSemanticBoundaries(t *testing.T) {
	cases := []struct {
		name         string
		sql          string
		wantTableCol []Entry[string, []string]
	}{
		{
			name: "derived distinct keeps unprojected dedup keys",
			sql:  "select n_name from (select distinct n_name, n_comment from nation) d",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name", "n_comment"}},
			},
		},
		{
			name: "union all prunes unprojected columns",
			sql:  "select n_name from (select n_name, n_comment from nation union all select r_name, r_comment from region) u",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name"}},
				{tableName: "region", colNames: []string{"r_name"}},
			},
		},
		{
			name: "union all compacts a retained nonzero position",
			sql:  "select n_comment from (select n_name, n_comment from nation union all select r_name, r_comment from region) u",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_comment"}},
				{tableName: "region", colNames: []string{"r_comment"}},
			},
		},
		{
			name: "union distinct keeps unprojected dedup keys",
			sql:  "select n_name from (select n_name, n_comment from nation union select r_name, r_comment from region) u",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name", "n_comment"}},
				{tableName: "region", colNames: []string{"r_name", "r_comment"}},
			},
		},
		{
			name: "intersect keeps unprojected set keys",
			sql:  "select n_name from (select n_name, n_comment from nation intersect select r_name, r_comment from region) u",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name", "n_comment"}},
				{tableName: "region", colNames: []string{"r_name", "r_comment"}},
			},
		},
		{
			name: "grouped aggregate prunes unused aggregate input",
			sql:  "select c_custkey from (select c_custkey, count(c_comment) as cnt from customer group by c_custkey) g",
			wantTableCol: []Entry[string, []string]{
				{tableName: "customer", colNames: []string{"c_custkey"}},
			},
		},
		{
			name: "scalar aggregate compacts a retained nonzero position",
			sql:  "select total from (select count(c_comment) as cnt, sum(c_acctbal) as total from customer) g",
			wantTableCol: []Entry[string, []string]{
				{tableName: "customer", colNames: []string{"c_acctbal"}},
			},
		},
		{
			name: "grouped aggregate keeps unprojected grouping keys",
			sql:  "select n_regionkey from (select n_regionkey, n_name from nation group by n_regionkey, n_name) g",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name", "n_regionkey"}},
			},
		},
		{
			name: "unused window output prunes window-only inputs",
			sql:  "select n_name from (select n_name, row_number() over (partition by n_regionkey order by n_comment) as rn from nation) w",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name"}},
			},
		},
		{
			name: "used window output keeps partition and order inputs",
			sql:  "select rn from (select row_number() over (partition by n_regionkey order by n_comment) as rn from nation) w",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_regionkey", "n_comment"}},
			},
		},
		{
			name: "order sensitive aggregate retains input window order",
			sql:  "select n_regionkey, json_arrayagg(n_name) from (select n_regionkey, n_name, row_number() over (partition by n_regionkey order by n_comment) as rn from nation) w group by n_regionkey",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name", "n_regionkey", "n_comment"}},
			},
		},
		{
			name: "having keeps only its aggregate input",
			sql:  "select c_custkey from (select c_custkey, count(c_comment) as cnt, sum(c_acctbal) as total from customer group by c_custkey having sum(c_acctbal) > 0) g",
			wantTableCol: []Entry[string, []string]{
				{tableName: "customer", colNames: []string{"c_custkey", "c_acctbal"}},
			},
		},
		{
			name: "scalar aggregate keeps a row-producing carrier",
			sql:  "select 1 from (select sum(c_acctbal) as total from customer) g",
			wantTableCol: []Entry[string, []string]{
				{tableName: "customer", colNames: []string{"c_acctbal"}},
			},
		},
		{
			name: "having without aggregate keeps row semantics",
			sql:  "select 1 from nation having true",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_nationkey"}},
			},
		},
		{
			name: "window post filter keeps window inputs",
			sql:  "select n_name from (select n_name, row_number() over (partition by n_regionkey order by n_comment) as rn from nation) w where rn = 1",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name", "n_regionkey", "n_comment"}},
			},
		},
		{
			name: "unused first window does not retain its inputs",
			sql:  "select rn2 from (select row_number() over (partition by n_regionkey order by n_comment) as rn1, row_number() over (order by n_name) as rn2 from nation) w",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name"}},
			},
		},
		{
			name: "union all constant projection keeps one row carrier",
			sql:  "select 1 from (select n_name, n_comment from nation union all select r_name, r_comment from region) u",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name"}},
				{tableName: "region", colNames: []string{"r_name"}},
			},
		},
		{
			name: "order by below limit retains its input",
			sql:  "select n_name from (select n_name, n_comment from nation order by n_comment limit 10) d",
			wantTableCol: []Entry[string, []string]{
				{tableName: "nation", colNames: []string{"n_name", "n_comment"}},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mock := plan2.NewMockOptimizer(false)
			logicPlan, err := buildOneStmt(mock, t, c.sql)
			require.NoError(t, err)
			columns, err := getPrunedTableColumns(logicPlan)
			require.NoError(t, err)
			require.Equal(t, c.wantTableCol, columns)
		})
	}
}

func TestColumnPruneOperatorShape(t *testing.T) {
	t.Run("union all compacts output positions", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select n_name from (select n_name, n_comment from nation union all select r_name, r_comment from region) u",
		)
		require.NoError(t, err)

		var unionAll *plan.Node
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType == plan.Node_UNION_ALL {
				unionAll = node
				break
			}
		}
		require.NotNil(t, unionAll)
		require.Len(t, unionAll.ProjectList, 1)
	})

	t.Run("union all retains side-effecting positions", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select a from (select n_name as a, sleep(0) as side_effect from nation union all select r_name, sleep(0) from region) u",
		)
		require.NoError(t, err)

		var unionAll *plan.Node
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType == plan.Node_UNION_ALL {
				unionAll = node
				break
			}
		}
		require.NotNil(t, unionAll)
		require.Len(t, unionAll.ProjectList, 2)
	})

	t.Run("aggregate compacts having slots", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select c_custkey from (select c_custkey, count(c_comment) as cnt, sum(c_acctbal) as total from customer group by c_custkey having sum(c_acctbal) > 0) g",
		)
		require.NoError(t, err)

		var agg *plan.Node
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType == plan.Node_AGG && len(node.FilterList) > 0 {
				agg = node
				break
			}
		}
		require.NotNil(t, agg)
		require.Len(t, agg.AggList, 1)
		require.Equal(t, "sum", agg.AggList[0].GetF().Func.ObjName)
		col := agg.FilterList[0].GetF().Args[0].GetCol()
		require.NotNil(t, col)
		require.Equal(t, int32(-2), col.RelPos)
		require.Equal(t, int32(1), col.ColPos, "group column occupies slot 0; compacted sum occupies slot 1")
	})

	t.Run("aggregate retains side-effecting inputs", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select c_custkey from (select c_custkey, sum(sleep(0)) as side_effect from customer group by c_custkey) g",
		)
		require.NoError(t, err)

		var agg *plan.Node
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType == plan.Node_AGG {
				agg = node
				break
			}
		}
		require.NotNil(t, agg)
		require.Len(t, agg.AggList, 1)
	})

	t.Run("unused window operators are unreachable", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select n_name from (select n_name, row_number() over (partition by n_regionkey order by n_comment) as rn from nation) w",
		)
		require.NoError(t, err)
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			require.NotEqual(t, plan.Node_WINDOW, node.NodeType)
			require.NotEqual(t, plan.Node_PARTITION, node.NodeType)
		}
	})

	t.Run("unused window with side effects remains reachable", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select n_name from (select n_name, row_number() over (order by sleep(0)) as rn from nation) w",
		)
		require.NoError(t, err)

		windowFound := false
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType == plan.Node_WINDOW {
				windowFound = true
				break
			}
		}
		require.True(t, windowFound)
	})

	t.Run("order sensitive aggregate retains window without exposing its output", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select n_regionkey, json_arrayagg(n_name) from (select n_regionkey, n_name, row_number() over (partition by n_regionkey order by n_comment) as rn from nation) w group by n_regionkey",
		)
		require.NoError(t, err)

		windowFound := false
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType == plan.Node_WINDOW {
				windowFound = true
			}
			if node.NodeType == plan.Node_AGG {
				require.Len(t, node.ProjectList, 2)
			}
		}
		require.True(t, windowFound)
	})

	t.Run("discarded order sensitive carrier does not retain window order", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select 1 from (select json_arrayagg(n_name) from (select n_name, row_number() over (order by n_comment) as rn from nation) w) g",
		)
		require.NoError(t, err)

		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			require.NotEqual(t, plan.Node_WINDOW, node.NodeType)
			require.NotEqual(t, plan.Node_PARTITION, node.NodeType)
		}
	})

	t.Run("order sensitive aggregate retains stacked windows", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"select json_arrayagg(n_name) from (select n_name, row_number() over (order by n_regionkey) as rn1, row_number() over (order by n_comment) as rn2 from nation) w",
		)
		require.NoError(t, err)

		windowCount := 0
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType == plan.Node_WINDOW {
				windowCount++
			}
		}
		require.Equal(t, 2, windowCount)
	})

	t.Run("cte consumers expose only referenced columns", func(t *testing.T) {
		logicPlan, err := buildOneStmt(
			plan2.NewMockOptimizer(false),
			t,
			"with c as (select n_name, n_comment from nation) select x.n_name from c x join c y on x.n_name = y.n_name",
		)
		require.NoError(t, err)

		scanCount := 0
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType != plan.Node_TABLE_SCAN {
				continue
			}
			scanCount++
			require.Len(t, node.TableDef.Cols, 1)
			require.Equal(t, "n_name", node.TableDef.Cols[0].Name)
		}
		require.Equal(t, 2, scanCount)
	})
}

func reachablePlanNodes(query *plan.Query) []*plan.Node {
	visited := make(map[int32]struct{}, len(query.Nodes))
	nodes := make([]*plan.Node, 0, len(query.Nodes))
	var visit func(int32)
	visit = func(nodeID int32) {
		if _, ok := visited[nodeID]; ok {
			return
		}
		visited[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		nodes = append(nodes, node)
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, rootID := range query.Steps {
		visit(rootID)
	}
	return nodes
}

func buildOneStmt(opt plan2.Optimizer, t *testing.T, sql string) (*plan.Plan, error) {
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// this sql always return one stmt
	ctx := opt.CurrentContext()
	return plan2.BuildPlan(ctx, stmts[0], false)
}

type Entry[K any, V any] struct {
	tableName K
	colNames  V
}

// Get the remaining columns after the table is cropped
func getPrunedTableColumns(logicPlan *plan.Plan) ([]Entry[string, []string], error) {
	query := logicPlan.GetQuery()
	if query.StmtType != plan.Query_SELECT {
		return nil, moerr.NewNotSupported(context.TODO(), "SQL is not a DQL")
	}

	res := make([]Entry[string, []string], 0)
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_TABLE_SCAN {
			tableDef := node.TableDef
			tableName := strings.ToLower(tableDef.Name)

			columns := make([]string, 0)
			for _, col := range tableDef.Cols {
				columns = append(columns, strings.ToLower(col.Name))
			}
			entry := Entry[string, []string]{
				tableName: tableName,
				colNames:  columns,
			}
			res = append(res, entry)
		}
	}
	return res, nil
}

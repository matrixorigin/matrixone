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

package plan2

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

//only use in developing
func TestSingleSql(t *testing.T) {
	// sql := `SELECT * FROM (SELECT relname as Tables_in_mo FROM mo_tables WHERE reldatabase = 'mo') a`
	// sql := "SELECT nation2.* FROM nation2 natural join region"
	// sql := `select n_name, avg(N_REGIONKEY) t from NATION where n_name != 'a' group by n_name having avg(N_REGIONKEY) > 10 order by t limit 20`
	// sql := `select date_add('1997-12-31 23:59:59',INTERVAL 100000 SECOND)`
	sql := "select @str_var, @int_var, @bool_var, @@global.float_var, @@session.null_var"
	// sql := "select 18446744073709551500"
	// stmts, err := mysql.Parse(sql)
	// if err != nil {
	// 	t.Fatalf("%+v", err)
	// }
	// t.Logf("%+v", string(getJson(stmts[0], t)))

	mock := NewMockOptimizer()
	logicPlan, err := runOneStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

//Test Query Node Tree
// func TestNodeTree(t *testing.T) {
// 	type queryCheck struct {
// 		steps    []int32                    //steps
// 		nodeType map[int]plan.Node_NodeType //node_type in each node
// 		children map[int][]int32            //children in each node
// 	}

// 	// map[sql string]checkData
// 	nodeTreeCheckList := map[string]queryCheck{
// 		"SELECT -1": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_VALUE_SCAN,
// 			},
// 			children: nil,
// 		},
// 		"SELECT -1 from dual": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_VALUE_SCAN,
// 			},
// 			children: nil,
// 		},
// 		// one node
// 		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 			},
// 			children: nil,
// 		},
// 		// two nodes- SCAN + SORT
// 		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3 Order By N_REGIONKEY": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// two nodes- SCAN + AGG(group by)
// 		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3 Group By N_NAME": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		"select sum(n_nationkey) from nation": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		"select sum(n_nationkey) from nation order by sum(n_nationkey)": {
// 			steps: []int32{2},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 			},
// 		},
// 		// two nodes- SCAN + AGG(distinct)
// 		"SELECT distinct N_NAME FROM NATION": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// three nodes- SCAN + AGG(group by) + SORT
// 		"SELECT N_NAME, count(*) as ttl FROM NATION Group By N_NAME Order By ttl": {
// 			steps: []int32{2},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 			},
// 		},
// 		// three nodes - SCAN, SCAN, JOIN
// 		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY": {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 			},
// 		},
// 		// three nodes - SCAN, SCAN, JOIN  //use where for join condition
// 		"SELECT N_NAME, N_REGIONKEY FROM NATION, REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY": {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 				3: {2},
// 			},
// 		},
// 		// 5 nodes - SCAN, SCAN, JOIN, SCAN, JOIN  //join three table
// 		"SELECT l.L_ORDERKEY FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10": {
// 			steps: []int32{6},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 				4: plan.Node_TABLE_SCAN,
// 				5: plan.Node_JOIN,
// 				6: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 				3: {2},
// 				5: {3, 4},
// 				6: {5},
// 			},
// 		},
// 		// 6 nodes - SCAN, SCAN, JOIN, SCAN, JOIN, SORT  //join three table
// 		"SELECT l.L_ORDERKEY FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10 order by c.C_CUSTKEY": {
// 			steps: []int32{7},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 				4: plan.Node_TABLE_SCAN,
// 				5: plan.Node_JOIN,
// 				6: plan.Node_PROJECT,
// 				7: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 				3: {2},
// 				5: {3, 4},
// 				6: {5},
// 				7: {6},
// 			},
// 		},
// 		// 3 nodes  //Derived table
// 		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey) a where ff > 0": {
// 			steps: []int32{2},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 			},
// 		},
// 		// 4 nodes  //Derived table
// 		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey": {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_PROJECT,
// 				3: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 				3: {2},
// 			},
// 		},
// 		// Derived table join normal table
// 		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10 order By b.N_REGIONKEY": {
// 			steps: []int32{6},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_PROJECT,
// 				3: plan.Node_TABLE_SCAN,
// 				4: plan.Node_JOIN,
// 				5: plan.Node_PROJECT,
// 				6: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 				4: {2, 3},
// 				5: {4},
// 				6: {5},
// 			},
// 		},
// 		// insert from values
// 		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_VALUE_SCAN,
// 				1: plan.Node_INSERT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// insert from select
// 		"INSERT NATION SELECT * FROM NATION2": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_INSERT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// update
// 		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_UPDATE,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// delete
// 		"DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_DELETE,
// 			},
// 		},
// 		// uncorrelated subquery
// 		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN, //nodeid = 1  here is the subquery
// 				1: plan.Node_TABLE_SCAN, //nodeid = 0, here is SELECT * FROM NATION where N_REGIONKEY > [subquery]
// 			},
// 			children: map[int][]int32{},
// 		},
// 		// correlated subquery
// 		`SELECT * FROM NATION where N_REGIONKEY >
// 			(select avg(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY group by R_NAME)
// 		order by N_NATIONKEY`: {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN, //nodeid = 1  subquery node，so,wo pop it to top
// 				1: plan.Node_TABLE_SCAN, //nodeid = 0
// 				2: plan.Node_AGG,        //nodeid = 2  subquery node，so,wo pop it to top
// 				3: plan.Node_SORT,       //nodeid = 3
// 			},
// 			children: map[int][]int32{
// 				2: {1}, //nodeid = 2, have children(NodeId=1, position=0)
// 				3: {0}, //nodeid = 3, have children(NodeId=0, position=2)
// 			},
// 		},
// 		// cte
// 		`with tbl(col1, col2) as (select n_nationkey, n_name from nation) select * from tbl order by col2`: {
// 			steps: []int32{1, 3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_MATERIAL,
// 				2: plan.Node_MATERIAL_SCAN,
// 				3: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				3: {2},
// 			},
// 		},
// 	}

// 	// run test and check node tree
// 	for sql, check := range nodeTreeCheckList {
// 		mock := NewMockOptimizer()
// 		logicPlan, err := runOneStmt(mock, t, sql)
// 		query := logicPlan.GetQuery()
// 		if err != nil {
// 			t.Fatalf("%+v, sql=%v", err, sql)
// 		}
// 		if len(query.Steps) != len(check.steps) {
// 			t.Fatalf("run sql[%+v] error, root should be [%+v] but now is [%+v]", sql, check.steps, query.Steps)
// 		}
// 		for idx, step := range query.Steps {
// 			if step != check.steps[idx] {
// 				t.Fatalf("run sql[%+v] error, root should be [%+v] but now is [%+v]", sql, check.steps, query.Steps)
// 			}
// 		}
// 		for idx, typ := range check.nodeType {
// 			if idx >= len(query.Nodes) {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].NodeType not exist", sql, idx)
// 			}
// 			if query.Nodes[idx].NodeType != typ {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].NodeType should be [%+v] but now is [%+v]", sql, idx, typ, query.Nodes[idx].NodeType)
// 			}
// 		}
// 		for idx, children := range check.children {
// 			if idx >= len(query.Nodes) {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].NodeType not exist", sql, idx)
// 			}
// 			if !reflect.DeepEqual(query.Nodes[idx].Children, children) {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].Children should be [%+v] but now is [%+v]", sql, idx, children, query.Nodes[idx].Children)
// 			}
// 		}
// 	}
// }

//test single table plan building
func TestSingleTableSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer()

	// should pass
	sqls := []string{
		"SELECT '1900-01-01 00:00:00' + INTERVAL 2147483648 SECOND",
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC", //test alias
		"SELECT NATION.N_NAME FROM NATION",                                       //test alias
		"SELECT * FROM NATION",                                                   //test star
		"SELECT a.* FROM NATION a",                                               //test star
		"SELECT count(*) FROM NATION",                                            //test star
		"SELECT count(*) FROM NATION group by N_NAME",                            //test star
		"SELECT N_NAME, count(distinct N_REGIONKEY) FROM NATION group by N_NAME", //test distinct agg function
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10", //test agg
		"SELECT DISTINCT N_NAME FROM NATION", //test distinct
		"select sum(n_nationkey) as s from nation order by s",
		"select date_add(date '2001-01-01', interval 1 day) as a",
		"select date_sub(date '2001-01-01', interval '1' day) as a",
		"select date_add('2001-01-01', interval '1' day) as a",
		"select n_name, count(*) from nation group by n_name order by 2 asc",
		"select count(distinct 12)",
		"select nullif(n_name, n_comment), ifnull(n_comment, n_name) from nation",

		"select 18446744073709551500",
		"select 0xffffffffffffffff",
		"select 0xffff",

		"SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                            //test more expr
		// "SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY IN (1, 2)",  //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY NOT IN (1)", //test more expr
		"select N_REGIONKEY from nation group by N_REGIONKEY having abs(nation.N_REGIONKEY - 1) >10",

		"SELECT -1",
		"select date_add('1997-12-31 23:59:59',INTERVAL 100000 SECOND)",
		"select date_sub('1997-12-31 23:59:59',INTERVAL 2 HOUR)",
		"select @str_var, @int_var, @bool_var, @float_var, @null_var",
		"select @str_var, @@global.int_var, @@session.bool_var",
		"select n_name from nation where n_name != @str_var and n_regionkey > @int_var",
		"select n_name from nation where n_name != @@global.str_var and n_regionkey > @@session.int_var",
		"select distinct(n_name), ((abs(n_regionkey))) from nation",
		"SET @var = abs(-1), @@session.string_var = 'aaa'",
		"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME, N_REGIONKEY FROM table_not_exist",                   //table not exist
		"SELECT N_NAME, column_not_exist FROM NATION",                       //column not exist
		"SELECT N_NAME, N_REGIONKEY a FROM NATION ORDER BY cccc",            //column alias not exist
		"SELECT N_NAME, b.N_REGIONKEY FROM NATION a ORDER BY b.N_REGIONKEY", //table alias not exist
		"SELECT N_NAME FROM NATION WHERE ffff(N_REGIONKEY) > 0",             //function name not exist
		"SELECT NATION.N_NAME FROM NATION a",                                // mysql should error, but i don't think it is necesssary
		"select n_nationkey, sum(n_nationkey) from nation",
		"select n_name from nation where n_name != @not_exist_var",
		"SET @var = abs(a)", // can't use column
		"SET @var = avg(2)", // can't use agg function

		"SELECT DISTINCT N_NAME FROM NATION GROUP BY N_REGIONKEY", //test distinct with group by
		//"select 18446744073709551500",                             //over int64
		//"select 0xffffffffffffffff",                               //over int64
	}
	runTestShouldError(mock, t, sqls)
}

//test join table plan building
func TestJoinTableSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer()

	// should pass
	sqls := []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE NATION2.R_REGIONKEY > 0",
		"SELECT N_NAME FROM NATION NATURAL JOIN REGION",                                                                                                     //have no same column name but it's ok
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                    //test alias
		"SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10", //join three tables
		"SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                  //test star
		"SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                    //test star
		"SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                                   //test star
		"SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
		"SELECT N_NAME, R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY)",
		"select nation.n_name from nation join nation2 on nation.n_name !='a' join region on nation.n_regionkey = region.r_regionkey",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.NotExistColumn",                    //column not exist
		"SELECT N_NAME, R_REGIONKEY FROM NATION join REGION using(R_REGIONKEY)",                                              //column not exist
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE aaaaa.N_REGIONKEY > 0", //table alias not exist
		"select *", //No table used
	}
	runTestShouldError(mock, t, sqls)
}

//test derived table plan building
func TestDerivedTableSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		"select c_custkey from (select c_custkey from CUSTOMER ) a",
		"select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a",
		"select col1 from (select c_custkey from CUSTOMER group by c_custkey ) a(col1)",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey",
		"select col1 from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a(col1, col2) where col2 > 0 order by col1",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select a.* from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select * from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"select C_NAME from (select c_custkey from CUSTOMER) a",                               //column not exist
		"select c_custkey2222 from (select c_custkey from CUSTOMER group by c_custkey ) a",    //column not exist
		"select col1 from (select c_custkey from CUSTOMER group by c_custkey ) a(col1, col2)", //column length not match
		"select c_custkey from (select c_custkey from CUSTOMER group by c_custkey) a(col1)",   //column not exist
	}
	runTestShouldError(mock, t, sqls)
}

//test CTE plan building
func TestCTESqlBuilder(t *testing.T) {
	mock := NewMockOptimizer()

	// should pass
	sqls := []string{
		"WITH qn AS (SELECT * FROM nation) SELECT * FROM qn;",
		"WITH qn(a, b) AS (SELECT * FROM nation) SELECT * FROM qn;",
		"with qn0 as (select 1), qn1 as (select * from qn0), qn2 as (select 1), qn3 as (select 1 from qn1, qn2) select 1 from qn3",

		`WITH qn AS (select "outer" as a)
		SELECT (WITH qn AS (SELECT "inner" as a) SELECT a from qn),
		qn.a
		FROM qn`,
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		`with qn1 as (with qn3 as (select * from qn2) select * from qn3),
		qn2 as (select 1)
		select * from qn1`,

		`WITH qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0),
		qn AS (SELECT b as a FROM qn2)
		SELECT qn.a  FROM qn`,
	}
	runTestShouldError(mock, t, sqls)
}

func TestInsert(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		//"INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		//"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",
		"INSERT INTO NATION SELECT * FROM NATION2",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		//"INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), ('NAME2', 22, 'COMMENT2')",                                // doesn't match value count
		//"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 'NAME1'), (2, 22, 'NAME2')",                     // doesn't match value count
		//"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",             // column not exist
		//"INSERT NATION333 (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2, 'NAME1'), (2, 22, 'NAME2')",           // table not exist
		//"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 'should int32', 'NAME1'), (2, 22, 'NAME2')", // column type not match
		//"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2.22, 'NAME1'), (2, 22, 'NAME2')",           // column type not match
		//"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2, 'NAME1'), (2, 22, 'NAME2')",              // function expr not support now
		"INSERT INTO region SELECT * FROM NATION2",                                            // column length not match
		"INSERT INTO region SELECT 1, 2, 3, 4, 5, 6 FROM NATION2",                             // column length not match
		"INSERT NATION333 (N_NATIONKEY, N_REGIONKEY, N_NAME2222) SELECT 1, 2, 3 FROM NATION2", // table not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestUpdate(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		//"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		//"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20",
		//"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		//"UPDATE NATION SET N_NAME2 ='U1', N_REGIONKEY=2",    // column not exist
		//"UPDATE NATION2222 SET N_NAME ='U1', N_REGIONKEY=2", // table not exist
		// "UPDATE NATION SET N_NAME = 2, N_REGIONKEY=2",       // column type not match
		// "UPDATE NATION SET N_NAME = 'U1', N_REGIONKEY=2.2",  // column type not match
	}
	runTestShouldError(mock, t, sqls)
}

func TestDelete(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		//"DELETE FROM NATION",
		//"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		//"DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		//"DELETE FROM NATION2222",                     // table not exist
		//"DELETE FROM NATION WHERE N_NATIONKEY2 > 10", // column not found
	}
	runTestShouldError(mock, t, sqls)
}

func TestSubQuery(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)",                                 // unrelated
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY = N_REGIONKEY)", // related
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY)", // related
		//"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		`select
		sum(l_extendedprice) / 7.0 as avg_yearly
	from
		lineitem,
		part
	where
		p_partkey = l_partkey
		and p_brand = 'Brand#54'
		and p_container = 'LG BAG'
		and l_quantity < (
			select
				0.2 * avg(l_quantity)
			from
				lineitem
			where
				l_partkey = p_partkey
		);`, //tpch q17
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION222)",                                 // table not exist
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY222)", // column not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestTcl(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		"start transaction",
		"start transaction read write",
		"begin",
		"commit and chain",
		"commit and chain no release",
		"rollback and chain",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{}
	runTestShouldError(mock, t, sqls)
}

func TestDdl(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		"create database db_name",               //db not exists and pass
		"create database if not exists db_name", //db not exists but pass
		"create database if not exists tpch",    //db exists and pass
		"drop database if exists db_name",       //db not exists but pass
		"drop database tpch",                    //db exists, pass

		"create table tbl_name (t bool(20), b int unsigned, c char(20), d varchar(20), primary key(b), index idx_t(c)) comment 'test comment'",
		"create table if not exists tbl_name (b int default 20 primary key, c char(20) default 'ss', d varchar(20) default 'kkk')",
		"create table if not exists nation (t bool(20), b int, c char(20), d varchar(20))",
		"drop table if exists tbl_name",
		"drop table if exists nation",
		"drop table nation",
		"drop table tpch.nation",
		"drop table if exists tpch.tbl_not_exist",
		"drop table if exists db_not_exist.tbl",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	//sqls = []string{
	//	"create database tpch",  //we mock database tpch。 so tpch is exist
	//	"drop database db_name", //we mock database tpch。 so tpch is exist
	//	"create table nation (t bool(20), b int, c char(20), d varchar(20))",             //table exists in tpch
	//	"create table nation (b int primary key, c char(20) primary key, d varchar(20))", //Multiple primary key
	//	"drop table tbl_name",           //table not exists in tpch
	//	"drop table tpch.tbl_not_exist", //database not exists
	//	"drop table db_not_exist.tbl",   //table not exists
	//
	//	"create index idx1 using bsi on a(a)", //unsupport now
	//	"drop index idx1 on tbl",              //unsupport now
	//}
	//runTestShouldError(mock, t, sqls)
}

func TestShow(t *testing.T) {
	mock := NewMockOptimizer()
	// should pass
	sqls := []string{
		"show variables",
		//"show create database tpch",
		"show create table nation",
		"show create table tpch.nation",
		"show databases",
		"show databases like '%d'",
		"show databases where `Database` = '11'",
		"show databases where `Database` = '11' or `Database` = 'ddd'",
		"show tables",
		"show tables from tpch",
		"show tables like '%dd'",
		"show tables from tpch where Tables_in_tpch = 'aa' or Tables_in_tpch like '%dd'",
		"show columns from nation",
		"show columns from nation from tpch",
		"show columns from nation where `Field` like '%ff' or `Type` = 1 or `Null` = 0",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"show create database db_not_exist",                    //db no exist
		"show create table tpch.nation22",                      //table not exist
		"show databases where d ='a'",                          //Column not exist,  show databases only have one column named 'Database'
		"show databases where `Databaseddddd` = '11'",          //column not exist
		"show tables from tpch22222",                           //database not exist
		"show tables from tpch where Tables_in_tpch222 = 'aa'", //column not exist
		"show columns from nation_ddddd",                       //table not exist
		"show columns from nation_ddddd from tpch",             //table not exist
		"show columns from nation where `Field22` like '%ff'",  //column not exist

		"show index from nation", //unsupport now
		"show warnings",          //unsupport now
		"show errors",            //unsupport now
		"show status",            //unsupport now
		"show processlist",       //unsupport now
	}
	runTestShouldError(mock, t, sqls)
}

func TestResultColumns(t *testing.T) {
	mock := NewMockOptimizer()
	getColumns := func(sql string) []*ColDef {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("sql %s build plan error:%+v", sql, err)
		}
		return GetResultColumnsFromPlan(logicPlan)
	}

	returnNilSql := []string{
		"begin",
		"commit",
		"rollback",
		//"INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		//"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		//"DELETE FROM NATION",
		"create database db_name",
		"drop database tpch",
		"create table tbl_name (b int unsigned, c char(20))",
		"drop table nation",
	}
	for _, sql := range returnNilSql {
		columns := getColumns(sql)
		if columns != nil {
			t.Fatalf("sql:%+v, return columns should be nil", sql)
		}
	}

	returnColumnsSql := map[string]string{
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC":            "N_NAME,a",
		"select n_nationkey, sum(n_regionkey) from (select * from nation) sub group by n_nationkey": "n_nationkey,sum(n_regionkey)",
		"show variables": "Variable_name,Value",
		// "show create database tpch": "Database,Create Database",
		// "show create table nation":  "Table,Create Table",
		"show databases":           "Database",
		"show tables":              "Tables_in_tpch",
		"show columns from nation": "Field,Type,Null,Key,Default,Comment",
	}
	for sql, colsStr := range returnColumnsSql {
		cols := strings.Split(colsStr, ",")
		columns := getColumns(sql)
		if len(columns) != len(cols) {
			t.Fatalf("sql:%+v, return columns should be [%s]", sql, colsStr)
		}
		for idx, col := range cols {
			// now ast always change col_name to lower string. will be fixed soon
			if !strings.EqualFold(columns[idx].Name, col) {
				t.Fatalf("sql:%+v, return columns should be [%s]", sql, colsStr)
			}
		}
	}
}

func getJson(v any, t *testing.T) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		t.Logf("%+v", v)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "  ")
	if err != nil {
		t.Logf("%+v", v)
	}
	return out.Bytes()
}

func outPutPlan(logicPlan *Plan, toFile bool, t *testing.T) {
	var json []byte
	switch logicPlan.Plan.(type) {
	case *plan.Plan_Query:
		json = getJson(logicPlan.GetQuery(), t)
	case *plan.Plan_Tcl:
		json = getJson(logicPlan.GetTcl(), t)
	case *plan.Plan_Ddl:
		json = getJson(logicPlan.GetDdl(), t)
	}
	if toFile {
		err := ioutil.WriteFile("/tmp/mo_plan2_test.json", json, 0777)
		if err != nil {
			t.Logf("%+v", err)
		}
	} else {
		t.Logf(string(json))
	}
}

func runOneStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	stmts, err := mysql.Parse(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// this sql always return one stmt
	ctx := opt.CurrentContext()
	return BuildPlan(ctx, stmts[0])
}

func runTestShouldPass(opt Optimizer, t *testing.T, sqls []string, printJson bool, toFile bool) {
	for _, sql := range sqls {
		logicPlan, err := runOneStmt(opt, t, sql)
		if err != nil {
			t.Fatalf("%+v, sql=%v", err, sql)
		}
		if printJson {
			outPutPlan(logicPlan, toFile, t)
		}
	}
}

func runTestShouldError(opt Optimizer, t *testing.T, sqls []string) {
	for _, sql := range sqls {
		_, err := runOneStmt(opt, t, sql)
		if err == nil {
			t.Fatalf("should error, but pass: %v", sql)
		}
	}
}

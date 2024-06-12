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

package plan

import (
	"bytes"
	"context"
	"encoding/json"
	"go/constant"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkInsert(b *testing.B) {
	typ := types.T_varchar.ToType()
	typ.Width = 1024
	targetType := makePlan2Type(&typ)
	targetType.Width = 1024

	originStr := "0123456789"
	testExpr := tree.NewNumValWithType(constant.MakeString(originStr), originStr, false, tree.P_char)
	targetT := &plan.Expr{
		Typ: targetType,
		Expr: &plan.Expr_T{
			T: &plan.TargetType{},
		},
	}
	ctx := context.TODO()
	for i := 0; i < b.N; i++ {
		binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
		expr, err := binder.BindExpr(testExpr, 0, true)
		if err != nil {
			break
		}
		_, err = forceCastExpr2(ctx, expr, typ, targetT)
		if err != nil {
			break
		}
	}
}

// only use in developing
func TestSingleSQL(t *testing.T) {
	// sql := "INSERT INTO NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')"
	// sql := "insert into dept values (11, 'aa', 'bb')"
	// sql := "delete from dept where deptno > 10"
	// sql := "delete from nation where n_nationkey > 10"
	// sql := "delete nation, nation2 from nation join nation2 on nation.n_name = nation2.n_name"
	// sql := "update nation set n_name ='a' where n_nationkey > 10"
	// sql := "update dept set deptno = 11 where deptno = 10"
	sqls := []string{"prepare stmt1 from update nation set n_name = ? where n_nationkey = ?",
		"prepare stmt1 from insert into  nation values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE n_name=?"}
	mock := NewMockOptimizer(true)

	for _, sql := range sqls {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
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
// 		mock := NewMockOptimizer(false)
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

// test single table plan building
func TestSingleTableSQLBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)
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
		"SELECT DISTINCT N_NAME FROM NATION ORDER BY N_NAME", //test distinct with order by

		"prepare stmt1 from select * from nation",
		"prepare stmt1 from select * from nation where n_name = ?",
		"prepare stmt1 from 'select * from nation where n_name = ?'",
		"prepare stmt1 from 'insert into nation select * from nation2 where n_name = ?'",
		"prepare stmt1 from 'select * from nation where n_name = ?'",
		"prepare stmt1 from 'drop table if exists t1'",
		"prepare stmt1 from 'create table t1 (a int)'",
		"prepare stmt1 from select N_REGIONKEY from nation group by N_REGIONKEY having abs(nation.N_REGIONKEY - ?) > ?",
		"execute stmt1",
		"execute stmt1 using @str_var, @@global.int_var",
		"deallocate prepare stmt1",
		"drop prepare stmt1",
		"select count(n_name) from nation limit 10",
		"select l_shipdate + interval '1' day from lineitem",
		"select interval '1' day + l_shipdate  from lineitem",
		"select interval '1' day + cast('2022-02-02 00:00:00' as datetime)",
		"select cast('2022-02-02 00:00:00' as datetime) + interval '1' day",
		"select true is unknown",
		"select null is not unknown",
		"select 1 as c,  1/2, abs(-2)",

		"select date('2022-01-01'), adddate(time'00:00:00', interval 1 day), subdate(time'00:00:00', interval 1 week), '2007-01-01' + interval 1 month, '2007-01-01' -  interval 1 hour",
		"select 2222332222222223333333333333333333, 0x616263,-10, bit_and(2), bit_or(2), bit_xor(10.1), 'aaa' like '%a',str_to_date('04/31/2004', '%m/%d/%Y'),unix_timestamp(from_unixtime(2147483647))",
		"select max(n_nationkey) over  (partition by N_REGIONKEY) from nation",
		"select * from generate_series(1, 5) g",
		"prepare stmt1 from select * from nation where n_name like ? or n_nationkey > 10 order by 2 limit '10'",

		"values row(1,1), row(2,2), row(3,3) order by column_0 limit 2",
		"select * from (values row(1,1), row(2,2), row(3,3)) a (c1, c2)",
		"prepare stmt1 from select * from nation where n_name like ? or n_nationkey > 10 order by 2 limit '10' for update",
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
		"SET @var = abs(a)", // can't use column
		"SET @var = avg(2)", // can't use agg function

		"SELECT DISTINCT N_NAME FROM NATION GROUP BY N_REGIONKEY", //test distinct with group by
		"SELECT DISTINCT N_NAME FROM NATION ORDER BY N_REGIONKEY", //test distinct with order by
		"select count(n_name) from nation limit 10 for update",
		//"select 18446744073709551500",                             //over int64
		//"select 0xffffffffffffffff",                               //over int64
	}
	runTestShouldError(mock, t, sqls)
}

// test join table plan building
func TestJoinTableSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)

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
		"select * from nation, nation2, region",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.NotExistColumn",                    //column not exist
		"SELECT N_NAME, R_REGIONKEY FROM NATION join REGION using(R_REGIONKEY)",                                              //column not exist
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE aaaaa.N_REGIONKEY > 0", //table alias not exist
		"select *", //No table used
		"SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0 for update", //Not support
		"select * from nation, nation2, region for update",                                                         // Not support
	}
	runTestShouldError(mock, t, sqls)
}

// test derived table plan building
func TestDerivedTableSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)
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
		"select c_custkey from (select c_custkey from CUSTOMER ) a for update ",               //not support
	}
	runTestShouldError(mock, t, sqls)
}

// test derived table plan building
func TestUnionSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"(select 1) union (select 1)",
		"(((select n_nationkey from nation order by n_nationkey))) union (((select n_nationkey from nation order by n_nationkey)))",
		"select 1 union select 2",
		"select 1 union (select 2 union select 3)",
		"(select 1 union select 2) union select 3 intersect select 4 order by 1",
		"select 1 union select null",
		"select n_name from nation intersect select n_name from nation2",
		"select n_name from nation minus select n_name from nation2",
		"select 1 union select 2 intersect select 2 union all select 1.1 minus select 22222",
		"select 1 as a union select 2 order by a limit 1",
		"select n_name from nation union select n_comment from nation order by n_name",
		"with qn (foo, bar) as (select 1 as col, 2 as coll union select 4, 5) select qn1.bar from qn qn1",
		"select n_name, n_comment from nation union all select n_name, n_comment from nation2",
		"select n_name from nation intersect all select n_name from nation2",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"select 1 union select 2, 'a'",
		"select n_name as a from nation union select n_comment from nation order by n_name",
		"select n_name from nation minus all select n_name from nation2", // not support
	}
	runTestShouldError(mock, t, sqls)
}

// test CTE plan building
func TestCTESqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)

	// should pass
	sqls := []string{
		"WITH qn AS (SELECT * FROM nation) SELECT * FROM qn;",
		"with qn0 as (select 1), qn1 as (select * from qn0), qn2 as (select 1), qn3 as (select 1 from qn1, qn2) select 1 from qn3",

		`WITH qn AS (select "outer" as a)
		SELECT (WITH qn AS (SELECT "inner" as a) SELECT a from qn),
		qn.a
		FROM qn`,
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"WITH qn(a, b) AS (SELECT * FROM nation) SELECT * FROM qn;",
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
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"INSERT INTO NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		"INSERT INTO NATION (N_NATIONKEY, N_REGIONKEY, N_NAME, N_COMMENT) VALUES (1, 21, 'NAME1','comment1'), (2, 22, 'NAME2', 'comment2')",
		"INSERT INTO NATION SELECT * FROM NATION2",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), ('NAME2', 22, 'COMMENT2')",                                // doesn't match value count
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 'NAME1'), (2, 22, 'NAME2')",                     // doesn't match value count
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",             // column not exist
		"INSERT NATION333 (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2, 'NAME1'), (2, 22, 'NAME2')",           // table not exist
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 'should int32', 'NAME1'), (2, 22, 'NAME2')", // column type not match
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2.22, 'NAME1'), (2, 22, 'NAME2')",           // column type not match
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2, 'NAME1'), (2, 22, 'NAME2')",              // function expr not support now
		"INSERT INTO region SELECT * FROM NATION2",                                                                   // column length not match
		"INSERT INTO region SELECT 1, 2, 3, 4, 5, 6 FROM NATION2",                                                    // column length not match
		"INSERT NATION333 (N_NATIONKEY, N_REGIONKEY, N_NAME2222) SELECT 1, 2, 3 FROM NATION2",                        // table not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestUpdate(t *testing.T) {
	mock := NewMockOptimizer(true)
	// should pass
	sqls := []string{
		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"update NATION a join NATION2 b on a.N_REGIONKEY = b.R_REGIONKEY set a.N_NAME = 'aa'",
		"prepare stmt1 from 'update nation set n_name = ? where n_nationkey > ?'",
		"drop index idx1 on test_idx",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"UPDATE NATION SET N_NAME2 ='U1', N_REGIONKEY=2",    // column not exist
		"UPDATE NATION2222 SET N_NAME ='U1', N_REGIONKEY=2", // table not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestDelete(t *testing.T) {
	mock := NewMockOptimizer(true)
	// should pass
	sqls := []string{
		"DELETE FROM NATION",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
		"delete nation from nation left join nation2 on nation.n_nationkey = nation2.n_nationkey",
		"delete from nation",
		"delete nation, nation2 from nation join nation2 on nation.n_name = nation2.n_name",
		"prepare stmt1 from 'delete from nation where n_nationkey > ?'",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"DELETE FROM NATION2222",                     // table not exist
		"DELETE FROM NATION WHERE N_NATIONKEY2 > 10", // column not found
	}
	runTestShouldError(mock, t, sqls)
}

func TestSubQuery(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)",                                 // unrelated
		"SELECT * FROM NATION where N_REGIONKEY in (select max(R_REGIONKEY) from REGION)",                                // unrelated
		"SELECT * FROM NATION where N_REGIONKEY not in (select max(R_REGIONKEY) from REGION)",                            // unrelated
		"SELECT * FROM NATION where exists (select max(R_REGIONKEY) from REGION)",                                        // unrelated
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY = N_REGIONKEY)", // related
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
		"select * from nation where n_regionkey in (select r_regionkey from region) and n_nationkey not in (1,2) and n_nationkey = some (select n_nationkey from nation2)",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION222)",                                 // table not exist
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY222)", // column not exist
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY)",    // related
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION) for update",                         // not support
	}
	runTestShouldError(mock, t, sqls)
}

func TestMysqlCompatibilityMode(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"SELECT n_nationkey FROM NATION group by n_name",
		"SELECT n_nationkey, min(n_name) FROM NATION",
		"SELECT n_nationkey + 100 FROM NATION group by n_name",
	}
	// withou mysql compatibility
	runTestShouldError(mock, t, sqls)
	// with mysql compatibility
	mock.ctxt.mysqlCompatible = true
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestTcl(t *testing.T) {
	mock := NewMockOptimizer(false)
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
	mock := NewMockOptimizer(true)
	// should pass
	sqls := []string{
		"create database db_name",               //db not exists and pass
		"create database if not exists db_name", //db not exists but pass
		"create database if not exists tpch",    //db exists and pass
		"drop database if exists db_name",       //db not exists but pass
		"drop database tpch",                    //db exists, pass
		"create view v1 as select * from nation",

		"create table tbl_name (t bool(20) comment 'dd', b int unsigned, c char(20), d varchar(20), primary key(b), index idx_t(c)) comment 'test comment'",
		"create table if not exists tbl_name (b int default 20 primary key, c char(20) default 'ss', d varchar(20) default 'kkk')",
		"create table if not exists nation (t bool(20), b int, c char(20), d varchar(20))",
		"drop table if exists tbl_name",
		"drop table if exists nation",
		"drop table nation",
		"drop table tpch.nation",
		"drop table if exists tpch.tbl_not_exist",
		"drop table if exists db_not_exist.tbl",
		"drop view v1",
		"truncate nation",
		"truncate tpch.nation",
		"truncate table nation",
		"truncate table tpch.nation",
		"create unique index idx_name on nation(n_regionkey)",
		"create view v_nation as select n_nationkey,n_name,n_regionkey,n_comment from nation",
		"CREATE TABLE t1(id INT PRIMARY KEY,name VARCHAR(25),deptId INT,CONSTRAINT fk_t1 FOREIGN KEY(deptId) REFERENCES nation(n_nationkey)) COMMENT='xxxxx'",
		"create table t2(empno int unsigned,ename varchar(15),job varchar(10)) cluster by(empno,ename)",
		"lock tables nation read",
		"lock tables nation write, supplier read",
		"unlock tables",
		"alter table emp drop foreign key fk1",
		"alter table nation add FOREIGN KEY fk_t1(n_nationkey) REFERENCES nation2(n_nationkey)",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		// "create database tpch",  // check in pipeline now
		// "drop database db_name", // check in pipeline now
		// "create table nation (t bool(20), b int, c char(20), d varchar(20))",             // check in pipeline now
		"create table nation (b int primary key, c char(20) primary key, d varchar(20))", //Multiple primary key
		"drop table tbl_name",           //table not exists in tpch
		"drop table tpch.tbl_not_exist", //database not exists
		"drop table db_not_exist.tbl",   //table not exists
		"create table t6(empno int unsigned,ename varchar(15) auto_increment) cluster by(empno,ename)",
		"lock tables t3 read",
		"lock tables t1 read, t1 write",
		"lock tables nation read, nation write",
		"alter table nation drop foreign key fk1", //key not exists
		"alter table nation add FOREIGN KEY fk_t1(col_not_exist) REFERENCES nation2(n_nationkey)",
		"alter table nation add FOREIGN KEY fk_t1(n_nationkey) REFERENCES nation2(col_not_exist)",
		"create table agg01 (col1 int, col2 enum('egwjqebwq', 'qwewqewqeqewq', 'weueiwqeowqehwgqjhenw') primary key)",
	}
	runTestShouldError(mock, t, sqls)
}

func TestShow(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"show variables",
		//"show create database tpch",
		"show create table nation",
		"show create table tpch.nation",
		"show databases",
		"show databases like '%d'",
		"show databases where `database` = '11'",
		"show databases where `database` = '11' or `database` = 'ddd'",
		"show tables",
		"show tables from tpch",
		"show tables like '%dd'",
		"show tables from tpch where `tables_in_tpch` = 'aa' or `tables_in_tpch` like '%dd'",
		"show columns from nation",
		"show full columns from nation",
		"show columns from nation from tpch",
		"show full columns from nation from tpch",
		"show columns from nation where `field` like '%ff' or `type` = 1 or `null` = 0",
		"show full columns from nation where `field` like '%ff' or `type` = 1 or `null` = 0",
		"show create view v1",
		"show create table v1",
		"show table_number",
		"show table_number from tpch",
		"show column_number from nation",
		"show config",
		"show index from tpch.nation",
		"show locks",
		"show node list",
		"show grants for ROLE role1",
		"show function status",
		"show function status like '%ff'",
		"show snapshots",
		"show snapshots where SNAPSHOT_NAME = 'snapshot_07'",
		// "show procedure status",
		// "show procedure status like '%ff'",
		"show roles",
		"show roles like '%ff'",
		"show stages",
		"show stages like 'my_stage%'",
		// "show grants",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"show create database db_not_exist",                    //db no exist
		"show create table tpch.nation22",                      //table not exist
		"show create view vvv",                                 //view not exist
		"show databases where d ='a'",                          //Column not exist,  show databases only have one column named 'Database'
		"show databases where `Databaseddddd` = '11'",          //column not exist
		"show tables from tpch22222",                           //database not exist
		"show tables from tpch where Tables_in_tpch222 = 'aa'", //column not exist
		"show columns from nation_ddddd",                       //table not exist
		"show full columns from nation_ddddd",
		"show columns from nation_ddddd from tpch", //table not exist
		"show full columns from nation_ddddd from tpch",
		"show columns from nation where `Field22` like '%ff'", //column not exist
		"show full columns from nation where `Field22` like '%ff'",
		"show index from tpch.dddd",
		"show table_number from tpch222",
		"show column_number from nation222",
	}
	runTestShouldError(mock, t, sqls)
}

func TestResultColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	getColumns := func(sql string) []*ColDef {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("sql %s build plan error:%+v", sql, err)
		}
		return GetResultColumnsFromPlan(logicPlan)
	}

	returnNilSQL := []string{
		"begin",
		"commit",
		"rollback",
		"INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		// "UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		// "DELETE FROM NATION",
		//"create database db_name",
		//"drop database tpch",
		//"create table tbl_name (b int unsigned, c char(20))",
		//"drop table nation",
	}
	for _, sql := range returnNilSQL {
		columns := getColumns(sql)
		if columns != nil {
			t.Fatalf("sql:%+v, return columns should be nil", sql)
		}
	}

	returnColumnsSQL := map[string]string{
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC":            "N_NAME,a",
		"select n_nationkey, sum(n_regionkey) from (select * from nation) sub group by n_nationkey": "n_nationkey,sum(n_regionkey)",
		"show variables":            "Variable_name,Value",
		"show create database tpch": "Database,Create Database",
		"show create table nation":  "Table,Create Table",
		"show databases":            "Database",
		"show tables":               "Tables_in_tpch",
		"show columns from nation":  "Field,Type,Null,Key,Default,Extra,Comment",
	}
	for sql, colsStr := range returnColumnsSQL {
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

func TestResultColumns2(t *testing.T) {
	mock := NewMockOptimizer(true)
	getColumns := func(sql string) []*ColDef {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("sql %s build plan error:%+v", sql, err)
		}
		return GetResultColumnsFromPlan(logicPlan)
	}

	returnNilSQL := []string{
		"create database db_name",
		"drop database tpch",
		"create table tbl_name (b int unsigned, c char(20))",
		"drop table nation",
	}
	for _, sql := range returnNilSQL {
		columns := getColumns(sql)
		if columns != nil {
			t.Fatalf("sql:%+v, return columns should be nil", sql)
		}
	}

	returnColumnsSQL := map[string]string{
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC":            "N_NAME,a",
		"select n_nationkey, sum(n_regionkey) from (select * from nation) sub group by n_nationkey": "n_nationkey,sum(n_regionkey)",
		"show variables":            "Variable_name,Value",
		"show create database tpch": "Database,Create Database",
		"show create table nation":  "Table,Create Table",
		"show databases":            "Database",
		"show tables":               "Tables_in_tpch",
		"show columns from nation":  "Field,Type,Null,Key,Default,Extra,Comment",
	}
	for sql, colsStr := range returnColumnsSQL {
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

func TestBuildUnnest(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		`select * from unnest('{"a":1}') as f`,
		`select * from unnest('{"a":1}', '') as f`,
		`select * from unnest('{"a":1}', '$', true) as f`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
	errSqls := []string{
		`select * from unnest(t.t1.a)`,
		`select * from unnest(t.a, "$.b")`,
		`select * from unnest(t.a, "$.b", true)`,
		`select * from unnest(t.a) as f`,
		`select * from unnest(t.a, "$.b") as f`,
		`select * from unnest(t.a, "$.b", true) as f`,
		`select * from unnest('{"a":1}')`,
		`select * from unnest('{"a":1}', "$")`,
		`select * from unnest('{"a":1}', "", true)`,
	}
	runTestShouldError(mock, t, errSqls)
}

func TestVisitRule(t *testing.T) {
	sql := "select * from nation where n_nationkey > 10 or n_nationkey=@int_var or abs(-1) > 1"
	mock := NewMockOptimizer(false)
	ctx := context.TODO()
	plan, err := runOneStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
	getParamRule := NewGetParamRule()
	vp := NewVisitPlan(plan, []VisitPlanRule{getParamRule})
	err = vp.Visit(context.TODO())
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
	getParamRule.SetParamOrder()
	args := getParamRule.params

	resetParamOrderRule := NewResetParamOrderRule(args)
	vp = NewVisitPlan(plan, []VisitPlanRule{resetParamOrderRule})
	err = vp.Visit(ctx)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}

	params := []*Expr{
		makePlan2Int64ConstExprWithType(10),
	}
	resetParamRule := NewResetParamRefRule(ctx, params)
	resetVarRule := NewResetVarRefRule(&mock.ctxt, testutil.NewProc())
	constantFoldRule := NewConstantFoldRule(&mock.ctxt)
	vp = NewVisitPlan(plan, []VisitPlanRule{resetParamRule, resetVarRule, constantFoldRule})
	err = vp.Visit(ctx)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
}

func getJSON(v any, t *testing.T) []byte {
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

func testDeepCopy(logicPlan *Plan) {
	switch logicPlan.Plan.(type) {
	case *plan.Plan_Query:
		_ = DeepCopyPlan(logicPlan)
	case *plan.Plan_Ddl:
		_ = DeepCopyPlan(logicPlan)
	case *plan.Plan_Dcl:
	}
}

func outPutPlan(logicPlan *Plan, toFile bool, t *testing.T) {
	var json []byte
	switch logicPlan.Plan.(type) {
	case *plan.Plan_Query:
		json = getJSON(logicPlan.GetQuery(), t)
	case *plan.Plan_Tcl:
		json = getJSON(logicPlan.GetTcl(), t)
	case *plan.Plan_Ddl:
		json = getJSON(logicPlan.GetDdl(), t)
	case *plan.Plan_Dcl:
		json = getJSON(logicPlan.GetDcl(), t)
	}
	if toFile {
		err := os.WriteFile("/tmp/mo_plan_test.json", json, 0777)
		if err != nil {
			t.Logf("%+v", err)
		}
	} else {
		t.Logf(string(json))
	}
}

func runOneStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1, 0)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// this sql always return one stmt
	ctx := opt.CurrentContext()
	return BuildPlan(ctx, stmts[0], false)
}

func runTestShouldPass(opt Optimizer, t *testing.T, sqls []string, printJSON bool, toFile bool) {
	for _, sql := range sqls {
		logicPlan, err := runOneStmt(opt, t, sql)
		if err != nil {
			t.Fatalf("%+v, sql=%v", err, sql)
		}
		testDeepCopy(logicPlan)
		if printJSON {
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

func Test_mergeContexts(t *testing.T) {
	b1 := NewBinding(0, 1, "db", "a", 0, nil, nil, nil, false, nil)
	bc1 := NewBindContext(nil, nil)
	bc1.bindings = append(bc1.bindings, b1)

	b2 := NewBinding(1, 2, "db", "a", 0, nil, nil, nil, false, nil)
	bc2 := NewBindContext(nil, nil)
	bc2.bindings = append(bc2.bindings, b2)

	bc := NewBindContext(nil, nil)

	//a merge a
	err := bc.mergeContexts(context.Background(), bc1, bc2)
	assert.Error(t, err)
	assert.EqualError(t, err, "invalid input: table 'a' specified more than once")

	//a merge b
	b3 := NewBinding(2, 3, "db", "b", 0, nil, nil, nil, false, nil)
	bc3 := NewBindContext(nil, nil)
	bc3.bindings = append(bc3.bindings, b3)

	err = bc.mergeContexts(context.Background(), bc1, bc3)
	assert.NoError(t, err)

	// a merge a, ctx is  nil
	var ctx context.Context
	err = bc.mergeContexts(ctx, bc1, bc2)
	assert.Error(t, err)
	assert.EqualError(t, err, "invalid input: table 'a' specified more than once")
}

func Test_limitUint64(t *testing.T) {
	sqls := []string{
		"select * from t1 limit 0, 18446744073709551615",
		"select * from t1 limit 18446744073709551615, 18446744073709551615",
		"SELECT IFNULL(CAST(@var AS BIGINT UNSIGNED), 1)",
	}
	testutil.NewProc()
	mock := NewMockOptimizer(false)

	for _, sql := range sqls {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

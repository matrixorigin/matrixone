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
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

//Test Query Node Tree
func TestNodeTree(t *testing.T) {
	mock := newMockOptimizer()
	type queryCheck struct {
		root     int32                      //root node index
		nodeType map[int]plan.Node_NodeType //node_type in each node
		children map[int][]int32            //children in each node
	}

	// map[sql string]checkData
	nodeTreeCheckList := map[string]queryCheck{
		//one node
		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3": {
			root: 0,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
			},
			children: nil,
		},
		//two nodes- SCAN + SORT
		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3 Order By N_REGIONKEY": {
			root: 1,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_SORT,
			},
			children: map[int][]int32{
				1: {0},
			},
		},
		//two nodes- SCAN + AGG(group by)
		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3 Group By N_NAME": {
			root: 1,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_AGG,
			},
			children: map[int][]int32{
				1: {0},
			},
		},
		//two nodes- SCAN + AGG(distinct)
		"SELECT distinct N_NAME FROM NATION": {
			root: 1,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_AGG,
			},
			children: map[int][]int32{
				1: {0},
			},
		},
		//3 nodes- SCAN + AGG(group by) + AGG(distinct)
		"SELECT distinct N_REGIONKEY FROM NATION GROUP BY N_NAME": {
			root: 2,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_AGG,
				2: plan.Node_AGG,
			},
			children: map[int][]int32{
				1: {0},
				2: {1},
			},
		},
		//three nodes- SCAN + AGG(group by) + SORT
		"SELECT N_NAME, count(*) as ttl FROM NATION Group By N_NAME Order By ttl": {
			root: 2,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_AGG,
				2: plan.Node_SORT,
			},
			children: map[int][]int32{
				1: {0},
				2: {1},
			},
		},
		//three nodes - SCAN, SCAN, JOIN
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY": {
			root: 2,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_TABLE_SCAN,
				2: plan.Node_JOIN,
			},
			children: map[int][]int32{
				2: {0, 1},
			},
		},
		//three nodes - SCAN, SCAN, JOIN  //use where for join condition
		"SELECT N_NAME, N_REGIONKEY FROM NATION, REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY": {
			root: 2,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_TABLE_SCAN,
				2: plan.Node_JOIN,
			},
			children: map[int][]int32{
				2: {0, 1},
			},
		},
		//5 nodes - SCAN, SCAN, JOIN, SCAN, JOIN  //join three table
		"SELECT l.L_ORDERKEY FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERDATE < 10": {
			root: 4,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_TABLE_SCAN,
				2: plan.Node_JOIN,
				3: plan.Node_TABLE_SCAN,
				4: plan.Node_JOIN,
			},
			children: map[int][]int32{
				2: {0, 1},
				4: {2, 3},
			},
		},
		//6 nodes - SCAN, SCAN, JOIN, SCAN, JOIN, SORT  //join three table
		"SELECT l.L_ORDERKEY FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERDATE < 10 order by c.C_CUSTKEY": {
			root: 5,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_TABLE_SCAN,
				2: plan.Node_JOIN,
				3: plan.Node_TABLE_SCAN,
				4: plan.Node_JOIN,
				5: plan.Node_SORT,
			},
			children: map[int][]int32{
				2: {0, 1},
				4: {2, 3},
				5: {4},
			},
		},
		//3 nodes  //Derived table
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey) a where ff > 0": {
			root: 2,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_AGG,
				2: plan.Node_PROJECT,
			},
			children: map[int][]int32{
				1: {0},
				2: {1},
			},
		},
		//4 nodes  //Derived table
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey": {
			root: 3,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_AGG,
				2: plan.Node_PROJECT,
				3: plan.Node_SORT,
			},
			children: map[int][]int32{
				1: {0},
				2: {1},
				3: {2},
			},
		},
		//Derived table join normal table
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10 order By b.N_REGIONKEY": {
			root: 4,
			nodeType: map[int]plan.Node_NodeType{
				0: plan.Node_TABLE_SCAN,
				1: plan.Node_AGG,
				2: plan.Node_TABLE_SCAN,
				3: plan.Node_JOIN,
				4: plan.Node_SORT,
			},
			children: map[int][]int32{
				1: {0},
				3: {1, 2},
				4: {3},
			},
		},
	}

	//run test and check node tree
	for sql, check := range nodeTreeCheckList {
		query, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if query.Steps[0] != check.root {
			t.Fatalf("run sql[%+v] error, query.Steps[0] should be [%+v] but now is [%+v]", sql, check.root, query.Steps[0])
		}
		for idx, typ := range check.nodeType {
			if idx >= len(query.Nodes) {
				t.Fatalf("query.Nodes[%+v].NodeType not exist", idx)
			}
			if query.Nodes[idx].NodeType != typ {
				t.Fatalf("query.Nodes[%+v].NodeType should be [%+v] but now is [%+v]", idx, typ, query.Nodes[idx].NodeType)
			}
		}
		for idx, children := range check.children {
			if idx >= len(query.Nodes) {
				t.Fatalf("query.Nodes[%+v].NodeType not exist", idx)
			}
			if !reflect.DeepEqual(query.Nodes[idx].Children, children) {
				t.Fatalf("query.Nodes[%+v].Children should be [%+v] but now is [%+v]", idx, children, query.Nodes[idx].Children)
			}
		}
	}
}

//only use in developing
func TestSingleSql(t *testing.T) {
	mock := newMockOptimizer()
	sql := "SELECT count(N_NAME) FROM NATION"
	// sql := "select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10  order By b.N_REGIONKEY"
	// sql := "SELECT N_NAME, abs(N_REGIONKEY) a FROM NATION WHERE abs(N_REGIONKEY) > 0 ORDER BY a DESC"
	// stmts, _ := mysql.Parse(sql)
	// t.Logf("%+v", string(getJson(stmts[0], t)))

	query, err := runOneStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutQuery(query, true, t)
}

//test single table plan building
func TestSingleTableSqlBuilder(t *testing.T) {
	mock := newMockOptimizer()

	// should pass
	sqls := []string{
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE abs(N_REGIONKEY) > 0 ORDER BY a DESC", //test alias
		"SELECT NATION.N_NAME FROM NATION",            // test alias
		"SELECT * FROM NATION",                        //test star
		"SELECT a.* FROM NATION a",                    //test star
		"SELECT count(*) FROM NATION",                 //test star
		"SELECT count(*) FROM NATION group by N_NAME", //test star
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10", //test agg
		"SELECT DISTINCT N_NAME FROM NATION",                      //test distinct
		"SELECT DISTINCT N_NAME FROM NATION GROUP BY N_REGIONKEY", //test distinct with group by

		"SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                            //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",                                                                                 //test more expr
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME, N_REGIONKEY FROM table_not_exist",                   //table not exist
		"SELECT N_NAME, column_not_exist FROM NATION",                       //column not exist
		"SELECT N_NAME, N_REGIONKEY a FROM NATION ORDER BY cccc",            //column alias not exist
		"SELECT N_NAME, b.N_REGIONKEY FROM NATION a ORDER BY b.N_REGIONKEY", //table alias not exist
		"SELECT N_NAME FROM NATION WHERE absTTTT(N_REGIONKEY) > 0",          //function name not exist
		"SELECT NATION.N_NAME FROM NATION a",                                // mysql should error, but i don't think it is necesssary
	}
	runTestShouldError(mock, t, sqls)
}

//test jion table plan building
func TestJoinTableSqlBuilder(t *testing.T) {
	mock := newMockOptimizer()

	//should pass
	sqls := []string{
		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE abs(NATION.N_REGIONKEY) > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE abs(NATION2.R_REGIONKEY) > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE abs(NATION2.R_REGIONKEY) > 0",
		"SELECT N_NAME FROM NATION NATURAL JOIN REGION",                                                                                                           //have no same column name but it's ok
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",                                     //test alias
		"SELECT abs(l.L_ORDERKEY) a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERDATE < 10", //join three tables
		"SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                        //test star
		"SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                          //test star
		"SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",                                                    //test star
		"SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",                                                      //test star
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.NotExistColumn",                         //column not exist
		"SELECT N_NAME, R_REGIONKEY FROM NATION join REGION using(R_REGIONKEY)",                                                   //column not exist
		"SELECT N_NAME, R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY)",                                                  //R_REGIONKEY is  ambiguous
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(aaaaa.N_REGIONKEY) > 0", //table alias not exist
	}
	runTestShouldError(mock, t, sqls)
}

//test derived table plan building
func TestDerivedTableSqlBuilder(t *testing.T) {
	mock := newMockOptimizer()
	//should pass
	sqls := []string{
		"select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select a.* from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select * from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"select c_custkey2222 from (select c_custkey from CUSTOMER group by c_custkey ) a", //column not exist
	}
	runTestShouldError(mock, t, sqls)
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

func outPutQuery(query *Query, toFile bool, t *testing.T) {
	json := getJson(query, t)
	if toFile {
		err := ioutil.WriteFile("/tmp/mo_plan2_test.json", json, 0777)
		if err != nil {
			t.Logf("%+v", err)
		}
	} else {
		t.Logf(string(json))
	}
}

func runOneStmt(opt Optimizer, t *testing.T, sql string) (*Query, error) {
	stmts, err := mysql.Parse(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	//this sql always return one stmt
	return opt.Optimize(stmts[0])
}

func runTestShouldPass(opt Optimizer, t *testing.T, sqls []string, printJson bool, toFile bool) {
	for _, sql := range sqls {
		query, err := runOneStmt(opt, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if printJson {
			outPutQuery(query, toFile, t)
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

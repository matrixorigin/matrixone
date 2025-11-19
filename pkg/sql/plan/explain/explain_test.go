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
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func TestSingleSql(t *testing.T) {
	// input := "explain verbose SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20"
	//input := "explain verbose SELECT N_REGIONKEY + 2 as a FROM NATION WHERE -N_NATIONKEY < -20"
	// input := "explain verbose select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a"
	// input := "explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA'"
	// input := "explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10"
	//input := "explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10"
	//input := "explain verbose select * from part where p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')"
	//input := "explain select abs(N_REGIONKEY) from NATION"
	//input := "explain verbose SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10"
	//input := "explain verbose update emp set sal = sal + 500, comm = 1200 where deptno = 10"
	input := "explain verbose select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) when p_type like 'PRX%' then l_extendedprice * (2 - l_discount) else 0 end from lineitem,part where l_shipdate < date '1996-04-01' + interval '1' month"
	mock := plan.NewMockOptimizer(true)
	err := runOneStmt(mock, t, input)
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestBasicSqlExplain(t *testing.T) {
	sqls := []string{
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC",
		"explain verbose SELECT N_NAME,N_REGIONKEY, 23 as a FROM NATION",
		"explain verbose SELECT N_NAME, N_REGIONKEY, 23 as a FROM NATION",
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10",
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10",
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10",
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10",
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10 ORDER BY N_NAME, N_REGIONKEY DESC",
		"explain verbose SELECT count(*) FROM NATION group by N_NAME",
		"explain verbose SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY limit 10",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10 offset 20",
		"explain verbose select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) when p_type like 'PRX%' then l_extendedprice * (2 - l_discount) else 0 end from lineitem,part where l_shipdate < date '1996-04-01' + interval '1' month",
		"explain verbose select column_2 from (values row(0, 1, cast('[3, 4, 5]' as vecf32(3))))",
	}
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Single table query
func TestSingleTableQuery(t *testing.T) {
	sqls := []string{
		"explain verbose SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC",           //test alias
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC",                   //test alias
		"explain verbose SELECT NATION.N_NAME FROM NATION",                                                         // test alias
		"explain SELECT NATION.N_NAME FROM NATION",                                                                 // test alias
		"explain verbose SELECT * FROM NATION",                                                                     //test star
		"explain SELECT * FROM NATION",                                                                             //test star
		"explain verbose SELECT a.* FROM NATION a",                                                                 //test star
		"explain SELECT a.* FROM NATION a",                                                                         //test star
		"explain verbose SELECT count(*) FROM NATION",                                                              //test star
		"explain SELECT count(*) FROM NATION",                                                                      //test star
		"explain verbose SELECT count(*) FROM NATION group by N_NAME",                                              //test star
		"explain SELECT count(*) FROM NATION group by N_NAME",                                                      //test star
		"explain verbose SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10", //test agg
		"explain SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",         //test agg
		"explain verbose SELECT DISTINCT N_NAME FROM NATION limit 10",
		"explain verbose SELECT DISTINCT N_NAME FROM NATION", //test distinct
		"explain SELECT DISTINCT N_NAME FROM NATION",         //test distinct
		"explain verbose SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"explain SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20",         //test more expr
		"explain verbose SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"explain SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                            //test more expr
		"explain verbose SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                            //test more expr
		"explain SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                                    //test more expr
		// "explain verbose SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",
		// "explain SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",
	}
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Join query
func TestJoinQuery(t *testing.T) {
	sqls := []string{
		"explain SELECT NATION.N_NAME, REGION.R_NAME FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND NATION.N_NAME > REGION.R_NAME",
		"explain verbose SELECT NATION.N_NAME, REGION.R_NAME FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND NATION.N_NAME > REGION.R_NAME",
		"explain SELECT NATION.N_NAME, REGION.R_NAME FROM NATION left join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND NATION.N_NAME > REGION.R_NAME",
		"explain verbose SELECT NATION.N_NAME, REGION.R_NAME FROM NATION left join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND NATION.N_NAME > REGION.R_NAME",
		// "explain SELECT l.l_orderkey FROM customer c, orders o, lineitem l WHERE c.c_custkey = o.o_custkey and l.l_orderkey = o.o_orderkey and o.o_orderdate < current_date",
		"explain verbose SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"explain verbose SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
		"explain SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
		"explain verbose SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE NATION2.R_REGIONKEY > 0",
		"explain SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE NATION2.R_REGIONKEY > 0",
		"explain verbose SELECT N_NAME FROM NATION NATURAL JOIN REGION", //have no same column name but it's ok
		"explain SELECT N_NAME FROM NATION NATURAL JOIN REGION",
		"explain verbose SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                    //test alias
		"explain SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                            //test alias
		"explain verbose SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10", //join three tables
		"explain SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10",         //join three tables
		"explain verbose SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                  //test star
		"explain SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",
		"explain verbose SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY", //test star
		"explain SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",         //test star
		"explain verbose SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                //test star
		"explain SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
		"explain verbose SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
		"explain SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
	}
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Nested query <no pass>
func TestNestedQuery(t *testing.T) {
	sqls := []string{
		"explain verbose SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)",
		//"explain SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY)",
		`explain verbose select
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
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Test Derived Table Query
func TestDerivedTableQuery(t *testing.T) {
	sqls := []string{
		"explain select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a",
		"explain verbose select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a",
		"explain select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey",
		"explain verbose select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey",
		"explain select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"explain verbose select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"explain select a.* from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"explain verbose select a.* from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"explain select * from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"explain verbose select * from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
	}
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Collection query
func TestCollectionQuery(t *testing.T) {
	sqls := []string{
		"explain verbose select 2 intersect select 2 union all select 22222",
		"explain verbose select 1 union select 2",
		"explain verbose select 1 union (select 2 union select 3)",
		"explain verbose (select 1 union select 2) union select 3 intersect select 4 order by 1",
		"explain verbose select 1 union select null",
		"explain verbose select n_name from nation intersect select n_name from nation2",
		"explain verbose select n_name from nation minus select n_name from nation2",
		"explain verbose select 1 union select 2 intersect select 2 union all select 1.1 minus select 22222",
		"explain verbose select 1 as a union select 2 order by a limit 1",
		"explain verbose select n_name from nation union select n_comment from nation order by n_name",
		"explain verbose with qn (foo, bar) as (select 1 as col, 2 as coll union select 4, 5) select qn1.bar from qn qn1",
		"explain verbose select n_name, n_comment from nation union all select n_name, n_comment from nation2",
		"explain verbose select n_name from nation intersect all select n_name from nation2",
		"explain verbose SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPINSTRUCT='DELIVER IN PERSON' UNION SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPMODE='AIR' OR  l.L_SHIPMODE='AIR REG'",
		"explain verbose SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPMODE IN ('AIR','AIR REG') EXCEPT SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPINSTRUCT='DELIVER IN PERSON'",
	}
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestDMLInsert(t *testing.T) {
	sqls := []string{
		//"explain INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		//"explain verbose INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		//"explain INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",
		//"explain verbose INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",
		"explain INSERT INTO NATION SELECT * FROM NATION2",
		"explain verbose INSERT INTO NATION SELECT * FROM NATION2",
		"explain verbose insert ignore into nation select * from nation2",
		"explain verbose insert into nation select * from nation2 on duplicate key update n_comment = n_name",
	}
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestDMLUpdate(t *testing.T) {
	sqls := []string{
		"explain UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		"explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		"explain UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"explain UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
	}
	mockOptimizer := plan.NewMockOptimizer(true)
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestDMLDelete(t *testing.T) {
	sqls := []string{
		"explain DELETE FROM NATION",
		"explain verbose DELETE FROM NATION",
		"explain DELETE FROM NATION WHERE N_NATIONKEY > 10",
		"explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10",
		"explain DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
		"explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
		"explain verbose DELETE FROM a1, a2 USING NATION AS a1 INNER JOIN NATION2 AS a2 WHERE a1.N_NATIONKEY=a2.N_NATIONKEY",
		"explain verbose UPDATE NATION,REGION set NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE REGION.R_NAME = 'AAA'",
		"explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"explain verbose UPDATE NATION,NATION2 SET NATION.N_NAME ='U1',NATION2.N_NATIONKEY=15 WHERE NATION.N_NATIONKEY = NATION2.N_NATIONKEY",
	}
	mockOptimizer := plan.NewMockOptimizer(true)
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestSystemVariableAndUserVariable(t *testing.T) {
	sqls := []string{
		"explain verbose select @@autocommit from NATION",
		"explain verbose select @@global.autocommit from NATION",
		"explain verbose select @@session.autocommit from NATION",
		"explain verbose select @@autocommit,N_NAME, N_REGIONKEY from NATION",
		"explain verbose select @@global.autocommit,N_NAME, N_REGIONKEY from NATION",
		"explain verbose select @@session.autocommit,N_NAME, N_REGIONKEY from NATION",
		"explain verbose select @val from NATION",
		"explain verbose select @val,@a,@b from NATION",
		"explain verbose select @val,N_NAME, N_REGIONKEY from NATION",
		"explain verbose select @@session.autocommit,@val from NATION",
		"explain verbose select @@session.autocommit,@val,N_NAME from NATION",
	}
	mockOptimizer := plan.NewMockOptimizer(false)
	runTestShouldPass(mockOptimizer, t, sqls)
}

// test index table
func TestSingleTableDeleteSQL(t *testing.T) {
	sqls := []string{
		"explain verbose DELETE FROM emp where sal > 2000",
		"explain verbose delete from emp t1 where t1.sal > 2000",
		"explain verbose delete from emp where empno > 3000",
		"explain verbose delete from emp where ename = 'SMITH'",
		"explain verbose delete from dept where deptno = 10",
		"explain verbose delete from dept where dname = 'RESEARCH'",
		"explain verbose delete from dept where deptno = 10 order by dname limit 1",
		"explain verbose delete from emp where deptno = 20 order by sal limit 2",
		"explain verbose delete from emp where empno > 7800 order by empno limit 2",
	}
	mockOptimizer := plan.NewMockOptimizer(true)
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Composite unique index
func TestCompositeUniqueIndexTableDeleteSQL(t *testing.T) {
	sqls := []string{
		"explain verbose delete from employees where sal > 2000",
		"explain verbose delete from employees t1 where t1.sal > 2000",
		"explain verbose delete from employees where empno > 3000",
		"explain verbose delete from employees where ename = 'SMITH'",
		"explain verbose delete from employees where empno = 7698",
		"explain verbose delete from employees where empno = 7698 and ename = 'BLAKE'",
		"explain verbose delete from employees where deptno = 20 order by sal limit 2",
		"explain verbose delete from employees where empno > 7800 order by empno limit 2",
		"explain verbose delete employees, dept from employees, dept where employees.deptno = dept.deptno and sal > 2000",
		"explain verbose DELETE FROM employees, dept USING employees INNER JOIN dept WHERE employees.deptno = dept.deptno",
	}
	mockOptimizer := plan.NewMockOptimizer(true)
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestMultiTableDeleteSQL(t *testing.T) {
	sqls := []string{
		"explain verbose delete emp,dept from emp ,dept where emp.deptno = dept.deptno and emp.deptno = 10",
		"explain verbose delete emp,dept from emp ,dept where emp.deptno = dept.deptno and sal > 2000",
		"explain verbose delete t1,t2  from emp as t1,dept as t2 where t1.deptno = t2.deptno and t1.deptno = 10",
		"explain verbose delete t1,dept from emp as t1,dept where t1.deptno = dept.deptno and t1.deptno = 10",
		"explain verbose delete emp,dept from emp ,dept where emp.deptno = dept.deptno and empno > 7800",
		"explain verbose delete emp,dept from emp ,dept where emp.deptno = dept.deptno and empno = 7839",
		"explain verbose DELETE FROM emp, dept USING emp INNER JOIN dept WHERE emp.deptno = dept.deptno",
	}
	mockOptimizer := plan.NewMockOptimizer(true)
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestGetUpdateCtxInfo(t *testing.T) {
	// Create a test node with update context containing partition columns
	node := &plan2.Node{
		UpdateCtxList: []*plan2.UpdateCtx{
			{
				TableDef: &plan.TableDef{
					Name: "test_table",
				},
				PartitionCols: []plan.ColRef{
					{
						RelPos: 0,
						ColPos: 0,
						Name:   "partition_col1",
					},
					{
						RelPos: 0,
						ColPos: 1,
						Name:   "partition_col2",
					},
				},
			},
		},
	}

	// Create node description implementation
	ndesc := NewNodeDescriptionImpl(node)

	// Create explain options
	options := &ExplainOptions{
		Format: EXPLAIN_FORMAT_TEXT,
	}

	// Get update context info
	lines, err := ndesc.GetUpdateCtxInfo(context.Background(), options)
	if err != nil {
		t.Fatalf("GetUpdateCtxInfo failed: %v", err)
	}

	// Verify the output
	if len(lines) != 1 {
		t.Fatalf("Expected 1 line of output, got %d", len(lines))
	}

	expectedOutput := "Table: test_table Partition Columns: partition_col1, partition_col2"
	if lines[0] != expectedOutput {
		t.Fatalf("Expected output: %s\nGot: %s", expectedOutput, lines[0])
	}
}

func runTestShouldPass(opt plan.Optimizer, t *testing.T, sqls []string) {
	for _, sql := range sqls {
		err := runOneStmt(opt, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
}

func runOneStmt(opt plan.Optimizer, t *testing.T, sql string) error {
	t.Logf("SQL: %v\n", sql)
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ctx := context.TODO()
	if stmt, ok := stmts[0].(*tree.ExplainStmt); ok {
		es := NewExplainDefaultOptions()
		for _, v := range stmt.Options {
			if strings.EqualFold(v.Name, "VERBOSE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return moerr.NewInvalidInputf(ctx, "boolean value %v", v.Value)
				}
			} else if strings.EqualFold(v.Name, "ANALYZE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Analyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Analyze = false
				} else {
					return moerr.NewInvalidInputf(ctx, "boolean value %v", v.Value)
				}
			} else if strings.EqualFold(v.Name, "FORMAT") {
				if v.Name == "NULL" {
					return moerr.NewInvalidInputf(ctx, "parameter name %v", v.Name)
				} else if strings.EqualFold(v.Value, "TEXT") {
					es.Format = EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					es.Format = EXPLAIN_FORMAT_JSON
				} else {
					return moerr.NewInvalidInputf(ctx, "explain format %v", v.Value)
				}
			} else {
				return moerr.NewInvalidInputf(ctx, "EXPLAIN option %v", v.Name)
			}
		}

		// this sql always return one stmt
		ctx := opt.CurrentContext()
		logicPlan, err := plan.BuildPlan(ctx, stmt.Statement, false)
		if err != nil {
			t.Errorf("Build Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}
		buffer := NewExplainDataBuffer()
		explainQuery := NewExplainQueryImpl(logicPlan.GetQuery())
		err = explainQuery.ExplainPlan(ctx.GetContext(), buffer, es)
		if err != nil {
			t.Errorf("explain Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}
		t.Log("\n" + buffer.ToString())
	}
	return nil
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{"zero bytes", 0, "0bytes"},
		{"small bytes", 1024, "1024bytes"},
		{"exactly 1MB", MB, "1mb"},
		{"less than 1MB", MB - 1, "1048575bytes"},
		{"1.5MB", MB + MB/2, "1mb"},
		{"10MB", 10 * MB, "10mb"},
		{"less than 10GB", 10*GB - 1, "10239mb"},
		{"exactly 10GB", 10 * GB, "10gb"},
		{"more than 10GB", 10*GB + 1, "10gb"},
		{"20GB", 20 * GB, "20gb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			if result != tt.expected {
				t.Errorf("formatBytes(%d) = %s, want %s", tt.bytes, result, tt.expected)
			}
		})
	}
}

func TestAnalyzeInfoDescribeImpl_GetDescription(t *testing.T) {
	ctx := context.Background()
	options := &ExplainOptions{
		Format:   EXPLAIN_FORMAT_TEXT,
		Verbose:  false,
		Analyze:  true,
		NodeType: plan2.Node_TABLE_SCAN,
	}

	tests := []struct {
		name            string
		analyzeInfo     *plan2.AnalyzeInfo
		wantContains    []string
		wantNotContains []string
	}{
		{
			name: "with ReadSize, S3ReadSize, DiskReadSize",
			analyzeInfo: &plan2.AnalyzeInfo{
				TimeConsumed:     1000000000, // 1000ms
				WaitTimeConsumed: 0,
				InputRows:        1000,
				OutputRows:       500,
				InputSize:        16 * MB,
				OutputSize:       8 * MB,
				MemorySize:       4 * MB,
				ReadSize:         153 * MB,
				S3ReadSize:       150 * MB,
				DiskReadSize:     3 * MB,
				InputBlocks:      10,
			},
			wantContains: []string{
				"timeConsumed=1000ms",
				"inputRows=1000",
				"outputRows=500",
				"InputSize=16mb",
				"OutputSize=8mb",
				"ReadSize=153mb",
				"S3ReadSize=150mb",
				"DiskReadSize=3mb",
				"MemorySize=4mb",
				"inputBlocks=10",
			},
			wantNotContains: []string{},
		},
		{
			name: "with ReadSize only, no S3ReadSize and DiskReadSize",
			analyzeInfo: &plan2.AnalyzeInfo{
				TimeConsumed:     500000000, // 500ms
				WaitTimeConsumed: 0,
				InputRows:        500,
				OutputRows:       250,
				InputSize:        8 * MB,
				OutputSize:       4 * MB,
				MemorySize:       2 * MB,
				ReadSize:         10 * MB,
				S3ReadSize:       0,
				DiskReadSize:     0,
			},
			wantContains: []string{
				"ReadSize=10mb",
			},
			wantNotContains: []string{
				"S3ReadSize",
				"DiskReadSize",
			},
		},
		{
			name: "with S3ReadSize only",
			analyzeInfo: &plan2.AnalyzeInfo{
				TimeConsumed:     2000000000, // 2000ms
				WaitTimeConsumed: 0,
				InputRows:        2000,
				OutputRows:       1000,
				InputSize:        32 * MB,
				OutputSize:       16 * MB,
				MemorySize:       8 * MB,
				ReadSize:         50 * MB,
				S3ReadSize:       50 * MB,
				DiskReadSize:     0,
			},
			wantContains: []string{
				"ReadSize=50mb",
				"S3ReadSize=50mb",
			},
			wantNotContains: []string{
				"DiskReadSize",
			},
		},
		{
			name: "with DiskReadSize only",
			analyzeInfo: &plan2.AnalyzeInfo{
				TimeConsumed:     1500000000, // 1500ms
				WaitTimeConsumed: 0,
				InputRows:        1500,
				OutputRows:       750,
				InputSize:        24 * MB,
				OutputSize:       12 * MB,
				MemorySize:       6 * MB,
				ReadSize:         30 * MB,
				S3ReadSize:       0,
				DiskReadSize:     30 * MB,
			},
			wantContains: []string{
				"ReadSize=30mb",
				"DiskReadSize=30mb",
			},
			wantNotContains: []string{
				"S3ReadSize",
			},
		},
		{
			name: "small bytes format",
			analyzeInfo: &plan2.AnalyzeInfo{
				TimeConsumed:     100000000, // 100ms
				WaitTimeConsumed: 0,
				InputRows:        100,
				OutputRows:       50,
				InputSize:        1024,
				OutputSize:       512,
				MemorySize:       256,
				ReadSize:         2048,
				S3ReadSize:       2048,
				DiskReadSize:     0,
			},
			wantContains: []string{
				"InputSize=1024bytes",
				"OutputSize=512bytes",
				"ReadSize=2048bytes",
				"S3ReadSize=2048bytes",
				"MemorySize=256bytes",
			},
			wantNotContains: []string{
				"DiskReadSize",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			impl := NewAnalyzeInfoDescribeImpl(tt.analyzeInfo)
			buf := bytes.NewBuffer(make([]byte, 0, 400))
			err := impl.GetDescription(ctx, options, buf)
			if err != nil {
				t.Fatalf("GetDescription() error = %v", err)
			}
			result := buf.String()

			// Check contains
			for _, want := range tt.wantContains {
				if !strings.Contains(result, want) {
					t.Errorf("GetDescription() result should contain %q, got: %s", want, result)
				}
			}

			// Check not contains
			for _, notWant := range tt.wantNotContains {
				if strings.Contains(result, notWant) {
					t.Errorf("GetDescription() result should not contain %q, got: %s", notWant, result)
				}
			}
		})
	}
}

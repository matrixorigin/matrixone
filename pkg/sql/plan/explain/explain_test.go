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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func TestSingleSql(t *testing.T) {
	// input := "explain verbose SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20"
	input := "explain verbose SELECT N_REGIONKEY + 2 as a FROM NATION WHERE -N_NATIONKEY < -20"
	// input := "explain verbose select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a"
	// input := "explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA'"
	// input := "explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10"
	//input := "explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10"
	//input := "explain verbose select * from part where p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')"
	//input := `explain verbose
	//    select
	//         case
	//              when p_type like 'PROMO%'
	//                  then l_extendedprice * (1 - l_discount)
	//              when p_type like 'PRX%'
	//                  then l_extendedprice * (2 - l_discount)
	//         else 0 end
	//	from lineitem,part
	//	where l_shipdate < date '1996-04-01' + interval '1 month'`
	//input := "explain select abs(N_REGIONKEY) from NATION"
	//input := "explain verbose SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10"

	mock := plan.NewMockOptimizer()
	err := runOneStmt(mock, t, input)
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestBasicSqlExplain(t *testing.T) {
	sqls := []string{
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
	}
	mockOptimizer := plan.NewMockOptimizer()
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
	mockOptimizer := plan.NewMockOptimizer()
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
	mockOptimizer := plan.NewMockOptimizer()
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
	mockOptimizer := plan.NewMockOptimizer()
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
	mockOptimizer := plan.NewMockOptimizer()
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Collection query
func TestCollectionQuery(t *testing.T) {

}

func TestDMLInsert(t *testing.T) {
	sqls := []string{
		//"explain INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		//"explain verbose INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		//"explain INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",
		//"explain verbose INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",
		"explain INSERT INTO NATION SELECT * FROM NATION2",
		"explain verbose INSERT INTO NATION SELECT * FROM NATION2",
	}
	mockOptimizer := plan.NewMockOptimizer()
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestDMLUpdate(t *testing.T) {
	sqls := []string{
		//"explain UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		//"explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		//"explain UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20",
		//"explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20",
		//"explain UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
		//"explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
	}
	mockOptimizer := plan.NewMockOptimizer()
	runTestShouldPass(mockOptimizer, t, sqls)
}

func TestDMLDelete(t *testing.T) {
	sqls := []string{
		//"explain DELETE FROM NATION",
		//"explain verbose DELETE FROM NATION",
		//"explain DELETE FROM NATION WHERE N_NATIONKEY > 10",
		//"explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10",
		//"explain DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
		//"explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
	}
	mockOptimizer := plan.NewMockOptimizer()
	runTestShouldPass(mockOptimizer, t, sqls)
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
	stmts, err := mysql.Parse(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if stmt, ok := stmts[0].(*tree.ExplainStmt); ok {
		es := NewExplainDefaultOptions()
		for _, v := range stmt.Options {
			if strings.EqualFold(v.Name, "VERBOSE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return errors.New(errno.InvalidOptionValue, fmt.Sprintf("%s requires a Boolean value", v.Name))
				}
			} else if strings.EqualFold(v.Name, "ANALYZE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Anzlyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Anzlyze = false
				} else {
					return errors.New(errno.InvalidOptionValue, fmt.Sprintf("%s requires a Boolean value", v.Name))
				}
			} else if strings.EqualFold(v.Name, "FORMAT") {
				if v.Name == "NULL" {
					return errors.New(errno.InvalidOptionValue, fmt.Sprintf("%s requires a parameter", v.Name))
				} else if strings.EqualFold(v.Value, "TEXT") {
					es.Format = EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					es.Format = EXPLAIN_FORMAT_JSON
				} else {
					return errors.New(errno.InvalidOptionValue, fmt.Sprintf("unrecognized value for EXPLAIN option \"%s\": \"%s\"", v.Name, v.Value))
				}
			} else {
				return errors.New(errno.InvalidOptionValue, fmt.Sprintf("unrecognized EXPLAIN option \"%s\"", v.Name))
			}
		}

		// this sql always return one stmt
		ctx := opt.CurrentContext()
		logicPlan, err := plan.BuildPlan(ctx, stmt.Statement)
		if err != nil {
			t.Errorf("Build Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}
		buffer := NewExplainDataBuffer()
		explainQuery := NewExplainQueryImpl(logicPlan.GetQuery())
		err = explainQuery.ExplainPlan(buffer, es)
		if err != nil {
			t.Errorf("explain Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}
	}
	return nil
}

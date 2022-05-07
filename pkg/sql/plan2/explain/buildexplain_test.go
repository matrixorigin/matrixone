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
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
	"strings"
	"testing"
)

func TestSingleSql(t *testing.T) {
	input := "explain SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0"

	mock := plan2.NewMockOptimizer2()
	err := runOneStmt(mock, t, input)
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestBasicSqlExplain(t *testing.T) {
	sqls := []string{
		"explain verbose SELECT N_NAME,N_REGIONKEY, 23 as a FROM NATION",
		"explain verbose SELECT N_NAME,abs(N_REGIONKEY), 23 as a FROM NATION",
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10",
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10",
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10 ORDER BY N_NAME, N_REGIONKEY DESC",
		"explain verbose SELECT count(*) FROM NATION group by N_NAME",
		"explain verbose SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY limit 10",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10 offset 20",
	}
	mockOptimizer := plan2.NewMockOptimizer2()
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Single table query
func TestSingleTableQuery(t *testing.T) {
	sqls := []string{
		"explain verbose SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE abs(N_REGIONKEY) > 0 ORDER BY a DESC",      //test alias
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE abs(N_REGIONKEY) > 0 ORDER BY a DESC",              //test alias
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
		"explain verbose SELECT DISTINCT N_NAME FROM NATION",                      //test distinct
		"explain SELECT DISTINCT N_NAME FROM NATION",                              //test distinct
		"explain verbose SELECT DISTINCT N_NAME FROM NATION GROUP BY N_REGIONKEY", //test distinct with group by
		"explain SELECT DISTINCT N_NAME FROM NATION GROUP BY N_REGIONKEY",         //test distinct with group by
		"explain verbose SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"explain SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20",         //test more expr
		"explain verbose SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"explain SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                            //test more expr
		"explain verbose SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                            //test more expr
		"explain SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                                    //test more expr
		"explain verbose SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",
		"explain SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",
	}
	mockOptimizer := plan2.NewMockOptimizer2()
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Join query
func TestJoinQuery(t *testing.T) {
	sqls := []string{
		"explain SELECT NATION.N_NAME, REGION.R_NAME FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND LENGTH(NATION.N_NAME) > LENGTH(REGION.R_NAME)",
		"explain verbose SELECT NATION.N_NAME, REGION.R_NAME FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND LENGTH(NATION.N_NAME) > LENGTH(REGION.R_NAME)",
		"explain SELECT NATION.N_NAME, REGION.R_NAME FROM NATION left join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND LENGTH(NATION.N_NAME) > LENGTH(REGION.R_NAME)",
		"explain verbose SELECT NATION.N_NAME, REGION.R_NAME FROM NATION left join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND LENGTH(NATION.N_NAME) > LENGTH(REGION.R_NAME)",
		"explain SELECT l.l_orderkey FROM customer c, orders o, lineitem l WHERE c.c_custkey = o.o_custkey and l.l_orderkey = o.o_orderkey and o.o_orderdate < current_date",
		"explain verbose SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE abs(NATION.N_REGIONKEY) > 0",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE abs(NATION.N_REGIONKEY) > 0",
		"explain verbose SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE abs(NATION2.R_REGIONKEY) > 0",
		"explain SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE abs(NATION2.R_REGIONKEY) > 0",
		"explain verbose SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE abs(NATION2.R_REGIONKEY) > 0",
		"explain SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE abs(NATION2.R_REGIONKEY) > 0",
		"explain verbose SELECT N_NAME FROM NATION NATURAL JOIN REGION", //have no same column name but it's ok
		"explain SELECT N_NAME FROM NATION NATURAL JOIN REGION",
		"explain verbose SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",                                     //test alias
		"explain SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",                                             //test alias
		"explain verbose SELECT abs(l.L_ORDERKEY) a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERDATE < 10", //join three tables
		"explain SELECT abs(l.L_ORDERKEY) a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERDATE < 10",         //join three tables
		"explain verbose SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                        //test star
		"explain SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",
		"explain verbose SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY", //test star
		"explain SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",         //test star
		"explain verbose SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",           //test star
		"explain SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",
		"explain verbose SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",
		"explain SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0",
	}
	mockOptimizer := plan2.NewMockOptimizer2()
	runTestShouldPass(mockOptimizer, t, sqls)
}

// Nested query
func TestNestedQuery(t *testing.T) {

}

// Collection query
func TestCollectionQuery(t *testing.T) {

}

func runTestShouldPass(opt plan2.Optimizer, t *testing.T, sqls []string) {
	for _, sql := range sqls {
		err := runOneStmt(opt, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
}

func runOneStmt(opt plan2.Optimizer, t *testing.T, sql string) error {
	fmt.Printf("SQL: %v\n", sql)
	stmts, err := mysql.Parse(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if stmt, ok := stmts[0].(*tree.ExplainStmt); ok {
		es := &ExplainOptions{
			Verbose: false,
			Anzlyze: false,
			Format:  EXPLAIN_FORMAT_TEXT,
		}

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

		//this sql always return one stmt
		ctx := opt.CurrentContext()
		query, err := plan2.BuildPlan2(ctx, stmt.Statement)
		if err != nil {
			fmt.Printf("Build Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}

		buffer := NewExplainDataBuffer(20)
		explainQuery := NewExplainQueryImpl(query)
		explainQuery.ExplainPlan(buffer, es)
	}
	return nil
}

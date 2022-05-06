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
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
	"strings"
	"testing"
)

func TestBasicSqlExplain(t *testing.T) {
	sqls := []string{
		"explain verbose SELECT N_NAME,N_REGIONKEY, 23 as a FROM NATION",
		"explain verbose SELECT N_NAME,abs(N_REGIONKEY), 23 as a FROM NATION",
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10",
		"explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10",
		"explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10 ORDER BY N_NAME, N_REGIONKEY DESC",
		"explain verbose SELECT count(*) FROM NATION group by N_NAME",
		"explain verbose SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",
		"explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10 offset 20",
	}
	mockOptimizer := plan2.NewMockOptimizer2()
	runTestShouldPass(mockOptimizer, t, sqls)
}

func runTestShouldPass(opt plan2.Optimizer, t *testing.T, sqls []string) {
	for _, sql := range sqls {
		err := runOneStmt(opt, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
}

func TestSingleTableSqlBuilder(t *testing.T) {

}

func TestSingleSql(t *testing.T) {

	mock := plan2.NewMockOptimizer2()
	//input := "explain verbose SELECT N_NAME,N_REGIONKEY, 23 as a FROM NATION"
	//input := "explain verbose SELECT N_NAME,abs(N_REGIONKEY), 23 as a FROM NATION"
	//input := "explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10"
	//input := "explain SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10"
	//input := "explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10 ORDER BY N_NAME, N_REGIONKEY DESC"
	//input := "explain verbose SELECT count(*) FROM NATION group by N_NAME" //test star
	//input := "explain verbose SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10" //test agg
	//input := "explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY limit 10" //Optimize statement error
	input := "explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10 offset 20"
	err := runOneStmt(mock, t, input)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	//outPutQuery(query, true, t)
}

func runOneStmt(opt plan2.Optimizer, t *testing.T, sql string) error {
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
					return errors.New("111111", fmt.Sprintf("%s requires a Boolean value", v.Name))
				}
			} else if strings.EqualFold(v.Name, "ANALYZE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Anzlyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Anzlyze = false
				} else {
					return errors.New("111111", fmt.Sprintf("%s requires a Boolean value", v.Name))
				}
			} else if strings.EqualFold(v.Name, "FORMAT") {
				if v.Name == "NULL" {
					return errors.New("111111", fmt.Sprintf("%s requires a parameter", v.Name))
				} else if strings.EqualFold(v.Value, "TEXT") {
					es.Format = EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					es.Format = EXPLAIN_FORMAT_JSON
				} else {
					return errors.New("111111", fmt.Sprintf("unrecognized value for EXPLAIN option \"%s\": \"%s\"", v.Name, v.Value))
				}
			} else {
				return errors.New("111111", fmt.Sprintf("unrecognized EXPLAIN option \"%s\"", v.Name))
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

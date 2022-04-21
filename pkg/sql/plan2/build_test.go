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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func outPutQuery(query *Query, toFile bool, t *testing.T) {
	b, err := json.Marshal(query)
	if err != nil {
		t.Logf("%+v", query)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		t.Logf("%+v", query)
	}
	if toFile {
		err := ioutil.WriteFile("/tmp/mo_plan2_test.json", out.Bytes(), 0777)
		if err != nil {
			t.Logf("%+v", err)
		}
	} else {
		t.Logf(out.String())
	}
}

func runTestShouldPass(t *testing.T, sqls []string, printJson bool, toFile bool) {
	mock := newMockOptimizer()
	for _, sql := range sqls {
		stmts, err := mysql.Parse(sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		for _, stmt := range stmts {
			query, err := mock.Optimize(stmt)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if printJson {
				outPutQuery(query, toFile, t)
			}
		}
	}
}

func runTestShouldError(t *testing.T, sqls []string) {
	mock := newMockOptimizer()
	for _, sql := range sqls {
		stmts, err := mysql.Parse(sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		for _, stmt := range stmts {
			query, err := mock.Optimize(stmt)
			if err == nil {
				t.Fatalf("should error, but pass: %v", query)
			}
		}
	}
}

func TestSingleTableSqlBuilder(t *testing.T) {
	//should pass
	sqls := []string{
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE abs(N_REGIONKEY) > 0 ORDER BY a DESC", //test alias
	}
	runTestShouldPass(t, sqls, false, false)

	//should error
	sqls = []string{
		"SELECT N_NAME, N_REGIONKEY FROM table_not_exist",                   //table not exist
		"SELECT N_NAME, column_not_exist FROM NATION",                       //column not exist
		"SELECT N_NAME, N_REGIONKEY a FROM NATION ORDER BY not_exist_alias", //alias not exist
	}
	runTestShouldError(t, sqls)
}

func TestAggregationSqlBuilder(t *testing.T) {
	//should pass
	sqls := []string{
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",
	}
	runTestShouldPass(t, sqls, false, false)
}

func TestJoinTableSqlBuilder(t *testing.T) {
	//should pass
	sqls := []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE abs(NATION.N_REGIONKEY) > 0",
		"SELECT N_NAME, R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE abs(NATION2.R_REGIONKEY) > 0",
		"SELECT N_NAME, R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE abs(NATION2.R_REGIONKEY) > 0",
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(a.N_REGIONKEY) > 0", //test alias
	}
	runTestShouldPass(t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.NotExistColumn",                         //column not exist
		"SELECT N_NAME, R_REGIONKEY FROM NATION join REGION using(R_REGIONKEY)",                                                   //column not exist
		"SELECT N_NAME, R_REGIONKEY FROM NATION NATURAL JOIN REGION",                                                              // have no same column
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE abs(aaaaa.N_REGIONKEY) > 0", //table alias not exist
	}
	runTestShouldError(t, sqls)
}

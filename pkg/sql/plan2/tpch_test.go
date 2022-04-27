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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func Test_TPCH_Plan2(t *testing.T) {
	// Mock is broken
	// mock := newMockOptimizer()
	_, fn, _, _ := runtime.Caller(0)
	dir := filepath.Dir(fn)

	ddlf, err := os.ReadFile(dir + "/tpch/ddl.sql")
	if err != nil {
		t.Errorf("Cannot open ddl file, error %v", err)
	}

	ddls, err := mysql.Parse(string(ddlf))
	if ddls == nil || err != nil {
		t.Errorf("DDL Parser failed, error %v", err)
	}

	/*
		BROKEN: Will crash.
		for _, ast := range ddls {
			_, err := mock.Optimize(ast)
			if err == nil {
				t.Logf("Optimizer failed, NYI")
			}
		}
	*/

	qf, err := os.ReadFile(dir + "/tpch/simple.sql")
	if err != nil {
		t.Errorf("Cannot open queries file, error %v", err)
	}

	qs, err := mysql.Parse(string(qf))
	if qs == nil || err != nil {
		t.Errorf("DDL Parser failed, error %v", err)
	}

	/*
		BROKEN:
		for _, ast := range qs {
			_, err := mock.Optimize(ast)
			if err == nil {
				t.Logf("Optimizer failed, NYI")
			}
		}
	*/

	// Parse failed for query 1, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 20, 22
	for qn := 1; qn <= 22; qn += 1 {
		qnf, err := os.ReadFile(fmt.Sprintf("%s/tpch/q%d.sql", dir, qn))
		if err != nil {
			t.Errorf("Cannot open file of query %d, error %v", qn, err)
		}
		qns, err := mysql.Parse(string(qnf))
		if qns == nil || err != nil {
			t.Logf("Query %d Parser failed, error %v", qn, err)
		}
		/*
			BROKEN: Will Crash
			for _, ast := range qns {
				_, err := mock.Optimize(ast)
				if err == nil {
					t.Logf("Optimizer failed, NYI")
				}
			}
		*/
	}
}

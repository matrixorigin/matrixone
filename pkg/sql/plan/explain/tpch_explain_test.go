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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func Test_TPCH_Plan2(t *testing.T) {
	ctx := context.TODO()

	mock := plan.NewMockOptimizer(false)
	es := &ExplainOptions{
		Verbose: true,
		Analyze: false,
		Format:  EXPLAIN_FORMAT_TEXT,
	}

	_, fn, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filepath.Dir(fn))

	ddlf, err := os.ReadFile(dir + "/tpch/ddl.sql")
	if err != nil {
		t.Errorf("Cannot open ddl file, error %v", err)
	}

	ddls, err := parsers.Parse(ctx, dialect.MYSQL, string(ddlf))
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

	// test simple sql
	qf, err := os.ReadFile(dir + "/tpch/simple.sql")
	t.Logf("# tpch file: /tpch/simple.sql")
	if err != nil {
		t.Errorf("Cannot open queries file, error %v", err)
	}
	qs, err := parsers.Parse(ctx, dialect.MYSQL, string(qf))
	if qs == nil || err != nil {
		t.Errorf("Query Parser failed, error %v", err)
	}
	for _, ast := range qs {
		out := tree.String(ast, dialect.MYSQL)
		t.Logf("SQL: %v\n", out)
		query, err := mock.Optimize(ast)
		if err != nil {
			t.Errorf("Optimizer failed, NYI")
		}
		buffer := NewExplainDataBuffer()
		explainQuery := NewExplainQueryImpl(query)
		err = explainQuery.ExplainPlan(ctx, buffer, es)
		if err != nil {
			t.Errorf("explain failed, WXL")
		}
	}

	// test tpch query
	for qn := 1; qn <= 22; qn += 1 {
		// qn := 15
		qnf, err := os.ReadFile(fmt.Sprintf("%s/tpch/q%d.sql", dir, qn))
		if err != nil {
			t.Errorf("Cannot open file of query %d, error %v", qn, err)
		}
		t.Logf("--<%d> tpch file: %s/tpch/q%d.sql\n", qn, dir, qn)
		t.Logf("SQL : %v \n", string(qnf))

		qns, err := parsers.Parse(ctx, dialect.MYSQL, string(qnf))
		if qns == nil || err != nil {
			t.Errorf("Query %d Parser failed, error %v", qn, err)
		}
		for _, ast := range qns {
			query, err := mock.Optimize(ast)
			if err != nil {
				t.Errorf("Optimizer %d failed, error %v", qn, err)
			}
			buffer := NewExplainDataBuffer()
			explainQuery := NewExplainQueryImpl(query)
			err = explainQuery.ExplainPlan(ctx, buffer, es)
			if err != nil {
				t.Errorf("explain failed, WXL %v", err)
			}
		}
	}
}

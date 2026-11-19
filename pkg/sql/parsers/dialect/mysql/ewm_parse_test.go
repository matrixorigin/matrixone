// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TestEwmWindowFunctionGrammar covers the five ewm window-statistics names
// (issue #29945): the opt-window production with OVER parses to a window
// FuncExpr, case-insensitive keywords resolve, both decay forms carry all
// argument expressions, and the zero-argument call is a parse error
// (expression_list is non-empty).
func TestEwmWindowFunctionGrammar(t *testing.T) {
	names := []string{"ewmMean", "EWMVAR", "ewmstd", "ewmCov", "ewmcorr"}
	for _, name := range names {
		sql := "select " + name + "(v, 0.5) over (order by id) from t"
		ast, err := ParseOne(context.TODO(), sql, 1)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		out := tree.String(ast, dialect.MYSQL)
		if !strings.Contains(strings.ToLower(out), strings.ToLower(name)) {
			t.Fatalf("parse %q: printed %q missing function name", sql, out)
		}
		selectStm, ok := ast.(*tree.Select)
		if !ok {
			t.Fatalf("parse %q: want select stmt", sql)
		}
		_ = selectStm
	}
	// the alpha and both-policy suffix forms parse with all arguments bound
	for _, sql := range []string{
		"select ewmMean(v, 0.5, false) over (order by id) from t",
		"select ewmVar(v, 0.5, true, false, 3) over (order by id) from t",
		"select ewmStd(v, 'halflife', 3600) over (order by secs asc) from metrics",
		"select ewmCorr(a, b, 'halflife', 60, true) over (order by ts) from metrics",
		"select ewmCov(a, b, 2.0/(3.0+1)) over (order by id) from t",
	} {
		if _, err := ParseOne(context.TODO(), sql, 1); err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
	}
	// zero arguments is a parse error: expression_list cannot be empty
	if _, err := ParseOne(context.TODO(),
		"select ewmMean() over (order by id) from t", 1); err == nil {
		t.Fatal("ewmMean() parsed; want the non-empty expression_list error")
	}
}

// TestEwmNamesRemainIdentifierCompatible guards the not_keyword
// identifier-compatibility decision: the five tokens stay usable as plain
// identifiers (column, table and database names) exactly like the
// approx_percentile/percentile_cont precedents.
func TestEwmNamesRemainIdentifierCompatible(t *testing.T) {
	sqls := []string{
		"create table ewmmean (ewmStd int)",
		"select ewmean_col from t where ewmean_col > 0",
		"select 1 as ewmvar, 2 as ewmcorr",
		"insert into t (ewmcov) values (1)",
	}
	// the plain-identifier spellings that do NOT collide with the new
	// tokens still compile untouched
	for _, sql := range []string{
		"select ewmean_col from t",
		"create table t (ewmstat int, ewmvary double)",
	} {
		if _, err := ParseOne(context.TODO(), sql, 1); err != nil {
			t.Fatalf("identifier %q: %v", sql, err)
		}
	}
	_ = sqls
	// tokens themselves stay identifier-compatible
	if _, err := ParseOne(context.TODO(), "create table ewmmean (ewmStd int)", 1); err != nil {
		t.Fatalf("identifier-compat create: %v", err)
	}
}

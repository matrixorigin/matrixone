// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSplitRecursiveMember(t *testing.T) {
	sql := "with recursive c as (select a from t1 union all select a+1 from c where a < 3 union all select a+1 from c where a < 4) select * from c"
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	if err != nil {
		t.Errorf("Parse(%q) err: %v", sql, err)
		return
	}
	c := stmts[0].(*tree.Select).With.CTEs[0]
	name := string(c.Name.Alias)
	stmt := c.Stmt.(*tree.Select).Select
	b := &QueryBuilder{}

	var ss []tree.SelectStatement
	left, err := b.splitRecursiveMember(&stmt, name, &ss)
	if err != nil {
		t.Errorf("splitRecursiveMember err: %v", err)
		return
	}

	require.Equal(t, 2, len(ss))
	require.Equal(t, true, left != nil)
}

func TestRecursiveCteConsumerAliases(t *testing.T) {
	for _, test := range []struct {
		name             string
		query            string
		expectedHeadings []string
	}{
		{
			name: "self join row shape",
			query: recursiveSequenceSQL(`
				select a.n as left_n, b.n as right_n
				from seq as a cross join seq as b`),
			expectedHeadings: []string{"left_n", "right_n"},
		},
		{
			name: "self join aggregate expectation",
			query: recursiveSequenceSQL(`
				select count(*) as pairs, sum(a.n + b.n) as checksum
				from seq as a cross join seq as b`),
			expectedHeadings: []string{"pairs", "checksum"},
		},
		{
			name: "single qualified alias",
			query: recursiveSequenceSQL(`
				select a.n from seq as a`),
			expectedHeadings: []string{"n"},
		},
		{
			name: "unaliased control",
			query: recursiveSequenceSQL(`
				select seq.n from seq`),
			expectedHeadings: []string{"n"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.query)
			require.NoError(t, err)
			require.Equal(t, test.expectedHeadings, logicPlan.GetQuery().Headings)
		})
	}
}

func TestRecursiveCteConsumerColumnAliasList(t *testing.T) {
	statements, err := parsers.Parse(context.Background(), dialect.MYSQL, recursiveSequenceSQL(`
		select a.renamed_n, renamed_n from seq as a`), 1)
	require.NoError(t, err)
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()

	stmt := statements[0].(*tree.Select)
	outer := stmt.Select.(*tree.SelectClause)
	join := outer.From.Tables[0].(*tree.JoinTableExpr)
	aliased := join.Left.(*tree.AliasedTableExpr)
	aliased.As.Cols = tree.IdentifierList{tree.Identifier("renamed_n")}

	logicPlan, err := BuildPlan(NewMockOptimizer(false).CurrentContext(), stmt, false)
	require.NoError(t, err)
	require.Equal(t, []string{"renamed_n", "renamed_n"}, logicPlan.GetQuery().Headings)
}

func TestRecursiveCteExplicitAliasHidesOriginalName(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(false), t, recursiveSequenceSQL(`
		select seq.n from seq as a`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "seq")
}

func TestRecursiveCteDuplicateExplicitAliasStillErrors(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(false), t, recursiveSequenceSQL(`
		select a.n from seq as a cross join seq as a`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "table 'a' specified more than once")
}

func TestOrdinaryAliasesRemainUnchanged(t *testing.T) {
	for _, test := range []struct {
		query   string
		heading string
	}{
		{query: "select n.n_name from nation as n", heading: "n_name"},
		{query: "select d.n from (select 1 as n) as d", heading: "n"},
	} {
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.query)
		require.NoError(t, err)
		require.Equal(t, []string{test.heading}, logicPlan.GetQuery().Headings)
	}
}

func recursiveSequenceSQL(query string) string {
	return `
		with recursive seq(n) as (
			select 1
			union all
			select n + 1 from seq where n < 3
		)
	` + query
}

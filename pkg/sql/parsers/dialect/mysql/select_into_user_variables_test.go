// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSelectIntoUserVariablesParse(t *testing.T) {
	testCases := []struct {
		sql            string
		wantSQL        string
		vars           []string
		deprecatedInto bool
	}{
		{
			sql:     "select abs(-5), 'ok' into @out, @label",
			wantSQL: "select abs(-5), ok into @out, @label",
			vars:    []string{"out", "label"},
		},
		{
			sql:     "select id, v from uv_src where id = 2 into @row_id, @row_v",
			wantSQL: "select id, v from uv_src where id = 2 into @row_id, @row_v",
			vars:    []string{"row_id", "row_v"},
		},
		{
			sql:     "select id into @pre_from_id from uv_src where id = 2",
			wantSQL: "select id into @pre_from_id from uv_src where id = 2",
			vars:    []string{"pre_from_id"},
		},
		{
			sql:     "select v from uv_src order by id desc limit 1 into @last_v",
			wantSQL: "select v from uv_src order by id desc limit 1 into @last_v",
			vars:    []string{"last_v"},
		},
		{
			sql:     "(select 3 into @paren_out)",
			wantSQL: "(select 3 into @paren_out)",
			vars:    []string{"paren_out"},
		},
		{
			sql:     "((select 4 into @nested_paren_out))",
			wantSQL: "((select 4 into @nested_paren_out))",
			vars:    []string{"nested_paren_out"},
		},
		{
			sql:            "select 1 union (select 1 into @union_out)",
			wantSQL:        "select 1 union (select 1 into @union_out)",
			vars:           []string{"union_out"},
			deprecatedInto: true,
		},
		{
			sql:            "select 1 union select 1 into @union_terminal",
			wantSQL:        "select 1 union select 1 into @union_terminal",
			vars:           []string{"union_terminal"},
			deprecatedInto: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			statement, err := ParseOne(context.Background(), testCase.sql, 1)
			require.NoError(t, err)
			selectStatement, ok := statement.(*tree.Select)
			require.True(t, ok)
			require.Nil(t, selectStatement.Ep)
			require.Len(t, selectStatement.IntoVars, len(testCase.vars))
			for i, name := range testCase.vars {
				require.Equal(t, name, selectStatement.IntoVars[i].Name)
			}
			require.Equal(t, testCase.deprecatedInto, selectStatement.DeprecatedInto)
			require.Equal(t, tree.OUTPUT_STATUS, selectStatement.StmtKind().OutputType())
			require.Equal(t, testCase.wantSQL, tree.String(statement, dialect.MYSQL))
		})
	}
}

func TestSelectIntoUserVariablesRejectsMisplacedUnionInto(t *testing.T) {
	testCases := []string{
		"select 1 into @bad_non_last union select 1",
		"select 1 into outfile '/tmp/mo_bad_non_last_outfile' union select 1",
		"select 1 union (select 1 into @bad_middle) union select 1",
		"select 1 union (select 1 into @bad_middle) into @bad_terminal",
	}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			_, err := ParseOne(context.Background(), testCase, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), tree.MisplacedIntoClauseMessage)
		})
	}
}

func TestSelectIntoUserVariablesRejectsNestedInto(t *testing.T) {
	testCases := []string{
		"select (select 1 into @bad_scalar)",
		"select * from (select 1 into @bad_derived) as d",
		"with d as (select 1 into @bad_cte) select * from d",
		"select exists(select 1 into @bad_exists)",
		"values row((select 1 into @bad_values))",
		"select * from (values row((select 1 into @bad_derived_values))) d",
		"select * from unnest((select 1 into @bad_table_function)) as u",
		"select sample((select 1 into @bad_sample), 1 rows)",
		"select match(((select 1 into @bad_match))) against ('x')",
		"select 1 order by (select 1 into @bad_order)",
		"select 1 limit (select 1 into @bad_limit)",
		"select 1 limit 1 offset (select 1 into @bad_offset)",
		"select * from uv_src{timestamp = (select 1 into @bad_table_timestamp)}",
		"select * from uv_src{mo_ts = (select 1 into @bad_table_mo_timestamp)}",
	}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			_, err := ParseOne(context.Background(), testCase, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), tree.MisplacedIntoClauseMessage)
		})
	}
}

func TestSelectIntoUserVariablesRejectsOwnerlessEnclosingStatements(t *testing.T) {
	testCases := []string{
		"create view uv_bad_view as select 1 into @bad_view",
		"with d as (select 1 into @bad_insert_cte) insert into uv_target select * from d",
		"update uv_target set a = (select 1 into @bad_update)",
		"delete from uv_target where a = (select 1 into @bad_delete)",
		"insert into uv_target values (1) on duplicate key update a = (select 1 into @bad_duplicate)",
		"insert into uv_target values (1) returning (select 1 into @bad_returning)",
		"replace into uv_target values (1) returning (select 1 into @bad_replace_returning)",
		"set @bad_outer = (select 1 into @bad_inner)",
		"do (select 1 into @bad_do)",
		"call uv_proc((select 1 into @bad_call))",
		"set @bad_outer = sample((select 1 into @bad_sample_set), 1 rows)",
		"do sample((select 1 into @bad_sample_do), 1 rows)",
		"call uv_proc(sample((select 1 into @bad_sample_call), 1 rows))",
		"show tables where exists (select 1 into @bad_show)",
		"insert into uv_target partition (p = (select 1 into @bad_partition)) values (1)",
		"load data infile 'dummy.csv' into table uv_target set a = (select 1 into @bad_load_set)",
	}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			_, err := ParseOne(context.Background(), testCase, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), tree.MisplacedIntoClauseMessage)
		})
	}
}

func TestSelectIntoUserVariablesAllowsOwnedSourceContexts(t *testing.T) {
	testCases := []string{
		"insert into uv_target select 1 into @insert_source",
		"create table uv_ctas as select 1 into @ctas_source",
		"create table uv_ctas_cols (a int) select 1 into @ctas_cols_source",
		"explain select 1 into @explain_source",
		"explain analyze select 1 into @explain_analyze_source",
	}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			_, err := ParseOne(context.Background(), testCase, 1)
			require.NoError(t, err)
		})
	}
}

func TestSelectIntoUserVariablesRejectsPerformInto(t *testing.T) {
	_, err := ParseOne(context.Background(), "perform select 1 into @bad_perform", 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), tree.PerformIntoClauseMessage)
}

func TestSelectIntoUserVariablesRejectsMixedIntoActions(t *testing.T) {
	testCases := []string{
		"select 1 into @bad_user_outfile into outfile '/tmp/mo_bad_user_outfile'",
		"select 1 into outfile '/tmp/mo_bad_outfile_user' into @bad_outfile_user",
	}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			_, err := ParseOne(context.Background(), testCase, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), tree.MisplacedIntoClauseMessage)
		})
	}
}

//go:build integration

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

package frontend_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type nativeViewMetadataCase struct {
	name, sql string
	want      [][]string
	allowed   []string
}

// Reuse S2's real catalog fixture. SQLExecutor parses, plans and executes the
// public SQL, while the explicit request opts in only this owned statement.
// Default protocol traffic is not switched to the new contract by this test.
func testNativeViewMetadataConsumers(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, database string, historical timestamp.Timestamp) {
	execDDL := func(sql string) {
		t.Helper()
		result, err := svc.GetSQLExecutor().Exec(ctx, sql, executor.Options{}.WithDatabase(database))
		require.NoError(t, err, sql)
		result.Close()
	}
	execDDL("create table metadata_missing_source (id int)")
	defer execDDL("drop table if exists metadata_missing_source")
	execDDL("create view metadata_invalid as select id from metadata_missing_source")
	defer execDDL("drop view metadata_invalid")
	execDDL("drop table metadata_missing_source")
	where := fmt.Sprintf(" from information_schema.columns where table_schema = '%s' ", database)
	cases := []nativeViewMetadataCase{
		{"selective", "select column_name, lower(column_type)" + where + "and table_name='outer_a' order by ordinal_position", [][]string{{"id", "int"}, {"label", "varchar(40)"}}, []string{"outer_a"}},
		{"historical", fmt.Sprintf("select column_name, lower(column_type) from information_schema.columns {MO_TS = '%s'} where table_schema='%s' and table_name='outer_a' order by ordinal_position", historical.DebugString(), database), [][]string{{"id", "int"}, {"label", "varchar(20)"}}, []string{"outer_a"}},
		{"legacy selective", "select column_name, lower(column_type)" + where + "and table_name='outer_a' order by ordinal_position", [][]string{{"id", "int"}, {"label", "varchar(40)"}}, nil},
		{"describe", "describe outer_a", [][]string{{"id", "int"}, {"label", "varchar(40)"}}, []string{"outer_a"}},
		{"show columns", "show columns from outer_a", [][]string{{"id", "int"}, {"label", "varchar(40)"}}, []string{"outer_a"}},
		{"show full columns", "show full columns from outer_a", [][]string{{"id", "int"}, {"label", "varchar(40)"}}, []string{"outer_a"}},
		{"union", "select column_name" + where + "and table_name='outer_a' union all select column_name" + where + "and table_name='outer_a' order by column_name", [][]string{{"id"}, {"id"}, {"label"}, {"label"}}, []string{"outer_a"}},
		{"derived filter and limit", "select column_name" + where + "and table_name in ('outer_a','outer_b') and data_type='varchar' order by column_name limit 1", [][]string{{"label"}}, []string{"outer_a", "outer_b"}},
		{"parameters", "select column_name" + where + "and table_name=? and (data_type=? or data_type is null)", [][]string{{"label"}}, []string{"outer_a"}},
		{"null parameter", "select cast(count(*) as char)" + where + "and table_name=?", [][]string{{"0"}}, nil},
		{"derived default predicate", "select column_name" + where + "and table_name='outer_a' and column_default like '%new%'", [][]string{{"label"}}, []string{"outer_a"}},
		{"aggregation", "select table_name, cast(count(*) as char)" + where + "and (table_name='outer_a' or table_name='outer_b') group by table_name order by table_name", [][]string{{"outer_a", "2"}, {"outer_b", "2"}}, []string{"outer_a", "outer_b"}},
		{"join", fmt.Sprintf("select a.column_name, b.column_name from information_schema.columns a join information_schema.columns b on a.ordinal_position=b.ordinal_position where a.table_schema='%s' and b.table_schema='%s' and a.table_name='outer_a' and b.table_name='outer_b' order by a.ordinal_position", database, database), [][]string{{"id", "id_alias"}, {"label", "label_alias"}}, []string{"outer_a", "outer_b"}},
		{"empty", "select cast(count(*) as char)" + where + "and table_name='absent'", [][]string{{"0"}}, nil},
		{"invalid bulk", "select column_name" + where + "and table_name in ('outer_a','metadata_invalid') order by ordinal_position", [][]string{{"id"}, {"label"}}, []string{"outer_a", "metadata_invalid"}},
		{"raw physical contract", fmt.Sprintf("select lower(mo_show_visible_bin(atttyp,3)) from mo_catalog.mo_columns where att_database='%s' and att_relname='outer_a' and attname='label'", database), [][]string{{"varchar(20)"}}, nil},
		{"base table fast path", "select column_name" + where + "and table_name='source' order by ordinal_position", [][]string{{"id"}, {"label"}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { testNativeViewMetadataCase(t, ctx, cn, svc, database, nil, tc) })
	}
}

func testNativeSubscribedViewMetadata(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, tenant *frontend.TenantInfo) {
	for _, tc := range []nativeViewMetadataCase{
		{"selective", "select table_name, column_name, lower(column_type) from information_schema.columns where table_schema='subscribed' and table_name in ('published','v_nested') order by table_name, ordinal_position", [][]string{{"published", "id", "int"}, {"v_nested", "id", "int"}}, []string{"published", "v_nested"}},
		{"describe", "describe subscribed.published", [][]string{{"id", "int"}}, []string{"published"}},
		{"mixed domains", "select table_schema, lower(column_type) from information_schema.columns where table_schema='subscribed' and table_name='published' union all select table_schema, lower(column_type) from information_schema.columns where table_schema='schema_other' and table_name='local_nested' order by table_schema", [][]string{{"schema_other", "varchar(30)"}, {"subscribed", "int"}}, []string{"published", "local_nested"}},
	} {
		t.Run(tc.name, func(t *testing.T) { testNativeViewMetadataCase(t, ctx, cn, svc, "subscribed", tenant, tc) })
	}
}

func testNativeViewMetadataCase(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, database string, tenant *frontend.TenantInfo, tc nativeViewMetadataCase) {
	t.Helper()
	withNativeViewSchemaRead(t, ctx, cn, svc, database, tenant, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
		before := op.GetWorkspace().WriteOffset()
		bound := make(map[string]int)
		authorize := frontend.NativeViewSchemaRoleAuthorizerForTest(compiler)
		request := compiler.NewViewMetadataRequest(ctx, func(ctx context.Context, db, name string, snapshot *plan.Snapshot) error {
			bound[name]++
			return authorize(ctx, db, name, snapshot)
		})
		defer request.Close()
		execCtx := request.Context()
		if tc.name == "legacy selective" {
			execCtx = ctx
		}
		var rows [][]string
		if tc.name == "describe" || tc.name == "show columns" || tc.name == "show full columns" {
			var err error
			rows, err = frontend.ReadNativeViewMetadataSQLForTest(execCtx, compiler, tc.sql)
			require.NoError(t, err, tc.sql)
			width := 7
			if tc.name == "show full columns" {
				width = 9
			}
			for i := range rows {
				require.Len(t, rows[i], width)
				rows[i] = rows[i][:2]
			}
		} else {
			options := executor.Options{}.WithDatabase(database).WithTxn(op).WithKeepTxnAlive().WithDisableIncrStatement().WithForceRebuildPlan()
			if tc.name == "parameters" {
				options = options.WithStatementOption(executor.StatementOption{}.WithParams([]string{"outer_a", "varchar"}))
			}
			if tc.name == "null parameter" {
				options = options.WithStatementOption(executor.StatementOption{}.WithParamsAndNulls([]string{""}, []bool{true}))
			}
			result, err := svc.GetSQLExecutor().Exec(execCtx, tc.sql, options)
			require.NoError(t, err, tc.sql)
			defer result.Close()
			result.ReadRows(func(n int, columns []*vector.Vector) bool {
				for row := 0; row < n; row++ {
					values := make([]string, len(columns))
					for i, column := range columns {
						if column.IsNull(uint64(row)) {
							values[i] = "<NULL>"
						} else {
							values[i] = column.GetStringAt(row)
						}
					}
					rows = append(rows, values)
				}
				return true
			})
		}
		require.NotContains(t, bound, "", "database/name arguments must survive execution casts")
		require.Equal(t, tc.want, rows)
		for name := range bound {
			require.Contains(t, tc.allowed, name, "excluded candidates must not reach derivation")
		}
		if len(tc.allowed) > 0 {
			require.NotEmpty(t, bound, "SQL must exercise the opt-in consumer, not the legacy generator")
		}
		require.Equal(t, before, op.GetWorkspace().WriteOffset(), "metadata does not write lifecycle/catalog state")
		request.Close()
		generation, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		require.Zero(t, generation.Used())
	})
}

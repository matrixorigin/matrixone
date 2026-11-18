// Copyright 2026 Matrix Origin
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

package plan

import (
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func buildCreateTableForCTASOptionsTest(t *testing.T, sql string) *planpb.CreateTable {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	p, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false, nil)
	require.NoError(t, err, sql)
	ddl := p.GetDdl()
	require.NotNil(t, ddl, sql)
	require.NotNil(t, ddl.GetCreateTable(), sql)
	return ddl.GetCreateTable()
}

// Options parsed before AS SELECT must reach the real DDL builder consumers
// (tableDatabaseDefaults/tableDefaultCharset) exactly like the ordinary
// CREATE TABLE spelling, instead of being dropped between parse and plan
// (#32542 gap 2 consumer half).
func TestCTASTableOptionsReachDDLPlanConsumers(t *testing.T) {
	collateTwins := []struct{ ctas, plain string }{
		{"create table ct_ci collate utf8mb4_general_ci as select 1 as a", "create table p_ci (a int) collate utf8mb4_general_ci"},
		{"create table ct_bin collate utf8mb4_bin as select 1 as a", "create table p_bin (a int) collate utf8mb4_bin"},
		{"create table ct_cs charset utf8mb4 as select 1 as a", "create table p_cs (a int) charset utf8mb4"},
	}
	previousCharset := uint32(0)
	for i, twin := range collateTwins {
		t.Run(twin.ctas, func(t *testing.T) {
			ctas := buildCreateTableForCTASOptionsTest(t, twin.ctas)
			plain := buildCreateTableForCTASOptionsTest(t, twin.plain)
			require.Equal(t, plain.GetTableDef().GetDefaultCharset(), ctas.GetTableDef().GetDefaultCharset(),
				"CTAS and ordinary CREATE must resolve the same charset identity for the same option")
			if i > 0 {
				require.NotEqual(t, previousCharset, ctas.GetTableDef().GetDefaultCharset(),
					"different collation options must resolve different charsets")
			}
			previousCharset = ctas.GetTableDef().GetDefaultCharset()
		})
	}

	t.Run("database defaults", func(t *testing.T) {
		ctas := buildCreateTableForCTASOptionsTest(t, "create table ct_dd collate utf8mb4_bin as select 1 as a")
		plain := buildCreateTableForCTASOptionsTest(t, "create table p_dd (a int) collate utf8mb4_bin")
		require.Equal(t, plain.DatabaseDefaults, ctas.DatabaseDefaults)
	})

	t.Run("output columns come from the select not options", func(t *testing.T) {
		noOpt := buildCreateTableForCTASOptionsTest(t, "create table ct_n as select 1 as a")
		withOpt := buildCreateTableForCTASOptionsTest(t, "create table ct_o charset utf8mb4 as select 1 as a")
		require.Equal(t, noOpt.GetTableDef().GetCols(), withOpt.GetTableDef().GetCols())
		require.Equal(t, "a", withOpt.GetTableDef().GetCols()[0].GetName())
	})
}

// The parenthesized query body of a WITH statement must bind the CTE through
// the real binder and produce the same query plan as the unparenthesized twin
// (#32606 consumer half); dropping the outer With would fail table resolution.
func TestWithParenBodyBindsCteLikePlainTwin(t *testing.T) {
	buildQuery := func(t *testing.T, sql string) *planpb.Query {
		t.Helper()
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err, sql)
		p, err := BuildPlan(NewMockCompilerContext(true, newPlanTestProcess(t)), stmt, false, nil)
		require.NoError(t, err, sql)
		require.NotNil(t, p.GetQuery(), sql)
		return p.GetQuery()
	}
	t.Run("paren body equals plain twin", func(t *testing.T) {
		paren := buildQuery(t, "with cte_u as (select 1 as a) (select a from cte_u)")
		plain := buildQuery(t, "with cte_u as (select 1 as a) select a from cte_u")
		require.Equal(t, plain, paren)
	})
}

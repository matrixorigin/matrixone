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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func buildDropViewPlanForTest(t *testing.T, sql string) (*Plan, error) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	return BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
}

// DROP VIEW accepts a list of views, like DROP TABLE (#31910).
func TestBuildDropViewMultipleNames(t *testing.T) {
	p, err := buildDropViewPlanForTest(t, "drop view v1, tpch.v1")
	require.NoError(t, err)
	drop := p.GetDdl().GetDropTable()
	require.True(t, drop.GetIsView())
	require.Len(t, drop.GetTables(), 2)
	for _, entry := range drop.GetTables() {
		require.True(t, entry.GetIsView())
		require.Equal(t, "tpch", entry.GetDatabase())
		require.Equal(t, "v1", entry.GetTable())
		require.False(t, entry.GetIfExists())
	}
}

func TestBuildDropViewIfExistsMultipleNames(t *testing.T) {
	p, err := buildDropViewPlanForTest(t, "drop view if exists v1, v_missing, nation")
	require.NoError(t, err)
	drop := p.GetDdl().GetDropTable()
	require.True(t, drop.GetIfExists())
	tables := drop.GetTables()
	require.Len(t, tables, 3)
	require.Equal(t, "v1", tables[0].GetTable())
	// A missing view is an IF EXISTS no-op handled by the executor.
	require.Equal(t, "v_missing", tables[1].GetTable())
	require.Nil(t, tables[1].GetTableDef())
	// A same-named base table must never be dropped by DROP VIEW.
	require.Empty(t, tables[2].GetTable())
	for _, entry := range tables {
		require.True(t, entry.GetIsView())
		require.True(t, entry.GetIfExists())
	}
}

func TestBuildDropViewMultipleNamesRejectsMissingOrBaseTable(t *testing.T) {
	for _, sql := range []string{
		"drop view v1, v_missing",
		"drop view v1, nation",
	} {
		_, err := buildDropViewPlanForTest(t, sql)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadView), "%s: %v", sql, err)
	}
}

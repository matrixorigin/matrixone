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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestLifecycleStatementsParseAndFormat(t *testing.T) {
	tests := []struct {
		sql      string
		wantType any
	}{
		{
			sql:      "alter table db.t set lifecycle (column created_at, expire after interval 90 day, action archive, stage archive_stage, purge eligible after interval 730 day)",
			wantType: &tree.AlterTable{},
		},
		{
			sql:      "alter table db.t set lifecycle (column created_at, expire after interval 7 day, action delete)",
			wantType: &tree.AlterTable{},
		},
		{sql: "alter table db.t pause lifecycle", wantType: &tree.AlterTable{}},
		{sql: "alter table db.t resume lifecycle", wantType: &tree.AlterTable{}},
		{sql: "alter table db.t unset lifecycle", wantType: &tree.AlterTable{}},
		{sql: "show lifecycle for table db.t", wantType: &tree.ShowLifecycle{}},
		{sql: "show lifecycle jobs", wantType: &tree.ShowLifecycle{}},
		{sql: "show lifecycle jobs limit 100 offset 2000", wantType: &tree.ShowLifecycle{}},
		{sql: "show lifecycle datasets for table db.t", wantType: &tree.ShowLifecycle{}},
		{sql: "show lifecycle datasets for table db.t limit 500 offset 1000", wantType: &tree.ShowLifecycle{}},
		{sql: "show lifecycle restores limit 100 offset 200", wantType: &tree.ShowLifecycle{}},
		{sql: "restore archive dataset 'dataset-1' to table db.restored_t", wantType: &tree.RestoreArchiveDataset{}},
		{sql: "restore archive table db.t between '2025-01-01 00:00:00' and '2025-04-01 00:00:00' to table db.restored_q1", wantType: &tree.RestoreArchiveRange{}},
		{sql: "purge archive dataset 'dataset-1'", wantType: &tree.PurgeArchiveDataset{}},
	}

	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.IsType(t, test.wantType, stmt)
			require.Equal(t, test.sql, tree.String(stmt, dialect.MYSQL))
		})
	}
}

func TestShowLifecyclePaginationAST(t *testing.T) {
	stmt, err := ParseOne(
		context.Background(),
		"show lifecycle datasets for table db.t limit 500 offset 1000",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()
	show := stmt.(*tree.ShowLifecycle)
	require.NotNil(t, show.Page)
	count, ok := show.Page.Count.(*tree.NumVal)
	require.True(t, ok)
	countValue, ok := count.Uint64()
	require.True(t, ok)
	require.Equal(t, uint64(500), countValue)
	offset, ok := show.Page.Offset.(*tree.NumVal)
	require.True(t, ok)
	offsetValue, ok := offset.Uint64()
	require.True(t, ok)
	require.Equal(t, uint64(1000), offsetValue)
}

func TestLifecyclePolicyAST(t *testing.T) {
	stmt, err := ParseOne(context.Background(),
		"alter table db.t set lifecycle (column created_at, expire after interval 90 day, action archive, stage archive_stage, purge eligible after interval 730 day)",
		1)
	require.NoError(t, err)
	defer stmt.Free()

	alter := stmt.(*tree.AlterTable)
	require.Len(t, alter.Options, 1)
	option := alter.Options[0].(*tree.AlterOptionLifecycle)
	require.Equal(t, tree.LifecycleOperationSet, option.Operation)
	require.Equal(t, "created_at", string(option.Policy.Column))
	require.Equal(t, uint32(90), option.Policy.ExpireAfterDays)
	require.Equal(t, tree.LifecycleActionArchive, option.Policy.Action)
	require.True(t, option.Policy.HasStage)
	require.Equal(t, "archive_stage", string(option.Policy.Stage))
	require.True(t, option.Policy.HasPurgeAfter)
	require.Equal(t, uint32(730), option.Policy.PurgeAfterDays)
}

func TestLifecycleStatementExecutionLocation(t *testing.T) {
	for _, sql := range []string{
		"alter table db.t set lifecycle (column created_at, expire after interval 7 day, action delete)",
		"alter table db.t pause lifecycle",
		"restore archive dataset 'dataset-1' to table db.restored_t",
		"restore archive table db.t between '2025-01-01 00:00:00' and '2025-04-01 00:00:00' to table db.restored_q1",
		"purge archive dataset 'dataset-1'",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err)
		require.Equal(t, tree.EXEC_IN_FRONTEND, stmt.StmtKind().ExecLocation(), sql)
		stmt.Free()
	}

	stmt, err := ParseOne(context.Background(), "show lifecycle jobs", 1)
	require.NoError(t, err)
	require.Equal(t, tree.EXEC_IN_FRONTEND, stmt.StmtKind().ExecLocation())
	require.Equal(t, tree.OUTPUT_RESULT_ROW, stmt.StmtKind().OutputType())
	stmt.Free()

	stmt, err = ParseOne(context.Background(), "alter table db.t add column c int", 1)
	require.NoError(t, err)
	require.Equal(t, tree.EXEC_IN_ENGINE, stmt.StmtKind().ExecLocation())
	stmt.Free()
}

func TestLifecycleParserRejectsInvalidPolicies(t *testing.T) {
	for _, sql := range []string{
		"alter table db.t set lifecycle (column created_at, expire after interval 0 day, action delete)",
		"alter table db.t set lifecycle (column created_at, expire after interval 1 month, action delete)",
		"alter table db.t set lifecycle (column created_at, expire after interval 90 day, action archive)",
		"alter table db.t set lifecycle (column created_at, expire after interval 90 day, action delete, stage s)",
		"alter table db.t set lifecycle (column created_at, expire after interval 90 day, action archive, stage s, purge eligible after interval 30 day)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			if stmt != nil {
				stmt.Free()
			}
			require.Error(t, err)
		})
	}
}

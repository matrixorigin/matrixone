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

package frontend

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestLifecycleShowPageIsBounded(t *testing.T) {
	limit, offset, err := lifecycleShowPage(
		context.Background(),
		&tree.ShowLifecycle{},
	)
	require.NoError(t, err)
	require.Equal(t, lifecycleShowDefaultLimit, limit)
	require.Zero(t, offset)

	limit, offset, err = lifecycleShowPage(
		context.Background(),
		&tree.ShowLifecycle{
			Page: tree.NewLimit(
				tree.NewNumVal(int64(2500), "2500", false, tree.P_int64),
				tree.NewNumVal(int64(100), "100", false, tree.P_int64),
			),
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(100), limit)
	require.Equal(t, int64(2500), offset)

	for _, statement := range []*tree.ShowLifecycle{
		{Page: tree.NewLimit(nil, tree.NewNumVal(int64(0), "0", false, tree.P_int64))},
		{Page: tree.NewLimit(nil, tree.NewNumVal(lifecycleShowMaxLimit+1, "1001", false, tree.P_int64))},
		{Page: tree.NewLimit(
			tree.NewNumVal(lifecycleShowMaxWindow, "1000000", false, tree.P_int64),
			tree.NewNumVal(int64(1), "1", false, tree.P_int64),
		)},
		{Page: tree.NewLimit(nil, tree.NewNumVal(float64(1.5), "1.5", false, tree.P_float64))},
	} {
		_, _, err = lifecycleShowPage(context.Background(), statement)
		require.Error(t, err)
	}
}

func TestHandleShowLifecycleJobsUsesSystemCatalogAndBuildsRows(t *testing.T) {
	ctx := context.Background()
	background := &backgroundExecTest{}
	background.init()
	query := `select hex(root_id),mode,state,cast(cleanup_after as varchar),last_error
from mo_catalog.mo_lifecycle_cleanup_roots where owner_account_id = 17
order by updated_at desc,root_id desc limit 2 offset 3`
	execResult := &MysqlResultSet{}
	for _, column := range lifecycleJobShowColumns {
		execResult.AddColumn(column)
	}
	for _, row := range [][]interface{}{
		{"00112233", "ARCHIVE_WHOLE", "PUBLISHED", "2026-08-04 12:00:00", nil},
		{"44556677", "ARCHIVE_REWRITE", "COMMIT_UNKNOWN", "2026-08-04 12:05:00", "commit result unknown"},
	} {
		execResult.AddRow(row)
	}
	background.sql2result[query] = execResult
	wrapped := &lifecycleRestoreContextExec{backgroundExecTest: background}
	stub := gostub.StubFunc(&NewBackgroundExec, wrapped)
	t.Cleanup(stub.Reset)

	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	ses.SetTenantInfo(&TenantInfo{TenantID: 17})
	ses.mrs = &MysqlResultSet{}

	err := handleShowLifecycle(ctx, ses, &tree.ShowLifecycle{
		Kind: tree.ShowLifecycleJobs,
		Page: tree.NewLimit(
			tree.NewNumVal(int64(3), "3", false, tree.P_int64),
			tree.NewNumVal(int64(2), "2", false, tree.P_int64),
		),
	})
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, wrapped.accountIDs)
	require.Equal(t, uint64(5), ses.mrs.GetColumnCount())
	require.Equal(t, uint64(2), ses.mrs.GetRowCount())
	row, err := ses.mrs.GetRow(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, []interface{}{
		"00112233", "ARCHIVE_WHOLE", "PUBLISHED", "2026-08-04 12:00:00", nil,
	}, row)
	row, err = ses.mrs.GetRow(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, "commit result unknown", row[4])
}

func TestHandleShowLifecycleJobsEmptyAndInvalidKind(t *testing.T) {
	ctx := context.Background()
	background := &backgroundExecTest{}
	background.init()
	query := `select hex(root_id),mode,state,cast(cleanup_after as varchar),last_error
from mo_catalog.mo_lifecycle_cleanup_roots where owner_account_id = 17
order by updated_at desc,root_id desc limit 1000 offset 0`
	execResult := &MysqlResultSet{}
	for _, column := range lifecycleJobShowColumns {
		execResult.AddColumn(column)
	}
	background.sql2result[query] = execResult
	stub := gostub.StubFunc(&NewBackgroundExec, background)
	t.Cleanup(stub.Reset)

	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	ses.SetTenantInfo(&TenantInfo{TenantID: 17})
	ses.mrs = &MysqlResultSet{}

	require.NoError(t, handleShowLifecycle(ctx, ses, &tree.ShowLifecycle{
		Kind: tree.ShowLifecycleJobs,
	}))
	require.Equal(t, uint64(5), ses.mrs.GetColumnCount())
	require.Zero(t, ses.mrs.GetRowCount())

	ses.mrs = &MysqlResultSet{}
	err := handleShowLifecycle(ctx, ses, &tree.ShowLifecycle{})
	require.ErrorContains(t, err, "unknown SHOW LIFECYCLE kind")
}

func TestHandleShowLifecycleRestoresIsBoundedAndTenantScoped(t *testing.T) {
	ctx := context.Background()
	background := &backgroundExecTest{}
	background.init()
	query := fmt.Sprintf(`select hex(restore_id),scope,dataset_count,source_logical_table_id,
case lifecycle_column_type when %d then 'DATE' when %d then 'DATETIME'
when %d then 'TIMESTAMP' else concat('UNKNOWN(',cast(lifecycle_column_type as varchar),')') end,
cast(range_start as varchar),cast(range_end as varchar),target_database_id,
target_name,state,next_chunk_ordinal,total_chunk_count,restored_rows,
cast(deadline as varchar),coalesce(last_error,''),cast(updated_at as varchar)
from mo_catalog.mo_lifecycle_restore_attempts
order by updated_at desc,restore_id desc limit 2 offset 4`,
		types.T_date,
		types.T_datetime,
		types.T_timestamp,
	)
	execResult := &MysqlResultSet{}
	for _, column := range lifecycleRestoreShowColumns {
		execResult.AddColumn(column)
	}
	row := []interface{}{
		"00112233", "RANGE", uint64(3), uint64(42), "TIMESTAMP", "100", "200",
		uint64(7), "events_q1", "IMPORTING", uint64(5), uint64(8),
		uint64(1000), "2026-08-10 12:00:00", "", "2026-08-09 12:00:00",
	}
	execResult.AddRow(row)
	background.sql2result[query] = execResult
	stub := gostub.StubFunc(&NewBackgroundExec, background)
	t.Cleanup(stub.Reset)

	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	ses.SetTenantInfo(&TenantInfo{TenantID: 17})
	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowLifecycle(ctx, ses, &tree.ShowLifecycle{
		Kind: tree.ShowLifecycleRestores,
		Page: tree.NewLimit(
			tree.NewNumVal(int64(4), "4", false, tree.P_int64),
			tree.NewNumVal(int64(2), "2", false, tree.P_int64),
		),
	}))
	require.Equal(t, uint64(len(lifecycleRestoreShowColumns)), ses.mrs.GetColumnCount())
	require.Equal(t, uint64(1), ses.mrs.GetRowCount())
	actual, err := ses.mrs.GetRow(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, row, actual)
}

func TestHandleShowLifecycleBindingAndDatasetsResolveExactTable(t *testing.T) {
	tests := []struct {
		name    string
		kind    tree.ShowLifecycleKind
		query   string
		columns []Column
		row     []interface{}
	}{
		{
			name: "binding",
			kind: tree.ShowLifecycleBinding,
			query: `select action,state,expire_after_days,stage_id,purge_after_days,binding_generation,cast(updated_at as varchar)
from mo_catalog.mo_lifecycle_bindings where account_id = 17 and physical_table_id = 42`,
			columns: lifecycleBindingShowColumns,
			row: []interface{}{
				"DELETE", "ACTIVE", uint64(30), nil, nil, uint64(7), "2026-08-04 12:00:00",
			},
		},
		{
			name: "datasets",
			kind: tree.ShowLifecycleDatasets,
			query: `select hex(dataset_id),state,row_count,logical_bytes,cast(purge_eligible_at as varchar),manifest_key
from mo_catalog.mo_lifecycle_datasets where account_id = 17 and logical_table_id = 42
order by created_at desc,dataset_id desc limit 1000 offset 0`,
			columns: lifecycleDatasetShowColumns,
			row: []interface{}{
				"00112233", "PUBLISHED", uint64(100), uint64(2048), "2027-08-04 12:00:00", "prefix/manifest.json",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			ses := newSes(nil, ctrl)
			ses.SetTenantInfo(&TenantInfo{TenantID: 17})
			ses.mrs = &MysqlResultSet{}

			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
			storage := mock_frontend.NewMockEngine(ctrl)
			database := mock_frontend.NewMockDatabase(ctrl)
			relation := mock_frontend.NewMockRelation(ctrl)
			tableDef := lifecycleTableDef(types.T_timestamp)
			storage.EXPECT().Database(gomock.Any(), "mo_catalog", txnOperator).
				Return(database, nil)
			database.EXPECT().Relation(gomock.Any(), "events", nil).
				Return(relation, nil)
			relation.EXPECT().GetTableDef(gomock.Any()).Return(tableDef)
			relation.EXPECT().GetTableID(gomock.Any()).Return(tableDef.TblId)
			ses.txnHandler = &TxnHandler{storage: storage, txnOp: txnOperator}
			ses.GetTxnCompileCtx().SetExecCtx(&ExecCtx{reqCtx: ctx, ses: ses})

			background := &backgroundExecTest{}
			background.init()
			execResult := &MysqlResultSet{}
			for _, column := range test.columns {
				execResult.AddColumn(column)
			}
			execResult.AddRow(test.row)
			background.sql2result[test.query] = execResult
			stub := gostub.StubFunc(&NewBackgroundExec, background)
			t.Cleanup(stub.Reset)

			table := tree.NewTableName(
				"events",
				tree.ObjectNamePrefix{SchemaName: "mo_catalog", ExplicitSchema: true},
				nil,
			)
			require.NoError(t, handleShowLifecycle(ctx, ses, &tree.ShowLifecycle{
				Kind:  test.kind,
				Table: table,
			}))
			require.Equal(t, uint64(1), ses.mrs.GetRowCount())
			row, err := ses.mrs.GetRow(ctx, 0)
			require.NoError(t, err)
			require.Equal(t, test.row, row)
		})
	}
}

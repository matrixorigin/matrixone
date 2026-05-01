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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestAcquireReleaseBuffer(t *testing.T) {
	t.Run("nil pool allocates fresh buffer", func(t *testing.T) {
		buf := acquireBuffer(nil)
		require.NotNil(t, buf)
		buf.WriteString("x")
		releaseBuffer(nil, buf)
		require.Zero(t, buf.Len())
	})

	t.Run("pool buffer is reset and reused", func(t *testing.T) {
		pool := &sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		}
		buf := acquireBuffer(pool)
		buf.WriteString("payload")
		releaseBuffer(pool, buf)

		reused := acquireBuffer(pool)
		require.Zero(t, reused.Len())
		releaseBuffer(pool, reused)
	})
}

func TestNewEmitter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stop, err := newEmitter(ctx, make(chan struct{}), make(chan batchWithKind, 1))(batchWithKind{})
	require.False(t, stop)
	require.ErrorIs(t, err, context.Canceled)

	stopCh := make(chan struct{})
	close(stopCh)
	stop, err = newEmitter(context.Background(), stopCh, make(chan batchWithKind, 1))(batchWithKind{})
	require.True(t, stop)
	require.NoError(t, err)

	retCh := make(chan batchWithKind, 1)
	wrapped := batchWithKind{kind: diffInsert, side: diffSideTarget}
	stop, err = newEmitter(context.Background(), make(chan struct{}), retCh)(wrapped)
	require.False(t, stop)
	require.NoError(t, err)
	require.Equal(t, wrapped, <-retCh)
}

func TestContainsDataBranchTempTableName(t *testing.T) {
	require.True(t, containsDataBranchTempTableName("delete from test.__mo_diff_del_merge_1"))
	require.True(t, containsDataBranchTempTableName("insert into __mo_diff_ins_merge_1 values (1)"))
	require.False(t, containsDataBranchTempTableName("select '__mo_diff_del_merge_1'"))
}

func TestQuoteSQLIdentifier(t *testing.T) {
	require.Equal(t, "`plain`", quoteSQLIdentifier("plain"))
	require.Equal(t, "`a``b`", quoteSQLIdentifier("a`b"))
}

func TestDataBranchPitrEntryValidation(t *testing.T) {
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()

	err := createDataBranchTablePitr(ctx, nil, bh, cloneReceipt{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires target table")

	err = createDataBranchTablePitr(ctx, nil, bh, cloneReceipt{
		dstDb:     "db",
		dstTbl:    "tbl",
		toAccount: 7,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires background transaction")

	err = createDataBranchDatabasePitr(ctx, nil, bh, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires clone database statement")

	err = createDataBranchDatabasePitr(ctx, nil, bh, &tree.CloneDatabase{}, []cloneReceipt{{toAccount: 7}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires target database")

	err = createDataBranchDatabasePitr(ctx, nil, bh, &tree.CloneDatabase{
		DstDatabase: tree.Identifier("dst"),
	}, []cloneReceipt{{toAccount: 7}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires background transaction")
}

func TestDataBranchPitrAccountName(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.SetTenantInfo(&TenantInfo{
		Tenant:   "tenant_42",
		TenantID: 42,
	})

	bh := &backgroundExecTest{}
	bh.init()

	name, err := dataBranchPitrAccountName(ctx, ses, bh, sysAccountID)
	require.NoError(t, err)
	require.Equal(t, sysAccountName, name)

	name, err = dataBranchPitrAccountName(ctx, ses, bh, 42)
	require.NoError(t, err)
	require.Equal(t, "tenant_42", name)

	lookupSQL := "select account_name from mo_catalog.mo_account where account_id = 99"
	bh.sql2result[lookupSQL] = dataBranchTestResultSet(
		[]defines.MysqlType{defines.MYSQL_TYPE_VARCHAR},
		[][]interface{}{{"tenant_99"}},
	)
	name, err = dataBranchPitrAccountName(ctx, ses, bh, 99)
	require.NoError(t, err)
	require.Equal(t, "tenant_99", name)

	bh.sql2result[lookupSQL] = dataBranchTestResultSet(
		[]defines.MysqlType{defines.MYSQL_TYPE_VARCHAR},
		nil,
	)
	_, err = dataBranchPitrAccountName(ctx, ses, bh, 99)
	require.Error(t, err)
	require.Contains(t, err.Error(), "account 99 does not exist")
}

func TestDataBranchPitrRecordSQLPaths(t *testing.T) {
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()

	record := dataBranchPitrRecord{
		pitrName:     dataBranchPitrName("table", "77"),
		level:        "table",
		accountID:    7,
		accountName:  "acc_7",
		databaseName: "db`x",
		tableName:    "tbl'x",
		objectID:     77,
	}

	selectSQL := fmt.Sprintf(
		"select kind, pitr_status, pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_name = %s and create_account = %d",
		quoteSQLStringLiteral(record.pitrName),
		record.accountID,
	)
	bh.sql2result[selectSQL] = dataBranchTestResultSet(
		[]defines.MysqlType{
			defines.MYSQL_TYPE_VARCHAR,
			defines.MYSQL_TYPE_LONGLONG,
			defines.MYSQL_TYPE_LONGLONG,
			defines.MYSQL_TYPE_VARCHAR,
		},
		[][]interface{}{{dataBranchPitrKind, uint64(0), uint64(1), "h"}},
	)

	existing, err := getDataBranchPitrRecord(ctx, bh, record.pitrName, record.accountID)
	require.NoError(t, err)
	require.True(t, existing.exists)
	require.False(t, existing.active)
	require.Equal(t, dataBranchPitrKind, existing.kind)
	require.Equal(t, uint64(1), existing.pitrLength)
	require.Equal(t, "h", existing.pitrUnit)

	err = ensureDataBranchPitrRecord(ctx, bh, 123, record, existing)
	require.NoError(t, err)
	requireExecutedSQLContains(t, bh.executedSQLs, "pitr_status_changed_time = 123")
	requireExecutedSQLContains(t, bh.executedSQLs, "pitr_length = 1")
	requireExecutedSQLContains(t, bh.executedSQLs, "pitr_unit = 'y'")
	requireExecutedSQLContains(t, bh.executedSQLs, "database_name = 'db`x'")
	requireExecutedSQLContains(t, bh.executedSQLs, "table_name = 'tbl''x'")

	sql, err := getSqlForCreateDataBranchPitrRecord(ctx, "pitr-id", 456, record)
	require.NoError(t, err)
	require.Contains(t, sql, "kind) values")
	require.Contains(t, sql, "'__mo_data_branch_pitr_table_77'")
	require.Contains(t, sql, "'internal'")
	require.Contains(t, sql, "'tbl''x'")
}

func TestCreateDataBranchPitrRecordRefreshesExistingRecords(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(100, 0).ToTimestamp()).AnyTimes()

	bh := &backgroundExecTest{}
	bh.init()

	record := dataBranchPitrRecord{
		pitrName:     dataBranchPitrName("table", "88"),
		level:        "table",
		accountID:    8,
		accountName:  "acc_8",
		databaseName: "db",
		tableName:    "tbl",
		objectID:     88,
	}

	recordSelectSQL := fmt.Sprintf(
		"select kind, pitr_status, pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_name = %s and create_account = %d",
		quoteSQLStringLiteral(record.pitrName),
		record.accountID,
	)
	bh.sql2result[recordSelectSQL] = dataBranchTestResultSet(
		[]defines.MysqlType{
			defines.MYSQL_TYPE_VARCHAR,
			defines.MYSQL_TYPE_LONGLONG,
			defines.MYSQL_TYPE_LONGLONG,
			defines.MYSQL_TYPE_VARCHAR,
		},
		[][]interface{}{{dataBranchPitrKind, uint64(0), uint64(1), "h"}},
	)

	sysSelectSQL := fmt.Sprintf(
		"select pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_name = %s",
		quoteSQLStringLiteral(SYSMOCATALOGPITR),
	)
	bh.sql2result[sysSelectSQL] = dataBranchTestResultSet(
		[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_VARCHAR},
		[][]interface{}{{uint64(2), "y"}},
	)

	err := createDataBranchPitrRecord(ctx, nil, bh, txnOp, record)
	require.NoError(t, err)
	requireExecutedSQLContains(t, bh.executedSQLs, "update mo_catalog.mo_pitr set modified_time =")
	requireExecutedSQLContains(t, bh.executedSQLs, "pitr_status_changed_time =")
	requireExecutedSQLContains(t, bh.executedSQLs, "where pitr_name = '__mo_data_branch_pitr_table_88' and create_account = 8")
	requireExecutedSQLContains(t, bh.executedSQLs, "where pitr_name = 'sys_mo_catalog_pitr' and create_account = 0")
}

func TestEnsureDataBranchMoCatalogPitrUpdatesDurationWhenNeeded(t *testing.T) {
	ctx := context.Background()
	selectSQL := fmt.Sprintf(
		"select pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_name = %s",
		quoteSQLStringLiteral(SYSMOCATALOGPITR),
	)

	t.Run("extends short existing pitr", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[selectSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_VARCHAR},
			[][]interface{}{{uint64(1), "h"}},
		)

		err := ensureDataBranchMoCatalogPitr(ctx, nil, bh, nil, 123)
		require.NoError(t, err)
		requireExecutedSQLContains(t, bh.executedSQLs, "pitr_length = 1")
		requireExecutedSQLContains(t, bh.executedSQLs, "pitr_unit = 'y'")
	})

	t.Run("keeps longer existing pitr duration", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[selectSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_VARCHAR},
			[][]interface{}{{uint64(2), "y"}},
		)

		err := ensureDataBranchMoCatalogPitr(ctx, nil, bh, nil, 456)
		require.NoError(t, err)
		lastSQL := bh.executedSQLs[len(bh.executedSQLs)-1]
		require.Contains(t, lastSQL, "modified_time = 456")
		require.NotContains(t, lastSQL, "pitr_length")
		require.NotContains(t, lastSQL, "pitr_unit")
	})
}

func TestDataBranchPitrCleanupHelpers(t *testing.T) {
	ctx := context.Background()

	t.Run("cleanup sys pitr when it is the last internal pitr", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		selectSQL := fmt.Sprintf(
			"select pitr_id from mo_catalog.mo_pitr where pitr_name != %s limit 1",
			quoteSQLStringLiteral(SYSMOCATALOGPITR),
		)
		bh.sql2result[selectSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_UUID},
			nil,
		)

		err := cleanupDataBranchMoCatalogPitrIfUnused(ctx, bh)
		require.NoError(t, err)
		requireExecutedSQLContains(t, bh.executedSQLs, "delete from mo_catalog.mo_pitr where pitr_name = 'sys_mo_catalog_pitr'")
		requireExecutedSQLContains(t, bh.executedSQLs, "kind = 'internal'")
	})

	t.Run("read database pitr for cleanup", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		selectSQL := fmt.Sprintf(
			"select create_account, obj_id, database_name, create_time from mo_catalog.mo_pitr where create_account = %d and pitr_name = %s and level = %s limit 1",
			uint64(7),
			quoteSQLStringLiteral(dataBranchPitrName("database", "55")),
			quoteSQLStringLiteral("database"),
		)
		bh.sql2result[selectSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{
				defines.MYSQL_TYPE_LONGLONG,
				defines.MYSQL_TYPE_LONGLONG,
				defines.MYSQL_TYPE_VARCHAR,
				defines.MYSQL_TYPE_LONGLONG,
			},
			[][]interface{}{{uint64(7), uint64(55), "db1", int64(999)}},
		)

		pitr, found, err := getDataBranchDatabasePitrForCleanup(ctx, bh, 7, 55)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, uint64(7), pitr.accountID)
		require.Equal(t, uint64(55), pitr.databaseID)
		require.Equal(t, "db1", pitr.database)
		require.Equal(t, int64(999), pitr.createTime)
	})

	t.Run("read table ids at database pitr timestamp", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		selectSQL := fmt.Sprintf(
			"select rel_id from mo_catalog.mo_tables {MO_TS = %d} where account_id = %d and reldatabase = %s",
			int64(999),
			uint64(7),
			quoteSQLStringLiteral("db1"),
		)
		bh.sql2result[selectSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG},
			[][]interface{}{{uint64(101)}, {uint64(102)}},
		)

		tableIDs, err := dataBranchDatabaseTableIDsAt(ctx, bh, 7, "db1", 999)
		require.NoError(t, err)
		require.Equal(t, []uint64{101, 102}, tableIDs)
	})

	t.Run("delete table pitr when no branch references remain", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		refSQL := "select table_id from mo_catalog.mo_branch_metadata where table_deleted = false and (table_id in (88) or p_table_id in (88)) limit 1"
		bh.sql2result[refSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG},
			nil,
		)
		sysPitrRefSQL := fmt.Sprintf(
			"select pitr_id from mo_catalog.mo_pitr where pitr_name != %s limit 1",
			quoteSQLStringLiteral(SYSMOCATALOGPITR),
		)
		bh.sql2result[sysPitrRefSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_UUID},
			[][]interface{}{{"pitr-id"}},
		)

		err := cleanupDataBranchTablePitrIfUnused(ctx, bh, 7, 88)
		require.NoError(t, err)
		requireExecutedSQLContains(t, bh.executedSQLs, "delete from mo_catalog.mo_pitr where pitr_name = '__mo_data_branch_pitr_table_88' and create_account = 7")
	})

	t.Run("keep table pitr when active branch still references it", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		refSQL := "select table_id from mo_catalog.mo_branch_metadata where table_deleted = false and (table_id in (88) or p_table_id in (88)) limit 1"
		bh.sql2result[refSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG},
			[][]interface{}{{uint64(88)}},
		)

		err := cleanupDataBranchTablePitrIfUnused(ctx, bh, 7, 88)
		require.NoError(t, err)
		for _, sql := range bh.executedSQLs {
			require.NotContains(t, sql, "delete from mo_catalog.mo_pitr")
		}
	})

	t.Run("delete database pitr when no table reference remains", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		refSQL := "select table_id from mo_catalog.mo_branch_metadata where table_deleted = false and (table_id in (101,102) or p_table_id in (101,102)) limit 1"
		bh.sql2result[refSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG},
			nil,
		)
		sysPitrRefSQL := fmt.Sprintf(
			"select pitr_id from mo_catalog.mo_pitr where pitr_name != %s limit 1",
			quoteSQLStringLiteral(SYSMOCATALOGPITR),
		)
		bh.sql2result[sysPitrRefSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_UUID},
			[][]interface{}{{"pitr-id"}},
		)

		err := cleanupDataBranchDatabasePitrIfUnused(ctx, bh, 7, 55, []uint64{101, 102})
		require.NoError(t, err)
		requireExecutedSQLContains(t, bh.executedSQLs, "delete from mo_catalog.mo_pitr where pitr_name = '__mo_data_branch_pitr_database_55' and create_account = 7")
	})

	t.Run("keep database pitr when active branch still references a table", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		refSQL := "select table_id from mo_catalog.mo_branch_metadata where table_deleted = false and (table_id in (101) or p_table_id in (101)) limit 1"
		bh.sql2result[refSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG},
			[][]interface{}{{uint64(101)}},
		)

		err := cleanupDataBranchDatabasePitrIfUnused(ctx, bh, 7, 55, []uint64{101})
		require.NoError(t, err)
		for _, sql := range bh.executedSQLs {
			require.NotContains(t, sql, "delete from mo_catalog.mo_pitr")
		}
	})

	t.Run("delete database pitr by id after resolving tables at branch timestamp", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		pitrSQL := fmt.Sprintf(
			"select create_account, obj_id, database_name, create_time from mo_catalog.mo_pitr where create_account = %d and pitr_name = %s and level = %s limit 1",
			uint64(7),
			quoteSQLStringLiteral(dataBranchPitrName("database", "66")),
			quoteSQLStringLiteral("database"),
		)
		bh.sql2result[pitrSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{
				defines.MYSQL_TYPE_LONGLONG,
				defines.MYSQL_TYPE_LONGLONG,
				defines.MYSQL_TYPE_VARCHAR,
				defines.MYSQL_TYPE_LONGLONG,
			},
			[][]interface{}{{uint64(7), uint64(66), "db2", int64(1001)}},
		)
		tablesSQL := fmt.Sprintf(
			"select rel_id from mo_catalog.mo_tables {MO_TS = %d} where account_id = %d and reldatabase = %s",
			int64(1001),
			uint64(7),
			quoteSQLStringLiteral("db2"),
		)
		bh.sql2result[tablesSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG},
			[][]interface{}{{uint64(201)}},
		)
		refSQL := "select table_id from mo_catalog.mo_branch_metadata where table_deleted = false and (table_id in (201) or p_table_id in (201)) limit 1"
		bh.sql2result[refSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG},
			nil,
		)
		sysPitrRefSQL := fmt.Sprintf(
			"select pitr_id from mo_catalog.mo_pitr where pitr_name != %s limit 1",
			quoteSQLStringLiteral(SYSMOCATALOGPITR),
		)
		bh.sql2result[sysPitrRefSQL] = dataBranchTestResultSet(
			[]defines.MysqlType{defines.MYSQL_TYPE_UUID},
			[][]interface{}{{"pitr-id"}},
		)

		err := cleanupDataBranchDatabasePitrByIDIfUnused(ctx, bh, 7, 66)
		require.NoError(t, err)
		requireExecutedSQLContains(t, bh.executedSQLs, "delete from mo_catalog.mo_pitr where pitr_name = '__mo_data_branch_pitr_database_66' and create_account = 7")
	})
}

func TestRunSQL_BackgroundExecPaths(t *testing.T) {
	ses := newValidateSession(t)

	t.Run("converts mysql result set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), "drop database test_db").Return(nil).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{buildRunSQLResultSet()}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		ret, err := runSql(context.Background(), ses, bh, "drop database test_db", nil, nil)
		require.NoError(t, err)
		require.Len(t, ret.Batches, 1)
		require.Equal(t, 1, ret.Batches[0].RowCount())
		require.Equal(t, int64(7), vectorValueAsInt64(ret.Batches[0], 0, 0))
		ret.Close()
	})

	t.Run("rejects unexpected result set type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), "drop database bad_db").Return(nil).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{"bad-result"}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		_, err := runSql(context.Background(), ses, bh, "drop database bad_db", nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected result set type")
	})
}

func TestRunSQL_DataBranchTempTablesUseBackgroundExec(t *testing.T) {
	ses := newValidateSession(t)
	spyExec := &pickStreamingExecutor{err: moerr.NewTxnNeedRetryWithDefChangedNoCtx()}
	_ = newPickStreamingBackExecForTest(t, ses, spyExec)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	stmt := "delete from test.__mo_diff_ins_merge_1"
	bh.EXPECT().Exec(gomock.Any(), stmt).Return(nil).Times(1)
	bh.EXPECT().GetExecResultSet().Return(nil).Times(1)
	bh.EXPECT().ClearExecResultSet().Times(1)

	_, err := runSql(context.Background(), ses, bh, stmt, nil, nil)
	require.NoError(t, err)
	require.Empty(t, spyExec.sql)
}

func TestScanSnapshotRelationByID_EarlyAndErrorPaths(t *testing.T) {
	ses := newValidateSession(t)

	t.Run("empty attrs returns nil", func(t *testing.T) {
		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			nil,
			nil,
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.NoError(t, err)
	})

	t.Run("attrs and col types mismatch", func(t *testing.T) {
		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "attrs/colTypes length mismatch")
	})

	t.Run("propagates get relation error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("get relation failed")
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(7)).
			Return("", "", nil, wantErr).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("returns error when range relation is missing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(7)).
			Return("", "", nil, nil).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot resolve range relation")
	})

	t.Run("propagates ranges error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()

		rangeRel := mock_frontend.NewMockRelation(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("ranges failed")
		rangeRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).
			Return(nil, wantErr).
			Times(1)

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(7)).
			Return("", "", rangeRel, nil).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.ErrorIs(t, err, wantErr)
	})
}

func buildRunSQLResultSet() *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col := &MysqlColumn{}
	col.SetName("id")
	col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col)
	mrs.AddRow([]interface{}{int64(7)})
	return mrs
}

func vectorValueAsInt64(bat *batch.Batch, colIdx int, rowIdx int) int64 {
	return vector.MustFixedColWithTypeCheck[int64](bat.Vecs[colIdx])[rowIdx]
}

func dataBranchTestResultSet(colTypes []defines.MysqlType, rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for i, colType := range colTypes {
		col := &MysqlColumn{}
		col.SetName(fmt.Sprintf("c%d", i))
		col.SetColumnType(colType)
		mrs.AddColumn(col)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

func requireExecutedSQLContains(t *testing.T, sqls []string, substr string) {
	t.Helper()
	for _, sql := range sqls {
		if strings.Contains(sql, substr) {
			return
		}
	}
	require.Failf(t, "expected executed SQL to contain substring", "substring: %s\nsqls:\n%s", substr, strings.Join(sqls, "\n"))
}

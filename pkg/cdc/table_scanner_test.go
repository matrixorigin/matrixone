// Copyright 2021 Matrix Origin
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

package cdc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
)

func TestGetTableScanner(t *testing.T) {
	gostub.Stub(&getSqlExecutor, func(cnUUID string) executor.SQLExecutor {
		return &mock_executor.MockSQLExecutor{}
	})
	assert.NotNil(t, GetTableDetector("cnUUID"))
}

func TestApplyTableDetectorOptions(t *testing.T) {
	opts := applyTableDetectorOptions(
		WithTableDetectorSlowThreshold(time.Millisecond),
		WithTableDetectorPrintInterval(2*time.Millisecond),
		WithTableDetectorCleanupPeriod(3*time.Millisecond),
		WithTableDetectorCleanupWarnThreshold(4*time.Millisecond),
	)
	assert.Equal(t, time.Millisecond, opts.SlowThreshold)
	assert.Equal(t, 2*time.Millisecond, opts.PrintInterval)
	assert.Equal(t, 3*time.Millisecond, opts.CleanupPeriod)
	assert.Equal(t, 4*time.Millisecond, opts.CleanupWarnThreshold)

	defaultOpts := applyTableDetectorOptions()
	assert.Equal(t, defaultSlowThreshold, defaultOpts.SlowThreshold)
	assert.Equal(t, defaultPrintInterval, defaultOpts.PrintInterval)
	assert.Equal(t, defaultWatermarkCleanupPeriod, defaultOpts.CleanupPeriod)
	assert.Equal(t, defaultCleanupWarnThreshold, defaultOpts.CleanupWarnThreshold)
}

func TestTableScanner1(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"tblName"}, nil)
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"dbName"}, nil)
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{"createSql"}, nil)
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil)
	bat.SetRowCount(1)
	res := executor.Result{
		Mp:      testutil.TestUtilMp,
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(res, nil)

	td := newTableDetector(
		mockSqlExecutor,
		WithTableDetectorCleanupPeriod(time.Hour),
	)
	defer td.Close()

	td.Register("id1", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error { return nil })
	assert.Equal(t, 1, len(td.Callbacks))
	td.Register("id2", 2, []string{"db2"}, []string{"tbl2"}, func(mp map[uint32]TblMap) error { return nil })
	assert.Equal(t, 2, len(td.Callbacks))
	assert.Equal(t, 2, len(td.SubscribedAccountIds))

	td.Register("id3", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error { return nil })
	assert.Equal(t, 3, len(td.Callbacks))
	assert.Equal(t, 2, len(td.SubscribedAccountIds))
	assert.Equal(t, 2, len(td.SubscribedDbNames["db1"]))
	assert.Equal(t, []string{"id1", "id3"}, td.SubscribedDbNames["db1"])

	td.UnRegister("id1")
	assert.Equal(t, 2, len(td.Callbacks))
	assert.Equal(t, 2, len(td.SubscribedAccountIds))
	assert.Equal(t, 1, len(td.SubscribedDbNames["db1"]))
	assert.Equal(t, []string{"id3"}, td.SubscribedDbNames["db1"])

	td.UnRegister("id2")
	assert.Equal(t, 1, len(td.Callbacks))
	assert.Equal(t, 1, len(td.SubscribedAccountIds))

	td.UnRegister("id3")
	assert.Equal(t, 0, len(td.Callbacks))
	assert.Equal(t, 0, len(td.SubscribedAccountIds))
	assert.Equal(t, 0, len(td.SubscribedDbNames))

	td.Register("id4", 1, []string{"db4"}, []string{"tbl4"}, func(mp map[uint32]TblMap) error { return nil })
	assert.Equal(t, 1, len(td.Callbacks))
	assert.Equal(t, 1, len(td.SubscribedAccountIds))

	err := td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(td.Mp))

	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQL("1", "'db4'", "'tbl4'"),
		executor.Options{}.WithStatementOption(executor.StatementOption{}.WithDisableLog()),
	).Return(executor.Result{}, moerr.NewInternalErrorNoCtx("mock error")).AnyTimes()

	err = td.scanTable()
	assert.Error(t, err)
	assert.Equal(t, 1, len(td.Mp))

	td.UnRegister("id4")
	assert.Equal(t, 0, len(td.Callbacks))
	assert.Equal(t, 0, len(td.SubscribedAccountIds))

	err = td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(td.Mp))
}

func Test_CollectTableInfoSQL(t *testing.T) {
	var builder cdcSQLBuilder
	sql := builder.CollectTableInfoSQL("1,2,3", "*", "*")
	sql = strings.ToUpper(sql)
	t.Log(sql)
	expected := "SELECT  REL_ID,  RELNAME,  RELDATABASE_ID,  " +
		"RELDATABASE,  REL_CREATESQL,  ACCOUNT_ID " +
		"FROM `MO_CATALOG`.`MO_TABLES` " +
		"WHERE  ACCOUNT_ID IN (1,2,3)  AND RELKIND = 'R'  " +
		"AND RELDATABASE NOT IN ('INFORMATION_SCHEMA','MO_CATALOG','MO_DEBUG','MO_TASK','MYSQL','SYSTEM','SYSTEM_METRICS')"
	assert.Equal(t, expected, sql)

	sql = builder.CollectTableInfoSQL("0", "'source_db'", "'orders'")
	expected = "SELECT  REL_ID,  RELNAME,  RELDATABASE_ID,  RELDATABASE,  REL_CREATESQL,  ACCOUNT_ID FROM `MO_CATALOG`.`MO_TABLES` WHERE  ACCOUNT_ID IN (0)  AND RELDATABASE IN ('SOURCE_DB')  AND RELNAME IN ('ORDERS')  AND RELKIND = 'R'  AND RELDATABASE NOT IN ('INFORMATION_SCHEMA','MO_CATALOG','MO_DEBUG','MO_TASK','MYSQL','SYSTEM','SYSTEM_METRICS')"
	assert.Equal(t, strings.ToUpper(expected), strings.ToUpper(sql))
}

func TestScanAndProcess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	td := newTableDetector(
		nil,
		WithTableDetectorCleanupPeriod(time.Hour),
	)
	defer td.Close()

	tables := map[uint32]TblMap{
		1: {
			"db1.tbl1": &DbTableInfo{
				SourceDbId:      1,
				SourceDbName:    "db1",
				SourceTblId:     1001,
				SourceTblName:   "tbl1",
				SourceCreateSql: "create table tbl1 (a int)",
				IdChanged:       false,
			},
		},
	}
	scanCount := 0
	td.scanTableFn = func() error {
		td.mu.Lock()
		if scanCount%5 == 0 {
			td.lastMp = tables
		} else {
			td.lastMp = nil
		}
		td.mu.Unlock()
		scanCount++
		return nil
	}

	fault.Enable()
	objectio.SimpleInject(objectio.FJ_CDCScanTableErr)
	rm, _ := objectio.InjectCDCScanTable("fast scan")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go td.scanTableLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	defer rm()
	fault.Disable()
}

func TestProcessCallBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	td := newTableDetector(
		nil,
		WithTableDetectorCleanupPeriod(time.Hour),
	)
	td.lastMp = make(map[uint32]TblMap)
	defer td.Close()

	tables := map[uint32]TblMap{
		1: {
			"db1.tbl1": &DbTableInfo{
				SourceDbId:      1,
				SourceDbName:    "db1",
				SourceTblId:     1001,
				SourceTblName:   "tbl1",
				SourceCreateSql: "create table tbl1 (a int)",
				IdChanged:       false,
			},
		},
	}
	td.Register("id1", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error { return moerr.NewInternalErrorNoCtx("ERR") })
	assert.Equal(t, 1, len(td.Callbacks))
	td.mu.Lock()
	td.lastMp = tables
	td.mu.Unlock()

	td.processCallback(context.Background(), tables)

	td.mu.Lock()
	defer td.mu.Unlock()

	assert.False(t, td.handling, "handling should be reset to false")

	assert.NotNil(t, td.lastMp, "lastMp should not be cleared on error")
	assert.Equal(t, tables, td.lastMp, "lastMp should remain unchanged")
}

func TestTableDetectorCleanupWatermarks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mock_executor.NewMockSQLExecutor(ctrl)
	td := newTableDetector(
		mockExec,
		WithTableDetectorCleanupWarnThreshold(time.Millisecond),
	)
	defer td.Close()

	td.SubscribedAccountIds[1] = []string{"task1"}

	mockExec.EXPECT().
		Exec(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
			assert.Contains(t, sql, "DELETE w FROM")
			assert.Contains(t, sql, "account_id")
			return executor.Result{AffectedRows: 5}, nil
		}).
		Times(1)

	td.cleanupOrphanWatermarks(context.Background())
}

func TestTableDetectorCleanupWatermarksNoAccounts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mock_executor.NewMockSQLExecutor(ctrl)
	td := newTableDetector(
		mockExec,
		WithTableDetectorCleanupPeriod(time.Hour),
	)
	defer td.Close()

	mockExec.EXPECT().
		Exec(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
			assert.Contains(t, sql, "DELETE w FROM")
			assert.NotContains(t, sql, "WHERE w.account_id IN")
			return executor.Result{AffectedRows: 0}, nil
		}).
		Times(1)

	td.cleanupOrphanWatermarks(context.Background())
}

func TestTableScanner_UpdateTableInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bat1 := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat1.Vecs[0] = testutil.MakeUint64Vector([]uint64{1001}, nil)
	bat1.Vecs[1] = testutil.MakeVarcharVector([]string{"tbl1"}, nil)
	bat1.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat1.Vecs[3] = testutil.MakeVarcharVector([]string{"db1"}, nil)
	bat1.Vecs[4] = testutil.MakeVarcharVector([]string{"create table tbl1 (a int)"}, nil)
	bat1.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil)
	bat1.SetRowCount(1)
	res1 := executor.Result{
		Mp:      testutil.TestUtilMp,
		Batches: []*batch.Batch{bat1},
	}

	bat2 := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat2.Vecs[0] = testutil.MakeUint64Vector([]uint64{1002}, nil) // 新的表ID
	bat2.Vecs[1] = testutil.MakeVarcharVector([]string{"tbl1"}, nil)
	bat2.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat2.Vecs[3] = testutil.MakeVarcharVector([]string{"db1"}, nil)
	bat2.Vecs[4] = testutil.MakeVarcharVector([]string{"create table tbl1 (a int)"}, nil)
	bat2.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil)
	bat2.SetRowCount(1)
	res2 := executor.Result{
		Mp:      testutil.TestUtilMp,
		Batches: []*batch.Batch{bat2},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)

	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQL("1", "'db1'", "'tbl1'"),
		gomock.Any(),
	).Return(res1, nil)

	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQL("1", "'db1'", "'tbl1'"),
		gomock.Any(),
	).Return(res2, nil)

	td := newTableDetector(
		mockSqlExecutor,
		WithTableDetectorCleanupPeriod(time.Hour),
	)
	defer td.Close()
	td.Register("test-task", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error {
		return nil
	})

	err := td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(td.Mp))

	accountMap, ok := td.Mp[1]
	assert.True(t, ok)

	tblInfo, ok := accountMap["db1.tbl1"]
	assert.True(t, ok)
	assert.Equal(t, uint64(1001), tblInfo.SourceTblId)
	assert.False(t, tblInfo.IdChanged)

	err = td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(td.Mp))

	accountMap = td.Mp[1]
	tblInfo = accountMap["db1.tbl1"]
	assert.Equal(t, uint64(1002), tblInfo.SourceTblId)
	assert.True(t, tblInfo.IdChanged)
}

func TestTableScanner_PrintActiveRunners(t *testing.T) {
	cdcStateManager := NewCDCStateManager()
	tableInfo := &DbTableInfo{
		SourceDbId:      1,
		SourceDbName:    "db1",
		SourceTblId:     1001,
		SourceTblName:   "tbl1",
		SourceCreateSql: "create table tbl1 (a int)",
		IdChanged:       false,
	}
	cdcStateManager.AddActiveRunner(tableInfo)
	cdcStateManager.PrintActiveRunners(0)
	cdcStateManager.UpdateActiveRunner(tableInfo, types.BuildTS(1, 1), types.BuildTS(2, 2), true)
	cdcStateManager.PrintActiveRunners(0)
	cdcStateManager.UpdateActiveRunner(tableInfo, types.BuildTS(1, 1), types.BuildTS(2, 2), false)
	cdcStateManager.PrintActiveRunners(0)
	assert.Equal(t, 1, len(cdcStateManager.activeRunners))
}

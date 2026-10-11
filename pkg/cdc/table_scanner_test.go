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
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTableScanner(t *testing.T) {
	gostub.Stub(&getSqlExecutor, func(cnUUID string) executor.SQLExecutor {
		return &mock_executor.MockSQLExecutor{}
	})
	assert.NotNil(t, GetTableDetector("cnUUID"))
}

// Reset is serialized with embedded fixture lifetimes, including first use.
func TestTableDetectorResetScanInterval(t *testing.T) {
	previous := detector
	detector = nil
	once = sync.Once{}
	stub := gostub.Stub(&getSqlExecutor, func(string) executor.SQLExecutor {
		return &mock_executor.MockSQLExecutor{}
	})
	defer stub.Reset()
	defer func() {
		if detector != nil {
			detector.Close()
		}
		detector = previous
		once = sync.Once{}
		if previous != nil {
			once.Do(func() {})
		}
	}()

	ResetTableDetectorForTest("private", time.Second)
	first := detector
	require.Same(t, first, GetTableDetector("private"), "first Get must preserve the reset instance")
	require.Equal(t, time.Second, first.scanInterval)
	for _, interval := range []time.Duration{0, -time.Second} {
		ResetTableDetectorForTest("private", interval)
		require.Equal(t, defaultTableScanInterval, GetTableDetector("private").scanInterval)
	}
	ResetTableDetectorForTest("next")
	require.Equal(t, defaultTableScanInterval, GetTableDetector("next").scanInterval, "private cadence must not leak to the next fixture")
}

func TestTableDetectorPrivatePeriodicScan(t *testing.T) {
	stub := gostub.Stub(&cdcScanTableInjected, func() (string, bool) { return "", false })
	defer stub.Reset()
	td := newTableDetector(&mock_executor.MockSQLExecutor{})
	td.scanInterval = 10 * time.Millisecond
	td.scanTableFn = func() error {
		td.mu.Lock()
		td.Mp = map[uint32]TblMap{1: {"db.tbl": {SourceDbName: "db", SourceTblName: "tbl"}}}
		td.mu.Unlock()
		return nil
	}
	defer func() {
		td.Close()
		waitUntil(t, func() bool {
			td.mu.Lock()
			defer td.mu.Unlock()
			return !td.loopRunning.Load() && !td.handling
		}, time.Second, "private detector did not stop")
	}()
	observed := make(chan map[uint32]TblMap, 1)
	require.True(t, td.RegisterIfAbsent("periodic", 1, []string{"db"}, []string{"tbl"}, func(tables map[uint32]TblMap) error {
		select {
		case observed <- tables:
		default:
		}
		return nil
	}))
	select {
	case tables := <-observed:
		require.Equal(t, "tbl", tables[1]["db.tbl"].SourceTblName)
	case <-time.After(time.Second):
		t.Fatal("periodic scan did not deliver the registered table without a manual scan")
	}
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
	assert.Equal(t, DefaultSlowThreshold, defaultOpts.SlowThreshold)
	assert.Equal(t, DefaultPrintInterval, defaultOpts.PrintInterval)
	assert.Equal(t, DefaultWatermarkCleanupPeriod, defaultOpts.CleanupPeriod)
	assert.Equal(t, DefaultCleanupWarnThreshold, defaultOpts.CleanupWarnThreshold)
}

func makeConstraintSQLValue(t *testing.T, constraints ...engine.Constraint) string {
	t.Helper()

	if len(constraints) == 0 {
		return ""
	}
	data, err := (&engine.ConstraintDef{Cts: constraints}).MarshalBinary()
	require.NoError(t, err)
	return string(data)
}

func makeForeignKeyConstraintSQLValue(t *testing.T) string {
	t.Helper()

	return makeConstraintSQLValue(t, &engine.ForeignKeyDef{
		Fkeys: []*plan.ForeignKeyDef{
			{
				Name:       "fk_child_parent",
				Cols:       []uint64{2},
				ForeignTbl: 1000,
				ForeignCols: []uint64{
					1,
				},
			},
		},
	})
}

func TestTableHasForeignKeyConstraint(t *testing.T) {
	hasForeignKey, err := TableHasForeignKeyConstraint(nil)
	require.NoError(t, err)
	assert.False(t, hasForeignKey)

	primaryKeyOnly := makeConstraintSQLValue(t, &engine.PrimaryKeyDef{
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	})
	hasForeignKey, err = TableHasForeignKeyConstraint([]byte(primaryKeyOnly))
	require.NoError(t, err)
	assert.False(t, hasForeignKey)

	foreignKey := makeForeignKeyConstraintSQLValue(t)
	hasForeignKey, err = TableHasForeignKeyConstraint([]byte(foreignKey))
	require.NoError(t, err)
	assert.True(t, hasForeignKey)

	_, err = TableHasForeignKeyConstraint([]byte{byte(engine.ForeignKey)})
	require.Error(t, err)
}

func TestTableScanner1(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	defer proc.Free()

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId", "constraint", "has_pk"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"tblName"}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"dbName"}, nil, proc.Mp())
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{"createSql"}, nil, proc.Mp())
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat.Vecs[6] = testutil.MakeVarcharVector([]string{""}, nil, proc.Mp())
	bat.Vecs[7] = testutil.MakeBoolVector([]bool{true}, nil, proc.Mp())
	bat.SetRowCount(1)
	res := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(res, nil)

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSqlExecutor,
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	assert.True(t, td.RegisterIfAbsent("id1", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error { return nil }))
	assert.Equal(t, 1, len(td.Callbacks))
	assert.True(t, td.RegisterIfAbsent("id2", 2, []string{"db2"}, []string{"tbl2"}, func(mp map[uint32]TblMap) error { return nil }))
	assert.Equal(t, 2, len(td.Callbacks))
	assert.Equal(t, 2, len(td.SubscribedAccountIds))

	assert.True(t, td.RegisterIfAbsent("id3", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error { return nil }))
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

	assert.True(t, td.RegisterIfAbsent("id4", 1, []string{"db4"}, []string{"tbl4"}, func(mp map[uint32]TblMap) error { return nil }))
	assert.Equal(t, 1, len(td.Callbacks))
	assert.Equal(t, 1, len(td.SubscribedAccountIds))

	err := td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(td.Mp))

	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "'db4'", "'tbl4'"),
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

func TestAuditTableScannerSkipsForeignKeyTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	defer proc.Free()

	createSQL := `CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_id BIGINT,
  FOREIGN KEY (parent_id) REFERENCES parent(id)
)`

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId", "constraint", "has_pk"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1001}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"child"}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{10}, nil, proc.Mp())
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"source_db"}, nil, proc.Mp())
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{createSQL}, nil, proc.Mp())
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat.Vecs[6] = testutil.MakeVarcharVector([]string{makeForeignKeyConstraintSQLValue(t)}, nil, proc.Mp())
	bat.Vecs[7] = testutil.MakeBoolVector([]bool{true}, nil, proc.Mp())
	bat.SetRowCount(1)
	res := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "'source_db'", "'child'"),
		gomock.Any(),
	).Return(res, nil)

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSqlExecutor,
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	td.mu.Lock()
	td.registerLocked("audit-task", 1, []string{"source_db"}, []string{"child"}, func(mp map[uint32]TblMap) error {
		return nil
	})
	td.mu.Unlock()

	err := td.scanTable()
	require.NoError(t, err)
	assert.Empty(t, td.Mp)
}

func TestTableScannerDoesNotSkipForeignKeyTextLiteral(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	defer proc.Free()

	createSQL := "CREATE TABLE child (note VARCHAR(32) DEFAULT 'foreign key')"

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId", "constraint", "has_pk"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1001}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"child"}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{10}, nil, proc.Mp())
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"source_db"}, nil, proc.Mp())
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{createSQL}, nil, proc.Mp())
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat.Vecs[6] = testutil.MakeVarcharVector([]string{""}, nil, proc.Mp())
	bat.Vecs[7] = testutil.MakeBoolVector([]bool{true}, nil, proc.Mp())
	bat.SetRowCount(1)
	res := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "'source_db'", "'child'"),
		gomock.Any(),
	).Return(res, nil)

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSqlExecutor,
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	td.mu.Lock()
	td.registerLocked("audit-task", 1, []string{"source_db"}, []string{"child"}, func(mp map[uint32]TblMap) error {
		return nil
	})
	td.mu.Unlock()

	err := td.scanTable()
	require.NoError(t, err)
	require.Contains(t, td.Mp, uint32(1))
	assert.Contains(t, td.Mp[1], "source_db.child")
}

func TestTableScannerSkipsForeignKeyMetadataWithoutCreateSQLText(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	defer proc.Free()

	createSQL := "CREATE TABLE child (id BIGINT PRIMARY KEY, parent_id BIGINT)"

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId", "constraint", "has_pk"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1001}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"child"}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{10}, nil, proc.Mp())
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"source_db"}, nil, proc.Mp())
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{createSQL}, nil, proc.Mp())
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat.Vecs[6] = testutil.MakeVarcharVector([]string{makeForeignKeyConstraintSQLValue(t)}, nil, proc.Mp())
	bat.Vecs[7] = testutil.MakeBoolVector([]bool{true}, nil, proc.Mp())
	bat.SetRowCount(1)
	res := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "'source_db'", "'child'"),
		gomock.Any(),
	).Return(res, nil)

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSqlExecutor,
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	td.mu.Lock()
	td.registerLocked("audit-task", 1, []string{"source_db"}, []string{"child"}, func(mp map[uint32]TblMap) error {
		return nil
	})
	td.mu.Unlock()

	err := td.scanTable()
	require.NoError(t, err)
	assert.Empty(t, td.Mp)
}

func TestTableScannerConstraintDecodeErrorPreservesOldTableMap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	defer proc.Free()

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId", "constraint", "has_pk"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1001, 1002}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"child", "broken"}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{10, 10}, nil, proc.Mp())
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"source_db", "source_db"}, nil, proc.Mp())
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{
		"CREATE TABLE child (id BIGINT PRIMARY KEY)",
		"CREATE TABLE broken (id BIGINT PRIMARY KEY)",
	}, nil, proc.Mp())
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1, 1}, nil, proc.Mp())
	bat.Vecs[6] = testutil.MakeVarcharVector([]string{"", string([]byte{byte(engine.ForeignKey)})}, nil, proc.Mp())
	bat.Vecs[7] = testutil.MakeBoolVector([]bool{true, true}, nil, proc.Mp())
	bat.SetRowCount(2)
	res := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "'source_db'", "*"),
		gomock.Any(),
	).Return(res, nil)

	oldInfo := &DbTableInfo{
		SourceDbId:      9,
		SourceDbName:    "source_db",
		SourceTblId:     9001,
		SourceTblName:   "child",
		SourceCreateSql: "CREATE TABLE child (old_id BIGINT PRIMARY KEY)",
	}
	td := &TableDetector{
		Mp:                   map[uint32]TblMap{1: {GenDbTblKey("source_db", "child"): oldInfo}},
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSqlExecutor,
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	td.mu.Lock()
	td.registerLocked("audit-task", 1, []string{"source_db"}, []string{"*"}, func(mp map[uint32]TblMap) error {
		return nil
	})
	td.mu.Unlock()

	err := td.scanTable()
	require.Error(t, err)
	require.Contains(t, td.Mp, uint32(1))
	gotInfo := td.Mp[1][GenDbTblKey("source_db", "child")]
	require.Same(t, oldInfo, gotInfo)
	assert.Equal(t, uint64(9), gotInfo.SourceDbId)
	assert.Equal(t, uint64(9001), gotInfo.SourceTblId)
	assert.Equal(t, "CREATE TABLE child (old_id BIGINT PRIMARY KEY)", gotInfo.SourceCreateSql)

	assert.NotContains(t, td.Mp[1], GenDbTblKey("source_db", "broken"))
}

func TestTableDetectorRegisterStartsScanAsync(t *testing.T) {
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	td.scanTableFn = func() error { return nil }
	defer td.Close()

	errCh := make(chan error, 1)
	registered := make(chan struct{})
	go func() {
		if !td.RegisterIfAbsent("async-task", 1, []string{"db"}, []string{"tbl"}, func(map[uint32]TblMap) error { return nil }) {
			errCh <- moerr.NewInternalErrorNoCtx("RegisterIfAbsent failed for async-task")
		}
		close(registered)
	}()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatalf("register blocked while starting scan loop")
	}

	select {
	case err := <-errCh:
		t.Fatalf("unexpected registration failure: %v", err)
	default:
	}

	td.mu.Lock()
	cancelSet := td.cancel != nil
	td.mu.Unlock()

	if !cancelSet {
		t.Fatalf("expected cancel function to be set after registration")
	}
}

func TestTableDetectorScanLoopSingleInstance(t *testing.T) {
	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCScanTable("fast scan")
	require.NoError(t, err)
	defer func() {
		if rmFn != nil {
			_, _ = rmFn()
		}
	}()

	var scanCalls atomic.Int32
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	td.scanTableFn = func() error {
		scanCalls.Add(1)
		return nil
	}
	defer td.Close()

	if !td.RegisterIfAbsent("task-0", 1, []string{"db"}, []string{"tbl"}, func(map[uint32]TblMap) error { return nil }) {
		t.Fatalf("RegisterIfAbsent failed for first task")
	}

	waitUntil(t, func() bool { return scanCalls.Load() > 0 }, 500*time.Millisecond, "scan loop did not start")

	firstSeq := td.loopSeq.Load()
	if firstSeq != 1 {
		t.Fatalf("expected loopSeq 1, got %d", firstSeq)
	}

	for i := 1; i < 5; i++ {
		id := fmt.Sprintf("task-%d", i)
		if !td.RegisterIfAbsent(id, uint32(i+1), []string{"db"}, []string{"tbl"}, func(map[uint32]TblMap) error { return nil }) {
			t.Fatalf("RegisterIfAbsent failed for %s", id)
		}
	}

	if td.loopSeq.Load() != firstSeq {
		t.Fatalf("expected loopSeq to remain %d, got %d", firstSeq, td.loopSeq.Load())
	}

	td.Close()
	waitUntil(t, func() bool { return !td.loopRunning.Load() }, time.Second, "scan loop did not stop after Close")
}

func TestTableDetectorProcessCallbackNoReentry(t *testing.T) {
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	defer td.Close()

	block := make(chan struct{})
	release := make(chan struct{})
	var count atomic.Int32

	cb := func(map[uint32]TblMap) error {
		count.Add(1)
		select {
		case <-block:
		default:
			close(block)
		}
		<-release
		return nil
	}

	require.True(t, td.RegisterIfAbsent("task", 1, []string{"db"}, []string{"tbl"}, cb))
	tables := map[uint32]TblMap{
		1: {"db.tbl": {SourceDbName: "db", SourceTblName: "tbl"}},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		td.processCallback(context.Background(), tables)
	}()

	<-block
	done := make(chan struct{})
	go func() {
		td.processCallback(context.Background(), tables)
		close(done)
	}()

	<-done
	if got := count.Load(); got != 1 {
		t.Fatalf("callback executed %d times", got)
	}

	close(release)
	wg.Wait()
}

func TestTableDetectorProcessCallbackUsesIndependentSnapshots(t *testing.T) {
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	defer td.Close()

	var first, second bool
	consume := func(tables map[uint32]TblMap) error {
		tables[1]["db.tbl"].SourceTblId = 8
		first = true
		return nil
	}
	observe := func(tables map[uint32]TblMap) error {
		second = tables[1]["db.tbl"].SourceTblId == 7
		return nil
	}
	require.True(t, td.RegisterIfAbsent("first", 1, []string{"db"}, []string{"tbl"}, consume))
	require.True(t, td.RegisterIfAbsent("second", 1, []string{"db"}, []string{"tbl"}, observe))

	td.processCallback(context.Background(), map[uint32]TblMap{
		1: {"db.tbl": {SourceDbName: "db", SourceTblName: "tbl", SourceTblId: 7}},
	})
	require.True(t, first)
	require.True(t, second, "one subscriber must not mutate another subscriber's source identity")
}

func TestTableDetectorRegisterDuringCallback(t *testing.T) {
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	defer td.Close()

	enter := make(chan struct{})
	release := make(chan struct{})
	var firstCount, secondCount atomic.Int32

	first := func(map[uint32]TblMap) error {
		firstCount.Add(1)
		select {
		case <-enter:
		default:
			close(enter)
		}
		<-release
		return nil
	}

	second := func(map[uint32]TblMap) error {
		secondCount.Add(1)
		return nil
	}

	require.True(t, td.RegisterIfAbsent("task-1", 1, []string{"db"}, []string{"tbl"}, first))

	tables := map[uint32]TblMap{
		1: {"db.tbl": {SourceDbName: "db", SourceTblName: "tbl"}},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		td.processCallback(context.Background(), tables)
	}()

	<-enter
	regDone := make(chan struct{})
	go func() {
		did := td.RegisterIfAbsent("task-2", 1, []string{"db"}, []string{"tbl"}, second)
		if !did {
			panic("register failed")
		}
		close(regDone)
	}()

	<-regDone
	close(release)
	wg.Wait()

	// Next callback invocation should execute both callbacks.
	td.processCallback(context.Background(), tables)

	if firstCount.Load() != 2 {
		t.Fatalf("unexpected first count: %d", firstCount.Load())
	}
	if secondCount.Load() != 1 {
		t.Fatalf("unexpected second count: %d", secondCount.Load())
	}
}

func TestTableDetectorProcessCallbackErrorResetsState(t *testing.T) {
	d := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	defer d.Close()

	var count atomic.Int32
	cb := func(map[uint32]TblMap) error {
		count.Add(1)
		return moerr.NewInternalErrorNoCtx("boom")
	}

	require.True(t, d.RegisterIfAbsent("task", 1, []string{"db"}, []string{"tbl"}, cb))
	tables := map[uint32]TblMap{1: {"db.tbl": {SourceDbName: "db", SourceTblName: "tbl"}}}

	d.mu.Lock()
	d.lastMp = tables
	d.mu.Unlock()

	d.processCallback(context.Background(), tables)
	d.processCallback(context.Background(), tables)

	if count.Load() != 2 {
		t.Fatalf("callback count %d", count.Load())
	}
	if d.handling {
		t.Fatalf("handling flag not reset")
	}
	if d.lastMp == nil {
		t.Fatalf("lastMp should be retained on error")
	}
}

func TestTableDetectorProcessCallbackPanicHandled(t *testing.T) {
	d := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	defer d.Close()

	require.True(t, d.RegisterIfAbsent("task", 1, []string{"db"}, []string{"tbl"}, func(map[uint32]TblMap) error {
		panic("panic in callback")
	}))

	tables := map[uint32]TblMap{1: {"db.tbl": {SourceDbName: "db", SourceTblName: "tbl"}}}

	func() {
		defer func() { _ = recover() }()
		d.processCallback(context.Background(), tables)
	}()

	if d.handling {
		t.Fatalf("handling flag not reset after panic")
	}
}

func TestTableDetectorCloseWhileCallbackRunning(t *testing.T) {
	d := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	defer d.Close()

	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCScanTable("fast scan")
	require.NoError(t, err)
	defer func() {
		if rmFn != nil {
			_, _ = rmFn()
		}
	}()

	block := make(chan struct{})
	release := make(chan struct{})

	require.True(t, d.RegisterIfAbsent("task", 1, []string{"db"}, []string{"tbl"}, func(map[uint32]TblMap) error {
		close(block)
		<-release
		return nil
	}))

	tables := map[uint32]TblMap{1: {"db.tbl": {SourceDbName: "db", SourceTblName: "tbl"}}}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.processCallback(context.Background(), tables)
	}()

	<-block
	d.Close()
	close(release)
	wg.Wait()

	d.mu.Lock()
	if d.handling {
		d.mu.Unlock()
		t.Fatalf("handling flag not reset after close")
	}
	d.mu.Unlock()

	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.cancel == nil && !d.loopRunning.Load()
	}, 5*time.Second, 10*time.Millisecond, "detector not fully closed")
}

func TestTableDetectorConcurrentRegisterUnregister(t *testing.T) {
	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCScanTable("fast scan")
	require.NoError(t, err)
	defer func() {
		if rmFn != nil {
			_, _ = rmFn()
		}
	}()

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	td.scanTableFn = func() error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	start := make(chan struct{})
	startWorkers := sync.OnceFunc(func() { close(start) })
	var workersDone <-chan struct{}
	defer func() {
		cancel()
		startWorkers()
		if workersDone != nil {
			select {
			case <-workersDone:
			case <-time.After(time.Second):
				t.Error("register/unregister workers did not exit")
			}
		}
		closed := make(chan struct{})
		go func() {
			td.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Error("table detector did not close")
			return
		}
		waitUntil(t, func() bool { return !td.loopRunning.Load() }, time.Second, "scan loop did not stop after Close")
	}()

	register := func(id string) bool {
		return td.RegisterIfAbsent(id, 1, []string{"db"}, []string{"tbl"}, func(map[uint32]TblMap) error { return nil })
	}
	require.True(t, register("anchor"))
	require.True(t, register("a"))

	// Registration and removal contend on the same subscription indexes. The
	// registration of c must follow a's removal, regardless of b's ordering.
	removed := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		td.UnRegister("a")
		close(removed)
		results <- nil
	}()
	go func() {
		defer wg.Done()
		<-start
		if !register("b") {
			results <- moerr.NewInternalErrorNoCtx("register b failed")
			return
		}
		select {
		case <-removed:
		case <-ctx.Done():
			results <- ctx.Err()
			return
		}
		if !register("c") {
			results <- moerr.NewInternalErrorNoCtx("register c failed")
			return
		}
		results <- nil
	}()
	done := make(chan struct{})
	workersDone = done
	go func() {
		wg.Wait()
		close(done)
	}()
	startWorkers()
	for range 2 {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatalf("concurrent register/unregister timed out: %v", ctx.Err())
		}
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("register/unregister workers did not finish")
	}

	td.mu.Lock()
	ids := make([]string, 0, len(td.Callbacks))
	for id := range td.Callbacks {
		ids = append(ids, id)
	}
	accountByTask := maps.Clone(td.CallBackAccountId)
	dbByTask := maps.Clone(td.CallBackDbName)
	tableByTask := maps.Clone(td.CallBackTableName)
	accountTasks := slices.Clone(td.SubscribedAccountIds[1])
	dbTasks := slices.Clone(td.SubscribedDbNames["db"])
	tableTasks := slices.Clone(td.SubscribedTableNames["tbl"])
	indexCounts := []int{len(td.SubscribedAccountIds), len(td.SubscribedDbNames), len(td.SubscribedTableNames)}
	td.mu.Unlock()

	wantIDs := []string{"anchor", "b", "c"}
	require.ElementsMatch(t, wantIDs, ids)
	require.Equal(t, map[string]uint32{"anchor": 1, "b": 1, "c": 1}, accountByTask)
	require.Equal(t, map[string][]string{"anchor": {"db"}, "b": {"db"}, "c": {"db"}}, dbByTask)
	require.Equal(t, map[string][]string{"anchor": {"tbl"}, "b": {"tbl"}, "c": {"tbl"}}, tableByTask)
	require.Equal(t, []int{1, 1, 1}, indexCounts)
	require.ElementsMatch(t, wantIDs, accountTasks)
	require.ElementsMatch(t, wantIDs, dbTasks)
	require.ElementsMatch(t, wantIDs, tableTasks)

	for _, id := range wantIDs {
		td.UnRegister(id)
	}
	td.mu.Lock()
	counts := []int{
		len(td.Callbacks), len(td.CallBackAccountId), len(td.SubscribedAccountIds),
		len(td.CallBackDbName), len(td.SubscribedDbNames),
		len(td.CallBackTableName), len(td.SubscribedTableNames),
	}
	td.mu.Unlock()
	require.Equal(t, []int{0, 0, 0, 0, 0, 0, 0}, counts)
}

func waitUntil(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for condition: %s", msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTableDetectorConcurrentRegister(t *testing.T) {
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Second,
		cleanupWarn:          time.Second,
		nowFn:                time.Now,
	}
	td.scanTableFn = func() error { return nil }
	defer td.Close()

	const concurrency = 8
	var wg sync.WaitGroup
	wg.Add(concurrency)
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			taskID := fmt.Sprintf("task-%d", idx)
			db := fmt.Sprintf("db%d", idx)
			table := fmt.Sprintf("tbl%d", idx)
			if !td.RegisterIfAbsent(taskID, uint32(idx+1), []string{db}, []string{table}, func(map[uint32]TblMap) error {
				return nil
			}) {
				errCh <- moerr.NewInternalErrorNoCtx(fmt.Sprintf("duplicate registration for %s", taskID))
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("concurrent register blocked")
	}

	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected duplicate: %v", err)
	}

	td.mu.Lock()
	defer td.mu.Unlock()
	if len(td.Callbacks) != concurrency {
		t.Fatalf("expected %d callbacks, got %d", concurrency, len(td.Callbacks))
	}
}

func Test_CollectTableInfoSQL(t *testing.T) {
	var builder cdcSQLBuilder
	sql := builder.CollectTableInfoSQL("1,2,3", "*", "*")
	_, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	upperSQL := strings.ToUpper(sql)
	assert.Contains(t, upperSQL, "AS HAS_USER_PK")
	assert.Contains(t, upperSQL, "PK.ATT_DATABASE_ID = TBL.RELDATABASE_ID")
	assert.Contains(t, upperSQL, "PK.ATT_RELNAME_ID = TBL.REL_ID")
	assert.Contains(t, upperSQL, "PK.ATT_CONSTRAINT_TYPE = 'P'")
	assert.Contains(t, upperSQL, "PK.ATTNAME <> '__MO_FAKE_PK_COL'")
	assert.NotContains(t, upperSQL, "PK.DB_NAME")
	assert.NotContains(t, upperSQL, "PK.CONSTRAINT_TYPE")
	assert.NotContains(t, upperSQL, "AND EXISTS")

	sql = builder.CollectTableInfoSQL("0", "'source_db'", "'orders'")
	_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	assert.Contains(t, strings.ToUpper(sql), "TBL.RELDATABASE IN ('SOURCE_DB')")
	assert.Contains(t, strings.ToUpper(sql), "TBL.RELNAME IN ('ORDERS')")

	// CDC source identifiers can be legal when quoted even if they contain a
	// SQL string delimiter. Candidate discovery must keep them inside the
	// catalog predicate rather than allowing the identifier to alter that SQL.
	sql = CollectCDCSourceCandidateSQL(1, "source'db", `orders\archive`)
	_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	assert.Contains(t, sql, "tbl.reldatabase IN ('source''db')")
	assert.Contains(t, sql, `tbl.relname IN ('orders\\archive')`)

	// Mode 2 keeps the user spelling in persisted task metadata, but catalog
	// selection must compare it case-insensitively before runtime matching.
	sql = CollectCDCSourceCandidateSQL(1, "mixedDB", "Orders", 2)
	_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	assert.Contains(t, sql, "lower(tbl.reldatabase) IN ('mixeddb')")
	assert.Contains(t, sql, "lower(tbl.relname) IN ('orders')")

	sql = builder.CollectTableInfoSQLCaseInsensitive("1", "'mixeddb'", "'orders'")
	_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	assert.Contains(t, sql, "lower(tbl.reldatabase) IN ('mixeddb')")
	assert.Contains(t, sql, "lower(tbl.relname) IN ('orders')")

	// Mode 2 catalog prefilter must use the same parser canonical key as task
	// matching, not Unicode simple case folding.
	sql = CollectCDCSourceCandidateSQL(1, "Σdb", "Orders", 2)
	_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	assert.Contains(t, sql, "lower(tbl.reldatabase) IN ('σdb')")

	// SQL lower() does not preserve malformed UTF-8 bytes while the parser's
	// mode-2 key does. Fall back to a catalog superset and let local matching
	// apply the byte-preserving key after scan.
	malformed := string([]byte{'1', 0xe9, 'A'})
	sql = CollectCDCSourceCandidateSQL(1, malformed, "Orders", 2)
	_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	assert.NotContains(t, sql, "lower(tbl.reldatabase)")
	assert.Contains(t, sql, "lower(tbl.relname) IN ('orders')")

	sql = CollectCDCSourceCandidateSQL(1, "MixedDB", malformed, 2)
	_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	assert.Contains(t, sql, "lower(tbl.reldatabase) IN ('mixeddb')")
	assert.NotContains(t, sql, "lower(tbl.relname)")
}

func TestTableScannerMalformedUTF8UsesCatalogSuperset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	malformed := string([]byte{'1', 0xe9, 'A'})
	mockSQLExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSQLExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "*", "'orders'"),
		gomock.Any(),
	).Return(executor.Result{}, nil)

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		CallBackDbName:       make(map[string][]string),
		SubscribedAccountIds: make(map[uint32][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSQLExecutor,
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	td.mu.Lock()
	td.registerLocked("malformed", 1, []string{malformed}, []string{"Orders"}, nil)
	td.mu.Unlock()
	require.NoError(t, td.scanTable())
}

func TestScanAndProcessStopsOnScanError(t *testing.T) {
	const wantErr = "scan failed"
	var scanCalls atomic.Int32
	var callbackCalls atomic.Int32
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	td.scanTableFn = func() error {
		scanCalls.Add(1)
		return moerr.NewInternalErrorNoCtx(wantErr)
	}
	require.True(t, td.RegisterIfAbsent("scan-error", 1, []string{"db"}, []string{"tbl"}, func(map[uint32]TblMap) error {
		callbackCalls.Add(1)
		return nil
	}))

	td.scanAndProcess(context.Background())

	require.Equal(t, int32(1), scanCalls.Load())
	require.Zero(t, callbackCalls.Load())
	td.mu.Lock()
	lastMp := td.lastMp
	td.mu.Unlock()
	require.Nil(t, lastMp)
}

func TestProcessCallBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 nil,
		lastMp:               make(map[uint32]TblMap),
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()

	tables := map[uint32]TblMap{
		1: {
			"db1.tbl1": &DbTableInfo{
				SourceDbId:      1,
				SourceDbName:    "db1",
				SourceTblId:     1001,
				SourceTblName:   "tbl1",
				SourceCreateSql: "create table tbl1 (a int)",
			},
		},
	}
	assert.True(t, td.RegisterIfAbsent("id1", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error { return moerr.NewInternalErrorNoCtx("ERR") }))
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
			assert.Contains(t, sql, "DELETE ")
			assert.Contains(t, sql, "account_id")
			return executor.Result{AffectedRows: 5}, nil
		}).
		Times(2)

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
			assert.Contains(t, sql, "DELETE ")
			assert.NotContains(t, sql, "WHERE w.account_id IN")
			return executor.Result{AffectedRows: 0}, nil
		}).
		Times(2)

	td.cleanupOrphanWatermarks(context.Background())
}

func TestTableScanner_UpdateTableInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	defer proc.Free()

	bat1 := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId", "constraint", "hasUserPK"})
	bat1.Vecs[0] = testutil.MakeUint64Vector([]uint64{1001}, nil, proc.Mp())
	bat1.Vecs[1] = testutil.MakeVarcharVector([]string{"tbl1"}, nil, proc.Mp())
	bat1.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat1.Vecs[3] = testutil.MakeVarcharVector([]string{"db1"}, nil, proc.Mp())
	bat1.Vecs[4] = testutil.MakeVarcharVector([]string{"create table tbl1 (a int)"}, nil, proc.Mp())
	bat1.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat1.Vecs[6] = testutil.MakeVarcharVector([]string{""}, nil, proc.Mp())
	bat1.Vecs[7] = testutil.MakeBoolVector([]bool{true}, nil, proc.Mp())
	bat1.SetRowCount(1)
	res1 := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat1},
	}

	bat2 := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId", "constraint", "hasUserPK"})
	bat2.Vecs[0] = testutil.MakeUint64Vector([]uint64{1002}, nil, proc.Mp())
	bat2.Vecs[1] = testutil.MakeVarcharVector([]string{"tbl1"}, nil, proc.Mp())
	bat2.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat2.Vecs[3] = testutil.MakeVarcharVector([]string{"db1"}, nil, proc.Mp())
	bat2.Vecs[4] = testutil.MakeVarcharVector([]string{"create table tbl1 (a int)"}, nil, proc.Mp())
	bat2.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat2.Vecs[6] = testutil.MakeVarcharVector([]string{""}, nil, proc.Mp())
	bat2.Vecs[7] = testutil.MakeBoolVector([]bool{false}, nil, proc.Mp())
	bat2.SetRowCount(1)
	res2 := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat2},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)

	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "'db1'", "'tbl1'"),
		gomock.Any(),
	).Return(res1, nil)

	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive("1", "'db1'", "'tbl1'"),
		gomock.Any(),
	).Return(res2, nil)

	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSqlExecutor,
		cleanupPeriod:        time.Hour,
		cleanupWarn:          DefaultCleanupWarnThreshold,
	}
	defer td.Close()
	assert.True(t, td.RegisterIfAbsent("test-task", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) error {
		return nil
	}))

	err := td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(td.Mp))

	accountMap, ok := td.Mp[1]
	assert.True(t, ok)

	tblInfo, ok := accountMap["db1.tbl1"]
	assert.True(t, ok)
	assert.Equal(t, uint64(1001), tblInfo.SourceTblId)

	assert.True(t, tblInfo.HasUserPrimaryKey)

	err = td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(td.Mp))

	accountMap = td.Mp[1]
	tblInfo = accountMap["db1.tbl1"]
	assert.Equal(t, uint64(1002), tblInfo.SourceTblId)

	assert.False(t, tblInfo.HasUserPrimaryKey)
}

func TestTableScanner_PrintActiveRunners(t *testing.T) {
	cdcStateManager := NewCDCStateManager()
	tableInfo := &DbTableInfo{
		SourceDbId:      1,
		SourceDbName:    "db1",
		SourceTblId:     1001,
		SourceTblName:   "tbl1",
		SourceCreateSql: "create table tbl1 (a int)",
	}
	cdcStateManager.AddActiveRunner(tableInfo)
	cdcStateManager.PrintActiveRunners(0)
	cdcStateManager.UpdateActiveRunner(tableInfo, types.BuildTS(1, 1), types.BuildTS(2, 2), true)
	cdcStateManager.PrintActiveRunners(0)
	cdcStateManager.UpdateActiveRunner(tableInfo, types.BuildTS(1, 1), types.BuildTS(2, 2), false)
	cdcStateManager.PrintActiveRunners(0)
	assert.Equal(t, 1, len(cdcStateManager.activeRunners))
}

func TestTableScanner_PrintActiveRunners_NilReceiver(t *testing.T) {
	var cdcStateManager *CDCStateManager
	assert.NotPanics(t, func() {
		cdcStateManager.PrintActiveRunners(0)
	})
}

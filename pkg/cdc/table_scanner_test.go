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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
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

func TestTableScanner1(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	defer proc.Free()

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"tblName"}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"dbName"}, nil, proc.Mp())
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{"createSql"}, nil, proc.Mp())
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
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

	time.Sleep(50 * time.Millisecond)
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
	defer td.Close()

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			id := fmt.Sprintf("task-%d", idx)
			if !td.RegisterIfAbsent(id, uint32(idx+1), []string{fmt.Sprintf("db%d", idx)}, []string{fmt.Sprintf("tbl%d", idx)}, func(map[uint32]TblMap) error { return nil }) {
				errCh <- moerr.NewInternalErrorNoCtx(fmt.Sprintf("register failed for %s", id))
				return
			}
			time.Sleep(time.Duration(idx%4) * 5 * time.Millisecond)
			td.UnRegister(id)
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
		t.Fatalf("concurrent register/unregister timed out")
	}

	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected error: %v", err)
	}

	waitUntil(t, func() bool {
		td.mu.Lock()
		defer td.mu.Unlock()
		return len(td.Callbacks) == 0 &&
			len(td.SubscribedAccountIds) == 0 &&
			len(td.SubscribedDbNames) == 0 &&
			len(td.SubscribedTableNames) == 0
	}, 500*time.Millisecond, "callbacks or subscriptions not cleaned up")

	td.Close()
	waitUntil(t, func() bool { return !td.loopRunning.Load() }, time.Second, "scan loop did not stop after Close")
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
				IdChanged:       false,
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

	proc := testutil.NewProcess(t)
	defer proc.Free()

	bat1 := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat1.Vecs[0] = testutil.MakeUint64Vector([]uint64{1001}, nil, proc.Mp())
	bat1.Vecs[1] = testutil.MakeVarcharVector([]string{"tbl1"}, nil, proc.Mp())
	bat1.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat1.Vecs[3] = testutil.MakeVarcharVector([]string{"db1"}, nil, proc.Mp())
	bat1.Vecs[4] = testutil.MakeVarcharVector([]string{"create table tbl1 (a int)"}, nil, proc.Mp())
	bat1.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat1.SetRowCount(1)
	res1 := executor.Result{
		Mp:      proc.Mp(),
		Batches: []*batch.Batch{bat1},
	}

	bat2 := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat2.Vecs[0] = testutil.MakeUint64Vector([]uint64{1002}, nil, proc.Mp())
	bat2.Vecs[1] = testutil.MakeVarcharVector([]string{"tbl1"}, nil, proc.Mp())
	bat2.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil, proc.Mp())
	bat2.Vecs[3] = testutil.MakeVarcharVector([]string{"db1"}, nil, proc.Mp())
	bat2.Vecs[4] = testutil.MakeVarcharVector([]string{"create table tbl1 (a int)"}, nil, proc.Mp())
	bat2.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil, proc.Mp())
	bat2.SetRowCount(1)
	res2 := executor.Result{
		Mp:      proc.Mp(),
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

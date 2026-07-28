// Copyright 2025 Matrix Origin
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type fakeSession struct {
	id     string
	tenant string
}

func (s *fakeSession) GetUUIDString() string                       { return s.id }
func (s *fakeSession) GetTenantName() string                       { return s.tenant }
func (s *fakeSession) StatusSession() *status.Session              { return nil }
func (s *fakeSession) SetSessionRoutineStatus(status string) error { return nil }

// stubExecutor implements executor.SQLExecutor
type stubExecutor struct {
	queryResult executor.Result
	drops       []string
}

func (e *stubExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	if strings.HasPrefix(strings.ToLower(sql), "select") {
		return e.queryResult, nil
	}
	e.drops = append(e.drops, sql)
	return executor.Result{}, nil
}

func (e *stubExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	return execFunc(&stubTxnExec{exec: e})
}

type stubTxnExec struct{ exec *stubExecutor }

func (t *stubTxnExec) Use(db string)                {}
func (t *stubTxnExec) LockTable(table string) error { return nil }
func (t *stubTxnExec) Exec(sql string, option executor.StatementOption) (executor.Result, error) {
	return t.exec.Exec(context.Background(), sql, executor.Options{})
}
func (t *stubTxnExec) Txn() client.TxnOperator { return nil }

// buildResult constructs executor.Result with two varchar columns.
func buildResult(dbs, names []string) executor.Result {
	mp := mpool.MustNewZeroNoFixed()
	vecDB := vector.NewVec(types.T_varchar.ToType())
	vecName := vector.NewVec(types.T_varchar.ToType())
	for _, v := range dbs {
		_ = vector.AppendBytes(vecDB, []byte(v), false, mp)
	}
	for _, v := range names {
		_ = vector.AppendBytes(vecName, []byte(v), false, mp)
	}
	bat := batch.NewWithSize(2)
	bat.SetRowCount(len(dbs))
	bat.Vecs[0] = vecDB
	bat.Vecs[1] = vecName
	return executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      mp,
	}
}

// TestCleanOrphanTempTables ensures only non-active session temp tables are dropped.
func TestCleanOrphanTempTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup runtime with internal executor
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, "cn-ut", nil, moruntime.WithClock(clock.NewHLCClock(func() int64 { return time.Now().UnixNano() }, time.Duration(0))))
	moruntime.SetupServiceBasedRuntime("cn-ut", rt)

	exec := &stubExecutor{
		queryResult: buildResult(
			[]string{"db1", "db2"},
			[]string{"__mo_tmp_active_t1", "__mo_tmp_dead_t1"},
		),
	}
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)

	// active session matches first table
	rm := &RoutineManager{
		ctx:            context.Background(),
		sessionManager: queryservice.NewSessionManager(),
	}
	rm.sessionManager.AddSession(&fakeSession{id: "active", tenant: "t"})

	// mock ExecWithResult
	origExecWithResult := iscp.ExecWithResult
	iscp.ExecWithResult = func(ctx context.Context, sql, cnUUID string, txn client.TxnOperator) (executor.Result, error) {
		return exec.Exec(ctx, sql, executor.Options{})
	}
	defer func() { iscp.ExecWithResult = origExecWithResult }()

	mo := &MOServer{
		rm:      rm,
		service: "cn-ut",
		pu: &config.ParameterUnit{
			StorageEngine: mock_frontend.NewMockEngine(ctrl),
			TxnClient:     mock_frontend.NewMockTxnClient(ctrl),
		},
	}

	err := mo.cleanOrphanTempTables()
	if err != nil {
		t.Fatalf("cleanOrphanTempTables err: %v", err)
	}

	// Should drop only the dead table
	if len(exec.drops) != 1 || !strings.Contains(exec.drops[0], "__mo_tmp_dead_t1") {
		t.Fatalf("unexpected drops: %+v", exec.drops)
	}
}

// TestStartTempTableGC_Lifecycle verifies the GC loop runs and can be stopped.
func TestStartTempTableGC_Lifecycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rt := moruntime.NewRuntime(metadata.ServiceType_CN, "cn-ut-gc", nil, moruntime.WithClock(clock.NewHLCClock(func() int64 { return time.Now().UnixNano() }, time.Duration(0))))
	moruntime.SetupServiceBasedRuntime("cn-ut-gc", rt)

	// We only care that cleanOrphanTempTables is called.
	// We mock ExecWithResult to signal via a channel.
	called := make(chan struct{}, 1)
	origExecWithResult := iscp.ExecWithResult
	iscp.ExecWithResult = func(ctx context.Context, sql, cnUUID string, txn client.TxnOperator) (executor.Result, error) {
		select {
		case called <- struct{}{}:
		default:
		}
		// return empty result to avoid further processing/errors in cleanOrphanTempTables
		return buildResult(nil, nil), nil
	}
	defer func() { iscp.ExecWithResult = origExecWithResult }()

	// Setup minimal server
	ctx, cancel := context.WithCancel(context.Background())
	rm := &RoutineManager{
		ctx:            ctx,
		sessionManager: queryservice.NewSessionManager(),
	}
	// Inject a valid executor for the check inside cleanOrphanTempTables
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, &stubExecutor{})

	mo := &MOServer{
		rm:      rm,
		service: "cn-ut-gc",
		pu: &config.ParameterUnit{
			StorageEngine: mock_frontend.NewMockEngine(ctrl),
			TxnClient:     mock_frontend.NewMockTxnClient(ctrl),
		},
	}

	// Start GC with short interval
	mo.startTempTableGC(time.Millisecond * 10)

	// valid it runs
	select {
	case <-called:
		// success
	case <-time.After(time.Second):
		t.Fatal("GC loop did not run within timeout")
	}

	// Stop
	cancel()
	mo.wg.Wait()
}

// TestStartTempTableGC_PanicRecovery verifies the GC loop recovers from panics.
func TestStartTempTableGC_PanicRecovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rt := moruntime.NewRuntime(metadata.ServiceType_CN, "cn-ut-panic", nil, moruntime.WithClock(clock.NewHLCClock(func() int64 { return time.Now().UnixNano() }, time.Duration(0))))
	moruntime.SetupServiceBasedRuntime("cn-ut-panic", rt)

	var callCount atomic.Int32
	recovered := make(chan struct{}, 1)

	origExecWithResult := iscp.ExecWithResult
	iscp.ExecWithResult = func(ctx context.Context, sql, cnUUID string, txn client.TxnOperator) (executor.Result, error) {
		n := callCount.Add(1)
		if n == 1 {
			panic("simulated panic in GC")
		}
		// Subsequent calls success
		select {
		case recovered <- struct{}{}:
		default:
		}
		return buildResult(nil, nil), nil
	}
	defer func() { iscp.ExecWithResult = origExecWithResult }()

	ctx, cancel := context.WithCancel(context.Background())
	rm := &RoutineManager{
		ctx:            ctx,
		sessionManager: queryservice.NewSessionManager(),
	}
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, &stubExecutor{})

	mo := &MOServer{
		rm:      rm,
		service: "cn-ut-panic",
		pu: &config.ParameterUnit{
			StorageEngine: mock_frontend.NewMockEngine(ctrl),
			TxnClient:     mock_frontend.NewMockTxnClient(ctrl),
		},
	}

	// Start GC
	mo.startTempTableGC(time.Millisecond * 10)

	// Wait for recovery (second call)
	select {
	case <-recovered:
		// success
	case <-time.After(time.Second):
		t.Fatal("GC loop did not recover from panic within timeout")
	}

	// Stop
	cancel()
	mo.wg.Wait()
}

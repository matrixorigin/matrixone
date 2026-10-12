// Copyright 2024 Matrix Origin
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

package cnservice

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var _ logservice.CNHAKeeperClient = new(testHAKClient)

type testHAKClient struct {
	cfg        *Config
	closeErr   error
	clusterErr error
	closed     int
}

func (client *testHAKClient) Close() error {
	client.closed++
	return client.closeErr
}

func (client *testHAKClient) AllocateID(ctx context.Context) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	return pb.ClusterDetails{}, client.clusterErr
}

func (client *testHAKClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	cs := pb.CheckerState{
		CNState: pb.CNState{
			Stores: make(map[string]pb.CNStoreInfo),
		},
	}
	return cs, nil
}

func (client *testHAKClient) CheckLogServiceHealth(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) GetBackupData(ctx context.Context) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	return pb.CommandBatch{}, moerr.NewInternalErrorNoCtx("return_err")
}

func (client *testHAKClient) UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) UpdateNonVotingLocality(ctx context.Context, locality pb.Locality) error {
	//TODO implement me
	panic("implement me")
}

var _ taskservice.TaskRunner = new(testRunner)

type testRunner struct {
	stopErr   error
	stopped   int
	executors map[task.TaskCode]taskservice.TaskExecutor
}

func (runner *testRunner) ID() string {
	//TODO implement me
	panic("implement me")
}

func (runner *testRunner) Start() error {
	//TODO implement me
	panic("implement me")
}

func (runner *testRunner) Stop() error {
	runner.stopped++
	return runner.stopErr
}

func (runner *testRunner) Parallelism() int {
	//TODO implement me
	panic("implement me")
}

func (runner *testRunner) RegisterExecutor(code task.TaskCode, executor taskservice.TaskExecutor) {
	if runner.executors == nil {
		runner.executors = make(map[task.TaskCode]taskservice.TaskExecutor)
	}
	runner.executors[code] = executor
}

func (runner *testRunner) GetExecutor(code task.TaskCode) taskservice.TaskExecutor {
	return runner.executors[code]
}

func (runner *testRunner) Attach(ctx context.Context, taskID uint64, routine taskservice.ActiveRoutine) error {
	//TODO implement me
	panic("implement me")
}

var _ taskservice.TaskServiceHolder = new(testHolder)

type testHolder struct {
	ts       taskservice.TaskService
	closeErr error
	closed   int
}

func TestInitTaskServiceHolderIsIdempotent(t *testing.T) {
	holder := &testHolder{}
	sv := &service{cfg: &Config{SQLAddress: "127.0.0.1:6001"}}
	sv.task.holder = holder
	sv.initTaskServiceHolder()
	require.Same(t, holder, sv.task.holder)
}

func (holder *testHolder) Close() error {
	holder.closed++
	return holder.closeErr
}

func (holder *testHolder) Get() (taskservice.TaskService, bool) {
	return holder.ts, true
}

func (holder *testHolder) Create(command pb.CreateTaskService) error {
	//TODO implement me
	panic("implement me")
}

func TestStopTaskStopsRunnerAfterHolderCloseFailure(t *testing.T) {
	holderErr := errors.New("holder close failed")
	runnerErr := errors.New("runner stop failed")
	holder := &testHolder{closeErr: holderErr}
	runner := &testRunner{stopErr: runnerErr}
	sv := &service{logger: zap.NewNop()}
	sv.task.holder = holder
	sv.task.runner = runner

	err := sv.stopTask()
	assert.ErrorIs(t, err, holderErr)
	assert.ErrorIs(t, err, runnerErr)
	assert.Equal(t, 1, holder.closed)
	assert.Equal(t, 1, runner.stopped)
}

var _ taskservice.TaskService = new(testTS)

type testTS struct {
	cronTasks []task.TaskMetadata
	cronExprs []string
	created   chan struct{}
}

type controlledCronTaskService struct {
	*testTS
	create func(context.Context, task.TaskMetadata, string) error
}

func (ts *controlledCronTaskService) CreateCronTask(ctx context.Context, metadata task.TaskMetadata, expr string) error {
	return ts.create(ctx, metadata, expr)
}

func TestLineageGCCronRegistrationDoesNotBlockCNClose(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	ts := &controlledCronTaskService{testTS: &testTS{}}
	ts.create = func(ctx context.Context, _ task.TaskMetadata, _ string) error {
		entered <- ctx
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return context.Canceled
		}
	}
	s := &service{cfg: &Config{}, logger: zap.NewNop(), stopper: stopper.NewStopper(t.Name())}
	s.task.runner = &testRunner{}
	s.task.holder = &testHolder{ts: ts}
	s.task.runnerReady.Store(true)
	// This is also the Start path: registration must return without waiting for
	// storage while Start holds lifecycleMu.
	registered := make(chan struct{})
	go func() { s.registerExecutorsLocked(); close(registered) }()
	select {
	case ctx := <-entered:
		account, err := defines.GetAccountId(ctx)
		require.NoError(t, err)
		require.Equal(t, catalog.System_Account, account)
	case <-time.After(2 * time.Second):
		t.Fatal("cron registration did not start")
	}
	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("startup blocked on cron registration")
	}
	closed := make(chan struct{})
	go func() { s.stopper.Stop(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("CN close did not cancel cron registration")
	}
}

func TestLineageGCCronRegistrationRetriesAndStops(t *testing.T) {
	var attempts atomic.Int32
	attempted := make(chan int32, 3)
	ts := &controlledCronTaskService{testTS: &testTS{}}
	ts.create = func(ctx context.Context, metadata task.TaskMetadata, expr string) error {
		account, err := defines.GetAccountId(ctx)
		if err != nil || account != catalog.System_Account || metadata.ID != "data_branch_lineage_gc" || expr != "0 */5 * * * *" {
			return errors.New("invalid lineage cron registration")
		}
		attempt := attempts.Add(1)
		attempted <- attempt
		if attempt == 1 {
			return errors.New("temporary storage failure")
		}
		return nil
	}
	s := &service{logger: zap.NewNop()}
	s.task.runnerReady.Store(true)
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.registerLineageGCCron(ctx, ts, ticks); close(done) }()
	select {
	case <-attempted:
	case <-time.After(2 * time.Second):
		t.Fatal("first registration did not start")
	}
	select {
	case ticks <- time.Now():
	case <-time.After(2 * time.Second):
		t.Fatal("failed registration did not enter bounded retry wait")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("successful retry did not finish")
	}
	require.Equal(t, int32(2), <-attempted)
	require.Equal(t, int32(2), attempts.Load())

	// A permanently failing registration must also leave its retry wait when
	// the CN lifetime ends, without needing another timer tick.
	ts.create = func(context.Context, task.TaskMetadata, string) error {
		attempted <- attempts.Add(1)
		return errors.New("storage unavailable")
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() { s.registerLineageGCCron(ctx2, ts, make(chan time.Time)); close(done2) }()
	select {
	case <-attempted:
	case <-time.After(2 * time.Second):
		cancel2()
		t.Fatal("failed registration did not start")
	}
	cancel2()
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("retry wait ignored CN cancellation")
	}
}

func (ts *testTS) Close() error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) CreateAsyncTask(ctx context.Context, metadata task.TaskMetadata) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) CreateBatch(ctx context.Context, metadata []task.TaskMetadata) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) CreateCronTask(ctx context.Context, metadata task.TaskMetadata, cronExpr string) error {
	ts.cronTasks = append(ts.cronTasks, metadata)
	ts.cronExprs = append(ts.cronExprs, cronExpr)
	if ts.created != nil {
		ts.created <- struct{}{}
	}
	return nil
}

func (ts *testTS) Allocate(ctx context.Context, value task.AsyncTask, taskRunner string) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) Complete(ctx context.Context, taskRunner string, task task.AsyncTask, result task.ExecuteResult) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) Heartbeat(ctx context.Context, task task.AsyncTask) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) QueryAsyncTask(ctx context.Context, condition ...taskservice.Condition) ([]task.AsyncTask, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) QueryCronTask(ctx context.Context, condition ...taskservice.Condition) ([]task.CronTask, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) CreateDaemonTask(ctx context.Context, value task.TaskMetadata, details *task.Details) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) QueryDaemonTask(ctx context.Context, conds ...taskservice.Condition) ([]task.DaemonTask, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, cond ...taskservice.Condition) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) UpdateDaemonTaskError(context.Context, task.DaemonTask, bool) (int, error) {
	panic("unexpected UpdateDaemonTaskError")
}

func (ts *testTS) UpdateDaemonTaskStatus(
	ctx context.Context,
	taskID uint64,
	status task.TaskStatus,
	updateAt time.Time,
	endAt time.Time,
	cond ...taskservice.Condition,
) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) HeartbeatDaemonTask(ctx context.Context, task task.DaemonTask) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) ValidateDaemonTask(ctx context.Context, task task.DaemonTask) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) StartScheduleCronTask() {
}

func (ts *testTS) StartScheduleSQLTask() {
}

func (ts *testTS) StopScheduleCronTask() {
}

func (ts *testTS) StopScheduleSQLTask() {
}

func (ts *testTS) TruncateCompletedTasks(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) GetStorage() taskservice.TaskStorage {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) AddCDCTask(ctx context.Context, metadata task.TaskMetadata, details *task.Details, f func(context.Context, taskservice.SqlExecutor) (int, error)) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) UpdateCDCTask(ctx context.Context, status task.TaskStatus, f func(context.Context, task.TaskStatus, map[taskservice.CDCTaskKey]struct{}, taskservice.SqlExecutor) (int, error), condition ...taskservice.Condition) (int, error) {
	//TODO implement me
	panic("implement me")
}

func Test_canClaimDaemonTask(t *testing.T) {
	conf := &Config{}
	client := &testHAKClient{
		cfg: conf,
	}

	run := &testRunner{}

	sv := &service{
		cfg:             conf,
		_hakeeperClient: client,
	}
	sv.task.runner = run

	ret := sv.canClaimDaemonTask("abc")
	assert.False(t, ret)
}

func Test_registerExecutorsLocked(t *testing.T) {
	conf := &Config{}
	client := &testHAKClient{
		cfg: conf,
	}

	run := &testRunner{}

	exec := executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, nil
	})

	sv := &service{
		cfg:             conf,
		_hakeeperClient: client,
		sqlExecutor:     exec,
	}
	sv.task.runner = run
	sv.stopper = stopper.NewStopper(t.Name())
	sv.task.runnerReady.Store(true)
	t.Cleanup(sv.stopper.Stop)

	ts := &testTS{created: make(chan struct{}, 1)}

	sv.task.holder = &testHolder{
		ts: ts,
	}

	sv.registerExecutorsLocked()
	select {
	case <-ts.created:
	case <-time.After(2 * time.Second):
		t.Fatal("lineage GC cron was not registered")
	}
	require.NotNil(t, run.GetExecutor(task.TaskCode_DataBranchLineageGC))
	require.Len(t, ts.cronTasks, 1)
	assert.Equal(t, task.TaskCode_DataBranchLineageGC, ts.cronTasks[0].Executor)
	assert.Equal(t, "data_branch_lineage_gc", ts.cronTasks[0].ID)
	assert.Equal(t, "0 */5 * * * *", ts.cronExprs[0])
}

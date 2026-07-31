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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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
	if code == task.TaskCode_MergeObject {
		tsk := &task.AsyncTask{}
		_ = executor(context.Background(), tsk)
	}
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
	cronTasks        []task.TaskMetadata
	cronExprs        []string
	queryDaemonTask  func(context.Context, ...taskservice.Condition) ([]task.DaemonTask, error)
	updateDaemonTask func(context.Context, []task.DaemonTask, ...taskservice.Condition) (int, error)
}

type observingTaskService struct {
	taskservice.TaskService
	canceled chan task.DaemonTask
}

func (s *observingTaskService) UpdateDaemonTask(
	ctx context.Context,
	tasks []task.DaemonTask,
	conds ...taskservice.Condition,
) (int, error) {
	updated, err := s.TaskService.UpdateDaemonTask(ctx, tasks, conds...)
	if err != nil || updated == 0 {
		return updated, err
	}
	for _, daemonTask := range tasks {
		if daemonTask.TaskStatus == task.TaskStatus_Canceled {
			s.canceled <- daemonTask
		}
	}
	return updated, nil
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
	if ts.queryDaemonTask == nil {
		panic("unexpected QueryDaemonTask call")
	}
	return ts.queryDaemonTask(ctx, conds...)
}

func (ts *testTS) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, cond ...taskservice.Condition) (int, error) {
	if ts.updateDaemonTask == nil {
		panic("unexpected UpdateDaemonTask call")
	}
	return ts.updateDaemonTask(ctx, tasks, cond...)
}

func (ts *testTS) HeartbeatDaemonTask(ctx context.Context, task task.DaemonTask) error {
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

	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "select mo_ctl") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
		}
		return executor.Result{}, nil
	})

	sv := &service{
		cfg:             conf,
		_hakeeperClient: client,
		sqlExecutor:     exec,
	}
	sv.task.runner = run

	ts := &testTS{}

	sv.task.holder = &testHolder{
		ts: ts,
	}

	sv.registerExecutorsLocked()
	require.NotNil(t, run.GetExecutor(retiredKafkaSinkTaskCode))
	require.NotNil(t, run.GetExecutor(task.TaskCode_DataBranchLineageGC))
	require.Len(t, ts.cronTasks, 1)
	assert.Equal(t, task.TaskCode_DataBranchLineageGC, ts.cronTasks[0].Executor)
	assert.Equal(t, "data_branch_lineage_gc", ts.cronTasks[0].ID)
	assert.Equal(t, "0 */5 * * * *", ts.cronExprs[0])
}

func TestRetiredKafkaSinkTaskExecutor(t *testing.T) {
	t.Run("retires owned running task", func(t *testing.T) {
		// Field 10 was the Connector oneof. A new binary preserves it as an
		// unknown field while leaving the current oneof unset.
		legacyDetails := &task.Details{}
		require.NoError(t, legacyDetails.Unmarshal(
			[]byte{0x52, 0x06, 0x0a, 0x04, 'd', 'b', '.', 't'},
		))
		require.Nil(t, legacyDetails.Details)
		require.NotEmpty(t, legacyDetails.XXX_unrecognized)
		legacyWire := append([]byte(nil), legacyDetails.XXX_unrecognized...)

		current := task.DaemonTask{
			ID: 7,
			Metadata: task.TaskMetadata{
				Executor: retiredKafkaSinkTaskCode,
			},
			TaskStatus: task.TaskStatus_Running,
			TaskRunner: "cn-1",
			Details:    legacyDetails,
		}
		ts := &testTS{
			queryDaemonTask: func(context.Context, ...taskservice.Condition) ([]task.DaemonTask, error) {
				return []task.DaemonTask{current}, nil
			},
			updateDaemonTask: func(_ context.Context, tasks []task.DaemonTask, conds ...taskservice.Condition) (int, error) {
				require.Len(t, tasks, 1)
				require.Len(t, conds, 4)
				updated := tasks[0]
				require.Equal(t, task.TaskStatus_Canceled, updated.TaskStatus)
				require.Equal(t, "cn-1", updated.TaskRunner)
				require.False(t, updated.UpdateAt.IsZero())
				require.Equal(t, updated.UpdateAt, updated.EndAt)
				require.Equal(t, legacyWire, updated.Details.XXX_unrecognized)
				wire, err := updated.Details.Marshal()
				require.NoError(t, err)
				roundTrip := &task.Details{}
				require.NoError(t, roundTrip.Unmarshal(wire))
				require.Equal(t, legacyWire, roundTrip.XXX_unrecognized)
				return 1, nil
			},
		}

		err := retiredKafkaSinkTaskExecutor(ts, "cn-1")(
			context.Background(),
			&task.DaemonTask{ID: current.ID, Metadata: current.Metadata},
		)
		require.NoError(t, err)
	})

	for name, current := range map[string]task.DaemonTask{
		"executor changed": {
			ID:         7,
			Metadata:   task.TaskMetadata{Executor: task.TaskCode_TestOnly},
			TaskStatus: task.TaskStatus_Running,
			TaskRunner: "cn-1",
		},
		"state changed": {
			ID:         7,
			Metadata:   task.TaskMetadata{Executor: retiredKafkaSinkTaskCode},
			TaskStatus: task.TaskStatus_CancelRequested,
			TaskRunner: "cn-1",
		},
		"owner changed": {
			ID:         7,
			Metadata:   task.TaskMetadata{Executor: retiredKafkaSinkTaskCode},
			TaskStatus: task.TaskStatus_Running,
			TaskRunner: "cn-2",
		},
	} {
		t.Run(name, func(t *testing.T) {
			ts := &testTS{
				queryDaemonTask: func(context.Context, ...taskservice.Condition) ([]task.DaemonTask, error) {
					return []task.DaemonTask{current}, nil
				},
				updateDaemonTask: func(context.Context, []task.DaemonTask, ...taskservice.Condition) (int, error) {
					t.Fatal("stale executor must not update the task")
					return 0, nil
				},
			}
			err := retiredKafkaSinkTaskExecutor(ts, "cn-1")(
				context.Background(),
				&task.DaemonTask{ID: current.ID},
			)
			require.NoError(t, err)
		})
	}

	t.Run("missing task is already terminal", func(t *testing.T) {
		ts := &testTS{
			queryDaemonTask: func(context.Context, ...taskservice.Condition) ([]task.DaemonTask, error) {
				return nil, nil
			},
		}
		err := retiredKafkaSinkTaskExecutor(ts, "cn-1")(
			context.Background(),
			&task.DaemonTask{ID: 7},
		)
		require.NoError(t, err)
	})

	t.Run("rejects non-daemon task", func(t *testing.T) {
		err := retiredKafkaSinkTaskExecutor(&testTS{}, "cn-1")(
			context.Background(),
			&task.AsyncTask{},
		)
		require.Error(t, err)
	})
}

func TestRetiredKafkaSinkTaskDispatch(t *testing.T) {
	store := taskservice.NewMemTaskStorage()
	baseService := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	service := &observingTaskService{
		TaskService: baseService,
		canceled:    make(chan task.DaemonTask, 2),
	}
	runner := taskservice.NewTaskRunner(
		"cn-1",
		service,
		func(string) bool { return true },
		taskservice.WithRunnerLogger(zap.NewNop()),
		taskservice.WithRunnerFetchInterval(time.Millisecond),
		taskservice.WithRunnerHeartbeatInterval(time.Hour),
	)
	runner.RegisterExecutor(
		retiredKafkaSinkTaskCode,
		retiredKafkaSinkTaskExecutor(service, runner.ID()),
	)
	t.Cleanup(func() {
		require.NoError(t, runner.Stop())
		require.NoError(t, baseService.Close())
	})

	legacyDetails := &task.Details{}
	require.NoError(t, legacyDetails.Unmarshal(
		[]byte{0x52, 0x06, 0x0a, 0x04, 'd', 'b', '.', 't'},
	))
	created := task.DaemonTask{
		ID: 1,
		Metadata: task.TaskMetadata{
			ID:       "legacy-kafka-sink",
			Executor: retiredKafkaSinkTaskCode,
		},
		Account:    "sys",
		TaskStatus: task.TaskStatus_Created,
		CreateAt:   time.Now(),
		UpdateAt:   time.Now(),
		Details:    legacyDetails,
	}
	migrated := created
	migrated.ID = 2
	migrated.Metadata.ID = "migrated-kafka-sink"
	migrated.TaskStatus = task.TaskStatus_CancelRequested
	added, err := store.AddDaemonTask(context.Background(), created, migrated)
	require.NoError(t, err)
	require.Equal(t, 2, added)
	require.NoError(t, runner.Start())

	retiredTasks := make(map[string]task.DaemonTask, 2)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(retiredTasks) < 2 {
		select {
		case retired := <-service.canceled:
			retiredTasks[retired.Metadata.ID] = retired
		case <-deadline.C:
			t.Fatalf("legacy kafka sink tasks that reached Canceled: %v", retiredTasks)
		}
	}
	for _, retired := range retiredTasks {
		require.Equal(t, retiredKafkaSinkTaskCode, retired.Metadata.Executor)
		require.Equal(t, task.TaskStatus_Canceled, retired.TaskStatus)
		require.False(t, retired.EndAt.IsZero())
		require.Equal(t, legacyDetails.XXX_unrecognized, retired.Details.XXX_unrecognized)
	}
	require.Equal(t, "cn-1", retiredTasks["legacy-kafka-sink"].TaskRunner)
	require.Empty(t, retiredTasks["migrated-kafka-sink"].TaskRunner)

	tasks, err := service.QueryDaemonTask(
		context.Background(),
		taskservice.WithTaskExecutorCond(taskservice.EQ, retiredKafkaSinkTaskCode),
	)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	for _, daemonTask := range tasks {
		require.Equal(t, task.TaskStatus_Canceled, daemonTask.TaskStatus)
	}
}

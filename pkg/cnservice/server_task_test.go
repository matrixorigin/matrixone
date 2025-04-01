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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var _ logservice.CNHAKeeperClient = new(testHAKClient)

type testHAKClient struct {
	cfg *Config
}

func (client *testHAKClient) Close() error {
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
}

func (runner *testRunner) Parallelism() int {
	//TODO implement me
	panic("implement me")
}

func (runner *testRunner) RegisterExecutor(code task.TaskCode, executor taskservice.TaskExecutor) {
	if code == task.TaskCode_MergeObject {
		tsk := &task.AsyncTask{}
		_ = executor(context.Background(), tsk)
	}
}

func (runner *testRunner) GetExecutor(code task.TaskCode) taskservice.TaskExecutor {
	//TODO implement me
	panic("implement me")
}

func (runner *testRunner) Attach(ctx context.Context, taskID uint64, routine taskservice.ActiveRoutine) error {
	//TODO implement me
	panic("implement me")
}

var _ taskservice.TaskServiceHolder = new(testHolder)

type testHolder struct {
	ts taskservice.TaskService
}

func (holder *testHolder) Close() error {
	//TODO implement me
	panic("implement me")
}

func (holder *testHolder) Get() (taskservice.TaskService, bool) {
	return holder.ts, true
}

func (holder *testHolder) Create(command pb.CreateTaskService) error {
	//TODO implement me
	panic("implement me")
}

var _ taskservice.TaskService = new(testTS)

type testTS struct {
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

func (ts *testTS) CreateCronTask(ctx context.Context, task task.TaskMetadata, cronExpr string) error {
	//TODO implement me
	panic("implement me")
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

func (ts *testTS) HeartbeatDaemonTask(ctx context.Context, task task.DaemonTask) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) StartScheduleCronTask() {
	//TODO implement me
	panic("implement me")
}

func (ts *testTS) StopScheduleCronTask() {
	//TODO implement me
	panic("implement me")
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
}

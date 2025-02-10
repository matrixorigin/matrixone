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

package txnentries

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
)

const (
	ModuleName = "TAETXN"
)

var _ tasks.TaskScheduler = new(testScheduler)

type testScheduler struct {
}

func (tSched *testScheduler) ScheduleTxnTask(ctx *tasks.Context, taskType tasks.TaskType, factory tasks.TxnTaskFactory) (tasks.Task, error) {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) ScheduleMultiScopedTxnTask(ctx *tasks.Context, taskType tasks.TaskType, scopes []common.ID, factory tasks.TxnTaskFactory) (tasks.Task, error) {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) ScheduleMultiScopedTxnTaskWithObserver(ctx *tasks.Context, taskType tasks.TaskType, scopes []common.ID, factory tasks.TxnTaskFactory, observers ...iops.Observer) (tasks.Task, error) {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) ScheduleMultiScopedFn(ctx *tasks.Context, taskType tasks.TaskType, scopes []common.ID, fn tasks.FuncT) (tasks.Task, error) {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) ScheduleFn(ctx *tasks.Context, taskType tasks.TaskType, fn func() error) (tasks.Task, error) {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) ScheduleScopedFn(ctx *tasks.Context, taskType tasks.TaskType, scope *common.ID, fn func() error) (tasks.Task, error) {
	return nil, fn()
}

func (tSched *testScheduler) CheckAsyncScopes(scopes []common.ID) error {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) GetCheckpointedLSN() uint64 {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) GetPenddingLSNCnt() uint64 {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) Start() {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) Stop() {
	//TODO implement me
	panic("implement me")
}

func (tSched *testScheduler) Schedule(task tasks.Task) error {
	//TODO implement me
	panic("implement me")
}

func Test_PrepareRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*0)
	defer cancel()
	dir := testutils.InitTestEnv(ModuleName, t)
	fs := objectio.TmpNewFileservice(ctx, path.Join(dir, "data"))
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeScheduler(&testScheduler{}),
	)

	ent := flushTableTailEntry{
		rt: rt,
	}
	err := ent.PrepareRollback()
	assert.NoError(t, err)
}

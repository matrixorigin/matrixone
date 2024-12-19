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

package tasks

import (
	"context"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type testTaskImpl struct{}

func (t testTaskImpl) OnExec(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) SetError(err error) {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) GetError() error {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) WaitDone(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) Waitable() bool {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) GetCreateTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) GetStartTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) GetEndTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) GetExecutTime() int64 {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) AddObserver(observer iops.Observer) {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) ID() uint64 {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) Type() TaskType {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) Cancel() error {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) Name() string {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) PreExecute() error {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) Execute(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t testTaskImpl) PostExecute() error {
	//TODO implement me
	panic("implement me")
}

func TestBaseTask(t *testing.T) {
	task := NewBaseTask(testTaskImpl{}, 0, nil)
	cancelFunc := func() {
		require.NoError(t, task.Cancel())
	}
	require.NotPanics(t, cancelFunc)
}

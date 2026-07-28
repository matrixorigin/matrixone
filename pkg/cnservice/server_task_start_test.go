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

package cnservice

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
)

type testTaskHolder struct {
	ok bool
}

func (holder *testTaskHolder) Close() error {
	return nil
}

func (holder *testTaskHolder) Get() (taskservice.TaskService, bool) {
	return nil, holder.ok
}

func (holder *testTaskHolder) Create(command pb.CreateTaskService) error {
	return nil
}

func Test_startTaskRunnerRequiresReadyAndHolder(t *testing.T) {
	conf := &Config{}
	sv := &service{cfg: conf}

	sv.startTaskRunner()
	assert.Nil(t, sv.task.runner)

	sv.task.holder = &testTaskHolder{ok: true}
	sv.startTaskRunner()
	assert.Nil(t, sv.task.runner)

	sv.task.runnerReady.Store(true)
	sv.task.holder = &testTaskHolder{ok: false}
	sv.startTaskRunner()
	assert.Nil(t, sv.task.runner)

	sv.task.holder = nil
	sv.startTaskRunner()
	assert.Nil(t, sv.task.runner)
}

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

package ops

import (
	"context"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type testOp struct{}

func (t testOp) OnExec(context.Context) error {
	return nil
}

func (t testOp) SetError(error) {}

func (t testOp) GetError() error {
	return nil
}

func (t testOp) WaitDone(context.Context) error {
	return nil
}

func (t testOp) Waitable() bool {
	return true
}

func (t testOp) GetCreateTime() time.Time {
	return time.Time{}
}

func (t testOp) GetStartTime() time.Time {
	return time.Time{}
}

func (t testOp) GetEndTime() time.Time {
	return time.Time{}
}

func (t testOp) GetExecutTime() int64 {
	return 0
}

func (t testOp) AddObserver(iops.Observer) {}

func TestOpWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker := NewOpWorker(ctx, "test", 100)
	worker.Start()
	worker.CmdC <- stuck
	for len(worker.CmdC) > 0 {
	}
	for i := 0; i < 100; i++ {
		require.True(t, worker.SendOp(testOp{}))
	}
	require.Falsef(t, worker.SendOp(testOp{}), "op channel should be full")
}

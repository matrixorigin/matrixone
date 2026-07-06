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

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestCleanupInOrderReturnsWhenMergeEndSignalIsMissing(t *testing.T) {
	oldCleanupWaitTimeout := process.PipelineCleanupWaitTimeout
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineCleanupWaitTimeout = time.Second
	process.PipelineSignalSendTimeout = time.Second
	t.Cleanup(func() {
		process.PipelineCleanupWaitTimeout = oldCleanupWaitTimeout
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	proc := process.NewTopProcess(context.Background(), mpool.MustNewZeroNoFixed(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	proc.BuildPipelineContext(context.Background())

	reg := &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
	proc.Reg.MergeReceivers = []*process.WaitRegister{reg}

	mergeOp := merge.NewArgument()
	t.Cleanup(mergeOp.Release)

	connectorOp := connector.NewArgument().WithReg(reg)
	t.Cleanup(connectorOp.Release)
	connectorOp.AppendChild(mergeOp)
	if err := connectorOp.Prepare(proc); err != nil {
		t.Fatal(err)
	}

	p := New(0, nil, connectorOp)

	done := make(chan struct{})
	start := time.Now()
	go func() {
		p.Cleanup(proc, true, true, nil)
		close(done)
	}()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("pipeline cleanup did not take the sender/receiver fast cleanup path, elapsed %s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline cleanup did not return after the merge cleanup timeout")
	}
}

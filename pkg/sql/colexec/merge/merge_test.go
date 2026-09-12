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

package merge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestMergePreservesQueryTerminalOverLateEdgeFailure(t *testing.T) {
	for _, test := range []struct {
		name         string
		buildContext func(*process.Process) (context.Context, func())
		wantErr      error
	}{
		{
			name: "query cancellation",
			buildContext: func(proc *process.Process) (context.Context, func()) {
				queryCtx := proc.Base.GetContextBase().BuildQueryCtx(proc.GetTopContext())
				_, cancel := process.GetQueryCtxFromProc(proc)
				return queryCtx, cancel
			},
			wantErr: context.Canceled,
		},
		{
			name: "query deadline",
			buildContext: func(proc *process.Process) (context.Context, func()) {
				parentCtx, cancel := context.WithDeadline(proc.GetTopContext(), time.Now().Add(-time.Second))
				return proc.Base.GetContextBase().BuildQueryCtx(parentCtx), cancel
			},
			wantErr: context.DeadlineExceeded,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			queryCtx, cancelQuery := test.buildContext(proc)
			proc.BuildPipelineContext(queryCtx)
			t.Cleanup(func() {
				proc.Cancel(nil)
				cancelQuery()
			})

			edge := process.NewPipelineEdge(1, 1)
			proc.Reg.MergeReceivers = []*process.WaitRegister{edge}
			merge := NewArgument()
			t.Cleanup(merge.Release)
			if err := merge.Prepare(proc); err != nil {
				t.Fatalf("Prepare returned error: %v", err)
			}

			edge.Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)
			cancelQuery()
			duplicateErr := moerr.NewDuplicateEntryNoCtx("6", "primary")
			if edge.SendError(duplicateErr) {
				t.Fatal("late error signal unexpectedly fit behind buffered data")
			}

			for attempt := 0; attempt < 2; attempt++ {
				result, err := merge.Call(proc)
				if err != nil {
					if !errors.Is(err, test.wantErr) {
						t.Fatalf("query terminal was replaced by late edge failure: got %v, want %v", err, test.wantErr)
					}
					return
				}
				if result.Batch == nil {
					t.Fatalf("merge stopped without query terminal on attempt %d", attempt)
				}
			}
			t.Fatal("merge returned buffered data without reporting the query terminal")
		})
	}
}

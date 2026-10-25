// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type limitPipelineSource struct {
	*colexec.MockOperator
	data   *batch.Batch
	calls  int
	second chan struct{}
}

func (s *limitPipelineSource) Call(proc *process.Process) (vm.CallResult, error) {
	r := vm.NewCallResult()
	if proc.Ctx.Err() != nil {
		r.Status = vm.ExecStop
		return r, nil
	}
	s.calls++
	if s.calls == 2 {
		close(s.second)
	}
	r.Batch = s.data
	return r, nil
}

type limitPipelineSink struct {
	*colexec.MockOperator
	second <-chan struct{}
	values []int64
}

func (s *limitPipelineSink) Call(proc *process.Process) (vm.CallResult, error) {
	// The first token fills Ch2 before the real LIMIT consumes input. The
	// producer has reached its next batch when this barrier opens.
	select {
	case <-s.second:
	case <-proc.Ctx.Done():
		return vm.CancelResult, proc.Ctx.Err()
	}
	r, err := s.GetChildren(0).Call(proc)
	if err == nil && r.Batch != nil {
		s.values = append(s.values, vector.MustFixedColNoTypeCheck[int64](r.Batch.Vecs[0])[:r.Batch.RowCount()]...)
	}
	return r, err
}

func TestRealLimitStopsBackpressuredProducerAndRetiresAttempt(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	query := proc.Base.GetContextBase().BuildQueryCtx(context.Background())
	proc.BuildPipelineContext(query)
	defer proc.Cancel(nil)
	source := batch.NewWithSize(1)
	defer source.Clean(proc.Mp())
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(source.Vecs[0], []int64{11, 12}, nil, proc.Mp()))
	source.SetRowCount(2)
	baseline := proc.Mp().AllocatedBytes()
	edge := process.NewPipelineEdge(1, 1)
	producer := &limitPipelineSource{MockOperator: colexec.NewMockOperator(), data: source, second: make(chan struct{})}
	send := connector.NewArgument().WithReg(edge)
	defer send.Release()
	send.AppendChild(producer)
	childProc := proc.NewContextChildProc(0)
	receive := merge.NewArgument()
	defer receive.Release()
	one := limit.NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1))
	defer one.Release()
	one.AppendChild(receive)
	sink := &limitPipelineSink{MockOperator: colexec.NewMockOperator(), second: producer.second}
	sink.AppendChild(one)
	proc.Reg.MergeReceivers = []*process.WaitRegister{edge}
	parent := &Scope{Magic: Merge, Proc: proc, RootOp: sink, PreScopes: []*Scope{{Magic: Normal, Proc: childProc, RootOp: send}}}
	c := &Compile{proc: proc, hasMergeOp: true, scopes: []*Scope{parent}}
	defer c.closePipelineSpoolsAfterReadersStopped()
	done := make(chan error, 1)
	go func() { done <- parent.MergeRun(c) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		proc.Cancel(context.Canceled)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("LIMIT graph did not join after fixture cancellation")
		}
		t.Fatal("real LIMIT required external cancellation")
	}
	require.Equal(t, []int64{11}, sink.values)
	require.GreaterOrEqual(t, producer.calls, 2)
	require.NoError(t, query.Err(), "successful LIMIT only stops the pipeline")
	require.NoError(t, edge.Err())
	c.closePipelineSpoolsAfterReadersStopped()
	require.Empty(t, edge.Ch2)
	require.Equal(t, baseline, proc.Mp().AllocatedBytes())
}

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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type partialFailureProducer struct {
	*colexec.MockOperator
	emitted  bool
	terminal error
}

func (p *partialFailureProducer) Call(proc *process.Process) (vm.CallResult, error) {
	if p.emitted && p.terminal != nil {
		return vm.CancelResult, p.terminal
	}
	result, err := p.MockOperator.Call(proc)
	p.emitted = result.Batch != nil
	return result, err
}

func TestNestedScopeFailureAfterPartialInput(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	defer runtime.SetupServiceBasedRuntime("", oldRuntime)
	catalog.SetupDefines("")
	for _, mode := range []string{"complete", "substantive failure", "fragment interrupted", "message interrupted"} {
		t.Run(mode, func(t *testing.T) {
			c := NewMockCompile(t)
			c.execType = plan2.ExecTypeAP_ONECN
			c.addr = "coordinator:1"
			c.anal = &AnalyzeModule{qry: &plan.Query{}}
			c.hasMergeOp = true
			c.isPrepare = false
			ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 5*time.Second)
			defer cancel()
			queryCtx := c.proc.Base.GetContextBase().BuildQueryCtx(ctx)
			c.proc.BuildPipelineContext(queryCtx)
			rootProc := c.proc.NewNoContextChildProc(2)
			leftProc := rootProc.NewNoContextChildProc(0)
			rightProc := rootProc.NewNoContextChildProc(1)
			leafProc := rightProc.NewNoContextChildProc(0)
			// One-slot edges make the producer's second Call wait for the first
			// batch to be consumed. No timing sleeps control the failure.
			rootProc.Reg.MergeReceivers[0] = process.NewPipelineEdge(1, 1)
			rootProc.Reg.MergeReceivers[1] = process.NewPipelineEdge(1, 1)
			rightProc.Reg.MergeReceivers[0] = process.NewPipelineEdge(1, 1)
			makeBatch := func(values ...int64) *batch.Batch {
				return partialInputBatch(c.proc, values)
			}
			leftData := makeBatch(1, 2)
			rightData := makeBatch(1)
			rightRemaining := makeBatch(2)
			var injected error
			if mode == "substantive failure" {
				injected = moerr.NewInternalErrorNoCtx("fragment failed after data")
			}
			if mode == "fragment interrupted" || mode == "message interrupted" {
				injected = moerr.NewQueryInterrupted(ctx)
			}
			leftLeaf := colexec.NewMockOperator().WithBatchs([]*batch.Batch{leftData})
			failingLeaf := &partialFailureProducer{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{rightData, rightRemaining}), terminal: injected}
			lc := connector.NewArgument().WithReg(rootProc.Reg.MergeReceivers[0])
			lc.AppendChild(leftLeaf)
			leafc := connector.NewArgument().WithReg(rightProc.Reg.MergeReceivers[0])
			leafc.AppendChild(failingLeaf)
			rm := merge.NewArgument()
			rc := connector.NewArgument().WithReg(rootProc.Reg.MergeReceivers[1])
			rc.AppendChild(rm)
			// Keep the real leaf Merge and its production cleanup dispatch. This
			// test-local parent delays only the observed error until the owner has
			// canceled siblings, making the historical classification race explicit.
			var errorBarrier *mergeFailureBarrier
			if mode == "fragment interrupted" || mode == "message interrupted" {
				errorBarrier = &mergeFailureBarrier{MockOperator: colexec.NewMockOperator()}
				errorBarrier.AppendChild(rm)
				rc.Children = nil
				rc.AppendChild(errorBarrier)
			}

			lm := merge.NewArgument().WithPartial(0, 1)
			rinput := merge.NewArgument().WithPartial(1, 2)
			diff := minus.NewArgument()
			diff.AppendChild(lm)
			diff.AppendChild(rinput)
			var found []int64
			out := output.NewArgument().WithFunc(func(b *batch.Batch, _ *perfcounter.CounterSet) error {
				if b != nil && b.RowCount() > 0 {
					found = append(found, vector.MustFixedColNoTypeCheck[int64](b.Vecs[0])...)
				}
				return nil
			})
			out.AppendChild(diff)
			leafScope := &Scope{Magic: Normal, Proc: leafProc, RootOp: leafc}
			if mode == "message interrupted" {
				ctrl := gomock.NewController(t)
				txClient, txOp := newTestTxnClientAndOp(ctrl)
				c.proc.Base.TxnClient = txClient
				c.proc.Base.TxnOperator = txOp
				responses := make(chan morpc.Message, 2)
				firstResponse := makeRemoteBatchMessage(t, rightData)
				stream := mock_morpc.NewMockStream(ctrl)
				stream.EXPECT().Receive().Return(responses, nil)
				stream.EXPECT().ID().Return(uint64(3)).AnyTimes()
				stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request morpc.Message) error {
					m := request.(*pipeline.Message)
					if m.GetCmd() == pipeline.Method_PipelineMessage {
						responses <- firstResponse
						end := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
						end.SetMessageType(pipeline.Method_PipelineMessage)
						end.SetMoError(context.Background(), injected)
						responses <- end
					}
					return nil
				}).AnyTimes()
				stream.EXPECT().Close(true).Return(nil).AnyTimes()
				rt.SetGlobalVariables(runtime.PipelineClient, &testPipelineClient{genStream: func(context.Context, string) (morpc.Stream, error) { return stream, nil }})
				leafc.Children = nil
				leafScope.Magic = Remote
				leafScope.NodeInfo = engine.Node{Addr: "fragment:1", Mcpu: 1}
			}
			root := &Scope{Magic: Merge, Proc: rootProc, RootOp: out, PreScopes: []*Scope{
				{Magic: Normal, Proc: leftProc, RootOp: lc},
				{Magic: Merge, Proc: rightProc, RootOp: rc, PreScopes: []*Scope{leafScope}},
			}}
			root.buildContextFromParentCtx(c.proc.Ctx)
			defer func() {
				rootProc.Cancel(process.ErrPipelineStopped)
				for _, op := range []vm.Operator{out, diff, lm, rinput, lc, rc, rm, leafc, leftLeaf, failingLeaf} {
					op.Free(c.proc, true, nil)
					op.Release()
				}
				if errorBarrier != nil {
					errorBarrier.Free(c.proc, true, nil)
				}
				leftData.Clean(c.proc.Mp())
				rightData.Clean(c.proc.Mp())
				rightRemaining.Clean(c.proc.Mp())
				for _, proc := range []*process.Process{leafProc, rightProc, leftProc, rootProc} {
					proc.Free()
				}
				c.proc.Free()
				require.Zero(t, c.proc.Mp().CurrNB())
			}()
			finalErr := root.MergeRun(c)
			t.Logf("mode=%s output=%v finalError=%v queryError=%v rightCause=%v", mode, found, finalErr, queryCtx.Err(), context.Cause(rightProc.Ctx))
			require.NoError(t, queryCtx.Err(), "the user did not cancel the query")
			if mode == "complete" {
				require.NoError(t, finalErr)
				require.Empty(t, found)
			} else {
				require.Error(t, finalErr, "partial fragment must not become successful EXCEPT output")
			}
		})
	}
}
func partialInputBatch(proc *process.Process, values []int64) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	if err := vector.AppendFixedList(b.Vecs[0], values, nil, proc.Mp()); err != nil {
		panic(err)
	}
	b.SetRowCount(len(values))
	return b
}

// Prepare/Reset/Free of the real child remain owned by the VM traversal.
type mergeFailureBarrier struct{ *colexec.MockOperator }

func (op *mergeFailureBarrier) Call(proc *process.Process) (vm.CallResult, error) {
	result, err := vm.ChildrenCall(op.GetChildren(0), proc, op.OpAnalyzer)
	if err != nil {
		<-proc.Ctx.Done()
	}
	return result, err
}

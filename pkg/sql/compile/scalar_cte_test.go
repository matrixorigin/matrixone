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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestScalarCTEReaderCopiesAndTerminalCleanup(t *testing.T) {
	for _, count := range []int{4, 8} {
		for _, mode := range []string{"end", "source_error", "cancel", "prepare_error", "partial_publication"} {
			t.Run(fmt.Sprintf("readers=%d/%s", count, mode), func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				mp := mpool.MustNewZeroNoFixed()
				proc := process.NewTopProcess(ctx, mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				c := &Compile{addr: "127.0.0.1:6001", proc: proc, ncpu: 1, anal: &AnalyzeModule{},
					nodeRegs: make(map[[2]int32]*process.WaitRegister), stepRegs: make(map[int32][][2]int32)}
				var scopes []*Scope
				input := batch.NewWithSize(1)
				input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(42), false, mp))
				input.SetRowCount(1)
				t.Cleanup(func() {
					cancel(context.Canceled)
					proc.Base.ReleasePipelineCurrentsAfterReadersStopped()
					proc.Base.ClosePipelineSpoolsAfterReadersStopped()
					ReleaseScopes(scopes)
					input.Clean(mp)
					proc.Free()
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				})
				for i := 0; i < count; i++ {
					c.appendStepRegs(0, int32(i+1), process.NewPipelineEdge(1, 0))
					r, err := c.compileSinkScanNode(&plan.Node{SourceStep: []int32{0}, ExtraOptions: plan2.ScalarCTESinkOption}, int32(i+1))
					require.NoError(t, err)
					scopes = append(scopes, r...)
					raw := r[0].PreScopes[0]
					raw.Proc.Ctx, r[0].Proc.Ctx = ctx, ctx
					require.NoError(t, raw.RootOp.GetOperatorBase().GetChildren(0).Prepare(raw.Proc))
					require.NoError(t, raw.RootOp.Prepare(raw.Proc))
					if mode == "prepare_error" && i == 1 {
						return // Attempt cleanup must own spools even before all receivers Prepare.
					}
					require.NoError(t, r[0].RootOp.Prepare(r[0].Proc))
				}
				edges := c.getStepRegs(0)
				shared := pSpool.InitMyPipelineSpool(mp, uint32(count))
				proc.RegisterPipelineSpool(shared, edges...)
				done, err := shared.SendBatch(ctx, pSpool.SendToAllLocal, input, nil)
				require.NoError(t, err)
				require.False(t, done)
				copies := make([]*batch.Batch, count)
				for i, edge := range edges {
					edge.Ch2 <- process.NewPipelineSignalToGetFromSpool(shared, i)
					raw, reader := scopes[i].PreScopes[0], scopes[i]
					_, err = raw.RootOp.Call(raw.Proc)
					require.NoError(t, err)
					out, callErr := reader.RootOp.Call(reader.Proc)
					require.NoError(t, callErr)
					require.Equal(t, int64(42), vector.MustFixedColNoTypeCheck[int64](out.Batch.Vecs[0])[0])
					copies[i] = out.Batch
					if mode == "partial_publication" && i == 0 {
						return // Includes a borrowed private row and unnotified shared readers.
					}
					if i > 0 {
						require.NotSame(t, copies[0].Vecs[0], copies[i].Vecs[0])
					}
				}
				var writers sync.WaitGroup
				for i, copy := range copies {
					writers.Add(1)
					go func() {
						defer writers.Done()
						vector.MustFixedColNoTypeCheck[int64](copy.Vecs[0])[0] = int64(i)
						typ := *copy.Vecs[0].GetType()
						typ.Width = int32(i)
						copy.Vecs[0].SetType(typ)
					}()
				}
				writers.Wait()
				for i, copy := range copies {
					require.Equal(t, int64(i), vector.MustFixedColNoTypeCheck[int64](copy.Vecs[0])[0])
				}
				require.Equal(t, int64(42), vector.MustFixedColNoTypeCheck[int64](input.Vecs[0])[0])
				var terminalErr error
				if mode != "end" {
					terminalErr = errors.New("scalar source stopped")
				}
				if mode == "cancel" {
					cancel(terminalErr)
				}
				for _, edge := range edges {
					_, ok := edge.PublishTerminal(process.BuildCleanupSignal(terminalErr != nil, terminalErr))
					require.True(t, ok)
				}
				for _, reader := range scopes {
					raw := reader.PreScopes[0]
					_, err := raw.RootOp.Call(raw.Proc)
					if mode == "end" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					raw.RootOp.Reset(raw.Proc, err != nil, err)
					raw.RootOp.GetOperatorBase().GetChildren(0).Reset(raw.Proc, err != nil, err)
					_, err = reader.RootOp.Call(reader.Proc)
					if mode == "end" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					reader.RootOp.Reset(reader.Proc, err != nil, err)
					raw.RootOp.(*connector.Connector).CleanupDeferredSpool()
				}
			})
		}
	}
}

func TestScalarCTESinkPublishesProjectedSchemaOnCoordinator(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, count := range []int{2, 4, 8} {
			t.Run(fmt.Sprintf("remote=%v/readers=%d", remote, count), func(t *testing.T) {
				mp := mpool.MustNewZeroNoFixed()
				proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				t.Cleanup(func() { proc.Free(); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
				c := &Compile{addr: "127.0.0.1:6001", proc: proc, ncpu: 1, anal: &AnalyzeModule{},
					nodeRegs: make(map[[2]int32]*process.WaitRegister), stepRegs: make(map[int32][][2]int32)}
				var readers []*Scope
				for i := 0; i < count; i++ {
					c.appendStepRegs(0, int32(i+1), process.NewPipelineEdge(1, 0))
					reader, err := c.compileSinkScanNode(&plan.Node{SourceStep: []int32{0}, ExtraOptions: plan2.ScalarCTESinkOption}, int32(i+1))
					require.NoError(t, err)
					readers = append(readers, reader...)
				}
				t.Cleanup(func() { ReleaseScopes(readers) })
				source := newScope(Remote)
				source.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
				if remote {
					source.NodeInfo.Addr = "127.0.0.1:7001"
				}
				source.Proc = proc.NewNoContextChildProc(0)
				source.RootOp = value_scan.NewArgument()
				col := func(pos int32) *plan.Expr {
					return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
				}
				node := &plan.Node{NodeType: plan.Node_SINK, ExtraOptions: plan2.ScalarCTESinkOption, ProjectList: []*plan.Expr{col(0), col(2)}}
				result, err := c.compileSinkNode(node, []*Scope{source}, 0)
				require.NoError(t, err)
				t.Cleanup(func() { ReleaseScopes(result) })
				require.Len(t, result, 1)
				require.Equal(t, c.addr, result[0].NodeInfo.Addr)
				require.Equal(t, []*Scope{source}, result[0].PreScopes)
				d := result[0].RootOp.(*dispatch.Dispatch)
				require.Equal(t, dispatch.SendToAllLocalFunc, d.FuncId)
				require.Len(t, d.LocalRegs, count)
				project := d.GetChildren(0).(*projection.Projection)
				require.Equal(t, node.ProjectList, project.ProjectList)
				require.IsType(t, &merge.Merge{}, project.GetChildren(0))
				for i, reader := range readers {
					require.Equal(t, c.addr, reader.NodeInfo.Addr)
					require.Len(t, reader.PreScopes, 1, "a private copy must precede expression evaluation")
					raw := reader.PreScopes[0]
					require.Same(t, d.LocalRegs[i], raw.Proc.Reg.MergeReceivers[0])
					require.True(t, raw.RootOp.GetOperatorBase().GetChildren(0).(*merge.Merge).SinkScan)
				}
			})
		}
	}
}

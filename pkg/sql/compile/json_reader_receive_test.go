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

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Exercise the production receive decoder and forwarding edge. Only the RPC
// transport is replaced by its message channel; this is not a two-CN SQL test.
func TestJSONReaderRemoteReceiveProjection(t *testing.T) {
	proc := testutil.NewProcess(t)
	before := proc.Mp().CurrNB()
	original := batch.NewWithSize(1)
	original.Vecs[0] = vector.NewVec(types.T_json.ToType())
	doc, err := types.ParseStringToByteJson(`{"keep":1,"second":"received"}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(original.Vecs[0], doc, false, proc.Mp()))
	original.SetRowCount(1)
	wire, err := original.MarshalBinary()
	require.NoError(t, err)
	original.Clean(proc.Mp())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := &messageSenderOnClient{ctx: ctx, ctxCancel: cancel, mp: proc.Mp(), receiveCh: make(chan morpc.Message, 2)}
	split := len(wire) / 2
	sender.receiveCh <- &pipeline.Message{Sid: pipeline.Status_WaitingNext, Data: wire[:split]}
	sender.receiveCh <- &pipeline.Message{Sid: pipeline.Status_Last, Data: wire[split:]}
	received, over, err := sender.receiveBatch()
	require.NoError(t, err)
	require.False(t, over)
	require.NotNil(t, received)
	edge := process.NewPipelineEdge(2, 1)
	proc.Reg.MergeReceivers = []*process.WaitRegister{edge}
	done, err := forwardRemoteBatchWithContext(sender, edge, received, proc.Mp())
	require.NoError(t, err)
	require.False(t, done)
	require.True(t, edge.SendEnd())
	source := merge.NewArgument()
	require.NoError(t, source.Prepare(proc))
	output := projection.NewArgument()
	output.AppendChild(source)
	overload, err := function.GetFunctionByName(proc.Ctx, "json_value", []types.Type{types.T_json.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	for _, path := range []string{"$.keep", "$.second"} {
		column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
		literal := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: path}}}}
		output.ProjectList = append(output.ProjectList, &plan.Expr{Typ: overload.GetReturnType().PlanType(), Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: overload.GetEncodedOverloadID(), ObjName: "json_value"}, Args: []*plan.Expr{column, literal}}}})
	}
	require.NoError(t, output.Prepare(proc))
	result, err := output.Call(proc)
	require.NoError(t, err)
	require.Equal(t, "1", string(result.Batch.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "received", string(result.Batch.Vecs[1].GetBytesAt(0)))
	result, err = output.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	output.Reset(proc, false, nil)
	output.Free(proc, false, nil)
	output.Release()
	source.Reset(proc, false, nil)
	source.Free(proc, false, nil)
	source.Release()
	// Merge borrows the received batch from its signal. The receive owner
	// releases it after the consumer has finished, independently of readers.
	received.Clean(proc.Mp())
	require.Equal(t, before, proc.Mp().CurrNB())
}

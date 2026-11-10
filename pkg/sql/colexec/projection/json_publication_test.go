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

package projection

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type observingJSONMerge struct {
	*merge.Merge
	published map[int]vector.JSONReader
	calls     int
}

func (source *observingJSONMerge) PublishJSONInputs(proc *process.Process, input *batch.Batch, columns []int) (map[int]vector.JSONReader, error) {
	readers, err := source.Merge.PublishJSONInputs(proc, input, columns)
	if err != nil {
		return nil, err
	}
	source.calls++
	source.published = readers // borrowed probes; the projection owns these handles
	// The ordinary received batch may change after publication. Consumers must
	// use the published version, not re-admit its mutable materialization.
	changed, err := types.ParseStringToByteJson(`{"keep":9,"second":8}`)
	if err != nil {
		return nil, err
	}
	raw, err := changed.Marshal()
	if err != nil {
		return nil, err
	}
	copy(input.Vecs[0].GetBytesAt(0), raw)
	return readers, nil
}

func TestProjectionConsumesMergeJSONPublication(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := batch.NewWithSize(1)
	original.Vecs[0] = vector.NewVec(types.T_json.ToType())
	defer original.Clean(proc.Mp())
	doc, err := types.ParseStringToByteJson(`{"keep":1,"second":2}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(original.Vecs[0], doc, false, proc.Mp()))
	original.SetRowCount(1)
	wire, err := original.MarshalBinary()
	require.NoError(t, err)
	received := batch.NewWithSize(0)
	defer received.Clean(proc.Mp())
	require.NoError(t, received.UnmarshalBinaryWithAnyMp(wire, proc.Mp()))
	edge := process.NewPipelineEdge(2, 1)
	proc.Reg.MergeReceivers = []*process.WaitRegister{edge}
	source := &observingJSONMerge{Merge: merge.NewArgument()}
	defer source.Release()
	require.NoError(t, source.Prepare(proc))
	defer func() { source.Reset(proc, false, nil); source.Free(proc, false, nil) }()
	edge.Ch2 <- process.NewPipelineSignalToDirectly(received, nil, nil)
	require.True(t, edge.SendEnd())
	projection := NewArgument()
	defer projection.Release()
	projection.AppendChild(source)
	overload, err := function.GetFunctionByName(proc.Ctx, "json_value", []types.Type{types.T_json.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	for _, text := range []string{"$.keep", "$.second"} {
		column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
		path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: text}}}}
		projection.ProjectList = append(projection.ProjectList, &plan.Expr{Typ: overload.GetReturnType().PlanType(), Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: overload.GetEncodedOverloadID(), ObjName: "json_value"}, Args: []*plan.Expr{column, path}}}})
	}
	require.NoError(t, projection.Prepare(proc))
	defer projection.Free(proc, false, nil)
	output, err := projection.Call(proc)
	require.NoError(t, err)
	require.Equal(t, "1", string(output.Batch.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "2", string(output.Batch.Vecs[1].GetBytesAt(0)))
	require.Equal(t, 1, source.calls)
	require.Len(t, source.published, 1, "different paths must share one input version")
	_, err = source.published[0].Window(0, 1)
	require.Error(t, err, "projection must release delivered versions before returning")
	output, err = projection.Call(proc)
	require.NoError(t, err)
	require.Nil(t, output.Batch)
	require.Equal(t, 1, source.calls, "terminal delivery must not publish input")
	projection.Reset(proc, false, nil)
}

func TestMergeJSONPublicationPreservesNullPathOrder(t *testing.T) {
	for _, pathNull := range []bool{true, false} {
		t.Run(fmt.Sprintf("null-path=%v", pathNull), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			input := batch.NewWithSize(2)
			input.Vecs[0] = vector.NewVec(types.T_json.ToType())
			input.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			defer input.Clean(proc.Mp())
			doc, err := types.ParseStringToByteJson(`{"keep":1}`)
			require.NoError(t, err)
			require.NoError(t, vector.AppendByteJson(input.Vecs[0], doc, false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("$.keep"), pathNull, proc.Mp()))
			input.SetRowCount(1)
			wire, err := input.MarshalBinary()
			require.NoError(t, err)
			received := batch.NewWithSize(0)
			defer received.Clean(proc.Mp())
			require.NoError(t, received.UnmarshalBinaryWithAnyMp(wire, proc.Mp()))
			received.Vecs[0].GetBytesAt(0)[0] = 0xfd
			edge := process.NewPipelineEdge(2, 1)
			proc.Reg.MergeReceivers = []*process.WaitRegister{edge}
			source := merge.NewArgument()
			defer source.Release()
			require.NoError(t, source.Prepare(proc))
			defer func() { source.Reset(proc, false, nil); source.Free(proc, false, nil) }()
			edge.Ch2 <- process.NewPipelineSignalToDirectly(received, nil, nil)
			require.True(t, edge.SendEnd())
			projection := NewArgument()
			defer projection.Release()
			projection.AppendChild(source)
			overload, err := function.GetFunctionByName(proc.Ctx, "json_value", []types.Type{types.T_json.ToType(), types.T_varchar.ToType()})
			require.NoError(t, err)
			column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
			projection.ProjectList = []*plan.Expr{{Typ: overload.GetReturnType().PlanType(), Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: overload.GetEncodedOverloadID(), ObjName: "json_value"}, Args: []*plan.Expr{column, path}}}}}
			require.NoError(t, projection.Prepare(proc))
			defer projection.Free(proc, false, nil)
			output, err := projection.Call(proc)
			if pathNull {
				require.NoError(t, err)
				require.True(t, output.Batch.Vecs[0].IsNull(0))
			} else {
				require.Error(t, err)
			}
		})
	}
}

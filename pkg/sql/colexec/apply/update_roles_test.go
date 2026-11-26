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

package apply

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExpandedUpdateRolesAreBatched(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := batch.NewWithSize(2)
	input.Vecs[0], input.Vecs[1] = vector.NewVec(types.T_Rowid.ToType()), vector.NewVec(types.T_Rowid.ToType())
	const rows = 1024
	var block types.Blockid
	for i := 0; i < rows; i++ {
		id := types.NewRowid(&block, uint32(i))
		require.NoError(t, vector.AppendFixed(input.Vecs[0], id, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], id, i%2 == 0, proc.Mp()))
	}
	input.SetRowCount(rows)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	defer child.Free(proc, false, nil)
	rowID := plan.Type{Id: int32(types.T_Rowid)}
	tf := table_function.NewArgument()
	tf.FuncName = plan2.UpdateRolesTableFunction
	tf.Attrs = []string{"role", "row_id"}
	tf.Rets = []*plan.ColDef{{Name: "role", Typ: plan.Type{Id: int32(types.T_int32)}}, {Name: "row_id", Typ: rowID}}
	tf.Args = []*plan.Expr{
		{Typ: rowID, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		{Typ: rowID, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
	}
	op := NewArgument()
	op.TableFunction, op.ApplyType = tf, CROSS
	op.Typs = []types.Type{types.T_int32.ToType(), types.T_Rowid.ToType()}
	op.Result = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}, {Rel: 1, Pos: 1}}
	op.AppendChild(child)
	defer op.Release()
	defer op.Free(proc, false, nil)
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, rows+rows/2, result.Batch.RowCount(), "roles must be returned in batches, not once per source row")
	result, err = op.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
}

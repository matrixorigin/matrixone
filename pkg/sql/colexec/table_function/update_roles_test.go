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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestExpandUpdateRoles(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := batch.NewWithSize(2)
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewVec(types.T_Rowid.ToType())
	}
	var block types.Blockid
	id1, id2 := types.NewRowid(&block, 1), types.NewRowid(&block, 2)
	for _, value := range []struct {
		column int
		id     types.Rowid
		null   bool
	}{{0, id1, false}, {0, id2, true}, {1, id1, false}, {1, id2, false}} {
		require.NoError(t, vector.AppendFixed(input.Vecs[value.column], value.id, value.null, proc.Mp()))
	}
	input.SetRowCount(2)
	defer input.Clean(proc.Mp())

	typ := plan.Type{Id: int32(types.T_Rowid)}
	tf := &TableFunction{
		FuncName: plan2.UpdateRolesTableFunction,
		Attrs:    []string{"role", "row_id"},
		Rets: []*plan.ColDef{
			{Name: "role", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "row_id", Typ: typ},
		},
		Args: []*plan.Expr{
			{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
			{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
		},
	}
	defer tf.Free(proc, false, nil)
	require.NoError(t, tf.ApplyPrepare(proc))
	require.NoError(t, tf.ApplyArgsEval(input, proc))
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, tf.ApplyStart(0, proc, analyzer))
	result, err := tf.ApplyCall(proc)
	require.NoError(t, err)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], 0))
	require.Equal(t, int32(1), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], 1))
	require.Equal(t, id1, vector.GetFixedAtNoTypeCheck[types.Rowid](result.Batch.Vecs[1], 0))
	require.Equal(t, id1, vector.GetFixedAtNoTypeCheck[types.Rowid](result.Batch.Vecs[1], 1))
	result, err = tf.ApplyCall(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)

	require.NoError(t, tf.ApplyStart(1, proc, analyzer))
	result, err = tf.ApplyCall(proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int32(1), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], 0))
	require.Equal(t, id2, vector.GetFixedAtNoTypeCheck[types.Rowid](result.Batch.Vecs[1], 0))
}

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

package projection

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// project (b, a + 1000) over a selected input of rows {1, 3, 4}.
func TestProjectionSelectedInput(t *testing.T) {
	int32Type := types.T_int32.ToType()
	plus, err := function.GetFunctionByName(context.TODO(), "+", []types.Type{int32Type, int32Type})
	require.NoError(t, err)
	for _, output := range []bool{false, true} {
		proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
		input := batch.NewWithSize(2)
		input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 20, 30, 40, 50}, nil, proc.Mp())
		var selection bitmap.Bitmap
		selection.Init(bitmapTestMP)
		mustBitmapOK(selection.InitWithSize(5, bitmapTestMP))
		selection.AddMany([]uint64{1, 3, 4})
		input.SetSelection(&selection)

		arg := NewArgument()
		arg.ProjectList = []*plan.Expr{
			makeProjectionCol(1, types.T_int32),
			{
				Typ: plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "+", Obj: plus.GetEncodedOverloadID()},
					Args: []*plan.Expr{
						makeProjectionCol(0, types.T_int32),
						{
							Typ:  plan.Type{Id: int32(types.T_int32), NotNullable: true},
							Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1000}}},
						},
					},
				}},
			},
		}
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		arg.AppendChild(child)
		require.NoError(t, arg.Prepare(proc))
		arg.SetSelectionOutput(output)

		result, err := arg.Call(proc)
		require.NoError(t, err)
		out := result.Batch
		require.Equal(t, 3, out.RowCount())
		require.Equal(t, output, out.HasSelection())
		rows := out.SelectionRows(nil)
		read := func(col, i int) int32 {
			row := i
			if out.HasSelection() {
				row = int(rows[i])
			}
			return vector.GetFixedAtNoTypeCheck[int32](out.Vecs[col], row)
		}
		for i, want := range [][2]int32{{20, 1002}, {40, 1004}, {50, 1005}} {
			require.Equal(t, want[0], read(0, i))
			require.Equal(t, want[1], read(1, i))
		}
		if output {
			// The column reference is passed through without a copy.
			require.Same(t, input.Vecs[1], out.Vecs[0])
		}

		child.Free(proc, false, nil)
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		input.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}
}

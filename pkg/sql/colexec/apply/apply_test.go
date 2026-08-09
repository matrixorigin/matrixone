// Copyright 2024 Matrix Origin
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

package apply

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	tests := []struct {
		name      string
		applyType int
		want      string
	}{
		{name: "cross", applyType: CROSS, want: "apply: cross apply "},
		{name: "outer", applyType: OUTER, want: "apply: outer apply "},
		{name: "unknown", applyType: -1, want: "apply"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			arg := NewArgument()
			arg.ApplyType = test.applyType
			buf := new(bytes.Buffer)
			arg.String(buf)
			require.Equal(t, test.want, buf.String())
		})
	}
}

func TestNilTableFunctionLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := NewArgument()

	err := arg.Prepare(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.ErrorContains(t, err, "apply operator missing table function")

	_, err = arg.Call(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.ErrorContains(t, err, "apply operator missing table function")

	require.NotPanics(t, func() {
		arg.Reset(proc, false, nil)
	})

	require.NotPanics(t, func() {
		arg.Free(proc, false, nil)
	})
}

func TestApplyMaintainsOuterNullCardinality(t *testing.T) {
	tests := []struct {
		name        string
		applyType   int
		wantIDs     []int32
		wantResults []int64
		wantNulls   int
	}{
		{
			name:        "outer preserves unmatched row",
			applyType:   OUTER,
			wantIDs:     []int32{1, 1, 1, 2},
			wantResults: []int64{1, 2, 3, 0},
			wantNulls:   1,
		},
		{
			name:        "cross omits unmatched row",
			applyType:   CROSS,
			wantIDs:     []int32{1, 1, 1},
			wantResults: []int64{1, 2, 3},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() {
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			arg := newGenerateSeriesApply(
				t,
				proc,
				test.applyType,
				[]int32{1, 2},
				[]int32{1, 1},
				[]int32{3, -1},
			)

			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, len(test.wantIDs), result.Batch.RowCount())
			require.Equal(t, test.wantIDs, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
			require.Equal(t, test.wantResults, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
			require.Equal(t, test.wantNulls, result.Batch.Vecs[1].GetNulls().Count())
		})
	}
}

func TestOuterApplyResumesAfterNullExtendedBatchBoundary(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	inputRows := colexec.DefaultBatchSize + 1
	ids := make([]int32, inputRows)
	starts := make([]int32, inputRows)
	ends := make([]int32, inputRows)
	for i := range inputRows {
		ids[i] = int32(i)
		starts[i] = 1
		ends[i] = -1
	}
	starts[inputRows-1] = 7
	ends[inputRows-1] = 7
	arg := newGenerateSeriesApply(t, proc, OUTER, ids, starts, ends)

	first, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, colexec.DefaultBatchSize, first.Batch.RowCount())
	firstIDs := vector.MustFixedColWithTypeCheck[int32](first.Batch.Vecs[0])
	require.Len(t, firstIDs, colexec.DefaultBatchSize)
	require.Equal(t, int32(0), firstIDs[0])
	require.Equal(t, int32(colexec.DefaultBatchSize-1), firstIDs[colexec.DefaultBatchSize-1])
	require.Equal(t, colexec.DefaultBatchSize, first.Batch.Vecs[1].GetNulls().Count())

	second, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 1, second.Batch.RowCount())
	require.Equal(t, []int32{int32(colexec.DefaultBatchSize)}, vector.MustFixedColWithTypeCheck[int32](second.Batch.Vecs[0]))
	require.Equal(t, []int64{7}, vector.MustFixedColWithTypeCheck[int64](second.Batch.Vecs[1]))
	require.Zero(t, second.Batch.Vecs[1].GetNulls().Count())

	done, err := arg.Call(proc)
	require.NoError(t, err)
	require.Nil(t, done.Batch)
}

func TestOuterApplyNullExtendsNullUnnestInput(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeJsonVector([]string{`{"a":1}`, "null"}, []uint64{1}, proc.Mp())
	input.SetRowCount(2)

	tf := table_function.NewArgument()
	tf.FuncName = "unnest"
	tf.Attrs = []string{"value"}
	tf.Rets = []*plan.ColDef{{
		Name: "value",
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
	}}
	tf.Args = []*plan.Expr{makeColumnExpr(1, types.T_json)}

	arg := NewArgument()
	arg.ApplyType = OUTER
	arg.Result = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}
	arg.Typs = []types.Type{types.New(types.T_varchar, types.MaxVarcharLen, 0)}
	arg.TableFunction = tf
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		arg.Release()
	})

	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, []int32{1, 2}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.False(t, result.Batch.Vecs[1].IsNull(0))
	require.True(t, result.Batch.Vecs[1].IsNull(1))
}

func newGenerateSeriesApply(
	t *testing.T,
	proc *process.Process,
	applyType int,
	ids, starts, ends []int32,
) *Apply {
	require.Len(t, starts, len(ids))
	require.Len(t, ends, len(ids))
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector(ids, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(starts, nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt32Vector(ends, nil, proc.Mp())
	input.SetRowCount(len(ids))

	tf := table_function.NewArgument()
	tf.FuncName = "generate_series"
	tf.Attrs = []string{"result"}
	tf.Rets = []*plan.ColDef{{
		Name: "result",
		Typ:  plan.Type{Id: int32(types.T_int64)},
	}}
	tf.Args = []*plan.Expr{
		makeColumnExpr(1, types.T_int32),
		makeColumnExpr(2, types.T_int32),
		plan2.MakePlan2Int32ConstExprWithType(1),
	}

	arg := NewArgument()
	arg.ApplyType = applyType
	arg.Result = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}
	arg.Typs = []types.Type{types.T_int64.ToType()}
	arg.TableFunction = tf
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		arg.Release()
	})

	require.NoError(t, arg.Prepare(proc))
	return arg
}

func makeColumnExpr(pos int32, typ types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

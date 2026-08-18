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
	"github.com/matrixorigin/matrixone/pkg/vm"
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
	require.ErrorContains(t, err, "apply operator missing parameterized source")

	_, err = arg.Call(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.ErrorContains(t, err, "apply operator missing parameterized source")

	require.NotPanics(t, func() {
		arg.Reset(proc, false, nil)
	})

	require.NotPanics(t, func() {
		arg.Free(proc, false, nil)
	})
}

func TestOuterApplyNullExtendsEmptyGenerateSeries(t *testing.T) {
	proc := testutil.NewProc(t)
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt32Vector([]int32{3, -1}, nil, proc.Mp())
	input.SetRowCount(2)

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
	arg.ApplyType = OUTER
	arg.Result = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}
	arg.Typs = []types.Type{types.T_int64.ToType()}
	arg.TableFunction = tf
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		arg.Release()
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 4, result.Batch.RowCount())
	require.Equal(t, []int32{1, 1, 1, 2}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.Equal(t, []int64{1, 2, 3, 0}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
	require.True(t, result.Batch.Vecs[1].IsNull(3))
}

func TestOuterApplyNullExtendsNullUnnestInput(t *testing.T) {
	proc := testutil.NewProc(t)
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
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, []int32{1, 2}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.False(t, result.Batch.Vecs[1].IsNull(0))
	require.True(t, result.Batch.Vecs[1].IsNull(1))
}

type scriptedAppliedSource struct {
	row     int
	emitted bool
	prepare int
	starts  int
	ends    int
	resets  int
	frees   int
	result  *batch.Batch
}

func (s *scriptedAppliedSource) ApplyPrepare(*process.Process) error              { s.prepare++; return nil }
func (*scriptedAppliedSource) ApplyArgsEval(*batch.Batch, *process.Process) error { return nil }
func (s *scriptedAppliedSource) ApplyStart(row int, proc *process.Process, _ process.Analyzer) error {
	s.row, s.emitted = row, false
	s.starts++
	if s.result == nil {
		s.result = batch.NewWithSize(1)
		s.result.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	} else {
		s.result.CleanOnlyData()
	}
	if err := vector.AppendFixed(s.result.Vecs[0], int64(row+10), false, proc.Mp()); err != nil {
		return err
	}
	s.result.SetRowCount(1)
	return nil
}
func (s *scriptedAppliedSource) ApplyCall(*process.Process) (vm.CallResult, error) {
	if s.emitted {
		return vm.CancelResult, nil
	}
	s.emitted = true
	return vm.CallResult{Status: vm.ExecNext, Batch: s.result}, nil
}
func (s *scriptedAppliedSource) ApplyEnd(*process.Process) error     { s.ends++; return nil }
func (s *scriptedAppliedSource) Reset(*process.Process, bool, error) { s.resets++ }
func (s *scriptedAppliedSource) Free(proc *process.Process, _ bool, _ error) {
	s.frees++
	if s.result != nil {
		s.result.Clean(proc.Mp())
		s.result = nil
	}
}

func TestApplyUsesParameterizedSourceLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	input.SetRowCount(2)
	source := &scriptedAppliedSource{}
	arg := NewArgument()
	arg.Source = source
	arg.Result = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}
	arg.Typs = []types.Type{types.T_int64.ToType()}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	t.Cleanup(func() {
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		arg.Release()
		proc.Free()
	})

	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 2}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.Equal(t, []int64{10, 11}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
	require.Equal(t, 1, source.prepare)
	require.Equal(t, 2, source.starts)
}

func makeColumnExpr(pos int32, typ types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

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
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	_ "github.com/matrixorigin/matrixone/pkg/indexplugin/all"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

type scriptedVectorReader struct {
	values   []int64
	next     int
	closed   int
	closeErr error
	readErr  error
}

var _ engine.Reader = (*scriptedVectorReader)(nil)

func (r *scriptedVectorReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.readErr != nil {
		return false, r.readErr
	}
	if r.next >= len(r.values) {
		return true, nil
	}
	out.CleanOnlyData()
	if err := vector.AppendFixed(out.Vecs[0], r.values[r.next], false, mp); err != nil {
		return false, err
	}
	out.SetRowCount(1)
	r.next++
	return false, nil
}

func (r *scriptedVectorReader) Close() error                  { r.closed++; return r.closeErr }
func (*scriptedVectorReader) SetOrderBy([]*plan.OrderBySpec)  {}
func (*scriptedVectorReader) GetOrderBy() []*plan.OrderBySpec { return nil }
func (*scriptedVectorReader) SetIndexParam(*plan.IndexReaderParam) {
}
func (*scriptedVectorReader) SetFilterZM(objectio.ZoneMap) {}

func vectorSourceSpec() *plan.VectorIndexScan {
	return &plan.VectorIndexScan{
		Index:           &plan.IndexDef{IndexAlgo: "ivfflat"},
		QueryVector:     plan2.MakePlan2Vecf32ConstExprWithType("[1,2]", 2),
		CandidateLimit:  plan2.MakePlan2Uint64ConstExprWithType(3),
		FirstRoundLimit: plan2.MakePlan2Uint64ConstExprWithType(1),
	}
}

func TestVectorSourceEvaluatesArgumentsAndUsesSearchPlugin(t *testing.T) {
	proc := testutil.NewProc(t)
	source := NewVectorSource(vectorSourceSpec(), []string{"pkid"}, []types.Type{types.T_int64.ToType()}).(*vectorSource)
	t.Cleanup(func() {
		source.Free(proc, false, nil)
		proc.Free()
	})

	require.NoError(t, source.ApplyPrepare(proc))
	in := batch.NewWithSize(0)
	in.SetRowCount(1)
	require.NoError(t, source.ApplyArgsEval(in, proc))
	require.Equal(t, 1, source.queryVec.Length())
	require.Equal(t, uint64(3), vector.GetFixedAtNoTypeCheck[uint64](source.limitVec, 0))
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](source.firstRoundVec, 0))

	// The registered IVF plugin is reached. The test process deliberately has
	// no transaction, so NewPlanReader rejects it after ApplyStart has passed
	// the registry and SearchPlugin dispatch boundary.
	err := source.ApplyStart(0, proc, nil)
	require.ErrorContains(t, err, "requires a process, transaction, and storage engine")
}

func TestApplyCarriesStatementTxnOffsetIntoVectorSource(t *testing.T) {
	proc := testutil.NewProc(t)
	apply := NewArgument()
	apply.VectorIndexScan = vectorSourceSpec()
	apply.VectorAttrs = []string{"pkid"}
	apply.Typs = []types.Type{types.T_int64.ToType()}
	apply.TxnOffset = 17
	t.Cleanup(func() {
		apply.Free(proc, false, nil)
		apply.Release()
		proc.Free()
	})
	require.NoError(t, apply.Prepare(proc))
	source, ok := apply.Source.(*vectorSource)
	require.True(t, ok)
	require.Equal(t, 17, source.txnOffset)
}

func TestVectorSourceReaderLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	source := &vectorSource{
		attrs:  []string{"pkid"},
		types:  []types.Type{types.T_int64.ToType()},
		reader: &scriptedVectorReader{values: []int64{42}},
	}
	t.Cleanup(func() {
		source.Free(proc, false, nil)
		proc.Free()
	})

	result, err := source.ApplyCall(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, []int64{42}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))

	result, err = source.ApplyCall(proc)
	require.NoError(t, err)
	require.Equal(t, vm.CancelResult.Status, result.Status)
	require.Nil(t, source.reader)

	readErr := errors.New("reader failed")
	source.reader = &scriptedVectorReader{readErr: readErr}
	result, err = source.ApplyCall(proc)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, vm.CancelResult.Status, result.Status)
}

func TestVectorSourceHandlesNullQueryAndCleansUp(t *testing.T) {
	proc := testutil.NewProc(t)
	source := &vectorSource{
		spec:  vectorSourceSpec(),
		attrs: []string{"pkid"},
		types: []types.Type{types.T_int64.ToType()},
	}
	t.Cleanup(func() { proc.Free() })

	source.queryVec = vector.NewVec(types.New(types.T_array_float32, 2, 0))
	require.NoError(t, vector.AppendArray(source.queryVec, []float32(nil), true, proc.Mp()))
	source.limitVec = vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(source.limitVec, uint64(1), false, proc.Mp()))
	require.NoError(t, source.ApplyStart(0, proc, nil))
	require.NotNil(t, source.reader)
	require.NoError(t, source.ApplyEnd(proc))
	require.Nil(t, source.reader)

	source.reader = &scriptedVectorReader{values: []int64{7}}
	source.output = batch.NewWithSize(1)
	source.output.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.output.Vecs[0], int64(7), false, proc.Mp()))
	source.output.SetRowCount(1)
	source.Reset(proc, false, nil)
	require.Nil(t, source.reader)
	require.Nil(t, source.queryVec)
	require.Zero(t, source.output.RowCount())

	source.reader = &scriptedVectorReader{closeErr: errors.New("close failed")}
	require.Error(t, source.closeReader())
	require.Nil(t, source.reader)
	source.Free(proc, false, nil)
	require.Nil(t, source.output)
}

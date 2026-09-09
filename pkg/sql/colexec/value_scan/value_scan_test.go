// Copyright 2021 Matrix Origin
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

package value_scan

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// add unit tests for cases
type valueScanTestCase struct {
	arg  *ValueScan
	proc *process.Process
}

type rowWindowExpressionExecutor struct {
	value    int64
	seenRows []int
	result   *vector.Vector
	mp       *mpool.MPool
}

func (e *rowWindowExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	if len(batches) != 1 || batches[0] == nil {
		return nil, moerr.NewInternalErrorNoCtx("row-window probe received an invalid input")
	}
	e.seenRows = append(e.seenRows, batches[0].RowCount())
	if batches[0].RowCount() != 1 {
		return nil, moerr.NewInternalErrorNoCtxf(
			"row-window probe received %d rows", batches[0].RowCount())
	}
	e.mp = proc.Mp()
	var err error
	e.result, err = vector.NewConstFixed(types.T_int64.ToType(), e.value, 1, e.mp)
	return e.result, err
}

func (e *rowWindowExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	return e.Eval(proc, batches, selectList)
}

func (e *rowWindowExpressionExecutor) ResetForNextQuery() {}

func (e *rowWindowExpressionExecutor) Free() {
	if e.result != nil {
		e.result.Free(e.mp)
		e.result = nil
	}
}

func (e *rowWindowExpressionExecutor) IsColumnExpr() bool { return false }
func (e *rowWindowExpressionExecutor) TypeName() string   { return "row-window-probe" }

func makeTestCases(t *testing.T) []valueScanTestCase {
	return []valueScanTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &ValueScan{
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestValueScan(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetBatchs(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetBatchs(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestValueScanEvaluatesRowLocalDependencyAgainstMaterializedColumn(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	intType := planpb.Type{Id: int32(types.T_int64), Width: 64}
	plus, err := function.GetFunctionByName(proc.Ctx, "+", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	localCol := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: pos,
		}}}
	}
	addOne := func(expr *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Typ: intType, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: plus.GetEncodedOverloadID(), ObjName: "+"},
			Args: []*planpb.Expr{expr, {
				Typ:  intType,
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}},
			}},
		}}}
	}
	rowset := &planpb.RowsetData{
		RowCount: 2,
		Cols: []*planpb.ColData{
			{Data: []*planpb.RowsetExpr{
				{RowPos: 0, Expr: addOne(localCol(1))},
				{RowPos: 1, Expr: addOne(localCol(1))},
			}},
			{Data: []*planpb.RowsetExpr{
				{RowPos: 0, Expr: &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 10}}}}},
				{RowPos: 1, Expr: &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 20}}}}},
			}},
		},
	}
	bat := batch.NewWithSize(2)
	bat.SetRowCount(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{0, 0}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []int64{0, 0}, nil, proc.Mp()))
	vs := &ValueScan{
		NodeType:   planpb.Node_VALUE_SCAN,
		ColCount:   2,
		Batchs:     []*batch.Batch{bat, nil},
		RowsetData: rowset,
	}
	require.NoError(t, vs.Prepare(proc))
	result, err := vs.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{11, 21}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
	require.Equal(t, []int64{10, 20}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1]))
	vs.Free(proc, false, nil)
}

func TestEvalRowsetDataUsesBoundedRowWindows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	const rowCount = 128
	intType := types.T_int64.ToType()
	planIntType := planpb.Type{Id: int32(types.T_int64), Width: 64}
	localCol := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planIntType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: pos}},
		}
	}

	input := batch.NewWithSize(1)
	input.SetRowCount(rowCount)
	input.Vecs[0] = vector.NewVec(intType)
	inputValues := make([]int64, rowCount)
	for i := range inputValues {
		inputValues[i] = int64(i)
	}
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], inputValues, nil, proc.Mp()))
	defer input.Clean(proc.Mp())

	target := vector.NewVec(intType)
	require.NoError(t, vector.AppendFixedList(target, make([]int64, rowCount), nil, proc.Mp()))
	defer target.Free(proc.Mp())

	rowset := make([]*planpb.RowsetExpr, rowCount)
	execs := make([]colexec.ExpressionExecutor, rowCount)
	probes := make([]*rowWindowExpressionExecutor, rowCount)
	for row := 0; row < rowCount; row++ {
		rowset[row] = &planpb.RowsetExpr{RowPos: int32(row), Expr: localCol(0)}
		probes[row] = &rowWindowExpressionExecutor{value: int64(row)}
		execs[row] = probes[row]
		defer probes[row].Free()
	}

	require.NoError(t, evalRowsetData(proc, rowset, target, execs, input))
	for row, probe := range probes {
		require.Equal(t, []int{1}, probe.seenRows,
			"row %d must not evaluate its expression against the full VALUES batch", row)
	}
	require.Equal(t, inputValues, vector.MustFixedColNoTypeCheck[int64](target))
}

func TestValueScanEvaluatesVolatileSourceOnceForDependentColumn(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	floatType := planpb.Type{Id: int32(types.T_float64), Width: 64}
	randFn, err := function.GetFunctionByName(proc.Ctx, "rand", nil)
	require.NoError(t, err)
	randExpr := func() *planpb.Expr {
		return &planpb.Expr{
			Typ: floatType,
			Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
				Obj: randFn.GetEncodedOverloadID(), ObjName: "rand",
			}}},
		}
	}
	localCol := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  floatType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: pos}},
		}
	}
	rowset := &planpb.RowsetData{
		RowCount: 2,
		Cols: []*planpb.ColData{
			{Data: []*planpb.RowsetExpr{{RowPos: 0, Expr: randExpr()}, {RowPos: 1, Expr: randExpr()}}},
			{Data: []*planpb.RowsetExpr{{RowPos: 0, Expr: localCol(0)}, {RowPos: 1, Expr: localCol(0)}}},
		},
	}
	bat := batch.NewWithSize(2)
	bat.SetRowCount(2)
	bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []float64{0, 0}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []float64{0, 0}, nil, proc.Mp()))
	vs := &ValueScan{
		NodeType:   planpb.Node_VALUE_SCAN,
		ColCount:   2,
		Batchs:     []*batch.Batch{bat, nil},
		RowsetData: rowset,
	}
	require.NoError(t, vs.Prepare(proc))
	result, err := vs.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	source := vector.MustFixedColNoTypeCheck[float64](result.Batch.Vecs[0])
	dependent := vector.MustFixedColNoTypeCheck[float64](result.Batch.Vecs[1])
	require.Len(t, source, 2)
	require.Equal(t, source, dependent,
		"a dependent default must consume the already materialized volatile source")
	vs.Free(proc, false, nil)
}

func TestValueScanDependencyCanReadConstantSourceColumn(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	intType := planpb.Type{Id: int32(types.T_int64), Width: 64}
	localCol := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: pos,
		}}}
	}
	batchData := batch.NewWithSize(3)
	batchData.SetRowCount(2)
	for i := range batchData.Vecs {
		batchData.Vecs[i] = vector.NewVec(types.T_int64.ToType())
	}
	// Column 2 represents a constant expression already folded by
	// constructValueScan and therefore has no RowsetData entries. The
	// dependent column still has to evaluate against this source vector.
	require.NoError(t, vector.AppendFixedList(batchData.Vecs[0], []int64{1, 2}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(batchData.Vecs[1], []int64{0, 0}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(batchData.Vecs[2], []int64{10, 20}, nil, proc.Mp()))
	rowset := &planpb.RowsetData{
		RowCount: 2,
		Cols: []*planpb.ColData{
			{},
			{Data: []*planpb.RowsetExpr{
				{RowPos: 0, Expr: localCol(2)},
				{RowPos: 1, Expr: localCol(2)},
			}},
			{},
		},
	}
	vs := &ValueScan{
		NodeType:   planpb.Node_VALUE_SCAN,
		ColCount:   3,
		Batchs:     []*batch.Batch{batchData, nil},
		RowsetData: rowset,
	}
	require.NoError(t, vs.Prepare(proc))
	result, err := vs.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{10, 20}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1]))
	vs.Free(proc, false, nil)
}

func TestRowsetExprHasLocalColumnRef(t *testing.T) {
	require.False(t, rowsetExprHasLocalColumnRef(nil))
	require.False(t, rowsetExprHasLocalColumnRef(&planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1}}}))
	require.True(t, rowsetExprHasLocalColumnRef(&planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
		{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0}}},
	}}}}))
}

func resetBatchs(arg *ValueScan, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	arg.Batchs = append(arg.Batchs, bat)
}

func TestGenSubBatchFromOriginBatch(t *testing.T) {
	testCases := []struct {
		name      string
		types     []types.Type
		batchSize int
		expected  int
	}{
		{"BatchSize8191", []types.Type{types.T_int32.ToType()}, 8191, 8191},
		{"BatchSize8192", []types.Type{types.T_int32.ToType()}, 8192, 8192},
		{"BatchSize8193", []types.Type{types.T_int32.ToType()}, 8193, 8193},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			vs := &ValueScan{
				Batchs: make([]*batch.Batch, 2),
			}

			// Create a mock batch with the specified size
			bat := testutil.NewBatch(tc.types, false, tc.batchSize, proc.Mp())
			vs.Batchs[0] = bat

			// Generate sub-batch
			rowCnt := 0
			curValue := int32(0)
			for {
				subBatch, err := vs.genSubBatchFromOriginBatch(proc)
				require.NoError(t, err)
				if subBatch == nil {
					break
				}
				require.LessOrEqual(t, subBatch.RowCount(), oneBatchMaxRow)
				rowCnt += subBatch.RowCount()
				for i := 0; i < subBatch.RowCount(); i++ {
					vec := subBatch.GetVector(0)
					v := vector.GetFixedAtNoTypeCheck[int32](vec, i)
					require.Equal(t, curValue, v)
					curValue++
				}
			}
			require.Equal(t, rowCnt, tc.expected)
		})
	}
}

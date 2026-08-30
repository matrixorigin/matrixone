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

package table_scan

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	arg := &TableScan{}
	arg.String(buf)
}

func TestPrepare(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Close().Return(nil).AnyTimes()
	reader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
	arg := &TableScan{
		Reader: reader,
	}
	proc := testutil.NewProc(t)
	err := arg.Prepare(proc)
	require.NoError(t, err)
}

func TestCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.TxnOperator = txnOperator

	typ1 := types.T_Rowid.ToType()
	typ2 := types.T_uint64.ToType()
	typ3 := types.T_varbinary.ToType()
	reader := getReader(t, ctrl, proc.Mp())
	arg := &TableScan{
		Reader: reader,
		Attrs:  []string{catalog.Row_ID, "int_col", "varchar_col"},
		Types:  []plan.Type{plan.MakePlan2Type(&typ1), plan.MakePlan2Type(&typ2), plan.MakePlan2Type(&typ3)},
	}
	err := arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(arg, proc)
	require.NoError(t, err)

	arg.Reset(proc, false, nil)

	reader = getReader(t, ctrl, proc.Mp())
	arg.Reader = reader
	err = arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestPadCharColumnsToFullLength(t *testing.T) {
	proc := testutil.NewProc(t)
	charType := types.New(types.T_char, 8, 0)
	varcharType := types.New(types.T_varchar, 8, 0)
	bat := batch.NewWithSize(5)
	var err error
	bat.Vecs[0] = vector.NewVec(charType)
	bat.Vecs[1] = vector.NewVec(varcharType)
	bat.Vecs[2], err = vector.NewConstBytes(types.New(types.T_char, 0, 0), []byte("MO"), 3, proc.Mp())
	require.NoError(t, err)
	bat.Vecs[3], err = vector.NewConstBytes(charType, []byte("12345678"), 3, proc.Mp())
	require.NoError(t, err)
	bat.Vecs[4], err = vector.NewConstBytes(charType, []byte("MO"), 3, proc.Mp())
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("MO"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("你好"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], nil, true, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("MO"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("你好"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], nil, true, proc.Mp()))
	bat.SetRowCount(3)

	require.NoError(t, padCharColumnsToFullLength(bat, proc))
	require.Equal(t, "MO      ", string(bat.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "你好      ", string(bat.Vecs[0].GetBytesAt(1)))
	require.True(t, bat.Vecs[0].GetNulls().Contains(2))
	require.Equal(t, "MO", string(bat.Vecs[1].GetBytesAt(0)))
	require.Equal(t, "你好", string(bat.Vecs[1].GetBytesAt(1)))
	require.Equal(t, "MO", string(bat.Vecs[2].GetBytesAt(0)))
	require.Equal(t, "12345678", string(bat.Vecs[3].GetBytesAt(0)))
	require.Equal(t, "MO      ", string(bat.Vecs[4].GetBytesAt(0)))

	bat.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestPrepareResolvesPadCharToFullLength(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Close().Return(nil).AnyTimes()
	reader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
	proc := testutil.NewProc(t)
	mode := "STRICT_TRANS_TABLES,PAD_CHAR_TO_FULL_LENGTH"
	proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
		return mode, nil
	})
	arg := &TableScan{Reader: reader}
	require.NoError(t, arg.Prepare(proc))
	require.True(t, arg.ctr.padCharToFullLength)

	arg.Reset(proc, false, nil)
	mode = "STRICT_TRANS_TABLES"
	require.NoError(t, arg.Prepare(proc))
	require.False(t, arg.ctr.padCharToFullLength)

	arg.Reset(proc, false, nil)
	wantErr := errors.New("resolve failed")
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return nil, wantErr
	})
	require.ErrorIs(t, arg.Prepare(proc), wantErr)

	arg.Free(proc, false, nil)
	proc.Free()
}

func getReader(t *testing.T, ctrl *gomock.Controller, m *mpool.MPool) engine.Reader {
	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, attrs []string, expr *plan.Expr, b interface{}, bat *batch.Batch) (bool, error) {
		err := vector.AppendFixed(bat.GetVector(0), types.Rowid([types.RowidSize]byte{}), false, m)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendFixed(bat.GetVector(1), uint64(272464), false, m)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendBytes(bat.GetVector(2), []byte("empno"), false, m)
		if err != nil {
			require.Nil(t, err)
		}
		bat.SetRowCount(bat.GetVector(1).Length())
		return false, nil
	}).AnyTimes()
	reader.EXPECT().Close().Return(nil).AnyTimes()
	reader.EXPECT().GetOrderBy().Return(nil).AnyTimes()

	return reader
}

// TestInlineFilter verifies that:
// 1. Inline filter correctly filters rows within TableScan.Call()
// 2. Filter-only columns are removed by ExecProjection (not leaked to caller)
// 3. Edge cases: all rows filtered out, no rows filtered out
func TestInlineFilter(t *testing.T) {
	int32Type := types.T_int32.ToType()
	boolType := types.T_bool.ToType()

	fr, err := function.GetFunctionByName(context.TODO(), ">", []types.Type{int32Type, int32Type})
	require.NoError(t, err)
	gtFuncID := fr.GetEncodedOverloadID()

	// makeColRef creates a column reference expression
	makeColRef := func(colPos int32, name string) *pbplan.Expr {
		return &pbplan.Expr{
			Typ: plan.MakePlan2Type(&int32Type),
			Expr: &pbplan.Expr_Col{
				Col: &pbplan.ColRef{
					RelPos: 0,
					ColPos: colPos,
					Name:   name,
				},
			},
		}
	}

	// makeInt32Const creates an int32 literal expression
	makeInt32Const := func(v int32) *pbplan.Expr {
		return &pbplan.Expr{
			Typ: pbplan.Type{
				Id:          int32(types.T_int32),
				NotNullable: true,
			},
			Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{
				Isnull: false,
				Value:  &pbplan.Literal_I32Val{I32Val: v},
			}},
		}
	}

	// makeGtFilter creates: colRef > constVal
	makeGtFilter := func(colPos int32, colName string, threshold int32) *pbplan.Expr {
		return &pbplan.Expr{
			Typ: plan.MakePlan2Type(&boolType),
			Expr: &pbplan.Expr_F{
				F: &pbplan.Function{
					Func: &pbplan.ObjectRef{
						ObjName: ">",
						Obj:     gtFuncID,
					},
					Args: []*pbplan.Expr{
						makeColRef(colPos, colName),
						makeInt32Const(threshold),
					},
				},
			},
		}
	}

	// Test schema: col_a(int32, output), col_b(int32, filter-only), col_c(int32, output)
	// Data: 10 rows, a=[1..10], b=[5,10,15,..,50], c=[100,200,..,1000]
	scanTypes := []plan.Type{
		plan.MakePlan2Type(&int32Type),
		plan.MakePlan2Type(&int32Type),
		plan.MakePlan2Type(&int32Type),
	}
	scanAttrs := []string{"col_a", "col_b", "col_c"}

	// ProjectList: output col_a (pos=0) and col_c (pos=2), col_b is filter-only
	projectList := []*pbplan.Expr{
		makeColRef(0, "col_a"),
		makeColRef(2, "col_c"),
	}

	// getFilterTestReader returns a mock reader that provides 10 rows once, then signals end.
	getFilterTestReader := func(ctrl *gomock.Controller, m *mpool.MPool) engine.Reader {
		reader := mock_frontend.NewMockReader(ctrl)
		callCount := 0
		reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, attrs []string, expr *plan.Expr, b interface{}, bat *batch.Batch) (bool, error) {
				callCount++
				if callCount > 1 {
					return true, nil // end of data
				}
				for i := 0; i < 10; i++ {
					require.NoError(t, vector.AppendFixed(bat.GetVector(0), int32(i+1), false, m))       // a: 1..10
					require.NoError(t, vector.AppendFixed(bat.GetVector(1), int32((i+1)*5), false, m))   // b: 5,10,..,50
					require.NoError(t, vector.AppendFixed(bat.GetVector(2), int32((i+1)*100), false, m)) // c: 100..1000
				}
				bat.SetRowCount(10)
				return false, nil
			}).AnyTimes()
		reader.EXPECT().Close().Return(nil).AnyTimes()
		reader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
		return reader
	}

	newTestProc := func(ctrl *gomock.Controller) *process.Process {
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		proc := testutil.NewProc(t)
		proc.Base.TxnClient = txnClient
		proc.Ctx = context.TODO()
		proc.Base.TxnOperator = txnOperator
		return proc
	}

	t.Run("partial rows filtered, filter-only column not leaked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		proc := newTestProc(ctrl)

		// Filter: col_b > 20 → rows with b=25,30,35,40,45,50 pass (6 of 10)
		arg := &TableScan{
			Reader:      getFilterTestReader(ctrl, proc.Mp()),
			Attrs:       scanAttrs,
			Types:       scanTypes,
			FilterExprs: []*pbplan.Expr{makeGtFilter(1, "col_b", 20)},
		}
		arg.ProjectList = projectList

		require.NoError(t, arg.Prepare(proc))
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)

		// Verify filtering: 6 rows pass (b > 20 → indices 4..9)
		require.NotNil(t, res.Batch)
		require.Equal(t, 6, res.Batch.RowCount())

		// Verify filter-only column not leaked: output has 2 columns (a, c), not 3
		require.Equal(t, 2, len(res.Batch.Vecs))

		// Verify projected column values
		aVec := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])
		cVec := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])
		require.Equal(t, []int32{5, 6, 7, 8, 9, 10}, aVec)
		require.Equal(t, []int32{500, 600, 700, 800, 900, 1000}, cVec)

		arg.Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
	})

	t.Run("all rows filtered out", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		proc := newTestProc(ctrl)

		// Filter: col_b > 100 → no rows pass
		arg := &TableScan{
			Reader:      getFilterTestReader(ctrl, proc.Mp()),
			Attrs:       scanAttrs,
			Types:       scanTypes,
			FilterExprs: []*pbplan.Expr{makeGtFilter(1, "col_b", 100)},
		}
		arg.ProjectList = projectList

		require.NoError(t, arg.Prepare(proc))
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)

		// All rows filtered → reads next block → reader returns isEnd=true → nil batch
		require.Nil(t, res.Batch)

		arg.Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
	})

	t.Run("no rows filtered, all pass", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		proc := newTestProc(ctrl)

		// Filter: col_b > 0 → all 10 rows pass
		arg := &TableScan{
			Reader:      getFilterTestReader(ctrl, proc.Mp()),
			Attrs:       scanAttrs,
			Types:       scanTypes,
			FilterExprs: []*pbplan.Expr{makeGtFilter(1, "col_b", 0)},
		}
		arg.ProjectList = projectList

		require.NoError(t, arg.Prepare(proc))
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)

		require.NotNil(t, res.Batch)
		require.Equal(t, 10, res.Batch.RowCount())
		require.Equal(t, 2, len(res.Batch.Vecs)) // only projected columns

		aVec := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])
		cVec := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])
		require.Equal(t, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, aVec)
		require.Equal(t, []int32{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}, cVec)

		arg.Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
	})

	t.Run("reset and reuse with filter", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		proc := newTestProc(ctrl)

		arg := &TableScan{
			Reader:      getFilterTestReader(ctrl, proc.Mp()),
			Attrs:       scanAttrs,
			Types:       scanTypes,
			FilterExprs: []*pbplan.Expr{makeGtFilter(1, "col_b", 20)},
		}
		arg.ProjectList = projectList

		// First execution
		require.NoError(t, arg.Prepare(proc))
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		require.Equal(t, 6, res.Batch.RowCount())
		require.Equal(t, 2, len(res.Batch.Vecs))

		// Reset and re-execute
		arg.Reset(proc, false, nil)
		arg.Reader = getFilterTestReader(ctrl, proc.Mp())
		require.NoError(t, arg.Prepare(proc))
		res, err = vm.Exec(arg, proc)
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		require.Equal(t, 6, res.Batch.RowCount())
		require.Equal(t, 2, len(res.Batch.Vecs))

		arg.Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
	})

	// makeConstBool creates a const bool literal expression (evaluates to a const vector)
	makeConstBool := func(v bool) *pbplan.Expr {
		return &pbplan.Expr{
			Typ: pbplan.Type{Id: int32(types.T_bool), NotNullable: true},
			Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{
				Isnull: false,
				Value:  &pbplan.Literal_Bval{Bval: v},
			}},
		}
	}

	t.Run("const false filter eliminates all rows via SetRowCount", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		proc := newTestProc(ctrl)

		// Const-false filter: triggers the vec.IsConst() path in evalFilter → SetRowCount(0)
		arg := &TableScan{
			Reader:      getFilterTestReader(ctrl, proc.Mp()),
			Attrs:       scanAttrs,
			Types:       scanTypes,
			FilterExprs: []*pbplan.Expr{makeConstBool(false)},
		}
		arg.ProjectList = projectList

		require.NoError(t, arg.Prepare(proc))
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		// const-false: all rows eliminated → continues to next block → reader ends → nil batch
		require.Nil(t, res.Batch)

		arg.Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
	})

	t.Run("runtime filter exprs lifecycle: prepare, reset, re-prepare", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		proc := newTestProc(ctrl)

		// RuntimeFilterExprs: col_b > 20 (set as runtime filter, not static)
		runtimeFilter := makeGtFilter(1, "col_b", 20)
		arg := &TableScan{
			Reader:             getFilterTestReader(ctrl, proc.Mp()),
			Attrs:              scanAttrs,
			Types:              scanTypes,
			RuntimeFilterExprs: []*pbplan.Expr{runtimeFilter},
		}
		arg.ProjectList = projectList

		require.NoError(t, arg.Prepare(proc))
		// After Prepare: runtimeFilterExecutors and allFilterExecutors should be built
		require.Equal(t, 1, len(arg.ctr.runtimeFilterExecutors))
		require.Equal(t, 1, len(arg.ctr.allFilterExecutors))

		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		require.Equal(t, 6, res.Batch.RowCount()) // b > 20 → 6 rows

		// Reset: runtimeFilterExecutors freed, allFilterExecutors nil'd
		arg.Reset(proc, false, nil)
		require.Nil(t, arg.ctr.runtimeFilterExecutors)
		require.Nil(t, arg.ctr.allFilterExecutors)
		require.Nil(t, arg.RuntimeFilterExprs)

		// Re-prepare with new runtime filter (col_b > 40 → 2 rows: b=45,50)
		arg.Reader = getFilterTestReader(ctrl, proc.Mp())
		arg.RuntimeFilterExprs = []*pbplan.Expr{makeGtFilter(1, "col_b", 40)}
		require.NoError(t, arg.Prepare(proc))
		res, err = vm.Exec(arg, proc)
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		require.Equal(t, 2, res.Batch.RowCount())

		arg.Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
	})

	t.Run("static and runtime filters combined via allFilterExecutors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		proc := newTestProc(ctrl)

		// Static: col_b > 20 (6 rows pass); Runtime: col_b > 40 (2 rows pass)
		// Combined: rows where col_b > 40 (runtime applied first in allFilterExecutors)
		arg := &TableScan{
			Reader:             getFilterTestReader(ctrl, proc.Mp()),
			Attrs:              scanAttrs,
			Types:              scanTypes,
			FilterExprs:        []*pbplan.Expr{makeGtFilter(1, "col_b", 20)},
			RuntimeFilterExprs: []*pbplan.Expr{makeGtFilter(1, "col_b", 40)},
		}
		arg.ProjectList = projectList

		require.NoError(t, arg.Prepare(proc))
		// allFilterExecutors = runtime (1) + static (1) = 2 total
		require.Equal(t, 2, len(arg.ctr.allFilterExecutors))

		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		// b > 40: rows b=45(row 9), b=50(row 10) → 2 rows
		require.Equal(t, 2, res.Batch.RowCount())
		require.Equal(t, 2, len(res.Batch.Vecs))

		arg.Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
	})
}

// TestShrinkPopulatedVecs_SkipsUnmaterializedColumn reproduces the batch the vector-TopN
// pushdown hands the reader filter: blockio.materializeVectorTopNRows loads only the
// selected rows and then CleanOnlyData()s the ORDER BY vector column itself, so that one
// column sits at 0 rows among siblings that hold the block's rows. batch.Shrink assumes
// every vector holds RowCount() rows and panicked with "index out of range [N] with
// length 0", killing the query. Every populated vector must still be shrunk.
func TestShrinkPopulatedVecs_SkipsUnmaterializedColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(3)

	pk := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(pk, []int64{10, 11, 12, 13, 14}, nil, mp))
	// The ORDER BY vector column: values already consumed to compute distances, then
	// cleared by the reader — deliberately 0 rows while its siblings hold 5.
	emptied := vector.NewVec(types.T_int64.ToType())
	dist := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(dist, []float64{0.5, 0.4, 0.3, 0.2, 0.1}, nil, mp))

	bat.Vecs[0], bat.Vecs[1], bat.Vecs[2] = pk, emptied, dist
	bat.SetRowCount(5)
	defer bat.Clean(mp)

	require.NotPanics(t, func() {
		shrinkPopulatedVecs(bat, []int64{1, 3})
	})

	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, []int64{11, 13}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]))
	require.Equal(t, []float64{0.4, 0.2}, vector.MustFixedColWithTypeCheck[float64](bat.Vecs[2]))
	require.Equal(t, 0, bat.Vecs[1].Length(), "the unmaterialized column is left alone")
}

// TestShrinkPopulatedVecs_NoOpAndEmptyBatch: selecting every row is a no-op (batch.Shrink
// has the same short-circuit), and a batch with no rows must not be walked at all.
func TestShrinkPopulatedVecs_NoOpAndEmptyBatch(t *testing.T) {
	mp := mpool.MustNewZero()

	bat := batch.NewWithSize(1)
	v := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(v, []int64{1, 2, 3}, nil, mp))
	bat.Vecs[0] = v
	bat.SetRowCount(3)
	defer bat.Clean(mp)

	shrinkPopulatedVecs(bat, []int64{0, 1, 2})
	require.Equal(t, 3, bat.RowCount())
	require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]))

	// RowCount 0 with all-empty vectors: nothing to shrink, must not panic.
	empty := batch.NewWithSize(1)
	empty.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	empty.SetRowCount(0)
	defer empty.Clean(mp)
	require.NotPanics(t, func() { shrinkPopulatedVecs(empty, []int64{}) })
	require.Equal(t, 0, empty.RowCount())
}

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

package table_scan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type lateMaterializationTestReader struct {
	reads     int
	eagerRead bool
	closed    bool
}

func (r *lateMaterializationTestReader) Read(
	context.Context,
	[]string,
	*pbplan.Expr,
	*mpool.MPool,
	*batch.Batch,
) (bool, error) {
	r.eagerRead = true
	return true, nil
}

func (r *lateMaterializationTestReader) ReadWithFilter(
	_ context.Context,
	_ []string,
	earlyColumns []int,
	filter engine.ReaderFilter,
	mp *mpool.MPool,
	bat *batch.Batch,
) (bool, error) {
	r.reads++
	if r.reads > 1 {
		return true, nil
	}
	for i := int32(0); i < 4; i++ {
		if err := vector.AppendFixed(bat.Vecs[0], i, false, mp); err != nil {
			return false, err
		}
	}
	bat.SetRowCount(4)
	_, err := filter(bat, earlyColumns)
	return false, err
}

func (r *lateMaterializationTestReader) Close() error {
	r.closed = true
	return nil
}

func (*lateMaterializationTestReader) SetFilterZM(objectio.ZoneMap)           {}
func (*lateMaterializationTestReader) SetOrderBy([]*pbplan.OrderBySpec)       {}
func (*lateMaterializationTestReader) SetIndexParam(*pbplan.IndexReaderParam) {}
func (*lateMaterializationTestReader) GetOrderBy() []*pbplan.OrderBySpec      { return nil }

func lateTestCol(pos int32, typ types.Type) *pbplan.Expr {
	return &pbplan.Expr{
		Typ: plan.MakePlan2Type(&typ),
		Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{
			RelPos: 0,
			ColPos: pos,
		}},
	}
}

func lateTestCompare(t *testing.T, name string, pos int32, value int32) *pbplan.Expr {
	t.Helper()
	intType := types.T_int32.ToType()
	boolType := types.T_bool.ToType()
	fn, err := function.GetFunctionByName(context.Background(), name, []types.Type{intType, intType})
	require.NoError(t, err)
	return &pbplan.Expr{
		Typ: plan.MakePlan2Type(&boolType),
		Expr: &pbplan.Expr_F{F: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: name, Obj: fn.GetEncodedOverloadID()},
			Args: []*pbplan.Expr{
				lateTestCol(pos, intType),
				{
					Typ:  plan.MakePlan2Type(&intType),
					Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_I32Val{I32Val: value}}},
				},
			},
		}},
	}
}

func TestConfigureLateMaterialization(t *testing.T) {
	intType := types.T_int32.ToType()
	shortVarchar := types.New(types.T_varchar, 36, 0)
	wideVarchar := types.New(types.T_varchar, 1024, 0)
	textType := types.T_text.ToType()

	newScan := func(filterPos int32) *TableScan {
		return &TableScan{
			Attrs: []string{"filter_col", "short_value", "text_value", "wide_value"},
			Types: []pbplan.Type{
				plan.MakePlan2Type(&intType),
				plan.MakePlan2Type(&shortVarchar),
				plan.MakePlan2Type(&textType),
				plan.MakePlan2Type(&wideVarchar),
			},
			FilterExprs: []*pbplan.Expr{lateTestCol(filterPos, func() types.Type {
				switch filterPos {
				case 2:
					return textType
				default:
					return intType
				}
			}())},
			ctr: container{allFilterExecutors: []colexec.ExpressionExecutor{nil}},
		}
	}

	t.Run("defers only wide output columns", func(t *testing.T) {
		scan := newScan(0)
		scan.configureLateMaterialization()
		require.Equal(t, []int{0, 1}, scan.ctr.earlyColumns)
		require.Equal(t, []int{2, 3}, scan.ctr.lateColumns)
	})

	t.Run("wide predicate column stays early", func(t *testing.T) {
		scan := newScan(2)
		scan.configureLateMaterialization()
		require.Equal(t, []int{0, 1, 2}, scan.ctr.earlyColumns)
		require.Equal(t, []int{3}, scan.ctr.lateColumns)
	})

	t.Run("unsupported reference stays eager", func(t *testing.T) {
		scan := newScan(0)
		scan.FilterExprs = []*pbplan.Expr{{Expr: &pbplan.Expr_Raw{Raw: &pbplan.RawColRef{}}}}
		scan.configureLateMaterialization()
		require.Empty(t, scan.ctr.earlyColumns)
		require.Empty(t, scan.ctr.lateColumns)
	})
}

func TestLateMaterializationFilterPreservesOriginalSelections(t *testing.T) {
	proc := testutil.NewProc(t)
	intType := types.T_int32.ToType()
	textType := types.T_text.ToType()
	scan := &TableScan{
		Attrs: []string{"filter_col", "payload"},
		Types: []pbplan.Type{plan.MakePlan2Type(&intType), plan.MakePlan2Type(&textType)},
		FilterExprs: []*pbplan.Expr{
			lateTestCompare(t, ">", 0, 1),
			lateTestCompare(t, "<", 0, 4),
		},
	}
	require.NoError(t, scan.Prepare(proc))
	require.Equal(t, []int{0}, scan.ctr.earlyColumns)
	require.Equal(t, []int{1}, scan.ctr.lateColumns)

	bat := batch.NewOffHeapWithSize(2)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(intType)
	bat.Vecs[1] = vector.NewOffHeapVecWithType(textType)
	for i := int32(0); i < 5; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], i, false, proc.Mp()))
	}
	bat.SetRowCount(5)

	result, err := scan.applyReaderFilter(proc, bat, []int{0})
	require.NoError(t, err)
	require.False(t, result.All)
	require.Equal(t, []int64{2, 3}, result.Sels)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, []int32{2, 3}, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0]))
	require.Zero(t, bat.Vecs[1].Length())

	bat.Clean(proc.Mp())
	scan.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.GetMPool().CurrNB())
}

func TestTableScanUsesLateMaterializationReader(t *testing.T) {
	proc := testutil.NewProc(t)
	intType := types.T_int32.ToType()
	textType := types.T_text.ToType()
	reader := &lateMaterializationTestReader{}
	scan := &TableScan{
		Reader: reader,
		Attrs:  []string{"filter_col", "payload"},
		Types:  []pbplan.Type{plan.MakePlan2Type(&intType), plan.MakePlan2Type(&textType)},
		FilterExprs: []*pbplan.Expr{
			lateTestCompare(t, ">", 0, 10),
		},
	}
	require.NoError(t, scan.Prepare(proc))

	result, err := vm.Exec(scan, proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, 2, reader.reads)
	require.False(t, reader.eagerRead)
	require.Zero(t, scan.ctr.buf.Vecs[0].Length())
	require.Zero(t, scan.ctr.buf.Vecs[1].Length())
	require.Equal(t, int64(4), scan.OpAnalyzer.GetOpStats().InputRows)

	scan.Reset(proc, false, nil)
	require.True(t, reader.closed)
	scan.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.GetMPool().CurrNB())
}

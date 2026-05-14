// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestIsContiguousMapping(t *testing.T) {
	cases := []struct {
		name string
		cols []int
		want bool
	}{
		{"empty", []int{}, false},
		{"single-zero", []int{0}, true},
		{"single-nonzero", []int{7}, true},
		{"contiguous-from-zero", []int{0, 1, 2, 3}, true},
		{"contiguous-from-nonzero", []int{3, 4, 5}, true},
		{"reordered", []int{2, 0, 1}, false},
		{"jump", []int{0, 1, 3}, false},
		{"jump-late", []int{3, 4, 6}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, isContiguousMapping(c.cols))
		})
	}
}

// insertTableHarness wires up just enough of MultiUpdate state for
// insert_table to run, capturing every batch that Source.Write receives.
type insertTableHarness struct {
	op         *MultiUpdate
	updateCtx  *MultiUpdateCtx
	info       *updateCtxInfo
	written    []*batch.Batch // captured batches passed to Source.Write
	insertBuf  *batch.Batch   // pre-allocated buffer (used by non-contiguous path)
	tableName  string
}

func newInsertTableHarness(
	t *testing.T,
	ctrl *gomock.Controller,
	mp *mpool.MPool,
	insertCols []int,
	colTypes []types.Type,
	attrs []string,
) *insertTableHarness {
	t.Helper()

	tableName := "t_unit"
	updateCtx := &MultiUpdateCtx{
		InsertCols: insertCols,
		TableDef:   nil,
	}

	h := &insertTableHarness{tableName: tableName, updateCtx: updateCtx}

	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *batch.Batch) error {
			h.written = append(h.written, b)
			return nil
		}).AnyTimes()

	h.info = &updateCtxInfo{
		Source:       rel,
		tableType:    UpdateMainTable,
		isContiguous: isContiguousMapping(insertCols),
	}

	h.op = &MultiUpdate{
		ctr: container{
			updateCtxInfos: map[string]*updateCtxInfo{tableName: h.info},
			action:         actionInsert,
		},
		MultiUpdateCtx: []*MultiUpdateCtx{updateCtx},
	}
	h.op.addAffectedRowsFunc = h.op.doAddAffectedRows

	// insert_table only reads updateCtx.TableDef.Name on the hot path.
	updateCtx.TableDef = &plan.TableDef{Name: tableName}

	// Pre-allocate the insertBuf only for the non-contiguous path. Contiguous
	// path never reads it; passing it in keeps the call site's signature happy.
	insertBuf := batch.NewOffHeapWithSize(len(insertCols))
	insertBuf.SetAttributes(attrs)
	for i := range insertCols {
		insertBuf.Vecs[i] = vector.NewOffHeapVecWithType(colTypes[i])
	}
	h.insertBuf = insertBuf
	return h
}

func (h *insertTableHarness) cleanup(mp *mpool.MPool) {
	if h.insertBuf != nil {
		h.insertBuf.Clean(mp)
	}
}

func makeInputBatch(t *testing.T, mp *mpool.MPool, rowCount int) *batch.Batch {
	t.Helper()
	a := testutil.MakeInt64Vector(makeTestPkArray(0, rowCount), nil, mp)
	b := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil, nil)
	c := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil, nil)
	d := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil, nil)
	bat := &batch.Batch{
		Vecs:  []*vector.Vector{a, b, c, d},
		Attrs: []string{"a", "b", "c", "d"},
	}
	bat.SetRowCount(rowCount)
	return bat
}

func TestInsertTable_Contiguous_BorrowsVectors(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, false)
	defer ctrl.Finish()
	mp := proc.GetMPool()

	insertCols := []int{1, 2, 3}
	colTypes := []types.Type{
		types.T_int32.ToType(),
		types.T_int32.ToType(),
		types.T_int32.ToType(),
	}
	attrs := []string{"b", "c", "d"}
	h := newInsertTableHarness(t, ctrl, mp, insertCols, colTypes, attrs)
	defer h.cleanup(mp)

	require.True(t, h.info.isContiguous, "InsertCols=[1,2,3] must be contiguous")

	input := makeInputBatch(t, mp, 4)
	defer input.Clean(mp)

	analyzer := process.NewTempAnalyzer()
	require.NoError(t, h.op.insert_table(proc, analyzer, h.updateCtx, input, h.insertBuf))

	require.Len(t, h.written, 1)
	written := h.written[0]
	require.Same(t, h.info.refBatch, written, "writeBatch must be info.refBatch on contiguous path")
	require.Equal(t, input.RowCount(), written.RowCount())

	// Vector ownership stays with input; refBatch.Vecs[i] are borrowed pointers.
	for i, inputIdx := range insertCols {
		require.Same(t, input.Vecs[inputIdx], written.Vecs[i],
			"contiguous path must borrow input vector at slot %d", i)
	}
	require.EqualValues(t, input.RowCount(), h.op.GetAffectedRows())
}

func TestInsertTable_Contiguous_RefBatchReused(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, false)
	defer ctrl.Finish()
	mp := proc.GetMPool()

	insertCols := []int{0, 1, 2, 3}
	colTypes := []types.Type{
		types.T_int64.ToType(),
		types.T_int32.ToType(),
		types.T_int32.ToType(),
		types.T_int32.ToType(),
	}
	attrs := []string{"a", "b", "c", "d"}
	h := newInsertTableHarness(t, ctrl, mp, insertCols, colTypes, attrs)
	defer h.cleanup(mp)

	analyzer := process.NewTempAnalyzer()
	in1 := makeInputBatch(t, mp, 3)
	defer in1.Clean(mp)
	in2 := makeInputBatch(t, mp, 5)
	defer in2.Clean(mp)

	require.NoError(t, h.op.insert_table(proc, analyzer, h.updateCtx, in1, h.insertBuf))
	firstRef := h.info.refBatch
	require.NotNil(t, firstRef)

	require.NoError(t, h.op.insert_table(proc, analyzer, h.updateCtx, in2, h.insertBuf))
	require.Same(t, firstRef, h.info.refBatch,
		"second insert_table call must reuse the same refBatch struct")

	// And the borrowed vectors should now point at in2's vectors.
	for i, inputIdx := range insertCols {
		require.Same(t, in2.Vecs[inputIdx], h.info.refBatch.Vecs[i])
	}
	require.Equal(t, in2.RowCount(), h.info.refBatch.RowCount())
}

func TestInsertTable_NonContiguous_CopiesVectors(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, false)
	defer ctrl.Finish()
	mp := proc.GetMPool()

	insertCols := []int{0, 2, 3}
	colTypes := []types.Type{
		types.T_int64.ToType(),
		types.T_int32.ToType(),
		types.T_int32.ToType(),
	}
	attrs := []string{"a", "c", "d"}
	h := newInsertTableHarness(t, ctrl, mp, insertCols, colTypes, attrs)
	defer h.cleanup(mp)
	require.False(t, h.info.isContiguous, "InsertCols=[0,2,3] must be non-contiguous")

	input := makeInputBatch(t, mp, 4)
	defer input.Clean(mp)

	analyzer := process.NewTempAnalyzer()
	require.NoError(t, h.op.insert_table(proc, analyzer, h.updateCtx, input, h.insertBuf))

	require.Len(t, h.written, 1)
	written := h.written[0]
	require.Same(t, h.insertBuf, written,
		"non-contiguous path writes via insertBuf, never via refBatch")
	require.Nil(t, h.info.refBatch, "non-contiguous path must not allocate refBatch")
	require.Equal(t, input.RowCount(), written.RowCount())

	// Copied vectors are owned by insertBuf — distinct from input vectors.
	for i, inputIdx := range insertCols {
		require.NotSame(t, input.Vecs[inputIdx], written.Vecs[i],
			"non-contiguous path must copy, not borrow, slot %d", i)
	}
}

func TestInsertTable_PathEquivalence(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, false)
	defer ctrl.Finish()
	mp := proc.GetMPool()

	// Use a contiguous mapping; we'll force the non-contiguous path by
	// flipping the flag, so both runs handle the *same* logical projection.
	insertCols := []int{0, 1, 2, 3}
	colTypes := []types.Type{
		types.T_int64.ToType(),
		types.T_int32.ToType(),
		types.T_int32.ToType(),
		types.T_int32.ToType(),
	}
	attrs := []string{"a", "b", "c", "d"}

	hContig := newInsertTableHarness(t, ctrl, mp, insertCols, colTypes, attrs)
	defer hContig.cleanup(mp)
	require.True(t, hContig.info.isContiguous)

	hCopy := newInsertTableHarness(t, ctrl, mp, insertCols, colTypes, attrs)
	defer hCopy.cleanup(mp)
	hCopy.info.isContiguous = false // force the slow path

	input := makeInputBatch(t, mp, 6)
	defer input.Clean(mp)

	analyzer := process.NewTempAnalyzer()
	require.NoError(t, hContig.op.insert_table(proc, analyzer, hContig.updateCtx, input, hContig.insertBuf))
	require.NoError(t, hCopy.op.insert_table(proc, analyzer, hCopy.updateCtx, input, hCopy.insertBuf))

	require.Len(t, hContig.written, 1)
	require.Len(t, hCopy.written, 1)
	a, b := hContig.written[0], hCopy.written[0]
	require.Equal(t, a.RowCount(), b.RowCount())
	require.Equal(t, len(a.Vecs), len(b.Vecs))
	for i := range a.Vecs {
		require.Equal(t, a.Vecs[i].String(), b.Vecs[i].String(),
			"vector %d (%s) must be identical between paths", i, attrs[i])
	}
}

func TestMultiUpdate_Free_RefBatchNoDoubleFree(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, false)
	defer ctrl.Finish()
	mp := proc.GetMPool()

	insertCols := []int{1, 2, 3}
	colTypes := []types.Type{
		types.T_int32.ToType(),
		types.T_int32.ToType(),
		types.T_int32.ToType(),
	}
	attrs := []string{"b", "c", "d"}
	h := newInsertTableHarness(t, ctrl, mp, insertCols, colTypes, attrs)

	input := makeInputBatch(t, mp, 4)
	analyzer := process.NewTempAnalyzer()
	require.NoError(t, h.op.insert_table(proc, analyzer, h.updateCtx, input, h.insertBuf))
	require.NotNil(t, h.info.refBatch)

	// Free should drop updateCtxInfos without touching the borrowed vectors;
	// input still owns them and must remain usable.
	h.op.Free(proc, false, nil)
	require.Nil(t, h.op.ctr.updateCtxInfos)

	for i, inputIdx := range insertCols {
		v := input.Vecs[inputIdx]
		require.NotNil(t, v, "input.Vecs[%d] must survive operator Free", inputIdx)
		require.Equal(t, input.RowCount(), v.Length(),
			"slot %d data still readable after Free", i)
	}

	// Cleanup leaves no mpool drift.
	input.Clean(mp)
	h.insertBuf.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

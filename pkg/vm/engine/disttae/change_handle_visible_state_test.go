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

package disttae

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type scriptedReader struct {
	batches    []*batch.Batch
	idx        int
	readErr    error
	closeCount int
}

func (r *scriptedReader) Close() error {
	r.closeCount++
	return nil
}

func (r *scriptedReader) Read(_ context.Context, _ []string, _ *pbplan.Expr, mp *mpool.MPool, dst *batch.Batch) (bool, error) {
	if r.readErr != nil {
		err := r.readErr
		r.readErr = nil
		return false, err
	}
	if r.idx >= len(r.batches) {
		return true, nil
	}
	src := r.batches[r.idx]
	r.idx++
	for row := 0; row < src.RowCount(); row++ {
		if err := dst.UnionOne(src, int64(row), mp); err != nil {
			return false, err
		}
	}
	dst.SetRowCount(src.RowCount())
	return false, nil
}

func (r *scriptedReader) SetOrderBy([]*plan.OrderBySpec) {}

func (r *scriptedReader) GetOrderBy() []*plan.OrderBySpec { return nil }

func (r *scriptedReader) SetIndexParam(*pbplan.IndexReaderParam) {}

func (r *scriptedReader) SetFilterZM(objectio.ZoneMap) {}

func TestNewVisibleStateChangesHandle(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	beforeRel := mock_frontend.NewMockRelation(ctrl)
	afterRel := mock_frontend.NewMockRelation(ctrl)
	tbl := &txnTable{
		tableId:  42,
		db:       &txnDatabase{op: txnOp},
		eng:      eng,
		tableDef: makeVisibleStateTableDef(true, true),
	}
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)
	beforeRowID := types.BuildTestRowid(1, 1)
	afterRowID := types.BuildTestRowid(2, 1)
	before := makeVisibleStateBatch(t, mp, []types.Rowid{beforeRowID}, [][2]int32{{1, 10}})
	after := makeVisibleStateBatch(t, mp, []types.Rowid{afterRowID}, [][2]int32{{1, 11}})
	t.Cleanup(func() {
		before.Clean(mp)
		after.Clean(mp)
	})
	beforeReader := &scriptedReader{batches: []*batch.Batch{before}}
	afterReader := &scriptedReader{batches: []*batch.Batch{after}}

	gomock.InOrder(
		txnOp.EXPECT().CloneSnapshotOp(start.Prev().ToTimestamp()).Return(txnOp),
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(42)).Return("", "", beforeRel, nil),
		beforeRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil),
		beforeRel.EXPECT().BuildReaders(
			gomock.Any(), gomock.Any(), nil, gomock.Any(), 1, 0, false,
			gomock.Eq(engine.TombstoneApplyPolicy(engine.Policy_CheckCommittedOnly)), engine.FilterHint{},
		).Return([]engine.Reader{beforeReader}, nil),
		txnOp.EXPECT().CloneSnapshotOp(end.ToTimestamp()).Return(txnOp),
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(42)).Return("", "", afterRel, nil),
		afterRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil),
		afterRel.EXPECT().BuildReaders(
			gomock.Any(), gomock.Any(), nil, gomock.Any(), 1, 0, false,
			gomock.Eq(engine.TombstoneApplyPolicy(engine.Policy_CheckCommittedOnly)), engine.FilterHint{},
		).Return([]engine.Reader{afterReader}, nil),
	)

	ctx := engine.WithRetainRowID(context.Background(), true)
	h, err := NewVisibleStateChangesHandle(ctx, tbl, start, end, false, 16, mp)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
		require.Equal(t, 1, afterReader.closeCount)
	})
	require.Equal(t, 1, beforeReader.closeCount)
	require.Equal(t, []string{catalog.Row_ID, "id", "v"}, h.scanAttrs)
	require.Equal(t, []string{"id", "v"}, h.dataAttrs)

	data, tombstone, hint, err := h.Next(ctx, mp)
	require.NoError(t, err)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.Equal(t, []types.Rowid{afterRowID}, vector.MustFixedColWithTypeCheck[types.Rowid](data.Vecs[0]))
	require.Equal(t, []types.Rowid{beforeRowID}, vector.MustFixedColWithTypeCheck[types.Rowid](tombstone.Vecs[0]))
	data.Clean(mp)
	tombstone.Clean(mp)
}

func TestNewVisibleStateChangesHandleRejectsInvalidInputs(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	validEnd := types.BuildTS(10, 0)
	_, err := NewVisibleStateChangesHandle(context.Background(), nil, types.TS{}, types.TS{}, false, 1, mp)
	require.ErrorContains(t, err, "invalid timestamp")

	_, err = NewVisibleStateChangesHandle(context.Background(), nil, types.BuildTS(11, 0), validEnd, false, 1, mp)
	require.ErrorContains(t, err, "invalid timestamp")

	_, err = NewVisibleStateChangesHandle(context.Background(), nil, types.TS{}, validEnd, false, 1, nil)
	require.ErrorContains(t, err, "non-nil mpool")

	_, err = NewVisibleStateChangesHandle(context.Background(), nil, types.TS{}, validEnd, false, 1, mp)
	require.ErrorContains(t, err, "requires a table")

	missingPK := &txnTable{tableDef: makeVisibleStateTableDef(false, true)}
	_, err = NewVisibleStateChangesHandle(context.Background(), missingPK, types.TS{}, validEnd, false, 1, mp)
	require.ErrorContains(t, err, "primary key column not found")

	missingRowID := &txnTable{tableDef: makeVisibleStateTableDef(true, false)}
	_, err = NewVisibleStateChangesHandle(
		engine.WithRetainRowID(context.Background(), true),
		missingRowID,
		types.TS{},
		validEnd,
		false,
		1,
		mp,
	)
	require.ErrorContains(t, err, "rowid column not found")
}

func TestVisibleStateChangesHandleNextChunksAndSkipsDeletes(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	h := &VisibleStateChangesHandle{
		end:           types.BuildTS(30, 0),
		skipDeletes:   true,
		coarseMaxRow:  1,
		mp:            mp,
		scanAttrs:     []string{"id", "v"},
		scanTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		dataScanIdxes: []int{0, 1},
		dataAttrs:     []string{"id", "v"},
		dataTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkScanIdx:     0,
		pkType:        types.T_int32.ToType(),
		beforeRows:    make(map[string]visibleStateRow),
	}
	before := makeInt32Batch(t, mp, [][2]int32{{1, 10}, {2, 20}})
	after := makeInt32Batch(t, mp, [][2]int32{{1, 11}, {3, 30}})
	t.Cleanup(func() {
		before.Clean(mp)
		after.Clean(mp)
	})
	for row := 0; row < before.RowCount(); row++ {
		pkBytes, rowBytes := h.encodeSnapshotRow(before, row)
		h.beforeRows[string(pkBytes)] = visibleStateRow{pk: pkBytes, row: rowBytes}
	}
	h.afterReaders = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	data, tombstone, _, err := h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[0]))
	require.Nil(t, tombstone)
	data.Clean(mp)

	data, tombstone, _, err = h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, []int32{3}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[0]))
	require.Nil(t, tombstone)
	data.Clean(mp)

	data, tombstone, _, err = h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Nil(t, data)
	require.Nil(t, tombstone)
}

func TestVisibleStateChangesHandleNextReaderError(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	wantErr := errors.New("snapshot read failed")
	h := &VisibleStateChangesHandle{
		end:           types.BuildTS(40, 0),
		coarseMaxRow:  1,
		mp:            mp,
		scanAttrs:     []string{"id"},
		scanTypes:     []types.Type{types.T_int32.ToType()},
		dataScanIdxes: []int{0},
		dataAttrs:     []string{"id"},
		dataTypes:     []types.Type{types.T_int32.ToType()},
		pkScanIdx:     0,
		pkType:        types.T_int32.ToType(),
		beforeRows:    make(map[string]visibleStateRow),
		afterReaders:  []engine.Reader{&scriptedReader{readErr: wantErr}},
	}
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	data, tombstone, _, err := h.Next(context.Background(), mp)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, data)
	require.Nil(t, tombstone)
}

func TestVisibleStateChangesHandleBuildSnapshotReadersErrors(t *testing.T) {
	at := types.BuildTS(50, 0)
	wantErr := errors.New("snapshot unavailable")

	t.Run("relation lookup", func(t *testing.T) {
		tbl, txnOp, eng := newVisibleStateMockTable(t)
		txnOp.EXPECT().CloneSnapshotOp(at.ToTimestamp()).Return(txnOp)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, tbl.tableId).Return("", "", nil, wantErr)

		h := &VisibleStateChangesHandle{tbl: tbl}
		readers, err := h.buildSnapshotReaders(context.Background(), at)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, readers)
	})

	t.Run("nil relation", func(t *testing.T) {
		tbl, txnOp, eng := newVisibleStateMockTable(t)
		txnOp.EXPECT().CloneSnapshotOp(at.ToTimestamp()).Return(txnOp)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, tbl.tableId).Return("", "", nil, nil)

		h := &VisibleStateChangesHandle{tbl: tbl}
		readers, err := h.buildSnapshotReaders(context.Background(), at)
		require.ErrorContains(t, err, "resolved to nil")
		require.Nil(t, readers)
	})

	t.Run("ranges", func(t *testing.T) {
		tbl, txnOp, eng := newVisibleStateMockTable(t)
		rel := mock_frontend.NewMockRelation(gomock.NewController(t))
		txnOp.EXPECT().CloneSnapshotOp(at.ToTimestamp()).Return(txnOp)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, tbl.tableId).Return("", "", rel, nil)
		rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, wantErr)

		h := &VisibleStateChangesHandle{tbl: tbl}
		readers, err := h.buildSnapshotReaders(context.Background(), at)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, readers)
	})

	t.Run("partial readers", func(t *testing.T) {
		tbl, txnOp, eng := newVisibleStateMockTable(t)
		rel := mock_frontend.NewMockRelation(gomock.NewController(t))
		partial := &scriptedReader{}
		txnOp.EXPECT().CloneSnapshotOp(at.ToTimestamp()).Return(txnOp)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, tbl.tableId).Return("", "", rel, nil)
		rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil)
		rel.EXPECT().BuildReaders(
			gomock.Any(), gomock.Any(), nil, gomock.Any(), 1, 0, false,
			gomock.Eq(engine.TombstoneApplyPolicy(engine.Policy_CheckCommittedOnly)), engine.FilterHint{},
		).Return([]engine.Reader{partial}, wantErr)

		h := &VisibleStateChangesHandle{tbl: tbl}
		readers, err := h.buildSnapshotReaders(context.Background(), at)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, readers)
		require.Equal(t, 1, partial.closeCount)
	})
}

func TestVisibleStateChangesHandleRejectsInvalidRowIDs(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	t.Run("insert has null rowid", func(t *testing.T) {
		h := newRetainedRowIDChangesHandle(mp)
		after := makeNullableRowIDBatch(t, mp, true)
		t.Cleanup(func() {
			after.Clean(mp)
		})
		h.afterReaders = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}
		t.Cleanup(func() {
			require.NoError(t, h.Close())
		})

		data, tombstone, _, err := h.Next(context.Background(), mp)
		require.ErrorContains(t, err, "null rowid")
		require.Nil(t, data)
		require.Nil(t, tombstone)
	})

	t.Run("delete has invalid rowid", func(t *testing.T) {
		h := newRetainedRowIDChangesHandle(mp)
		pk := types.EncodeValue(int32(1), types.T_int32)
		h.beforeRows[string(pk)] = visibleStateRow{pk: pk, rowID: []byte{1}}
		t.Cleanup(func() {
			require.NoError(t, h.Close())
		})

		data, tombstone, _, err := h.Next(context.Background(), mp)
		require.ErrorContains(t, err, "invalid rowid")
		require.Nil(t, data)
		require.Nil(t, tombstone)
	})
}

func TestVisibleStateChangesHandleNext(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	h := &VisibleStateChangesHandle{
		end:           types.BuildTS(10, 0),
		coarseMaxRow:  16,
		mp:            mp,
		scanAttrs:     []string{"a", "b"},
		scanTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		dataScanIdxes: []int{0, 1},
		dataAttrs:     []string{"a", "b"},
		dataTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkScanIdx:     0,
		pkType:        types.T_int32.ToType(),
		beforeRows:    make(map[string]visibleStateRow),
	}

	before := makeInt32Batch(t, mp, [][2]int32{
		{1, 10},
		{2, 20},
	})
	defer before.Clean(mp)
	for row := 0; row < before.RowCount(); row++ {
		pkBytes, rowBytes := h.encodeSnapshotRow(before, row)
		h.beforeRows[string(pkBytes)] = visibleStateRow{pk: pkBytes, row: rowBytes}
	}

	after := makeInt32Batch(t, mp, [][2]int32{
		{1, 11},
		{3, 30},
	})
	defer after.Clean(mp)
	h.afterReaders = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}

	data, tombstone, hint, err := h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.NotNil(t, data)
	require.NotNil(t, tombstone)
	defer data.Clean(mp)
	defer tombstone.Clean(mp)

	require.Equal(t, []int32{1, 3}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[0]))
	require.Equal(t, []int32{11, 30}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[1]))
	require.Equal(t, []int32{1, 2}, vector.MustFixedColWithTypeCheck[int32](tombstone.Vecs[0]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](data.Vecs[2]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](tombstone.Vecs[1]))

	data, tombstone, hint, err = h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.Nil(t, data)
	require.Nil(t, tombstone)
}

func TestVisibleStateChangesHandleIgnoresCompactionRowIDRewrite(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	h := &VisibleStateChangesHandle{
		end:           types.BuildTS(20, 0),
		coarseMaxRow:  16,
		mp:            mp,
		scanAttrs:     []string{"__mo_rowid", "id", "v"},
		scanTypes:     []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType()},
		dataScanIdxes: []int{1, 2},
		dataAttrs:     []string{"id", "v"},
		dataTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkScanIdx:     1,
		pkType:        types.T_int32.ToType(),
		rowIDScanIdx:  0,
		rowIDType:     types.T_Rowid.ToType(),
		retainRowID:   true,
		beforeRows:    make(map[string]visibleStateRow),
	}

	beforeRowIDs := []types.Rowid{
		types.BuildTestRowid(1, 1),
		types.BuildTestRowid(1, 2),
		types.BuildTestRowid(1, 4),
	}
	before := makeVisibleStateBatch(t, mp, beforeRowIDs, [][2]int32{
		{1, 10},
		{2, 20},
		{4, 40},
	})
	defer before.Clean(mp)
	for row := 0; row < before.RowCount(); row++ {
		pkBytes, rowBytes := h.encodeSnapshotRow(before, row)
		h.beforeRows[string(pkBytes)] = visibleStateRow{
			pk:    pkBytes,
			rowID: h.encodeValue(before.Vecs[h.rowIDScanIdx], row),
			row:   rowBytes,
		}
	}

	afterRowIDs := []types.Rowid{
		types.BuildTestRowid(2, 1),
		types.BuildTestRowid(2, 2),
		types.BuildTestRowid(2, 3),
	}
	after := makeVisibleStateBatch(t, mp, afterRowIDs, [][2]int32{
		{1, 10}, // unchanged payload, rewritten physical rowid
		{2, 21}, // update
		{3, 30}, // insert
	})
	defer after.Clean(mp)
	h.afterReaders = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}

	data, tombstone, hint, err := h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.NotNil(t, data)
	require.NotNil(t, tombstone)
	defer data.Clean(mp)
	defer tombstone.Clean(mp)

	require.Equal(t, []string{"__mo_rowid", "id", "v", objectio.DefaultCommitTS_Attr}, data.Attrs)
	require.Equal(t, afterRowIDs[1:], vector.MustFixedColWithTypeCheck[types.Rowid](data.Vecs[0]))
	require.Equal(t, []int32{2, 3}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[1]))
	require.Equal(t, []int32{21, 30}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[2]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](data.Vecs[3]))

	require.Equal(t, []string{"__mo_rowid", objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}, tombstone.Attrs)
	require.Equal(t, []types.Rowid{beforeRowIDs[1], beforeRowIDs[2]}, vector.MustFixedColWithTypeCheck[types.Rowid](tombstone.Vecs[0]))
	require.Equal(t, []int32{2, 4}, vector.MustFixedColWithTypeCheck[int32](tombstone.Vecs[1]))
	require.Equal(t, []types.TS{h.end, h.end}, vector.MustFixedColWithTypeCheck[types.TS](tombstone.Vecs[2]))
}

func TestVisibleStateChangesHandleCloseNilReceiver(t *testing.T) {
	var h *VisibleStateChangesHandle
	require.NotPanics(t, func() {
		_ = h.Close()
	})
}

func TestVisibleStateChangesHandleCloseNilMPoolAndTypedNilReader(t *testing.T) {
	var nilReader *scriptedReader
	h := &VisibleStateChangesHandle{
		mp:             nil,
		beforeRows:     map[string]visibleStateRow{"before": {}},
		pendingDeletes: []visibleStateRow{{pk: []byte("pending")}},
		currentAfter: func() *batch.Batch {
			bat := batch.NewWithSize(1)
			bat.SetAttributes([]string{"a"})
			bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			bat.SetRowCount(0)
			return bat
		}(),
		afterReaders: []engine.Reader{
			nilReader,
		},
	}
	require.NotPanics(t, func() {
		_ = h.Close()
	})
	require.Nil(t, h.currentAfter)
	require.Nil(t, h.beforeRows)
	require.Nil(t, h.pendingDeletes)
}

func makeInt32Batch(t *testing.T, mp *mpool.MPool, rows [][2]int32) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{"a", "b"})
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row[0], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], row[1], false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}

func makeVisibleStateBatch(
	t *testing.T,
	mp *mpool.MPool,
	rowIDs []types.Rowid,
	rows [][2]int32,
) *batch.Batch {
	t.Helper()
	require.Len(t, rowIDs, len(rows))

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"__mo_rowid", "id", "v"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	for i, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowIDs[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], row[0], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], row[1], false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}

func makeVisibleStateTableDef(includePK, includeRowID bool) *pbplan.TableDef {
	cols := make([]*pbplan.ColDef, 0, 3)
	if includeRowID {
		cols = append(cols, &pbplan.ColDef{
			Name: catalog.Row_ID,
			Typ:  pbplan.Type{Id: int32(types.T_Rowid)},
		})
	}
	cols = append(cols,
		&pbplan.ColDef{Name: "id", Typ: pbplan.Type{Id: int32(types.T_int32)}},
		&pbplan.ColDef{Name: "v", Typ: pbplan.Type{Id: int32(types.T_int32)}},
	)
	tblDef := &pbplan.TableDef{Cols: cols}
	if includePK {
		tblDef.Pkey = &pbplan.PrimaryKeyDef{PkeyColName: "id"}
	} else {
		tblDef.Pkey = &pbplan.PrimaryKeyDef{PkeyColName: "missing"}
	}
	return tblDef
}

func newVisibleStateMockTable(
	t *testing.T,
) (*txnTable, *mock_frontend.MockTxnOperator, *mock_frontend.MockEngine) {
	t.Helper()
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	tbl := &txnTable{
		tableId: 42,
		db:      &txnDatabase{op: txnOp},
		eng:     eng,
	}
	return tbl, txnOp, eng
}

func newRetainedRowIDChangesHandle(mp *mpool.MPool) *VisibleStateChangesHandle {
	return &VisibleStateChangesHandle{
		end:           types.BuildTS(60, 0),
		coarseMaxRow:  16,
		mp:            mp,
		scanAttrs:     []string{catalog.Row_ID, "id", "v"},
		scanTypes:     []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType()},
		dataScanIdxes: []int{1, 2},
		dataAttrs:     []string{"id", "v"},
		dataTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkScanIdx:     1,
		pkType:        types.T_int32.ToType(),
		rowIDScanIdx:  0,
		rowIDType:     types.T_Rowid.ToType(),
		retainRowID:   true,
		beforeRows:    make(map[string]visibleStateRow),
	}
}

func makeNullableRowIDBatch(t *testing.T, mp *mpool.MPool, rowIDIsNull bool) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{catalog.Row_ID, "id", "v"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(1, 1), rowIDIsNull, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], int32(10), false, mp))
	bat.SetRowCount(1)
	return bat
}

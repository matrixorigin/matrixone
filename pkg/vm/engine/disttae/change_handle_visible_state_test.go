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
	"sort"
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

type testVisibleStateStore struct {
	rows     map[string][]byte
	putErr   error
	closeErr error
	closed   bool
}

func newTestVisibleStateStore() *testVisibleStateStore {
	return &testVisibleStateStore{rows: make(map[string][]byte)}
}

func (s *testVisibleStateStore) PutBatch(entries []engine.VisibleStateEntry) error {
	if s.putErr != nil {
		return s.putErr
	}
	for _, entry := range entries {
		s.rows[string(entry.Key)] = append([]byte(nil), entry.Value...)
	}
	return nil
}

func (s *testVisibleStateStore) Pop(key []byte) ([]byte, bool, error) {
	value, ok := s.rows[string(key)]
	if !ok {
		return nil, false, nil
	}
	delete(s.rows, string(key))
	return append([]byte(nil), value...), true, nil
}

func (s *testVisibleStateStore) Drain(max int, fn func(key, value []byte) error) (int, error) {
	keys := make([]string, 0, len(s.rows))
	for key := range s.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > max {
		keys = keys[:max]
	}
	for _, key := range keys {
		value := s.rows[key]
		delete(s.rows, key)
		if err := fn([]byte(key), value); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}

func (s *testVisibleStateStore) Len() int64 { return int64(len(s.rows)) }

func (s *testVisibleStateStore) Close() error {
	s.closed = true
	s.rows = nil
	return s.closeErr
}

type testVisibleStateResources struct {
	store      *testVisibleStateStore
	reserveErr error
	failAt     int
	reserveCnt int
	reserved   int64
}

func newTestVisibleStateResources() *testVisibleStateResources {
	return &testVisibleStateResources{store: newTestVisibleStateStore()}
}

func (r *testVisibleStateResources) NewVisibleStateStore() (engine.VisibleStateStore, error) {
	return r.store, nil
}

func (r *testVisibleStateResources) ReserveBuffer(bytes int64) error {
	r.reserveCnt++
	if r.reserveErr != nil && (r.failAt == 0 || r.reserveCnt == r.failAt) {
		return r.reserveErr
	}
	r.reserved += bytes
	return nil
}

func (r *testVisibleStateResources) ReleaseBuffer(bytes int64) { r.reserved -= bytes }

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
	tableDef := makeVisibleStateTableDef(true, true)
	resources := newTestVisibleStateResources()

	gomock.InOrder(
		txnOp.EXPECT().CloneSnapshotOp(end.ToTimestamp()).Return(txnOp),
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(42)).Return("", "", afterRel, nil),
		beforeRel.EXPECT().GetTableDef(gomock.Any()).Return(tableDef),
		afterRel.EXPECT().GetTableDef(gomock.Any()).Return(tableDef),
		beforeRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil),
		beforeRel.EXPECT().BuildReaders(
			gomock.Any(), gomock.Any(), nil, gomock.Any(), 1, 0, false,
			gomock.Eq(engine.TombstoneApplyPolicy(engine.Policy_CheckCommittedOnly)), engine.FilterHint{},
		).Return([]engine.Reader{beforeReader}, nil),
		afterRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil),
		afterRel.EXPECT().BuildReaders(
			gomock.Any(), gomock.Any(), nil, gomock.Any(), 1, 0, false,
			gomock.Eq(engine.TombstoneApplyPolicy(engine.Policy_CheckCommittedOnly)), engine.FilterHint{},
		).Return([]engine.Reader{afterReader}, nil),
	)

	ctx := engine.WithRetainRowID(context.Background(), true)
	h, err := NewVisibleStateChangesHandle(ctx, tbl, start, end, false, 16, mp, resources, beforeRel)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
		require.Equal(t, 1, afterReader.closeCount)
	})
	require.Equal(t, 1, beforeReader.closeCount)
	require.Equal(t, []string{catalog.Row_ID, "id", "v"}, h.beforeScan.attrs)
	require.Equal(t, []string{catalog.Row_ID, "id", "v"}, h.afterScan.attrs)
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
	resources := newTestVisibleStateResources()
	_, err := NewVisibleStateChangesHandle(context.Background(), nil, types.TS{}, types.TS{}, false, 1, mp, resources, nil)
	require.ErrorContains(t, err, "invalid timestamp")

	_, err = NewVisibleStateChangesHandle(context.Background(), nil, types.BuildTS(11, 0), validEnd, false, 1, mp, resources, nil)
	require.ErrorContains(t, err, "invalid timestamp")

	_, err = NewVisibleStateChangesHandle(context.Background(), nil, types.TS{}, validEnd, false, 1, nil, resources, nil)
	require.ErrorContains(t, err, "requires a table")

	_, err = NewVisibleStateChangesHandle(context.Background(), &txnTable{}, types.TS{}, validEnd, false, 1, mp, nil, nil)
	require.ErrorContains(t, err, "bounded recovery resources")

	h := &VisibleStateChangesHandle{
		beforeScan: visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
		afterScan:  visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
	}
	err = h.initSchema(nil, makeVisibleStateTableDef(false, true))
	require.ErrorContains(t, err, "primary key column not found")

	h = &VisibleStateChangesHandle{
		retainRowID: true,
		beforeScan:  visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
		afterScan:   visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
	}
	err = h.initSchema(nil, makeVisibleStateTableDef(true, false))
	require.ErrorContains(t, err, "rowid column not found")
}

func TestVisibleStateChangesHandleNextChunksAndSkipsDeletes(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	h := newInt32VisibleStateHandle(mp, true, 1)
	before := makeInt32Batch(t, mp, [][2]int32{{1, 10}, {2, 20}})
	after := makeInt32Batch(t, mp, [][2]int32{{1, 11}, {3, 30}})
	t.Cleanup(func() {
		before.Clean(mp)
		after.Clean(mp)
	})
	putVisibleStateBatch(t, h, before, &h.beforeScan)
	h.afterScan.readers = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}
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
	h := newInt32VisibleStateHandle(mp, false, 1)
	h.afterScan.readers = []engine.Reader{&scriptedReader{readErr: wantErr}}
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
		rel, err := h.getRelationAt(context.Background(), at)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, rel)
	})

	t.Run("nil relation", func(t *testing.T) {
		tbl, txnOp, eng := newVisibleStateMockTable(t)
		txnOp.EXPECT().CloneSnapshotOp(at.ToTimestamp()).Return(txnOp)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, tbl.tableId).Return("", "", nil, nil)

		h := &VisibleStateChangesHandle{tbl: tbl}
		got, err := h.getRelationAt(context.Background(), at)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("ranges", func(t *testing.T) {
		tbl, _, _ := newVisibleStateMockTable(t)
		rel := mock_frontend.NewMockRelation(gomock.NewController(t))
		rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, wantErr)

		h := &VisibleStateChangesHandle{tbl: tbl}
		readers, err := h.buildSnapshotReaders(context.Background(), rel)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, readers)
	})

	t.Run("partial readers", func(t *testing.T) {
		tbl, _, _ := newVisibleStateMockTable(t)
		rel := mock_frontend.NewMockRelation(gomock.NewController(t))
		partial := &scriptedReader{}
		rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil)
		rel.EXPECT().BuildReaders(
			gomock.Any(), gomock.Any(), nil, gomock.Any(), 1, 0, false,
			gomock.Eq(engine.TombstoneApplyPolicy(engine.Policy_CheckCommittedOnly)), engine.FilterHint{},
		).Return([]engine.Reader{partial}, wantErr)

		h := &VisibleStateChangesHandle{tbl: tbl}
		readers, err := h.buildSnapshotReaders(context.Background(), rel)
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
		h.afterScan.readers = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}
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
		require.NoError(t, h.beforeRows.PutBatch([]engine.VisibleStateEntry{{
			Key: pk, Value: encodeVisibleStateRow([]byte{1}, nil),
		}}))
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

	h := newInt32VisibleStateHandle(mp, false, 16)

	before := makeInt32Batch(t, mp, [][2]int32{
		{1, 10},
		{2, 20},
	})
	defer before.Clean(mp)
	putVisibleStateBatch(t, h, before, &h.beforeScan)

	after := makeInt32Batch(t, mp, [][2]int32{
		{1, 11},
		{3, 30},
	})
	defer after.Clean(mp)
	h.afterScan.readers = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}

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

	h := newRetainedRowIDChangesHandle(mp)
	h.end = types.BuildTS(20, 0)

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
	putVisibleStateBatch(t, h, before, &h.beforeScan)

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
	h.afterScan.readers = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}

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

func TestVisibleStateChangesHandleProjectsHistoricalSchema(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })

	beforeDef := makeVisibleStateTableDef(true, true)
	afterDef := makeVisibleStateTableDef(true, true)
	afterDef.Cols = append(afterDef.Cols[:2], append([]*pbplan.ColDef{{
		Name: "branch_only", Typ: pbplan.Type{Id: int32(types.T_int32)}, Seqnum: 2,
	}}, afterDef.Cols[2:]...)...)

	h := &VisibleStateChangesHandle{
		end: types.BuildTS(70, 0), coarseMaxRow: 16, mp: mp,
		beforeScan: visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
		afterScan:  visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
		beforeRows: newTestVisibleStateStore(),
	}
	require.NoError(t, h.initSchema(beforeDef, afterDef))
	require.Equal(t, []string{catalog.Row_ID, "id", "v"}, h.beforeScan.attrs)
	require.Equal(t, []int{1, 2}, h.beforeScan.compareIdxes)
	require.Equal(t, []int{1, 3}, h.afterScan.compareIdxes)

	before := makeVisibleStateBatch(t, mp,
		[]types.Rowid{types.BuildTestRowid(1, 1), types.BuildTestRowid(1, 2)},
		[][2]int32{{1, 10}, {2, 20}},
	)
	after := makeSchemaEvolutionBatch(t, mp, [][3]int32{{1, 7, 10}, {2, 7, 21}})
	t.Cleanup(func() {
		before.Clean(mp)
		after.Clean(mp)
	})
	putVisibleStateBatch(t, h, before, &h.beforeScan)
	h.afterScan.readers = []engine.Reader{&scriptedReader{batches: []*batch.Batch{after}}}
	t.Cleanup(func() { require.NoError(t, h.Close()) })

	data, tombstone, _, err := h.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "branch_only", "v", objectio.DefaultCommitTS_Attr}, data.Attrs)
	require.Equal(t, []int32{2}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[0]))
	require.Equal(t, []int32{7}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[1]))
	require.Equal(t, []int32{21}, vector.MustFixedColWithTypeCheck[int32](data.Vecs[2]))
	require.Equal(t, []int32{2}, vector.MustFixedColWithTypeCheck[int32](tombstone.Vecs[0]))
	data.Clean(mp)
	tombstone.Clean(mp)
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
		mp:         nil,
		beforeRows: newTestVisibleStateStore(),
		currentAfter: func() *batch.Batch {
			bat := batch.NewWithSize(1)
			bat.SetAttributes([]string{"a"})
			bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			bat.SetRowCount(0)
			return bat
		}(),
		afterScan: visibleStateSnapshotScan{readers: []engine.Reader{nilReader}},
	}
	require.NotPanics(t, func() {
		_ = h.Close()
	})
	require.Nil(t, h.currentAfter)
	require.Nil(t, h.beforeRows)
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

func makeSchemaEvolutionBatch(t *testing.T, mp *mpool.MPool, rows [][3]int32) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(4)
	bat.SetAttributes([]string{catalog.Row_ID, "id", "branch_only", "v"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_int32.ToType())
	for i, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(2, int64(i+1)), false, mp))
		for col := range row {
			require.NoError(t, vector.AppendFixed(bat.Vecs[col+1], row[col], false, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func makeVisibleStateTableDef(includePK, includeRowID bool) *pbplan.TableDef {
	cols := make([]*pbplan.ColDef, 0, 3)
	if includeRowID {
		cols = append(cols, &pbplan.ColDef{
			Name:   catalog.Row_ID,
			Typ:    pbplan.Type{Id: int32(types.T_Rowid)},
			Seqnum: objectio.SEQNUM_ROWID,
		})
	}
	cols = append(cols,
		&pbplan.ColDef{Name: "id", Typ: pbplan.Type{Id: int32(types.T_int32)}, Seqnum: 0},
		&pbplan.ColDef{Name: "v", Typ: pbplan.Type{Id: int32(types.T_int32)}, Seqnum: 1},
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
		end:          types.BuildTS(60, 0),
		coarseMaxRow: 16,
		mp:           mp,
		beforeScan: visibleStateSnapshotScan{
			attrs:        []string{catalog.Row_ID, "id", "v"},
			types:        []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType()},
			compareIdxes: []int{1, 2}, pkIdx: 1, rowIDIdx: 0,
		},
		afterScan: visibleStateSnapshotScan{
			attrs:        []string{catalog.Row_ID, "id", "v"},
			types:        []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType()},
			compareIdxes: []int{1, 2}, pkIdx: 1, rowIDIdx: 0,
		},
		dataScanIdxes: []int{1, 2},
		dataAttrs:     []string{"id", "v"},
		dataTypes:     []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkType:        types.T_int32.ToType(),
		rowIDType:     types.T_Rowid.ToType(),
		retainRowID:   true,
		beforeRows:    newTestVisibleStateStore(),
	}
}

func newInt32VisibleStateHandle(mp *mpool.MPool, skipDeletes bool, maxRows uint32) *VisibleStateChangesHandle {
	scan := visibleStateSnapshotScan{
		attrs:        []string{"a", "b"},
		types:        []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		compareIdxes: []int{0, 1}, pkIdx: 0, rowIDIdx: -1,
	}
	return &VisibleStateChangesHandle{
		end: types.BuildTS(30, 0), skipDeletes: skipDeletes,
		coarseMaxRow: maxRows, mp: mp,
		beforeScan: scan, afterScan: scan,
		dataScanIdxes: []int{0, 1}, dataAttrs: []string{"a", "b"},
		dataTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		pkType:    types.T_int32.ToType(), beforeRows: newTestVisibleStateStore(),
	}
}

func putVisibleStateBatch(t *testing.T, h *VisibleStateChangesHandle, bat *batch.Batch, scan *visibleStateSnapshotScan) {
	t.Helper()
	entries := make([]engine.VisibleStateEntry, bat.RowCount())
	for row := 0; row < bat.RowCount(); row++ {
		pk, encoded := h.encodeSnapshotRow(bat, row, scan)
		var rowID []byte
		if h.retainRowID {
			rowID = h.encodeValue(bat.Vecs[scan.rowIDIdx], row)
		}
		entries[row] = engine.VisibleStateEntry{Key: pk, Value: encodeVisibleStateRow(rowID, encoded)}
	}
	require.NoError(t, h.beforeRows.PutBatch(entries))
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

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

type replayTerminalProbe struct {
	client.TxnOperator
	finish func(context.Context, bool)
}

func (p replayTerminalProbe) Rollback(ctx context.Context) error { p.finish(ctx, true); return nil }
func (p replayTerminalProbe) Commit(ctx context.Context) error   { p.finish(ctx, false); return nil }

func TestFinishReplayTransactionContext(t *testing.T) {
	type contextKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "trace"))
	cancel()
	called := false
	probe := replayTerminalProbe{finish: func(cleanup context.Context, rollback bool) {
		called = true
		require.True(t, rollback)
		require.NoError(t, cleanup.Err())
		require.Equal(t, "trace", cleanup.Value(contextKey{}))
		_, ok := cleanup.Deadline()
		require.True(t, ok)
	}}
	finishReplayTransaction(ctx, time.Second, probe, context.Canceled)
	require.True(t, called)

	called = false
	probe.finish = func(commit context.Context, rollback bool) {
		called = true
		require.False(t, rollback)
		require.ErrorIs(t, commit.Err(), context.Canceled)
	}
	finishReplayTransaction(ctx, time.Second, probe, nil)
	require.True(t, called)
}

func replayTestBatch(t *testing.T, mp *mpool.MPool, ids ...uint64) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(7)
	for i, typ := range []types.T{types.T_uint64, types.T_uint64, types.T_uint32, types.T_uint64, types.T_varchar, types.T_uint64, types.T_varchar} {
		b.Vecs[i] = vector.NewVec(typ.ToType())
	}
	for _, id := range ids {
		require.NoError(t, vector.AppendFixed(b.Vecs[0], id, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], id, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[2], uint32(1), false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[3], uint64(1), false, mp))
		require.NoError(t, vector.AppendBytes(b.Vecs[4], []byte("db"), false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[5], id, false, mp))
		require.NoError(t, vector.AppendBytes(b.Vecs[6], []byte{byte('a' + id)}, false, mp))
	}
	b.SetRowCount(len(ids))
	return b
}
func TestConsumeReplayColumns(t *testing.T) {
	t.Run("split and empty", func(t *testing.T) {
		mp := mpool.MustNewZero()
		bs := []*batch.Batch{replayTestBatch(t, mp, 1, 2), replayTestBatch(t, mp), replayTestBatch(t, mp, 2), replayTestBatch(t, mp, 2, 3, 4)}
		var groups [][]uint64
		err := consumeReplayColumns(context.Background(), bs, mp, types.TS{}, func(b *batch.Batch) {
			var got []uint64
			for i := 0; i < b.RowCount(); i++ {
				got = append(got, replayKey(b, i, 2).table)
			}
			groups = append(groups, got)
		})
		require.NoError(t, err)
		require.Equal(t, [][]uint64{{1}, {2, 2, 2}, {3, 4}}, groups)
		require.Zero(t, mp.CurrNB())
	})
	for _, position := range []int{0, 1, 2} {
		t.Run(string(rune('0'+position)), func(t *testing.T) {
			mp := mpool.MustNewZero()
			bs := make([]*batch.Batch, 2, 3)
			bs[0], bs[1] = replayTestBatch(t, mp, 1), replayTestBatch(t, mp, 2)
			bs = append(bs, nil)
			copy(bs[position+1:], bs[position:])
			bs[position] = nil
			err := consumeReplayColumns(context.Background(), bs, mp, types.TS{}, func(*batch.Batch) { t.Fatal("unexpected insert") })
			require.Error(t, err)
			require.Zero(t, mp.CurrNB())
		})
	}
	t.Run("order", func(t *testing.T) {
		mp := mpool.MustNewZero()
		err := consumeReplayColumns(context.Background(), []*batch.Batch{replayTestBatch(t, mp, 2, 1)}, mp, types.TS{}, func(*batch.Batch) { t.Fatal("unexpected insert") })
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())
	})
	t.Run("cancel", func(t *testing.T) {
		mp := mpool.MustNewZero()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := consumeReplayColumns(ctx, []*batch.Batch{replayTestBatch(t, mp, 1)}, mp, types.TS{}, func(*batch.Batch) { t.Fatal("unexpected insert") })
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, mp.CurrNB())
	})
}

// TestReplayColumnsParserOwnership exercises actual catalog tuples and parser after
// the consumer has released both borrowed windows and copied carry batches.
func TestReplayColumnsParserOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()
	typ := types.T_uint64.ToType()
	encoded, err := typ.Marshal()
	require.NoError(t, err)
	def, err := (&plan.Default{OriginString: "12345678901234567890"}).Marshal()
	require.NoError(t, err)
	update, err := (&plan.OnUpdate{OriginString: "c0 + 123456789"}).Marshal()
	require.NoError(t, err)
	generated, err := (&plan.GeneratedCol{OriginString: "c0 + c1 + 123456789"}).Marshal()
	require.NoError(t, err)
	column := func(account uint32, id uint64, name string, num int32) catalog.Column {
		return catalog.Column{AccountId: account, DatabaseId: 8, TableId: id, DatabaseName: "database_long_name", TableName: "same_table_name_long", Name: name, Num: num, Seqnum: uint16(num - 1), Typ: encoded, TypLen: int32(len(encoded)), Comment: "long column comment retained after release", ConstraintType: catalog.SystemColNoConstraint}
	}
	a, b, c := column(1, 9, "first_column_long", 1), column(1, 9, "second_column_long", 2), column(1, 9, "third_column_long", 3)
	a.HasDef, a.DefaultExpr, a.ConstraintType = 1, def, catalog.SystemColPKConstraint
	b.HasUpdate, b.UpdateExpr, b.IsClusterBy = 1, update, 1
	c.HasGenerated, c.GeneratedExpr = 1, generated
	d, e := column(1, 10, "new_version_long", 1), column(2, 9, "other_tenant_long", 1)
	makeBatch := func(cols ...catalog.Column) *batch.Batch {
		data, err := catalog.GenCreateColumnTuples(cols, mp, packer)
		require.NoError(t, err)
		out := batch.NewWithSize(len(data.Vecs) + 1)
		out.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		for range cols {
			require.NoError(t, vector.AppendFixed(out.Vecs[0], types.Rowid{}, false, mp))
		}
		copy(out.Vecs[1:], data.Vecs)
		out.SetRowCount(len(cols))
		return out
	}
	var parsed []cache.Columns
	var keys []cache.TableItemKey
	insert := func(bat *batch.Batch) {
		cache.ParseColumnsBatchAnd(bat, func(groups map[cache.TableItemKey]cache.Columns) {
			for k, cols := range groups {
				keys = append(keys, k)
				parsed = append(parsed, cols)
			}
		})
	}
	err = consumeReplayColumns(context.Background(), []*batch.Batch{makeBatch(a), makeBatch(b), makeBatch(c, d), makeBatch(e)}, mp, types.BuildTS(100, 0), insert)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
	require.Len(t, parsed, 3)
	byKey := make(map[[2]uint64]cache.Columns)
	for i, key := range keys {
		require.Equal(t, "same_table_name_long", key.Name)
		byKey[[2]uint64{uint64(key.AccountId), key.Id}] = parsed[i]
	}
	cols := byKey[[2]uint64{1, 9}]
	require.Len(t, cols, 3)
	require.Equal(t, []string{a.Name, b.Name, c.Name}, []string{cols[0].Name, cols[1].Name, cols[2].Name})
	require.Equal(t, a.Comment, cols[0].Comment)
	require.Equal(t, encoded, cols[0].Typ)
	require.Equal(t, def, cols[0].DefaultExpr)
	require.Equal(t, update, cols[1].UpdateExpr)
	require.Equal(t, generated, cols[2].GeneratedExpr)
	require.Equal(t, d.Name, byKey[[2]uint64{1, 10}][0].Name)
	require.Equal(t, e.Name, byKey[[2]uint64{2, 9}][0].Name)
	item := &cache.TableItem{AccountId: 1, DatabaseId: 8, Id: 9, Name: a.TableName, PrimaryIdx: -1, PrimarySeqnum: -1, ClusterByIdx: -1}
	cache.InitTableItemWithColumns(item, cols)
	require.Equal(t, 0, item.PrimaryIdx)
	require.Equal(t, 1, item.ClusterByIdx)
	require.Equal(t, a.Name, item.TableDef.Cols[0].Name)
	require.Equal(t, "12345678901234567890", item.TableDef.Cols[0].Default.OriginString)
	require.Equal(t, "c0 + 123456789", item.TableDef.Cols[1].OnUpdate.OriginString)
	require.Equal(t, "c0 + c1 + 123456789", item.TableDef.Cols[2].GeneratedCol.OriginString)
}

func TestReplayColumnsCancelAndRetry(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var first []uint64
	err := consumeReplayColumns(ctx, []*batch.Batch{replayTestBatch(t, mp, 1), replayTestBatch(t, mp, 2)}, mp, types.TS{}, func(b *batch.Batch) {
		first = append(first, replayKey(b, 0, 2).table)
		cancel()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{1}, first)
	require.Zero(t, mp.CurrNB())
	var retry []uint64
	err = consumeReplayColumns(context.Background(), []*batch.Batch{replayTestBatch(t, mp, 1), replayTestBatch(t, mp, 2)}, mp, types.TS{}, func(b *batch.Batch) {
		for i := 0; i < b.RowCount(); i++ {
			retry = append(retry, replayKey(b, i, 2).table)
		}
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, retry)
	require.Zero(t, mp.CurrNB())
}

func TestReplayColumnsRejectsConflictingNames(t *testing.T) {
	mp := mpool.MustNewZero()
	first, second := replayTestBatch(t, mp, 1), replayTestBatch(t, mp, 1)
	second.Vecs[6].Free(mp)
	second.Vecs[6] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(second.Vecs[6], []byte("different"), false, mp))
	err := consumeReplayColumns(context.Background(), []*batch.Batch{first, second}, mp, types.TS{}, func(*batch.Batch) { t.Fatal("invalid input was published") })
	require.Error(t, err)
	require.Zero(t, mp.CurrNB())
}

// Cancellation after a window is inserted still leaves a split table in carry.
func TestReplayColumnsCancelWithCarryAndDeadline(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var inserted []uint64
	err := consumeReplayColumns(ctx, []*batch.Batch{replayTestBatch(t, mp, 1, 2), replayTestBatch(t, mp, 2, 3)}, mp, types.TS{}, func(b *batch.Batch) {
		inserted = append(inserted, replayKey(b, 0, 2).table)
		cancel()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{1}, inserted)
	require.Zero(t, mp.CurrNB())

	deadline, stop := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer stop()
	err = consumeReplayColumns(deadline, []*batch.Batch{replayTestBatch(t, mp, 1), replayTestBatch(t, mp, 1)}, mp, types.TS{}, func(*batch.Batch) {
		t.Fatal("expired replay inserted data")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, mp.CurrNB())

	inserted = nil
	err = consumeReplayColumns(context.Background(), []*batch.Batch{replayTestBatch(t, mp, 1, 2), replayTestBatch(t, mp, 2, 3)}, mp, types.TS{}, func(b *batch.Batch) {
		for i := 0; i < b.RowCount(); i++ {
			inserted = append(inserted, replayKey(b, i, 2).table)
		}
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 2, 3}, inserted)
	require.Zero(t, mp.CurrNB())
}

// TestReplayPartialCopyFailure verifies the cleanup owners after a failure has
// already allocated data, rather than relying on a pool limit for heap vectors.
func TestReplayPartialCopyFailure(t *testing.T) {
	injected := errors.New("injected allocation failure")
	t.Run("timestamp", func(t *testing.T) {
		mp := mpool.MustNewZero()
		bs := []*batch.Batch{replayTestBatch(t, mp, 1, 1), replayTestBatch(t, mp, 2)}
		beforeHeap := mp.OnHeapCurrNB()
		calls := 0
		err := consumeReplayColumnsWithFaults(context.Background(), bs, mp, types.TS{}, func(*batch.Batch) {
			t.Fatal("incomplete timestamp was inserted")
		}, nil, func(v *vector.Vector, ts types.TS, mp *mpool.MPool) error {
			calls++
			if calls == 2 {
				require.Greater(t, mp.OnHeapCurrNB(), beforeHeap)
				return injected
			}
			return vector.AppendFixed(v, ts, false, mp)
		})
		require.ErrorIs(t, err, injected)
		require.Equal(t, 2, calls)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	t.Run("carry", func(t *testing.T) {
		mp := mpool.MustNewZero()
		bs := []*batch.Batch{replayTestBatch(t, mp, 1), replayTestBatch(t, mp, 1)}
		called := false
		err := consumeReplayColumnsWithFaults(context.Background(), bs, mp, types.TS{}, func(*batch.Batch) {
			t.Fatal("partial carry was inserted")
		}, func(dst, src *batch.Batch, offset, count int, mp *mpool.MPool) error {
			called = true
			before := mp.OnHeapCurrNB()
			require.NoError(t, dst.Vecs[0].UnionBatch(src.Vecs[0], int64(offset), count, nil, mp))
			require.Greater(t, mp.OnHeapCurrNB(), before)
			return injected
		}, nil)
		require.ErrorIs(t, err, injected)
		require.True(t, called)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
}

// Exercise the real cache across a failed column consumer and a retry at the
// same snapshot timestamp. This is a component test, not a connect-loop test.
func TestReplayPartialInsertRetryCache(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	packer := types.NewPacker()
	defer packer.Close()
	cc := cache.NewCatalog()
	ts := types.BuildTS(100, 0)
	insertTables := func() {
		for _, tbl := range []catalog.Table{
			{AccountId: 1, DatabaseId: 8, DatabaseName: "db", TableId: 9, TableName: "first"},
			{AccountId: 1, DatabaseId: 8, DatabaseName: "db", TableId: 10, TableName: "second"},
		} {
			func() {
				b, err := catalog.GenCreateTableTuple(tbl, mp, packer)
				require.NoError(t, err)
				defer b.Clean(mp)
				_, err = fillRandomRowidAndZeroTs(b, mp)
				require.NoError(t, err)
				vector.MustFixedColWithTypeCheck[types.TS](b.Vecs[cache.MO_TIMESTAMP_IDX])[0] = ts
				cc.InsertTable(b)
			}()
		}
	}
	typ := types.T_uint64.ToType()
	encoded, err := typ.Marshal()
	require.NoError(t, err)
	column := func(id uint64, table, name string, num int32) catalog.Column {
		return catalog.Column{AccountId: 1, DatabaseId: 8, DatabaseName: "db", TableId: id, TableName: table, Name: name, Num: num, Seqnum: uint16(num - 1), Typ: encoded, ConstraintType: catalog.SystemColNoConstraint}
	}
	makeBatch := func(cols ...catalog.Column) *batch.Batch {
		data, err := catalog.GenCreateColumnTuples(cols, mp, packer)
		require.NoError(t, err)
		b := batch.NewWithSize(len(data.Vecs) + 1)
		b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		copy(b.Vecs[1:], data.Vecs)
		for range cols {
			if err := vector.AppendFixed(b.Vecs[0], types.Rowid{}, false, mp); err != nil {
				b.Clean(mp)
				require.NoError(t, err)
			}
		}
		b.SetRowCount(len(cols))
		return b
	}
	first := column(9, "first", "keep", 1)
	stale := column(9, "first", "stale", 2)
	second := column(10, "second", "other", 1)
	insertTables()
	injected := errors.New("second batch failure")
	calls := 0
	err = consumeReplayColumnsWithFaults(t.Context(), []*batch.Batch{makeBatch(first, stale), makeBatch(second)}, mp, ts, cc.InsertColumns, nil,
		func(v *vector.Vector, ts types.TS, mp *mpool.MPool) error {
			calls++
			if calls == 3 {
				return injected
			}
			return vector.AppendFixed(v, ts, false, mp)
		})
	require.ErrorIs(t, err, injected)
	require.Equal(t, 3, calls)
	require.Equal(t, []string{"keep", "stale"}, []string{cc.GetTableByName(1, 8, "first").TableDef.Cols[0].Name, cc.GetTableByName(1, 8, "first").TableDef.Cols[1].Name})
	require.False(t, cc.CanServe(ts))
	require.Zero(t, mp.CurrNB())
	require.Zero(t, mp.OnHeapCurrNB())

	insertTables()
	err = consumeReplayColumns(t.Context(), []*batch.Batch{makeBatch(first), makeBatch(second)}, mp, ts, cc.InsertColumns)
	require.NoError(t, err)
	got := cc.GetTableByName(1, 8, "first")
	require.NotNil(t, got.TableDef)
	require.Len(t, got.TableDef.Cols, 1)
	require.Equal(t, "keep", got.TableDef.Cols[0].Name)
	require.Equal(t, "other", cc.GetTableByName(1, 8, "second").TableDef.Cols[0].Name)
	require.Zero(t, mp.CurrNB())
	require.Zero(t, mp.OnHeapCurrNB())
}

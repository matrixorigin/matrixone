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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

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

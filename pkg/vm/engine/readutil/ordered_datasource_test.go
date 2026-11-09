// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestUsableOrderZoneMap(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varbinary)}
	truncated := objectio.NewZM(types.T_varbinary, 0)
	index.UpdateZM(truncated, bytes.Repeat([]byte{0xff}, 31))
	require.True(t, truncated.MaxTruncated())
	require.True(t, UsableOrderZoneMap(truncated, typ, false))
	require.False(t, UsableOrderZoneMap(truncated, typ, true))
	require.False(t, UsableOrderZoneMap(nil, typ, false))
	require.False(t, UsableOrderZoneMap(truncated, plan.Type{Id: int32(types.T_int32)}, false))
	decimal := objectio.NewZM(types.T_decimal64, 2)
	index.UpdateZM(decimal, types.EncodeDecimal64(new(types.Decimal64)))
	require.True(t, UsableOrderZoneMap(decimal, plan.Type{Id: int32(types.T_decimal64), Scale: 2}, false))
	require.False(t, UsableOrderZoneMap(decimal, plan.Type{Id: int32(types.T_decimal64), Scale: 3}, false))
}

type orderedScanFailReadFS struct {
	fileservice.FileService
	err error
}

func (fs orderedScanFailReadFS) Read(context.Context, *fileservice.IOVector) error { return fs.err }

func TestRemoteDataSourceOrderedBlocks(t *testing.T) {
	ctx := t.Context()
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	fs := testutil.NewSharedFS()
	t.Cleanup(func() { fs.Close(ctx) })
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	// Eight rows suffice: a late winner, an equal boundary, a worse block,
	// and unknown metadata. No cluster or full storage-sized blocks are needed.
	for i, values := range [][]int64{{0, 1}, {9, 10}, {8, 9}, {0, 0}} {
		bat := batch.NewWithSize(1)
		t.Cleanup(func() { bat.Clean(mp) })
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, i == 3, mp))
		}
		bat.SetRowCount(len(values))
		_, err := writer.WriteBatch(bat)
		require.NoError(t, err)
	}
	_, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	require.Equal(t, uint32(4), stats.BlkCnt())
	data := NewBlockListRelationData(0)
	for i := uint16(0); i < 4; i++ {
		blk := stats.ConstructBlockInfo(i)
		data.AppendBlockInfo(&blk)
	}
	order := func(flag plan.OrderBySpec_OrderByFlag, nullable bool) []*plan.OrderBySpec {
		return []*plan.OrderBySpec{{
			Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: !nullable},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "db.t.k", ColPos: 5}}},
			Flag: flag,
		}}
	}
	next := func(ds *RemoteDataSource, callCtx context.Context) (*objectio.BlockInfo, engine.DataState, error) {
		return ds.Next(callCtx, []string{"k"}, []types.Type{types.T_int64.ToType()}, []uint16{0}, -1, nil, nil, nil)
	}
	threshold := func(value int64) objectio.ZoneMap {
		zm := objectio.NewZM(types.T_int64, 0)
		index.UpdateZM(zm, types.EncodeInt64(&value))
		return zm
	}

	t.Run("metadata_error", func(t *testing.T) {
		want := errors.New("ordered metadata read failed")
		ds := NewRemoteDataSource(ctx, orderedScanFailReadFS{FileService: fs, err: want}, timestamp.Timestamp{}, data)
		defer ds.Close()
		ds.SetOrderBy(order(plan.OrderBySpec_DESC, false))
		_, _, err := next(ds, ctx)
		require.ErrorIs(t, err, want)
		require.Zero(t, ds.cursor)
		require.Nil(t, ds.orderedBlocks)
	})

	t.Run("appendable_blocks_are_not_pruned", func(t *testing.T) {
		appendableData := NewBlockListRelationData(0)
		for i := uint16(0); i < 4; i++ {
			blk := stats.ConstructBlockInfo(i)
			if i == 1 { // its old zonemap would otherwise be worse than 8
				blk.ObjectFlags |= objectio.ObjectFlag_Appendable
			}
			appendableData.AppendBlockInfo(&blk)
		}
		ds := NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, appendableData)
		defer ds.Close()
		ds.SetOrderBy(order(plan.OrderBySpec_ASC, false))
		ds.SetFilterZM(threshold(8))
		var got []uint16
		for {
			blk, state, err := next(ds, ctx)
			require.NoError(t, err)
			if state == engine.End {
				break
			}
			got = append(got, blk.MetaLocation().ID())
		}
		require.ElementsMatch(t, []uint16{0, 1, 2, 3}, got)
	})

	for _, tc := range []struct {
		name     string
		flag     plan.OrderBySpec_OrderByFlag
		value    int64
		want     []uint16
		nullable bool
		unmapped bool
	}{
		{name: "descending_tie", flag: plan.OrderBySpec_DESC, value: 9, want: []uint16{3, 1, 2}},
		{name: "ascending_tie", value: 8, want: []uint16{3, 0, 2}},
		{name: "nullable_fallback", value: -1, nullable: true, want: []uint16{0, 1, 2, 3}},
		{name: "unmapped_fallback", value: -1, unmapped: true, want: []uint16{0, 1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, data)
			defer ds.Close()
			by := order(tc.flag, tc.nullable)
			if tc.unmapped {
				by[0].Expr.GetCol().Name = "absent"
			}
			ds.SetOrderBy(by)
			zm := threshold(tc.value)
			ds.SetFilterZM(zm)
			// A weaker later message must not loosen an already accepted bound.
			weaker := tc.value + 1
			if tc.flag&plan.OrderBySpec_DESC != 0 {
				weaker = tc.value - 1
			}
			ds.SetFilterZM(threshold(weaker))
			if !tc.nullable && !tc.unmapped {
				require.Equal(t, zm, ds.filterZM)
			}
			var got []uint16
			for {
				blk, state, err := next(ds, ctx)
				require.NoError(t, err)
				if state == engine.End {
					break
				}
				got = append(got, blk.MetaLocation().ID())
			}
			require.Equal(t, tc.want, got)
			for i := 0; i < 4; i++ {
				blk := data.GetBlockInfo(i)
				require.Equal(t, uint16(i), blk.MetaLocation().ID(), "不能修改共享输入排列")
			}
			ds.Close()
			require.Nil(t, ds.filterZM)
			require.Nil(t, ds.orderedBlocks)
			require.Zero(t, ds.cursor)
		})
	}

	for _, tc := range []struct {
		name      string
		orderName string
		cols      []string
	}{
		{name: "ambiguous_quoted_name_fallback", orderName: "t.k", cols: []string{"t.k", "k"}},
		{name: "distinct_unicode_names", orderName: "t.ς", cols: []string{"σ", "ς"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, data)
			defer ds.Close()
			by := order(plan.OrderBySpec_DESC, false)
			by[0].Expr.GetCol().Name = tc.orderName
			ds.SetOrderBy(by)
			ds.SetFilterZM(threshold(9))
			// The requested column has no metadata in this older object. Neither
			// qualification ambiguity nor Unicode case folding may select its neighbor.
			var got []uint16
			for {
				blk, state, err := ds.Next(ctx, tc.cols,
					[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, []uint16{0, 1}, -1, nil, nil, nil)
				require.NoError(t, err)
				if state == engine.End {
					break
				}
				got = append(got, blk.MetaLocation().ID())
			}
			require.ElementsMatch(t, []uint16{0, 1, 2, 3}, got)
		})
	}

	t.Run("concurrent_readers", func(t *testing.T) {
		type result struct {
			desc bool
			ids  []uint16
			err  error
		}
		results := make(chan result, 2)
		var workers sync.WaitGroup
		for _, desc := range []bool{false, true} {
			workers.Go(func() {
				ds := NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, data)
				defer ds.Close()
				flag := plan.OrderBySpec_ASC
				if desc {
					flag = plan.OrderBySpec_DESC
				}
				ds.SetOrderBy(order(flag, false))
				ds.SetFilterZM(threshold(8))
				got := result{desc: desc}
				for {
					blk, state, err := next(ds, ctx)
					if err != nil || state == engine.End {
						got.err = err
						break
					}
					got.ids = append(got.ids, blk.MetaLocation().ID())
				}
				results <- got
			})
		}
		workers.Wait()
		close(results)
		for got := range results {
			require.NoError(t, got.err)
			want := []uint16{3, 0, 2}
			if got.desc {
				want = []uint16{3, 1, 2}
			}
			require.Equal(t, want, got.ids)
		}
	})

	t.Run("cancel_then_reuse", func(t *testing.T) {
		ds := NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, data)
		defer ds.Close()
		ds.SetOrderBy(order(plan.OrderBySpec_DESC, false))
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err := next(ds, canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, ds.cursor)
		ds.Close()
		blk, state, err := next(ds, ctx)
		require.NoError(t, err)
		require.Equal(t, engine.Persisted, state)
		require.Equal(t, uint16(3), blk.MetaLocation().ID())
	})
}

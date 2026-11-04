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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

func newStatsLookupEngine(t *testing.T) *Engine {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	eng := &Engine{mp: mp}
	eng.catalog.Store(cache.NewCatalog())
	packer := types.NewPacker()
	defer packer.Close()
	for _, tbl := range []catalog.Table{
		{AccountId: 1, DatabaseId: 10, TableId: 101, DatabaseName: "db1", TableName: "a", Kind: "r"},
		{AccountId: 1, DatabaseId: 10, TableId: 102, DatabaseName: "db1", TableName: "v", Kind: "v"},
		{AccountId: 1, DatabaseId: 20, TableId: 201, DatabaseName: "db2", TableName: "a", Kind: "r"},
		{AccountId: 2, DatabaseId: 10, TableId: 301, DatabaseName: "tenant2", TableName: "a", Kind: "r"},
	} {
		bat, err := catalog.GenCreateTableTuple(tbl, mp, packer)
		require.NoError(t, err)
		_, err = fillRandomRowidAndZeroTs(bat, mp)
		require.NoError(t, err)
		vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))[0] = types.BuildTS(1, 0)
		eng.GetLatestCatalogCache().InsertTable(bat)
		bat.Clean(mp)
		eng.GetLatestCatalogCache().GetTableById(tbl.AccountId, tbl.DatabaseId, tbl.TableId).PrimarySeqnum = 7
	}
	return eng
}

func TestBuildTablePairsFromCachePreservesOrderAndMetadata(t *testing.T) {
	eng := newStatsLookupEngine(t)
	accs := []uint64{2, 1, 1, 1, 1, 1, 1}
	dbs := []uint64{10, 20, 10, 10, 10, 0, 10}
	ids := []uint64{301, 201, 999, 102, 101, 201, 101}
	snapshot := types.BuildTS(10, 0)
	want := []tablePair{
		{valid: true, acc: 2, db: 10, tbl: 301, dbName: "tenant2", tblName: "a", relKind: "r", pkSequence: 7, snapshot: snapshot, onlyUpdateTS: true},
		{valid: true, acc: 1, db: 20, tbl: 201, dbName: "db2", tblName: "a", relKind: "r", pkSequence: 7, snapshot: snapshot, onlyUpdateTS: true},
		{valid: true, acc: 1, db: 10, tbl: 101, dbName: "db1", tblName: "a", relKind: "r", pkSequence: 7, snapshot: snapshot, onlyUpdateTS: true},
		{valid: true, acc: 1, db: 0, tbl: 201, dbName: "db2", tblName: "a", relKind: "r", pkSequence: 7, snapshot: snapshot, onlyUpdateTS: true},
		{valid: true, acc: 1, db: 10, tbl: 101, dbName: "db1", tblName: "a", relKind: "r", pkSequence: 7, snapshot: snapshot, onlyUpdateTS: true},
	}
	require.Equal(t, want, buildTablePairsFromCache(eng, accs, dbs, ids, snapshot, true))
	require.Nil(t, buildTablePairsFromCache(eng, nil, nil, nil, snapshot, false))
	for _, id := range []uint64{101, 102, 999} {
		t.Run("singleton-"+strconv.FormatUint(id, 10), func(t *testing.T) {
			got := buildTablePairsFromCache(eng, []uint64{1}, []uint64{10}, []uint64{id}, snapshot, true)
			if id == 101 {
				require.Equal(t, []tablePair{want[2]}, got)
			} else {
				require.Empty(t, got)
			}
		})
	}
	require.Empty(t, buildTablePairsFromCache(eng, []uint64{1, 1}, []uint64{10, 10}, []uint64{102, 999}, snapshot, false))
}

func TestGetChangedTableListBatchLookupConsumer(t *testing.T) {
	eng := newStatsLookupEngine(t)
	waiter := client.NewTimestampWaiter(runtime.DefaultRuntime().Logger())
	defer waiter.Close()
	waiter.NotifyLatestCommitTS(timestamp.Timestamp{PhysicalTime: 100})
	client.RunTxnTests(func(cli client.TxnClient, _ rpc.TxnSender) {
		eng.cli = cli
		for _, typ := range []cmd_util.ChangedListType{cmd_util.CheckChanged, cmd_util.CollectChanged} {
			t.Run(typ.String(), func(t *testing.T) {
				var pairs []tablePair
				var to, oldest types.TS
				calls := 0
				from := timestamp.Timestamp{PhysicalTime: 10}
				latest := timestamp.Timestamp{PhysicalTime: 20}
				stamps := []timestamp.Timestamp{{}, from, {}, latest}
				if typ == cmd_util.CollectChanged {
					stamps = []timestamp.Timestamp{from, latest}
				}
				err := GetChangedTableList(t.Context(), "", eng,
					[]uint64{1, 1, 1, 1}, []uint64{10, 10, 10, 10}, []uint64{101, 999, 102, 101}, stamps,
					&to, &oldest, typ,
					func(aid, db, id int64, name, dbName, kind string, sequence int, snapshot types.TS) {
						pairs = append(pairs, tablePair{acc: aid, db: db, tbl: id, tblName: name,
							dbName: dbName, relKind: kind, pkSequence: sequence, snapshot: snapshot})
					},
					func(_ context.Context, _ txn.TxnMeta, req *cmd_util.GetChangedTableListReq, resp *cmd_util.GetChangedTableListResp) (func(), error) {
						calls++
						require.Equal(t, typ, req.Type)
						if typ == cmd_util.CheckChanged {
							require.Equal(t, []uint64{999, 101}, req.TableIds)
							require.Equal(t, []uint64{1, 1}, req.AccIds)
							require.Equal(t, []uint64{10, 10}, req.DatabaseIds)
						} else {
							require.Empty(t, req.TableIds)
						}
						require.Equal(t, []*timestamp.Timestamp{&from, &latest}, req.TS)
						resp.AccIds = []uint64{1, 1, 1, 1, 2, 1}
						resp.DatabaseIds = []uint64{20, 10, 10, 10, 10, 10}
						resp.TableIds = []uint64{201, 101, 999, 101, 301, 102}
						resp.Oldest = &from
						return nil, nil
					})
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				require.Equal(t, types.TimestampToTS(from), oldest)
				require.Equal(t, types.TimestampToTS(timestamp.Timestamp{PhysicalTime: 100}.Next()), to)
				wantIDs := []int64{201, 101, 101, 301}
				if typ == cmd_util.CheckChanged {
					wantIDs = append([]int64{101}, wantIDs...)
				}
				gotIDs := make([]int64, 0, len(pairs))
				for _, pair := range pairs {
					gotIDs = append(gotIDs, pair.tbl)
					require.Equal(t, to, pair.snapshot)
					require.Equal(t, "r", pair.relKind)
					require.Equal(t, 7, pair.pkSequence)
					require.Equal(t, "a", pair.tblName)
					switch pair.tbl {
					case 101:
						require.Equal(t, int64(1), pair.acc)
						require.Equal(t, int64(10), pair.db)
						require.Equal(t, "db1", pair.dbName)
					case 201:
						require.Equal(t, int64(1), pair.acc)
						require.Equal(t, int64(20), pair.db)
						require.Equal(t, "db2", pair.dbName)
					case 301:
						require.Equal(t, int64(2), pair.acc)
						require.Equal(t, int64(10), pair.db)
						require.Equal(t, "tenant2", pair.dbName)
					}
				}
				require.Equal(t, wantIDs, gotIDs)
			})
		}
	}, client.WithTimestampWaiter(waiter), client.WithEnableSacrificingFreshness())
}

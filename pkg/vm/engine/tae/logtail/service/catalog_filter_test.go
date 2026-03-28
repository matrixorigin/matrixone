// Copyright 2024 Matrix Origin
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

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestSessionConfigureLazyCatalogSubscription(t *testing.T) {
	ss := newCatalogTestSession(t)

	req := &logtail.SubscribeRequest{
		Table:                 &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
		LazyCatalog:           true,
		InitialActiveAccounts: []uint32{0, 10},
	}
	require.NoError(t, ss.configureLazyCatalogSubscription(req))
	require.True(t, ss.lazyCatalog.enabled)
	_, ok := ss.lazyCatalog.activeAccounts[0]
	require.True(t, ok)
	_, ok = ss.lazyCatalog.activeAccounts[10]
	require.True(t, ok)
	require.Nil(t, ss.lazyCatalog.activatingSeqByAccount)

	err := ss.configureLazyCatalogSubscription(&logtail.SubscribeRequest{
		Table:       &api.TableID{DbId: 10, TbId: 20},
		LazyCatalog: true,
	})
	require.Error(t, err)
}

func TestSessionPrepareLazyCatalogPublishWrapsFiltersCatalogRowsByAccount(t *testing.T) {
	ss := newCatalogTestSession(t)
	table := api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID}
	id := MarshalTableID(&table)

	require.False(t, ss.Register(id, table))
	require.NoError(t, ss.configureLazyCatalogSubscription(&logtail.SubscribeRequest{
		Table:                 &table,
		LazyCatalog:           true,
		InitialActiveAccounts: []uint32{0},
	}))
	ss.AdvanceState(id)

	wraps := []wrapLogtail{{
		id: id,
		tail: logtail.TableLogtail{
			Table: &table,
			Ts:    &timestamp.Timestamp{PhysicalTime: 1},
			Commands: []api.Entry{
				mustCatalogColumnInsertEntry(t, []uint32{0}),
				mustCatalogColumnInsertEntry(t, []uint32{10}),
			},
		},
	}}
	filtered, err := ss.prepareLazyCatalogPublishWrapsFromIndex(wraps, firstLazyCatalogWrapIndex(wraps))
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Len(t, filtered[0].tail.Commands, 1)
	require.Equal(t, []uint32{0}, mustAccountIDsFromEntry(t, filtered[0].tail.Commands[0]))
}

func TestFilterLazyCatalogSubscribeRowsInTailCopiesMixedInsertEntry(t *testing.T) {
	filtered, closeCB, err := filterLazyCatalogSubscribeRowsInTail(logtail.TableLogtail{
		Table:       &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID},
		CkpLocation: "ckp:should-be-stripped",
		Commands: []api.Entry{
			mustCatalogColumnInsertEntry(t, []uint32{0, 10, 20}),
		},
	}, &lazyCatalogAllowedAccounts{accounts: map[uint32]struct{}{10: {}}})
	require.NoError(t, err)
	require.NotNil(t, closeCB)
	if closeCB != nil {
		t.Cleanup(closeCB)
	}
	require.Empty(t, filtered.CkpLocation)
	require.Len(t, filtered.Commands, 1)
	require.Equal(t, []uint32{10}, mustAccountIDsFromEntry(t, filtered.Commands[0]))
}

func TestSessionPrepareLazyCatalogPublishWrapsLeavesNormalTableUntouched(t *testing.T) {
	ss := newCatalogTestSession(t)
	require.NoError(t, ss.configureLazyCatalogSubscription(&logtail.SubscribeRequest{
		Table:                 &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
		LazyCatalog:           true,
		InitialActiveAccounts: []uint32{0},
	}))

	normal := wrapLogtail{
		id: MarshalTableID(&api.TableID{DbId: 10, TbId: 20}),
		tail: logtail.TableLogtail{
			Table: &api.TableID{DbId: 10, TbId: 20},
			Ts:    &timestamp.Timestamp{PhysicalTime: 1},
			Commands: []api.Entry{{
				EntryType: api.Entry_Insert,
			}},
		},
	}

	wraps := []wrapLogtail{normal}
	filtered, err := ss.prepareLazyCatalogPublishWrapsFromIndex(wraps, firstLazyCatalogWrapIndex(wraps))
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, normal.tail.Table.String(), filtered[0].tail.Table.String())
	require.Len(t, filtered[0].tail.Commands, 1)
}

func TestFilterLazyCatalogSubscribeRowsInTailUsesCPKeyForDelete(t *testing.T) {
	filtered, closeCB, err := filterLazyCatalogSubscribeRowsInTail(logtail.TableLogtail{
		Table: &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
		Commands: []api.Entry{
			mustCatalogTableDeleteEntry(t, []uint32{0, 10}),
		},
	}, &lazyCatalogAllowedAccounts{accounts: map[uint32]struct{}{10: {}}})
	require.NoError(t, err)
	require.NotNil(t, closeCB)
	if closeCB != nil {
		t.Cleanup(closeCB)
	}
	require.Len(t, filtered.Commands, 1)
	require.Equal(t, []uint32{10}, mustCPKeyAccountsFromEntry(t, filtered.Commands[0]))
}

func TestFilterLazyCatalogPublishRowsInTailUsesEntryAccountSummary(t *testing.T) {
	table := api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID}
	entry := api.Entry{
		EntryType:  api.Entry_Insert,
		TableId:    catalog.MO_COLUMNS_ID,
		DatabaseId: catalog.MO_CATALOG_ID,
	}
	catalog.SetLazyCatalogEntryAccountSummary(&entry, 10)

	tail := logtail.TableLogtail{
		Table:    &table,
		Commands: []api.Entry{entry},
	}

	filtered, changed, err := filterLazyCatalogPublishRowsInTail(
		tail,
		&lazyCatalogAllowedAccounts{accounts: map[uint32]struct{}{10: {}}},
	)
	require.NoError(t, err)
	require.False(t, changed)
	require.True(t, &filtered.Commands[0] == &tail.Commands[0])

	filtered, changed, err = filterLazyCatalogPublishRowsInTail(
		tail,
		&lazyCatalogAllowedAccounts{accounts: map[uint32]struct{}{20: {}}},
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Empty(t, filtered.Commands)
}

func newCatalogTestSession(t *testing.T) *Session {
	t.Helper()
	return NewSession(
		context.Background(),
		mockMOLogger(),
		NewLogtailResponsePool(),
		mockSessionErrorNotifier(logutil.GetGlobalLogger()),
		mockMorpcStream(&normalStream{}, 1, 1024),
		time.Second,
		time.Second,
		time.Second,
	)
}

func mustCatalogColumnInsertEntry(t *testing.T, accountIDs []uint32) api.Entry {
	t.Helper()
	mp := mpool.MustNew("catalog-filter-columns")
	packer := types.NewPacker()
	defer packer.Close()
	typ := types.T_int64.ToType()
	typEncoded, err := types.Encode(&typ)
	require.NoError(t, err)

	cols := make([]catalog.Column, 0, len(accountIDs))
	for i, accountID := range accountIDs {
		cols = append(cols, catalog.Column{
			AccountId:    accountID,
			DatabaseId:   uint64(100 + i),
			DatabaseName: "db",
			TableId:      uint64(200 + i),
			TableName:    "tbl",
			Name:         string(rune('a' + i)),
			Num:          int32(i),
			Typ:          typEncoded,
			TypLen:       int32(len(typEncoded)),
		})
	}

	bat, err := catalog.GenCreateColumnTuples(cols, mp, packer)
	require.NoError(t, err)
	t.Cleanup(func() { bat.Clean(mp) })

	pbBat, err := batch.BatchToProtoBatch(bat)
	require.NoError(t, err)
	return api.Entry{
		EntryType:  api.Entry_Insert,
		TableId:    catalog.MO_COLUMNS_ID,
		DatabaseId: catalog.MO_CATALOG_ID,
		Bat:        pbBat,
	}
}

func mustCatalogTableDeleteEntry(t *testing.T, accountIDs []uint32) api.Entry {
	t.Helper()
	mp := mpool.MustNew("catalog-filter-delete")
	packer := types.NewPacker()
	defer packer.Close()

	var batAcc *batch.Batch
	for i, accountID := range accountIDs {
		bat, err := catalog.GenDropTableTuple(
			types.RandomRowid(),
			accountID,
			uint64(200+i),
			uint64(100+i),
			"tbl",
			"db",
			mp,
			packer,
		)
		require.NoError(t, err)
		if batAcc == nil {
			batAcc = bat
			continue
		}
		require.NoError(t, batAcc.UnionOne(bat, 0, mp))
		bat.Clean(mp)
	}
	t.Cleanup(func() { batAcc.Clean(mp) })

	pbBat, err := batch.BatchToProtoBatch(batAcc)
	require.NoError(t, err)
	return api.Entry{
		EntryType:  api.Entry_Delete,
		TableId:    catalog.MO_TABLES_ID,
		DatabaseId: catalog.MO_CATALOG_ID,
		Bat:        pbBat,
	}
}

func mustAccountIDsFromEntry(t *testing.T, entry api.Entry) []uint32 {
	t.Helper()
	bat, err := batch.ProtoBatchToBatch(entry.Bat)
	require.NoError(t, err)
	accountIdx := findBatchAttrIndex(bat.Attrs, catalog.SystemDBAttr_AccID)
	require.GreaterOrEqual(t, accountIdx, 0)
	return append([]uint32(nil), vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(int32(accountIdx)))...)
}

func mustCPKeyAccountsFromEntry(t *testing.T, entry api.Entry) []uint32 {
	t.Helper()
	bat, err := batch.ProtoBatchToBatch(entry.Bat)
	require.NoError(t, err)
	cpkeyIdx := findBatchAttrIndex(bat.Attrs, catalog.CPrimaryKeyColName)
	require.GreaterOrEqual(t, cpkeyIdx, 0)

	accounts := make([]uint32, 0, bat.RowCount())
	cpkeyVec := bat.GetVector(int32(cpkeyIdx))
	for row := 0; row < bat.RowCount(); row++ {
		accountID, err := catalog.DecodeLazyCatalogAccountFromCPKey(cpkeyVec.GetBytesAt(row))
		require.NoError(t, err)
		accounts = append(accounts, accountID)
	}
	return accounts
}

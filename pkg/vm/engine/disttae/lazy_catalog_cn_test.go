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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestLazyCatalogCNState_BasicStateTransitions(t *testing.T) {
	s := newLazyCatalogCNState()
	s.enable()

	assert.False(t, s.isAccountReady(10))
	_, ok := s.getAccountReadyTS(10)
	assert.False(t, ok)

	seq := s.beginCatchingUp(10)
	assert.True(t, seq > 0)
	assert.False(t, s.isAccountReady(10))
	assert.True(t, s.hasCatchingUp())

	readyTS := timestamp.Timestamp{PhysicalTime: 100}
	s.setAccountReady(10, readyTS)

	assert.True(t, s.isAccountReady(10))
	assert.False(t, s.hasCatchingUp())

	gotTS, ok := s.getAccountReadyTS(10)
	assert.True(t, ok)
	assert.Equal(t, readyTS, gotTS)
}

func TestLazyCatalogCNState_CatchingUpCount(t *testing.T) {
	s := newLazyCatalogCNState()

	assert.False(t, s.hasCatchingUp())

	s.beginCatchingUp(1)
	assert.True(t, s.hasCatchingUp())

	s.beginCatchingUp(2)
	assert.True(t, s.hasCatchingUp())

	s.setAccountReady(1, timestamp.Timestamp{PhysicalTime: 100})
	assert.True(t, s.hasCatchingUp())

	s.setAccountReady(2, timestamp.Timestamp{PhysicalTime: 200})
	assert.False(t, s.hasCatchingUp())
}

func TestLazyCatalogCNState_CleanupFailedActivation(t *testing.T) {
	s := newLazyCatalogCNState()
	s.enable()

	seq := s.beginCatchingUp(10)
	assert.True(t, s.hasCatchingUp())
	assert.True(t, s.matchesPendingSeq(10, seq))

	ok := s.cleanupFailedActivation(10, seq)
	assert.True(t, ok)
	assert.False(t, s.hasCatchingUp())
	assert.False(t, s.isAccountReady(10))

	ok = s.cleanupFailedActivation(10, seq)
	assert.False(t, ok)
}

func TestLazyCatalogCNState_StaleSeqCleanup(t *testing.T) {
	s := newLazyCatalogCNState()

	seq1 := s.beginCatchingUp(10)
	seq2 := s.beginCatchingUp(10)
	assert.NotEqual(t, seq1, seq2)

	ok := s.cleanupFailedActivation(10, seq1)
	assert.False(t, ok)

	ok = s.cleanupFailedActivation(10, seq2)
	assert.True(t, ok)
}

func TestLazyCatalogCNState_DelayAccountCacheApply(t *testing.T) {
	s := newLazyCatalogCNState()

	called := false
	f := func() { called = true }

	delayed := s.delayAccountCacheApply(10, f)
	assert.False(t, delayed)

	s.beginCatchingUp(10)

	delayed = s.delayAccountCacheApply(10, f)
	assert.True(t, delayed)
	assert.False(t, called)

	for _, fn := range s.beginAccountReadyTransition(10) {
		fn()
	}
	s.finishAccountReady(10, timestamp.Timestamp{PhysicalTime: 100})
	assert.True(t, called)
}

func TestLazyCatalogCNState_ReadyTransitionDrainsAccountDCA(t *testing.T) {
	s := newLazyCatalogCNState()
	s.enable()
	s.beginCatchingUp(10)

	count := 0
	for i := 0; i < 5; i++ {
		s.delayAccountCacheApply(10, func() { count++ })
	}

	fns := s.beginAccountReadyTransition(10)
	assert.False(t, s.delayAccountCacheApply(10, func() { count = 999 }))
	_, ok := s.getAccountReadyTS(10)
	assert.False(t, ok)
	for _, fn := range fns {
		fn()
	}
	s.finishAccountReady(10, timestamp.Timestamp{PhysicalTime: 200})
	assert.Equal(t, 5, count)

	readyTS, ok := s.getAccountReadyTS(10)
	assert.True(t, ok)
	assert.Equal(t, int64(200), readyTS.PhysicalTime)
}

func TestLazyCatalogCNState_WantedAccounts(t *testing.T) {
	s := newLazyCatalogCNState()

	s.setAccountReady(0, timestamp.Timestamp{PhysicalTime: 10})
	s.setAccountReady(5, timestamp.Timestamp{PhysicalTime: 20})
	s.setAccountReady(10, timestamp.Timestamp{PhysicalTime: 30})

	s.collectWantedAccounts()
	wanted := s.snapshotWantedAccounts()

	assert.Len(t, wanted, 3)
	wantedSet := make(map[uint32]struct{})
	for _, id := range wanted {
		wantedSet[id] = struct{}{}
	}
	assert.Contains(t, wantedSet, uint32(0))
	assert.Contains(t, wantedSet, uint32(5))
	assert.Contains(t, wantedSet, uint32(10))
}

func TestLazyCatalogCNState_ResetAllStates(t *testing.T) {
	s := newLazyCatalogCNState()
	s.enable()

	s.beginCatchingUp(10)
	s.setAccountReady(20, timestamp.Timestamp{PhysicalTime: 100})
	s.inflightActivations.Store(uint32(30), &inflightActivation{done: make(chan struct{})})

	s.collectWantedAccounts()
	s.resetAllStates()

	assert.False(t, s.isAccountReady(20))
	assert.False(t, s.hasCatchingUp())

	wanted := s.snapshotWantedAccounts()
	wantedSet := make(map[uint32]struct{})
	for _, id := range wanted {
		wantedSet[id] = struct{}{}
	}
	assert.Contains(t, wantedSet, uint32(20))

	_, ok := s.inflightActivations.Load(uint32(30))
	assert.False(t, ok)
}

func TestLazyCatalogCNState_ResetAllStatesNotifiesPendingResponses(t *testing.T) {
	s := newLazyCatalogCNState()
	ch := s.registerPendingResponse(10, 42)

	s.resetAllStates()

	resp := <-ch
	assert.Nil(t, resp)
}

func TestLazyCatalogCNState_PendingResponse(t *testing.T) {
	s := newLazyCatalogCNState()

	ch := s.registerPendingResponse(10, 42)
	require.NotNil(t, ch)

	resp := &logtail.ActivateAccountForCatalogResponse{
		AccountId: 10,
		Seq:       42,
	}
	ok := s.deliverActivationResponse(resp)
	assert.True(t, ok)

	received := <-ch
	assert.Equal(t, uint32(10), received.AccountId)
	assert.Equal(t, uint64(42), received.Seq)

	ok = s.deliverActivationResponse(resp)
	assert.False(t, ok)
}

func TestLazyCatalogCNState_PendingResponseIgnoresStaleSeq(t *testing.T) {
	s := newLazyCatalogCNState()

	seq1 := s.beginCatchingUp(10)
	ch1 := s.registerPendingResponse(10, seq1)
	require.NotNil(t, ch1)
	require.True(t, s.cleanupFailedActivation(10, seq1))

	seq2 := s.beginCatchingUp(10)
	ch2 := s.registerPendingResponse(10, seq2)
	require.NotNil(t, ch2)

	ok := s.deliverActivationResponse(&logtail.ActivateAccountForCatalogResponse{
		AccountId: 10,
		Seq:       seq1,
	})
	assert.False(t, ok)

	select {
	case <-ch2:
		t.Fatal("stale response should not be delivered to new waiter")
	default:
	}

	ok = s.deliverActivationResponse(&logtail.ActivateAccountForCatalogResponse{
		AccountId: 10,
		Seq:       seq2,
	})
	require.True(t, ok)
	got := <-ch2
	require.Equal(t, seq2, got.Seq)
}

func TestCanServeAccount(t *testing.T) {
	c := &PushClient{}
	assert.True(t, c.CanServeAccount(10, timestamp.Timestamp{PhysicalTime: 100}))

	c.lazyCatalog = newLazyCatalogCNState()
	assert.True(t, c.CanServeAccount(10, timestamp.Timestamp{PhysicalTime: 100}))

	c.lazyCatalog.enable()
	assert.False(t, c.CanServeAccount(10, timestamp.Timestamp{PhysicalTime: 100}))

	c.lazyCatalog.setAccountReady(10, timestamp.Timestamp{PhysicalTime: 50})
	assert.True(t, c.CanServeAccount(10, timestamp.Timestamp{PhysicalTime: 50}))
	assert.True(t, c.CanServeAccount(10, timestamp.Timestamp{PhysicalTime: 100}))
	assert.False(t, c.CanServeAccount(10, timestamp.Timestamp{PhysicalTime: 30}))
}

func TestIsLazyCatalogTableID(t *testing.T) {
	require.True(t, isLazyCatalogTableID(catalog.MO_DATABASE_ID))
	require.True(t, isLazyCatalogTableID(catalog.MO_TABLES_ID))
	require.True(t, isLazyCatalogTableID(catalog.MO_COLUMNS_ID))
	require.False(t, isLazyCatalogTableID(catalog.MO_TABLES_LOGICAL_ID_INDEX_ID))
}

func TestReconnectInitialActiveAccounts(t *testing.T) {
	c := &PushClient{lazyCatalog: newLazyCatalogCNState()}
	c.lazyCatalog.setAccountReady(5, timestamp.Timestamp{PhysicalTime: 10})
	c.lazyCatalog.collectWantedAccounts()

	require.ElementsMatch(t, []uint32{0, 5}, c.reconnectInitialActiveAccounts())
}

func TestLazyCatalogCNStateShouldDelayCatalogCacheApplyEntryInsert(t *testing.T) {
	s := newLazyCatalogCNState()
	s.enable()
	s.beginCatchingUp(10)

	accountID, shouldDelay, err := s.shouldDelayCatalogCacheApplyEntry(mustCatalogColumnInsertEntry(t, []uint32{10, 10}))
	require.NoError(t, err)
	require.Equal(t, uint32(10), accountID)
	require.True(t, shouldDelay)

	accountID, shouldDelay, err = s.shouldDelayCatalogCacheApplyEntry(mustCatalogColumnInsertEntry(t, []uint32{20, 20}))
	require.NoError(t, err)
	require.Equal(t, uint32(20), accountID)
	require.False(t, shouldDelay)
}

func TestLazyCatalogCNStateShouldDelayCatalogCacheApplyEntryDelete(t *testing.T) {
	s := newLazyCatalogCNState()
	s.enable()
	s.beginCatchingUp(10)

	accountID, shouldDelay, err := s.shouldDelayCatalogCacheApplyEntry(mustCatalogTableDeleteEntry(t, []uint32{10, 10}))
	require.NoError(t, err)
	require.Equal(t, uint32(10), accountID)
	require.True(t, shouldDelay)

	accountID, shouldDelay, err = s.shouldDelayCatalogCacheApplyEntry(mustCatalogTableDeleteEntry(t, []uint32{20, 20}))
	require.NoError(t, err)
	require.Equal(t, uint32(20), accountID)
	require.False(t, shouldDelay)
}

func TestLazyCatalogCNStateShouldDelayCatalogCacheApplyEntryUsesSummary(t *testing.T) {
	s := newLazyCatalogCNState()
	s.enable()
	s.beginCatchingUp(10)

	entry := api.Entry{
		EntryType:  api.Entry_Insert,
		TableId:    catalog.MO_COLUMNS_ID,
		DatabaseId: catalog.MO_CATALOG_ID,
	}
	catalog.SetLazyCatalogEntryAccountSummary(&entry, 10)

	accountID, shouldDelay, err := s.shouldDelayCatalogCacheApplyEntry(entry)
	require.NoError(t, err)
	require.Equal(t, uint32(10), accountID)
	require.True(t, shouldDelay)
}

func mustCatalogColumnInsertEntry(t *testing.T, accountIDs []uint32) api.Entry {
	t.Helper()
	mp := mpool.MustNew("lazy-catalog-cn-columns")
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
	mp := mpool.MustNew("lazy-catalog-cn-delete")
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

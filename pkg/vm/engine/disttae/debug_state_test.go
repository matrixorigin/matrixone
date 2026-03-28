// Copyright 2024 Matrix Origin
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

func TestLazyCatalogDebugState(t *testing.T) {
	state := newLazyCatalogCNState()
	state.enabled.Store(true)
	state.accounts[20] = &accountCatalogEntry{
		state:   accountReady,
		readyTS: timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 2},
	}
	state.accounts[10] = &accountCatalogEntry{
		state: accountCatchingUp,
	}
	state.pendingSeq[10] = 7
	state.accountDCA[10] = []func(){func() {}, func() {}}
	state.catchingUpCount.Store(1)
	state.wantedAccounts[0] = struct{}{}
	state.wantedAccounts[20] = struct{}{}
	state.pendingActivationResponses[activationResponseKey{accountID: 10, seq: 7}] = make(chan *logtail.ActivateAccountForCatalogResponse)
	state.inflightActivations.Store(uint32(10), &inflightActivation{done: make(chan struct{})})

	got := state.debugState(nil)
	require.True(t, got.Enabled)
	require.Equal(t, 1, got.PendingActivationResponseCount)
	require.Equal(t, 1, got.InflightActivationCount)
	require.Equal(t, []uint32{0, 20}, got.WantedAccounts)
	require.Len(t, got.Accounts, 2)

	require.Equal(t, uint32(10), got.Accounts[0].AccountID)
	require.Equal(t, "catching_up", got.Accounts[0].State)
	require.Nil(t, got.Accounts[0].ReadyTS)
	require.Equal(t, uint64(7), got.Accounts[0].PendingSeq)
	require.Equal(t, 2, got.Accounts[0].DelayedApplyCount)

	require.Equal(t, uint32(20), got.Accounts[1].AccountID)
	require.Equal(t, "ready", got.Accounts[1].State)
	require.NotNil(t, got.Accounts[1].ReadyTS)
	require.Equal(t, int64(200), got.Accounts[1].ReadyTS.PhysicalTime)
	require.Equal(t, uint32(2), got.Accounts[1].ReadyTS.LogicalTime)

	account := state.debugAccountState(20)
	require.True(t, account.Present)
	require.True(t, account.InWantedAccounts)
	require.Equal(t, "ready", account.State)
	require.NotNil(t, account.ReadyTS)
}

func TestLazyCatalogDebugActivationHistory(t *testing.T) {
	state := newLazyCatalogCNState()
	now := time.Now()
	later := now.Add(time.Second)
	targetTS := timestamp.Timestamp{PhysicalTime: 300}
	replayTS := timestamp.Timestamp{PhysicalTime: 320}

	state.recordActivationEvent(DebugCatalogActivationEvent{
		AccountID:  10,
		Seq:        7,
		Source:     "activation",
		Phase:      "complete",
		Result:     "ok",
		TargetTS:   &targetTS,
		ReplayTS:   &replayTS,
		StartedAt:  &now,
		FinishedAt: &later,
	})
	state.recordActivationEvent(DebugCatalogActivationEvent{
		AccountID: 20,
		Source:    "reconnect_restore",
		Phase:     "complete",
		Result:    "ok",
	})

	accountID := uint32(10)
	got := state.debugActivationHistory(&accountID, 10)
	require.Len(t, got, 1)
	require.Equal(t, uint32(10), got[0].AccountID)
	require.Equal(t, uint64(7), got[0].Seq)
	require.Equal(t, "activation", got[0].Source)
	require.Equal(t, "complete", got[0].Phase)
	require.Equal(t, "ok", got[0].Result)
	require.NotNil(t, got[0].TargetTS)
	require.NotNil(t, got[0].ReplayTS)
	require.NotNil(t, got[0].StartedAt)
	require.NotNil(t, got[0].FinishedAt)
}

func TestDebugCatalogStateWithAccountInspection(t *testing.T) {
	eng := &Engine{}
	cc := cache.NewCatalog()
	cc.UpdateDuration(types.TS{}, types.MaxTs())
	eng.catalog.Store(cc)
	eng.pClient.lazyCatalog = newLazyCatalogCNState()
	eng.pClient.lazyCatalog.enable()
	eng.pClient.lazyCatalog.setAccountReady(42, timestamp.Timestamp{PhysicalTime: 80})
	eng.pClient.receivedLogTailTime.tList = make([]atomic.Value, 1)
	latest := timestamp.Timestamp{PhysicalTime: 100}
	eng.pClient.receivedLogTailTime.tList[0].Store(latest)

	snapshot := timestamp.Timestamp{PhysicalTime: 90}
	accountID := uint32(42)
	got := eng.DebugCatalogState(&accountID, &snapshot)
	require.Equal(t, latest, got.LatestLogtailAppliedTS)
	require.NotNil(t, got.Account)
	require.Equal(t, uint32(42), got.Account.AccountID)
	require.Equal(t, "ready", got.Account.State)
	require.True(t, got.Account.InWantedAccounts)
	require.True(t, got.Account.CanServeLatest)
	require.NotNil(t, got.Account.CanServeSnapshot)
	require.True(t, *got.Account.CanServeSnapshot)
}

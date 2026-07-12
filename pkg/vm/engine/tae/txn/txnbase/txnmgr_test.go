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

package txnbase

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func newTxnManagerForLifecycleTest() *TxnManager {
	mgr := &TxnManager{}
	mgr.txns.store = new(sync.Map)
	return mgr
}

func newTxnForLifecycleTest(mgr *TxnManager, id string) *Txn {
	return NewTxn(mgr, &NoopTxnStore{}, []byte(id), types.TS{}, types.TS{})
}

func waitTxnManagerEmpty(t *testing.T, mgr *TxnManager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, mgr.WaitEmpty(ctx))
}

func TestLoadOrStoreTxnBalancesWaitGroupWhenLoaded(t *testing.T) {
	mgr := newTxnManagerForLifecycleTest()
	first, loaded, offline := mgr.loadOrStoreTxn(
		newTxnForLifecycleTest(mgr, "txn"), TxnFlag_Normal)
	require.False(t, loaded)
	require.False(t, offline)

	actual, loaded, offline := mgr.loadOrStoreTxn(
		newTxnForLifecycleTest(mgr, "txn"), TxnFlag_Normal)
	require.True(t, loaded)
	require.False(t, offline)
	require.Same(t, first, actual)

	require.NoError(t, mgr.DeleteTxn("txn"))
	waitTxnManagerEmpty(t, mgr)
}

func TestLoadOrStoreTxnSkipsTrackingInReadonlyMode(t *testing.T) {
	mgr := newTxnManagerForLifecycleTest()
	mgr.txns.skipFlags.Store(uint64(TxnFlag_Normal))
	txn := newTxnForLifecycleTest(mgr, "offline")

	actual, loaded, offline := mgr.loadOrStoreTxn(txn, TxnFlag_Normal)
	require.False(t, loaded)
	require.True(t, offline)
	require.Same(t, txn, actual)
	_, exists := mgr.txns.store.Load("offline")
	require.False(t, exists)
	waitTxnManagerEmpty(t, mgr)
}

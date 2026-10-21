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

package runtime

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestTxnModeTestConfigScopeAndRestore(t *testing.T) {
	sid := t.Name()
	rt := NewRuntime(metadata.ServiceType_CN, sid, nil)
	SetupServiceBasedRuntime(sid, rt)
	defer CompareAndDeleteServiceBasedRuntime(sid, rt)
	_, ok := GetTxnModeTestConfig(sid, 12)
	require.False(t, ok)
	_, ok = GetTxnModeTestConfig(sid+"-absent", 12)
	require.False(t, ok)
	SetupServiceRuntimeTestingContext(sid)
	tc := MustGetTestingContext(sid)
	_, ok = GetTxnModeTestConfig(sid, 12)
	require.False(t, ok)
	previous := tc.SetTxnModeTestConfig(TxnModeTestConfig{
		AccountID: 12, Mode: txn.TxnMode_Optimistic, Enabled: true,
	})
	defer tc.SetTxnModeTestConfig(previous)
	mode, ok := GetTxnModeTestConfig(sid, 12)
	require.True(t, ok)
	require.Equal(t, txn.TxnMode_Optimistic, mode)
	for _, account := range []uint32{0, 13} {
		_, ok = GetTxnModeTestConfig(sid, account)
		require.False(t, ok, "unrelated accounts keep their normal mode")
	}
	tc.SetTxnModeTestConfig(previous)
	_, ok = GetTxnModeTestConfig(sid, 12)
	require.False(t, ok)
}

func TestTxnModeTestConfigConcurrentAccess(t *testing.T) {
	sid := t.Name()
	rt := NewRuntime(metadata.ServiceType_CN, sid, nil)
	SetupServiceBasedRuntime(sid, rt)
	defer CompareAndDeleteServiceBasedRuntime(sid, rt)
	SetupServiceRuntimeTestingContext(sid)
	tc := MustGetTestingContext(sid)
	cfg := TxnModeTestConfig{AccountID: 12, Mode: txn.TxnMode_Optimistic, Enabled: true}
	tc.SetTxnModeTestConfig(cfg)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 64 {
			tc.SetTxnModeTestConfig(cfg)
		}
	})
	wg.Go(func() {
		for range 64 {
			mode, ok := GetTxnModeTestConfig(sid, 12)
			assertionOK := ok && mode == txn.TxnMode_Optimistic
			if !assertionOK {
				t.Error("selected account lost its mode during concurrent access")
			}
			if _, ok := GetTxnModeTestConfig(sid, 0); ok {
				t.Error("override leaked to the system account")
			}
		}
	})
	wg.Wait()
}

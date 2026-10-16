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

package lockservice

import (
	"context"
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionalUnlockWithoutReplacement(t *testing.T) {
	for _, test := range []struct {
		name       string
		rangeLock  bool
		mutations  []pb.ExtraMutation
		retainKeys [][]byte
	}{
		{
			name: "rows first mutation wins",
			mutations: []pb.ExtraMutation{
				{Key: []byte{12}, Skip: true}, {Key: []byte{12}},
				{Key: []byte{16}}, {Key: []byte{16}, Skip: true}, {Key: []byte{18}},
			},
			retainKeys: newTestRows(12),
		},
		{
			name: "range interior mixed skip", rangeLock: true,
			mutations: []pb.ExtraMutation{
				{Key: []byte{14}, Skip: true}, {Key: []byte{15}}, {Key: []byte{18}},
			},
		},
		{
			name: "range interior all skip", rangeLock: true,
			mutations: []pb.ExtraMutation{
				{Key: []byte{14}, Skip: true}, {Key: []byte{14}}, {Key: []byte{18}},
			},
			retainKeys: newTestRows(12, 16),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			getRunner(false)(t, 10, func(ctx context.Context, s *service, lt *localLockTable) {
				source, survivor := newTestTxnID(1), newTestTxnID(2)
				cleanup := []pb.ExtraMutation{{Key: []byte{12}}, {Key: []byte{16}}, {Key: []byte{18}}}
				for _, id := range [][]byte{source, survivor} {
					options := newTestRowSharedOptions()
					if test.rangeLock {
						options = newTestRangeSharedOptions()
					}
					_, err := s.Lock(ctx, 10, newTestRows(12, 16), id, options)
					require.NoError(t, err)
					defer func() { assert.NoError(t, s.Unlock(context.Background(), id, timestamp.Timestamp{}, cleanup...)) }()
					_, err = s.Lock(ctx, 10, newTestRows(18), id, newTestRowSharedOptions())
					require.NoError(t, err)
				}
				txn := s.activeTxnHolder.getActiveTxn(source, false, "")
				lt.mu.RLock()
				commitTS := lt.mu.tableCommittedAt
				lt.mu.RUnlock()
				commitTS.PhysicalTime++
				apply := func() error {
					txn.Lock()
					defer txn.Unlock()
					return lt.unlockWithContext(ctx, txn, txn.lockHolders[0].tableKeys[10], commitTS, test.mutations...)
				}
				require.NoError(t, apply())
				require.NoError(t, apply(), "replaying conditional cleanup must preserve the remaining holder")
				for _, key := range newTestRows(12, 16, 18) {
					lt.mu.RLock()
					lock, ok := lt.mu.store.Get(key)
					var holders []pb.WaitTxn
					if ok {
						holders = append(holders, lock.holders.getTxnSlice()...)
					}
					lt.mu.RUnlock()
					require.True(t, ok)
					ids := make([][]byte, 0, len(holders))
					for _, holder := range holders {
						ids = append(ids, holder.TxnID)
					}
					require.Contains(t, ids, survivor)
					retained := false
					for _, retainedKey := range test.retainKeys {
						retained = retained || key[0] == retainedKey[0]
					}
					if retained {
						require.Contains(t, ids, source)
					} else {
						require.NotContains(t, ids, source)
					}
				}
				lt.mu.RLock()
				frontier := lt.mu.tableCommittedAt
				lt.mu.RUnlock()
				require.Equal(t, commitTS, frontier)
			})
		})
	}
}

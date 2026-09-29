// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockservice

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestExactRowsAdmissionAcrossOwners(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(t, []string{"exact-owner", "exact-origin", "exact-forward"}, 10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			owner := services[0]
			var table uint64 = 29300
			for _, route := range []string{"local", "remote-proxy", "forward"} {
				for _, exact := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/exact=%t", route, exact), func(t *testing.T) {
						table++
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						_, err := owner.getLockTableWithCreate(ctx, 0, table, nil, pb.Sharding_None)
						require.NoError(t, err)
						origin, closer := owner, owner
						opts := newTestRowExclusiveOptions()
						if route != "local" {
							origin, closer = services[1], services[1]
						}
						if route == "forward" {
							opts.ForwardTo = services[2].serviceID
							closer = services[2]
							_, err = closer.getLockTableWithCreate(ctx, 0, table, nil, pb.Sharding_None)
							require.NoError(t, err)
						}
						id := []byte("exact-owner-txn")
						defer func() { require.NoError(t, closer.Unlock(context.Background(), id, timestamp.Timestamp{})) }()
						// Admit one row, then reuse that row with KeepRows. Re-entry must also
						// disable later ordinary coarsening, even though no new lock was added.
						_, err = origin.Lock(ctx, table, [][]byte{{1}}, id, opts)
						require.NoError(t, err)
						opts.KeepRows = exact
						_, err = origin.Lock(ctx, table, [][]byte{{1}}, id, opts)
						require.NoError(t, err)
						opts.KeepRows = false
						for _, row := range []byte{3, 5} {
							result, err := origin.Lock(ctx, table, [][]byte{{row}}, id, opts)
							require.NoError(t, err)
							require.Equal(t, owner.serviceID, result.LockedOn.ServiceID)
						}
						// Probe the unrequested gap through the actual owner. Ordinary locks
						// still coarsen; exact admission leaves the gap available.
						probe := newTestRowExclusiveOptions()
						probe.Policy = pb.WaitPolicy_FastFail
						probeID := []byte("gap-probe")
						defer func() { require.NoError(t, owner.Unlock(context.Background(), probeID, timestamp.Timestamp{})) }()
						_, err = owner.Lock(ctx, table, [][]byte{{2}}, probeID, probe)
						if exact {
							require.NoError(t, err)
						} else {
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockConflict), "%v", err)
						}
						opts.KeepRows = true
						_, err = origin.Lock(ctx, table, [][]byte{{9}}, id, opts)
						if exact {
							require.NoError(t, err)
							_, err = origin.Lock(ctx, table, [][]byte{{11}}, id, opts)
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade), "fixed capacity must reject without widening: %v", err)
						} else {
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade), "%v", err)
						}
						if route == "remote-proxy" {
							require.IsType(t, &localLockTableProxy{}, origin.tableGroups.get(0, table))
						}
					})
				}
			}
		}, func(cfg *Config) {
			cfg.MaxLockRowCount = 2
			cfg.MaxFixedSliceSize = 4
			cfg.EnableRemoteLocalProxy = true
		})
}

func TestExactRowsRejectsOwnedRangeThroughProxy(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(t, []string{"range-owner", "range-origin"}, 10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			const table = 29320
			_, err := owner.getLockTableWithCreate(ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)
			id := []byte("range-owner-txn")
			defer func() { require.NoError(t, origin.Unlock(context.Background(), id, timestamp.Timestamp{})) }()
			_, err = origin.Lock(ctx, table, [][]byte{{1}, {5}}, id, newTestRangeExclusiveOptions())
			require.NoError(t, err)
			// A Shared singleton could otherwise take the proxy cache route and reuse
			// this owner's covering Exclusive range as a successful exact-row grant.
			opts := newTestRowSharedOptions()
			opts.KeepRows = true
			_, err = origin.Lock(ctx, table, [][]byte{{3}}, id, opts)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade), "%v", err)
			require.IsType(t, &localLockTableProxy{}, origin.tableGroups.get(0, table))
			// A fresh exact request traverses the proxy to the owner and can acquire a
			// disjoint key; its row remains compatible with an independent Shared owner.
			fresh := []byte("fresh-exact")
			defer func() { require.NoError(t, origin.Unlock(context.Background(), fresh, timestamp.Timestamp{})) }()
			result, err := origin.Lock(ctx, table, [][]byte{{9}}, fresh, opts)
			require.NoError(t, err)
			require.Equal(t, owner.serviceID, result.LockedOn.ServiceID)
			follower := []byte("fresh-exact-follower")
			defer func() { require.NoError(t, origin.Unlock(context.Background(), follower, timestamp.Timestamp{})) }()
			_, err = origin.Lock(ctx, table, [][]byte{{9}}, follower, opts)
			require.NoError(t, err)
			lt := owner.tableGroups.get(0, table).(*localLockTable)
			lt.mu.RLock()
			held, ok := lt.mu.store.Get([]byte{9})
			exactHolder := ok && held.isLockRow() && held.holders.contains(fresh) && held.holders.contains(follower)
			lt.mu.RUnlock()
			require.True(t, exactHolder, "real owner must hold the caller txn, not a cached proxy grant")

			// A first Exclusive batch can itself exceed the coarsening budget.
			// KeepRows must preserve its gaps before any txn ledger exists.
			_, err = owner.getLockTableWithCreate(ctx, 0, table+1, nil, pb.Sharding_None)
			require.NoError(t, err)
			bulk := newTestRowExclusiveOptions()
			bulk.KeepRows = true
			_, err = origin.Lock(ctx, table+1, [][]byte{{1}, {3}, {5}}, fresh, bulk)
			require.NoError(t, err)
			bulk.Policy = pb.WaitPolicy_FastFail
			probeID := []byte("bulk-gap-probe")
			defer func() { require.NoError(t, owner.Unlock(context.Background(), probeID, timestamp.Timestamp{})) }()
			_, err = owner.Lock(ctx, table+1, [][]byte{{2}}, probeID, bulk)
			require.NoError(t, err)
		}, func(cfg *Config) { cfg.MaxLockRowCount = 2; cfg.EnableRemoteLocalProxy = true })
}

func TestExactRowsWaitingOutcomes(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(t, []string{"waiting-exact"}, 10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			for i, name := range []string{"cancel-empty", "cancel-partial", "coarsening-wins"} {
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					table := uint64(29330 + i)
					id, blocker, probe := []byte("waiting-exact-txn"), []byte("blocker"), []byte("probe")
					for _, txn := range [][]byte{id, blocker, probe} {
						defer func() { require.NoError(t, s.Unlock(context.Background(), txn, timestamp.Timestamp{})) }()
					}
					ordinary := newTestRowExclusiveOptions()
					_, err := s.Lock(ctx, table, [][]byte{{1}, {3}}, id, ordinary)
					require.NoError(t, err)
					_, err = s.Lock(ctx, table, [][]byte{{9}}, blocker, ordinary)
					require.NoError(t, err)
					exact := ordinary
					exact.KeepRows = true
					rows := [][]byte{{9}}
					if name == "cancel-partial" {
						rows = [][]byte{{7}, {9}}
					}
					requestCtx, stop := context.WithCancel(ctx)
					done := make(chan error, 1)
					go func() { _, err := s.Lock(requestCtx, table, rows, id, exact); done <- err }()
					defer func() { stop(); <-done }()
					waitWaiters(t, s, table, []byte{9}, 1)
					if name == "coarsening-wins" {
						_, err = s.Lock(ctx, table, [][]byte{{5}}, id, ordinary)
						require.NoError(t, err)
						require.NoError(t, s.Unlock(ctx, blocker, timestamp.Timestamp{}))
					} else {
						stop()
					}
					select {
					case err = <-done:
						done <- err // Leave one terminal receipt for unconditional cleanup/join.
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if name == "coarsening-wins" {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade), "%v", err)
					} else {
						require.ErrorIs(t, err, context.Canceled)
						require.NoError(t, s.Unlock(ctx, blocker, timestamp.Timestamp{}))
						_, err = s.Lock(ctx, table, [][]byte{{5}}, id, ordinary)
						require.NoError(t, err)
					}
					gap := ordinary
					gap.Policy = pb.WaitPolicy_FastFail
					_, err = s.Lock(ctx, table, [][]byte{{2}}, probe, gap)
					if name == "cancel-partial" {
						require.NoError(t, err)
					} else {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockConflict), "%v", err)
					}
				})
			}
		}, func(cfg *Config) { cfg.MaxLockRowCount = 2 })
}

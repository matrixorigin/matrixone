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

package lockservice

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// Cleanup must observe retirement, not turn the closing origin into a new
// lock owner. A subsequent caller must be able to allocate the next binding.
func TestRemoteUnlockDoesNotAllocateReplacementBinding(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(t, []string{"cleanup-owner", "cleanup-origin", "cleanup-survivor"}, time.Hour,
		func(allocator *lockTableAllocator, services []*service) {
			owner, origin, survivor := services[0], services[1], services[2]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			const table = uint64(42)
			previous := allocator.Get(owner.serviceID, 0, table, table, pb.Sharding_None)
			require.True(t, previous.Valid)
			txn := []byte("closing-origin")
			_, err := origin.Lock(ctx, table, [][]byte{{1}}, txn, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, owner.Close())
			allocator.disableTableBinds(allocator.getServiceBinds(owner.serviceID))

			require.NoError(t, origin.UnlockWithContext(ctx, txn, timestamp.Timestamp{}))
			allocator.mu.RLock()
			afterCleanup := allocator.mu.lockTables[0][table]
			allocator.mu.RUnlock()
			require.False(t, afterCleanup.Valid, "transaction cleanup must not allocate a replacement owner: %s", afterCleanup.DebugString())
			require.Nil(t, origin.tableGroups.get(0, table), "cleanup must not publish a fresh physical table")

			nextTxn := []byte("survivor-lock")
			_, err = survivor.Lock(ctx, table, [][]byte{{1}}, nextTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, survivor.UnlockWithContext(ctx, nextTxn, timestamp.Timestamp{}))
			allocator.mu.RLock()
			afterLock := allocator.mu.lockTables[0][table]
			allocator.mu.RUnlock()
			require.Equal(t, survivor.serviceID, afterLock.ServiceID)
			require.True(t, afterLock.Valid)
		}, func(cfg *Config) {
			cfg.KeepBindDuration.Duration = time.Hour
			cfg.KeepRemoteLockDuration.Duration = time.Hour
		})
}

func TestAllocatorBindLookupHasNoOwnershipOrLivenessSideEffects(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		const group, table = uint32(7), uint64(42)
		before := a.Get("owner", group, table, table, pb.Sharding_None)
		requester := a.registerService("requester")
		requester.Lock()
		requester.lastKeepaliveTime = time.Unix(1, 0)
		generation := requester.keepaliveGeneration
		requester.Unlock()
		request := pb.GetBindRequest{ServiceID: "requester", Group: group, Table: table,
			OriginTable: table, LookupOnly: true}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, _, err := requestLockTableBind(ctx, a.client, request)
		require.NoError(t, err)
		require.Equal(t, before, got)
		requester.RLock()
		afterKeepalive, afterGeneration := requester.lastKeepaliveTime, requester.keepaliveGeneration
		requester.RUnlock()
		require.Equal(t, time.Unix(1, 0), afterKeepalive)
		require.Equal(t, generation, afterGeneration)

		// Even a requester whose admission was retired must be able to finish
		// cleanup, without recreating its service registration.
		a.disableTableBinds(requester)
		a.disableTableBinds(a.getServiceBinds("owner"))
		got, _, err = requestLockTableBind(ctx, a.client, request)
		require.NoError(t, err)
		require.False(t, got.Valid)
		require.True(t, before.Changed(got))
		require.Nil(t, a.getServiceBinds("requester"))
		a.mu.RLock()
		validAfterLookup := a.mu.lockTables[group][table].Valid
		a.mu.RUnlock()
		require.False(t, validAfterLookup)

		request.Group++
		got, _, err = requestLockTableBind(ctx, a.client, request)
		require.NoError(t, err)
		require.False(t, got.Valid)
		a.mu.RLock()
		_, created := a.mu.lockTables[request.Group]
		a.mu.RUnlock()
		require.False(t, created, "lookup must not even create an absent group map")
		next := a.Get("survivor", group, table, table, pb.Sharding_None)
		require.True(t, next.Valid)
		require.Equal(t, "survivor", next.ServiceID)
	})
}

func TestCleanupBindLookupRequiresAcknowledgementForRetirement(t *testing.T) {
	for _, tc := range []struct {
		name                                                      string
		ack, valid, wrongTable, wrongGroup, emptyOwner, wantError bool
	}{
		{name: "retired owner", ack: true},
		{name: "valid owner", ack: true, valid: true},
		{name: "legacy valid fallback", valid: true},
		{name: "legacy absence is not evidence", wantError: true},
		{name: "wrong table", ack: true, wrongTable: true, wantError: true},
		{name: "wrong group", ack: true, wrongGroup: true, wantError: true},
		{name: "valid flag without owner", ack: true, valid: true, emptyOwner: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := pb.GetBindRequest{ServiceID: "origin", Group: 2, Table: 42, OriginTable: 42, LookupOnly: true}
			client := &bindResponseClient{send: func(_ context.Context, got *pb.Request) (*pb.Response, error) {
				require.Equal(t, req, got.GetBind)
				resp := acquireResponse()
				resp.GetBind.LookupOnly = tc.ack
				resp.GetBind.AllocatorID, resp.GetBind.AllocatorVersion = "allocator", 1
				resp.GetBind.LockTable = pb.LockTable{Group: req.Group, Table: req.Table, Valid: tc.valid}
				if tc.valid && !tc.emptyOwner {
					resp.GetBind.LockTable.ServiceID = "owner"
				}
				if tc.wrongTable {
					resp.GetBind.LockTable.Table++
				}
				if tc.wrongGroup {
					resp.GetBind.LockTable.Group++
				}
				return resp, nil
			}}
			got, epoch, err := requestLockTableBind(context.Background(), client, req)
			if tc.wantError {
				require.ErrorIs(t, err, ErrLockTableBindChanged)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.valid, got.Valid)
				require.Equal(t, allocatorState{id: "allocator", version: 1}, epoch)
			}
		})
	}
}

func TestCleanupBindLookupRetainsLiveOwnerUntilUnlockCompletes(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(t, []string{"live-owner", "cleanup-origin"}, time.Hour,
		func(a *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			const table = uint64(42)
			before := a.Get(owner.serviceID, 0, table, table, pb.Sharding_None)
			// Fail only the first transport attempt. The real allocator still
			// reports a live owner, so cleanup must retry that owner and release its
			// transaction ledger rather than treating a network failure as absence.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			txn := []byte("live-owner-cleanup")
			_, err := origin.Lock(ctx, table, [][]byte{{1}}, txn, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.True(t, owner.activeTxnHolder.hasActiveTxn(txn))
			client := &failOnceSendClient{Client: origin.remote.client, method: pb.Method_Unlock, err: syscall.EPIPE}
			remote := newRemoteLockTable(origin.serviceID, time.Second, before, client,
				func(pb.LockTable) { t.Fatal("live owner must not be replaced") }, getLogger(""))
			cleanupTxn := newActiveTxn(txn, string(txn), newFixedSlicePool(8), "")
			defer reuse.Free(cleanupTxn, nil)
			require.NoError(t, remote.unlockWithContext(ctx, cleanupTxn, nil, timestamp.Timestamp{}))
			require.False(t, owner.activeTxnHolder.hasActiveTxn(txn), "a live owner's cleanup must actually complete")
			require.NoError(t, origin.UnlockWithContext(ctx, txn, timestamp.Timestamp{}))
			require.Equal(t, before, a.lookupBind(pb.GetBindRequest{Group: 0, Table: table}))
		}, func(cfg *Config) {
			cfg.KeepBindDuration.Duration = time.Hour
			cfg.KeepRemoteLockDuration.Duration = time.Hour
		})
}

func TestClosedServiceRetiresBindingsBeforeReturning(t *testing.T) {
	type reportState struct {
		method                     pb.Method
		sealed, tableGone, txnGone bool
	}
	reported := make(chan reportState, 8)
	txn := []byte("owner-transaction")
	const table = uint64(42)
	runLockServiceTestsWithLevel(t, zapcore.DebugLevel, []string{"closing-owner", "survivor"}, time.Hour,
		func(a *lockTableAllocator, services []*service) {
			owner, survivor := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := owner.Lock(ctx, table, [][]byte{{1}}, txn, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.True(t, a.lookupBind(pb.GetBindRequest{Table: table}).Valid)
			require.NoError(t, owner.Close())
			for _, method := range []pb.Method{pb.Method_BeginDrain, pb.Method_KeepLockTableBind} {
				select {
				case got := <-reported:
					require.Equal(t, method, got.method)
					require.True(t, got.sealed)
					require.True(t, got.tableGone, "report must follow physical-table destruction")
					require.True(t, got.txnGone, "report must follow transaction-holder destruction")
				default:
					t.Fatal("completed Close did not report allocator retirement")
				}
			}
			require.False(t, a.lookupBind(pb.GetBindRequest{Table: table}).Valid)
			_, _, err = getLockTableBindWithContext(ctx, survivor.remote.client, 0, table, table, owner.serviceID, pb.Sharding_None)
			require.Error(t, err, "late requests must not resurrect a closed incarnation")
			_, err = survivor.Lock(ctx, table, [][]byte{{1}}, []byte("survivor-transaction"), newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, survivor.Unlock(ctx, []byte("survivor-transaction"), timestamp.Timestamp{}))
			require.NoError(t, owner.Close())
			select {
			case <-reported:
				t.Fatal("duplicate Close repeated the allocator report")
			default:
			}
		}, func(cfg *Config) {
			cfg.KeepBindDuration.Duration = time.Hour
			cfg.KeepRemoteLockDuration.Duration = time.Hour
		}, func(s *service) {
			if s.cfg.ServiceID != "closing-owner" {
				return
			}
			s.cfg.RPC.BackendOptions = append(s.cfg.RPC.BackendOptions, morpc.WithBackendFilter(func(message morpc.Message, _ string) bool {
				req, ok := message.(*pb.Request)
				if !ok || (req.Method != pb.Method_BeginDrain &&
					!(req.Method == pb.Method_KeepLockTableBind && req.KeepLockTableBind.Status == pb.Status_ServiceUnLockSucc)) {
					return true
				}
				s.lifecycle.RLock()
				sealed := s.lifecycle.closing
				s.lifecycle.RUnlock()
				select {
				case reported <- reportState{method: req.Method, sealed: sealed,
					tableGone: s.tableGroups.get(0, table) == nil, txnGone: !s.activeTxnHolder.hasActiveTxn(txn)}:
				default:
				}
				return true
			}))
		})
}

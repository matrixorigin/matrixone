// Copyright 2023 Matrix Origin
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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestForwardLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			_, err := l2.getLockTableWithCreate(0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)

			txn1 := []byte("txn1")
			row1 := []byte{1}

			_, err = l1.Lock(ctx, tableID, [][]byte{row1}, txn1, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				ForwardTo:   "s2",
			})
			require.NoError(t, err)

			txn := l2.activeTxnHolder.getActiveTxn(txn1, false, "")
			require.NotNil(t, txn)
			require.Equal(t, l1.serviceID, txn.remoteService)
			require.True(t, l2.activeTxnHolder.hasRemoteLockBind(l1.serviceID, l2.tableGroups.get(0, tableID).getBind(), time.Second))
		},
	)
}

func TestForwardLockOwnerWaitTimeoutFallbackReleasesPartialRemoteLock(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			origin := s[0]
			owner := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			_, err := owner.getLockTableWithCreate(0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)

			row1 := []byte{1}
			row2 := []byte{2}
			holderTxn := []byte("holder")
			originTxn := []byte("origin")
			verifyTxn := []byte("verify")
			mustAddTestLock(t, ctx, owner, tableID, holderTxn, [][]byte{row2}, pb.Granularity_Row)

			opt := newTestRowExclusiveOptions()
			opt.ForwardTo = owner.serviceID
			_, err = origin.Lock(ctx, tableID, [][]byte{row1, row2}, originTxn, opt)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected), "got %v", err)

			// An old origin treats the v1-compatible error as whole-transaction
			// rollback, and its Unlock must release the partial grant on owner.
			require.NoError(t, origin.Unlock(ctx, originTxn, timestamp.Timestamp{}))
			_, err = owner.Lock(ctx, tableID, [][]byte{row1}, verifyTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, verifyTxn, timestamp.Timestamp{}))
			require.NoError(t, owner.Unlock(ctx, holderTxn, timestamp.Timestamp{}))
		},
		func(cfg *Config) {
			cfg.RemoteLockOwnerWaitTimeout = &toml.Duration{Duration: 100 * time.Millisecond}
		},
	)
}

func TestDeadLockWithForward(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			row2 := []byte{2}

			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			_, err := l1.Lock(ctx, 1, [][]byte{row2}, txn2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				ForwardTo:   "s2",
			})
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l1, 1, txn1, [][]byte{row2},
					pb.Granularity_Row)
				require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			}()
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l2, 1, txn2, [][]byte{row1},
					pb.Granularity_Row)
				require.NoError(t, l2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			}()
			wg.Wait()
		},
	)
}

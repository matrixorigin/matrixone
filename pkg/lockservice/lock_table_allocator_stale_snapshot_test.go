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
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestCleanerDoesNotDeleteTombstoneCreatedAfterServiceSnapshot(
	t *testing.T,
) {
	firstService := make(chan string, 1)
	secondQueryEntered := make(chan struct{})
	releaseSecondQuery := make(chan struct{})
	releaseLaterCycles := make(chan struct{})

	var queryCalls atomic.Int32

	runLockTableAllocatorTest(
		t,
		20*time.Millisecond,
		func(a *lockTableAllocator) {
			// Stop subsequent cleaner rounds from interfering with the
			// assertions for the first round.
			defer close(releaseLaterCycles)

			// Ensure both services are included in the cleaner snapshot.
			a.getCtl("s1")
			a.getCtl("s2")

			targetService := <-firstService
			<-secondQueryEntered

			otherService := "s1"
			if targetService == otherService {
				otherService = "s2"
			}

			txnID := []byte("txn-after-snapshot")

			// The cleaner has already obtained targetService's active-txn
			// snapshot and is blocked while querying the other service.
			// Create a new tombstone after that snapshot.
			committing := a.AddCannotCommit([]pb.OrphanTxn{{
				Service: targetService,
				Txn:     [][]byte{txnID},
			}})
			require.Empty(t, committing)

			targetCtl := a.getCtl(targetService)
			state, ok := targetCtl.getCommitState(string(txnID))
			require.True(t, ok)
			require.Equal(t, cannotCommitState, state.state)

			// Let the cleaner finish collecting snapshots and apply them.
			close(releaseSecondQuery)

			// Removal of the empty control state for the other service proves
			// that the first cleanup round has been applied.
			require.Eventually(t, func() bool {
				_, exists := a.ctl.Load(otherService)
				return !exists
			}, time.Second, time.Millisecond)

			// A snapshot obtained before this tombstone was created must not
			// be allowed to delete it.
			state, ok = targetCtl.getCommitState(string(txnID))
			require.True(
				t,
				ok,
				"a stale active-txn snapshot deleted a newer "+
					"cannot-commit tombstone",
			)
			require.Equal(t, cannotCommitState, state.state)
		},
		func(a *lockTableAllocator) {
			a.options.getActiveTxnFunc = func(
				serviceID string,
			) (bool, [][]byte, error) {
				switch queryCalls.Add(1) {
				case 1:
					// The first service snapshot is now complete.
					firstService <- serviceID
					return true, nil, nil

				case 2:
					// Hold the cleaner between collecting the first
					// service snapshot and applying all snapshots.
					close(secondQueryEntered)
					<-releaseSecondQuery
					return true, nil, nil

				default:
					// Prevent a later cleaner round from legitimately
					// removing the tombstone before the assertion.
					<-releaseLaterCycles
					return true, nil, nil
				}
			}
		})
}

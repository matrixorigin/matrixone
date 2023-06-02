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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetWaitingList(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn2"),
					option)
				require.NoError(t, err)
				assert.Equal(t, timestamp.Timestamp{PhysicalTime: 1}, res.Timestamp)
			}()

			waitWaiters(t, l1, 0, []byte{1}, 1)

			ok, waiters, err := l1.GetWaitingList(ctx, []byte("txn1"))
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, 1, len(waiters))

			ok, waiters, err = l2.GetWaitingList(ctx, []byte("txn1"))
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Equal(t, 0, len(waiters))

			require.NoError(t, l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{PhysicalTime: 1}))
			wg.Wait()
		},
	)
}

func TestForceRefreshLockTableBinds(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			l1.ForceRefreshLockTableBinds()
			l2.ForceRefreshLockTableBinds()

			mustAddTestLock(
				t,
				ctx,
				l1,
				table,
				[]byte("txn1"),
				[][]byte{{1}},
				pb.Granularity_Row,
			)

			mustAddTestLock(
				t,
				ctx,
				l2,
				table,
				[]byte("txn2"),
				[][]byte{{2}},
				pb.Granularity_Row,
			)
		},
	)
}

func TestGetLockTableBind(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			bind1, err := l1.GetLockTableBind(table)
			require.NoError(t, err)

			bind2, err := l2.GetLockTableBind(table)
			require.NoError(t, err)

			assert.Equal(t, bind1, bind2)
		},
	)
}

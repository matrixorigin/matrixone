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
	"github.com/stretchr/testify/require"
)

func TestForwardLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			row1 := []byte{1}

			_, err := l1.Lock(ctx, 1, [][]byte{row1}, txn1, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				ForwardTo:   "s2",
			})
			require.NoError(t, err)

			require.NotNil(t, l2.activeTxnHolder.getActiveTxn(txn1, false, ""))
			require.NotNil(t, l2.activeTxnHolder.getActiveTxn(txn1, false, ""))
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

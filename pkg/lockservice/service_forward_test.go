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

			require.Nil(t, l1.activeTxnHolder.getActiveTxn(txn1, false, ""))
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

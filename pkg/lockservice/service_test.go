// Copyright 2022 Matrix Origin
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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowLock(t *testing.T) {
	l := NewLockService()
	ctx := context.Background()
	option := LockOptions{
		granularity: Row,
		mode:        Exclusive,
		policy:      Wait,
	}
	acquired := false

	ok, err := l.Lock(context.Background(), 0, [][]byte{{1}}, []byte{1}, option)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)
	go func() {
		ok, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte{2}, option)
		assert.NoError(t, err)
		assert.Equal(t, true, ok)
		acquired = true
		err = l.Unlock([]byte{2})
		assert.NoError(t, err)
	}()
	time.Sleep(time.Second)
	err = l.Unlock([]byte{1})
	assert.NoError(t, err)
	time.Sleep(time.Second)
	ok, err = l.Lock(context.Background(), 0, [][]byte{{1}}, []byte{3}, option)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)

	assert.Equal(t, true, acquired)

	err = l.Unlock([]byte{3})
	assert.NoError(t, err)
}

func TestMultipleRowLocks(t *testing.T) {
	l := NewLockService()
	ctx := context.Background()
	option := LockOptions{
		granularity: Row,
		mode:        Exclusive,
		policy:      Wait,
	}
	iter := 0
	sum := 1000
	var wg sync.WaitGroup

	for i := 0; i < sum; i++ {
		wg.Add(1)
		go func(i int) {
			ok, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte(strconv.Itoa(i)), option)
			assert.NoError(t, err)
			assert.Equal(t, true, ok)
			iter++
			err = l.Unlock([]byte(strconv.Itoa(i)))
			assert.NoError(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, sum, iter)
}

func TestDeadLock(t *testing.T) {
	l := NewLockService().(*service)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	txn1 := []byte("txn1")
	txn2 := []byte("txn2")
	txn3 := []byte("txn3")
	row1 := []byte{1}
	row2 := []byte{2}
	row3 := []byte{3}

	mustAddTestLock(t, ctx, l, txn1, [][]byte{row1}, Row)
	mustAddTestLock(t, ctx, l, txn2, [][]byte{row2}, Row)
	mustAddTestLock(t, ctx, l, txn3, [][]byte{row3}, Row)

	var wg sync.WaitGroup
	wg.Add(3)
	maxDeadLockCount := uint32(1)
	deadLockCounter := atomic.Uint32{}
	go func() {
		defer wg.Done()
		maybeAddTestLockWithDeadlock(t, ctx, l, txn1, [][]byte{row2}, Row, &deadLockCounter, maxDeadLockCount)
		require.NoError(t, l.Unlock(txn1))
	}()
	go func() {
		defer wg.Done()
		maybeAddTestLockWithDeadlock(t, ctx, l, txn2, [][]byte{row3}, Row, &deadLockCounter, maxDeadLockCount)
		require.NoError(t, l.Unlock(txn2))
	}()
	go func() {
		defer wg.Done()
		maybeAddTestLockWithDeadlock(t, ctx, l, txn3, [][]byte{row1}, Row, &deadLockCounter, maxDeadLockCount)
		require.NoError(t, l.Unlock(txn3))
	}()
	wg.Wait()
}

func TestDeadLockWithRange(t *testing.T) {
	l := NewLockService().(*service)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	txn1 := []byte("txn1")
	txn2 := []byte("txn2")
	txn3 := []byte("txn3")
	row1 := []byte{1, 2}
	row2 := []byte{3, 4}
	row3 := []byte{5, 6}

	mustAddTestLock(t, ctx, l, txn1, [][]byte{row1}, Range)
	mustAddTestLock(t, ctx, l, txn2, [][]byte{row2}, Range)
	mustAddTestLock(t, ctx, l, txn3, [][]byte{row3}, Range)

	var wg sync.WaitGroup
	wg.Add(3)
	maxDeadLockCount := uint32(1)
	var deadLockCounter atomic.Uint32
	go func() {
		defer wg.Done()
		maybeAddTestLockWithDeadlock(t, ctx, l, txn1, [][]byte{row2}, Range, &deadLockCounter, maxDeadLockCount)
		require.NoError(t, l.Unlock(txn1))
	}()
	go func() {
		defer wg.Done()
		maybeAddTestLockWithDeadlock(t, ctx, l, txn2, [][]byte{row3}, Range, &deadLockCounter, maxDeadLockCount)
		require.NoError(t, l.Unlock(txn2))
	}()
	go func() {
		defer wg.Done()
		maybeAddTestLockWithDeadlock(t, ctx, l, txn3, [][]byte{row1}, Range, &deadLockCounter, maxDeadLockCount)
		require.NoError(t, l.Unlock(txn3))
	}()
	wg.Wait()
}

func mustAddTestLock(t *testing.T,
	ctx context.Context,
	l *service,
	txnID []byte,
	lock [][]byte,
	granularity Granularity) {
	maybeAddTestLockWithDeadlock(t,
		ctx,
		l,
		txnID,
		lock,
		granularity,
		nil,
		0)
}

func maybeAddTestLockWithDeadlock(t *testing.T,
	ctx context.Context,
	l *service,
	txnID []byte,
	lock [][]byte,
	granularity Granularity,
	deadLockCount *atomic.Uint32,
	maxDeadLockCount uint32) {
	t.Logf("%s try lock %+v", string(txnID), lock)
	ok, err := l.Lock(ctx, 1, lock, txnID, LockOptions{
		granularity: Row,
		mode:        Exclusive,
		policy:      Wait,
	})
	if err == ErrDeadlockDetectorClosed {
		t.Logf("%s lock %+v, found dead lock", string(txnID), lock)
		require.True(t, maxDeadLockCount >= deadLockCount.Add(1))
		require.False(t, ok)
		return
	}
	t.Logf("%s lock %+v, ok", string(txnID), lock)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRangeLock(t *testing.T) {
	l := NewLockService()
	ctx := context.Background()
	option := LockOptions{
		granularity: Range,
		mode:        Exclusive,
		policy:      Wait,
	}
	acquired := false

	ok, err := l.Lock(context.Background(), 0, [][]byte{{1}, {2}}, []byte{1}, option)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)
	go func() {
		ok, err := l.Lock(ctx, 0, [][]byte{{1}, {2}}, []byte{2}, option)
		assert.NoError(t, err)
		assert.Equal(t, true, ok)
		acquired = true
		err = l.Unlock([]byte{2})
		assert.NoError(t, err)
	}()
	time.Sleep(time.Second)
	err = l.Unlock([]byte{1})
	assert.NoError(t, err)
	time.Sleep(time.Second)
	ok, err = l.Lock(context.Background(), 0, [][]byte{{1}, {2}}, []byte{3}, option)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)

	assert.Equal(t, true, acquired)

	err = l.Unlock([]byte{3})
	assert.NoError(t, err)
}

func TestMultipleRangeLocks(t *testing.T) {
	l := NewLockService()
	ctx := context.Background()
	option := LockOptions{
		granularity: Range,
		mode:        Exclusive,
		policy:      Wait,
	}

	sum := 100
	var wg sync.WaitGroup
	for i := 0; i < sum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			start := i % 10
			if start == 9 {
				return
			}
			end := (i + 1) % 10
			ok, err := l.Lock(ctx, 0, [][]byte{{byte(start)}, {byte(end)}}, []byte(strconv.Itoa(i)), option)
			assert.NoError(t, err)
			assert.Equal(t, true, ok)
			err = l.Unlock([]byte(strconv.Itoa(i)))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

func BenchmarkMultipleRowLock(b *testing.B) {
	l := NewLockService()
	ctx := context.Background()
	option := LockOptions{
		granularity: Row,
		mode:        Exclusive,
		policy:      Wait,
	}
	iter := 0

	b.Run("lock-service", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			go func(i int) {
				l.Lock(ctx, 0, [][]byte{{1}}, []byte{byte(i)}, option)
				iter++
				l.Unlock([]byte{byte(i)})
			}(i)
		}
	})
}

func BenchmarkWithoutConflict(b *testing.B) {
	runBenchmark(b, "1-table", 1)
	runBenchmark(b, "unlimited-table", 32)
}

var tableID atomic.Uint64
var txnID atomic.Uint64
var rowID atomic.Uint64

func runBenchmark(b *testing.B, name string, t uint64) {
	b.Run(name, func(b *testing.B) {
		l := NewLockService()
		getTableID := func() uint64 {
			if t == 1 {
				return 0
			}
			return tableID.Add(1)
		}

		// total p goroutines to run test
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(p *testing.PB) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			row := [][]byte{buf.Uint64ToBytes(rowID.Add(1))}
			txn := buf.Uint64ToBytes(txnID.Add(1))
			table := getTableID()
			// fmt.Printf("on table %d\n", table)
			for p.Next() {
				if _, err := l.Lock(ctx, table, row, txn, LockOptions{}); err != nil {
					panic(err)
				}
				if err := l.Unlock(txn); err != nil {
					panic(err)
				}
			}
		})
	})

}

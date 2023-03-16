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
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestRowLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx := context.Background()
			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}
			acquired := false

			_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte{1}, option)
			assert.NoError(t, err)
			go func() {
				_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte{2}, option)
				assert.NoError(t, err)
				acquired = true
				err = l.Unlock(ctx, []byte{2})
				assert.NoError(t, err)
			}()
			time.Sleep(time.Second / 2)
			err = l.Unlock(ctx, []byte{1})
			assert.NoError(t, err)
			time.Sleep(time.Second / 2)
			_, err = l.Lock(ctx, 0, [][]byte{{1}}, []byte{3}, option)
			assert.NoError(t, err)
			assert.True(t, acquired)

			err = l.Unlock(ctx, []byte{3})
			assert.NoError(t, err)
		},
	)
}

func TestRowLockWithMany(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx := context.Background()
			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}
			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}, {2}, {3}, {4}, {5}, {6}},
				[]byte("txn1"),
				option)
			assert.NoError(t, err)
			lt, _ := l.getLockTable(0)
			assert.Equal(t, 6, lt.(*localLockTable).mu.store.Len())
		},
	)
}

func TestMultipleRowLocks(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx := context.Background()
			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}
			iter := 0
			sum := 100
			var wg sync.WaitGroup

			for i := 0; i < sum; i++ {
				wg.Add(1)
				go func(i int) {
					_, err := l.Lock(ctx, 0, [][]byte{{1}, {2}, {3}, {4}, {5}, {6}}, []byte(strconv.Itoa(i)), option)
					assert.NoError(t, err)
					iter++
					err = l.Unlock(ctx, []byte(strconv.Itoa(i)))
					assert.NoError(t, err)
					wg.Done()
				}(i)
			}
			wg.Wait()
			assert.Equal(t, sum, iter)
		},
	)
}

func TestCtxCancelWhileWaiting(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithCancel(context.Background())
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}
			var wg sync.WaitGroup
			wg.Add(1)
			_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn1"), option)
			assert.NoError(t, err)
			go func(ctx context.Context) {
				_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn2"), option)
				assert.Error(t, err)
				wg.Done()
			}(ctx)
			cancel()
			wg.Wait()
			assert.NoError(t, l.Unlock(ctx, []byte(strconv.Itoa(1))))
		},
	)
}

func TestDeadLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			txn3 := []byte("txn3")
			row1 := []byte{1}
			row2 := []byte{2}
			row3 := []byte{3}

			mustAddTestLock(t, ctx, l, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			mustAddTestLock(t, ctx, l, 1, txn2, [][]byte{row2}, pb.Granularity_Row)
			mustAddTestLock(t, ctx, l, 1, txn3, [][]byte{row3}, pb.Granularity_Row)

			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l, 1, txn1, [][]byte{row2},
					pb.Granularity_Row)
				require.NoError(t, l.Unlock(ctx, txn1))
			}()
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l, 1, txn2, [][]byte{row3},
					pb.Granularity_Row)
				require.NoError(t, l.Unlock(ctx, txn2))
			}()
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l, 1, txn3, [][]byte{row1},
					pb.Granularity_Row)
				require.NoError(t, l.Unlock(ctx, txn3))
			}()
			wg.Wait()
		},
	)
}

func TestDeadLockWithRange(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			txn3 := []byte("txn3")
			row1 := []byte{1, 2}
			row2 := []byte{3, 4}
			row3 := []byte{5, 6}

			mustAddTestLock(t, ctx, l, 1, txn1, [][]byte{row1}, pb.Granularity_Range)
			mustAddTestLock(t, ctx, l, 1, txn2, [][]byte{row2}, pb.Granularity_Range)
			mustAddTestLock(t, ctx, l, 1, txn3, [][]byte{row3}, pb.Granularity_Range)

			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l, 1, txn1, [][]byte{row2},
					pb.Granularity_Range)
				require.NoError(t, l.Unlock(ctx, txn1))
			}()
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l, 1, txn2, [][]byte{row3},
					pb.Granularity_Range)
				require.NoError(t, l.Unlock(ctx, txn2))
			}()
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l, 1, txn3, [][]byte{row1},
					pb.Granularity_Range)
				require.NoError(t, l.Unlock(ctx, txn3))
			}()
			wg.Wait()
		},
	)
}

func TestRowLockWithSameTxn(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx := context.Background()
			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			for i := 0; i < 10; i++ {
				_, err := l.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn1"),
					option)
				assert.NoError(t, err)
				lt, _ := l.getLockTable(0)
				assert.Equal(t, 1, lt.(*localLockTable).mu.store.Len())
			}
		},
	)
}

func mustAddTestLock(t *testing.T,
	ctx context.Context,
	l *service,
	table uint64,
	txnID []byte,
	lock [][]byte,
	granularity pb.Granularity) {
	maybeAddTestLockWithDeadlock(t,
		ctx,
		l,
		table,
		txnID,
		lock,
		granularity)
}

func TestRangeLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx := context.Background()
			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}
			acquired := false

			_, err := l.Lock(context.Background(), 0, [][]byte{{1}, {2}}, []byte{1}, option)
			assert.NoError(t, err)
			go func() {
				_, err := l.Lock(ctx, 0, [][]byte{{1}, {2}}, []byte{2}, option)
				assert.NoError(t, err)
				acquired = true
				err = l.Unlock(ctx, []byte{2})
				assert.NoError(t, err)
			}()
			time.Sleep(time.Second / 2)
			err = l.Unlock(ctx, []byte{1})
			assert.NoError(t, err)
			time.Sleep(time.Second / 2)
			_, err = l.Lock(context.Background(), 0, [][]byte{{1}, {2}}, []byte{3}, option)
			assert.NoError(t, err)
			assert.True(t, acquired)

			err = l.Unlock(ctx, []byte{3})
			assert.NoError(t, err)
		},
	)
}

func TestRangeLockWithMany(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx := context.Background()
			option := LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}
			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}, {2}, {3}, {4}, {5}, {6}},
				[]byte("txn1"),
				option)
			assert.NoError(t, err)
			lt, _ := l.getLockTable(0)
			assert.Equal(t, 6, lt.(*localLockTable).mu.store.Len())

		},
	)
}

func TestMultipleRangeLocks(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			option := LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
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
					_, err := l.Lock(ctx, 0, [][]byte{{byte(start)}, {byte(end)}}, []byte(strconv.Itoa(i)), option)
					require.NoError(t, err, hex.EncodeToString([]byte(strconv.Itoa(i))))
					err = l.Unlock(ctx, []byte(strconv.Itoa(i)))
					require.NoError(t, err)
				}(i)
			}
			wg.Wait()
		},
	)
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
		runLockServiceTestsWithLevel(
			b,
			zapcore.FatalLevel,
			[]string{"s1"},
			time.Second*10,
			func(alloc *lockTableAllocator, s []*service) {
				l := s[0]
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
						if err := l.Unlock(ctx, txn); err != nil {
							panic(err)
						}
					}
				})
			},
			nil,
		)
	})
}

func maybeAddTestLockWithDeadlock(t *testing.T,
	ctx context.Context,
	l *service,
	table uint64,
	txnID []byte,
	lock [][]byte,
	granularity pb.Granularity) {
	t.Logf("%s try lock %+v", string(txnID), lock)
	_, err := l.Lock(ctx, table, lock, txnID, LockOptions{
		Granularity: pb.Granularity_Row,
		Mode:        pb.LockMode_Exclusive,
		Policy:      pb.WaitPolicy_Wait,
	})

	if moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected) {
		t.Logf("%s lock %+v, found dead lock", string(txnID), lock)
		return
	}
	t.Logf("%s lock %+v, ok", string(txnID), lock)
	require.NoError(t, err)
}

func runLockServiceTests(
	t assert.TestingT,
	serviceIDs []string,
	fn func(*lockTableAllocator, []*service)) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		serviceIDs,
		time.Second*10,
		fn,
		nil)
}

func runLockServiceTestsWithAdjustConfig(
	t assert.TestingT,
	serviceIDs []string,
	lockTableBindTimeout time.Duration,
	fn func(*lockTableAllocator, []*service),
	adjustConfig func(*Config)) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		serviceIDs,
		lockTableBindTimeout,
		fn,
		adjustConfig)
}

func runLockServiceTestsWithLevel(
	t assert.TestingT,
	level zapcore.Level,
	serviceIDs []string,
	lockTableBindTimeout time.Duration,
	fn func(*lockTableAllocator, []*service),
	adjustConfig func(*Config)) {
	defaultRPCTimeout = time.Second
	defer leaktest.AfterTest(t.(testing.TB))()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntimeWithLevel(level))

	assert.NoError(t, os.RemoveAll(testSockets[:7]))

	allocator := NewLockTableAllocator(testSockets, lockTableBindTimeout, morpc.Config{})
	defer func() {
		assert.NoError(t, allocator.Close())
	}()

	services := make([]*service, 0, len(serviceIDs))
	defer func() {
		for _, s := range services {
			assert.NoError(t, s.Close())
		}
	}()

	cns := make([]metadata.CNService, 0, len(serviceIDs))
	configs := make([]Config, 0, len(serviceIDs))
	for _, v := range serviceIDs {
		address := fmt.Sprintf("unix:///tmp/service-%s.sock", v)
		assert.NoError(t, os.RemoveAll(address[7:]))
		cns = append(cns, metadata.CNService{
			ServiceID:          v,
			LockServiceAddress: address,
		})
		configs = append(configs, Config{ServiceID: v, ServiceAddress: address})
	}
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			cns,
			[]metadata.DNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	for _, cfg := range configs {
		if adjustConfig != nil {
			adjustConfig(&cfg)
		}
		services = append(services,
			NewLockService(cfg).(*service))
	}
	fn(allocator.(*lockTableAllocator), services)
}

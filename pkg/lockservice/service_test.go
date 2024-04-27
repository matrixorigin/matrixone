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
	"hash/crc32"
	"hash/crc64"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

var (
	runners = map[string]func(t *testing.T, table uint64, fn func(context.Context, *service, *localLockTable)){
		"local":  getRunner(false),
		"remote": getRunner(true),
	}
)

func getRunner(remote bool) func(t *testing.T, table uint64, fn func(context.Context, *service, *localLockTable)) {
	return func(
		t *testing.T,
		table uint64,
		fn func(context.Context, *service, *localLockTable)) {
		runLockServiceTests(
			t,
			[]string{"s1", "s2"},
			func(alloc *lockTableAllocator, ss []*service) {
				s1 := ss[0]
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
				defer cancel()

				option := newTestRowExclusiveOptions()
				rows := newTestRows(1)
				txn1 := newTestTxnID(1)
				_, err := s1.Lock(ctx, table, rows, txn1, option)
				require.NoError(t, err, err)
				require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

				lt, err := s1.getLockTable(0, table)
				require.NoError(t, err)
				require.Equal(t, table, lt.getBind().Table)
				require.Equal(t, table, lt.getBind().OriginTable)

				target := s1
				if remote {
					target = ss[1]
				}

				fn(ctx, target, lt.(*localLockTable))
			})
	}
}

func TestRowLock(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()

					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)
				})
		})
	}
}

func TestRowLockWithSharedAndExclusive(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					sharedOptions := newTestRowSharedOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					// txn1 hold the lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					// txn2 blocked by txn1
					c := make(chan struct{})
					go func() {
						defer close(c)

						_, err := s.Lock(ctx, table, rows, txn2, sharedOptions)
						require.NoError(t, err)
						defer func() {
							assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						}()

						checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					<-c
				})
		})
	}
}

func TestRowLockWithSharedAndShared(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowSharedOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
						checkLock(t, lt, rows[0], nil, nil, nil)
					}()
					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)

					_, err = s.Lock(ctx, table, rows, txn2, option)
					require.NoError(t, err)
					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)
					}()
					checkLock(t, lt, rows[0], [][]byte{txn1, txn2}, nil, nil)
				})
		})
	}
}

func TestRangeLock(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()

					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)
				})
		})
	}
}

func TestRangeLockWithSharedAndExclusive(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeExclusiveOptions()
					sharedOptions := newTestRangeSharedOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					// txn1 hold the lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					// txn2 blocked by txn1
					c := make(chan struct{})
					go func() {
						defer close(c)

						_, err := s.Lock(ctx, table, rows, txn2, sharedOptions)
						require.NoError(t, err)
						defer func() {
							assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						}()

						checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
						checkLock(t, lt, rows[1], [][]byte{txn2}, nil, nil)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					require.NoError(t, waitLocalWaiters(lt, rows[1], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					<-c
				})
		})
	}
}

func TestRangeLockWithSharedAndShared(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeSharedOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
						checkLock(t, lt, rows[0], nil, nil, nil)
						checkLock(t, lt, rows[1], nil, nil, nil)
					}()
					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)
					checkLock(t, lt, rows[1], [][]byte{txn1}, nil, nil)

					_, err = s.Lock(ctx, table, rows, txn2, option)
					require.NoError(t, err)
					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)
						checkLock(t, lt, rows[1], [][]byte{txn1}, nil, nil)
					}()
					checkLock(t, lt, rows[0], [][]byte{txn1, txn2}, nil, nil)
					checkLock(t, lt, rows[1], [][]byte{txn1, txn2}, nil, nil)
				})
		})
	}
}

func TestRowLockWithConflict(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					// txn1 hold the lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					// txn2 blocked by txn1
					c := make(chan struct{})
					go func() {
						defer close(c)

						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
						defer func() {
							assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						}()

						checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					<-c
				})
		})
	}
}

func TestRangeLockWithConflict(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					// txn1 hold the lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					// txn2 blocked by txn1
					c := make(chan struct{})
					go func() {
						defer close(c)

						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
						defer func() {
							assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						}()

						checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
						checkLock(t, lt, rows[1], [][]byte{txn2}, nil, nil)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					require.NoError(t, waitLocalWaiters(lt, rows[1], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					<-c
				})
		})
	}
}

func TestRowLockWithWaitQueue(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {

					option := newTestRowExclusiveOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 into wait queue
					close2 := make(chan struct{})
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
						<-close2
						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					// add txn3 into wait queue
					close3 := make(chan struct{})
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn3, option)
						require.NoError(t, err)

						<-close3
						require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 2))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2, txn3}, []int32{3, 3})

					// close txn1, txn2 get lock
					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn2}, [][]byte{txn3}, []int32{3})

					// close txn2, txn3 get lock
					close(close2)
					require.NoError(t, waitLocalWaiters(lt, rows[0], 0))
					checkLock(t, lt, rows[0], [][]byte{txn3}, nil, nil)

					close(close3)
					wg.Wait()
				})
		})
	}
}

func TestRangeLockWithWaitQueue(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {

					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 into wait queue
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					// add txn3 into wait queue
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn3, option)
						require.NoError(t, err)
						require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 2))
					require.NoError(t, waitLocalWaiters(lt, rows[1], 2))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2, txn3}, []int32{3, 3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, [][]byte{txn2, txn3}, []int32{3, 3})

					// close txn1, txn2 or txn3 get lock
					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					wg.Wait()
				})
		})
	}
}

func TestRowLockWithSameTxnWithConflict(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {

					option := newTestRowExclusiveOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 op1 into wait queue
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
					}()
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					// add txn2 op2 into wait queue
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 2))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2, txn2}, []int32{3, 3})

					// close txn1, txn2 get lock
					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					require.NoError(t, waitLocalWaiters(lt, rows[0], 0))
					checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)

					wg.Wait()
					require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
				})
		})
	}
}

func TestRangeLockWithSameTxnWithConflict(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {

					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 op1 into wait queue
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
					}()
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					require.NoError(t, waitLocalWaiters(lt, rows[1], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					// add txn2 op2 into wait queue
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 2))
					require.NoError(t, waitLocalWaiters(lt, rows[1], 2))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2, txn2}, []int32{3, 3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, [][]byte{txn2, txn2}, []int32{3, 3})

					// close txn1, txn2 get lock
					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))

					require.NoError(t, waitLocalWaiters(lt, rows[0], 0))
					require.NoError(t, waitLocalWaiters(lt, rows[1], 0))

					wg.Wait()

					checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
					checkLock(t, lt, rows[1], [][]byte{txn2}, nil, nil)
					require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
				})
		})
	}
}

func TestManyRowLock(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()

					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)
					checkLock(t, lt, rows[1], [][]byte{txn1}, nil, nil)
				})
		})
	}
}

func TestManyRangeLock(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2, 3, 4)
					txn1 := newTestTxnID(1)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()

					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)
					checkLock(t, lt, rows[1], [][]byte{txn1}, nil, nil)
					checkLock(t, lt, rows[2], [][]byte{txn1}, nil, nil)
					checkLock(t, lt, rows[3], [][]byte{txn1}, nil, nil)
				})
		})
	}
}

func TestManyRowLockWithConflict(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					// txn1 hold the lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					// txn2 blocked by txn1
					c := make(chan struct{})
					go func() {
						defer close(c)

						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
						defer func() {
							assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						}()

						checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
						checkLock(t, lt, rows[1], [][]byte{txn2}, nil, nil)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))

					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, nil, nil)

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					<-c
				})
		})
	}
}

func TestManyRangeLockWithConflict(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2, 3, 4)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					// txn1 hold the lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					// txn2 blocked by txn1
					c := make(chan struct{})
					go func() {
						defer close(c)

						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
						defer func() {
							assert.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
						}()

						checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
						checkLock(t, lt, rows[1], [][]byte{txn2}, nil, nil)
						checkLock(t, lt, rows[2], [][]byte{txn2}, nil, nil)
						checkLock(t, lt, rows[3], [][]byte{txn2}, nil, nil)
					}()

					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))

					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, [][]byte{txn2}, []int32{3})
					checkLock(t, lt, rows[2], [][]byte{txn1}, nil, nil)
					checkLock(t, lt, rows[3], [][]byte{txn1}, nil, nil)

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					<-c
				})
		})
	}
}

func TestManyRowLockInManyGoroutines(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					rows := newTestRows(1, 2, 3, 4, 5, 6)

					var succeeds atomic.Int32
					sum := int32(10)
					var wg sync.WaitGroup
					for i := int32(0); i < sum; i++ {
						wg.Add(1)
						go func(i int32) {
							defer wg.Done()
							txnID := newTestTxnID(byte(i))
							_, err := s.Lock(ctx, table, rows, txnID, option)
							require.NoError(t, err)
							require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
							succeeds.Add(1)
						}(i)
					}
					wg.Wait()
					assert.Equal(t, sum, succeeds.Load())
				})
		})
	}
}

func TestManyRangeLockInManyGoroutines(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

					var succeeds atomic.Int32
					sum := int32(10)
					var wg sync.WaitGroup
					for i := int32(0); i < sum; i++ {
						wg.Add(1)
						go func(i int32) {
							defer wg.Done()
							txnID := newTestTxnID(byte(i))
							_, err := s.Lock(ctx, table, rows, txnID, option)
							require.NoError(t, err)
							require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
							succeeds.Add(1)
						}(i)
					}
					wg.Wait()
					assert.Equal(t, sum, succeeds.Load())
				})
		})
	}
}

func TestCtxCancelWhileWaiting(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)

					// txn1 hold the lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					// txn2 blocked by txn1
					ctx2, cancel := context.WithTimeout(context.Background(), time.Second*10)
					var wg1 sync.WaitGroup
					var wg2 sync.WaitGroup
					wg1.Add(1)
					go func() {
						defer wg1.Done()

						_, err := s.Lock(ctx2, table, rows, txn2, option)
						require.Error(t, err)

						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					waitLocalWaiters(lt, rows[0], 1)

					wg2.Add(1)
					go func() {
						defer wg2.Done()

						_, err := s.Lock(ctx, table, rows, txn3, option)
						require.NoError(t, err)
						require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
					}()
					waitLocalWaiters(lt, rows[0], 2)
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2, txn3}, []int32{3, 3})

					// cancel txn2
					cancel()
					wg1.Wait()
					// unlock txn1, txn3 will get lock
					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))

					wg2.Wait()
				})
		})
	}
}

func TestDeadLock(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					row1 := newTestRows(1)
					row2 := newTestRows(2)
					row3 := newTestRows(3)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)

					mustAddTestLock(t, ctx, s, 1, txn1, row1, pb.Granularity_Row)
					mustAddTestLock(t, ctx, s, 1, txn2, row2, pb.Granularity_Row)
					mustAddTestLock(t, ctx, s, 1, txn3, row3, pb.Granularity_Row)

					var wg sync.WaitGroup
					wg.Add(3)
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn1, row2,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn2, row3,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn3, row1,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
					}()
					wg.Wait()
				})
		})
	}
}

func TestDeadLockWithRange(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)
					range1 := newTestRows(1, 2)
					range2 := newTestRows(3, 4)
					range3 := newTestRows(5, 6)

					mustAddTestLock(t, ctx, s, 1, txn1, range1, pb.Granularity_Range)
					mustAddTestLock(t, ctx, s, 1, txn2, range2, pb.Granularity_Range)
					mustAddTestLock(t, ctx, s, 1, txn3, range3, pb.Granularity_Range)

					var wg sync.WaitGroup
					wg.Add(3)
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn1, range2,
							pb.Granularity_Range)
						require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn2, range3,
							pb.Granularity_Range)
						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn3, range1,
							pb.Granularity_Range)
						require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
					}()
					wg.Wait()
				})
		})
	}
}

func TestDeadLockWith2Txn(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					row1 := newTestRows(1)
					row2 := newTestRows(2)

					mustAddTestLock(t, ctx, s, 1, txn1, row1, pb.Granularity_Row)
					mustAddTestLock(t, ctx, s, 1, txn2, row2, pb.Granularity_Row)

					var wg sync.WaitGroup
					wg.Add(2)
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, 1, txn1, row2,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()
						maybeAddTestLockWithDeadlock(t, ctx, s, 1, txn2, row1,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					wg.Wait()
				})
		})
	}
}

func TestDeadLockWithIndirectDependsOn(t *testing.T) {
	for name, runner := range runners {
		if name == "local" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			table := uint64(10)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					// row1: txn1 : txn2, txn3
					// row4: txn4 : txn3, txn2

					row1 := newTestRows(1)
					row4 := newTestRows(4)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)
					txn4 := newTestTxnID(4)

					mustAddTestLock(t, ctx, s, table, txn1, row1, pb.Granularity_Row)
					mustAddTestLock(t, ctx, s, table, txn4, row4, pb.Granularity_Row)

					var wg sync.WaitGroup
					wg.Add(4)
					go func() {
						defer wg.Done()

						// row1 wait list: txn2
						maybeAddTestLockWithDeadlockWithWaitRetry(
							t,
							ctx,
							s,
							table,
							txn2,
							row1,
							pb.Granularity_Row,
							time.Second*5,
						)
					}()
					go func() {
						defer wg.Done()

						// row4 wait list: txn3
						waitLocalWaiters(lt, row4[0], 1)

						// row4 wait list: txn3, txn2
						maybeAddTestLockWithDeadlockWithWaitRetry(
							t,
							ctx,
							s,
							table,
							txn2,
							row4,
							pb.Granularity_Row,
							time.Second*5,
						)

						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()

					go func() {
						defer wg.Done()

						// row1 wait list: txn2
						waitLocalWaiters(lt, row1[0], 1)

						// row1 wait list: txn2, txn3
						maybeAddTestLockWithDeadlockWithWaitRetry(
							t,
							ctx,
							s,
							table,
							txn3,
							row1,
							pb.Granularity_Row,
							time.Second*5,
						)

						require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()

						// row4 wait list: txn3
						maybeAddTestLockWithDeadlockWithWaitRetry(
							t,
							ctx,
							s,
							table,
							txn3,
							row4,
							pb.Granularity_Row,
							time.Second*5)
					}()

					// row1:  txn1 : txn2, txn3
					waitLocalWaiters(lt, row1[0], 2)
					// row4:  txn4 : txn3, txn2
					waitLocalWaiters(lt, row4[0], 2)

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					require.NoError(t, s.Unlock(ctx, txn4, timestamp.Timestamp{}))

					wg.Wait()
				})
		})
	}
}

func TestLockSuccWithKeepBindTimeout(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Second*1,
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			err = l.remote.keeper.Close()
			require.NoError(t, err)

			time.Sleep(time.Second * 3)

			p := alloc.GetLatest(0, 0)
			require.True(t, p.Valid)
		},
		nil,
	)

}

func TestReLockSuccWithLockTableBindChanged(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Second*1,
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

			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			b := l1.tableGroups.get(0, 0).getBind()
			r := &remoteLockTable{bind: b}
			l1.tableGroups.Lock()
			l1.tableGroups.holders[0].tables[0] = r
			l1.tableGroups.Unlock()

			// should lock failed
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

		},
		nil,
	)
}

func TestReLockSuccWithReStartCN(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Second*1,
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

			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			l1.mu.Lock()
			l1.serviceID = getServiceIdentifier("s1", time.Now().UnixNano())
			l1.mu.Unlock()
			l1.tableGroups.removeWithFilter(
				func(table uint64, lt lockTable) bool {
					return true
				})
			time.Sleep(time.Second * 5)

			// should lock succ
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
		},
		nil,
	)
}

func TestReLockSuccWithKeepBindTimeout(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Second*1,
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

			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			err = l1.remote.keeper.Close()
			require.NoError(t, err)

			time.Sleep(time.Second * 3)

			p := alloc.GetLatest(0, 0)
			require.True(t, p.Valid)

			l1.tableGroups.removeWithFilter(func(key uint64, value lockTable) bool {
				return true
			})

			// should lock succ
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
		},
		nil,
	)

}

func TestLockResultWithNoConflict(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			res, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
			assert.False(t, res.Timestamp.IsEmpty())

			lb, err := l.getLockTable(0, 0)
			require.NoError(t, err)
			assert.Equal(t, lb.getBind(), res.LockedOn)
		},
	)
}

func TestLockResultWithConflictAndTxnCommitted(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

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
			_, err := l.Lock(
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
				res, err := l.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn2"),
					option)
				require.NoError(t, err)
				assert.True(t, !res.Timestamp.IsEmpty())
			}()

			waitWaiters(t, l, 0, []byte{1}, 1)
			require.NoError(t, l.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{PhysicalTime: 1}))
			wg.Wait()
		},
	)
}

func TestRestartInRollingRestartCN(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			alloc.setRestartService("s1")
			require.Equal(t, true, alloc.canRestartService("s1"))
		},
	)
}

func TestUnlockInRollingRestartCN(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			t1, _ := l.clock.Now()
			option.SnapShotTs = t1
			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			err = l.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l.isStatus(pb.Status_ServiceCanRestart) {
					break
				}
				select {
				case <-ctx.Done():
					require.Equal(t, false, true)
					return
				default:
				}
			}
		},
	)
}

func TestOldTxnLockInRollingRestartCN(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			t1, _ := l.clock.Now()
			option.SnapShotTs = t1
			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}

			// old txn
			_, err = l.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
		},
	)
}

func TestNewTxnLockInRollingRestartCN(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			t1, _ := l.clock.Now()
			option.SnapShotTs = t1
			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}

			// new txn
			t2, _ := l.clock.Now()
			option.SnapShotTs = t2
			_, err = l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.Error(t, err)
		},
	)
}

func TestStatusInRollingRestartCN(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			t1, _ := l.clock.Now()
			option.SnapShotTs = t1
			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l.isStatus(pb.Status_ServiceLockWaiting))

			err = l.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)

			for {
				if l.isStatus(pb.Status_ServiceCanRestart) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l.isStatus(pb.Status_ServiceCanRestart))
			require.Equal(t, true, alloc.canRestartService("s1"))
		},
	)
}

func TestRemoteLockFailedInRollingRestartCN(t *testing.T) {
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

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceLockWaiting))

			// remote lock should be failed
			t2, _ := l2.clock.Now()
			option.SnapShotTs = t2
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.Error(t, err)
		},
	)
}

func TestRemoteLockSuccInRollingRestartCN(t *testing.T) {
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

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			t2, _ := l2.clock.Now()
			option.SnapShotTs = t2
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceLockWaiting))

			// remote lock should be succ
			t3, _ := l2.clock.Now()
			option.SnapShotTs = t3
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{3}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
		},
	)
}

func TestRetryLockSuccInRollingRestartCN(t *testing.T) {
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

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			t2, _ := l2.clock.Now()
			option.SnapShotTs = t2
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceLockWaiting))

			// remote lock should be failed
			t3, _ := l2.clock.Now()
			option.SnapShotTs = t3
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn3"),
				option)
			require.Error(t, err)

			err = l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)

			err = l2.Unlock(
				ctx,
				[]byte("txn2"),
				timestamp.Timestamp{})
			require.NoError(t, err)
			for {
				if l1.isStatus(pb.Status_ServiceCanRestart) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceCanRestart))

			// remote lock should be succ
			option.SnapShotTs = t3
			for {
				if _, err = l2.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn3"),
					option); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
					require.Error(t, moerr.NewRetryForCNRollingRestart(), err)
				}
			}
		},
	)
}

func TestMoveTableSuccInRollingRestartCN(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceLockWaiting))

			err = l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)

			for {
				if !alloc.validLockTable(0, 0) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.False(t, alloc.validLockTable(0, 0))
		},
	)
}

func TestMoveTableLockSuccInRollingRestartCN(t *testing.T) {
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

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceLockWaiting))

			err = l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)

			for {
				if !alloc.validLockTable(0, 0) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.False(t, alloc.validLockTable(0, 0))

			// new txn should be got right lock table
			t2, _ := l2.clock.Now()
			option.SnapShotTs = t2
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
		},
	)
}

func TestMoveTableRetryLockSuccInRollingRestartCN(t *testing.T) {
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

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			t2, _ := l2.clock.Now()
			option.SnapShotTs = t2
			_, err = l2.Lock(
				ctx,
				1,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			t3, _ := l1.clock.Now()
			option.SnapShotTs = t3
			_, err = l1.Lock(
				ctx,
				2,
				[][]byte{{1}},
				[]byte("txn3"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceLockWaiting))

			err = l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)

			for {
				if _, err = l2.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn2"),
					option); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
					require.Error(t, moerr.NewRetryForCNRollingRestart(), err)
				}
			}
		},
	)
}

func TestPreTxnLockInRollingRestartCN(t *testing.T) {
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

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			t2, _ := l2.clock.Now()
			option.SnapShotTs = t2
			_, err = l2.Lock(
				ctx,
				1,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s2")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			require.Equal(t, true, l1.isStatus(pb.Status_ServiceLockWaiting))

			// remote lock should be succ, because txn3 start time earlier than restart time
			option.SnapShotTs = t1
			_, err = l1.Lock(
				ctx,
				1,
				[][]byte{{1}},
				[]byte("txn3"),
				option)
			require.NoError(t, err)
		},
	)
}

func TestLockResultWithConflictAndTxnAborted(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

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
			_, err := l.Lock(
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
				res, err := l.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn2"),
					option)
				require.NoError(t, err)
				assert.False(t, res.Timestamp.IsEmpty())
			}()

			waitWaiters(t, l, 0, []byte{1}, 1)
			require.NoError(t, l.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{}))
			wg.Wait()
		},
	)
}

func TestRowLockWithConflictAndUnlock(t *testing.T) {
	table := uint64(0)
	getRunner(false)(
		t,
		table,
		func(
			ctx context.Context,
			s *service,
			lt *localLockTable) {
			option := newTestRowExclusiveOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)

			// txn1 hold the lock
			_, err := s.Lock(ctx, table, rows, txn1, option)
			require.NoError(t, err)

			// txn2 blocked by txn1
			c := make(chan struct{})
			go func() {
				defer close(c)

				_, err := s.Lock(ctx, table, rows, txn2, option)
				require.Error(t, err)
			}()

			require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
			checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))

			<-c
			checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{1})
			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
		})
}

func TestUnlockRangeLockCanNotifyAllWaiters(t *testing.T) {
	table := uint64(0)
	getRunner(false)(
		t,
		table,
		func(
			ctx context.Context,
			s *service,
			lt *localLockTable) {
			rangeOption := newTestRangeExclusiveOptions()
			rowOption := newTestRowExclusiveOptions()
			rows := newTestRows(1, 10)
			rows2 := newTestRows(2)
			rows3 := newTestRows(3, 4)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)

			// txn1 hold the lock
			_, err := s.Lock(ctx, table, rows, txn1, rangeOption)
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(2)

			// txn2 blocked by txn1
			go func() {
				defer wg.Done()

				_, err := s.Lock(ctx, table, rows2, txn2, rowOption)
				require.NoError(t, err)
			}()
			require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
			checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

			// txn3 blocked by txn1
			go func() {
				defer wg.Done()

				_, err := s.Lock(ctx, table, rows3, txn3, rangeOption)
				require.NoError(t, err)
			}()
			require.NoError(t, waitLocalWaiters(lt, rows[0], 2))
			checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2, txn3}, []int32{3, 3})

			// unlock txn, txn2 and txn3 can both get lock
			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))

			wg.Wait()
			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
		})
}

func TestHasAnyHolderCannotNotifyWaiters(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowSharedOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)

					// txn1 get lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)

					// txn2 get lock, shared
					_, err = s.Lock(ctx, table, rows, txn2, option)
					require.NoError(t, err)
					checkLock(t, lt, rows[0], [][]byte{txn1, txn2}, nil, nil)

					c := make(chan struct{})
					// txn1 blocked by txn1 and txn2
					go func() {
						defer close(c)
						_, err = s.Lock(ctx, table, rows, txn3, newTestRowExclusiveOptions())
						require.NoError(t, err)
					}()
					waitLocalWaiters(lt, rows[0], 1)

					// close txn1 cannot notify txn3
					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					require.True(t, checkLocalWaitersStatus(lt, rows[0], []waiterStatus{blocking}))

					require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					<-c
					require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
				})
		})
	}
}

func TestTxnUnlockWithBindChanged(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(10)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					// txn1 get lock
					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)

					// changed bind
					bind := lt.getBind()
					bind.Version = bind.Version + 1
					new := newLocalLockTable(bind, s.fsp, s.events, s.clock, s.activeTxnHolder)
					s.tableGroups.set(0, table, new)
					lt.close()

					// txn2 get lock, shared
					_, err = s.Lock(ctx, table, rows, txn2, option)
					require.NoError(t, err)
					checkLock(t, new.(*localLockTable), rows[0], [][]byte{txn2}, nil, nil)

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
				})
		})
	}
}

func TestMultiGroupWithSameTableID(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(10)
			g1 := uint32(1)
			g2 := uint32(2)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {

					rows := newTestRows(1)

					txn1 := newTestTxnID(1)
					option1 := newTestRowSharedOptions()
					option1.Group = g1

					txn2 := newTestTxnID(2)
					option2 := newTestRowSharedOptions()
					option2.Group = g2

					// txn1 get lock
					_, err := s.Lock(ctx, table, rows, txn1, option1)
					require.NoError(t, err)
					lt1, err := s.getLockTable(g1, table)
					assert.NoError(t, err)
					checkLock(t, lt1.(*localLockTable), rows[0], [][]byte{txn1}, nil, nil)

					// txn2 get lock, shared
					_, err = s.Lock(ctx, table, rows, txn2, option2)
					require.NoError(t, err)
					lt2, err := s.getLockTable(g2, table)
					assert.NoError(t, err)
					checkLock(t, lt2.(*localLockTable), rows[0], [][]byte{txn2}, nil, nil)

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
				})
		})
	}
}

func TestShardingByRowWithSameTableID(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(10)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {

					option := newTestRowSharedOptions()
					option.Sharding = pb.Sharding_ByRow
					option.Group = 1
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					rows1 := newTestRows(1)
					rows2 := newTestRows(2)

					// txn1 get lock
					_, err := s.Lock(ctx, table, rows1, txn1, option)
					require.NoError(t, err)
					l := s.tableGroups.get(1, shardingByRow(rows1[0]))
					assert.Equal(t, table, l.getBind().OriginTable)
					checkLock(t, l.(*localLockTable), rows1[0], [][]byte{txn1}, nil, nil)

					// txn2 get lock, shared
					_, err = s.Lock(ctx, table, rows2, txn2, option)
					require.NoError(t, err)
					l = s.tableGroups.get(1, shardingByRow(rows2[0]))
					assert.Equal(t, table, l.getBind().OriginTable)
					checkLock(t, l.(*localLockTable), rows2[0], [][]byte{txn2}, nil, nil)

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
				})
		})
	}
}

func TestRowLockWithFailFast(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRowExclusiveOptions()
					option.Policy = pb.WaitPolicy_FastFail
					rows := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()
					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)

					_, err = s.Lock(ctx, table, rows, txn2, option)
					require.Error(t, ErrLockConflict, err)
				})
		})
	}
}

func TestRangeLockWithFailFast(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable) {
					option := newTestRangeExclusiveOptions()
					option.Policy = pb.WaitPolicy_FastFail
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					defer func() {
						assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()
					checkLock(t, lt, rows[0], [][]byte{txn1}, nil, nil)

					_, err = s.Lock(ctx, table, rows, txn2, option)
					require.Error(t, ErrLockConflict, err)
				})
		})
	}
}

func TestIssue2128(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, ss []*service) {
			l := ss[1]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			_, err := l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			lb, err := l.getLockTable(0, 0)
			require.NoError(t, err)
			b := lb.getBind()
			b.ServiceID = "1705661824807004000s3"
			l.handleBindChanged(b)

			_, err = l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.Error(t, err)

			_, err = l.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
		},
	)
}

func TestIssue14008(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, ss []*service) {
			s1 := ss[0]
			alloc.server.RegisterMethodHandler(pb.Method_GetBind,
				func(
					ctx context.Context,
					cf context.CancelFunc,
					r1 *pb.Request,
					r2 *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cf, r2, ErrTxnNotFound, cs)
				})
			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := s1.getLockTableWithCreate(0, 10, nil, pb.Sharding_None)
					require.Error(t, err)
				}()
			}
			wg.Wait()
		})
}

func TestHandleBindChangedConcurrently(t *testing.T) {
	table := uint64(10)
	getRunner(false)(
		t,
		table,
		func(
			ctx context.Context,
			s *service,
			lt *localLockTable) {
			bind := lt.getBind()

			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					option := newTestRowSharedOptions()
					rows := newTestRows(1)
					txn := newTestTxnID(byte(i))
					for i := 0; i < 1000; i++ {
						_, err := s.Lock(ctx, table, rows, txn, option)
						require.NoError(t, err)
						require.NoError(t, s.Unlock(ctx, txn, timestamp.Timestamp{}))
					}
				}(i)
			}
			for i := 0; i < 1000; i++ {
				s.handleBindChanged(bind)
			}
			wg.Wait()
		},
	)
}

func BenchmarkWithoutConflict(b *testing.B) {
	runBenchmark(b, "1-table", 1)
	runBenchmark(b, "unlimited-table", 32)
}

func BenchmarkCrc64(b *testing.B) {
	b.Run("crc64-ISO", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			sum := uint64(0)
			for p.Next() {
				n := crc64.Checksum([]byte("hello"), crc64.MakeTable(crc64.ISO))
				sum += n
			}
			b.Log(sum)
		})
	})
	b.Run("crc64-ECMA", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			sum := uint64(0)
			for p.Next() {
				n := crc64.Checksum([]byte("hello"), crc64.MakeTable(crc64.ECMA))
				sum += n
			}
			b.Log(sum)
		})
	})
	b.Run("crc32", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			sum := uint32(0)
			for p.Next() {
				n := crc32.Checksum([]byte("hello"), crc32.MakeTable(crc32.IEEE))
				sum += n
			}
			b.Log(sum)
		})
	})
}

var tableID atomic.Uint64
var txnID atomic.Uint64
var rowID atomic.Uint64

func runBenchmark(b *testing.B, name string, t uint64) {
	b.Run(name, func(b *testing.B) {
		runLockServiceTestsWithLevel(
			b,
			zapcore.InfoLevel,
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
					for p.Next() {
						if _, err := l.Lock(ctx, table, row, txn, pb.LockOptions{}); err != nil {
							panic(err)
						}
						if err := l.Unlock(ctx, txn, timestamp.Timestamp{}); err != nil {
							panic(err)
						}
					}
				})
			},
			nil,
		)
	})
}

func maybeAddTestLockWithDeadlock(
	t *testing.T,
	ctx context.Context,
	l *service,
	table uint64,
	txnID []byte,
	lock [][]byte,
	granularity pb.Granularity,
) pb.Result {
	return maybeAddTestLockWithDeadlockWithWaitRetry(
		t,
		ctx,
		l,
		table,
		txnID,
		lock,
		granularity,
		0,
	)
}

func maybeAddTestLockWithDeadlockWithWaitRetry(
	t *testing.T,
	ctx context.Context,
	l *service,
	table uint64,
	txnID []byte,
	lock [][]byte,
	granularity pb.Granularity,
	wait time.Duration,
) pb.Result {
	res, err := l.Lock(ctx, table, lock, txnID, pb.LockOptions{
		Granularity: granularity,
		Mode:        pb.LockMode_Exclusive,
		Policy:      pb.WaitPolicy_Wait,
		RetryWait:   int64(wait),
	})

	if moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNotFound) ||
		moerr.IsMoErrCode(err, moerr.ErrInvalidState) {
		return res
	}
	require.NoError(t, err)
	return res
}

func runLockServiceTests(
	t assert.TestingT,
	serviceIDs []string,
	fn func(*lockTableAllocator, []*service),
	opts ...Option,
) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		serviceIDs,
		time.Second*10,
		fn,
		nil,
		opts...,
	)
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
	adjustConfig func(*Config),
	opts ...Option,
) {
	defer leaktest.AfterTest(t.(testing.TB))()

	reuse.RunReuseTests(func() {
		RunLockServicesForTest(
			level,
			serviceIDs,
			lockTableBindTimeout,
			func(lta LockTableAllocator, ls []LockService) {
				services := make([]*service, 0, len(ls))
				for _, s := range ls {
					services = append(services, s.(*service))
				}
				fn(lta.(*lockTableAllocator), services)
			},
			adjustConfig,
			opts...,
		)
	})
}

func waitWaiters(
	t *testing.T,
	s *service,
	table uint64,
	key []byte,
	waitersCount int) {
	require.NoError(t, WaitWaiters(s, 0, table, key, waitersCount))
}

func newTestRowExclusiveOptions() pb.LockOptions {
	return pb.LockOptions{
		Granularity: pb.Granularity_Row,
		Mode:        pb.LockMode_Exclusive,
		Policy:      pb.WaitPolicy_Wait,
	}
}

func newTestRowSharedOptions() pb.LockOptions {
	return pb.LockOptions{
		Granularity: pb.Granularity_Row,
		Mode:        pb.LockMode_Shared,
		Policy:      pb.WaitPolicy_Wait,
	}
}

func newTestRangeSharedOptions() pb.LockOptions {
	return pb.LockOptions{
		Granularity: pb.Granularity_Range,
		Mode:        pb.LockMode_Shared,
		Policy:      pb.WaitPolicy_Wait,
	}
}

func newTestRangeExclusiveOptions() pb.LockOptions {
	return pb.LockOptions{
		Granularity: pb.Granularity_Range,
		Mode:        pb.LockMode_Exclusive,
		Policy:      pb.WaitPolicy_Wait,
	}
}

func newTestRows(rows ...byte) [][]byte {
	values := make([][]byte, 0, len(rows))
	for _, row := range rows {
		values = append(values, []byte{row})
	}
	return values
}

func newTestTxnID(id byte) []byte {
	return []byte{id}
}

func checkLock(
	t *testing.T,
	lt *localLockTable,
	key []byte,
	expectHolders [][]byte,
	expectWaiters [][]byte,
	expectWaiterRefs []int32) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	lock, ok := lt.mu.store.Get(key)
	require.Equal(t, len(expectHolders) == 0, !ok)
	if !ok {
		return
	}

	for _, txn := range expectHolders {
		require.True(t, lock.holders.contains(txn))
	}

	require.Equal(t, len(expectWaiters), lock.waiters.size())

	idx := 0
	lock.waiters.iter(func(w *waiter) bool {
		require.Equal(t, expectWaiters[idx], w.txn.TxnID)

		n := expectWaiterRefs[idx]
		lt.events.mu.Lock()
		for _, v := range lt.events.mu.blockedWaiters {
			if v == w {
				n += 1
			}
		}
		require.Equal(t, n, w.refCount.Load())
		lt.events.mu.Unlock()
		idx++
		return true
	})
}

func mustAddTestLock(t *testing.T,
	ctx context.Context,
	s *service,
	table uint64,
	txnID []byte,
	lock [][]byte,
	granularity pb.Granularity) pb.Result {
	return maybeAddTestLockWithDeadlock(t,
		ctx,
		s,
		table,
		txnID,
		lock,
		granularity)
}

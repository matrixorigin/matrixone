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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()

				option := newTestRowExclusiveOptions()
				rows := newTestRows(1)
				txn1 := newTestTxnID(1)
				_, err := s1.Lock(ctx, table, rows, txn1, option)
				require.NoError(t, err, err)
				require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

				lt, err := s1.getLockTable(table)
				require.NoError(t, err)

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
							assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
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
							assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
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
							assert.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
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

			lb, err := l.getLockTable(0)
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
					new := newLocalLockTable(bind, s.fsp, s.events, s.clock)
					s.tables.set(table, new)
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
					_, err := s1.getLockTableWithCreate(10, true)
					require.Error(t, err)
				}()
			}
			wg.Wait()
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
	granularity pb.Granularity) pb.Result {
	t.Logf("%s try lock %+v", string(txnID), lock)
	res, err := l.Lock(ctx, table, lock, txnID, pb.LockOptions{
		Granularity: granularity,
		Mode:        pb.LockMode_Exclusive,
		Policy:      pb.WaitPolicy_Wait,
	})

	if moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected) {
		t.Logf("%s lock %+v, found dead lock", string(txnID), lock)
		return res
	}
	t.Logf("%s lock %+v, ok", string(txnID), lock)
	require.NoError(t, err)
	return res
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
	defer leaktest.AfterTest(t.(testing.TB))()
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
	)
}

func TestUnsafeStringToByteSlice(t *testing.T) {
	v := "abc"
	assert.Equal(t, []byte(v), util.UnsafeStringToBytes(v))
}

func waitWaiters(
	t *testing.T,
	s *service,
	table uint64,
	key []byte,
	waitersCount int) {
	require.NoError(t, WaitWaiters(s, table, key, waitersCount))
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

func TestSharedTableID(t *testing.T) {
	tenantID := uint32(955)
	tableID := uint64(2)

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, tenantID)
	tenantID2, tableID2, ok := decodeSharedTableID(encodeSharedTableID(ctx, tableID))
	require.True(t, ok)
	require.Equal(t, tenantID, tenantID2)
	require.Equal(t, tableID, tableID2)
}

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
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

var (
	runners = map[string]func(t *testing.T, table uint64, fn func(context.Context, *service, *localLockTable)){
		"local": getRunner(false),
		// "remote": getRunner(true),
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

func TestReentrantRowLock(t *testing.T) {
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

					res, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					require.True(t, res.NewLockAdd)

					res, err = s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					require.False(t, res.NewLockAdd)

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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

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

func TestReentrantRangeLock(t *testing.T) {
	for name, runner := range runners {
		t.Run(name, func(t *testing.T) {
			table := uint64(0)
			runner(
				t,
				table,
				func(
					ctx context.Context,
					s *service,
					lt *localLockTable,
				) {
					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)

					res, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					require.True(t, res.NewLockAdd)

					res, err = s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)
					require.True(t, res.NewLockAdd)

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

					// keep txn1 cannot close by orphan txn
					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

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
					err := os.Setenv("mo_reuse_enable_checker", "true")
					require.NoError(t, err)
					option := newTestRangeExclusiveOptions()
					rows := newTestRows(1, 2)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

					// txn1 hold the lock
					_, err = s.Lock(ctx, table, rows, txn1, option)
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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
						f(txn3)
					}

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 into wait waitTxns
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

					// add txn3 into wait waitTxns
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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
						f(txn3)
					}

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 into wait waitTxns
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					// add txn3 into wait waitTxns
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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 op1 into wait waitTxns
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
					}()
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					// add txn2 op2 into wait waitTxns
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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

					_, err := s.Lock(ctx, table, rows, txn1, option)
					require.NoError(t, err)

					var wg sync.WaitGroup
					wg.Add(2)

					// add txn2 op1 into wait waitTxns
					go func() {
						defer wg.Done()
						_, err := s.Lock(ctx, table, rows, txn2, option)
						require.NoError(t, err)
					}()
					require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
					require.NoError(t, waitLocalWaiters(lt, rows[1], 1))
					checkLock(t, lt, rows[0], [][]byte{txn1}, [][]byte{txn2}, []int32{3})
					checkLock(t, lt, rows[1], [][]byte{txn1}, [][]byte{txn2}, []int32{3})

					// add txn2 op2 into wait waitTxns
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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
					}

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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
						f(txn3)
					}

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
						// txn2 <- txn1
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn1, row2,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()
						// txn3 <- txn2
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn2, row3,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					}()
					go func() {
						defer wg.Done()
						// txn1 <- txn3
						maybeAddTestLockWithDeadlock(t, ctx, s, table, txn3, row1,
							pb.Granularity_Row)
						require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
					}()
					// txn1 - txn3 - txn2 - txn1
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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
						f(txn3)
						f(txn4)
					}

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

func TestWaiterAwakeOnDeadLock(t *testing.T) {
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

					row1 := newTestRows(1)
					txn1 := newTestTxnID(1)
					txn2 := newTestTxnID(2)
					txn3 := newTestTxnID(3)

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
						f(txn3)
					}

					mustAddTestLock(t, ctx, s, table, txn1, row1, pb.Granularity_Row)

					var wg sync.WaitGroup
					wg.Add(2)
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

					}()

					// row1:  txn1 : txn2, txn3
					waitLocalWaiters(lt, row1[0], 2)

					t2 := lt.txnHolder.getActiveTxn(txn2, false, "")
					t2.Lock()
					t2.deadlockFound = true
					t2.Unlock()

					require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))

					t2.Lock()
					for _, w := range t2.blockedWaiters {
						w.notify(notifyValue{err: ErrDeadLockDetected}, getLogger(s.GetConfig().ServiceID))
					}
					t2.Unlock()

					require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
					for {
						t3 := lt.txnHolder.getActiveTxn(txn3, false, "")
						t3.Lock()
						if len(t3.getHoldLocksLocked(0).tableBinds) > 0 {
							t3.Unlock()
							break
						}
						t3.Unlock()

						select {
						case <-ctx.Done():
							t3.Lock()
							require.True(t, len(t3.getHoldLocksLocked(0).tableBinds) > 0)
							t3.Unlock()
							return
						default:
						}
					}

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
		time.Millisecond*200,
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
			require.NoError(t, l.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			for i := 0; i < 10; i++ {
				p := alloc.GetLatest(0, 0)
				require.True(t, p.Valid)
			}
		},
		nil,
	)

}

func TestAbortRemoteDeadlockTxn(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Second*10,
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

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := l2.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn2"),
					option)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected))
			}()

			// Wait until txn2's waiter is queued on l1 (one blocked waiter). Poll with
			// timeout to avoid unbounded spin or hang; small sleep avoids busy loop.
			deadline := time.Now().Add(time.Second * 5)
			var n int
			for time.Now().Before(deadline) {
				l1.events.mu.RLock()
				n = len(l1.events.mu.blockedWaiters)
				l1.events.mu.RUnlock()
				if n == 1 {
					break
				}
				time.Sleep(time.Millisecond * 5)
			}
			require.Equal(t, 1, n, "expected exactly one blocked waiter on l1 before abort")

			wait := pb.WaitTxn{TxnID: []byte("txn2"), WaiterAddress: l1.serviceID}
			l2.abortDeadlockTxn(wait, ErrDeadLockDetected)

			wg.Wait()
		},
		nil,
	)

}

func TestIssue3693(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Millisecond*200,
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()

			alloc.registerService(l.serviceID)

			alloc.setRestartService("s1")
			for {
				if l.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}
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

func TestIssue4007(t *testing.T) {
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

			_, err = l2.Lock(
				ctx,
				1,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			// should lock failed
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			// retry lock should be succ
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

func TestIssue3654(t *testing.T) {
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
				time.Nanosecond)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				RetryWait:   int64(time.Second),
			}

			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			if err != nil {
				require.True(
					t,
					moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) ||
						errors.Is(err, context.DeadlineExceeded) ||
						errors.Is(err, context.Canceled))
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cleanupCancel()
				require.NoError(t, l2.Unlock(cleanupCtx, []byte("txn2"), timestamp.Timestamp{}))
			}
		},
		nil,
	)
}

func TestIssue17225(t *testing.T) {
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

			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
			require.NoError(t, l2.Unlock(ctx, []byte("txn2"), timestamp.Timestamp{}))

			// equivalent to restart tn
			alloc.mu.Lock()
			alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
			alloc.mu.services = make(map[string]*serviceBinds)
			alloc.mu.Unlock()

			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{3}},
				[]byte("txn3"),
				option)
			require.NoError(t, err)

			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))
			require.NoError(t, l2.Unlock(ctx, []byte("txn3"), timestamp.Timestamp{}))
		},
		nil,
	)
}

func TestIssue17655(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Second*1,
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
					panic("timeout bug")
				default:
				}
			}

			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			_, err = l1.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNewTxnInCNRollingRestart))
		},
		nil,
	)
}

func TestIssue3537(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Second*1,
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
					panic("timeout bug")
				default:
				}
			}

			b := alloc.getServiceBindsWithoutPrefix("s1")
			b.setStatus(pb.Status_ServiceLockEnable)

			for {
				if b.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}

			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))
			for {
				if b.isStatus(pb.Status_ServiceCanRestart) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}
		},
		nil,
	)
}

func TestIssue3537_2(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Second*1,
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
					panic("timeout bug")
				default:
				}
			}

			b := alloc.getServiceBindsWithoutPrefix("s1")
			b.setStatus(pb.Status_ServiceLockEnable)

			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			for {
				if b.isStatus(pb.Status_ServiceCanRestart) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}
		},
		nil,
	)
}

func TestIssue3537_3(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Second*1,
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
					panic("timeout bug")
				default:
				}
			}

			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			b := alloc.getServiceBindsWithoutPrefix("s1")
			b.setStatus(pb.Status_ServiceLockEnable)

			for {
				if b.isStatus(pb.Status_ServiceCanRestart) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}
		},
		nil,
	)
}

func TestIssue3288(t *testing.T) {
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

			l1.Close()

			// Real scenario: after s1 (l1) dies, s2 (l2) acquires the lock. Caller retries on
			// ErrBackendClosed or ErrLockTableBindChanged per API contract until success.
			//
			// No infinite wait: (1) success -> break; (2) retryable error -> continue; (3) any other
			// error -> require.NoError fails and test stops; (4) ctx has 10s timeout, so even if we
			// only ever get retryable errors, Lock() will eventually return context.DeadlineExceeded
			// and (3) triggers. So the loop always terminates.
			//
			// No flakiness: no sleep; outcome is determined only by retry-until-success. CI stable
			// unless the environment is so slow that allocator cannot move bind within 10s.
			var result pb.Result
			txnID := []byte("txn2")
			txnSeq := byte(3)
			for {
				result, err = l2.Lock(ctx, 0, [][]byte{{1}}, txnID, option)
				if err == nil {
					break
				}
				if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
					txnID = []byte{txnSeq}
					txnSeq++
					continue
				}
				if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) {
					continue
				}
				require.NoError(t, err, "lock must succeed or return retryable error (ErrBackendClosed/ErrLockTableBindChanged)")
			}
			_ = result

		},
		nil,
	)
}

func TestIssue3538(t *testing.T) {
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

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}

			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			txnID := []byte("txn2")
			txnSeq := byte(3)
			for {
				_, err = l2.Lock(
					ctx,
					0,
					[][]byte{{1}},
					txnID,
					option)
				if err == nil {
					break
				}
				if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
					txnID = []byte{txnSeq}
					txnSeq++
				} else if moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart) ||
					moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
					moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
					// retry with the same txn while the old owner is draining
				} else {
					require.NoError(t, err)
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}
		},
		nil,
	)
}

func TestIssue16121(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Second*10,
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

			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			// change lock table to remote from local
			b := l1.tableGroups.get(0, 0).getBind()
			b.ServiceID = l2.serviceID
			r := &remoteLockTable{
				serviceID: l2.serviceID,
				bind:      b,
			}
			l1.tableGroups.Lock()
			l1.tableGroups.holders[0].tables[0] = r
			l1.tableGroups.Unlock()

			require.NoError(t, l2.Unlock(ctx, []byte("txn2"), timestamp.Timestamp{}))

		},
		nil,
	)
}

func TestReLockSuccWithBindChanged(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Second*10,
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

			// should lock succ
			for {
				select {
				case <-ctx.Done():
					require.Fail(t, "timeout")
				default:
				}
				_, err = l2.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn2"),
					option)
				if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
					continue
				}
				require.NoError(t, err)
				break
			}
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

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceCanRestart) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}

			// should lock succ
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, []byte("txn2"), timestamp.Timestamp{}))
		},
		nil,
	)
}

func TestCheckTxnTimeout(t *testing.T) {
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

			// table 0 in s2
			_, err := l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
			require.NoError(t, l2.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			// table 1 in s1
			_, err = l1.Lock(
				ctx,
				1,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
			_, err = l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
			l1.tableGroups.removeWithFilter(func(_ uint64, v lockTable) bool {
				return true
			}, closeReasonBindChanged)
			require.NoError(t, l1.Unlock(ctx, []byte("txn2"), timestamp.Timestamp{}))

			require.False(t, l2.activeTxnHolder.empty())

			alloc.setRestartService("s2")
			for {
				if l2.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					panic("timeout bug")
				default:
				}
			}

			l2.checkTxnTimeout(ctx)
			require.True(t, l2.activeTxnHolder.empty())
		},
		nil,
	)
}

func TestIssue5176(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Second*1,
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			l.cfg.TxnIterFunc = func(f func([]byte) bool) {}

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()

			l.activeTxnHolder.getActiveTxn([]byte("txn1"), true, "")

			l.setStatus(pb.Status_ServiceLockWaiting)

			l.checkTxnTimeout(ctx)
			require.True(t, l.activeTxnHolder.empty())
		},
		nil,
	)
}

func TestIssue5176_2(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Second*1,
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			l.cfg.TxnIterFunc = func(f func([]byte) bool) {
				f([]byte("txn1"))
			}

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()

			l.activeTxnHolder.getActiveTxn([]byte("txn1"), true, "")

			l.setStatus(pb.Status_ServiceLockWaiting)

			l.checkTxnTimeout(ctx)
			require.False(t, l.activeTxnHolder.empty())
		},
		nil,
	)
}

func TestReLockSuccWithKeepBindTimeout(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Millisecond*200,
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

			p := alloc.GetLatest(0, 0)
			require.True(t, p.Valid)

			l1.tableGroups.removeWithFilter(func(key uint64, value lockTable) bool {
				return true
			}, closeReasonBindChanged)

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

func TestIssue3231(t *testing.T) {
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

			t1, _ := l1.clock.Now()
			option.SnapShotTs = t1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))

			alloc.setRestartService("s1")

			// should lock succ
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
			b1 := alloc.GetLatest(0, 0)

			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)
			b2 := alloc.GetLatest(0, 0)
			// maybe not equal before fixed, because table bind 0 has moved
			// when service is waiting status
			require.True(t, b1.ServiceID == b2.ServiceID)
			require.True(t, b1.Version == b2.Version)
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

func TestReLockInRollingRestartCN(t *testing.T) {
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

			alloc.setRestartService("s1")
			for {
				if l1.isStatus(pb.Status_ServiceLockWaiting) {
					break
				}
				select {
				case <-ctx.Done():
					require.True(t, false)
					return
				default:
				}
			}

			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{3}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			err = l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)
			// Actually, txn1 and txn2 are executed concurrently.
			// it should use a loop check.
			for {
				if l1.validGroupTable(0, 0) {
					break
				}
				select {
				case <-ctx.Done():
					require.True(t, false)
				default:
				}
			}
			require.True(t, l1.isStatus(pb.Status_ServiceLockWaiting))

			err = l2.Unlock(
				ctx,
				[]byte("txn2"),
				timestamp.Timestamp{})
			require.NoError(t, err)
			require.False(t, l1.validGroupTable(0, 0))
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

func TestIssue4017(t *testing.T) {
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

			_, err = l.Lock(
				ctx,
				1,
				[][]byte{{2}},
				[]byte("txn1"),
				option)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNewTxnInCNRollingRestart))

			err = l.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{})
			require.NoError(t, err)
		},
	)
}

func TestLeaveGetBindInRollingRestartCN(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]
			l1 := s[1]

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

			// test
			t2, _ := l1.clock.Now()
			option.SnapShotTs = t2
			_, err = l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			alloc.setRestartService("s1")

			err = l1.Unlock(
				ctx,
				[]byte("txn2"),
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
			// get bind
			_, _, err = getLockTableBind(
				l.remote.client,
				0,
				0,
				0,
				l.serviceID,
				pb.Sharding_ByRow)
			require.Error(t, err)
			require.True(t, alloc.canRestartService("s1"))
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

			// remote lock should be failed,
			t2, _ := l2.clock.Now()
			option.SnapShotTs = t2
			_, err = l2.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn2"),
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

func TestCannotCommit(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			orphanTxn := pb.OrphanTxn{
				Service: "s1",
				Txn:     [][]byte{[]byte("testTxn")},
			}
			alloc.AddCannotCommit([]pb.OrphanTxn{orphanTxn})
			// a.cleanCommitState(context.Background())

			_, err := alloc.Valid("s1", []byte("testTxn"), nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
		},
	)
}

func TestResumeInvalidService(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			alloc.inactiveService.Store(s[0].serviceID, time.Now())
			_, err := alloc.Valid(s[0].serviceID, []byte("testTxn"), nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOnInvalidCN))

			require.NoError(t, s[0].Resume())
			_, err = alloc.Valid(s[0].serviceID, []byte("testTxn"), nil)
			require.NoError(t, err)
		},
	)
}

func TestCommitDetectsStaleLocalBindAfterAllocatorRestart(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1"},
		time.Hour,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			staleTable := uint64(272592)
			freshTable := uint64(50011829)
			option := newTestRowExclusiveOptions()

			_, err := l1.Lock(ctx, staleTable, newTestRows(1), newTestTxnID(1), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))

			staleLT := l1.tableGroups.get(0, staleTable)
			require.NotNil(t, staleLT)
			staleBind := staleLT.getBind()
			require.Equal(t, l1.serviceID, staleBind.ServiceID)

			// Simulate a DN/TN pod rebuild: the allocator gets a new epoch and
			// loses its in-memory bind map, while the CN service keeps running
			// with the old local lock table cached.
			alloc.mu.Lock()
			alloc.mu.services = make(map[string]*serviceBinds)
			alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
			alloc.version++
			restartedVersion := alloc.version
			alloc.mu.Unlock()

			require.Equal(t, staleBind, l1.tableGroups.get(0, staleTable).getBind())

			invalid, err := alloc.Valid(
				l1.serviceID,
				[]byte("commit-txn-before-refresh"),
				[]pb.LockTable{staleBind})
			require.NoError(t, err)
			require.Equal(t, []uint64{staleTable}, invalid)

			// Touching another table registers the same CN service in the new
			// allocator epoch. The response-level allocator version should make
			// the CN purge the stale local table before caching the fresh bind.
			_, err = l1.Lock(ctx, freshTable, newTestRows(2), newTestTxnID(2), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(2), timestamp.Timestamp{}))

			require.Nil(t, l1.tableGroups.get(0, staleTable))
			freshBind := l1.tableGroups.get(0, freshTable).getBind()
			require.Equal(t, l1.serviceID, freshBind.ServiceID)
			require.Equal(t, restartedVersion, freshBind.Version)
			require.True(t, alloc.KeepLockTableBind(l1.serviceID))

			alloc.mu.Lock()
			_, ok := alloc.getLockTablesLocked(0)[staleTable]
			alloc.mu.Unlock()
			require.False(t, ok)

			_, err = l1.Lock(ctx, staleTable, newTestRows(3), newTestTxnID(3), option)
			require.NoError(t, err)
			refreshedBind := l1.tableGroups.get(0, staleTable).getBind()
			require.True(t, refreshedBind.Changed(staleBind))
			require.Equal(t, restartedVersion, refreshedBind.Version)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(3), timestamp.Timestamp{}))
		},
		nil,
	)
}

func TestAllocatorVersionZeroKeepsLocalBinds(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			table := uint64(14039)
			option := newTestRowExclusiveOptions()
			_, err := l1.Lock(ctx, table, newTestRows(1), newTestTxnID(1), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))

			lt := l1.tableGroups.get(0, table)
			require.NotNil(t, lt)
			bind := lt.getBind()
			lastVersion := l1.lastAllocatorVersion

			removed := l1.observeAllocatorVersion("compat-test", 0)
			require.Zero(t, removed)
			require.Equal(t, lastVersion, l1.lastAllocatorVersion)
			require.Equal(t, bind, l1.tableGroups.get(0, table).getBind())
		},
	)
}

func TestAllocatorObserverDoesNotPurgeSameEpochBindVersions(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			tableA := uint64(18001)
			tableB := uint64(18002)
			option := newTestRowExclusiveOptions()
			_, err := l1.Lock(ctx, tableA, newTestRows(1), newTestTxnID(1), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))

			bindA := l1.tableGroups.get(0, tableA).getBind()
			bindB := bindA
			bindB.Table = tableB
			bindB.OriginTable = tableB
			bindB.Version = bindA.Version + 1
			l1.tableGroups.set(0, tableB, l1.createLockTableByBind(bindB))

			l1.allocatorVersionMu.Lock()
			l1.lastAllocatorVersion = 0
			l1.allocatorVersionMu.Unlock()

			removed := l1.observeAllocatorVersion("same-epoch-test", bindA.Version)
			require.Zero(t, removed)
			require.Equal(t, bindA, l1.tableGroups.get(0, tableA).getBind())
			require.Equal(t, bindB, l1.tableGroups.get(0, tableB).getBind())
		},
	)
}

func TestKeepBindFailedSkipsBindPublishedAfterSnapshot(t *testing.T) {
	logger := getLogger("")
	s := &service{
		serviceID: "s1",
		logger:    logger,
	}
	s.tableGroups = &lockTableHolders{
		service: s.serviceID,
		logger:  logger,
		holders: map[uint32]*lockTableHolder{},
	}
	oldVersion := s.tableGroups.getVersion()
	bind := pb.LockTable{
		Group:       0,
		Table:       1,
		OriginTable: 1,
		ServiceID:   s.serviceID,
		Version:     1,
		Valid:       true,
	}
	s.tableGroups.set(bind.Group, bind.Table, newRemoteLockTable(
		s.serviceID,
		time.Second,
		bind,
		nil,
		s.handleBindChanged,
		logger,
	))
	require.NotEqual(t, oldVersion, s.tableGroups.getVersion())

	removed := s.handleKeepBindFailed(
		s.serviceID,
		s.tableGroups,
		oldVersion,
		allocatorState{},
		allocatorState{})
	require.Zero(t, removed)
	require.NotNil(t, s.tableGroups.get(bind.Group, bind.Table))
}

func TestGetBindPurgesStaleBindWhenAllocatorIDChangesWithRegressedVersion(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			staleTable := uint64(19001)
			freshTable := uint64(19002)
			option := newTestRowExclusiveOptions()

			_, err := l1.Lock(ctx, staleTable, newTestRows(1), newTestTxnID(1), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))

			staleBind := l1.tableGroups.get(0, staleTable).getBind()
			oldAllocatorID := staleBind.AllocatorID
			require.NotEmpty(t, oldAllocatorID)

			l1.allocatorVersionMu.Lock()
			l1.lastAllocatorID = oldAllocatorID
			l1.lastAllocatorVersion = staleBind.Version
			l1.allocatorVersionMu.Unlock()

			alloc.mu.Lock()
			alloc.mu.services = make(map[string]*serviceBinds)
			alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
			alloc.allocatorID = "restarted-allocator-with-lower-version"
			alloc.version = staleBind.Version - 1
			restartedVersion := alloc.version
			restartedAllocatorID := alloc.allocatorID
			alloc.mu.Unlock()

			_, err = l1.getLockTableWithCreate(0, freshTable, newTestRows(2), pb.Sharding_None)
			require.NoError(t, err)
			require.Nil(t, l1.tableGroups.get(0, staleTable))
			require.Equal(t, restartedVersion, l1.lastAllocatorVersion)
			require.Equal(t, restartedAllocatorID, l1.lastAllocatorID)
			freshBind := l1.tableGroups.get(0, freshTable).getBind()
			require.Equal(t, restartedVersion, freshBind.Version)
			require.Equal(t, restartedAllocatorID, freshBind.AllocatorID)
		},
	)
}

func TestKeepaliveEpochPurgeKeepsGroupMovePop(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			table := uint64(21001)
			option := newTestRowExclusiveOptions()
			_, err := l1.Lock(ctx, table, newTestRows(1), newTestTxnID(1), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))
			require.NotNil(t, l1.tableGroups.get(0, table))

			l1.checkCanMoveGroupTables()
			require.NotNil(t, l1.topGroupTables())

			alloc.mu.Lock()
			alloc.version++
			restartedVersion := alloc.version
			alloc.mu.Unlock()

			l1.remote.keeper.(*lockTableKeeper).doKeepLockTableBind(ctx)

			require.Nil(t, l1.topGroupTables())
			require.Nil(t, l1.tableGroups.get(0, table))
			require.Equal(t, restartedVersion, l1.lastAllocatorVersion)
		},
	)
}

func TestKeepaliveOKFalseWithNewAllocatorVersionPurgesStaleBinds(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			alloc.mu.Lock()
			oldVersion := alloc.version
			oldAllocatorID := alloc.allocatorID
			alloc.mu.Unlock()

			l1.allocatorVersionMu.Lock()
			l1.lastAllocatorID = oldAllocatorID
			l1.lastAllocatorVersion = oldVersion
			l1.allocatorVersionMu.Unlock()

			staleLocalTable := uint64(21501)
			staleRemoteTable := uint64(21502)
			staleProxyTable := uint64(21503)
			currentProxyTable := uint64(21504)

			staleLocalBind := pb.LockTable{
				Group:       0,
				Table:       staleLocalTable,
				OriginTable: staleLocalTable,
				ServiceID:   l1.serviceID,
				Version:     oldVersion,
				Valid:       true,
				AllocatorID: oldAllocatorID,
			}
			l1.tableGroups.set(0, staleLocalTable, l1.createLockTableByBind(staleLocalBind))

			staleRemoteBind := pb.LockTable{
				Group:       0,
				Table:       staleRemoteTable,
				OriginTable: staleRemoteTable,
				ServiceID:   "remote-service",
				Version:     oldVersion,
				Valid:       true,
				AllocatorID: oldAllocatorID,
			}
			l1.tableGroups.set(
				0,
				staleRemoteTable,
				newRemoteLockTable(
					l1.serviceID,
					time.Second,
					staleRemoteBind,
					l1.remote.client,
					l1.handleBindChanged,
					l1.logger))

			staleProxyBind := staleRemoteBind
			staleProxyBind.Table = staleProxyTable
			staleProxyBind.OriginTable = staleProxyTable
			staleProxy := l1.createLockTableByBind(staleProxyBind)
			_, ok := staleProxy.(*localLockTableProxy)
			require.True(t, ok)
			l1.tableGroups.set(0, staleProxyTable, staleProxy)

			alloc.mu.Lock()
			alloc.mu.services = make(map[string]*serviceBinds)
			alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
			alloc.allocatorID = "restarted-allocator-for-keepalive"
			alloc.version++
			restartedAllocatorID := alloc.allocatorID
			restartedVersion := alloc.version
			alloc.mu.Unlock()

			currentProxyBind := staleRemoteBind
			currentProxyBind.Table = currentProxyTable
			currentProxyBind.OriginTable = currentProxyTable
			currentProxyBind.Version = restartedVersion
			currentProxyBind.AllocatorID = restartedAllocatorID
			currentProxy := l1.createLockTableByBind(currentProxyBind)
			_, ok = currentProxy.(*localLockTableProxy)
			require.True(t, ok)
			l1.tableGroups.set(0, currentProxyTable, currentProxy)

			require.False(t, alloc.KeepLockTableBind(l1.serviceID))
			require.NotNil(t, l1.tableGroups.get(0, staleLocalTable))
			require.NotNil(t, l1.tableGroups.get(0, staleRemoteTable))
			require.NotNil(t, l1.tableGroups.get(0, staleProxyTable))
			require.NotNil(t, l1.tableGroups.get(0, currentProxyTable))

			l1.remote.keeper.(*lockTableKeeper).doKeepLockTableBind(ctx)

			require.Nil(t, l1.tableGroups.get(0, staleLocalTable))
			require.Nil(t, l1.tableGroups.get(0, staleRemoteTable))
			require.Nil(t, l1.tableGroups.get(0, staleProxyTable))
			preserved := l1.tableGroups.get(0, currentProxyTable)
			require.NotNil(t, preserved)
			require.Equal(t, currentProxyBind, preserved.getBind())
			require.Equal(t, restartedVersion, l1.lastAllocatorVersion)
			require.Equal(t, restartedAllocatorID, l1.lastAllocatorID)
		},
		func(cfg *Config) {
			cfg.EnableRemoteLocalProxy = true
		})
}

func TestKeepaliveOKFalseFencesActiveTxn(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			table := uint64(21511)
			txnID := newTestTxnID(1)
			option := newTestRowExclusiveOptions()
			_, err := l1.Lock(ctx, table, newTestRows(1), txnID, option)
			require.NoError(t, err)
			require.NotNil(t, l1.tableGroups.get(0, table))

			alloc.mu.Lock()
			alloc.mu.services = make(map[string]*serviceBinds)
			alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
			alloc.allocatorID = "restarted-allocator-for-ok-false-fence"
			alloc.version++
			alloc.mu.Unlock()
			require.False(t, alloc.KeepLockTableBind(l1.serviceID))

			l1.remote.keeper.(*lockTableKeeper).doKeepLockTableBind(ctx)
			require.Nil(t, l1.tableGroups.get(0, table))

			txn := l1.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, txn)
			txn.Lock()
			require.True(t, txn.bindChanged)
			txn.Unlock()

			_, err = l1.Lock(ctx, table, newTestRows(2), txnID, option)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			require.NoError(t, l1.Unlock(ctx, txnID, timestamp.Timestamp{}))
		},
	)
}

func TestAllocatorObserverRejectsSupersededGetBindResponse(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]

			oldAllocator := allocatorState{id: "old-allocator", version: 100}
			newAllocator := allocatorState{id: "new-allocator", version: 90}
			staleTable := uint64(21512)

			l1.allocatorVersionMu.Lock()
			l1.lastAllocatorID = oldAllocator.id
			l1.lastAllocatorVersion = oldAllocator.version
			l1.allocatorVersionMu.Unlock()

			requestAllocator := l1.allocatorStateSnapshot()
			_, accepted := l1.observeAllocatorStateWithHoldersFromSnapshot(
				"accept-new-allocator-test",
				newAllocator,
				allocatorState{},
				false,
				l1.tableGroups)
			require.True(t, accepted)
			require.Equal(t, newAllocator.id, l1.lastAllocatorID)
			require.Equal(t, newAllocator.version, l1.lastAllocatorVersion)

			_, accepted = l1.observeAllocatorStateWithHoldersFromSnapshot(
				"reject-stale-get-bind-test",
				oldAllocator,
				requestAllocator,
				true,
				l1.tableGroups)
			require.False(t, accepted)
			require.Equal(t, newAllocator.id, l1.lastAllocatorID)
			require.Equal(t, newAllocator.version, l1.lastAllocatorVersion)

			if accepted {
				staleBind := pb.LockTable{
					Group:       0,
					Table:       staleTable,
					OriginTable: staleTable,
					ServiceID:   l1.serviceID,
					Version:     oldAllocator.version,
					Valid:       true,
					AllocatorID: oldAllocator.id,
				}
				l1.tableGroups.set(0, staleTable, l1.createLockTableByBind(staleBind))
			}
			require.Nil(t, l1.tableGroups.get(0, staleTable))
		},
	)
}

func TestAllocatorPublishRejectsStaleBindAfterNewAllocatorObserved(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]

			oldAllocator := allocatorState{id: "old-allocator-publish", version: 100}
			newAllocator := allocatorState{id: "new-allocator-publish", version: 90}
			staleTable := uint64(21514)
			staleBind := pb.LockTable{
				Group:       0,
				Table:       staleTable,
				OriginTable: staleTable,
				ServiceID:   l1.serviceID,
				Version:     oldAllocator.version,
				Valid:       true,
				AllocatorID: oldAllocator.id,
			}

			l1.allocatorVersionMu.Lock()
			l1.lastAllocatorID = oldAllocator.id
			l1.lastAllocatorVersion = oldAllocator.version
			l1.allocatorVersionMu.Unlock()
			l1.tableGroups.set(0, staleTable, l1.createLockTableByBind(staleBind))
			requestAllocator := l1.allocatorStateSnapshot()

			_, accepted := l1.observeAllocatorStateWithHoldersFromSnapshot(
				"allocator-publish-race-new",
				newAllocator,
				allocatorState{},
				false,
				l1.tableGroups)
			require.True(t, accepted)
			require.Nil(t, l1.tableGroups.get(0, staleTable))

			lt, err := l1.publishLockTableBindFromAllocator(
				"allocator-publish-race-old",
				staleBind.Group,
				staleBind.Table,
				staleBind,
				oldAllocator,
				requestAllocator)
			require.ErrorIs(t, err, ErrLockTableBindChanged)
			require.Nil(t, lt)
			require.Nil(t, l1.tableGroups.get(0, staleTable))
			require.Equal(t, newAllocator.id, l1.lastAllocatorID)
			require.Equal(t, newAllocator.version, l1.lastAllocatorVersion)
		},
	)
}

func TestAllocatorPublishRejectsOverwriteAfterConcurrentBindChanged(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]

			allocator := allocatorState{id: "allocator-publish-current", version: 100}
			table := uint64(21515)
			delayedBind := pb.LockTable{
				Group:       0,
				Table:       table,
				OriginTable: table,
				ServiceID:   l1.serviceID,
				Version:     allocator.version,
				Valid:       true,
				AllocatorID: allocator.id,
			}
			freshBind := delayedBind
			freshBind.ServiceID = "fresh-service"
			freshBind.Version++

			l1.allocatorVersionMu.Lock()
			l1.lastAllocatorID = allocator.id
			l1.lastAllocatorVersion = allocator.version
			l1.allocatorVersionMu.Unlock()
			requestAllocator := l1.allocatorStateSnapshot()
			l1.handleBindChanged(freshBind)

			lt, err := l1.publishLockTableBindFromAllocator(
				"allocator-publish-current-race",
				delayedBind.Group,
				delayedBind.Table,
				delayedBind,
				allocator,
				requestAllocator)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			require.Nil(t, lt)
			require.Equal(t, freshBind, l1.tableGroups.get(0, table).getBind())
			require.Equal(t, allocator.id, l1.lastAllocatorID)
			require.Equal(t, allocator.version, l1.lastAllocatorVersion)
		},
	)
}

func TestGetLatestLockTableBindRejectsSupersededAllocator(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldAllocator := allocatorState{id: "old-latest-bind", version: 100}
			newAllocator := allocatorState{id: "new-latest-bind", version: 90}
			oldBind := pb.LockTable{
				Group:       0,
				Table:       1,
				OriginTable: 1,
				ServiceID:   "s2",
				Version:     oldAllocator.version,
				Valid:       true,
				AllocatorID: oldAllocator.id,
			}
			staleBind := oldBind
			staleBind.ServiceID = "s3"
			staleBind.Version++

			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = staleBind
					resp.GetBind.AllocatorID = oldAllocator.id
					resp.GetBind.AllocatorVersion = oldAllocator.version
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			logger := getLogger("")
			svc := &service{
				serviceID: "s1",
				logger:    logger,
			}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.allocatorVersionMu.Lock()
			svc.lastAllocatorID = newAllocator.id
			svc.lastAllocatorVersion = newAllocator.version
			svc.addSupersededAllocatorIDLocked(oldAllocator.id)
			svc.allocatorVersionMu.Unlock()

			latest, err := svc.GetLatestLockTableBind(oldBind)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			require.Equal(t, pb.LockTable{}, latest)
			require.Equal(t, newAllocator.id, svc.lastAllocatorID)
			require.Equal(t, newAllocator.version, svc.lastAllocatorVersion)
		},
	)
}

func TestAllocatorObserverBoundsSupersededAllocatorIDs(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]
			table := uint64(21513)
			currentBind := pb.LockTable{
				Group:       0,
				Table:       table,
				OriginTable: table,
				ServiceID:   l1.serviceID,
				Version:     1000,
				Valid:       true,
				AllocatorID: "allocator-0",
			}
			l1.tableGroups.set(0, table, l1.createLockTableByBind(currentBind))

			l1.allocatorVersionMu.Lock()
			l1.lastAllocatorID = currentBind.AllocatorID
			l1.lastAllocatorVersion = currentBind.Version
			l1.allocatorVersionMu.Unlock()

			var previous allocatorState
			for i := 1; i <= maxSupersededAllocatorIDs+3; i++ {
				previous = l1.allocatorStateSnapshot()
				next := allocatorState{
					id:      fmt.Sprintf("allocator-%d", i),
					version: currentBind.Version + uint64(i),
				}
				_, accepted := l1.observeAllocatorStateWithHoldersFromSnapshot(
					"allocator-restart-sequence-test",
					next,
					allocatorState{},
					false,
					l1.tableGroups)
				require.True(t, accepted)
				require.Equal(t, next.id, l1.lastAllocatorID)
				require.Equal(t, next.version, l1.lastAllocatorVersion)
				require.LessOrEqual(t, len(l1.supersededAllocatorIDs), maxSupersededAllocatorIDs)
				require.LessOrEqual(t, len(l1.supersededAllocatorIDOrder), maxSupersededAllocatorIDs)
			}

			_, accepted := l1.observeAllocatorStateWithHoldersFromSnapshot(
				"allocator-restart-superseded-test",
				previous,
				l1.allocatorStateSnapshot(),
				true,
				l1.tableGroups)
			require.False(t, accepted)
			require.Equal(t, fmt.Sprintf("allocator-%d", maxSupersededAllocatorIDs+3), l1.lastAllocatorID)
		},
	)
}

func TestAllocatorIDDistinguishesBindIdentity(t *testing.T) {
	bindA := pb.LockTable{
		Group:       1,
		Table:       2,
		OriginTable: 3,
		ServiceID:   "s1",
		Version:     4,
		Sharding:    pb.Sharding_ByRow,
		AllocatorID: "allocator-a",
	}
	bindB := bindA
	bindB.AllocatorID = "allocator-b"

	require.True(t, bindA.Changed(bindB))
	require.False(t, bindA.Equal(bindB))
	require.NotEqual(t,
		getRemoteLockBindKey("remote-service", bindA),
		getRemoteLockBindKey("remote-service", bindB))
}

func TestAllocatorObserverPurgesRemoteAndProxyLockTables(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]
			allocatorVersion := uint64(100)
			remoteTable := uint64(22001)
			proxyTable := uint64(22002)

			remoteBind := pb.LockTable{
				Group:       0,
				Table:       remoteTable,
				OriginTable: remoteTable,
				ServiceID:   "s2",
				Version:     allocatorVersion - 1,
				Valid:       true,
			}
			l1.tableGroups.set(
				0,
				remoteTable,
				newRemoteLockTable(
					l1.serviceID,
					time.Second,
					remoteBind,
					l1.remote.client,
					l1.handleBindChanged,
					l1.logger))

			proxyBind := remoteBind
			proxyBind.Table = proxyTable
			proxyBind.OriginTable = proxyTable
			proxy := l1.createLockTableByBind(proxyBind)
			_, ok := proxy.(*localLockTableProxy)
			require.True(t, ok)
			l1.tableGroups.set(0, proxyTable, proxy)

			removed := l1.observeAllocatorVersion("remote-proxy-test", allocatorVersion)
			require.Equal(t, 2, removed)
			require.Nil(t, l1.tableGroups.get(0, remoteTable))
			require.Nil(t, l1.tableGroups.get(0, proxyTable))
		},
		func(cfg *Config) {
			cfg.EnableRemoteLocalProxy = true
		})
}

func TestAllocatorObserverConcurrentKeepaliveAndGetBind(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			staleTable := uint64(23001)
			freshTable := uint64(23002)
			option := newTestRowExclusiveOptions()
			_, err := l1.Lock(ctx, staleTable, newTestRows(1), newTestTxnID(1), option)
			require.NoError(t, err)
			require.NoError(t, l1.Unlock(ctx, newTestTxnID(1), timestamp.Timestamp{}))
			require.NotNil(t, l1.tableGroups.get(0, staleTable))

			alloc.mu.Lock()
			alloc.mu.services = make(map[string]*serviceBinds)
			alloc.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
			alloc.version++
			restartedVersion := alloc.version
			alloc.mu.Unlock()

			var wg sync.WaitGroup
			errC := make(chan error, 1)
			wg.Add(3)
			go func() {
				defer wg.Done()
				l1.observeAllocatorVersion("concurrent-test", restartedVersion)
			}()
			go func() {
				defer wg.Done()
				l1.remote.keeper.(*lockTableKeeper).doKeepLockTableBind(ctx)
			}()
			go func() {
				defer wg.Done()
				_, err := l1.Lock(ctx, freshTable, newTestRows(2), newTestTxnID(2), option)
				if err != nil {
					errC <- err
					return
				}
				if err := l1.Unlock(ctx, newTestTxnID(2), timestamp.Timestamp{}); err != nil {
					errC <- err
				}
			}()
			wg.Wait()
			close(errC)
			require.NoError(t, <-errC)
			require.Nil(t, l1.tableGroups.get(0, staleTable))
			require.Equal(t, restartedVersion, l1.lastAllocatorVersion)
		},
	)
}

func TestAllocatorObserverCloseWaitersOnStaleLocalBind(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			table := uint64(24001)
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			option := newTestRowExclusiveOptions()

			_, err := l1.Lock(ctx, table, rows, txn1, option)
			require.NoError(t, err)
			staleLT := l1.tableGroups.get(0, table)
			require.NotNil(t, staleLT)
			staleBind := staleLT.getBind()

			errC := make(chan error, 1)
			go func() {
				_, err := l1.Lock(ctx, table, rows, txn2, option)
				errC <- err
			}()
			require.NoError(t, waitLocalWaiters(staleLT.(*localLockTable), rows[0], 1))

			removed := l1.observeAllocatorVersion("waiter-close-test", staleBind.Version+1)
			require.Equal(t, 1, removed)
			require.Nil(t, l1.tableGroups.get(0, table))

			select {
			case err := <-errC:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound), err)
			case <-ctx.Done():
				t.Fatal("waiter was not notified by stale bind purge")
			}

			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			require.NoError(t, l1.Unlock(ctx, txn2, timestamp.Timestamp{}))
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

func TestIssue5543(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
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

			oldVersion := l1.tableGroups.getVersion()
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{2}, {3}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			newVersion := l1.tableGroups.getVersion()
			if oldVersion == newVersion {
				// Heartbeat
				l1.tableGroups.removeWithFilter(func(_ uint64, v lockTable) bool {
					bind := v.getBind()
					if bind.ServiceID == l1.serviceID {
						return true
					}
					return false
				}, closeReasonBindChanged)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err1 := l1.Lock(
					ctx,
					0,
					[][]byte{{2}},
					[]byte("txn3"),
					option)
				require.NoError(t, err1)
			}()

			waitWaiters(t, l1, 0, []byte{2}, 1)
			err = l1.Unlock(
				ctx,
				[]byte("txn2"),
				timestamp.Timestamp{})
			require.NoError(t, err)
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

func TestIssue19913(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			err := os.Setenv("mo_reuse_enable_checker", "true")
			require.NoError(t, err)
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

			_, err = l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			_, err = l1.Lock(
				ctx,
				0,
				[][]byte{{2}},
				[]byte("txn2"),
				option)
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				// blocked by txn1
				_, err := l1.Lock(
					ctx,
					0,
					newTestRows(1, 2),
					[]byte("txn3"),
					option)
				require.NoError(t, err)
			}()

			waitWaiters(t, l1, 0, []byte{1}, 1)

			w := l1.activeTxnHolder.getActiveTxn([]byte("txn3"), false, "").blockedWaiters[0]

			require.NoError(t, l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{}))

			waitWaiters(t, l1, 0, []byte{2}, 1)

			require.NoError(t, l1.Unlock(
				ctx,
				[]byte("txn2"),
				timestamp.Timestamp{}))
			wg.Wait()

			require.NoError(t, l1.Unlock(
				ctx,
				[]byte("txn3"),
				timestamp.Timestamp{}))

			require.Less(t, w.refCount.Load(), int32(2))
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

			s.cfg.TxnIterFunc = func(f func([]byte) bool) {
				f(txn1)
				f(txn2)
			}

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

			s.cfg.TxnIterFunc = func(f func([]byte) bool) {
				f(txn1)
				f(txn2)
				f(txn3)
			}

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

					s.cfg.TxnIterFunc = func(f func([]byte) bool) {
						f(txn1)
						f(txn2)
						f(txn3)
					}

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
					new := newLocalLockTable(bind, s.fsp, s.events, s.clock, s.activeTxnHolder, getLogger(s.GetConfig().ServiceID))
					s.tableGroups.set(0, table, new)
					lt.close(closeReasonBindChanged)

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
					l := s.tableGroups.get(1, ShardingByRow(rows1[0]))
					assert.Equal(t, table, l.getBind().OriginTable)
					checkLock(t, l.(*localLockTable), rows1[0], [][]byte{txn1}, nil, nil)

					// txn2 get lock, shared
					_, err = s.Lock(ctx, table, rows2, txn2, option)
					require.NoError(t, err)
					l = s.tableGroups.get(1, ShardingByRow(rows2[0]))
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
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
		},
	)
}

func TestBindChangedFencesActiveTxnHoldingOldBind(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, ss []*service) {
			s := ss[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			option := newTestRowExclusiveOptions()
			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			_, err := s.Lock(ctx, 0, [][]byte{{1}}, txn1, option)
			require.NoError(t, err)
			_, err = s.Lock(ctx, 1, [][]byte{{1}}, txn2, option)
			require.NoError(t, err)

			oldBind := s.tableGroups.get(0, 0).getBind()
			newBind := oldBind
			newBind.Version++
			newBind.ServiceID = "new-service"
			s.handleBindChanged(newBind)

			_, err = s.Lock(ctx, 1, [][]byte{{2}}, txn1, option)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			_, err = s.Lock(ctx, 1, [][]byte{{2}}, txn2, option)
			require.NoError(t, err)

			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
		},
	)
}

func TestBindChangedFencesActiveTxnAfterOldTableRemoved(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, ss []*service) {
			s := ss[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txnID := []byte("txn1")
			_, err := s.Lock(ctx, 0, [][]byte{{1}}, txnID, newTestRowExclusiveOptions())
			require.NoError(t, err)

			oldBind := s.tableGroups.get(0, 0).getBind()
			s.tableGroups.removeWithFilter(func(table uint64, lt lockTable) bool {
				return table == oldBind.Table
			}, closeReasonBindChanged)

			newBind := oldBind
			newBind.Version++
			newBind.ServiceID = "new-service"
			s.handleBindChanged(newBind)

			_, err = s.Lock(ctx, 1, [][]byte{{1}}, txnID, newTestRowExclusiveOptions())
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
		},
	)
}

func TestBindChangedBeforeLockSuccessReturnsBindChanged(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, ss []*service) {
			s := ss[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txnID := []byte("txn1")
			txn := s.activeTxnHolder.getActiveTxn(txnID, true, "")
			fenceDone := make(chan struct{})
			var once sync.Once
			txn.beforeLockAdded = func([]byte, [][]byte) error {
				once.Do(func() {
					oldBind := s.tableGroups.get(0, 0).getBind()
					newBind := oldBind
					newBind.Version++
					newBind.ServiceID = "new-service"
					go func() {
						s.handleBindChanged(newBind)
						close(fenceDone)
					}()
					require.Eventually(t, func() bool {
						if s.bindChangeMu.TryRLock() {
							s.bindChangeMu.RUnlock()
							return false
						}
						return true
					}, time.Second, time.Millisecond)
				})
				return nil
			}

			_, err := s.Lock(ctx, 0, [][]byte{{1}}, txnID, newTestRowExclusiveOptions())
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			require.Eventually(t, func() bool {
				select {
				case <-fenceDone:
					return true
				default:
					return false
				}
			}, time.Second, time.Millisecond)
			require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
		},
	)
}

func TestLeakWaiterForErr(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			_ = os.Setenv("mo_reuse_enable_checker", "true")

			tableID := uint64(20)
			row8 := [][]byte{{8}}
			row5 := [][]byte{{5}}
			row2 := [][]byte{{2}}
			rng19 := newTestRows(1, 9)

			txn1 := []byte("rt1")
			txn2 := []byte("rt2")
			txn3 := []byte("rt3")
			txn4 := []byte("rt4")

			ll, err := l1.getLockTableWithCreate(0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)
			lt := ll.(*localLockTable)
			lt.options.afterWait = func(c *lockContext) func() {
				if c.opts.Granularity == pb.Granularity_Range {
					time.Sleep(time.Second)
				}
				return func() {}
			}
			txn := lt.txnHolder.getActiveTxn(txn4, true, "")
			txn.Lock()
			txn.beforeLockAdded = func(id []byte, locks [][]byte) error {
				return ErrTxnNotFound
			}
			txn.Unlock()

			_, err = l1.Lock(ctx, tableID, row8, txn1, newTestRowExclusiveOptions())
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = l1.Lock(ctx, tableID, rng19, txn4, newTestRangeExclusiveOptions())
			}()

			require.NoError(t, WaitWaiters(l1, 0, tableID, row8[0], 1))

			var wt *waiter
			lt.mu.Lock()
			lock, _ := lt.mu.store.Get(row8[0])
			lock.waiters.iter(func(w *waiter) bool {
				wt = w
				return false
			})
			lt.mu.Unlock()

			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			_, err = l1.Lock(ctx, tableID, row5, txn2, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, WaitWaiters(l1, 0, tableID, row5[0], 1))
			require.NoError(t, l1.Unlock(ctx, txn2, timestamp.Timestamp{}))

			_, err = l1.Lock(ctx, tableID, row2, txn3, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, WaitWaiters(l1, 0, tableID, row2[0], 1))
			require.NoError(t, l1.Unlock(ctx, txn3, timestamp.Timestamp{}))

			wg.Wait()
			require.NoError(t, l1.Unlock(ctx, txn4, timestamp.Timestamp{}))

			time.Sleep(500 * time.Millisecond)
			require.NotEqual(t, int32(1), wt.refCount.Load())
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
					writeResponse(getLogger(s1.GetConfig().ServiceID), cf, r2, ErrTxnNotFound, cs)
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

func TestLockWaitTimeout(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// Use a long-lived context so it doesn't interfere with LockWaitTimeout.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			// txn1 acquires and holds the lock.
			_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn1"), option)
			require.NoError(t, err)

			// txn2 tries to lock the same row with a 1-second LockWaitTimeout.
			option2 := option
			option2.LockWaitTimeout = 1 // 1 second

			start := time.Now()
			_, err = l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn2"), option2)
			elapsed := time.Since(start)

			// Should time out after ~1 second, not immediately and not indefinitely.
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState),
				"expected lock-timeout error (ErrInvalidState), got %v", err)
			require.Contains(t, err.Error(), "lock timeout")
			require.GreaterOrEqual(t, elapsed, time.Second)
			require.Less(t, elapsed, 3*time.Second)
		},
	)
}

func TestLockWaitTimeoutDefaultNoTimeout(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// Short context to bound the test.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// txn1 holds the lock.
			_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn1"), option)
			require.NoError(t, err)

			// txn2 tries to lock the same row WITHOUT LockWaitTimeout.
			// Should be blocked until ctx expires (no internal/default timeout interception).
			option2 := option
			option2.LockWaitTimeout = 0 // no session/internal timeout; rely on caller context deadline
			start := time.Now()
			_, err = l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn2"), option2)
			elapsed := time.Since(start)

			// Should have waited for the context timeout (~2s), not failed immediately.
			require.Error(t, err)
			require.GreaterOrEqual(t, elapsed, time.Second)
		},
	)
}

func TestLockWaitTimeoutSucceedsWhenHolderReleases(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			// txn1 holds the lock briefly then releases.
			_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn1"), option)
			require.NoError(t, err)

			option2 := option
			option2.LockWaitTimeout = 3 // 3 seconds, more than enough

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(200 * time.Millisecond)
				require.NoError(t, l.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))
			}()

			start := time.Now()
			_, err = l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn2"), option2)
			elapsed := time.Since(start)

			wg.Wait()
			require.NoError(t, err)
			require.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
			require.Less(t, elapsed, 2*time.Second) // well within LockWaitTimeout
		},
	)
}

func TestLockWaitTimeoutZeroMeansFallbackToContext(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// Short context to trigger fast timeout.
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			// txn1 holds the lock.
			_, err := l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn1"), option)
			require.NoError(t, err)

			// txn2 with LockWaitTimeout=0 should wait for context expiry (500ms),
			// NOT the default 5-minute configLockWaitTimeout.
			option2 := option
			option2.LockWaitTimeout = 0
			start := time.Now()
			_, err = l.Lock(ctx, 0, [][]byte{{1}}, []byte("txn2"), option2)
			elapsed := time.Since(start)

			require.Error(t, err)
			// Should have completed near the context deadline, not after 5 minutes.
			require.GreaterOrEqual(t, elapsed, 300*time.Millisecond)
			require.Less(t, elapsed, 2*time.Second)
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

	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
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
		},
	)
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

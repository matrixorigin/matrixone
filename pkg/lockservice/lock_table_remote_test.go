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
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func TestLockRemote(t *testing.T) {
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			txn.Lock()
			defer txn.Unlock()
			l.lock(ctx, txn, [][]byte{{1}}, LockOptions{}, func(r pb.Result, err error) {
				assert.NoError(t, err)
			})
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestIssue20747(t *testing.T) {
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, io.EOF, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			txn.Lock()
			defer txn.Unlock()
			l.lock(ctx, txn, [][]byte{{1}}, LockOptions{}, func(r pb.Result, err error) {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
			})
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestLockRemoteWithContextTimeoutTracksLockForUnlock(t *testing.T) {
	unlockCalled := make(chan struct{})
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					// Simulate a slow or dropped response. The server just waits for
					// the client context to expire.
					<-ctx.Done()
				},
			)
			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					close(unlockCalled)
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn-timeout")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			closed := false
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			txn.Lock()
			defer func() {
				txn.Unlock()
				if !closed {
					reuse.Free(txn, nil)
				}
			}()

			l.lock(ctx, txn, [][]byte{{1}}, LockOptions{}, func(r pb.Result, err error) {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
			})
			holder := txn.getHoldLocksLocked(l.bind.Group)
			require.Contains(t, holder.tableKeys, l.bind.Table)
			require.Contains(t, holder.tableBinds, l.bind.Table)

			require.NoError(t, txn.close(txnID, timestamp.Timestamp{}, func(uint32, uint64) (lockTable, error) {
				return l, nil
			}, l.logger))
			closed = true
			select {
			case <-unlockCalled:
			default:
				require.Fail(t, "expected remote unlock")
			}
		},
		func(lt pb.LockTable) {},
	)
}

func TestLockRemoteWithCanceledContextBeforeSendSkipsRemoteTracking(t *testing.T) {
	bind := pb.LockTable{ServiceID: "s1", Valid: true}
	lockCalled := make(chan struct{}, 1)
	unlockCalled := make(chan struct{}, 1)
	runRemoteLockTableTests(
		t,
		bind,
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					lockCalled <- struct{}{}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
			s.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = bind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					unlockCalled <- struct{}{}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn-canceled-before-send")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			closed := false
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			txn.Lock()
			defer func() {
				txn.Unlock()
				if !closed {
					reuse.Free(txn, nil)
				}
			}()

			l.lock(ctx, txn, [][]byte{{1}}, LockOptions{}, func(r pb.Result, err error) {
				require.Error(t, err)
			})
			holder := txn.getHoldLocksLocked(l.bind.Group)
			require.NotContains(t, holder.tableKeys, l.bind.Table)
			require.NotContains(t, holder.tableBinds, l.bind.Table)

			require.NoError(t, txn.close(txnID, timestamp.Timestamp{}, func(uint32, uint64) (lockTable, error) {
				return l, nil
			}, l.logger))
			closed = true
			select {
			case <-unlockCalled:
				require.Fail(t, "remote unlock should not be sent when the lock request was never attempted")
			default:
			}
			select {
			case <-lockCalled:
				require.Fail(t, "lock request should not be sent after caller context is already canceled")
			default:
			}
		},
		func(lt pb.LockTable) {},
	)
}

func TestRemoteNewBindFromNewAllocatorPurgesStaleBinds(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			logger := getLogger("")
			oldAllocator := allocatorState{id: "old-allocator", version: 100}
			newAllocator := allocatorState{id: "new-allocator", version: 90}
			staleA := pb.LockTable{
				Group:       0,
				Table:       1,
				OriginTable: 1,
				ServiceID:   "old-service",
				Version:     oldAllocator.version,
				Valid:       true,
				AllocatorID: oldAllocator.id,
			}
			currentA := pb.LockTable{
				Group:       0,
				Table:       3,
				OriginTable: 3,
				ServiceID:   "new-service-a",
				Version:     newAllocator.version,
				Valid:       true,
				AllocatorID: newAllocator.id,
			}
			oldB := pb.LockTable{
				Group:       0,
				Table:       2,
				OriginTable: 2,
				ServiceID:   "old-service",
				Version:     oldAllocator.version,
				Valid:       true,
				AllocatorID: oldAllocator.id,
			}
			newB := oldB
			newB.ServiceID = "new-service-b"
			newB.Version = newAllocator.version + 1
			newB.AllocatorID = newAllocator.id

			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = newB
					resp.GetBind.AllocatorID = newAllocator.id
					resp.GetBind.AllocatorVersion = newAllocator.version
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)

			svc := &service{
				serviceID: "s1",
				logger:    logger,
			}
			svc.tableGroups = &lockTableHolders{
				service: svc.serviceID,
				logger:  logger,
				holders: map[uint32]*lockTableHolder{},
			}
			svc.lastAllocatorID = oldAllocator.id
			svc.lastAllocatorVersion = oldAllocator.version
			svc.tableGroups.set(staleA.Group, staleA.Table, newRemoteLockTable(
				svc.serviceID,
				time.Second,
				staleA,
				nil,
				svc.handleBindChanged,
				logger,
			))
			svc.tableGroups.set(currentA.Group, currentA.Table, newRemoteLockTable(
				svc.serviceID,
				time.Second,
				currentA,
				nil,
				svc.handleBindChanged,
				logger,
			))
			remote := newRemoteLockTable(
				svc.serviceID,
				time.Second,
				oldB,
				c,
				svc.handleBindChanged,
				logger,
			)
			remote.allocatorStateProvider = svc.allocatorStateSnapshot
			remote.allocatorBindChangedHandler = svc.handleBindChangedFromAllocator

			err := remote.maybeHandleBindChanged(&pb.Response{NewBind: &newB})
			require.ErrorIs(t, err, ErrLockTableBindChanged)
			require.Nil(t, svc.tableGroups.get(staleA.Group, staleA.Table))
			require.NotNil(t, svc.tableGroups.get(currentA.Group, currentA.Table))
			require.NotNil(t, svc.tableGroups.get(newB.Group, newB.Table))
			require.Equal(t, newAllocator.id, svc.lastAllocatorID)
			require.Equal(t, newAllocator.version, svc.lastAllocatorVersion)

			removed, accepted := svc.observeAllocatorStateWithHoldersFromSnapshot(
				"remote-new-bind-same-epoch",
				newAllocator,
				allocatorState{},
				false,
				svc.tableGroups)
			require.True(t, accepted)
			require.Zero(t, removed)
			require.NotNil(t, svc.tableGroups.get(currentA.Group, currentA.Table))
		},
	)
}

func TestLockRemoteWithNeedUpgrade(t *testing.T) {
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(4), "")
			rows := newTestRows(1, 2, 3, 4, 5)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			txn.Lock()
			defer txn.Unlock()
			l.lock(ctx, txn, rows, LockOptions{}, func(r pb.Result, err error) {
				assert.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade))
			})
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestUnlockRemote(t *testing.T) {
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			l.unlock(txn, nil, timestamp.Timestamp{})
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestUnlockRemoteWithRetry(t *testing.T) {
	n := 0
	c := make(chan struct{})
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					n++
					if n == 1 {
						writeResponse(getLogger(""), cancel, resp, moerr.NewRPCTimeout(ctx), cs)
						return
					}
					close(c)
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
			s.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = pb.LockTable{
						ServiceID: "s1",
						Valid:     true,
					}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			l.unlock(txn, nil, timestamp.Timestamp{})
			<-c
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestRemoteBindRefreshRejectsSupersededAllocatorBind(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldAllocator := allocatorState{id: "old-remote-refresh", version: 100}
			newAllocator := allocatorState{id: "new-remote-refresh", version: 90}
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
				},
			)

			logger := getLogger("")
			svc := &service{
				serviceID: "s1",
				logger:    logger,
			}
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.allocatorVersionMu.Lock()
			svc.lastAllocatorID = newAllocator.id
			svc.lastAllocatorVersion = newAllocator.version
			svc.addSupersededAllocatorIDLocked(oldAllocator.id)
			svc.allocatorVersionMu.Unlock()

			l := newRemoteLockTable(svc.serviceID, time.Second, oldBind, c, svc.handleBindChanged, logger)
			l.allocatorStateProvider = svc.allocatorStateSnapshot
			l.allocatorBindChangedHandler = svc.handleBindChangedFromAllocator

			err := l.handleError(moerr.NewRPCTimeoutNoCtx(), true)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			require.Nil(t, svc.tableGroups.get(oldBind.Group, oldBind.Table))
			require.Equal(t, newAllocator.id, svc.lastAllocatorID)
			require.Equal(t, newAllocator.version, svc.lastAllocatorVersion)
		},
	)
}

func TestRemoteRetryBackoffCapped(t *testing.T) {
	oldInitial := remoteRetryInitialBackoff
	oldMaxBackoff := remoteRetryMaxBackoff
	remoteRetryInitialBackoff = time.Millisecond
	remoteRetryMaxBackoff = 5 * time.Millisecond
	defer func() {
		remoteRetryInitialBackoff = oldInitial
		remoteRetryMaxBackoff = oldMaxBackoff
	}()

	assert.Equal(t, 2*time.Millisecond, nextRemoteRetryBackoff(time.Millisecond))
	assert.Equal(t, 5*time.Millisecond, nextRemoteRetryBackoff(4*time.Millisecond))
	assert.Equal(t, time.Millisecond, nextRemoteRetryBackoff(0))
}

func TestGetLockRemoteWithRetry(t *testing.T) {
	oldInitial := remoteRetryInitialBackoff
	oldMaxBackoff := remoteRetryMaxBackoff
	remoteRetryInitialBackoff = time.Millisecond
	remoteRetryMaxBackoff = time.Millisecond
	defer func() {
		remoteRetryInitialBackoff = oldInitial
		remoteRetryMaxBackoff = oldMaxBackoff
	}()

	n := 0
	called := false
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_GetTxnLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					n++
					if n == 1 {
						writeResponse(getLogger(""), cancel, resp, moerr.NewRPCTimeout(ctx), cs)
						return
					}
					resp.GetTxnLock.Value = int32(pb.Granularity_Row)
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
			s.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = pb.LockTable{
						ServiceID: "s1",
						Valid:     true,
					}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			l.getLock([]byte("row1"), pb.WaitTxn{TxnID: []byte("txn1")}, func(lock Lock) {
				called = true
				assert.Equal(t, byte(pb.Granularity_Row), lock.value)
			})
			assert.True(t, called)
			assert.Equal(t, 2, n)
		},
		func(lt pb.LockTable) {},
	)
}

func TestRemoteWithBindChanged(t *testing.T) {
	newBind := pb.LockTable{
		ServiceID: "s2",
		Table:     1,
		Version:   2,
	}

	c := make(chan pb.LockTable, 1)
	defer close(c)
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1", Table: 1, Version: 1},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)

			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)

			s.RegisterMethodHandler(
				pb.Method_GetTxnLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			txn.Lock()
			l.lock(ctx, txn, [][]byte{{1}}, LockOptions{}, func(r pb.Result, err error) {
				assert.Error(t, ErrLockTableBindChanged, err)
			})
			assert.Equal(t, newBind, <-c)
			txn.Unlock()

			l.unlock(txn, nil, timestamp.Timestamp{})
			assert.Equal(t, newBind, <-c)

			l.getLock(txnID, pb.WaitTxn{TxnID: []byte{1}}, nil)
			assert.Equal(t, newBind, <-c)
			reuse.Free(txn, nil)
		},
		func(bind pb.LockTable) {
			c <- bind
		},
	)
}

func TestRetryRemoteLockError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "net timeout error",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded},
			want: true,
		},
		{
			name: "io EOF error",
			err:  io.EOF,
			want: true,
		},
		{
			name: "io unexpected EOF error",
			err:  io.ErrUnexpectedEOF,
			want: true,
		},
		{
			name: "os deadline exceeded error",
			err:  os.ErrDeadlineExceeded,
			want: true,
		},
		{
			name: "context deadline exceeded error",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "moerr unexpected EOF error",
			err:  moerr.NewUnexpectedEOF(context.Background(), "test"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryRemoteLockError(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func runRemoteLockTableTests(
	t *testing.T,
	binding pb.LockTable,
	register func(s Server),
	fn func(l *remoteLockTable, s Server),
	changed func(pb.LockTable)) {
	defer leaktest.AfterTest(t)()
	runRPCTests(t, func(c Client, s Server) {
		register(s)
		l := newRemoteLockTable(
			"",
			time.Second,
			binding,
			c,
			changed,
			getLogger(""),
		)
		defer l.close()
		fn(l, s)
	})
}

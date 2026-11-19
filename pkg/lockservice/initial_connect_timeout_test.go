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
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// connectTimeoutConsumerFactory changes only the connection establishment leg
// of one peer. All allocator RPCs and the recovered peer use real backends.
// TLS against a plain, unresponsive localhost listener produces a real
// net.Error.Timeout without depending on firewall privileges or external IPs.
type connectTimeoutConsumerFactory struct {
	ordinary morpc.ContextBackendFactory
	timeout  morpc.ContextBackendFactory
	target   string
	stalled  string
	entered  chan struct{}
	release  chan struct{}
	retried  chan struct{}
	attempts atomic.Int32
	healthy  atomic.Bool
	gateOnce sync.Once
}

func (f *connectTimeoutConsumerFactory) Create(
	remote string,
	opts ...morpc.BackendOption,
) (morpc.Backend, error) {
	return f.CreateWithContext(context.Background(), remote, opts...)
}

func (f *connectTimeoutConsumerFactory) CreateWithContext(
	ctx context.Context,
	remote string,
	opts ...morpc.BackendOption,
) (morpc.Backend, error) {
	if remote != f.target || f.healthy.Load() {
		return f.ordinary.CreateWithContext(ctx, remote, opts...)
	}
	if f.attempts.Add(1) == 1 {
		close(f.entered)
	} else {
		select {
		case f.retried <- struct{}{}:
		default:
		}
	}
	select {
	case <-f.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return f.timeout.CreateWithContext(ctx, f.stalled, opts...)
}

func (f *connectTimeoutConsumerFactory) releaseGate() {
	f.gateOnce.Do(func() { close(f.release) })
}

type connectTimeoutBindObserver struct {
	Client
	refreshed chan pb.LockTable
	refreshes atomic.Int32
}

func (c *connectTimeoutBindObserver) Send(
	ctx context.Context,
	req *pb.Request,
) (*pb.Response, error) {
	resp, err := c.Client.Send(ctx, req)
	if req.Method == pb.Method_GetBind && err == nil {
		c.refreshes.Add(1)
		select {
		case c.refreshed <- resp.GetBind.LockTable:
		default:
		}
	}
	return resp, err
}

func TestRemoteInitialConnectTimeoutReleasesDrainAdmission(t *testing.T) {
	for _, changeBind := range []bool{false, true} {
		name := "uncertain-timeout-preserves-owner"
		if changeBind {
			name = "authoritative-bind-change-fences-transaction"
		}
		t.Run(name, func(t *testing.T) {
			runLockServiceTestsWithAdjustConfig(t, []string{"connect-owner", "connect-caller"}, time.Hour,
				func(alloc *lockTableAllocator, services []*service) {
					owner, caller := services[0], services[1]
					const tableID = uint64(3090201)
					rows := newTestRows(1)
					options := newTestRowExclusiveOptions()
					ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
					defer cancel()
					guard, cancelGuard := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancelGuard()

					// Publish and cache the owner without opening a caller-to-owner
					// data connection. The pending Lock therefore exercises creation,
					// not an established connection's response/read timeout.
					warmTxn := []byte("connect-owner-warmup")
					_, err := owner.Lock(guard, tableID, rows, warmTxn, options)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(guard, warmTxn, timestamp.Timestamp{}))
					table, err := caller.getLockTableWithCreate(guard, 0, tableID, rows, pb.Sharding_None)
					require.NoError(t, err)
					remote, ok := table.(*remoteLockTable)
					require.True(t, ok)
					oldBind := remote.getBind()

					listener, err := net.Listen("tcp4", "127.0.0.1:0")
					require.NoError(t, err)
					stopListener := make(chan struct{})
					listenerDone := make(chan struct{})
					accepted := make(chan error, 1)
					go func() {
						defer close(listenerDone)
						conn, err := listener.Accept()
						accepted <- err
						if err != nil {
							return
						}
						defer conn.Close()
						<-stopListener
					}()
					defer func() {
						close(stopListener)
						require.NoError(t, listener.Close())
						<-listenerDone
					}()

					// A private client avoids replacing the service's shared client
					// while its keepers or recovery workers could be using it.
					private, err := NewClient(caller.cfg.ServiceID, morpc.Config{})
					require.NoError(t, err)
					defer func() { require.NoError(t, private.Close()) }()
					concrete := private.(*client)
					codec := morpc.NewMessageCodec(caller.cfg.ServiceID,
						func() morpc.Message { return acquireResponse() },
						morpc.WithCodecEnableChecksum())
					factory := &connectTimeoutConsumerFactory{
						ordinary: morpc.NewGoettyBasedBackendFactory(codec,
							concrete.cfg.BackendOptions...).(morpc.ContextBackendFactory),
						timeout: morpc.NewGoettyBasedBackendFactory(codec,
							morpc.WithBackendConnectTimeout(200*time.Millisecond),
							morpc.WithBackendGoettyOptions(goetty.WithSessionTLS(&tls.Config{
								InsecureSkipVerify: true, // Test-owned listener never presents a certificate.
							}))).(morpc.ContextBackendFactory),
						target:  owner.cfg.ListenAddress,
						stalled: listener.Addr().String(),
						entered: make(chan struct{}),
						release: make(chan struct{}),
						retried: make(chan struct{}, 1),
					}
					transport, err := morpc.NewClient(t.Name(), factory, concrete.cfg.ClientOptions...)
					require.NoError(t, err)
					require.NoError(t, concrete.client.Close())
					concrete.client = transport
					observer := &connectTimeoutBindObserver{Client: private, refreshed: make(chan pb.LockTable, 1)}
					originalClient := remote.client
					remote.client = observer

					txnID := []byte("initial-connect-timeout")
					lockDone := make(chan error, 1)
					finished := false
					defer func() {
						// Failures must not strand an hour-long Lock or let unbounded
						// Unlock cleanup retry the deliberately stalled route.
						cancel()
						factory.healthy.Store(true)
						factory.releaseGate()
						cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cleanupCancel()
						if !finished {
							select {
							case <-lockDone:
							case <-cleanupCtx.Done():
								t.Error("canceled Lock did not release its admission")
								return // Do not mutate remote.client while Lock can still read it.
							}
						}
						remote.client = originalClient
						require.NoError(t, caller.UnlockWithContext(cleanupCtx, txnID, timestamp.Timestamp{}))
					}()
					go func() {
						_, err := caller.Lock(ctx, tableID, rows, txnID, options)
						lockDone <- err
					}()
					select {
					case <-factory.entered:
					case <-guard.Done():
						t.Fatal("ordinary remote Lock did not reach the initial connection factory")
					}

					caller.checkCanMoveGroupTables()
					require.True(t, caller.isStatus(pb.Status_ServiceLockWaiting))
					caller.mu.RLock()
					admissions, snapshotReady := caller.mu.lockAdmissions, caller.mu.drainSnapshotReady
					caller.mu.RUnlock()
					require.Equal(t, uint64(1), admissions)
					require.False(t, snapshotReady, "pre-drain Lock must hold the snapshot gate")
					require.NotNil(t, caller.activeTxnHolder.getActiveTxn(txnID, false, ""))
					if changeBind {
						// This is independent authoritative retirement, not a health
						// verdict inferred from the ambiguous connect timeout.
						alloc.disableTableBinds(alloc.getServiceBinds(owner.serviceID))
					}
					factory.releaseGate()
					select {
					case err := <-accepted:
						require.NoError(t, err)
					case <-guard.Done():
						t.Fatal("initial connection did not reach the real localhost listener")
					}

					var lockErr error
					select {
					case lockErr = <-lockDone:
						finished = true
					case <-factory.retried:
						t.Fatal("completed initial-connect timeout was hidden by another factory retry")
					case <-guard.Done():
						t.Fatal("initial-connect timeout did not return to the lockservice consumer")
					}
					require.NoError(t, ctx.Err(), "the long lock context must remain live")
					var refreshed pb.LockTable
					select {
					case refreshed = <-observer.refreshed:
					default:
						t.Fatal("returned transport error did not refresh the real allocator binding")
					}
					require.Equal(t, int32(1), observer.refreshes.Load())
					if changeBind {
						require.ErrorIs(t, lockErr, ErrLockTableBindChanged)
						require.True(t, refreshed.Changed(oldBind))
					} else {
						require.True(t, moerr.IsMoErrCode(lockErr, moerr.ErrRPCTimeout), lockErr)
						require.True(t, isRetryError(lockErr), "timeout is not authoritative owner-death evidence")
						require.Equal(t, oldBind, refreshed)
						require.False(t, alloc.HasInvalidService(owner.serviceID))
						require.Equal(t, oldBind, alloc.Get(caller.serviceID, 0, tableID, tableID, pb.Sharding_None))
					}
					require.Equal(t, int32(1), factory.attempts.Load())
					require.False(t, caller.isStatus(pb.Status_ServiceUnLockSucc),
						"a returned error alone must not erase the transaction's cleanup ownership")

					factory.healthy.Store(true)
					remote.client = originalClient
					require.NoError(t, caller.Unlock(guard, txnID, timestamp.Timestamp{}))
					require.True(t, caller.activeTxnHolder.empty())
					caller.mu.RLock()
					admissions, snapshotReady = caller.mu.lockAdmissions, caller.mu.drainSnapshotReady
					refs := caller.hasLockTableRefsLocked()
					caller.mu.RUnlock()
					require.Zero(t, admissions)
					require.True(t, snapshotReady)
					require.False(t, refs)
					require.True(t, caller.isStatus(pb.Status_ServiceUnLockSucc),
						"caller Unlock cleanup must complete drain without the caller deadline or force exit")
				},
				func(cfg *Config) {
					cfg.KeepBindDuration.Duration = time.Hour
					cfg.KeepRemoteLockDuration.Duration = time.Hour
				})
		})
	}
}

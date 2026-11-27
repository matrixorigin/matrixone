// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

type lifecycleRPCClient struct {
	morpc.RPCClient
	send  func(context.Context, morpc.Message) (*morpc.Future, error)
	close func() error
}

func (c *lifecycleRPCClient) Send(ctx context.Context, _ string, msg morpc.Message) (*morpc.Future, error) {
	return c.send(ctx, msg)
}

func (c *lifecycleRPCClient) Close() error { return c.close() }

func lifecycleHAKeeperClient(rpc morpc.RPCClient) *hakeeperClient {
	pool := &sync.Pool{}
	pool.New = func() any { return &RPCRequest{pool: pool} }
	return &hakeeperClient{client: rpc, pool: pool, addr: "test"}
}

func TestManagedHAKeeperCloseDuringRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"native_reset", syscall.ECONNRESET},
		{"backend_closed", moerr.NewBackendClosedNoCtx()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				oldNew, oldSend, oldInterval := newHAKeeperClientFunc, sendCNAllocateIDFunc, hakeeperClientRetryInterval
				defer func() {
					newHAKeeperClientFunc, sendCNAllocateIDFunc, hakeeperClientRetryInterval = oldNew, oldSend, oldInterval
				}()
				hakeeperClientRetryInterval = time.Hour
				started := make(chan struct{})
				creates, sends := 0, 0
				newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
					creates++
					return &hakeeperClient{}, nil
				}
				sendCNAllocateIDFunc = func(_ *hakeeperClient, _ context.Context, _ string, _ uint64) (uint64, error) {
					sends++
					if sends == 1 {
						close(started)
					}
					return 0, tc.err
				}
				c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
				ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
				defer cancel()
				done := make(chan error, 1)
				go func() { _, err := c.AllocateID(ctx); done <- err }()
				<-started
				before := time.Now()
				require.NoError(t, c.Close())
				err := <-done
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "%v", err)
				require.Equal(t, before, time.Now(), "Close must wake the retry without waiting for its timer")
				require.Equal(t, 1, creates)
				require.Equal(t, 1, sends)
				require.Nil(t, c.mu.client)
			})
		})
	}
}

func TestManagedHAKeeperAdmissionAfterClose(t *testing.T) {
	c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
	c.mu.sharedAllocID = allocID{nextID: 1, lastID: 2}
	c.mu.allocIDByKey = map[string]*allocID{"key": {nextID: 1, lastID: 2}}
	require.NoError(t, c.Close())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.AllocateID(ctx)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "%v", err)
	_, err = c.AllocateIDByKeyWithBatch(ctx, "key", 1)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "%v", err)
	_, err = c.AllocateIDByKeyWithRequestID(ctx, "key", 1, "request")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "%v", err)
	_, err = c.GetClusterDetails(ctx)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "%v", err)
	require.NoError(t, c.Close())
}

func TestManagedHAKeeperLateFailureKeepsReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		oldNew, oldInterval := newHAKeeperClientFunc, hakeeperClientRetryInterval
		defer func() { newHAKeeperClientFunc, hakeeperClientRetryInterval = oldNew, oldInterval }()
		hakeeperClientRetryInterval = 0
		firstStarted, secondStarted := make(chan struct{}), make(chan struct{})
		releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
		defer func() {
			for _, ch := range []chan struct{}{releaseFirst, releaseSecond} {
				select {
				case <-ch:
				default:
					close(ch)
				}
			}
		}()
		var sends, oldCloses, newCloses atomic.Int32
		oldRPC := &lifecycleRPCClient{
			send: func(context.Context, morpc.Message) (*morpc.Future, error) {
				switch sends.Add(1) {
				case 1:
					close(firstStarted)
					<-releaseFirst
				case 2:
					close(secondStarted)
					<-releaseSecond
				default:
					panic("unexpected old-generation request")
				}
				return nil, syscall.ECONNRESET
			},
			close: func() error { oldCloses.Add(1); return nil },
		}
		newRPC := &lifecycleRPCClient{
			send:  func(context.Context, morpc.Message) (*morpc.Future, error) { return nil, context.Canceled },
			close: func() error { newCloses.Add(1); return nil },
		}
		oldClient, newClient := lifecycleHAKeeperClient(oldRPC), lifecycleHAKeeperClient(newRPC)
		replaced := make(chan struct{})
		var creates atomic.Int32
		newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
			if creates.Add(1) != 1 {
				return nil, context.Canceled
			}
			close(replaced)
			return newClient, nil
		}
		c := &managedHAKeeperClient{}
		c.mu.client = oldClient
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		go func() { _, err := c.GetClusterDetails(ctx); firstDone <- err }()
		<-firstStarted
		go func() { _, err := c.GetClusterDetails(ctx); secondDone <- err }()
		<-secondStarted
		close(releaseFirst)
		<-replaced
		require.ErrorIs(t, <-firstDone, context.Canceled)
		close(releaseSecond)
		require.ErrorIs(t, <-secondDone, context.Canceled)
		require.EqualValues(t, 1, oldCloses.Load())
		require.Zero(t, newCloses.Load())
		require.Same(t, newClient, c.mu.client)
		require.NoError(t, c.Close())
		require.EqualValues(t, 1, newCloses.Load())
	})
}

func TestManagedHAKeeperResetCleanupFailure(t *testing.T) {
	oldSend := sendCNAllocateIDFunc
	defer func() { sendCNAllocateIDFunc = oldSend }()
	cleanupErr := errors.New("injected close failure")
	var closes atomic.Int32
	rpc := &lifecycleRPCClient{close: func() error { closes.Add(1); return cleanupErr }}
	client := lifecycleHAKeeperClient(rpc)
	c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
	c.mu.client = client
	sendCNAllocateIDFunc = func(got *hakeeperClient, _ context.Context, _ string, _ uint64) (uint64, error) {
		require.Same(t, client, got)
		return 0, syscall.ECONNRESET
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.AllocateID(ctx)
	require.ErrorIs(t, err, syscall.ECONNRESET)
	require.Nil(t, c.mu.client)
	require.ErrorIs(t, c.Close(), cleanupErr)
	require.ErrorIs(t, c.Close(), cleanupErr)
	require.EqualValues(t, 1, closes.Load())
	_, err = c.AllocateID(ctx)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "%v", err)
}

func TestManagedHAKeeperConcurrentCloseJoinsCleanup(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	cleanupErr := errors.New("injected close failure")
	var closes atomic.Int32
	rpc := &lifecycleRPCClient{close: func() error { closes.Add(1); close(started); <-release; return cleanupErr }}
	c := &managedHAKeeperClient{}
	c.mu.client = lifecycleHAKeeperClient(rpc)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- c.Close() }()
	<-started
	secondStarted := make(chan struct{})
	go func() { close(secondStarted); second <- c.Close() }()
	<-secondStarted
	close(release)
	require.ErrorIs(t, <-first, cleanupErr)
	require.ErrorIs(t, <-second, cleanupErr)
	require.ErrorIs(t, c.Close(), cleanupErr)
	require.EqualValues(t, 1, closes.Load())
}

func TestManagedHAKeeperCanceledConstructorCleansClient(t *testing.T) {
	for _, phase := range []string{"initial", "replacement"} {
		t.Run(phase, func(t *testing.T) {
			oldNew, oldSend := newHAKeeperClientFunc, sendCNAllocateIDFunc
			defer func() { newHAKeeperClientFunc, sendCNAllocateIDFunc = oldNew, oldSend }()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var closes atomic.Int32
			rpc := &lifecycleRPCClient{close: func() error { closes.Add(1); return nil }}
			newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
				cancel()
				return lifecycleHAKeeperClient(rpc), nil
			}
			sendCNAllocateIDFunc = func(*hakeeperClient, context.Context, string, uint64) (uint64, error) {
				t.Fatal("must not send after cancellation")
				return 0, nil
			}
			if phase == "initial" {
				c, err := newManagedHAKeeperClient(ctx, "", HAKeeperClientConfig{})
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, c)
			} else {
				c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
				_, err := c.AllocateID(ctx)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, c.mu.client)
				require.NoError(t, c.Close())
			}
			require.EqualValues(t, 1, closes.Load())
		})
	}
}

func TestManagedHAKeeperCanceledAdmissionPreservesCache(t *testing.T) {
	c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
	c.mu.sharedAllocID = allocID{nextID: 1, lastID: 2}
	c.mu.allocIDByKey = map[string]*allocID{"key": {nextID: 5, lastID: 6}}
	canceled, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	_, err := c.AllocateID(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.AllocateIDByKeyWithBatch(canceled, "key", 1)
	require.ErrorIs(t, err, context.Canceled)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	id, err := c.AllocateID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)
	id, err = c.AllocateIDByKeyWithBatch(ctx, "key", 1)
	require.NoError(t, err)
	require.Equal(t, uint64(5), id)
	require.NoError(t, c.Close())
}

func TestManagedHAKeeperCompletedResultWinsCancellation(t *testing.T) {
	oldNew, oldSend := newHAKeeperClientFunc, sendCNAllocateIDFunc
	defer func() { newHAKeeperClientFunc, sendCNAllocateIDFunc = oldNew, oldSend }()
	newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
		return &hakeeperClient{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sendCNAllocateIDFunc = func(*hakeeperClient, context.Context, string, uint64) (uint64, error) { cancel(); return 42, nil }
	c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
	id, err := c.AllocateID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(42), id)
	require.NoError(t, c.Close())
}

func TestManagedHAKeeperHonorsBackendWaitDeadline(t *testing.T) {
	t.Run("selected_failure_is_returned_without_another_attempt", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  error
		}{
			{"native_reset", syscall.ECONNRESET},
			{"backend_closed", moerr.NewBackendClosedNoCtx()},
		} {
			t.Run(tc.name, func(t *testing.T) {
				oldNew, oldSend := newHAKeeperClientFunc, sendCNAllocateIDFunc
				defer func() { newHAKeeperClientFunc, sendCNAllocateIDFunc = oldNew, oldSend }()
				newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
					t.Fatal("must not start another client generation")
					return nil, nil
				}
				sends := 0
				sendCNAllocateIDFunc = func(*hakeeperClient, context.Context, string, uint64) (uint64, error) {
					sends++
					return 0, tc.err
				}
				parent, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				ctx := morpc.WithBackendWaitDeadline(parent, time.Now().Add(500*time.Millisecond))
				c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
				c.mu.client = &hakeeperClient{}
				_, err := c.AllocateID(ctx)
				require.ErrorIs(t, err, tc.err)
				require.Equal(t, 1, sends)
				require.NoError(t, parent.Err())
				require.NoError(t, c.Close())
			})
		}
	})
	t.Run("legacy_eof_retry_is_preserved", func(t *testing.T) {
		oldNew, oldSend, oldInterval := newHAKeeperClientFunc, sendCNAllocateIDFunc, hakeeperClientRetryInterval
		defer func() {
			newHAKeeperClientFunc, sendCNAllocateIDFunc, hakeeperClientRetryInterval = oldNew, oldSend, oldInterval
		}()
		hakeeperClientRetryInterval = 0
		newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
			return &hakeeperClient{}, nil
		}
		sends := 0
		sendCNAllocateIDFunc = func(*hakeeperClient, context.Context, string, uint64) (uint64, error) {
			sends++
			if sends == 1 {
				return 0, io.EOF
			}
			return 42, nil
		}
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx := morpc.WithBackendWaitDeadline(parent, time.Now().Add(500*time.Millisecond))
		c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
		id, err := c.AllocateID(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(42), id)
		require.Equal(t, 2, sends)
		require.NoError(t, c.Close())
	})

	t.Run("expired_budget_rejects_construction_but_not_cached_id", func(t *testing.T) {
		oldNew := newHAKeeperClientFunc
		defer func() { newHAKeeperClientFunc = oldNew }()
		newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
			t.Fatal("must not construct a client after the backend budget")
			return nil, nil
		}
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx := morpc.WithBackendWaitDeadline(parent, time.Now().Add(-time.Second))
		c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
		_, err := newManagedHAKeeperClient(ctx, "", HAKeeperClientConfig{})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect), "%v", err)
		c.mu.sharedAllocID = allocID{nextID: 1, lastID: 1}
		id, err := c.AllocateID(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(1), id)
		require.NoError(t, c.Close())
	})
	t.Run("completed_close_or_cancel_precedes_failure", func(t *testing.T) {
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx := morpc.WithBackendWaitDeadline(parent, time.Now().Add(-time.Second))
		c := &managedHAKeeperClient{}
		require.NoError(t, c.Close())
		err := c.waitRetry(ctx, syscall.ECONNRESET)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "%v", err)
		cancel()
		require.ErrorIs(t, c.waitRetry(ctx, syscall.ECONNRESET), context.Canceled)
	})
	t.Run("inflight_request_keeps_parent_deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			oldNew, oldSend := newHAKeeperClientFunc, sendCNAllocateIDFunc
			defer func() { newHAKeeperClientFunc, sendCNAllocateIDFunc = oldNew, oldSend }()
			newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
				return &hakeeperClient{}, nil
			}
			sendCNAllocateIDFunc = func(_ *hakeeperClient, requestCtx context.Context, _ string, _ uint64) (uint64, error) {
				time.Sleep(60 * time.Millisecond)
				require.NoError(t, requestCtx.Err(), "the backend wait budget must not cancel an in-flight request")
				return 42, nil
			}
			parent, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ctx := morpc.WithBackendWaitDeadline(parent, time.Now().Add(30*time.Millisecond))
			c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{AllocateIDBatch: 2}}
			id, err := c.AllocateID(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(42), id)
			require.NoError(t, c.Close())
		})
	})
}

type timeoutBackendFactory struct {
	started chan struct{}
	release chan struct{}
	starts  atomic.Int32
}

func (f *timeoutBackendFactory) Create(string, ...morpc.BackendOption) (morpc.Backend, error) {
	if f.starts.Add(1) == 1 {
		close(f.started)
	}
	<-f.release
	return nil, errors.New("test backend released")
}

func TestManagedHAKeeperHonorsMORPCCreateTimeout(t *testing.T) {
	f := &timeoutBackendFactory{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	rpc, err := morpc.NewClient("hakeeper-create-timeout-test", f,
		morpc.WithClientAutoCreateWaitTimeout(20*time.Millisecond),
		morpc.WithClientDisableCircuitBreaker())
	require.NoError(t, err)
	t.Cleanup(func() { releaseOnce.Do(func() { close(f.release) }); _ = rpc.Close() })
	wrapped := &lifecycleRPCClient{
		send:  func(ctx context.Context, msg morpc.Message) (*morpc.Future, error) { return rpc.Send(ctx, "test", msg) },
		close: func() error { releaseOnce.Do(func() { close(f.release) }); return rpc.Close() },
	}
	oldNew := newHAKeeperClientFunc
	defer func() { newHAKeeperClientFunc = oldNew }()
	attempts := 0
	newHAKeeperClientFunc = func(context.Context, string, HAKeeperClientConfig) (*hakeeperClient, error) {
		attempts++
		return nil, context.Canceled
	}
	c := &managedHAKeeperClient{}
	c.mu.client = lifecycleHAKeeperClient(wrapped)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.GetClusterDetails(ctx); done <- err }()
	<-f.started
	err = <-done
	require.ErrorIs(t, err, morpc.ErrBackendCreateTimeout)
	require.Zero(t, attempts)
	require.EqualValues(t, 1, f.starts.Load())
	require.NoError(t, c.Close())
}

func TestHAKeeperDiscoveryPropagatesCancellation(t *testing.T) {
	original := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = original }()
	queryShardInfoRawFn = func(ctx context.Context, _ string, _ string, _ uint64, _ bool, _ bool) (pb.ShardInfoQueryResult, bool, error) {
		require.Empty(t, GetBackendOptions(ctx))
		require.Empty(t, GetClientOptions(ctx))
		return pb.ShardInfoQueryResult{}, false, ctx.Err()
	}
	parent := SetClientOptions(SetBackendOptions(context.Background(), morpc.WithBackendConnectTimeout(time.Second)), morpc.WithClientAutoCreateWaitTimeout(time.Second))
	ctx, cancel := context.WithCancel(parent)
	cancel()
	_, err := connectByReverseProxy(ctx, "", "discovery", HAKeeperClientConfig{})
	require.ErrorIs(t, err, context.Canceled)
}

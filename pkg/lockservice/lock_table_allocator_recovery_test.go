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

package lockservice

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

type resetTrackingClient struct {
	Client
	resets   atomic.Int32
	resetErr error
}

func (c *resetTrackingClient) ResetBackend(context.Context, string) error {
	c.resets.Add(1)
	return c.resetErr
}

func (c *resetTrackingClient) Close() error { return nil }

func TestCleanCommitStateRetriesTransientBackendClose(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		client := &resetTrackingClient{}
		require.NoError(t, a.client.Close())
		a.client = client
		c := a.getCtl("s1")
		require.Equal(t, cannotCommitState, c.tryCannotCommit("active"))

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				if calls.Add(1) == 1 {
					return false, nil, moerr.NewBackendClosedNoCtx()
				}
				return true, [][]byte{[]byte("active")}, nil
			},
			time.Hour,
		)

		require.Equal(t, int32(2), calls.Load())
		require.Equal(t, int32(1), client.resets.Load())
		require.False(t, a.HasInvalidService("s1"))
		_, ok := c.getCommitState("active")
		require.True(t, ok)
	})
}

func TestCleanCommitStateConfirmsInvalidAfterEndpointReset(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		client := &resetTrackingClient{}
		require.NoError(t, a.client.Close())
		a.client = client
		ctl := a.getCtl("s1")
		require.Equal(t, cannotCommitState, ctl.tryCannotCommit("active"))

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				if calls.Add(1) == 1 {
					// Simulate a stale recovery IP now owned by another CN.
					return false, nil, nil
				}
				return true, [][]byte{[]byte("active")}, nil
			},
			time.Hour,
		)

		require.Equal(t, int32(2), calls.Load())
		require.Equal(t, int32(1), client.resets.Load())
		_, ok := ctl.getCommitState("active")
		require.True(t, ok, "a response from the stale endpoint deleted live state")
	})
}

func TestCleanCommitStateAcceptsConfirmedInvalidService(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		client := &resetTrackingClient{}
		require.NoError(t, a.client.Close())
		a.client = client
		ctl := a.getCtl("s1")
		require.Equal(t, cannotCommitState, ctl.tryCannotCommit("old"))

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				calls.Add(1)
				return false, nil, nil
			},
			time.Hour,
		)

		require.Equal(t, int32(2), calls.Load())
		require.Equal(t, int32(1), client.resets.Load())
		_, ok := ctl.getCommitState("old")
		require.False(t, ok)
	})
}

func TestCleanCommitStatePreservesStateWhenNegativeConfirmationResetFails(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		client := &resetTrackingClient{resetErr: moerr.NewBackendClosedNoCtx()}
		require.NoError(t, a.client.Close())
		a.client = client
		ctl := a.getCtl("s1")
		require.Equal(t, cannotCommitState, ctl.tryCannotCommit("live"))

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				calls.Add(1)
				return false, nil, nil
			},
			time.Hour,
		)

		require.Equal(t, int32(1), calls.Load())
		require.Equal(t, int32(1), client.resets.Load())
		require.False(t, a.HasInvalidService("s1"))
		_, ok := ctl.getCommitState("live")
		require.True(t, ok, "failed reset must leave control state unknown")
	})
}

func TestCleanCommitStateFencesAfterBoundedConnectionFailures(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		c := a.getCtl("s1")
		require.Equal(t, cannotCommitState, c.tryCannotCommit("orphan"))

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				calls.Add(1)
				return false, nil, moerr.NewBackendCannotConnectNoCtx("s1")
			},
			time.Hour,
		)

		require.Equal(t, int32(getActiveTxnMaxAttempts), calls.Load())
		require.True(t, a.HasInvalidService("s1"))
		_, ok := c.getCommitState("orphan")
		require.True(t, ok, "unknown service must retain cannot-commit tombstones")
	})
}

func TestCleanCommitStatePersistentDisconnectExpires(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		c := a.getCtl("s1")
		require.Equal(t, cannotCommitState, c.tryCannotCommit("orphan"))
		// Inject elapsed time instead of depending on the background timer.
		firstDisconnect := time.Now().Add(-30 * time.Minute)
		a.inactiveService.Store("s1", firstDisconnect)
		calls := 0
		probe := func(context.Context, string) (bool, [][]byte, error) {
			calls++
			return false, nil, moerr.NewBackendCannotConnectNoCtx("s1")
		}
		for range 2 {
			a.cleanCommitStateOnce(context.Background(), probe, time.Hour)
			observed, ok := a.inactiveService.Load("s1")
			require.True(t, ok)
			require.Equal(t, firstDisconnect, observed, "retry extended disconnect retention")
			_, err := a.Valid("s1", []byte("late"), nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOnInvalidCN))
		}
		require.Equal(t, 2*getActiveTxnMaxAttempts, calls)
		// The same first-disconnect time is now beyond the selected retention.
		a.cleanCommitStateOnce(context.Background(), probe, time.Minute)
		require.False(t, a.HasInvalidService("s1"))
		_, exists := a.ctl.Load("s1")
		require.False(t, exists)
		calls = 0
		a.cleanCommitStateOnce(context.Background(), probe, time.Minute)
		require.Zero(t, calls, "expired owner remained in the probe set")
	})
}

func TestCleanCommitStateExpiryDoesNotRequireSuccessfulProbe(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unknown", moerr.NewInternalErrorNoCtx("unknown owner state")},
		{"connection", moerr.NewBackendClosedNoCtx()},
		{"negative_with_failed_reset", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
				require.NoError(t, a.client.Close())
				a.client = &resetTrackingClient{resetErr: moerr.NewBackendClosedNoCtx()}
				a.getCtl("s1").tryCannotCommit("orphan")
				a.inactiveService.Store("s1", time.Now().Add(-2*time.Hour))
				// A service can also have an inactive marker without any ctl.
				a.inactiveService.Store("marker-only", time.Now().Add(-2*time.Hour))
				calls := 0
				a.cleanCommitStateOnce(context.Background(), func(_ context.Context, sid string) (bool, [][]byte, error) {
					require.Equal(t, "s1", sid)
					calls++
					return false, nil, tc.err
				}, time.Hour)
				require.Positive(t, calls)
				require.False(t, a.HasInvalidService("s1"))
				require.False(t, a.HasInvalidService("marker-only"))
				_, exists := a.ctl.Load("s1")
				require.False(t, exists)
			})
		})
	}
}

func TestCleanCommitStateExpiryPreservesFreshRecoveryEpoch(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unknown", moerr.NewInternalErrorNoCtx("unknown owner state")},
		{"connection", moerr.NewBackendClosedNoCtx()},
		{"successful_empty_snapshot", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
				c := a.getCtl("s1")
				c.tryCannotCommit("orphan")
				a.inactiveService.Store("s1", time.Now().Add(-2*time.Hour))
				var once sync.Once
				a.cleanCommitStateOnce(context.Background(), func(context.Context, string) (bool, [][]byte, error) {
					once.Do(func() {
						a.resumeService("s1")
						a.AddInvalidService("s1")
					})
					return true, nil, tc.err
				}, time.Hour)
				require.True(t, a.HasInvalidService("s1"))
				_, exists := c.getCommitState("orphan")
				require.True(t, exists, "old sweep deleted a fresh epoch's fence")
				a.resumeService("s1")
				_, err := a.Valid("s1", []byte("orphan"), nil)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
				_, err = a.Valid("s1", []byte("new"), nil)
				require.NoError(t, err)
				a.FinishCommit("s1", []byte("new"))
			})
		})
	}
}

func TestCleanCommitStateExpiryPreservesNewStateAndCommitSafety(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		c := a.getCtl("s1")
		c.tryCannotCommit("old")
		require.Equal(t, committingState, c.beginCommit("inflight"))
		c.tryCannotCommit("unknown", commitFence{
			persist: true, expiresAt: time.Now().Add(time.Hour).UnixNano(), commitSequence: 10,
		})
		a.inactiveService.Store("s1", time.Now().Add(-2*time.Hour))
		a.cleanCommitStateOnce(context.Background(), func(context.Context, string) (bool, [][]byte, error) {
			a.AddCannotCommit([]pb.OrphanTxn{{Service: "s1", Txn: [][]byte{[]byte("new")}}})
			return false, nil, moerr.NewInternalErrorNoCtx("unknown owner state")
		}, time.Hour)
		require.False(t, a.HasInvalidService("s1"))
		_, exists := c.getCommitState("old")
		require.False(t, exists)
		state, exists := c.getCommitState("inflight")
		require.True(t, exists)
		require.Equal(t, uint32(1), state.inflight)
		_, err := a.Valid("s1", []byte("new"), nil, CommitRequestMeta{Sequence: 11})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
		_, err = a.Valid("s1", []byte("late-unknown"), nil, CommitRequestMeta{Sequence: 10})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
		_, err = a.Valid("s1", []byte("fresh"), nil, CommitRequestMeta{Sequence: 11})
		require.NoError(t, err)
		a.FinishCommit("s1", []byte("fresh"))
		a.FinishCommit("s1", []byte("inflight"))
	})
}

func TestCleanCommitStateExpiryPreservesReplacementCtl(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		a.getCtl("s1").tryCannotCommit("old")
		a.inactiveService.Store("s1", time.Now().Add(-2*time.Hour))
		var replacement *commitCtl
		a.cleanCommitStateOnce(context.Background(), func(context.Context, string) (bool, [][]byte, error) {
			// Simulate an intervening cleanup and a new ctl with a lower watermark.
			a.ctlMu.Lock()
			a.inactiveMu.Lock()
			a.ctl.Delete("s1")
			a.inactiveService.Delete("s1")
			replacement = a.getCtl("s1")
			replacement.tryCannotCommit("new")
			a.inactiveMu.Unlock()
			a.ctlMu.Unlock()
			return true, nil, nil
		}, time.Hour)
		current, exists := a.ctl.Load("s1")
		require.True(t, exists)
		require.Same(t, replacement, current)
		_, err := a.Valid("s1", []byte("new"), nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
	})
}

func TestCleanCommitStateRetriesDeadlineExceeded(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		client := &resetTrackingClient{}
		require.NoError(t, a.client.Close())
		a.client = client
		a.getCtl("s1")

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				calls.Add(1)
				return false, nil, context.DeadlineExceeded
			},
			time.Hour,
		)

		require.Equal(t, int32(getActiveTxnMaxAttempts), calls.Load())
		require.Equal(t, int32(getActiveTxnMaxAttempts), client.resets.Load())
		require.True(t, a.HasInvalidService("s1"))
	})
}

func TestCleanCommitStateDoesNotLoopOnUnknownError(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		c := a.getCtl("s1")
		require.Equal(t, cannotCommitState, c.tryCannotCommit("orphan"))

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				calls.Add(1)
				return false, nil, moerr.NewInternalErrorNoCtx("injected jitter")
			},
			time.Hour,
		)

		require.Equal(t, int32(1), calls.Load())
		require.False(t, a.HasInvalidService("s1"))
		_, ok := c.getCommitState("orphan")
		require.True(t, ok)
	})
}

func TestCleanCommitStateCannotRefenceAfterResume(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		a.getCtl("s1")
		a.AddInvalidService("s1")

		entered := make(chan struct{})
		release := make(chan struct{})
		done := make(chan struct{})
		var once sync.Once
		go func() {
			defer close(done)
			a.cleanCommitStateOnce(
				context.Background(),
				func(context.Context, string) (bool, [][]byte, error) {
					once.Do(func() { close(entered) })
					<-release
					return false, nil, moerr.NewBackendClosedNoCtx()
				},
				time.Hour,
			)
		}()

		<-entered
		a.resumeService("s1")
		close(release)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("cleanup did not terminate")
		}
		require.False(t, a.HasInvalidService("s1"),
			"a pre-resume cleanup result fenced the resumed incarnation")
	})
}

func TestCleanCommitStateHonorsCanceledContext(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		a.getCtl("s1")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var calls atomic.Int32
		a.cleanCommitStateOnce(
			ctx,
			func(context.Context, string) (bool, [][]byte, error) {
				calls.Add(1)
				return true, nil, nil
			},
			time.Hour,
		)
		require.Zero(t, calls.Load())
	})
}

func TestCleanCommitStateCancellationStopsConnectionRetries(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		a.getCtl("s1")
		ctx, cancel := context.WithCancel(context.Background())

		var calls atomic.Int32
		done := make(chan struct{})
		go func() {
			defer close(done)
			a.cleanCommitStateOnce(
				ctx,
				func(context.Context, string) (bool, [][]byte, error) {
					calls.Add(1)
					cancel()
					return false, nil, moerr.NewBackendClosedNoCtx()
				},
				time.Hour,
			)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("cleanup did not stop after context cancellation")
		}
		require.Equal(t, int32(1), calls.Load())
		require.False(t, a.HasInvalidService("s1"))
	})
}

func TestCleanCommitStateIsolatesServices(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		a.getCtl("healthy")
		a.getCtl("disconnected")

		var mu sync.Mutex
		calls := make(map[string]int)
		a.cleanCommitStateOnce(
			context.Background(),
			func(_ context.Context, sid string) (bool, [][]byte, error) {
				mu.Lock()
				calls[sid]++
				mu.Unlock()
				if sid == "disconnected" {
					return false, nil, moerr.NewBackendClosedNoCtx()
				}
				return true, nil, nil
			},
			time.Hour,
		)

		require.False(t, a.HasInvalidService("healthy"))
		require.True(t, a.HasInvalidService("disconnected"))
		require.Equal(t, 1, calls["healthy"])
		require.Equal(t, getActiveTxnMaxAttempts, calls["disconnected"])
	})
}

func TestResumeEpochIsScopedToService(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		ctl1 := a.getCtl("s1")
		ctl2 := a.getCtl("s2")
		epoch1 := ctl1.currentRecoveryEpoch()
		epoch2 := ctl2.currentRecoveryEpoch()

		a.resumeService("s1")

		require.False(t, a.markServiceInactive("s1", ctl1, epoch1, true))
		require.True(t, a.markServiceInactive("s2", ctl2, epoch2, true))
		require.False(t, a.HasInvalidService("s1"))
		require.True(t, a.HasInvalidService("s2"))
	})
}

func TestRecoveryEpochOverflowFailsOpen(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		ctl := a.getCtl("s1")
		ctl.mu.Lock()
		ctl.recoveryEpoch = ^uint64(0)
		ctl.mu.Unlock()

		a.AddInvalidService("s1")
		a.resumeService("s1")
		require.False(t, a.markServiceInactive("s1", ctl, ^uint64(0), true))
		require.False(t, a.HasInvalidService("s1"))
	})
}

func TestFenceCannotInterleaveCommitAdmission(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		ctl := a.getCtl("s1")
		ctl.mu.Lock()
		ctlLocked := true
		defer func() {
			if ctlLocked {
				ctl.mu.Unlock()
			}
		}()

		validDone := make(chan error, 1)
		go func() {
			_, err := a.Valid("s1", []byte("txn"), nil)
			validDone <- err
		}()

		// Valid holds the recovery read lock while waiting to register its
		// commit. This proves fencing cannot linearize between the admission
		// check and beginCommit.
		require.Eventually(t, func() bool {
			if !a.inactiveMu.TryLock() {
				return true
			}
			a.inactiveMu.Unlock()
			return false
		}, 5*time.Second, time.Millisecond)

		fenceStarted := make(chan struct{})
		fenceDone := make(chan struct{})
		go func() {
			close(fenceStarted)
			a.AddInvalidService("s1")
			close(fenceDone)
		}()
		<-fenceStarted

		ctl.mu.Unlock()
		ctlLocked = false
		select {
		case err := <-validDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("commit admission did not finish")
		}
		select {
		case <-fenceDone:
		case <-time.After(5 * time.Second):
			t.Fatal("fence did not finish")
		}
		require.True(t, a.HasInvalidService("s1"))
		require.True(t, ctl.finishCommit("txn"))
	})
}

func BenchmarkValidDuringRecoveryChanges(b *testing.B) {
	a := &lockTableAllocator{}
	a.mu.services = make(map[string]*serviceBinds)
	a.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
	txnID := []byte("txn")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := a.Valid("s1", txnID, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

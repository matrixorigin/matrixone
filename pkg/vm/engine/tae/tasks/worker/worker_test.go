// Copyright 2024 Matrix Origin
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

package ops

import (
	"context"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type testOp struct{}

func (t testOp) OnExec(context.Context) error {
	return nil
}

func (t testOp) SetError(error) {}

func (t testOp) GetError() error {
	return nil
}

func (t testOp) WaitDone(context.Context) error {
	return nil
}

func (t testOp) Waitable() bool {
	return true
}

func (t testOp) GetCreateTime() time.Time {
	return time.Time{}
}

func (t testOp) GetStartTime() time.Time {
	return time.Time{}
}

func (t testOp) GetEndTime() time.Time {
	return time.Time{}
}

func (t testOp) GetExecutTime() int64 {
	return 0
}

func (t testOp) AddObserver(iops.Observer) {}

func TestOpWorkerDrainAndConcurrentStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		worker := NewOpWorker(ctx, "drain", 1)
		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		defer func() {
			releaseOnce.Do(func() { close(release) })
			worker.Stop()
		}()
		var processed atomic.Int64
		worker.ExecFunc = func(iops.IOp) {
			if processed.Add(1) == 1 {
				close(started)
				<-release
			}
		}
		worker.Start()
		require.Panics(t, worker.Start)
		require.True(t, worker.SendOp(testOp{}))
		<-started
		require.True(t, worker.SendOp(testOp{}))
		require.False(t, worker.SendOp(testOp{}))
		worker.StopReceiver()
		var stopped sync.WaitGroup
		stopped.Add(2)
		for range 2 {
			go func() { defer stopped.Done(); worker.WaitStop() }()
		}
		synctest.Wait()
		require.Equal(t, int32(StoppingCMD), worker.State.Load())
		require.False(t, worker.SendOp(testOp{}))
		cancel()
		synctest.Wait()
		require.Equal(t, int32(StoppingCMD), worker.State.Load())
		releaseOnce.Do(func() { close(release) })
		stopped.Wait()
		require.Equal(t, int64(2), processed.Load())
		require.Zero(t, worker.Pending.Load())
		worker.Stop()
	})
}

func TestOpWorkerStopBeforeStart(t *testing.T) {
	worker := NewOpWorker(t.Context(), "stopped", 0)
	worker.Stop()
	worker.Stop()
	require.Equal(t, int32(STOPPED), worker.State.Load())
	require.False(t, worker.SendOp(testOp{}))
	require.Panics(t, worker.Start)
}

// Model a sender which reserved its slot before shutdown but then rolls back.
func TestOpWorkerStopWakesOnReservationRollback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		worker := NewOpWorker(t.Context(), "rollback", 0)
		worker.Start()
		worker.Pending.Add(1)
		var rollback sync.Once
		defer func() {
			worker.StopReceiver()
			rollback.Do(func() { worker.sendReservedOp(testOp{}) })
			worker.WaitStop()
		}()
		worker.StopReceiver()
		done := make(chan struct{})
		go func() { worker.WaitStop(); close(done) }()
		synctest.Wait()
		require.Equal(t, int32(StoppingCMD), worker.State.Load())
		rollback.Do(func() { require.False(t, worker.sendReservedOp(testOp{})) })
		<-done
		require.Zero(t, worker.Pending.Load())
	})
}

func TestOpWorkerConcurrentStartStop(t *testing.T) {
	for range 100 {
		worker := NewOpWorker(t.Context(), "start-stop", 0)
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			defer func() { _ = recover() }() // Start may lose to Stop.
			<-start
			worker.Start()
		}()
		go func() { defer group.Done(); <-start; worker.Stop() }()
		close(start)
		group.Wait()
		require.Equal(t, int32(STOPPED), worker.State.Load())
	}
}

type identifiedOp struct {
	testOp
	id int
}

func TestOpWorkerConcurrentSendStop(t *testing.T) {
	for range 20 {
		worker := NewOpWorker(t.Context(), "send-stop", 32)
		var mu sync.Mutex
		accepted := make(map[int]bool)
		executed := make(map[int]int)
		worker.ExecFunc = func(op iops.IOp) { mu.Lock(); executed[op.(identifiedOp).id]++; mu.Unlock() }
		worker.Start()
		start := make(chan struct{})
		firstAccepted := make(chan struct{})
		var firstOnce sync.Once
		var producers sync.WaitGroup
		for n := range 8 {
			producers.Add(1)
			go func() {
				defer producers.Done()
				<-start
				for i := range 100 {
					id := n*100 + i
					if worker.SendOp(identifiedOp{id: id}) {
						mu.Lock()
						accepted[id] = true
						mu.Unlock()
						firstOnce.Do(func() { close(firstAccepted) })
					}
				}
			}()
		}
		close(start)
		<-firstAccepted
		worker.Stop()
		producers.Wait()
		require.Zero(t, worker.Pending.Load())
		require.NotEmpty(t, accepted)
		require.Len(t, executed, len(accepted))
		for id := range accepted {
			require.Equal(t, 1, executed[id])
		}
	}
}

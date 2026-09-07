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

package process

import (
	"context"
	"errors"
	"sync"
)

// sequenceGate is a one-permit semaphore.  It deliberately lives on
// BaseProcess, which is shared by all local child processes for one statement,
// rather than on SessionInfo (which is copied/rebuilt as session state).
//
// The zero value is ready for use.  A channel is used instead of sync.Mutex so
// a canceled waiter can leave without a goroutine or an unbounded waiter
// registry.
type sequenceGate struct {
	once sync.Once
	sem  chan struct{}
}

func (g *sequenceGate) init() {
	g.once.Do(func() {
		g.sem = make(chan struct{}, 1)
		g.sem <- struct{}{}
	})
}

func (g *sequenceGate) acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	g.init()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-g.sem:
	}
	// Availability and cancellation can race.  A canceled caller must never
	// start a metadata read/write after it has been admitted.
	if err := ctx.Err(); err != nil {
		g.sem <- struct{}{}
		return nil, err
	}

	var once sync.Once
	return func() {
		once.Do(func() { g.sem <- struct{}{} })
	}, nil
}

// AcquireSequence admits one complete sequence operation for this process's
// shared statement Base.  The returned release function is idempotent so
// error paths can safely use a deferred cleanup without leaking the permit.
func (proc *Process) AcquireSequence(ctx context.Context) (func(), error) {
	if proc == nil || proc.Base == nil {
		return nil, errors.New("sequence gate: process base is nil")
	}
	return proc.Base.sequenceGate.acquire(ctx)
}

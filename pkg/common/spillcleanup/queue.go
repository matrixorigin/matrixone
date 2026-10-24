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

// Package spillcleanup retains failed physical cleanup after query teardown.
package spillcleanup

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Capacity bounds all live named spill files, including empty files and files
// whose removal is pending after query teardown. Admission happens BEFORE
// CreateFile, not when a failed cleanup is queued. See docs/design/spill_cleanup_owner_v1.md.
const Capacity = 16 * 1024

const maxWorkers = 4

var ErrCapacity error = moerr.NewInternalErrorNoCtx("spill file metadata capacity exhausted")
var admitted atomic.Int64

// Reservation is move-only file ownership. Release is legal only when creation
// failed without a file, or the physical file has been removed. It deliberately
// owns neither a query nor an FD. State: 0 invalid, 1 active, 2 queued, 3 released.
type Reservation struct{ state atomic.Uint32 }

func Reserve() (*Reservation, error) {
	for {
		used := admitted.Load()
		if used >= Capacity {
			return nil, ErrCapacity
		}
		if admitted.CompareAndSwap(used, used+1) {
			r := new(Reservation)
			r.state.Store(1)
			return r, nil
		}
	}
}

// Used includes active, queued and currently executing cleanup owners.
func Used() int64 { return admitted.Load() }

func (r *Reservation) Release() {
	if r != nil {
		for old := r.state.Load(); old == 1 || old == 2; old = r.state.Load() {
			if r.state.CompareAndSwap(old, 3) {
				admitted.Add(-1)
				return
			}
		}
	}
}

// Enqueue transfers this file's cleanup at most once. The detached callback
// must release the reservation only after successful physical deletion. It may
// retain file metadata/reservations, but not a query, operator or batch.
func (r *Reservation) Enqueue(cleanup func() error) {
	if r != nil && cleanup != nil && r.state.CompareAndSwap(1, 2) {
		terminal.enqueue(func() error {
			if r.state.Load() == 3 {
				return nil
			}
			err := cleanup()
			// A close/flush error can accompany successful deletion. Once the
			// owner releases its slot, retaining retries would evade admission.
			if r.state.Load() == 3 {
				return nil
			}
			return err
		})
	}
}

type task struct {
	cleanup func() error
	after   time.Time
}

type queue struct {
	mu      sync.Mutex
	pending []task
	workers int
	delay   time.Duration
}

var terminal = queue{delay: time.Second}

func (q *queue) enqueue(cleanup func() error) {
	if cleanup == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, task{cleanup: cleanup})
	if q.workers < maxWorkers {
		q.workers++
		go q.run()
	}
}

func (q *queue) run() {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.workers--
			q.mu.Unlock()
			return
		}
		now := time.Now()
		index := -1
		wait := q.delay
		for i, work := range q.pending {
			if !work.after.After(now) {
				index = i
				break
			}
			if d := work.after.Sub(now); d < wait {
				wait = d
			}
		}
		if index < 0 {
			q.mu.Unlock()
			time.Sleep(wait)
			continue
		}
		work := q.pending[index]
		if index == 0 {
			q.pending[0] = task{}
			q.pending = q.pending[1:]
		} else {
			copy(q.pending[index:], q.pending[index+1:])
			q.pending[len(q.pending)-1] = task{}
			q.pending = q.pending[:len(q.pending)-1]
		}
		q.mu.Unlock()
		// A blocked syscall consumes one of the fixed workers. Never launch a
		// timeout replacement: even four permanently blocked calls stay bounded.
		if err := work.cleanup(); err != nil {
			work.after = time.Now().Add(q.delay)
			q.mu.Lock()
			q.pending = append(q.pending, work)
			q.mu.Unlock()
		}
	}
}

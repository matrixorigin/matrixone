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
	"time"
)

type queue struct {
	mu      sync.Mutex
	pending []func() error
	running bool
	delay   time.Duration
}

var terminal = queue{delay: time.Second}

// Enqueue transfers exclusive cleanup ownership to a process-wide retry queue.
// The callback must retain only detached file metadata/reservations, must be
// safe after query teardown, and must return promptly on a filesystem error.
// Failed work is never dropped. One worker retries every pending owner each
// round and exits when the queue drains; it holds no lock during callbacks.
func Enqueue(cleanup func() error) {
	terminal.enqueue(cleanup)
}

func (q *queue) enqueue(cleanup func() error) {
	if cleanup == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, cleanup)
	if !q.running {
		q.running = true
		go q.run()
	}
}

func (q *queue) run() {
	for {
		time.Sleep(q.delay)
		q.mu.Lock()
		work := q.pending
		q.pending = nil
		q.mu.Unlock()
		for _, cleanup := range work {
			if err := cleanup(); err != nil {
				q.mu.Lock()
				q.pending = append(q.pending, cleanup)
				q.mu.Unlock()
			}
		}
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.running = false
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
	}
}

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

package stopper

import (
	"context"
	"sync"
	"sync/atomic"
)

// AdmissionGate tracks senders that may block while a worker is stopping.
// A caller must set closed before Seal and pair each successful Enter with Leave.
type AdmissionGate struct {
	mu sync.RWMutex
	wg sync.WaitGroup
}

// Enter registers a sender unless the worker is closed.
func (g *AdmissionGate) Enter(closed *atomic.Bool) bool {
	if closed.Load() {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if closed.Load() {
		return false
	}
	g.wg.Add(1)
	return true
}

// Leave releases an admitted sender.
func (g *AdmissionGate) Leave() {
	g.wg.Done()
}

// Seal cancels blocked senders and waits for all admitted senders to leave.
func (g *AdmissionGate) Seal(cancel context.CancelFunc) {
	g.mu.Lock()
	cancel()
	g.mu.Unlock()
	g.wg.Wait()
}

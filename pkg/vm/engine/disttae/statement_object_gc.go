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

package disttae

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type statementObjectGC struct {
	sync.Mutex
	head, tail   *statementObjectPin
	views        int
	wake         chan struct{}
	changed      chan struct{}
	done         chan struct{}
	cancel       context.CancelFunc
	inflight     bool
	closing      bool
	stopped      bool
	lastErr      error
	stopErr      error
	shutdownDone chan struct{}
	finalized    bool
}

// notifyLocked broadcasts state changes to shutdown waiters, without making
// the IO owner depend on any waiter. Its live storage is constant per engine.
func (g *statementObjectGC) notifyLocked() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

func (e *Engine) registerStatementView() error {
	g := &e.statementGC
	g.Lock()
	defer g.Unlock()
	if g.closing {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	g.views++
	return nil
}

// releaseStatementView publishes the complete handoff before releasing the
// view's admission. Shutdown cannot observe zero views with a chain in transit.
// The caller has released all transaction/pin locks and view resources.
func (e *Engine) releaseStatementView(head, tail *statementObjectPin) {
	g := &e.statementGC
	g.Lock()
	defer g.Unlock()
	g.appendLocked(head, tail)
	g.views--
	e.wakeStatementGCLocked()
	g.notifyLocked()
}

func (g *statementObjectGC) appendLocked(head, tail *statementObjectPin) {
	if head == nil {
		return
	}
	if g.tail == nil {
		g.head = head
	} else {
		g.tail.next = head
	}
	g.tail = tail
}

func (e *Engine) wakeStatementGCLocked() {
	g := &e.statementGC
	if g.stopped || g.head == nil {
		return
	}
	if g.done == nil {
		ctx, cancel := context.WithCancel(context.Background())
		g.cancel = cancel
		g.done = make(chan struct{})
		g.wake = make(chan struct{}, 1)
		go e.runStatementGC(ctx)
	}
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) runStatementGC(ctx context.Context) {
	g := &e.statementGC
	defer func() {
		g.Lock()
		close(g.done)
		g.notifyLocked()
		g.Unlock()
	}()
	backoff := 50 * time.Millisecond
	for {
		g.Lock()
		if g.stopped {
			g.Unlock()
			return
		}
		var pins [GCBatchOfFileCount]*statementObjectPin
		var names [GCBatchOfFileCount]string
		n := 0
		for g.head != nil && n < len(pins) {
			pin := g.head
			// Failed batch members retry alone: one permanently bad name must
			// not poison unrelated names in every following batch.
			if n > 0 && pin.retryOne {
				break
			}
			g.head = pin.next
			pin.next = nil
			pins[n], names[n] = pin, pin.name
			n++
			if pin.retryOne {
				break
			}
		}
		if g.head == nil {
			g.tail = nil
		}
		g.inflight = n != 0
		g.Unlock()
		if n == 0 {
			// Enqueue sends while holding the same mutex as the empty check.
			// A send before this select remains buffered; none can be lost.
			select {
			case <-g.wake:
				continue
			case <-ctx.Done():
				return
			}
		}
		deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := e.fs.Delete(deleteCtx, names[:n]...)
		cancel()
		if err == nil {
			e.ReleaseQuota(int64(n) * statementObjectPinQuota)
		}
		g.Lock()
		if err != nil {
			// Delete may have partially succeeded. Absent names are safe to
			// retry; no part of the charge is released on ambiguous failure.
			for i, pin := range pins[:n] {
				pin.retryOne = true
				if i+1 < n {
					pin.next = pins[i+1]
				}
			}
			g.appendLocked(pins[0], pins[n-1])
			g.lastErr = err
		}
		g.inflight = false
		g.notifyLocked()
		g.Unlock()
		if err != nil {
			// A new enqueue cannot bypass failure backoff and create a hot
			// retry loop. Shutdown cancellation interrupts both IO and timer.
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
			backoff = min(2*backoff, 5*time.Second)
		} else {
			backoff = 50 * time.Millisecond
		}
	}
}

// CloseStatementObjects is a terminal, idempotent shutdown hook. CN calls it
// after stopping producers and before tearing down IO; Engine.Close also uses
// it. A failed shutdown retains unconfirmed ownership and never restarts IO.
func (e *Engine) CloseStatementObjects() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultLoadFileCleanupTimeout)
	defer cancel()
	return e.closeStatementObjects(ctx)
}

func (e *Engine) closeStatementObjects(ctx context.Context) (err error) {
	g := &e.statementGC
	g.Lock()
	if g.shutdownDone != nil {
		done, finalized, result := g.shutdownDone, g.finalized, g.stopErr
		g.Unlock()
		if finalized {
			return result
		}
		select {
		case <-done:
			g.Lock()
			result = g.stopErr
			g.Unlock()
			return result
		case <-ctx.Done():
			// This caller cannot cancel the authoritative close or replace
			// its eventual result with a shorter caller-local deadline.
			return context.Cause(ctx)
		}
	}
	g.shutdownDone = make(chan struct{})
	g.closing = true
	g.notifyLocked()
	defer func() {
		g.Lock()
		g.stopErr, g.finalized = err, true
		close(g.shutdownDone)
		g.Unlock()
	}()
	for !g.stopped {
		if cause := context.Cause(ctx); cause != nil {
			err = errors.Join(cause, g.lastErr)
			g.stopped = true
			break
		}
		if g.views == 0 && g.head == nil && !g.inflight {
			g.stopped = true
			break
		}
		changed := g.changed
		g.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		g.Lock()
	}
	cancel, done := g.cancel, g.done
	g.notifyLocked()
	g.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		default:
			select {
			case <-done:
			case <-ctx.Done():
				err = errors.Join(err, context.Cause(ctx))
			}
		}
	}
	return err
}

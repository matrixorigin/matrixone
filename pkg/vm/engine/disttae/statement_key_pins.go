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
	"reflect"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Pins protect existing workspace storage, not a second writable workspace.
// Batch changes take txn.Lock before this mutex. GC only needs this mutex and
// never acquires txn.Lock; reader cancellation never waits for a reader.
type statementKeyPins struct {
	sync.Mutex
	batches map[*batch.Batch]*statementBatchPin
	objects map[string]*statementObjectPin
	views   map[*statementKeyView]struct{}
}

type statementBatchPin struct {
	refs     int
	detached bool
}
type statementObjectPin struct {
	refs      int
	pendingGC bool
	name      string
	retryOne  bool
	next      *statementObjectPin
}

// An object name is a UUID and a uint16 suffix (at most 42 bytes). Include
// allocator rounding and list ownership; the existing pin doubles as the GC
// record, with no allocation at the last-reader handoff.
// Computed once from the record layout; every pin uses the same fixed charge.
var statementObjectPinQuota = int64(reflect.TypeFor[statementObjectPin]().Size()) + 128

func (txn *Transaction) batchPinned(b *batch.Batch) bool {
	p := txn.statementKeyPins.Load()
	if p == nil || b == nil {
		return false
	}
	p.Lock()
	defer p.Unlock()
	return p.batches[b] != nil
}

// releaseWorkspaceBatchLocked transfers detached storage to its last reader.
// The workspace entry has already relinquished its accounting and selections.
func (txn *Transaction) releaseWorkspaceBatchLocked(b *batch.Batch) {
	p := txn.statementKeyPins.Load()
	if p != nil {
		p.Lock()
		pin := p.batches[b]
		if pin != nil {
			pin.detached = true
			p.Unlock()
			return
		}
		p.Unlock()
	}
	b.Clean(txn.proc.Mp())
}

func (txn *Transaction) invalidateStatementKeyViewsLocked() {
	p := txn.statementKeyPins.Load()
	if p == nil {
		return
	}
	p.Lock()
	defer p.Unlock()
	for v := range p.views {
		v.cancel(moerr.NewTxnNeedRetryNoCtx())
	}
}

// deferPinnedObjectGC runs after the existing clone-file protection check.
// A rollback may detach an object while a cancelled reader still owns its IO.
func (txn *Transaction) deferPinnedObjectGC(names []string) []string {
	p := txn.statementKeyPins.Load()
	if p == nil {
		return names
	}
	p.Lock()
	defer p.Unlock()
	out := names[:0]
	for _, name := range names {
		if pin := p.objects[name]; pin != nil {
			pin.pendingGC = true
		} else {
			out = append(out, name)
		}
	}
	return out
}

func (v *statementKeyView) releasePins() (head, tail *statementObjectPin) {
	txn := v.txn
	txn.Lock()
	p := txn.statementKeyPins.Load()
	p.Lock()
	for _, t := range v.tables {
		for _, e := range t.entries {
			pin := p.batches[e.bat]
			pin.refs--
			if pin.refs == 0 {
				delete(p.batches, e.bat)
				if pin.detached {
					e.bat.Clean(txn.proc.Mp())
				}
			}
		}
	}
	for name := range v.objects {
		pin := p.objects[name]
		pin.refs--
		if pin.refs == 0 {
			if pin.pendingGC {
				if tail == nil {
					head = pin
				} else {
					tail.next = pin
				}
				tail = pin
			} else {
				txn.engine.ReleaseQuota(statementObjectPinQuota)
			}
			delete(p.objects, name)
		}
	}
	delete(p.views, v)
	if len(p.views) == 0 {
		// All detached batches and deferred object ownership have moved to
		// this last close. Release the maps' retained capacity as well.
		p.batches, p.objects, p.views = nil, nil, nil
		txn.statementKeyPins.Store(nil)
	}
	p.Unlock()
	txn.Unlock()
	// Submission/IO and budget release are outside the transaction mutex.
	v.clearCapture()
	for _, reservation := range v.reservations {
		reservation.Release()
	}
	v.reservations = nil
	return head, tail
}

func statementViewCause(ctx context.Context, v *statementKeyView) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return context.Cause(v.ctx)
}

// LOAD rollback already owns the install lock. Lookup performs no internal SQL;
// cancel and join its reader directly, outside txn.Lock, instead of waiting for
// the caller's own runSQL token or deferring deletion beyond install ownership.
func (txn *Transaction) quiesceStatementViewsForLoad(names []string) error {
	txn.Lock()
	p := txn.statementKeyPins.Load()
	if p == nil {
		txn.Unlock()
		return nil
	}
	p.Lock()
	var views []*statementKeyView
	for view := range p.views {
		for _, name := range names {
			if _, captured := view.objects[name]; captured {
				view.cancel(moerr.NewTxnNeedRetryNoCtx())
				views = append(views, view)
				break
			}
		}
	}
	p.Unlock()
	txn.Unlock()
	for _, view := range views {
		if err := view.Close(); err != nil {
			return err
		}
	}
	return nil
}

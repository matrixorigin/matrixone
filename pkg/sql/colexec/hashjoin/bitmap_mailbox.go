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

package hashjoin

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
)

// BitmapMailbox is the single ownership boundary for the parallel right-join
// bitmap exchange. A successful Send transfers ownership to the mailbox. Once
// sealed, late senders retain ownership and normal operator cleanup frees it.
type BitmapMailbox struct {
	mu     sync.Mutex
	sealed bool
	ch     chan *bitmap.Bitmap
}

func NewBitmapMailbox(workers int) *BitmapMailbox {
	if workers < 1 {
		workers = 1
	}
	return &BitmapMailbox{ch: make(chan *bitmap.Bitmap, workers)}
}

func (m *BitmapMailbox) Send(value *bitmap.Bitmap) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealed {
		return false
	}
	// Every non-merger publishes at most once and the mailbox capacity equals
	// the worker count, so publication cannot block while holding mu.
	m.ch <- value
	return true
}

func (m *BitmapMailbox) Receive(
	ctx context.Context,
) (*bitmap.Bitmap, bool) {
	if m == nil || ctx == nil {
		return nil, false
	}
	select {
	case <-ctx.Done():
		return nil, false
	case value := <-m.ch:
		return value, true
	}
}

// SealAndDrain makes cancellation order-independent. It owns and frees every
// value already transferred into the mailbox; concurrent or later Send calls
// fail and leave ownership with their sender.
func (m *BitmapMailbox) SealAndDrain(mp *mpool.MPool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.sealed = true
	for {
		select {
		case value := <-m.ch:
			colexec.FreeAccountedBitmap(value, mp)
		default:
			m.mu.Unlock()
			return
		}
	}
}

func (m *BitmapMailbox) Terminal() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sealed && len(m.ch) == 0
}

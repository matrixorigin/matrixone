// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	// FIXME: do we need to make event buffer size configurable?
	eventBufferSize = 6 * 1024
)

// Notifier provides incremental logtail.
type Notifier struct {
	ctx context.Context
	C   chan event

	mu     sync.RWMutex
	closed bool
}

func NewNotifier(ctx context.Context, buffer int) *Notifier {
	return &Notifier{
		ctx: ctx,
		C:   make(chan event, buffer),
	}
}

// NotifyLogtail provides incremental logtail.
func (n *Notifier) NotifyLogtail(
	from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail,
) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
		return context.Canceled
	}
	select {
	case <-n.ctx.Done():
		return n.ctx.Err()
	case n.C <- event{from: from, to: to, closeCB: closeCB, logtails: tails}:
	}
	return nil
}

// Drain releases events which were accepted by the notifier but cannot be
// published because the server is shutting down. Call it only after all event
// consumers have stopped, otherwise ownership would race with publication.
func (n *Notifier) Drain() {
	n.mu.Lock()
	n.closed = true
	callbacks := make([]func(), 0, len(n.C))
	for {
		select {
		case event := <-n.C:
			if event.closeCB != nil {
				callbacks = append(callbacks, event.closeCB)
			}
		default:
			n.mu.Unlock()
			for _, callback := range callbacks {
				callback()
			}
			return
		}
	}
}

type event struct {
	from, to timestamp.Timestamp
	closeCB  func()
	logtails []logtail.TableLogtail
}

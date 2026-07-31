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

package compile

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// statementAllocationAttempt owns one local execution generation. The
// MessageBoard pointer is captured at open so prepared/retry Reset cannot make
// terminal cleanup drain a newer board.
type statementAllocationAttempt struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
	board    *message.MessageBoard
	exporter func(mpool.AllocationAccountTerminalSnapshot)

	once     sync.Once
	snapshot mpool.AllocationAccountTerminalSnapshot
	err      error
}

func (c *Compile) beginAllocationAccountAttempt() (
	*statementAllocationAttempt,
	error,
) {
	if c == nil || c.allocationAccountRegistry == nil {
		return nil, nil
	}
	if c.proc == nil || c.MessageBoard == nil || c.allocationAttempt != nil ||
		c.allocationTerminalExporter == nil {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	account, err := c.allocationAccountRegistry.Open(c.allocationAccountLimit)
	if err != nil {
		return nil, err
	}
	attempt := &statementAllocationAttempt{
		registry: c.allocationAccountRegistry,
		account:  account,
		board:    c.MessageBoard,
		exporter: c.allocationTerminalExporter,
	}
	c.allocationAttempt = attempt
	return attempt, nil
}

func (a *statementAllocationAttempt) finish() (
	mpool.AllocationAccountTerminalSnapshot,
	error,
) {
	if a == nil {
		return mpool.AllocationAccountTerminalSnapshot{}, nil
	}
	a.once.Do(func() {
		// Scope.Run/MergeRun and remote notifier barriers must have returned
		// before this point. Draining the board first releases queued JoinMap
		// and spill payload ownership through their normal Destroy methods.
		a.board.CloseAndDrain()
		var first bool
		a.snapshot, first, a.err = a.registry.CompleteTerminal(a.account)
		if first && a.exporter != nil {
			a.exporter(a.snapshot)
		}
	})
	return a.snapshot, a.err
}

func (c *Compile) finishAllocationAccountAttempt() error {
	if c == nil || c.allocationAttempt == nil {
		return nil
	}
	attempt := c.allocationAttempt
	c.allocationAttempt = nil
	_, err := attempt.finish()
	return err
}

func (c *Compile) copyAllocationAccountLifecycleTo(dst *Compile) {
	if c == nil || dst == nil {
		return
	}
	dst.ConfigureAllocationAccountLifecycle(
		c.allocationAccountRegistry,
		c.allocationAccountLimit,
		c.allocationTerminalExporter,
	)
}

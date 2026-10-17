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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	ScalarSetMemoryLimit        = uint64(256 << 20)
	ScalarSetCandidatePairLimit = uint64(1 << 24)
	ScalarSetConcurrencyLimit   = uint64(2)
)

// ScalarSetExecutionLease belongs to one compile attempt. All marked joins in
// that attempt share its monotonically consumed work budget; output yields do
// not refund work. The CN permit is released after scope and account drainage.
type ScalarSetExecutionLease struct {
	budget *ExecutionResourceBudget
	used   atomic.Uint64
	closed atomic.Bool
}

func (g *ExecutionResourceGeneration) AcquireScalarSetExecution() (*ScalarSetExecutionLease, error) {
	if g == nil || g.budget == nil {
		return nil, ErrExecutionResourceInvalid
	}
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || g.closed {
		return nil, ErrExecutionResourceClosed
	}
	if b.scalarSetQueries >= ScalarSetConcurrencyLimit {
		return nil, moerr.NewScalarSetResourceBudgetExceeded(context.Background(), "concurrency", b.scalarSetQueries, 1, ScalarSetConcurrencyLimit)
	}
	b.scalarSetQueries++
	return &ScalarSetExecutionLease{budget: b}, nil
}

func (l *ScalarSetExecutionLease) ChargeCandidatePairs(pairs uint64) error {
	if l == nil || l.closed.Load() {
		return ErrExecutionResourceClosed
	}
	for {
		used := l.used.Load()
		if used > ScalarSetCandidatePairLimit || pairs > ScalarSetCandidatePairLimit-used {
			return moerr.NewScalarSetResourceBudgetExceeded(context.Background(), "candidate-pair", used, pairs, ScalarSetCandidatePairLimit)
		}
		if l.used.CompareAndSwap(used, used+pairs) {
			return nil
		}
	}
}

func (l *ScalarSetExecutionLease) CandidatePairs() uint64 {
	if l == nil {
		return 0
	}
	return l.used.Load()
}

func (l *ScalarSetExecutionLease) Close() {
	if l != nil && l.closed.CompareAndSwap(false, true) {
		l.budget.mu.Lock()
		l.budget.scalarSetQueries--
		l.budget.mu.Unlock()
	}
}

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

package process

import (
	"context"
	"sync"
	"time"
)

// PipelineCleanupTimeout bounds synchronous object cleanup across Reset, Free
// and remote finalization. Tests may shorten it before starting an execution.
var PipelineCleanupTimeout = 10 * time.Minute

type cleanupBudgetKey struct{}
type cleanupBudget struct {
	sync.Mutex
	deadline time.Time
}

func withCleanupBudget(ctx context.Context) context.Context {
	return context.WithValue(ctx, cleanupBudgetKey{}, &cleanupBudget{})
}

// BeginPipelineCleanup starts the execution's shared budget once, before any
// operator teardown. It does not change execution cancellation. A new query
// context gives prepared executions a fresh budget; retries use a fresh context.
func BeginPipelineCleanup(ctx context.Context) {
	if budget, ok := ctx.Value(cleanupBudgetKey{}).(*cleanupBudget); ok {
		budget.Lock()
		defer budget.Unlock()
		if budget.deadline.IsZero() {
			budget.deadline = time.Now().Add(PipelineCleanupTimeout)
		}
	}
}

func PipelineCleanupDeadline(ctx context.Context) (time.Time, bool) {
	if budget, ok := ctx.Value(cleanupBudgetKey{}).(*cleanupBudget); ok {
		budget.Lock()
		defer budget.Unlock()
		return budget.deadline, !budget.deadline.IsZero()
	}
	return time.Time{}, false
}

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

package resource

import "sync"

// AttemptState is the coordinator-owned lifecycle state.
type AttemptState uint8

const (
	AttemptOpen AttemptState = iota
	AttemptClosing
	AttemptSealed
)

// Attempt reduces the facts of one retry generation. Remote fragment and
// memory-domain completeness are already reduced by the compile layer before
// they reach this object; keeping a second unused slot state machine here made
// the ownership model look stronger than the production implementation.
type Attempt struct {
	mu      sync.Mutex
	state   AttemptState
	summary AttemptSummary
}

func NewAttempt() *Attempt { return &Attempt{} }

// AddLocal merges a quiescent attempt-local delta before sealing.
func (a *Attempt) AddLocal(delta Delta) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == AttemptSealed {
		return false
	}
	a.summary.Quality |= delta.Quality | MergeUsage(&a.summary.Usage, delta.Usage)
	return true
}

// BeginClosing establishes the single terminal owner.
func (a *Attempt) BeginClosing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != AttemptOpen {
		return false
	}
	a.state = AttemptClosing
	return true
}

// Seal records terminal wall time and outcome. Repeated calls are idempotent.
func (a *Attempt) Seal(wallNS uint64, outcome Outcome) AttemptSummary {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == AttemptSealed {
		return a.summary
	}
	if a.state == AttemptOpen {
		a.summary.Quality |= QualityInvariantFailure
	}
	a.summary.WallNS = wallNS
	a.summary.Outcome = outcome
	a.state = AttemptSealed
	return a.summary
}

func (a *Attempt) State() AttemptState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

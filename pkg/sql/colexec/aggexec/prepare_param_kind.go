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

package aggexec

import "github.com/matrixorigin/matrixone/pkg/container/vector"

type prepareParamKindState struct {
	kind      vector.PrepareParamKind
	seen      bool
	preserves bool
}

// PrepareParamKindStates holds prepared-parameter conversion provenance for
// one operator execution. Aggregate expressions are plan configuration and can
// be shared by parallel scopes, while these states must be owned by exactly one
// operator container and reset at each Prepare generation.
type PrepareParamKindStates struct {
	states []prepareParamKindState
}

// Reset starts a new execution generation. It reuses the bounded state slice
// and precomputes which aggregates can preserve their first argument's source
// conversion category.
func (s *PrepareParamKindStates) Reset(aggs []AggFuncExecExpression) {
	if cap(s.states) < len(aggs) {
		s.states = make([]prepareParamKindState, len(aggs))
	} else {
		s.states = s.states[:len(aggs)]
	}
	for i := range aggs {
		s.states[i] = prepareParamKindState{
			preserves: aggs[i].PreservesFirstArgPrepareParamKind(),
		}
	}
}

// Observe folds one input vector's source conversion category into the
// aggregate's execution state. Mixed categories conservatively fall back to
// ordinary string conversion.
func (s *PrepareParamKindStates) Observe(index int, kind vector.PrepareParamKind) {
	state := &s.states[index]
	if !state.preserves {
		return
	}
	if !state.seen {
		state.kind = kind
		state.seen = true
		return
	}
	if state.kind != kind {
		state.kind = vector.PrepareParamNone
	}
}

// ObserveState folds a serialized partial aggregate state. An unobserved
// partial (for example an empty input scope) does not affect the reduction.
func (s *PrepareParamKindStates) ObserveState(
	index int,
	kind vector.PrepareParamKind,
	seen bool,
) {
	if seen {
		s.Observe(index, kind)
	}
}

func (s *PrepareParamKindStates) GetState(index int) (vector.PrepareParamKind, bool) {
	if index < 0 || index >= len(s.states) {
		return vector.PrepareParamNone, false
	}
	state := &s.states[index]
	return state.kind, state.seen
}

func (s *PrepareParamKindStates) Get(index int) vector.PrepareParamKind {
	if index < 0 || index >= len(s.states) {
		return vector.PrepareParamNone
	}
	state := &s.states[index]
	if !state.seen {
		return vector.PrepareParamNone
	}
	return state.kind
}

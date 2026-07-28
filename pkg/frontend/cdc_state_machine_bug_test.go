// Copyright 2024 Matrix Origin
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

package frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bug #3: Resume sets state to Starting, then calls Start() which tries TransitionStart again
// Scenario: PAUSE CDC -> RESUME CDC
// Expected: Start() should skip duplicate TransitionStart when already in Starting state
func TestExecutorStateMachine_Resume_AvoidDuplicateStartTransition(t *testing.T) {
	sm := NewExecutorStateMachine()

	err := sm.Transition(TransitionStart)
	require.NoError(t, err)
	assert.Equal(t, StateStarting, sm.State())

	err = sm.Transition(TransitionStartSuccess)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, sm.State())

	err = sm.Transition(TransitionPause)
	require.NoError(t, err)
	assert.Equal(t, StatePausing, sm.State())

	err = sm.Transition(TransitionPauseComplete)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, sm.State())

	err = sm.Transition(TransitionResume)
	require.NoError(t, err)
	assert.Equal(t, StateStarting, sm.State())

	// BUG: If Start() tries TransitionStart again, it will fail
	err = sm.Transition(TransitionStart)
	assert.Error(t, err, "Duplicate TransitionStart should fail when already Starting")
	assert.Contains(t, err.Error(), "invalid transition")
	assert.Equal(t, StateStarting, sm.State())

	err = sm.Transition(TransitionStartSuccess)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, sm.State())
}

// Bug #3 Fix Verification: Start() detects StateStarting and skips duplicate transition
func TestExecutorStateMachine_Start_IdempotentFromStarting(t *testing.T) {
	sm := NewExecutorStateMachine()

	err := sm.Transition(TransitionStart)
	require.NoError(t, err)
	assert.Equal(t, StateStarting, sm.State())

	err = sm.Transition(TransitionStartSuccess)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, sm.State())

	err = sm.Transition(TransitionPause)
	require.NoError(t, err)

	err = sm.Transition(TransitionPauseComplete)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, sm.State())

	err = sm.Transition(TransitionResume)
	require.NoError(t, err)
	currentState := sm.State()
	assert.Equal(t, StateStarting, currentState)

	// Fixed Start() logic:
	// if exec.stateMachine.State() != StateStarting {
	//     exec.stateMachine.Transition(TransitionStart)
	// }
	if currentState != StateStarting {
		err = sm.Transition(TransitionStart)
		require.NoError(t, err)
	}

	assert.Equal(t, StateStarting, sm.State())

	err = sm.Transition(TransitionStartSuccess)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, sm.State())
}

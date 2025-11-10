// Copyright 2022 Matrix Origin
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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorStateMachine_InitialState(t *testing.T) {
	sm := NewExecutorStateMachine()
	assert.Equal(t, StateIdle, sm.State())
	assert.False(t, sm.IsRunning())
	assert.False(t, sm.IsActive())
}

func TestExecutorStateMachine_ValidTransitions(t *testing.T) {
	tests := []struct {
		name        string
		transitions []Transition
		finalState  ExecutorState
	}{
		{
			name:        "Start to Running",
			transitions: []Transition{TransitionStart, TransitionStartSuccess},
			finalState:  StateRunning,
		},
		{
			name:        "Start to Failed",
			transitions: []Transition{TransitionStart, TransitionStartFail},
			finalState:  StateFailed,
		},
		{
			name:        "Pause flow",
			transitions: []Transition{TransitionStart, TransitionStartSuccess, TransitionPause, TransitionPauseComplete},
			finalState:  StatePaused,
		},
		{
			name:        "Resume flow",
			transitions: []Transition{TransitionStart, TransitionStartSuccess, TransitionPause, TransitionPauseComplete, TransitionResume, TransitionStartSuccess},
			finalState:  StateRunning,
		},
		{
			name:        "Restart from Running",
			transitions: []Transition{TransitionStart, TransitionStartSuccess, TransitionRestart, TransitionRestartBegin, TransitionStartSuccess},
			finalState:  StateRunning,
		},
		{
			name:        "Restart from Failed",
			transitions: []Transition{TransitionStart, TransitionStartFail, TransitionRestart, TransitionRestartBegin, TransitionStartSuccess},
			finalState:  StateRunning,
		},
		{
			name:        "Cancel from Running",
			transitions: []Transition{TransitionStart, TransitionStartSuccess, TransitionCancel, TransitionCancelComplete},
			finalState:  StateCancelled,
		},
		{
			name:        "Cancel from Paused",
			transitions: []Transition{TransitionStart, TransitionStartSuccess, TransitionPause, TransitionPauseComplete, TransitionCancel, TransitionCancelComplete},
			finalState:  StateCancelled,
		},
		{
			name:        "Cancel during Start",
			transitions: []Transition{TransitionStart, TransitionCancel, TransitionCancelComplete},
			finalState:  StateCancelled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewExecutorStateMachine()
			for i, trans := range tt.transitions {
				err := sm.Transition(trans)
				require.NoError(t, err, "transition %d (%s) should succeed", i, trans)
			}
			assert.Equal(t, tt.finalState, sm.State())
		})
	}
}

func TestExecutorStateMachine_InvalidTransitions(t *testing.T) {
	tests := []struct {
		name              string
		setupTransitions  []Transition
		invalidTransition Transition
	}{
		{
			name:              "Cannot Start from Running",
			setupTransitions:  []Transition{TransitionStart, TransitionStartSuccess},
			invalidTransition: TransitionStart,
		},
		{
			name:              "Cannot Pause from Idle",
			setupTransitions:  []Transition{},
			invalidTransition: TransitionPause,
		},
		{
			name:              "Cannot Resume from Idle",
			setupTransitions:  []Transition{},
			invalidTransition: TransitionResume,
		},
		{
			name:              "Cannot Pause twice",
			setupTransitions:  []Transition{TransitionStart, TransitionStartSuccess, TransitionPause},
			invalidTransition: TransitionPause,
		},
		{
			name:              "Cannot Resume from Running",
			setupTransitions:  []Transition{TransitionStart, TransitionStartSuccess},
			invalidTransition: TransitionResume,
		},
		{
			name:              "Cannot transition from Cancelled",
			setupTransitions:  []Transition{TransitionStart, TransitionStartSuccess, TransitionCancel, TransitionCancelComplete},
			invalidTransition: TransitionStart,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewExecutorStateMachine()
			for _, trans := range tt.setupTransitions {
				require.NoError(t, sm.Transition(trans))
			}
			err := sm.Transition(tt.invalidTransition)
			assert.Error(t, err, "invalid transition should fail")
		})
	}
}

func TestExecutorStateMachine_CanTransition(t *testing.T) {
	sm := NewExecutorStateMachine()

	// From Idle
	assert.True(t, sm.CanTransition(TransitionStart))
	assert.False(t, sm.CanTransition(TransitionPause))

	// Transition to Running
	require.NoError(t, sm.Transition(TransitionStart))
	require.NoError(t, sm.Transition(TransitionStartSuccess))

	// From Running
	assert.True(t, sm.CanTransition(TransitionPause))
	assert.True(t, sm.CanTransition(TransitionRestart))
	assert.True(t, sm.CanTransition(TransitionCancel))
	assert.False(t, sm.CanTransition(TransitionStart))
	assert.False(t, sm.CanTransition(TransitionResume))
}

func TestExecutorStateMachine_IsRunning(t *testing.T) {
	sm := NewExecutorStateMachine()

	assert.False(t, sm.IsRunning(), "Idle should not be running")

	require.NoError(t, sm.Transition(TransitionStart))
	assert.True(t, sm.IsRunning(), "Starting should be running")

	require.NoError(t, sm.Transition(TransitionStartSuccess))
	assert.True(t, sm.IsRunning(), "Running should be running")

	require.NoError(t, sm.Transition(TransitionPause))
	assert.False(t, sm.IsRunning(), "Pausing should not be running")

	require.NoError(t, sm.Transition(TransitionPauseComplete))
	assert.False(t, sm.IsRunning(), "Paused should not be running")
}

func TestExecutorStateMachine_IsActive(t *testing.T) {
	sm := NewExecutorStateMachine()

	assert.False(t, sm.IsActive(), "Idle should not be active")

	require.NoError(t, sm.Transition(TransitionStart))
	assert.True(t, sm.IsActive(), "Starting should be active")

	require.NoError(t, sm.Transition(TransitionStartSuccess))
	assert.True(t, sm.IsActive(), "Running should be active")

	require.NoError(t, sm.Transition(TransitionPause))
	assert.True(t, sm.IsActive(), "Pausing should be active")

	require.NoError(t, sm.Transition(TransitionPauseComplete))
	assert.True(t, sm.IsActive(), "Paused should be active")

	require.NoError(t, sm.Transition(TransitionCancel))
	assert.True(t, sm.IsActive(), "Cancelling should be active")

	require.NoError(t, sm.Transition(TransitionCancelComplete))
	assert.False(t, sm.IsActive(), "Cancelled should not be active")
}

func TestExecutorStateMachine_SetFailed(t *testing.T) {
	sm := NewExecutorStateMachine()

	// Cannot set failed from Idle
	err := sm.SetFailed("test error")
	assert.Error(t, err)

	// Can set failed from Starting
	require.NoError(t, sm.Transition(TransitionStart))
	err = sm.SetFailed("startup failed")
	assert.NoError(t, err)
	assert.Equal(t, StateFailed, sm.State())
	assert.Equal(t, "startup failed", sm.GetErrorMessage())

	// Error message cleared after leaving Failed
	require.NoError(t, sm.Transition(TransitionRestart))
	require.NoError(t, sm.Transition(TransitionRestartBegin))
	assert.Equal(t, StateStarting, sm.State())
	assert.Equal(t, "", sm.GetErrorMessage())
}

func TestExecutorStateMachine_Concurrency(t *testing.T) {
	sm := NewExecutorStateMachine()
	require.NoError(t, sm.Transition(TransitionStart))
	require.NoError(t, sm.Transition(TransitionStartSuccess))

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sm.State()
			_ = sm.IsRunning()
			_ = sm.IsActive()
			_ = sm.CanTransition(TransitionPause)
		}()
	}

	// Concurrent writes (only one should succeed)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := sm.Transition(TransitionPause)
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Should be in Pausing or PauseComplete state
	state := sm.State()
	assert.True(t, state == StatePausing || state == StatePaused,
		"should be in Pausing or Paused state, got %s", state)

	// Most transitions should fail
	errorCount := len(errors)
	assert.Greater(t, errorCount, 40, "most concurrent transitions should fail")
}

func TestExecutorStateMachine_String(t *testing.T) {
	sm := NewExecutorStateMachine()

	assert.Contains(t, sm.String(), "Idle")

	require.NoError(t, sm.Transition(TransitionStart))
	require.NoError(t, sm.SetFailed("test error"))
	assert.Contains(t, sm.String(), "Failed")
	assert.Contains(t, sm.String(), "test error")
}

func TestExecutorState_String(t *testing.T) {
	tests := []struct {
		state    ExecutorState
		expected string
	}{
		{StateIdle, "Idle"},
		{StateStarting, "Starting"},
		{StateRunning, "Running"},
		{StatePausing, "Pausing"},
		{StatePaused, "Paused"},
		{StateRestarting, "Restarting"},
		{StateCancelling, "Cancelling"},
		{StateCancelled, "Cancelled"},
		{StateFailed, "Failed"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestTransition_String(t *testing.T) {
	tests := []struct {
		transition Transition
		expected   string
	}{
		{TransitionStart, "Start"},
		{TransitionStartSuccess, "StartSuccess"},
		{TransitionStartFail, "StartFail"},
		{TransitionPause, "Pause"},
		{TransitionPauseComplete, "PauseComplete"},
		{TransitionResume, "Resume"},
		{TransitionRestart, "Restart"},
		{TransitionRestartBegin, "RestartBegin"},
		{TransitionCancel, "Cancel"},
		{TransitionCancelComplete, "CancelComplete"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.transition.String())
		})
	}
}

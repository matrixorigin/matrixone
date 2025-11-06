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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ExecutorState represents the state of a CDCTaskExecutor
type ExecutorState int

const (
	// StateIdle is the initial state after creation
	StateIdle ExecutorState = iota
	// StateStarting is the state when Start() is in progress
	StateStarting
	// StateRunning is the state when the executor is actively running
	StateRunning
	// StatePausing is the state when Pause() is in progress
	StatePausing
	// StatePaused is the state when the executor is paused
	StatePaused
	// StateRestarting is the state when Restart() is in progress
	StateRestarting
	// StateCancelling is the state when Cancel() is in progress
	StateCancelling
	// StateCancelled is the state after Cancel() completes
	StateCancelled
	// StateFailed is the state when Start() or other operations fail
	StateFailed
)

// String returns the string representation of ExecutorState
func (s ExecutorState) String() string {
	switch s {
	case StateIdle:
		return "Idle"
	case StateStarting:
		return "Starting"
	case StateRunning:
		return "Running"
	case StatePausing:
		return "Pausing"
	case StatePaused:
		return "Paused"
	case StateRestarting:
		return "Restarting"
	case StateCancelling:
		return "Cancelling"
	case StateCancelled:
		return "Cancelled"
	case StateFailed:
		return "Failed"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// Transition represents a state transition action
type Transition int

const (
	// TransitionStart represents the Start action
	TransitionStart Transition = iota
	// TransitionStartSuccess represents successful Start completion
	TransitionStartSuccess
	// TransitionStartFail represents failed Start
	TransitionStartFail
	// TransitionPause represents the Pause action
	TransitionPause
	// TransitionPauseComplete represents Pause completion
	TransitionPauseComplete
	// TransitionResume represents the Resume action
	TransitionResume
	// TransitionRestart represents the Restart action
	TransitionRestart
	// TransitionRestartBegin represents Restart beginning new Start
	TransitionRestartBegin
	// TransitionCancel represents the Cancel action
	TransitionCancel
	// TransitionCancelComplete represents Cancel completion
	TransitionCancelComplete
)

// String returns the string representation of Transition
func (t Transition) String() string {
	switch t {
	case TransitionStart:
		return "Start"
	case TransitionStartSuccess:
		return "StartSuccess"
	case TransitionStartFail:
		return "StartFail"
	case TransitionPause:
		return "Pause"
	case TransitionPauseComplete:
		return "PauseComplete"
	case TransitionResume:
		return "Resume"
	case TransitionRestart:
		return "Restart"
	case TransitionRestartBegin:
		return "RestartBegin"
	case TransitionCancel:
		return "Cancel"
	case TransitionCancelComplete:
		return "CancelComplete"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// ExecutorStateMachine manages state transitions for CDCTaskExecutor
type ExecutorStateMachine struct {
	mu           sync.RWMutex
	currentState ExecutorState
	transitions  map[ExecutorState]map[Transition]ExecutorState
	errorMessage string // Store error message when in StateFailed
}

// NewExecutorStateMachine creates a new state machine in Idle state
func NewExecutorStateMachine() *ExecutorStateMachine {
	sm := &ExecutorStateMachine{
		currentState: StateIdle,
		transitions:  make(map[ExecutorState]map[Transition]ExecutorState),
	}
	sm.initTransitions()
	return sm
}

// initTransitions defines all valid state transitions
func (sm *ExecutorStateMachine) initTransitions() {
	// From Idle
	sm.addTransition(StateIdle, TransitionStart, StateStarting)

	// From Starting
	sm.addTransition(StateStarting, TransitionStartSuccess, StateRunning)
	sm.addTransition(StateStarting, TransitionStartFail, StateFailed)
	sm.addTransition(StateStarting, TransitionCancel, StateCancelling) // Cancel during startup

	// From Running
	sm.addTransition(StateRunning, TransitionPause, StatePausing)
	sm.addTransition(StateRunning, TransitionRestart, StateRestarting)
	sm.addTransition(StateRunning, TransitionCancel, StateCancelling)

	// From Pausing
	sm.addTransition(StatePausing, TransitionPauseComplete, StatePaused)
	sm.addTransition(StatePausing, TransitionCancel, StateCancelling) // Cancel during pause

	// From Paused
	sm.addTransition(StatePaused, TransitionResume, StateStarting)
	sm.addTransition(StatePaused, TransitionRestart, StateRestarting)
	sm.addTransition(StatePaused, TransitionCancel, StateCancelling)

	// From Restarting
	sm.addTransition(StateRestarting, TransitionRestartBegin, StateStarting)
	sm.addTransition(StateRestarting, TransitionCancel, StateCancelling)

	// From Cancelling
	sm.addTransition(StateCancelling, TransitionCancelComplete, StateCancelled)

	// From Failed
	sm.addTransition(StateFailed, TransitionRestart, StateRestarting)
	sm.addTransition(StateFailed, TransitionCancel, StateCancelling)
}

// addTransition adds a valid state transition
func (sm *ExecutorStateMachine) addTransition(from ExecutorState, via Transition, to ExecutorState) {
	if sm.transitions[from] == nil {
		sm.transitions[from] = make(map[Transition]ExecutorState)
	}
	sm.transitions[from][via] = to
}

// Transition attempts to transition to a new state
// Returns error if transition is not allowed
func (sm *ExecutorStateMachine) Transition(t Transition) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	nextState, valid := sm.transitions[sm.currentState][t]
	if !valid {
		return moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("invalid transition: %s -> %s (current state: %s)",
				sm.currentState, t, sm.currentState),
		)
	}

	oldState := sm.currentState
	sm.currentState = nextState

	// Clear error message when leaving Failed state
	if oldState == StateFailed && nextState != StateFailed {
		sm.errorMessage = ""
	}

	return nil
}

// MustTransition attempts to transition and panics on error
// Use this only when the transition is guaranteed to be valid
func (sm *ExecutorStateMachine) MustTransition(t Transition) {
	if err := sm.Transition(t); err != nil {
		panic(err)
	}
}

// State returns the current state
func (sm *ExecutorStateMachine) State() ExecutorState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentState
}

// IsState checks if current state matches the given state
func (sm *ExecutorStateMachine) IsState(state ExecutorState) bool {
	return sm.State() == state
}

// CanTransition checks if a transition is valid from current state
func (sm *ExecutorStateMachine) CanTransition(t Transition) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, valid := sm.transitions[sm.currentState][t]
	return valid
}

// SetFailed transitions to Failed state and stores error message
func (sm *ExecutorStateMachine) SetFailed(errMsg string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Can transition to Failed from Starting
	if sm.currentState == StateStarting {
		sm.currentState = StateFailed
		sm.errorMessage = errMsg
		return nil
	}

	return moerr.NewInternalErrorNoCtx(
		fmt.Sprintf("cannot set failed from state: %s", sm.currentState),
	)
}

// GetErrorMessage returns the error message if in Failed state
func (sm *ExecutorStateMachine) GetErrorMessage() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.currentState == StateFailed {
		return sm.errorMessage
	}
	return ""
}

// IsRunning returns true if the executor is in a running state
func (sm *ExecutorStateMachine) IsRunning() bool {
	state := sm.State()
	return state == StateRunning || state == StateStarting
}

// IsActive returns true if the executor is in an active state (not idle, cancelled, or failed)
func (sm *ExecutorStateMachine) IsActive() bool {
	state := sm.State()
	return state != StateIdle && state != StateCancelled && state != StateFailed
}

// String returns the string representation of the state machine
func (sm *ExecutorStateMachine) String() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.currentState == StateFailed && sm.errorMessage != "" {
		return fmt.Sprintf("State=%s, Error=%s", sm.currentState, sm.errorMessage)
	}
	return fmt.Sprintf("State=%s", sm.currentState)
}

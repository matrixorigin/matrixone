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
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// sequenceStatementState is the session-visible sequence state at the start
// of a statement attempt. Sequence functions update SeqAddValues and
// SeqLastValue while the statement runs, but those updates are only published
// to the frontend session after the statement succeeds. A retry reuses the
// same Process, so the failed attempt must not leave its values behind.
type sequenceStatementState struct {
	curValues  map[uint64]string
	addValues  map[uint64]string
	deleteKeys []uint64
	lastValue  string
}

func captureSequenceStatementState(proc *process.Process) sequenceStatementState {
	if proc == nil {
		return sequenceStatementState{}
	}
	info := proc.GetSessionInfo()
	state := sequenceStatementState{
		curValues:  cloneSequenceValues(info.SeqCurValues),
		addValues:  cloneSequenceValues(info.SeqAddValues),
		deleteKeys: append([]uint64(nil), info.SeqDeleteKeys...),
	}
	if len(info.SeqLastValue) != 0 {
		state.lastValue = info.SeqLastValue[0]
	}
	return state
}

func (c *Compile) restoreSequenceStatementState() {
	if c == nil || c.proc == nil {
		return
	}
	c.sequenceState.apply(c.proc)
}

func (state sequenceStatementState) apply(proc *process.Process) {
	info := proc.GetSessionInfo()
	info.SeqCurValues = cloneSequenceValues(state.curValues)
	info.SeqAddValues = cloneSequenceValues(state.addValues)
	info.SeqDeleteKeys = append([]uint64(nil), state.deleteKeys...)
	if len(info.SeqLastValue) == 0 {
		info.SeqLastValue = make([]string, 1)
	}
	info.SeqLastValue[0] = state.lastValue
}

func cloneSequenceValues(values map[uint64]string) map[uint64]string {
	if values == nil {
		return nil
	}
	cloned := make(map[uint64]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

// internalSequenceState is a synchronous CTAS handoff. The child owns its
// cloned attempt state; only a successful child supplies an output snapshot.
// The outer statement remains responsible for frontend publication and rollback.
type internalSequenceState struct {
	input  sequenceStatementState
	output *sequenceStatementState
}

type internalSequenceStateKey struct{}

func attachInternalSequenceState(ctx context.Context, proc *process.Process) (context.Context, *internalSequenceState) {
	state := &internalSequenceState{input: captureSequenceStatementState(proc)}
	return context.WithValue(ctx, internalSequenceStateKey{}, state), state
}

func consumeInternalSequenceState(ctx context.Context) (context.Context, *internalSequenceState) {
	state, _ := ctx.Value(internalSequenceStateKey{}).(*internalSequenceState)
	if state == nil {
		return ctx, nil
	}
	// Nested metadata SQL inherits this context. It must not consume or publish
	// the population statement's handoff while a sequence allocation is active.
	return context.WithValue(ctx, internalSequenceStateKey{}, (*internalSequenceState)(nil)), state
}

func initInternalSequenceState(proc *process.Process, database string, state *internalSequenceState) {
	proc.GetSessionInfo().Database = database
	proc.InitSeq()
	if state != nil {
		state.input.apply(proc)
		// A caller without prior sequence state still needs writable containers.
		info := proc.GetSessionInfo()
		if info.SeqCurValues == nil {
			info.SeqCurValues = make(map[uint64]string)
		}
		if info.SeqAddValues == nil {
			info.SeqAddValues = make(map[uint64]string)
		}
	}
}

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

import "github.com/matrixorigin/matrixone/pkg/vm/process"

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
	info := c.proc.GetSessionInfo()
	info.SeqCurValues = cloneSequenceValues(c.sequenceState.curValues)
	info.SeqAddValues = cloneSequenceValues(c.sequenceState.addValues)
	info.SeqDeleteKeys = append([]uint64(nil), c.sequenceState.deleteKeys...)
	if len(info.SeqLastValue) == 0 {
		info.SeqLastValue = make([]string, 1)
	}
	info.SeqLastValue[0] = c.sequenceState.lastValue
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

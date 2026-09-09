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

const warningDiagnosticRetentionLimit = 64

// warningAttemptState stages warnings produced by one physical execution
// attempt. A retry must not publish diagnostics from the failed attempt to the
// session, so the state is kept on BaseProcess and shared by child processes.
type warningAttemptState struct {
	generation  uint64
	accumulator WarningAccumulator
}

// warningDiagnosticBatchAppender is intentionally optional.  The execution
// process.Session interface is shared by internal/background sessions and must
// not acquire a dependency on frontend diagnostic storage.
type warningDiagnosticBatchAppender interface {
	AppendWarningBatch(total uint64, codes []uint16, messages []string)
}

type warningDiagnosticAppender interface {
	AppendWarningDiagnostic(code uint16, msg string)
}

// AppendWarningBatch forwards row diagnostics to the session that owns the
// statement.  Implementations with a batch API receive one lock/wire update;
// older or test-only sessions fall back to the retained records one by one.
// The caller owns the slices and may reuse them after this function returns.
func AppendWarningBatch(proc *Process, total uint64, codes []uint16, messages []string) {
	if proc == nil || total == 0 {
		return
	}
	if proc.appendWarningBatchToAttempt(total, codes, messages) {
		return
	}
	appendWarningBatchToSession(proc, total, codes, messages)
}

// AppendWarningBatchForAttempt appends a remote terminal batch only when its
// execution generation is still current. A late terminal from a canceled
// generation is consumed and discarded rather than being mistaken for a
// warning from the retry generation.
func AppendWarningBatchForAttempt(proc *Process, generation uint64, total uint64, codes []uint16, messages []string) {
	if proc == nil || total == 0 {
		return
	}
	if proc.appendWarningBatchToAttemptForGeneration(&generation, total, codes, messages) {
		return
	}
	appendWarningBatchToSession(proc, total, codes, messages)
}

func appendWarningBatchToSession(proc *Process, total uint64, codes []uint16, messages []string) {
	if proc == nil || total == 0 {
		return
	}
	session := proc.GetSession()
	if session == nil {
		return
	}
	if appender, ok := session.(warningDiagnosticBatchAppender); ok {
		appender.AppendWarningBatch(total, codes, messages)
		return
	}
	appender, ok := session.(warningDiagnosticAppender)
	if !ok {
		return
	}
	limit := len(codes)
	if len(messages) < limit {
		limit = len(messages)
	}
	for i := 0; i < limit; i++ {
		appender.AppendWarningDiagnostic(codes[i], messages[i])
	}
}

// BeginWarningAttempt starts a staging boundary for one physical execution
// attempt. The state lives on BaseProcess because pipeline child processes
// share it. Callers must end the previous attempt before starting another.
func (proc *Process) BeginWarningAttempt(generation uint64) {
	if proc == nil || proc.Base == nil {
		return
	}
	proc.Base.warningAttemptMu.Lock()
	proc.Base.warningAttempt = &warningAttemptState{generation: generation}
	proc.Base.warningAttemptTracking = true
	proc.Base.warningAttemptMu.Unlock()
}

func (proc *Process) appendWarningBatchToAttempt(total uint64, codes []uint16, messages []string) bool {
	return proc.appendWarningBatchToAttemptForGeneration(nil, total, codes, messages)
}

func (proc *Process) appendWarningBatchToAttemptForGeneration(generation *uint64, total uint64, codes []uint16, messages []string) bool {
	if proc == nil || proc.Base == nil || total == 0 {
		return false
	}
	proc.Base.warningAttemptMu.Lock()
	defer proc.Base.warningAttemptMu.Unlock()
	if proc.Base.warningAttempt == nil {
		// A generation-bound remote sender is only valid while its execution
		// attempt is open. Once the attempt is aborted or committed, a missing
		// state means a late terminal and must not fall through to the session.
		// A remote fragment executed through MergeRun has never opened this
		// lifecycle, so its nested remote warnings still use the legacy direct
		// forwarding path.
		return generation != nil && proc.Base.warningAttemptTracking
	}
	state := proc.Base.warningAttempt
	if generation != nil && state.generation != *generation {
		return true
	}
	if ^uint64(0)-state.accumulator.Total < total {
		state.accumulator.Total = ^uint64(0)
	} else {
		state.accumulator.Total += total
	}
	limit := len(codes)
	if len(messages) < limit {
		limit = len(messages)
	}
	for i := 0; i < limit && len(state.accumulator.Codes) < warningDiagnosticRetentionLimit; i++ {
		state.accumulator.Codes = append(state.accumulator.Codes, codes[i])
		state.accumulator.Messages = append(state.accumulator.Messages, messages[i])
	}
	return true
}

// CommitWarningAttempt publishes the surviving attempt's staged warnings to
// the owning session. The state is detached before publishing so the session
// callback cannot accidentally stage the committed batch again.
func (proc *Process) CommitWarningAttempt(generation uint64) {
	if proc == nil || proc.Base == nil {
		return
	}
	proc.Base.warningAttemptMu.Lock()
	state := proc.Base.warningAttempt
	if state == nil || state.generation != generation {
		proc.Base.warningAttemptMu.Unlock()
		return
	}
	proc.Base.warningAttempt = nil
	total := state.accumulator.Total
	codes := append([]uint16(nil), state.accumulator.Codes...)
	messages := append([]string(nil), state.accumulator.Messages...)
	proc.Base.warningAttemptMu.Unlock()
	appendWarningBatchToSession(proc, total, codes, messages)
}

// AbortWarningAttempt discards diagnostics generated by a failed physical
// execution attempt. It is called only after the attempt's producers have
// been quiesced by Compile.Run.
func (proc *Process) AbortWarningAttempt(generation uint64) {
	if proc == nil || proc.Base == nil {
		return
	}
	proc.Base.warningAttemptMu.Lock()
	if proc.Base.warningAttempt != nil && proc.Base.warningAttempt.generation == generation {
		proc.Base.warningAttempt = nil
	}
	proc.Base.warningAttemptMu.Unlock()
}

// WarningAccumulator keeps the exact statement warning count while retaining
// only the bounded records required by SHOW WARNINGS.  It is deliberately
// allocation-free after the first retained records and is local to one
// operator/function invocation, so large INSERT IGNORE statements cannot grow
// execution memory with the number of skipped rows.
type WarningAccumulator struct {
	Total    uint64
	Codes    []uint16
	Messages []string
}

func (a *WarningAccumulator) Add(code uint16, message string) {
	if a == nil {
		return
	}
	a.AddCount()
	if len(a.Codes) >= warningDiagnosticRetentionLimit {
		return
	}
	a.Codes = append(a.Codes, code)
	a.Messages = append(a.Messages, message)
}

// AddCount records a warning that is not retained because its diagnostic is
// unavailable or the bounded SHOW WARNINGS buffer is already full.
func (a *WarningAccumulator) AddCount() {
	if a == nil {
		return
	}
	if a.Total != ^uint64(0) {
		a.Total++
	}
}

// NeedsDiagnostic reports whether the next warning can still be retained.
// Callers can use it to avoid formatting large internal keys once the bounded
// diagnostic buffer is full.
func (a *WarningAccumulator) NeedsDiagnostic() bool {
	return a != nil && len(a.Codes) < warningDiagnosticRetentionLimit
}

func (a *WarningAccumulator) Flush(proc *Process) {
	if a == nil || a.Total == 0 {
		return
	}
	AppendWarningBatch(proc, a.Total, a.Codes, a.Messages)
	a.Total = 0
	a.Codes = a.Codes[:0]
	a.Messages = a.Messages[:0]
}

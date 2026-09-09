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

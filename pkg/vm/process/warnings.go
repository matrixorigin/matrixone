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

import (
	"strings"
	"unicode/utf8"
)

const warningDiagnosticRetentionLimit = 64

// WarningDiagnosticMaxMessageBytes bounds one retained warning message. The
// warning count remains exact even when the human-readable record is
// truncated or omitted.
const WarningDiagnosticMaxMessageBytes = 4 << 10

// WarningDiagnosticMaxBytes bounds the retained diagnostic payload for one
// execution attempt. Producers may still format a transient value, but no
// session, attempt collector, or terminal result retains more than this
// budget.
const WarningDiagnosticMaxBytes = 256 << 10

// WarningDiagnosticBatchAppender carries an exact count separately from the
// bounded records retained for SHOW WARNINGS. It is intentionally optional:
// process.Session is shared by internal/background sessions.
type WarningDiagnosticBatchAppender interface {
	AppendWarningBatch(total uint64, codes []uint16, messages []string)
}

// WarningDiagnosticAppender is the legacy one-record warning surface.
type WarningDiagnosticAppender interface {
	AppendWarningDiagnostic(code uint16, msg string)
}

// WarningDiagnosticCountAppender adds warnings which have no retained record.
// The caller passes only the unrepresented part of a batch, so a sink that
// implements both this interface and WarningDiagnosticAppender can preserve
// the exact count without fabricating records.
type WarningDiagnosticCountAppender interface {
	AppendWarningCount(total uint64)
}

// BoundWarningMessage returns an owned, UTF-8-safe warning message no longer
// than maxBytes. Cloning the result prevents a small retained slice from
// keeping a large producer buffer alive. A deterministic suffix makes
// truncation visible to clients.
func BoundWarningMessage(message string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(message) <= maxBytes {
		return strings.Clone(message)
	}
	const suffix = "… (truncated)"
	if maxBytes <= len(suffix) {
		candidate := []byte(suffix)
		candidate = candidate[:maxBytes]
		for len(candidate) > 0 && !utf8.Valid(candidate) {
			candidate = candidate[:len(candidate)-1]
		}
		return string(candidate)
	}
	prefixBytes := maxBytes - len(suffix)
	prefix := message[:prefixBytes]
	for prefixBytes > 0 && !utf8.ValidString(prefix) {
		prefixBytes--
		prefix = message[:prefixBytes]
	}
	out := make([]byte, 0, maxBytes)
	out = append(out, prefix...)
	out = append(out, suffix...)
	return string(out)
}

// AppendWarningBatch forwards row diagnostics to the process's current warning
// sink. Compile.Run installs a generation-specific sink for an execution
// attempt; using GetWarningSink here keeps local operators, nested execution,
// and remote terminal callbacks on that same ownership boundary. The caller
// owns the slices and may reuse them after this function returns.
func AppendWarningBatch(proc *Process, total uint64, codes []uint16, messages []string) {
	if proc == nil || total == 0 {
		return
	}
	AppendWarningBatchToSink(proc.GetWarningSink(), total, codes, messages)
}

// AppendWarningBatchToSink forwards a batch while preserving the exact count
// on sinks which expose the count separately. A record-only legacy sink is
// intentionally best-effort: it cannot represent a count larger than its
// retained diagnostics without inventing warning records.
func AppendWarningBatchToSink(destination any, total uint64, codes []uint16, messages []string) {
	if destination == nil || total == 0 {
		return
	}
	limit := len(codes)
	if len(messages) < limit {
		limit = len(messages)
	}
	if uint64(limit) > total {
		limit = int(total)
	}
	codes = codes[:limit]
	messages = messages[:limit]
	if appender, ok := destination.(WarningDiagnosticBatchAppender); ok {
		appender.AppendWarningBatch(total, codes, messages)
		return
	}
	appender, ok := destination.(WarningDiagnosticAppender)
	counter, hasCounter := destination.(WarningDiagnosticCountAppender)
	if !ok {
		// A count-only sink cannot represent individual records. Forward the
		// entire total instead of subtracting records that are not sent.
		if hasCounter {
			counter.AppendWarningCount(total)
		}
		return
	}
	if uint64(limit) > total {
		limit = int(total)
	}
	if hasCounter && total > uint64(limit) {
		counter.AppendWarningCount(total - uint64(limit))
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
	bytes    int
}

func (a *WarningAccumulator) Add(code uint16, message string) {
	if a == nil {
		return
	}
	a.AddCount()
	if len(a.Codes) >= warningDiagnosticRetentionLimit {
		return
	}
	remaining := WarningDiagnosticMaxBytes - a.bytes
	if remaining <= 0 {
		return
	}
	if remaining > WarningDiagnosticMaxMessageBytes {
		remaining = WarningDiagnosticMaxMessageBytes
	}
	message = BoundWarningMessage(message, remaining)
	a.Codes = append(a.Codes, code)
	a.Messages = append(a.Messages, message)
	a.bytes += len(message)
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
	return a != nil && len(a.Codes) < warningDiagnosticRetentionLimit &&
		a.bytes < WarningDiagnosticMaxBytes
}

func (a *WarningAccumulator) Flush(proc *Process) {
	if a == nil || a.Total == 0 {
		return
	}
	AppendWarningBatch(proc, a.Total, a.Codes, a.Messages)
	a.Total = 0
	a.Codes = a.Codes[:0]
	a.Messages = a.Messages[:0]
	a.bytes = 0
}

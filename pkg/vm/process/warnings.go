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
	"sync"
	"unicode/utf8"
)

const (
	// WarningDiagnosticDefaultRetentionLimit is the capacity used by newly
	// created local executions when no session-specific value is available.
	WarningDiagnosticDefaultRetentionLimit = 1024
	// WarningDiagnosticLegacyRetentionLimit is the capacity used when an older
	// remote ProcessInfo does not carry the new session fields.
	WarningDiagnosticLegacyRetentionLimit = 64

	// Keep the package-local name for tests and callers which historically used
	// the default execution capacity directly.
	warningDiagnosticRetentionLimit = WarningDiagnosticDefaultRetentionLimit
)

// WarningDiagnosticMaxMessageBytes bounds one retained warning message. The
// warning count remains exact even when the human-readable record is
// truncated or omitted.
const WarningDiagnosticMaxMessageBytes = 4 << 10

// WarningDiagnosticMaxBytes is retained for source compatibility with older
// callers which used the former byte-budget constant. It is also the default
// statement-wide payload budget for retained diagnostics; max_error_count and
// the per-message bound remain independent semantic limits.
const WarningDiagnosticMaxBytes = 256 << 10

// WarningDiagnosticRecordOverhead accounts for the Go slice/string headers
// retained alongside one diagnostic payload, including the code/message/level
// arrays and their bounded append slack in the largest sink. The value is
// deliberately conservative: the budget is a safety bound, not an allocator
// measurement.
const WarningDiagnosticRecordOverhead = 96

// WarningDiagnosticBudget is the statement/session ownership account for
// retained diagnostic payloads. It is separate from the mpool allocation
// account because warning messages are Go-heap strings and may outlive the
// execution generation until SHOW WARNINGS is replaced. One budget is shared
// by local accumulators, remote attempts, and the initiating session whenever
// they are on the same CN; transfers move the existing charge instead of
// charging a second copy.
type WarningDiagnosticBudget struct {
	mu    sync.Mutex
	limit uint64
	used  uint64
}

func NewWarningDiagnosticBudget(limit uint64) *WarningDiagnosticBudget {
	return &WarningDiagnosticBudget{limit: limit}
}

func (b *WarningDiagnosticBudget) Limit() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit
}

func (b *WarningDiagnosticBudget) Used() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func (b *WarningDiagnosticBudget) Reserve(size uint64) bool {
	if b == nil || size == 0 {
		return size == 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if size > b.limit-b.used {
		return false
	}
	b.used += size
	return true
}

func (b *WarningDiagnosticBudget) Release(size uint64) {
	if b == nil || size == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if size > b.used {
		panic("warning diagnostic budget release underflow")
	}
	b.used -= size
}

func WarningDiagnosticRecordBytes(message string) uint64 {
	return uint64(len(message)) + WarningDiagnosticRecordOverhead
}

// WarningDiagnosticBudgetProvider exposes the account captured by one
// statement/session generation without widening process.Session.
type WarningDiagnosticBudgetProvider interface {
	GetWarningDiagnosticBudget() *WarningDiagnosticBudget
}

// WarningDiagnosticBatchTransfer is implemented by sinks which can accept
// already-owned warning strings. The return value is true when the source
// charge was consumed as an ownership transfer (including releasing records
// rejected by the destination); false means the caller still owns the source
// charge and must release it after the destination copied or rejected it.
type WarningDiagnosticBatchTransfer interface {
	AppendWarningBatchOwned(
		total uint64,
		codes []uint16,
		messages []string,
		source *WarningDiagnosticBudget,
		chargedBytes uint64,
	) bool
}

func warningDiagnosticBudgetLimit(proc *Process) uint64 {
	limit := uint64(WarningDiagnosticMaxBytes)
	if proc != nil && proc.Base != nil && proc.Base.Lim.Size > 0 {
		if statementLimit := uint64(proc.Base.Lim.Size); statementLimit < limit {
			limit = statementLimit
		}
	}
	return limit
}

// WarningDiagnosticBudgetForProcess resolves the account captured by the
// current sink first, then falls back to the process generation. A process
// limitation smaller than the compatibility budget narrows the fallback.
func WarningDiagnosticBudgetForProcess(proc *Process) *WarningDiagnosticBudget {
	if proc != nil {
		if sink := proc.GetWarningSink(); sink != nil {
			if provider, ok := sink.(WarningDiagnosticBudgetProvider); ok {
				if budget := provider.GetWarningDiagnosticBudget(); budget != nil {
					return budget
				}
			}
		}
		if proc.Base != nil {
			proc.Base.warningDiagnosticBudgetMu.Lock()
			defer proc.Base.warningDiagnosticBudgetMu.Unlock()
			if proc.Base.warningDiagnosticBudget == nil {
				proc.Base.warningDiagnosticBudget = NewWarningDiagnosticBudget(
					warningDiagnosticBudgetLimit(proc))
			}
			return proc.Base.warningDiagnosticBudget
		}
	}
	return NewWarningDiagnosticBudget(WarningDiagnosticMaxBytes)
}

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

// WarningDiagnosticRetentionLimitProvider exposes the session-scoped
// diagnostic capacity without making it part of process.Session. It is an
// optional capability so internal sessions and old remote implementations can
// continue to use the legacy fallback.
type WarningDiagnosticRetentionLimitProvider interface {
	GetWarningRetentionLimit() int
}

func clampWarningRetentionLimit(limit int) int {
	if limit < 0 {
		return 0
	}
	if limit > int(^uint16(0)) {
		return int(^uint16(0))
	}
	return limit
}

// WarningDiagnosticRetentionLimitForProcess resolves the capacity captured by
// a process generation. A bound attempt sink wins so nested/internal work
// inherits its parent's generation; otherwise the statement snapshot and then
// the session capability provide the fallback.
func WarningDiagnosticRetentionLimitForProcess(proc *Process) int {
	if proc == nil {
		return WarningDiagnosticDefaultRetentionLimit
	}
	if proc.WarningSink != nil {
		if provider, ok := proc.WarningSink.(WarningDiagnosticRetentionLimitProvider); ok {
			return clampWarningRetentionLimit(provider.GetWarningRetentionLimit())
		}
	}
	if proc.Base != nil {
		if limit, ok := WarningRetentionLimitFromContext(proc.GetTopContext()); ok {
			return limit
		}
	}
	if proc.Base != nil && proc.Base.SessionInfo.MaxErrorCountSet {
		return clampWarningRetentionLimit(proc.Base.SessionInfo.MaxErrorCount)
	}
	if sink := proc.GetWarningSink(); sink != nil {
		if provider, ok := sink.(WarningDiagnosticRetentionLimitProvider); ok {
			return clampWarningRetentionLimit(provider.GetWarningRetentionLimit())
		}
	}
	return WarningDiagnosticDefaultRetentionLimit
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

// AppendWarningBatchToSinkOwned publishes a batch whose message strings are
// already charged to source. Same-budget sinks can transfer those strings
// without cloning; all other sinks copy and the caller's charge is released.
func AppendWarningBatchToSinkOwned(
	destination any,
	total uint64,
	codes []uint16,
	messages []string,
	source *WarningDiagnosticBudget,
	chargedBytes uint64,
) bool {
	if destination == nil {
		if source != nil {
			source.Release(chargedBytes)
		}
		return false
	}
	if transfer, ok := destination.(WarningDiagnosticBatchTransfer); ok {
		if transfer.AppendWarningBatchOwned(total, codes, messages, source, chargedBytes) {
			return true
		}
		if source != nil {
			source.Release(chargedBytes)
		}
		return false
	}
	AppendWarningBatchToSink(destination, total, codes, messages)
	if source != nil {
		source.Release(chargedBytes)
	}
	return false
}

// WarningAccumulator keeps the exact statement warning count while retaining
// only the bounded records required by SHOW WARNINGS.  It is deliberately
// allocation-free after the first retained records and is local to one
// operator/function invocation, so large INSERT IGNORE statements cannot grow
// execution memory with the number of skipped rows.
type WarningAccumulator struct {
	Total                  uint64
	Codes                  []uint16
	Messages               []string
	bytes                  int
	budgetBytes            uint64
	budget                 *WarningDiagnosticBudget
	retentionLimit         int
	retentionLimitSet      bool
	warningRetentionSealed bool
}

func (a *WarningAccumulator) ensureBudget() *WarningDiagnosticBudget {
	if a == nil {
		return nil
	}
	if a.budget == nil {
		a.budget = NewWarningDiagnosticBudget(WarningDiagnosticMaxBytes)
	}
	return a.budget
}

// SetWarningBudget binds the accumulator to the statement-wide account. It is
// normally called before the first Add; existing records are re-admitted in
// production order when a reused accumulator is rebound.
func (a *WarningAccumulator) SetWarningBudget(budget *WarningDiagnosticBudget) {
	if a == nil || a.budget == budget {
		return
	}
	if a.budget != nil {
		a.budget.Release(a.budgetBytes)
	}
	if budget == nil {
		budget = NewWarningDiagnosticBudget(WarningDiagnosticMaxBytes)
	}
	a.budget = budget
	a.budgetBytes = 0
	a.warningRetentionSealed = false
	keep := 0
	a.bytes = 0
	for keep < len(a.Messages) {
		charge := WarningDiagnosticRecordBytes(a.Messages[keep])
		if !budget.Reserve(charge) {
			a.warningRetentionSealed = true
			break
		}
		a.budgetBytes += charge
		a.bytes += len(a.Messages[keep])
		keep++
	}
	clear(a.Codes[keep:])
	clear(a.Messages[keep:])
	a.Codes = a.Codes[:keep]
	a.Messages = a.Messages[:keep]
}

// SetWarningRetentionForProcess binds both semantic capacity and the
// statement-wide resource account in one call for row-level producers.
func (a *WarningAccumulator) SetWarningRetentionForProcess(proc *Process) {
	if a == nil {
		return
	}
	a.SetWarningRetentionLimit(WarningDiagnosticRetentionLimitForProcess(proc))
	a.SetWarningBudget(WarningDiagnosticBudgetForProcess(proc))
}

// SetWarningRetentionLimit binds the accumulator to one execution's session
// capacity. It may be called before the first warning is added; reducing the
// capacity also releases records already retained by a reused accumulator.
func (a *WarningAccumulator) SetWarningRetentionLimit(limit int) {
	if a == nil {
		return
	}
	a.retentionLimit = clampWarningRetentionLimit(limit)
	a.retentionLimitSet = true
	if len(a.Codes) > a.retentionLimit {
		if budget := a.budget; budget != nil {
			for _, message := range a.Messages[a.retentionLimit:] {
				budget.Release(WarningDiagnosticRecordBytes(message))
			}
		}
		clear(a.Codes[a.retentionLimit:])
		clear(a.Messages[a.retentionLimit:])
		a.Codes = a.Codes[:a.retentionLimit]
		a.Messages = a.Messages[:a.retentionLimit]
		a.bytes = 0
		a.budgetBytes = 0
		for _, message := range a.Messages {
			a.bytes += len(message)
			a.budgetBytes += WarningDiagnosticRecordBytes(message)
		}
	}
	if a.retentionLimit == 0 {
		if budget := a.budget; budget != nil {
			budget.Release(a.budgetBytes)
		}
		a.Codes = nil
		a.Messages = nil
		a.bytes = 0
		a.budgetBytes = 0
		return
	}
	if cap(a.Codes) > a.retentionLimit*2 || cap(a.Messages) > a.retentionLimit*2 {
		// Keep the compacted backing arrays within the advertised capacity bound;
		// append(nil, ...) is allowed to round a small allocation upward.
		codes := make([]uint16, len(a.Codes))
		messages := make([]string, len(a.Messages))
		copy(codes, a.Codes)
		copy(messages, a.Messages)
		a.Codes = codes
		a.Messages = messages
	}
}

func (a *WarningAccumulator) warningRetentionLimit() int {
	if a == nil || !a.retentionLimitSet {
		return WarningDiagnosticDefaultRetentionLimit
	}
	return a.retentionLimit
}

func (a *WarningAccumulator) Add(code uint16, message string) {
	if a == nil {
		return
	}
	a.AddCount()
	if len(a.Codes) >= a.warningRetentionLimit() {
		return
	}
	if a.warningRetentionSealed {
		return
	}
	if budget := a.ensureBudget(); budget != nil {
		candidateBytes := len(message)
		if candidateBytes > WarningDiagnosticMaxMessageBytes {
			candidateBytes = WarningDiagnosticMaxMessageBytes
		}
		available := budget.Limit() - budget.Used()
		if uint64(candidateBytes)+WarningDiagnosticRecordOverhead > available {
			a.warningRetentionSealed = true
			return
		}
	}
	message = BoundWarningMessage(message, WarningDiagnosticMaxMessageBytes)
	charge := WarningDiagnosticRecordBytes(message)
	if !a.ensureBudget().Reserve(charge) {
		a.warningRetentionSealed = true
		return
	}
	a.Codes = append(a.Codes, code)
	a.Messages = append(a.Messages, message)
	a.bytes += len(message)
	a.budgetBytes += charge
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
	if a == nil || len(a.Codes) >= a.warningRetentionLimit() || a.warningRetentionSealed {
		return false
	}
	if a.budget == nil {
		return true
	}
	return a.budget.Used()+WarningDiagnosticRecordOverhead < a.budget.Limit()
}

func (a *WarningAccumulator) Flush(proc *Process) {
	if a == nil {
		return
	}
	if a.Total != 0 {
		var destination any
		if proc != nil {
			destination = proc.GetWarningSink()
		}
		AppendWarningBatchToSinkOwned(
			destination, a.Total, a.Codes, a.Messages,
			a.budget, a.budgetBytes)
	} else if a.budget != nil {
		a.budget.Release(a.budgetBytes)
	}
	a.Total = 0
	clear(a.Codes)
	clear(a.Messages)
	a.Codes = a.Codes[:0]
	a.Messages = a.Messages[:0]
	a.bytes = 0
	a.budgetBytes = 0
	a.warningRetentionSealed = false
}

// Reset releases retained payload ownership without publishing it. It is the
// cleanup path for a producer which aborts before Flush.
func (a *WarningAccumulator) Reset() {
	if a == nil {
		return
	}
	if a.budget != nil {
		a.budget.Release(a.budgetBytes)
	}
	a.Total = 0
	clear(a.Codes)
	clear(a.Messages)
	a.Codes = a.Codes[:0]
	a.Messages = a.Messages[:0]
	a.bytes = 0
	a.budgetBytes = 0
	a.warningRetentionSealed = false
}

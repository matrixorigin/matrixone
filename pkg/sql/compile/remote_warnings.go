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

package compile

import (
	"encoding/json"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// remoteWarningDiagnostic is carried in the existing terminal JSON envelope.
// Keeping it out of the protobuf message preserves compatibility with older
// CNs, which simply ignore unknown JSON fields.
type remoteWarningDiagnostic struct {
	Code    uint16 `json:"code"`
	Message string `json:"message"`
}

type warningDiagnosticSink = process.WarningDiagnosticAppender

// warningDiagnosticBatchSink carries the total number of diagnostics separately
// from the bounded records retained for SHOW WARNINGS. Remote fragments may
// produce one warning per input row, but only the engine's diagnostic capacity
// needs to cross the wire.
type warningDiagnosticBatchSink = process.WarningDiagnosticBatchAppender

type warningDiagnosticCountSink = process.WarningDiagnosticCountAppender

type groupConcatCutMarker interface {
	markGroupConcatCut(string)
	markGroupConcatReportingIncomplete()
}

// appendWarningBatchToSink preserves the bounded diagnostic batch when the
// sink supports it and falls back to the legacy one-record interface for
// older sessions. The sink is captured by the remote sender for one execution
// attempt, so a closed collector rejects late callbacks from a failed retry.
func appendWarningBatchToSink(destination any, total uint64, codes []uint16, messages []string) {
	process.AppendWarningBatchToSink(destination, total, codes, messages)
}

const remoteWarningRetentionLimit = process.WarningDiagnosticLegacyRetentionLimit

// terminalWarningDiagnosticsField is appended only when at least one
// diagnostic fits in the terminal envelope byte budget. Keeping the warning
// records out of the initial marshal avoids constructing a full-size JSON
// frame before the MORPC limit has been applied.
const terminalWarningDiagnosticsField = `"warning_diagnostics":[`

// marshalRemoteTerminalEnvelope preserves the exact warning count while
// retaining the longest prefix of diagnostics that fits in maxBytes. The
// terminal envelope has no independent fragmentation channel, so the byte
// budget is applied before the JSON is attached to a MORPC message.
func marshalRemoteTerminalEnvelope(envelope remoteTerminalEnvelope, maxBytes int) ([]byte, error) {
	warnings := envelope.WarningDiagnostics
	envelope.WarningDiagnostics = nil
	base, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(warnings) == 0 || maxBytes <= len(base) || len(base) == 0 {
		return base, nil
	}

	// json.Marshal emits a compact object, so the final byte is the closing
	// brace. Add the diagnostics field immediately before it. Appending the
	// field keeps all existing envelope fields and their compatibility intact.
	result := append([]byte(nil), base[:len(base)-1]...)
	hasBaseFields := len(result) > 1 // the object is not just "{}"
	started := false
	used := len(result)
	for _, warning := range warnings {
		encoded, err := json.Marshal(warning)
		if err != nil {
			return nil, err
		}
		extra := len(encoded)
		if started {
			extra++ // comma between array elements
		} else {
			extra += len(terminalWarningDiagnosticsField) + 2 // field, []
			if hasBaseFields {
				extra++ // comma before the appended field
			}
		}
		if used+extra > maxBytes {
			break
		}
		if !started {
			if hasBaseFields {
				result = append(result, ',')
			}
			result = append(result, terminalWarningDiagnosticsField...)
			started = true
		} else {
			result = append(result, ',')
		}
		result = append(result, encoded...)
		used += extra
	}
	if started {
		result = append(result, "]}"...)
	} else {
		result = append(result, '}')
	}
	return result, nil
}

// remoteWarningCollector gives a remote pipeline the small process.Session
// surface it needs while collecting row-level warnings. It deliberately does
// not expose a frontend session or variable state to the remote CN.
type remoteWarningCollector struct {
	mu                             sync.Mutex
	warningCount                   uint64
	warnings                       []remoteWarningDiagnostic
	warningBytes                   int
	warningChargeBytes             uint64
	warningBudget                  *process.WarningDiagnosticBudget
	warningRetentionSealed         bool
	maxRetained                    int
	maxRetainedSet                 bool
	groupConcatCut                 bool
	groupConcatCutMessage          string
	groupConcatReportingIncomplete bool
	// Immutable intent inherited by internal SQL compiles in this attempt.
	requiresCutReporting bool
	closed               bool
}

func (*remoteWarningCollector) GetTempTable(string, string) (string, bool) { return "", false }
func (*remoteWarningCollector) AddTempTable(string, string, string)        {}
func (*remoteWarningCollector) RemoveTempTable(string, string)             {}
func (*remoteWarningCollector) RemoveTempTableByRealName(string)           {}
func (*remoteWarningCollector) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }

func (s *remoteWarningCollector) GetWarningRetentionLimit() int {
	if s == nil {
		return remoteWarningRetentionLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.warningRetentionLimitLocked()
}

func (s *remoteWarningCollector) GetWarningDiagnosticBudget() *process.WarningDiagnosticBudget {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.warningBudget == nil {
		s.warningBudget = process.NewWarningDiagnosticBudget(process.WarningDiagnosticMaxBytes)
	}
	return s.warningBudget
}

func (s *remoteWarningCollector) warningRetentionLimitLocked() int {
	if s.maxRetainedSet {
		if s.maxRetained < 0 {
			return 0
		}
		if s.maxRetained > int(^uint16(0)) {
			return int(^uint16(0))
		}
		return s.maxRetained
	}
	if s.maxRetained > 0 {
		if s.maxRetained > int(^uint16(0)) {
			return int(^uint16(0))
		}
		return s.maxRetained
	}
	return remoteWarningRetentionLimit
}

func (s *remoteWarningCollector) AppendWarningDiagnostic(code uint16, msg string) {
	if s == nil {
		return
	}
	s.AppendWarningBatch(1, []uint16{code}, []string{msg})
}

func (s *remoteWarningCollector) AppendWarningCount(total uint64) {
	if s == nil || total == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if ^uint64(0)-s.warningCount < total {
		s.warningCount = ^uint64(0)
	} else {
		s.warningCount += total
	}
}

func (s *remoteWarningCollector) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendWarningBatchLocked(total, codes, messages, nil, 0)
}

// AppendWarningBatchOwned accepts payload already charged to source. A local
// same-budget transfer moves the retained string ownership without cloning;
// a cross-CN/foreign-budget transfer falls back to the ordinary bounded copy.
func (s *remoteWarningCollector) AppendWarningBatchOwned(
	total uint64,
	codes []uint16,
	messages []string,
	source *process.WarningDiagnosticBudget,
	chargedBytes uint64,
) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if s.warningBudget == nil {
		s.warningBudget = process.NewWarningDiagnosticBudget(process.WarningDiagnosticMaxBytes)
	}
	sameBudget := source != nil && source == s.warningBudget
	if sameBudget {
		for _, message := range messages {
			if len(message) > process.WarningDiagnosticMaxMessageBytes {
				// The ownership contract covers already-bounded strings. A
				// defensive fallback keeps malformed callers from retaining an
				// unbounded payload or under-accounting the source charge.
				sameBudget = false
				break
			}
		}
	}
	appendSource := source
	if !sameBudget && source == s.warningBudget {
		appendSource = nil
	}
	s.appendWarningBatchLocked(total, codes, messages, appendSource, chargedBytes)
	return sameBudget
}

func (s *remoteWarningCollector) appendWarningBatchLocked(
	total uint64,
	codes []uint16,
	messages []string,
	source *process.WarningDiagnosticBudget,
	chargedBytes uint64,
) {
	if s.closed {
		return
	}
	if s.warningBudget == nil {
		s.warningBudget = process.NewWarningDiagnosticBudget(process.WarningDiagnosticMaxBytes)
	}
	if ^uint64(0)-s.warningCount < total {
		s.warningCount = ^uint64(0)
	} else {
		s.warningCount += total
	}
	codeLimit := len(codes)
	if uint64(codeLimit) > total {
		codeLimit = int(total)
	}
	for i := 0; i < codeLimit; i++ {
		if codes[i] != moerr.ER_CUT_VALUE_GROUP_CONCAT {
			continue
		}
		message := ""
		if i < len(messages) {
			message = messages[i]
		}
		s.markGroupConcatCutLocked(message)
		break
	}
	limit := s.warningRetentionLimitLocked()
	batchLimit := len(codes)
	if len(messages) < batchLimit {
		batchLimit = len(messages)
	}
	if uint64(batchLimit) > total {
		batchLimit = int(total)
	}
	sameBudget := source != nil && source == s.warningBudget
	for i := 0; i < len(messages); i++ {
		charge := process.WarningDiagnosticRecordBytes(messages[i])
		if i >= batchLimit || len(s.warnings) >= limit || s.warningRetentionSealed {
			if sameBudget {
				source.Release(charge)
			}
			continue
		}
		message := messages[i]
		if !sameBudget {
			candidateBytes := len(message)
			if candidateBytes > process.WarningDiagnosticMaxMessageBytes {
				candidateBytes = process.WarningDiagnosticMaxMessageBytes
			}
			available := s.warningBudget.Limit() - s.warningBudget.Used()
			if uint64(candidateBytes)+process.WarningDiagnosticRecordOverhead > available {
				s.warningRetentionSealed = true
				continue
			}
			message = process.BoundWarningMessage(message, process.WarningDiagnosticMaxMessageBytes)
			charge = process.WarningDiagnosticRecordBytes(message)
			if !s.warningBudget.Reserve(charge) {
				s.warningRetentionSealed = true
				continue
			}
		}
		s.warnings = append(s.warnings, remoteWarningDiagnostic{
			Code:    codes[i],
			Message: message,
		})
		s.warningBytes += len(message)
		s.warningChargeBytes += charge
	}
	if sameBudget {
		// The source charge is a per-message sum. The loop above accounts for
		// every source message; release any defensive remainder if a malformed
		// caller supplied a larger aggregate.
		accounted := uint64(0)
		for _, message := range messages {
			accounted += process.WarningDiagnosticRecordBytes(message)
		}
		if chargedBytes > accounted {
			source.Release(chargedBytes - accounted)
		}
	}
}

func (s *remoteWarningCollector) markGroupConcatCut(message string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.markGroupConcatCutLocked(message)
}

func (s *remoteWarningCollector) markGroupConcatCutLocked(message string) {
	s.groupConcatCut = true
	if s.groupConcatCutMessage == "" && message != "" {
		s.groupConcatCutMessage = process.BoundWarningMessage(message, process.WarningDiagnosticMaxMessageBytes)
	}
}

func (s *remoteWarningCollector) groupConcatCutDiagnostic() (bool, string) {
	if s == nil {
		return false, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groupConcatCut, s.groupConcatCutMessage
}

func (s *remoteWarningCollector) markGroupConcatReportingIncomplete() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.groupConcatReportingIncomplete = true
	}
}

func (s *remoteWarningCollector) incompleteGroupConcatReporting() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groupConcatReportingIncomplete
}

func requiresGroupConcatCutReporting(sink any) bool {
	collector, ok := sink.(*remoteWarningCollector)
	return ok && collector != nil && collector.requiresCutReporting
}

func (s *remoteWarningCollector) SnapshotWarnings() (uint64, []remoteWarningDiagnostic) {
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.warningCount, append([]remoteWarningDiagnostic(nil), s.warnings...)
}

// closeWarnings atomically seals an attempt against late local/RPC writers.
// Failed attempts discard without copying; successful attempts transfer the
// bounded records exactly once. A collector is never reopened for a retry.
func (s *remoteWarningCollector) closeWarnings(success bool) (
	uint64,
	[]remoteWarningDiagnostic,
	bool,
	string,
	bool,
	*process.WarningDiagnosticBudget,
	uint64,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, nil, false, "", false, nil, 0
	}
	s.closed = true
	total, warnings := s.warningCount, s.warnings
	cut, message := s.groupConcatCut, s.groupConcatCutMessage
	incomplete := s.groupConcatReportingIncomplete
	budget, charged := s.warningBudget, s.warningChargeBytes
	s.warningCount, s.warnings, s.warningBytes, s.warningChargeBytes = 0, nil, 0, 0
	s.groupConcatCut, s.groupConcatCutMessage = false, ""
	s.groupConcatReportingIncomplete = false
	if !success {
		budget.Release(charged)
		return 0, nil, false, "", false, nil, 0
	}
	return total, warnings, cut, message, incomplete, budget, charged
}

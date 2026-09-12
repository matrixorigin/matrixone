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
	mu             sync.Mutex
	warningCount   uint64
	warnings       []remoteWarningDiagnostic
	warningBytes   int
	maxRetained    int
	maxRetainedSet bool
	closed         bool
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
	if s.closed {
		s.mu.Unlock()
		return
	}
	if ^uint64(0)-s.warningCount < total {
		s.warningCount = ^uint64(0)
	} else {
		s.warningCount += total
	}
	limit := s.warningRetentionLimitLocked()
	batchLimit := len(codes)
	if len(messages) < batchLimit {
		batchLimit = len(messages)
	}
	if uint64(batchLimit) > total {
		batchLimit = int(total)
	}
	for i := 0; i < batchLimit && len(s.warnings) < limit; i++ {
		message := process.BoundWarningMessage(messages[i], process.WarningDiagnosticMaxMessageBytes)
		s.warnings = append(s.warnings, remoteWarningDiagnostic{
			Code:    codes[i],
			Message: message,
		})
		s.warningBytes += len(message)
	}
	s.mu.Unlock()
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
func (s *remoteWarningCollector) closeWarnings(success bool) (uint64, []remoteWarningDiagnostic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, nil
	}
	s.closed = true
	total, warnings := s.warningCount, s.warnings
	s.warningCount, s.warnings, s.warningBytes = 0, nil, 0
	if !success {
		return 0, nil
	}
	return total, warnings
}

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
}

// appendWarningBatchToSink preserves the bounded diagnostic batch when the
// sink supports it and falls back to the legacy one-record interface for
// older sessions. The sink is captured by the remote sender for one execution
// attempt, so a closed collector rejects late callbacks from a failed retry.
func appendWarningBatchToSink(destination any, total uint64, codes []uint16, messages []string) {
	process.AppendWarningBatchToSink(destination, total, codes, messages)
}

const remoteWarningRetentionLimit = 64

// remoteWarningCollector gives a remote pipeline the small process.Session
// surface it needs while collecting row-level warnings. It deliberately does
// not expose a frontend session or variable state to the remote CN.
type remoteWarningCollector struct {
	mu                    sync.Mutex
	warningCount          uint64
	warnings              []remoteWarningDiagnostic
	warningBytes          int
	maxRetained           int
	groupConcatCut        bool
	groupConcatCutMessage string
	closed                bool
}

func (*remoteWarningCollector) GetTempTable(string, string) (string, bool) { return "", false }
func (*remoteWarningCollector) AddTempTable(string, string, string)        {}
func (*remoteWarningCollector) RemoveTempTable(string, string)             {}
func (*remoteWarningCollector) RemoveTempTableByRealName(string)           {}
func (*remoteWarningCollector) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }

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
			message = process.BoundWarningMessage(
				messages[i], process.WarningDiagnosticMaxMessageBytes)
		}
		s.markGroupConcatCutLocked(message)
		break
	}
	limit := s.maxRetained
	if limit <= 0 {
		limit = remoteWarningRetentionLimit
	}
	batchLimit := len(codes)
	if len(messages) < batchLimit {
		batchLimit = len(messages)
	}
	if uint64(batchLimit) > total {
		batchLimit = int(total)
	}
	for i := 0; i < batchLimit && len(s.warnings) < limit; i++ {
		remaining := process.WarningDiagnosticMaxBytes - s.warningBytes
		if remaining <= 0 {
			break
		}
		if remaining > process.WarningDiagnosticMaxMessageBytes {
			remaining = process.WarningDiagnosticMaxMessageBytes
		}
		message := process.BoundWarningMessage(messages[i], remaining)
		s.warnings = append(s.warnings, remoteWarningDiagnostic{
			Code:    codes[i],
			Message: message,
		})
		s.warningBytes += len(message)
	}
	s.mu.Unlock()
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
		s.groupConcatCutMessage = message
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
	s.groupConcatCut, s.groupConcatCutMessage = false, ""
	if !success {
		return 0, nil
	}
	return total, warnings
}

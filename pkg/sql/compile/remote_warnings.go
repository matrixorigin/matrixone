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

import "sync"

// remoteWarningDiagnostic is carried in the existing terminal JSON envelope.
// Keeping it out of the protobuf message preserves compatibility with older
// CNs, which simply ignore unknown JSON fields.
type remoteWarningDiagnostic struct {
	Code    uint16 `json:"code"`
	Message string `json:"message"`
}

type warningDiagnosticSink interface {
	AppendWarningDiagnostic(code uint16, msg string)
}

// warningDiagnosticBatchSink carries the total number of diagnostics separately
// from the bounded records retained for SHOW WARNINGS. Remote fragments may
// produce one warning per input row, but only the engine's diagnostic capacity
// needs to cross the wire.
type warningDiagnosticBatchSink interface {
	AppendWarningBatch(total uint64, codes []uint16, messages []string)
}

const remoteWarningRetentionLimit = 64

// remoteWarningCollector gives a remote pipeline the small process.Session
// surface it needs while collecting row-level warnings. It deliberately does
// not expose a frontend session or variable state to the remote CN.
type remoteWarningCollector struct {
	mu           sync.Mutex
	warningCount uint64
	warnings     []remoteWarningDiagnostic
	maxRetained  int
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

func (s *remoteWarningCollector) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.warningCount += total
	limit := s.maxRetained
	if limit <= 0 {
		limit = remoteWarningRetentionLimit
	}
	for i := 0; i < len(codes) && i < len(messages) && len(s.warnings) < limit; i++ {
		s.warnings = append(s.warnings, remoteWarningDiagnostic{
			Code:    codes[i],
			Message: messages[i],
		})
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

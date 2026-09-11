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
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

type warningTestSession struct {
	total    uint64
	codes    []uint16
	messages []string
}

type legacyWarningSink struct {
	total    uint64
	codes    []uint16
	messages []string
}

type countOnlyWarningSink struct {
	total uint64
}

func (s *countOnlyWarningSink) AppendWarningCount(total uint64) {
	if ^uint64(0)-s.total < total {
		s.total = ^uint64(0)
	} else {
		s.total += total
	}
}

func (s *legacyWarningSink) AppendWarningCount(total uint64) {
	if ^uint64(0)-s.total < total {
		s.total = ^uint64(0)
	} else {
		s.total += total
	}
}

func (s *legacyWarningSink) AppendWarningDiagnostic(code uint16, message string) {
	s.total++
	s.codes = append(s.codes, code)
	s.messages = append(s.messages, message)
}

func (*warningTestSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*warningTestSession) AddTempTable(string, string, string)        {}
func (*warningTestSession) RemoveTempTable(string, string)             {}
func (*warningTestSession) RemoveTempTableByRealName(string)           {}
func (*warningTestSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *warningTestSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.total += total
	s.codes = append(s.codes, codes...)
	s.messages = append(s.messages, messages...)
}

func TestWarningAccumulatorRetainsBoundedDiagnosticsAndExactCount(t *testing.T) {
	var accumulator WarningAccumulator
	for i := 0; i < warningDiagnosticRetentionLimit+17; i++ {
		accumulator.Add(1062, "duplicate")
	}

	require.Equal(t, uint64(warningDiagnosticRetentionLimit+17), accumulator.Total)
	require.Len(t, accumulator.Codes, warningDiagnosticRetentionLimit)
	require.Len(t, accumulator.Messages, warningDiagnosticRetentionLimit)
	require.False(t, accumulator.NeedsDiagnostic())

	accumulator.AddCount()
	require.Equal(t, uint64(warningDiagnosticRetentionLimit+18), accumulator.Total)
	require.Len(t, accumulator.Codes, warningDiagnosticRetentionLimit)
}

func TestWarningAccumulatorUsesConfiguredRetentionPrefix(t *testing.T) {
	for _, limit := range []int{0, 1, 10, 64, 128, 1024, 65535} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			var accumulator WarningAccumulator
			accumulator.SetWarningRetentionLimit(limit)
			for i := 0; i < 20; i++ {
				accumulator.Add(uint16(1000+i), fmt.Sprintf("warning-%d", i))
			}
			want := limit
			if want > 20 {
				want = 20
			}
			require.Len(t, accumulator.Codes, want)
			for i := 0; i < want; i++ {
				require.Equal(t, uint16(1000+i), accumulator.Codes[i])
			}
			require.Equal(t, uint64(20), accumulator.Total)
		})
	}
}

func TestAppendWarningBatchUsesCurrentAttemptSink(t *testing.T) {
	session := new(warningTestSession)
	proc := &Process{Base: &BaseProcess{}, Session: session}
	attempt := new(warningTestSession)
	proc.WarningSink = attempt
	child := proc.NewNoContextChildProc(0)

	AppendWarningBatch(child, 2, []uint16{1062, 1062}, []string{"duplicate", "duplicate"})
	require.Zero(t, session.total, "an active execution sink must own diagnostics")
	require.Equal(t, uint64(2), attempt.total)
	require.Equal(t, []uint16{1062, 1062}, attempt.codes)

	// Outside Compile.Run (or for an internal/background process without an
	// attempt sink), the session remains the compatibility fallback.
	proc.WarningSink = nil
	AppendWarningBatch(proc, 1, []uint16{1292}, []string{"truncated"})
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{1292}, session.codes)
}

func TestWarningDiagnosticRetentionLimitForProcessPrefersSnapshot(t *testing.T) {
	proc := &Process{
		Base: &BaseProcess{SessionInfo: SessionInfo{
			MaxErrorCount:    0,
			MaxErrorCountSet: true,
		}},
		Session: &retentionWarningSession{limit: 128},
	}
	proc.WarningSink = &retentionWarningSession{limit: 256}
	require.Equal(t, 256, WarningDiagnosticRetentionLimitForProcess(proc))

	proc.WarningSink = nil
	proc.Base.SessionInfo.MaxErrorCountSet = false
	require.Equal(t, 128, WarningDiagnosticRetentionLimitForProcess(proc))
}

type retentionWarningSession struct{ limit int }

func (s *retentionWarningSession) GetWarningRetentionLimit() int            { return s.limit }
func (*retentionWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*retentionWarningSession) AddTempTable(string, string, string)        {}
func (*retentionWarningSession) RemoveTempTable(string, string)             {}
func (*retentionWarningSession) RemoveTempTableByRealName(string)           {}
func (*retentionWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }

func TestAppendWarningBatchLegacySinkPreservesUnretainedCount(t *testing.T) {
	sink := new(legacyWarningSink)
	codes := make([]uint16, warningDiagnosticRetentionLimit)
	messages := make([]string, warningDiagnosticRetentionLimit)
	for i := range codes {
		codes[i] = 1062
		messages[i] = "duplicate"
	}
	proc := &Process{Base: &BaseProcess{}, WarningSink: sink}
	AppendWarningBatch(proc, warningDiagnosticRetentionLimit+36, codes, messages)
	require.Equal(t, uint64(warningDiagnosticRetentionLimit+36), sink.total)
	require.Len(t, sink.codes, warningDiagnosticRetentionLimit)

	AppendWarningBatch(proc, 1, nil, nil)
	require.Equal(t, uint64(warningDiagnosticRetentionLimit+37), sink.total)
}

func TestAppendWarningBatchCountOnlySinkReceivesEntireTotal(t *testing.T) {
	sink := new(countOnlyWarningSink)
	proc := &Process{Base: &BaseProcess{}, WarningSink: sink}
	AppendWarningBatch(proc, warningDiagnosticRetentionLimit+3,
		[]uint16{1062, 1062}, []string{"duplicate", "duplicate"})
	require.Equal(t, uint64(warningDiagnosticRetentionLimit+3), sink.total)
}

func TestAppendWarningBatchClampsRecordsToTotal(t *testing.T) {
	sink := new(legacyWarningSink)
	proc := &Process{Base: &BaseProcess{}, WarningSink: sink}
	AppendWarningBatch(proc, 1,
		[]uint16{1062, 1062}, []string{"first", "second"})
	require.Equal(t, uint64(1), sink.total)
	require.Len(t, sink.codes, 1)
}

func TestAppendWarningBatchBatchSinkClampsRecordsToTotal(t *testing.T) {
	sink := new(warningTestSession)
	proc := &Process{Base: &BaseProcess{}, WarningSink: sink}
	AppendWarningBatch(proc, 1,
		[]uint16{1062, 1062}, []string{"first", "second"})
	require.Equal(t, uint64(1), sink.total)
	require.Len(t, sink.codes, 1)
}

func TestBoundWarningMessageIsOwnedAndUTF8Safe(t *testing.T) {
	message := strings.Repeat("界", WarningDiagnosticMaxMessageBytes)
	bounded := BoundWarningMessage(message, 32)
	require.LessOrEqual(t, len(bounded), 32)
	require.True(t, utf8.ValidString(bounded))
	require.Contains(t, bounded, "truncated")
}

func TestWarningAccumulatorBoundsRetainedMessageWithoutLosingCount(t *testing.T) {
	var accumulator WarningAccumulator
	for i := 0; i < warningDiagnosticRetentionLimit+10; i++ {
		accumulator.Add(1062, strings.Repeat("x", WarningDiagnosticMaxMessageBytes*2))
	}
	require.Equal(t, uint64(warningDiagnosticRetentionLimit+10), accumulator.Total)
	require.Len(t, accumulator.Codes, warningDiagnosticRetentionLimit)
	require.Greater(t, accumulator.bytes, WarningDiagnosticMaxBytes)
	for _, message := range accumulator.Messages {
		require.LessOrEqual(t, len(message), WarningDiagnosticMaxMessageBytes)
	}
}

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
	"testing"

	"github.com/stretchr/testify/require"
)

type warningTestSession struct {
	total    uint64
	codes    []uint16
	messages []string
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

func TestWarningAttemptDiscardsFailedAttemptAndCommitsSurvivor(t *testing.T) {
	session := new(warningTestSession)
	proc := &Process{Base: &BaseProcess{}, Session: session}

	proc.BeginWarningAttempt(0)
	AppendWarningBatch(proc, 2, []uint16{1062}, []string{"duplicate"})
	proc.AbortWarningAttempt(0)
	require.Zero(t, session.total)
	require.Empty(t, session.codes)

	proc.BeginWarningAttempt(1)
	AppendWarningBatch(proc, 1, []uint16{3819}, []string{"check failed"})
	proc.CommitWarningAttempt(1)
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{3819}, session.codes)
	require.Equal(t, []string{"check failed"}, session.messages)
}

func TestWarningAttemptSharesStateWithChildProcess(t *testing.T) {
	session := new(warningTestSession)
	proc := &Process{Base: &BaseProcess{}, Session: session}
	child := proc.NewNoContextChildProc(0)

	proc.BeginWarningAttempt(0)
	AppendWarningBatch(child, 1, []uint16{1062}, []string{"duplicate"})
	proc.CommitWarningAttempt(0)
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{1062}, session.codes)
}

func TestUnboundWarningBatchRemainsDirectAfterAttempt(t *testing.T) {
	session := new(warningTestSession)
	proc := &Process{Base: &BaseProcess{}, Session: session}
	proc.BeginWarningAttempt(0)
	proc.AbortWarningAttempt(0)

	// Unbound local diagnostics do not carry a generation and remain compatible
	// with callers that use WarningAccumulator outside Compile.Run.
	AppendWarningBatch(proc, 1, []uint16{1292}, []string{"truncated"})
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{1292}, session.codes)
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestStrictWriteRejectsIncompleteOldWorkerReport(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	c := &Compile{proc: proc}
	attempt := newWarningAttempt(proc, true)
	defer attempt.finish(false, nil)
	old := remoteTerminalEnvelope{WarningCount: 65}
	for i := 0; i < remoteWarningRetentionLimit; i++ {
		old.WarningDiagnostics = append(old.WarningDiagnostics, remoteWarningDiagnostic{Code: 1, Message: "earlier warning"})
	}
	payload, err := json.Marshal(old)
	require.NoError(t, err)
	sender := &messageSenderOnClient{warningSink: attempt.collector}
	require.NoError(t, sender.dealRemoteTerminal(payload))
	cut, _ := attempt.groupConcatCutDiagnostic()
	require.False(t, cut, "old worker omitted the cut beyond the retained records")
	err = c.strictWriteGroupConcatCutError(attempt, true)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	require.NoError(t, c.strictWriteGroupConcatCutError(attempt, false), "SELECT/IGNORE policy is unchanged")
	attempt.finish(false, nil)
	retry := newWarningAttempt(proc, true)
	defer retry.finish(false, nil)
	require.NoError(t, c.strictWriteGroupConcatCutError(retry, true), "incompleteness cannot leak into a fresh attempt")
}
func TestIncompleteReportingSurvivesForwardingAndEmptyTerminal(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	parent := newWarningAttempt(proc, true)
	defer parent.finish(false, nil)
	childProc := proc.NewNoContextChildProc(0)
	child := newWarningAttempt(childProc, false)
	require.True(t, child.collector.requiresCutReporting)
	sender := &messageSenderOnClient{warningSink: child.collector}
	require.NoError(t, sender.dealRemoteTerminal(nil))
	child.finish(true, parent.collector)
	require.True(t, parent.collector.incompleteGroupConcatReporting())
	receiver := &messageReceiverOnServer{groupConcatReportingIncomplete: parent.collector.incompleteGroupConcatReporting()}
	wire := &pipeline.Message{}
	require.NoError(t, receiver.setTerminalAnalysis(wire))
	var envelope remoteTerminalEnvelope
	require.NoError(t, json.Unmarshal(wire.GetAnalyse(), &envelope))
	require.False(t, envelope.GroupConcatCutReported)
	parent.finish(false, nil)
	parent.collector.markGroupConcatReportingIncomplete()
	require.False(t, parent.collector.incompleteGroupConcatReporting(), "late callbacks cannot reopen a sealed collector")
	receiver = &messageReceiverOnServer{}
	require.NoError(t, receiver.setTerminalAnalysis(wire))
	require.NoError(t, json.Unmarshal(wire.GetAnalyse(), &envelope))
	require.True(t, envelope.GroupConcatCutReported, "new workers explicitly report completeness even without warnings")
}

func TestMissingOrMalformedTruncationReportFailsClosed(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		attempt := newWarningAttempt(proc, true)
		sender := &messageSenderOnClient{warningSink: attempt.collector}
		sender.markMissingGroupConcatTerminal()
		require.False(t, attempt.collector.incompleteGroupConcatReporting(), "a stream that never attempted work needs no report")
		sender.markReportingRequestStarted()
		if malformed {
			require.Error(t, sender.dealRemoteTerminal([]byte("{")))
		}
		sender.markMissingGroupConcatTerminal()
		c := &Compile{proc: proc}
		require.Error(t, c.strictWriteGroupConcatCutError(attempt, true))
		attempt.finish(false, nil)
		proc.Free()
	}
}

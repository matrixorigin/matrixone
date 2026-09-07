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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSequenceStatementStateRestoresRetryBaseline(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.InitSeq()
	info := proc.GetSessionInfo()
	info.SeqCurValues[7] = "prior-curval"
	info.SeqAddValues[8] = "prior-pending"
	info.SeqDeleteKeys = []uint64{9}
	info.SeqLastValue[0] = "prior-lastval"

	c := &Compile{proc: proc, sequenceState: captureSequenceStatementState(proc)}
	// Simulate an attempt that allocated values and changed unrelated sequence
	// bookkeeping before a later operator requested transaction retry.
	info.SeqCurValues[7] = "mutated-curval"
	info.SeqCurValues[10] = "failed-curval"
	info.SeqAddValues[8] = "failed-pending"
	info.SeqAddValues[11] = "failed-new"
	info.SeqDeleteKeys = []uint64{12}
	info.SeqLastValue = nil

	c.restoreSequenceStatementState()
	require.Equal(t, map[uint64]string{7: "prior-curval"}, info.SeqCurValues)
	require.Equal(t, map[uint64]string{8: "prior-pending"}, info.SeqAddValues)
	require.Equal(t, []uint64{9}, info.SeqDeleteKeys)
	require.Equal(t, []string{"prior-lastval"}, info.SeqLastValue)

	// Restored containers are independent from the captured baseline and from
	// one another, so the next generation cannot mutate the snapshot itself.
	info.SeqCurValues[7] = "retry-curval"
	info.SeqAddValues[8] = "retry-pending"
	c.restoreSequenceStatementState()
	require.Equal(t, "prior-curval", info.SeqCurValues[7])
	require.Equal(t, "prior-pending", info.SeqAddValues[8])
}

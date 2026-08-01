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

package hashjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type terminalBudgetAdmissionChild struct {
	*colexec.MockOperator
	err error
}

func (child *terminalBudgetAdmissionChild) Call(*process.Process) (vm.CallResult, error) {
	return vm.NewCallResult(), child.err
}

func TestHashJoinCallConvertsTerminalBudgetAdmission(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	admission := &process.HashBuildBudgetError{
		Kind:      process.HashBuildBudgetErrorAdmission,
		Resource:  process.HashBuildBudgetResourceMemory,
		Requested: 2,
		Used:      1,
		Cap:       1,
	}
	child := &terminalBudgetAdmissionChild{
		MockOperator: colexec.NewMockOperator(),
		err:          admission,
	}
	arg := &HashJoin{
		IsShuffle:  true,
		ShuffleIdx: 0,
		JoinMapTag: 91001,
	}
	var callErr error
	t.Cleanup(func() {
		arg.Free(proc, true, callErr)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	arg.OpAnalyzer = process.NewAnalyzer(0, false, false, "terminal-budget-test")
	arg.AppendChild(child)
	message.SendJoinMapResult(
		message.NewJoinMapResult(nil),
		arg.JoinMapTag,
		arg.IsShuffle,
		arg.ShuffleIdx,
		proc.GetMessageBoard(),
	)

	_, callErr = arg.Call(proc)
	require.Error(t, callErr)
	require.True(t, moerr.IsMoErrCode(callErr, moerr.ErrOOM), callErr)
	require.NotErrorIs(t, callErr, process.ErrHashBuildBudgetAdmission)
	require.NotContains(t, callErr.Error(), "convert go error")
	require.NotContains(t, callErr.Error(), process.ErrHashBuildBudgetAdmission.Error())
	require.Contains(t, callErr.Error(), "hash build memory budget exceeded")
}

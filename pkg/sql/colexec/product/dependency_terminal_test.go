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

package product

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestProductProbeBuildOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		empty bool
	}{
		{name: "empty_success"},
		{name: "empty_probe_without_build", empty: true},
		{name: "build_failure", err: moerr.NewInternalErrorNoCtx("build failed")},
		{name: "build_canceled", err: context.Canceled},
		{name: "build_deadline", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			proc.SetMessageBoard(message.NewMessageBoard())
			arg := &Product{JoinMapTag: 1}
			registry, err := mpool.NewAllocationAccountRegistry(1, 4)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				proc.GetMessageBoard().Reset()
				require.NoError(t, arg.ClearAllocationAccount(account))
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, arg.SetAllocationAccount(account))
			// Empty batches are not EOF. Repeat with the same operator/board
			// generation boundary so a failed dependency cannot poison reuse.
			for _, buildErr := range []error{tc.err, nil} {
				arg.Children = nil
				batches := []*batch.Batch{batch.EmptyBatch}
				if !tc.empty {
					batches = append(batches, colexec.MakeMockBatchs(proc.Mp()))
				}
				probe := colexec.NewMockOperator().WithBatchs(batches)
				t.Cleanup(func() { probe.Free(proc, false, nil) })
				arg.AppendChild(probe)
				require.NoError(t, arg.Prepare(proc))
				terminal := message.NewJoinMapResult(nil)
				if buildErr != nil {
					terminal = message.NewJoinMapBuildErrorResult(buildErr)
				}
				if !tc.empty {
					require.True(t, message.SendJoinMapResult(terminal, 1, false, 0, proc.GetMessageBoard()))
				}
				result, err := vm.Exec(arg, proc)
				if buildErr == nil {
					require.Nil(t, result.Batch)
					require.NoError(t, err)
					require.Equal(t, vm.ExecStop, result.Status)
				} else if buildErr == context.Canceled || buildErr == context.DeadlineExceeded {
					require.ErrorIs(t, err, buildErr)
				} else {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
					require.Contains(t, err.Error(), buildErr.Error())
				}
				arg.Reset(proc, err != nil, err)
				probe.Free(proc, false, nil)
				proc.GetMessageBoard().Reset()
				require.Zero(t, account.Snapshot().Used)
			}
		})
	}
}

func TestProductBuildReferencesOnProbeReuse(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)})
	t.Cleanup(func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		for _, child := range tc.arg.Children {
			child.Free(tc.proc, false, nil)
		}
		for _, child := range tc.barg.Children {
			child.Free(tc.proc, false, nil)
		}
		tc.resultBatch.Clean(tc.proc.Mp())
		tc.proc.Free()
		tc.cancel()
		require.Zero(t, tc.proc.Mp().CurrNB())
	})
	for _, empty := range []bool{true, false, true} {
		probe := colexec.NewMockOperator()
		if !empty {
			probe.WithBatchs([]*batch.Batch{colexec.MakeMockBatchs(tc.proc.Mp())})
		}
		tc.arg.Children = nil
		tc.arg.AppendChild(probe)
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		require.NoError(t, tc.arg.Prepare(tc.proc))
		require.NoError(t, tc.barg.Prepare(tc.proc))
		_, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		rows := 0
		for {
			result, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			if result.Batch != nil {
				rows += result.Batch.RowCount()
			}
			if result.Status == vm.ExecStop {
				break
			}
		}
		if empty {
			require.Zero(t, rows)
		} else {
			require.Equal(t, 4, rows)
		}
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		probe.Free(tc.proc, false, nil)
		for _, child := range tc.barg.Children {
			child.Free(tc.proc, false, nil)
		}
		tc.proc.GetMessageBoard().Reset()
		require.Zero(t, tc.arg.allocationAccount.Snapshot().Used)
	}
}

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashjoin

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func sameAreaOwnership(before, after []byte) bool {
	if (before == nil) != (after == nil) || cap(before) != cap(after) {
		return false
	}
	if cap(before) == 0 {
		return true
	}
	return unsafe.SliceData(before) == unsafe.SliceData(after)
}

// Exercise public Call -> emptyProbe -> UnionBatch -> Reset/Free with a real
// HashBuild budget controller. No synthetic admission error is injected.
func TestHashJoinAreaAdmissionTerminalCleanup(t *testing.T) {
	for _, denied := range []bool{false, true} {
		name := "success"
		if denied {
			name = "budget_denied_after_result_reuse"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			proc.SetMessageBoard(message.NewMessageBoard())
			budget := process.MustNewHashBuildBudget(32768, 32768)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 64)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, generation)
			require.NoError(t, err)
			arg := &HashJoin{JoinType: plan.Node_LEFT, NumCPU: 1,
				JoinMapTag: 92003,
				ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}},
				LeftTypes:  []types.Type{types.T_varchar.ToType()},
				EqConds:    [][]*plan.Expr{{newExpr(0, types.T_varchar.ToType())}, {newExpr(0, types.T_varchar.ToType())}}}
			require.NoError(t, arg.SetAllocationAccount(account))
			var inputs []*batch.Batch
			var orphan []byte
			argCleaned := false
			t.Cleanup(func() {
				if !argCleaned {
					arg.Reset(proc, true, nil)
					arg.Free(proc, true, nil)
				}
				for _, input := range inputs {
					input.Clean(proc.Mp())
				}
				if orphan != nil {
					proc.Mp().Free(orphan)
				}
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			makeInput := func(n int) *batch.Batch {
				b := batch.NewWithSize(1)
				b.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
				require.NoError(t, vector.AppendBytes(b.Vecs[0], bytes.Repeat([]byte("x"), n), false, proc.Mp()))
				b.SetRowCount(1)
				inputs = append(inputs, b)
				return b
			}
			nextSize := 4096
			if denied {
				nextSize = 65536
			}
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{makeInput(4096), makeInput(nextSize)})
			arg.AppendChild(child)
			require.NoError(t, child.Prepare(proc))
			require.NoError(t, arg.Prepare(proc))
			message.SendJoinMapResult(message.NewJoinMapResult(nil), arg.JoinMapTag, false, 0, proc.GetMessageBoard())
			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, 1, result.Batch.RowCount())
			require.Equal(t, bytes.Repeat([]byte("x"), 4096), result.Batch.Vecs[0].GetBytesAt(0))
			oldArea := arg.ctr.resBat.Vecs[0].GetArea()
			_, callErr := arg.Call(proc)
			areaLost := false
			if denied {
				require.Error(t, callErr)
				require.True(t, moerr.IsMoErrCode(callErr, moerr.ErrOOM), callErr)
				require.Contains(t, callErr.Error(), "hash build memory budget exceeded")
				currentArea := arg.ctr.resBat.Vecs[0].GetArea()
				if !sameAreaOwnership(oldArea, currentArea) {
					areaLost = true
					t.Errorf("rejected result growth discarded owned area: before=(nil=%t,len=%d,cap=%d,ptr=%p) after=(nil=%t,len=%d,cap=%d,ptr=%p)",
						oldArea == nil, len(oldArea), cap(oldArea), unsafe.SliceData(oldArea),
						currentArea == nil, len(currentArea), cap(currentArea), unsafe.SliceData(currentArea))
				}
			} else {
				require.NoError(t, callErr)
			}
			arg.Reset(proc, callErr != nil, callErr)
			arg.Free(proc, callErr != nil, callErr)
			argCleaned = true
			require.NoError(t, arg.ClearAllocationAccount(account))
			snapshot, _, terminalErr := registry.CompleteTerminal(account)
			suspended := registry.AdmissionSuspended()
			next, nextErr := registry.Open(1 << 20)
			t.Logf("call=%v terminal=%v used=%d owner=%d site=%d live=%d budget=%d suspended=%v next_admission=%v", callErr, terminalErr, snapshot.Used, snapshot.LiveOwner, snapshot.LiveSite, snapshot.LiveAllocations, generation.Used(), suspended, nextErr)
			if snapshot.Used > 0 && areaLost {
				// The unpatched implementation discarded the Vector Area
				// header. Reclaim the retained alias only after recording the
				// production terminal snapshot.
				orphan = oldArea
			}
			if terminalErr != nil {
				t.Errorf("terminal invariant after production cleanup: %v", terminalErr)
			}
			if snapshot.Used > 0 && !suspended {
				t.Errorf("admission was not suspended while residual allocation remained")
			}
			if snapshot.Used > 0 && nextErr == nil {
				t.Errorf("next admission succeeded while residual allocation remained")
			}
			if snapshot.Used == 0 && nextErr != nil {
				t.Errorf("next admission failed after production cleanup: %v", nextErr)
			}
			if nextErr == nil {
				next.Seal()
				if _, finalizeErr := registry.Finalize(next); finalizeErr != nil {
					t.Errorf("next admission finalization failed: %v", finalizeErr)
				}
			}
			if orphan != nil {
				proc.Mp().Free(orphan)
				orphan = nil
			}
			require.Zero(t, account.Snapshot().Used)
			if nextErr != nil {
				recovered, recoveredErr := registry.Open(1 << 20)
				require.NoError(t, recoveredErr)
				recovered.Seal()
				_, recoveredErr = registry.Finalize(recovered)
				require.NoError(t, recoveredErr)
			}
		})
	}
}

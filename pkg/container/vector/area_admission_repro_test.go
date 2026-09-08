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

package vector

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

// This is a contract test, not a test which expects the leak. Only the
// contiguous bulk-copy case with an existing area should expose the defect.
func TestAreaGrowthAdmissionPreservesCleanup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		initial int
		areaCap int
		payload int
		method  string
		denied  bool
	}{
		{"bulk_success", 4096, 0, 4096, "bulk", false},
		{"bulk_empty_denied", 0, 4096, 65536, "bulk", true},
		{"bulk_existing_denied", 4096, 0, 65536, "bulk", true},
		{"selected_existing_denied", 4096, 0, 65536, "selected", true},
		{"append_existing_denied", 4096, 0, 65536, "append", true},
		{"union_one_existing_denied", 4096, 0, 65536, "one", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			state := newTestVectorAllocationAccount(t, 32768, 64)
			dst := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
			src := NewVec(types.T_varchar.ToType())
			var orphan []byte
			dstFreed, srcFreed := false, false
			t.Cleanup(func() {
				if !dstFreed {
					dst.Free(mp)
				}
				if !srcFreed {
					src.Free(mp)
				}
				// Reclaim only a proven orphan, AFTER collecting the failed
				// production cleanup evidence. Never hide it from the oracle.
				if orphan != nil {
					mp.Free(orphan)
				}
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})
			if tc.areaCap > 0 {
				require.NoError(t, dst.PreExtendWithArea(2, tc.areaCap, mp))
			} else {
				require.NoError(t, dst.PreExtend(2, mp))
			}
			if tc.initial > 0 {
				require.NoError(t, AppendBytes(dst, bytes.Repeat([]byte("a"), tc.initial), false, mp))
			}
			require.NoError(t, AppendBytes(src, bytes.Repeat([]byte("b"), tc.payload), false, mp))
			oldArea := dst.GetArea()
			oldBytes := bytes.Clone(oldArea)
			oldLength := dst.Length()
			oldRows := make([][]byte, oldLength)
			for i := range oldRows {
				oldRows[i] = bytes.Clone(dst.GetBytesAt(i))
			}
			oldUsed := state.account.Snapshot().Used
			areaLost := false
			var err error
			switch tc.method {
			case "bulk":
				err = dst.UnionBatch(src, 0, 1, nil, mp)
			case "selected":
				err = dst.UnionBatch(src, 0, 1, []uint8{1}, mp)
			case "append":
				err = AppendBytes(dst, src.GetBytesAt(0), false, mp)
			case "one":
				err = dst.UnionOne(src, 0, mp)
			}
			if tc.denied {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				if !sameAreaOwnership(oldArea, dst.GetArea()) {
					areaLost = true
					t.Errorf("rejected growth discarded owned area: before=(nil=%t,len=%d,cap=%d,ptr=%p) after=(nil=%t,len=%d,cap=%d,ptr=%p)",
						oldArea == nil, len(oldArea), cap(oldArea), unsafe.SliceData(oldArea),
						dst.GetArea() == nil, len(dst.GetArea()), cap(dst.GetArea()), unsafe.SliceData(dst.GetArea()))
				}
				if !bytes.Equal(oldBytes, dst.GetArea()) {
					t.Errorf("rejected growth changed owned bytes: before=%d after=%d", len(oldArea), len(dst.GetArea()))
				}
				require.Equal(t, oldUsed, state.account.Snapshot().Used)
				if tc.method == "one" {
					// UnionOne advances length before building varlena; this
					// control checks ownership, not atomic append semantics.
					require.Equal(t, oldLength+1, dst.Length())
				} else {
					require.Equal(t, oldLength, dst.Length())
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, oldLength+1, dst.Length())
				require.Equal(t, src.GetBytesAt(0), dst.GetBytesAt(dst.Length()-1))
			}
			// A lost Area is already a failure; avoid dereferencing its invalid
			// varlena offsets so the production cleanup oracle still executes.
			if !areaLost {
				for i, want := range oldRows {
					if !bytes.Equal(want, dst.GetBytesAt(i)) {
						t.Errorf("row %d changed after append (denied=%t)", i, tc.denied)
					}
				}
			}
			dst.Free(mp)
			dstFreed = true
			src.Free(mp)
			srcFreed = true
			remaining := state.account.Snapshot().Used
			snapshot, _, terminalErr := state.registry.CompleteTerminal(state.account)
			suspended := state.registry.AdmissionSuspended()
			next, nextErr := state.registry.Open(1 << 20)
			t.Logf("after Free: used=%d owner=%d site=%d live=%d suspended=%v error=%v next_admission=%v", remaining, snapshot.LiveOwner, snapshot.LiveSite, snapshot.LiveAllocations, suspended, terminalErr, nextErr)
			if remaining != 0 || snapshot.LiveAllocations != 0 {
				t.Errorf("production cleanup retained allocations: account=%d live=%d", remaining, snapshot.LiveAllocations)
			}
			if remaining > 0 && areaLost {
				// The unpatched implementation loses the vector's slice header
				// on a failed grow. The alias is retained solely to clean up the
				// known test allocation after recording the production failure.
				orphan = oldArea
			}
			if terminalErr != nil {
				t.Errorf("terminal invariant after production cleanup: %v", terminalErr)
			}
			if remaining > 0 && !suspended {
				t.Errorf("admission was not suspended while residual allocation remained")
			}
			if remaining > 0 && nextErr == nil {
				t.Errorf("next admission succeeded while residual allocation remained")
			}
			if remaining == 0 && nextErr != nil {
				t.Errorf("next admission failed after production cleanup: %v", nextErr)
			}
			if nextErr == nil {
				next.Seal()
				if _, finalizeErr := state.registry.Finalize(next); finalizeErr != nil {
					t.Errorf("next admission finalization failed: %v", finalizeErr)
				}
			}
			if orphan != nil {
				mp.Free(orphan)
				orphan = nil
			}
			require.Zero(t, state.account.Snapshot().Used)
			if nextErr != nil {
				recovered, recoveredErr := state.registry.Open(1 << 20)
				require.NoError(t, recoveredErr)
				recovered.Seal()
				_, recoveredErr = state.registry.Finalize(recovered)
				require.NoError(t, recoveredErr)
			}
		})
	}
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package preinsertunique

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOrderedSelfReferenceCompositeTuple(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		parent, child                [2]int32
		generated, nullParent, match bool
	}{
		{"complete_match", [2]int32{1, 2}, [2]int32{1, 2}, false, false, true},
		{"partial_match", [2]int32{1, 2}, [2]int32{1, 3}, false, false, false},
		{"generated_match", [2]int32{0, 2}, [2]int32{7, 2}, true, false, true},
		{"generated_mismatch", [2]int32{0, 2}, [2]int32{1, 2}, true, false, false},
		{"null_parent", [2]int32{1, 2}, [2]int32{1, 2}, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			arg := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{AutoIncrementColumn: 0}}
			arg.ctr.ordered.foreignKeyResults = make([]orderedForeignKeyResult, 1)
			t.Cleanup(func() { arg.freeAcceptedState(proc) })
			input := batch.NewWithSize(4)
			for i, v := range []int32{tc.parent[0], tc.parent[1], tc.child[0], tc.child[1]} {
				input.Vecs[i] = testutil.MakeInt32Vector([]int32{v}, nil, proc.Mp())
			}
			if tc.generated {
				input.Vecs[0].SetNull(0)
			}
			if tc.nullParent {
				input.Vecs[1].SetNull(0)
			}
			input.SetRowCount(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			fk := &plan.OrderedForeignKey{ChildColumns: []*plan.ColRef{{ColPos: 2}, {ColPos: 3}}, SelfParentColumns: []*plan.ColRef{{ColPos: 0}, {ColPos: 1}}}
			state := &arg.ctr.ordered.foreignKeyResults[0]
			valid, err := arg.fillOrderedForeignKeyTuple(proc, state, fk.ChildColumns, input, 0, tc.generated, 7)
			require.NoError(t, err)
			require.True(t, valid)
			for range 2 { // Reuse the bounded scratch and expression across candidates.
				matches, err := arg.currentRowSatisfiesForeignKey(proc, state, fk, input, 0, tc.generated, 7)
				require.NoError(t, err)
				require.Equal(t, tc.match, matches)
				require.Nil(t, state.parents, "局部自匹配不得发布父键")
			}
			arg.Reset(proc, false, nil)
			require.Empty(t, arg.ctr.ordered.foreignKeyResults)
		})
	}
}

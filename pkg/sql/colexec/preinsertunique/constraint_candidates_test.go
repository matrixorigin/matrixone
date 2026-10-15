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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestInsertIgnoreConstraintCandidates(t *testing.T) {
	for _, tc := range []struct {
		name                string
		ids, keys           []int32
		generated, eligible []bool
		wantIDs, wantKeys   []int32
	}{
		{"first_and_duplicate", []int32{1, 2, 3, 4}, []int32{1, 2, 2, 3},
			[]bool{true, true, true, true}, []bool{false, true, true, true}, []int32{1, 2}, []int32{2, 3}},
		{"middle", []int32{1, 2, 3}, []int32{1, 2, 3},
			[]bool{true, true, true}, []bool{true, false, true}, []int32{1, 2}, []int32{1, 3}},
		{"rejected_key_is_not_owned", []int32{1, 2}, []int32{7, 7},
			[]bool{true, true}, []bool{false, true}, []int32{1}, []int32{7}},
		{"non_default_series", []int32{2, 5, 8}, []int32{1, 2, 3},
			[]bool{true, true, true}, []bool{false, true, true}, []int32{2, 5}, []int32{2, 3}},
		{"rejected_explicit_does_not_lend", []int32{100, 101}, []int32{7, 7},
			[]bool{false, true}, []bool{false, true}, []int32{101}, []int32{7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			input := makeInsertIgnoreAutoIncrementBatch(t, proc, tc.ids, tc.keys,
				make([]bool, len(tc.ids)), make([]bool, len(tc.ids)), tc.generated)
			t.Cleanup(func() { input.Clean(proc.Mp()); require.Zero(t, proc.Mp().CurrNB()) })
			eligibility := vector.NewVec(types.T_bool.ToType())
			input.Vecs = append(input.Vecs, eligibility)
			for _, pass := range tc.eligible {
				require.NoError(t, vector.AppendFixed(eligibility, pass, false, proc.Mp()))
			}
			arg := newInsertIgnoreAutoIncrementArgument(input)
			arg.PreInsertCtx.RowEligibility = &plan.ColRef{ColPos: 5}
			t.Cleanup(func() { arg.Free(proc, false, nil) })
			require.NoError(t, arg.Prepare(proc))
			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, tc.wantIDs, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
			require.Equal(t, tc.wantKeys, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])
			require.Equal(t, uint64(tc.wantIDs[0]), proc.GetStatementLastInsertID())
		})
	}
}

func TestInsertIgnoreConstraintCandidatesEligibilityBoundary(t *testing.T) {
	for _, mode := range []string{"constant_false", "null", "wrong_type", "missing"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			input := makeInsertIgnoreAutoIncrementBatch(t, proc, []int32{1, 2}, []int32{1, 2},
				[]bool{false, false}, []bool{false, false}, []bool{true, true})
			t.Cleanup(func() { input.Clean(proc.Mp()); require.Zero(t, proc.Mp().CurrNB()) })
			switch mode {
			case "constant_false":
				v, err := vector.NewConstFixed(types.T_bool.ToType(), false, 2, proc.Mp())
				require.NoError(t, err)
				input.Vecs = append(input.Vecs, v)
			case "null":
				input.Vecs = append(input.Vecs, vector.NewConstNull(types.T_bool.ToType(), 2, proc.Mp()))
			case "wrong_type":
				v, err := vector.NewConstFixed(types.T_int32.ToType(), int32(1), 2, proc.Mp())
				require.NoError(t, err)
				input.Vecs = append(input.Vecs, v)
			}
			arg := newInsertIgnoreAutoIncrementArgument(input)
			arg.PreInsertCtx.RowEligibility = &plan.ColRef{ColPos: 5}
			t.Cleanup(func() { arg.Free(proc, false, nil) })
			require.NoError(t, arg.Prepare(proc))
			result, err := arg.Call(proc)
			if mode == "wrong_type" || mode == "missing" {
				require.ErrorContains(t, err, "row eligibility column")
			} else {
				require.NoError(t, err)
				require.True(t, result.Batch.IsEmpty())
				require.Zero(t, proc.GetStatementLastInsertID())
			}
		})
	}
}

func TestInsertIgnoreConstraintCandidatesAcrossEmptyBatch(t *testing.T) {
	proc := testutil.NewProc(t)
	makeInput := func(id int32, pass bool) *batch.Batch {
		input := makeInsertIgnoreAutoIncrementBatch(t, proc, []int32{id}, []int32{7},
			[]bool{false}, []bool{false}, []bool{true})
		t.Cleanup(func() { input.Clean(proc.Mp()) })
		eligibility := vector.NewVec(types.T_bool.ToType())
		input.Vecs = append(input.Vecs, eligibility)
		require.NoError(t, vector.AppendFixed(eligibility, pass, false, proc.Mp()))
		return input
	}
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	arg := newInsertIgnoreAutoIncrementArgument(makeInput(1, false), makeInput(2, true))
	arg.PreInsertCtx.RowEligibility = &plan.ColRef{ColPos: 5}
	t.Cleanup(func() { arg.Free(proc, false, nil) })
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.IsEmpty())
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
	arg.Reset(proc, false, nil)
	require.Empty(t, arg.ctr.autoIncrementCandidates.runs)
}

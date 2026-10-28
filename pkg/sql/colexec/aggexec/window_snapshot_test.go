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

package aggexec

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestWindowPrefixSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int64
	}{
		{"count_star", AggIdOfCountStar}, {"count_column", AggIdOfCountColumn},
		{"sum", AggIdOfSum}, {"avg", AggIdOfAvg}, {"min", AggIdOfMin}, {"max", AggIdOfMax},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := tc.id
			mp := mpool.MustNewZero()
			input := vector.NewOffHeapVecWithType(types.T_float64.ToType())
			values := []float64{0, math.Copysign(0, -1), 1e16, 1, -1e16, math.Float64frombits(0x7ff8000000000001), math.Inf(1)}
			for i, value := range values {
				require.NoError(t, vector.AppendFixed(input, value, i == 0, mp))
			}
			running, err := MakeAgg(mp, id, false, *input.GetType())
			require.NoError(t, err)
			output, err := MakeAgg(mp, id, false, *input.GetType())
			require.NoError(t, err)
			oracle, err := MakeAgg(mp, id, false, *input.GetType())
			require.NoError(t, err)
			t.Cleanup(func() { running.Free(); output.Free(); oracle.Free(); input.Free(mp); require.Zero(t, mp.CurrNB()) })
			require.True(t, SupportsWindowPrefixSnapshot(running))
			require.NoError(t, running.GroupGrow(1))
			require.NoError(t, output.GroupGrow(len(values)))
			require.NoError(t, oracle.GroupGrow(len(values)))
			for row := range values {
				require.NoError(t, running.Fill(0, row, []*vector.Vector{input}))
				require.NoError(t, SnapshotWindowPrefix(output, running, row, 0))
				for previous := 0; previous <= row; previous++ {
					require.NoError(t, oracle.Fill(row, previous, []*vector.Vector{input}))
				}
			}
			got, err := output.Flush()
			require.NoError(t, err)
			want, err := oracle.Flush()
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Len(t, want, 1)
			t.Cleanup(func() { got[0].Free(mp); want[0].Free(mp) })
			for row := range values {
				require.Equal(t, want[0].IsNull(uint64(row)), got[0].IsNull(uint64(row)))
				if !want[0].IsNull(uint64(row)) {
					require.Equal(t, want[0].GetRawBytesAt(row), got[0].GetRawBytesAt(row), "row %d", row)
				}
			}
		})
	}
}

func TestWindowPrefixSnapshotRejectsUnboundedStates(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, tc := range []struct {
		id       int64
		distinct bool
		typ      types.Type
	}{
		{AggIdOfSum, true, types.T_int32.ToType()},
		{AggIdOfMin, false, types.T_varchar.ToType()},
		{AggIdOfVarPop, false, types.T_int32.ToType()},
	} {
		exec, err := MakeAgg(mp, tc.id, tc.distinct, tc.typ)
		require.NoError(t, err)
		require.False(t, SupportsWindowPrefixSnapshot(exec))
		exec.Free()
	}
	require.Zero(t, mp.CurrNB())
}

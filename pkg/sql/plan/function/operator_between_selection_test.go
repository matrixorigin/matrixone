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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBetweenRowSelectionLiteralDateBounds(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	typ := types.T_date.ToType()
	source := vector.NewVec(typ)
	t.Cleanup(func() { source.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixedList(source, []types.Date{-1, 1, 2, 4}, nil, proc.Mp()))
	lower, err := vector.NewConstFixed(typ, types.Date(1), 1, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { lower.Free(proc.Mp()) })
	upper, err := vector.NewConstFixed(typ, types.Date(2), 1, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { upper.Free(proc.Mp()) })
	parameters := []*vector.Vector{source, lower, upper}
	scratch := make([]int64, 0, 4)
	for _, literal := range []bool{false, true} {
		if literal {
			require.NoError(t, lower.SetStringSource(types.StringSourceLiteral))
			require.NoError(t, upper.SetStringSource(types.StringSourceLiteral))
		}
		rows, admitted, err := TrySelectBetweenRows(parameters, proc, 4, scratch)
		require.NoError(t, err)
		require.True(t, admitted)
		require.Equal(t, []int64{1, 2}, rows)
	}
	// Literal provenance on scalar bounds is safe; the source's unproved
	// provenance and a wrong logical domain still take ordinary evaluation.
	require.NoError(t, source.SetStringSource(types.StringSourceLiteral))
	scratch = append(scratch[:0], 99)
	rows, admitted, err := TrySelectBetweenRows(parameters, proc, 4, scratch)
	require.NoError(t, err)
	require.False(t, admitted)
	require.Equal(t, []int64{99}, rows)
	require.NoError(t, source.SetStringSource(types.StringSourceExpression))
	rows, admitted, err = TrySelectBetweenRows(parameters, proc, 5, scratch)
	require.NoError(t, err)
	require.False(t, admitted)
	require.Equal(t, []int64{99}, rows)
}

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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBetweenAfterShuffleInvalidatesSorted(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()
	v := vector.NewVec(typ)
	t.Cleanup(func() { v.Free(mp) })
	require.NoError(t, vector.AppendFixedList(v, []int64{1, 2, 3}, nil, mp))
	v.SetSorted(true)
	require.NoError(t, v.Shuffle([]int64{2, 0}, mp))
	lower, err := vector.NewConstFixed(typ, int64(1), 2, mp)
	require.NoError(t, err)
	t.Cleanup(func() { lower.Free(mp) })
	upper, err := vector.NewConstFixed(typ, int64(2), 2, mp)
	require.NoError(t, err)
	t.Cleanup(func() { upper.Free(mp) })
	result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, betweenImpl([]*vector.Vector{v, lower, upper}, result, nil, 2, nil))
	actual := append([]bool(nil), vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())...)
	sorted := v.GetSorted()
	v.SetSorted(false)
	require.NoError(t, betweenImpl([]*vector.Vector{v, lower, upper}, result, nil, 2, nil))
	require.Equal(t, []bool{false, true}, vector.MustFixedColWithTypeCheck[bool](result.GetResultVector()), "unsorted control")
	require.Equal(t, []bool{false, true}, actual)
	require.False(t, sorted)
}

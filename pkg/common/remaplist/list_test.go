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

package remaplist

import (
	"math"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestListBuild(t *testing.T) {
	mp := mpool.MustNewZero()
	var r List
	require.NoError(t, r.Init(mp, 2))
	require.Equal(t, 0, r.Len())

	require.NoError(t, r.Identity(5))
	require.Equal(t, []int64{0, 1, 2, 3, 4}, r.Rows())
	// A sort reorders the rows in place.
	slices.Reverse(r.Rows())
	require.Equal(t, []int64{4, 3, 2, 1, 0}, r.Rows())

	// Appends and repeats grow the block and keep what is there.
	require.NoError(t, r.Append(7))
	require.NoError(t, r.Repeat(2, 3))
	require.NoError(t, r.Repeat(9, 0))
	require.Equal(t, []int64{4, 3, 2, 1, 0, 7, 2, 2, 2}, r.Rows())
	for i := 0; i < 1000; i++ {
		require.NoError(t, r.Append(int64(i)))
	}
	require.Equal(t, 1009, r.Len())
	require.Equal(t, int64(999), r.Rows()[1008])

	// Reset keeps the capacity: refilling does not allocate.
	before := mp.CurrNB()
	r.Reset()
	require.Equal(t, 0, r.Len())
	require.NoError(t, r.Identity(1000))
	require.Equal(t, before, mp.CurrNB())

	r.Free()
	require.Zero(t, mp.CurrNB())
	r.Free()
	// After Free the List keeps its pool and can be refilled.
	require.NoError(t, r.Append(1))
	require.Equal(t, []int64{1}, r.Rows())
	r.Free()
	require.Zero(t, mp.CurrNB())
}

func TestListErrors(t *testing.T) {
	var r List
	require.Error(t, r.Append(1), "a List needs Init before it can grow")
	require.Error(t, r.Identity(-1))
	mp := mpool.MustNewZero()
	require.NoError(t, r.Init(mp, 0))
	require.Error(t, r.Repeat(0, math.MaxInt/4))
	require.Equal(t, 0, r.Len())
	r.Free()
	require.Zero(t, mp.CurrNB())
}

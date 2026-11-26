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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestInvertInto(t *testing.T) {
	mp := mpool.MustNewZero()
	var fwd, inv List
	require.NoError(t, fwd.Init(mp, 0))
	require.NoError(t, inv.Init(mp, 0))

	// Source rows 1 and 4 are dropped.
	for _, row := range []int64{3, 0, 5, 2} {
		require.NoError(t, fwd.Append(row))
	}
	require.NoError(t, fwd.InvertInto(6, &inv))
	require.Equal(t, []int64{1, -1, 3, 0, -1, 2}, inv.Rows())

	// An empty list drops every row; zero source rows give an empty map.
	fwd.Reset()
	require.NoError(t, fwd.InvertInto(3, &inv))
	require.Equal(t, []int64{-1, -1, -1}, inv.Rows())
	require.NoError(t, fwd.InvertInto(0, &inv))
	require.Equal(t, 0, inv.Len())

	// A random permutation inverts to its inverse, and inverting twice gives
	// it back.
	rng := rand.New(rand.NewSource(1))
	const n = 5000
	require.NoError(t, fwd.Identity(n))
	rng.Shuffle(n, func(i, j int) { fwd.Rows()[i], fwd.Rows()[j] = fwd.Rows()[j], fwd.Rows()[i] })
	require.NoError(t, fwd.InvertInto(n, &inv))
	for out, src := range fwd.Rows() {
		require.Equal(t, int64(out), inv.Rows()[src])
	}
	var back List
	require.NoError(t, back.Init(mp, 0))
	require.NoError(t, inv.InvertInto(n, &back))
	require.Equal(t, fwd.Rows(), back.Rows())

	// Refilling reuses the block.
	before := mp.CurrNB()
	require.NoError(t, fwd.InvertInto(n, &inv))
	require.Equal(t, before, mp.CurrNB())

	fwd.Free()
	inv.Free()
	back.Free()
	require.Zero(t, mp.CurrNB())
}

func TestInvertIntoErrors(t *testing.T) {
	mp := mpool.MustNewZero()
	var fwd, inv List
	require.NoError(t, fwd.Init(mp, 0))
	require.NoError(t, inv.Init(mp, 0))
	for _, row := range []int64{1, 0} {
		require.NoError(t, fwd.Append(row))
	}
	require.NoError(t, fwd.InvertInto(2, &inv))

	require.Error(t, fwd.InvertInto(-1, &inv))
	require.Error(t, fwd.InvertInto(1, &inv), "row 1 is out of range")
	require.Equal(t, 0, inv.Len(), "a failed InvertInto leaves dst empty")
	require.Error(t, fwd.InvertInto(2, &fwd))

	require.NoError(t, fwd.Append(1))
	require.Error(t, fwd.InvertInto(2, &inv), "row 1 repeats")
	fwd.Reset()
	require.NoError(t, fwd.Append(-1))
	require.Error(t, fwd.InvertInto(3, &inv), "row -1 is out of range")

	var noPool List
	require.Error(t, fwd.InvertInto(3, &noPool), "dst needs Init")

	fwd.Free()
	inv.Free()
	require.Zero(t, mp.CurrNB())
}

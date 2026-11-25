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

package batch

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func requireNotAliased(t *testing.T, a, b *vector.Vector) {
	t.Helper()
	da, db := a.GetData(), b.GetData()
	if len(da) == 0 || len(db) == 0 {
		return
	}
	require.False(t, &da[0] == &db[0], "vectors share backing memory")
}

// Clone, Dup and CloneSelectedColumns copy into the pool they are given:
// the copy shares nothing with the source and outlives it.
func TestBatchCopiesAreDeepAndOwnedByTheirPool(t *testing.T) {
	src := mpool.MustNewZero()
	dst := mpool.MustNewZero()
	source := newTwoColumnTestBatch(t, src)
	sourceBytes := src.CurrNB()

	cloned, err := source.Clone(dst)
	require.NoError(t, err)
	dup, err := source.Dup(dst)
	require.NoError(t, err)
	selected, err := source.CloneSelectedColumns([]int{1}, dst)
	require.NoError(t, err)
	require.Equal(t, sourceBytes, src.CurrNB(), "copies allocate from the destination pool")
	for i, copied := range []*Batch{cloned, dup} {
		for c := range source.Vecs {
			requireNotAliased(t, source.Vecs[c], copied.Vecs[c])
		}
		require.Equal(t, source.RowCount(), copied.RowCount(), "copy %d", i)
	}
	requireNotAliased(t, source.Vecs[1], selected.Vecs[0])
	require.Equal(t, source.RowCount(), selected.RowCount())

	source.Clean(src)
	require.Zero(t, src.CurrNB())
	for _, copied := range []*Batch{cloned, dup} {
		require.Equal(t, int64(31), vector.GetFixedAtWithTypeCheck[int64](copied.Vecs[0], 31))
		require.Equal(t, "batch test payload that is not inline", copied.Vecs[1].GetStringAt(31))
		copied.Clean(dst)
	}
	require.Equal(t, "batch test payload that is not inline", selected.Vecs[0].GetStringAt(0))
	selected.Clean(dst)
	require.Zero(t, dst.CurrNB())
}

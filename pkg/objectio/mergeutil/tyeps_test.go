// Copyright 2022 Matrix Origin
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

package mergeutil

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSortColumnsByIndex(t *testing.T) {
	mp := mpool.MustNewZero()

	const (
		vecNum = 3
		vecLen = 50
	)

	var vecs = make([]*vector.Vector, vecNum)
	for i := 0; i < vecNum; i++ {
		if vecs[i] == nil {
			vecs[i] = vector.NewVec(types.T_int32.ToType())
		}

		for j := 0; j < vecLen; j++ {
			x := rand.Int32N(10000)
			err := vector.AppendFixed[int32](vecs[i], x, false, mp)
			require.NoError(t, err)
		}
	}

	for i := 0; i < vecNum; i++ {
		err := SortColumnsByIndex(vecs, i, mp)
		require.NoError(t, err)

		for j := 0; j < vecNum; j++ {
			vals := vector.MustFixedColNoTypeCheck[int32](vecs[j])
			if j == i {
				require.True(t, slices.IsSorted(vals))
				require.True(t, vecs[j].GetSorted())
			} else {
				require.False(t, slices.IsSorted(vals))
				require.False(t, vecs[j].GetSorted())
			}
		}
	}
}

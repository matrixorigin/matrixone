// Copyright 2021 - 2026 Matrix Origin
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

package vector

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestConstNullCleanAndUnionBatch(t *testing.T) {
	for _, oid := range []types.T{types.T_any, types.T_int32, types.T_float64, types.T_text} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			src := NewConstNull(oid.ToType(), 3, mp)
			dst := NewConstNull(oid.ToType(), 3, mp)
			defer func() { src.Free(mp); dst.Free(mp); require.Zero(t, mp.CurrNB()) }()
			for _, length := range []int{3, 1, 0, 4} {
				t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
					src.SetLength(length)
					dst.CleanOnlyData()
					require.Zero(t, dst.Length(), "clean must remove logical rows even without a payload")
					require.NoError(t, dst.UnionBatch(src, 0, length, nil, mp))
					require.Equal(t, length, dst.Length())
					for row := 0; row < dst.Length(); row++ {
						require.True(t, dst.IsNull(uint64(row)))
					}
				})
			}
		})
	}
}

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

package batch

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBatchClonePreservesPartialConstantGrouping(t *testing.T) {
	for _, nullValue := range []bool{false, true} {
		name := "bytes"
		if nullValue {
			name = "null"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewWithSize(1)
			typ := types.T_varchar.ToType()
			var err error
			if nullValue {
				source.Vecs[0] = vector.NewConstNull(typ, 5, mp)
			} else {
				source.Vecs[0], err = vector.NewConstBytes(typ, []byte("constant payload"), 5, mp)
				require.NoError(t, err)
			}
			source.Vecs[0].GetGrouping().Add(0, 2)
			source.SetRowCount(5)
			defer func() { source.Clean(mp); require.Zero(t, mp.CurrNB()) }()
			cloned, err := source.Clone(mp, true)
			require.NoError(t, err)
			defer cloned.Clean(mp)
			require.Equal(t, 5, cloned.RowCount())
			for row := range 5 {
				require.Equal(t, row == 0 || row == 2, cloned.Vecs[0].GetGrouping().Contains(uint64(row)), "row %d", row)
				require.Equal(t, nullValue, cloned.Vecs[0].IsNull(uint64(row)))
				if !nullValue {
					require.Equal(t, "constant payload", cloned.Vecs[0].GetStringAt(row))
				}
			}
		})
	}
}

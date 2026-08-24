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

package compare

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestGroupingSentinelComparesAsNull(t *testing.T) {
	mp := mpool.MustNewZero()
	tests := []struct {
		name string
		typ  types.Type
		add  func(*vector.Vector, bool) error
	}{
		{
			name: "fixed",
			typ:  types.T_int64.ToType(),
			add: func(v *vector.Vector, isNull bool) error {
				return vector.AppendFixed(v, int64(0), isNull, mp)
			},
		},
		{
			name: "varlen",
			typ:  types.T_varchar.ToType(),
			add: func(v *vector.Vector, isNull bool) error {
				return vector.AppendBytes(v, nil, isNull, mp)
			},
		},
		{
			name: "array",
			typ:  types.T_array_float32.ToType(),
			add: func(v *vector.Vector, isNull bool) error {
				return vector.AppendArray(v, []float32{0}, isNull, mp)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			grouping := vector.NewVec(test.typ)
			ordinary := vector.NewVec(test.typ)
			nullValue := vector.NewVec(test.typ)
			t.Cleanup(func() {
				grouping.Free(mp)
				ordinary.Free(mp)
				nullValue.Free(mp)
			})
			require.NoError(t, test.add(grouping, false))
			require.NoError(t, test.add(ordinary, false))
			require.NoError(t, test.add(nullValue, true))
			grouping.GetGrouping().Add(0)

			for _, nullsLast := range []bool{false, true} {
				for _, desc := range []bool{false, true} {
					cmp := New(test.typ, desc, nullsLast)
					cmp.Set(0, grouping)
					cmp.Set(1, ordinary)
					if nullsLast {
						require.Positive(t, cmp.Compare(0, 1, 0, 0))
						require.Negative(t, cmp.Compare(1, 0, 0, 0))
					} else {
						require.Negative(t, cmp.Compare(0, 1, 0, 0))
						require.Positive(t, cmp.Compare(1, 0, 0, 0))
					}

					cmp.Set(1, nullValue)
					require.Zero(t, cmp.Compare(0, 1, 0, 0))
				}
			}
		})
	}
}

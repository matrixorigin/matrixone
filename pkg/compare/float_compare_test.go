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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestFloatCompareOrdersNaNAndNull(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		append func(*vector.Vector, *mpool.MPool) error
	}{
		{
			name: "float32",
			typ:  types.T_float32.ToType(),
			append: func(vec *vector.Vector, mp *mpool.MPool) error {
				for _, value := range []float32{1, math.Float32frombits(0x7fc00001), math.Float32frombits(0x7fc00002)} {
					if err := vector.AppendFixed(vec, value, false, mp); err != nil {
						return err
					}
				}
				return vector.AppendFixed(vec, float32(0), true, mp)
			},
		},
		{
			name: "float64",
			typ:  types.T_float64.ToType(),
			append: func(vec *vector.Vector, mp *mpool.MPool) error {
				for _, value := range []float64{1, math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002)} {
					if err := vector.AppendFixed(vec, value, false, mp); err != nil {
						return err
					}
				}
				return vector.AppendFixed(vec, float64(0), true, mp)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := vector.NewVec(tc.typ)
			defer vec.Free(mp)
			require.NoError(t, tc.append(vec, mp))

			for _, desc := range []bool{false, true} {
				cmp := New(tc.typ, desc, true)
				cmp.Set(0, vec)
				cmp.Set(1, vec)
				if desc {
					require.Negative(t, cmp.Compare(0, 0, 0, 1))
					require.Positive(t, cmp.Compare(0, 0, 1, 2))
				} else {
					require.Positive(t, cmp.Compare(0, 0, 0, 1))
					require.Negative(t, cmp.Compare(0, 0, 1, 2))
				}
				require.Positive(t, cmp.Compare(0, 0, 3, 1))
			}
		})
	}
}

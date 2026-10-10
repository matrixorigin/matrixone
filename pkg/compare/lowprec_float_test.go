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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// lpVec builds a vector of oid (bf16, float16, float8 or float4) from vals.
func lpVec(t *testing.T, oid types.T, mp *mpool.MPool, vals ...float32) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(oid.ToType())
	for _, v := range vals {
		var err error
		switch oid {
		case types.T_bf16:
			err = vector.AppendFixed(vec, types.BF16FromFloat32(v), false, mp)
		case types.T_float16:
			err = vector.AppendFixed(vec, types.Float16FromFloat32(v), false, mp)
		case types.T_float8:
			err = vector.AppendFixed(vec, types.Float8FromFloat32(v), false, mp)
		case types.T_float4:
			err = vector.AppendFixed(vec, types.Float4FromFloat32(v), false, mp)
		}
		require.NoError(t, err)
	}
	return vec
}

var lpTypes = []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4}

// TestLowPrecisionFloatCompare checks that the bf16, float16, float8 and float4
// comparators order by value (not by bits, where negative values sort after positive
// ones) in both directions and copy values.
func TestLowPrecisionFloatCompare(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range lpTypes {
		vec := lpVec(t, oid, mp, -2, 0.5, 1.5)
		for _, desc := range []bool{false, true} {
			for _, c := range []Compare{New(oid.ToType(), desc, false), NewOrder(oid.ToType(), desc, false)} {
				require.NotNil(t, c, oid.String())
				c.Set(0, vec)
				c.Set(1, vec)
				want := -1
				if desc {
					want = 1
				}
				require.Equal(t, want, c.Compare(0, 1, 0, 2), "%s desc=%v", oid, desc)
				require.Equal(t, want, c.Compare(0, 1, 0, 1), "%s desc=%v", oid, desc)
				require.Equal(t, 0, c.Compare(0, 1, 1, 1))
			}
		}
		dst := lpVec(t, oid, mp, 0, 0, 0)
		c := New(oid.ToType(), false, false)
		c.Set(0, dst)
		c.Set(1, vec)
		require.NoError(t, c.Copy(1, 0, 0, 2, testutil.NewProcessWithMPool(t, "", mp)))
		f, _ := vector.GetLowPrecisionFloatAt(dst, 2)
		require.Equal(t, float32(-2), f)
		vec.Free(mp)
		dst.Free(mp)
	}
}

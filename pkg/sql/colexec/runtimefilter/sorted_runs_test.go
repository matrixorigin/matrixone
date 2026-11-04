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

package runtimefilter

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMarshalExactFilterRuns(t *testing.T) {
	for _, variable := range []bool{false, true} {
		t.Run(map[bool]string{false: "fixed", true: "variable"}[variable], func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
			require.NoError(t, err)
			account, err := registry.Open(1 << 30)
			require.NoError(t, err)
			typ := types.T_int64.ToType()
			if variable {
				typ = types.T_varchar.ToType()
			}
			x, y, expected := vector.NewVec(typ), vector.NewVec(typ), vector.NewVec(typ)
			defer func() {
				x.Free(mp)
				y.Free(mp)
				expected.Free(mp)
				require.Zero(t, mp.CurrNB())
				require.Zero(t, account.Snapshot().Used)
			}()
			for i := 0; i < 31; i++ {
				v := x
				if i%2 != 0 {
					v = y
				}
				if variable {
					value := append([]byte{byte('a' + i%26)}, make([]byte, i+1)...)
					require.NoError(t, vector.AppendBytes(v, value, false, mp))
					require.NoError(t, vector.AppendBytes(expected, value, false, mp))
				} else {
					require.NoError(t, vector.AppendFixed(v, int64(31-i), false, mp))
					require.NoError(t, vector.AppendFixed(expected, int64(31-i), false, mp))
				}
			}
			for _, v := range []*vector.Vector{x, y, expected} {
				require.NoError(t, v.InplaceSort(mp))
			}
			data, release, err := MarshalExactFilterRuns(context.Background(), []*vector.Vector{x, y}, mp, account, mpool.AllocationOwnerHashBuild, mpool.AllocationSiteMin)
			require.NoError(t, err)
			defer release()
			var actual vector.Vector
			require.NoError(t, actual.UnmarshalBinary(data))
			defer actual.Free(nil)
			require.Equal(t, expected.Length(), actual.Length())
			for row := 0; row < actual.Length(); row++ {
				if variable {
					require.Equal(t, expected.GetRawBytesAt(row), actual.GetRawBytesAt(row))
				} else {
					require.Equal(t, vector.MustFixedColNoTypeCheck[int64](expected)[row], vector.MustFixedColNoTypeCheck[int64](&actual)[row])
				}
			}
			require.True(t, actual.GetSorted())
		})
	}
}

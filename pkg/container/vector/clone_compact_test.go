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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCloneToFlatCompactFreesPartialAllocation(t *testing.T) {
	srcMP := mpool.MustNew("clone-compact-source")
	src := NewVec(types.T_varchar.ToType(), srcMP)
	require.NoError(t, AppendBytes(src, make([]byte, 2<<20), false, srcMP))

	dstMP := mpool.MustNew("clone-compact-destination")
	oldLimit := mpool.CapLimit
	mpool.CapLimit = 1 << 20
	t.Cleanup(func() { mpool.CapLimit = oldLimit })
	got, err := src.CloneToFlatCompact(dstMP)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, int64(0), dstMP.CurrNB())

	src.Free(srcMP)
	require.Equal(t, int64(0), srcMP.CurrNB())
}

func TestCloneToFlatCompactExpandsConst(t *testing.T) {
	mp := mpool.MustNewZero()
	tests := []struct {
		name       string
		newSource  func(t *testing.T) *Vector
		wantNull   bool
		checkValue func(t *testing.T, vec *Vector)
	}{
		{
			name: "fixed-value",
			newSource: func(t *testing.T) *Vector {
				vec, err := NewConstFixed(types.T_int8.ToType(), int8(7), 3, mp)
				require.NoError(t, err)
				return vec
			},
			checkValue: func(t *testing.T, vec *Vector) {
				for row := range 3 {
					require.Equal(t, int8(7), GetFixedAtNoTypeCheck[int8](vec, row))
				}
			},
		},
		{
			name: "varlen-value",
			newSource: func(t *testing.T) *Vector {
				vec, err := NewConstBytes(types.T_varchar.ToType(), []byte("rollup"), 3, mp)
				require.NoError(t, err)
				return vec
			},
			checkValue: func(t *testing.T, vec *Vector) {
				for row := range 3 {
					require.Equal(t, "rollup", string(vec.GetBytesAt(row)))
				}
			},
		},
		{
			name: "fixed-const-null",
			newSource: func(t *testing.T) *Vector {
				return NewConstNull(types.T_int8.ToType(), 3, mp)
			},
			wantNull: true,
		},
		{
			name: "varlen-const-null",
			newSource: func(t *testing.T) *Vector {
				return NewConstNull(types.T_varchar.ToType(), 3, mp)
			},
			wantNull: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			src := test.newSource(t)
			dst, err := src.CloneToFlatCompact(mp)
			require.NoError(t, err)
			require.False(t, dst.IsConst())
			require.Equal(t, 3, dst.Length())
			for row := range 3 {
				require.Equal(t, test.wantNull, dst.GetNulls().Contains(uint64(row)))
			}
			if test.checkValue != nil {
				test.checkValue(t, dst)
			}

			src.Free(mp)
			dst.Free(mp)
			require.Equal(t, int64(0), mp.CurrNB())
		})
	}
}

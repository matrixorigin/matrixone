// Copyright 2022 Matrix Origin
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

package docfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// buildVarcharVec builds a varchar vector (non-integer PK -> bloom fallback).
func buildVarcharVec(t *testing.T, mp *mpool.MPool, vals []string) *vector.Vector {
	v := vector.NewVec(types.T_varchar.ToType())
	for _, s := range vals {
		require.NoError(t, vector.AppendBytes(v, []byte(s), false, mp))
	}
	return v
}

// TestBuildNewRoundTrip drives the public Build -> New path and checks that each
// PK shape selects the right structure (tag), reconstructs into a working
// filter, and reports the expected exactness.
func TestBuildNewRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("cbitmap-dense-int", func(t *testing.T) {
		v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 100, 4096}, nil)
		defer v.Free(mp)
		assertRoundTrip(t, v, TagCbitmap, true)
	})

	t.Run("croaring-sparse-int", func(t *testing.T) {
		// max value exceeds MaxCbitmapBits -> cbitmap infeasible -> CRoaring.
		v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, int64(MaxCbitmapBits) + 5, 7}, nil)
		defer v.Free(mp)
		assertRoundTrip(t, v, TagCRoaring, true)
	})

	t.Run("bloom-varchar", func(t *testing.T) {
		v := buildVarcharVec(t, mp, []string{"alpha", "beta", "gamma"})
		defer v.Free(mp)
		assertRoundTrip(t, v, TagBloom, false)
	})
}

// assertRoundTrip builds a filter from v, asserts the tag and exactness, and
// verifies that every (present) row of v tests positive through Test,
// TestVector, and a Shared copy.
func assertRoundTrip(t *testing.T, v *vector.Vector, wantTag byte, wantExact bool) {
	payload, err := Build(v)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, wantTag, payload[0], "selected structure tag")

	f, err := New(payload)
	require.NoError(t, err)
	require.True(t, f.Valid())
	require.Equal(t, wantExact, f.Exact())

	// Every present row tests positive (no false negatives in any structure).
	for i := 0; i < v.Length(); i++ {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "row %d should be present", i)
	}
	res := f.TestVector(v, nil)
	require.Len(t, res, v.Length())
	for i, r := range res {
		require.Equal(t, uint8(1), r, "TestVector row %d", i)
	}

	// A shared copy is independently usable and freeable.
	s := f.Share()
	require.True(t, s.Valid())
	require.True(t, s.Test(v.GetRawBytesAt(0)))
	s.Free()
	f.Free()
}

func TestNewErrors(t *testing.T) {
	// empty / tag-only payloads
	_, err := New(nil)
	require.Error(t, err)
	_, err = New([]byte{TagCbitmap})
	require.Error(t, err)

	// unknown tag
	_, err = New([]byte{0x7f, 1, 2, 3})
	require.Error(t, err)

	// valid tag, corrupt payload (too short to deserialize)
	_, err = New([]byte{TagCbitmap, 1, 2, 3})
	require.Error(t, err)
	_, err = New([]byte{TagCRoaring, 1, 2, 3})
	require.Error(t, err)
}

// TestCFilterBridge covers the cgo bridge methods (CHandle/CKind) on
// MembershipFilter used by the usearch filtered search, for all three structures.
func TestCFilterBridge(t *testing.T) {
	mp := mpool.MustNewZero()

	cases := []struct {
		name     string
		build    func() *vector.Vector
		wantKind byte
	}{
		{"cbitmap", func() *vector.Vector {
			return buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3}, nil)
		}, TagCbitmap},
		{"croaring", func() *vector.Vector {
			return buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, int64(MaxCbitmapBits) + 5}, nil)
		}, TagCRoaring},
		{"bloom", func() *vector.Vector {
			return buildVarcharVec(t, mp, []string{"a", "b"})
		}, TagBloom},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.build()
			defer v.Free(mp)
			f, err := New(must(Build(v)))
			require.NoError(t, err)
			defer f.Free()

			require.Equal(t, tc.wantKind, f.CKind())
			require.NotNil(t, f.CHandle())
		})
	}
}

func TestCbitmapFeasible(t *testing.T) {
	require.True(t, CbitmapFeasible(0))
	require.True(t, CbitmapFeasible(100))
	require.True(t, CbitmapFeasible(MaxCbitmapBits-1))
	require.False(t, CbitmapFeasible(MaxCbitmapBits))
	require.False(t, CbitmapFeasible(MaxCbitmapBits+1000))
}

// TestConcreteFilterEdges covers the C-backed filters' callback path in
// TestVector and their nil-receiver safety.
func TestConcreteFilterEdges(t *testing.T) {
	mp := mpool.MustNewZero()

	for _, tc := range []struct {
		name string
		vals []int64
	}{
		{"cbitmap", []int64{1, 2, 3}},
		{"croaring", []int64{1, int64(MaxCbitmapBits) + 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := buildIntVec(t, mp, types.T_int64.ToType(), tc.vals, nil)
			defer v.Free(mp)
			f, err := New(must(Build(v)))
			require.NoError(t, err)
			defer f.Free()

			hits := 0
			f.TestVector(v, func(exist, isnull bool, row int) {
				if exist {
					hits++
				}
			})
			require.Equal(t, len(tc.vals), hits)
		})
	}

	// nil-receiver safety for the C-backed filters
	var cf *CbitmapFilter
	require.False(t, cf.Valid())
	require.False(t, cf.Test([]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	require.Nil(t, cf.TestVector(nil, nil))
	cf.Free() // must not panic

	var rf *CRoaringFilter
	require.False(t, rf.Valid())
	require.False(t, rf.Test([]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	require.Nil(t, rf.TestVector(nil, nil))
	rf.Free() // must not panic
}

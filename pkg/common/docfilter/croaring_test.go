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

func TestCRoaringFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	present := []int64{1, 2, 100, 1 << 40, 7}
	v := buildIntVec(t, mp, types.T_int64.ToType(), present, nil)
	defer v.Free(mp)

	payload, err := BuildCRoaringBytes(v)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	f, err := NewCRoaringFilter(payload)
	require.NoError(t, err)
	require.True(t, f.Valid())
	defer f.Free()

	for i := range present {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "value %d present", present[i])
	}
	absent := buildIntVec(t, mp, types.T_int64.ToType(), []int64{3, 4, 999, 1 << 41}, nil)
	defer absent.Free(mp)
	for i := 0; i < absent.Length(); i++ {
		require.False(t, f.Test(absent.GetRawBytesAt(i)))
	}

	probe := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 100, 0}, map[int]bool{3: true})
	defer probe.Free(mp)
	require.Equal(t, []uint8{1, 0, 1, 0}, f.TestVector(probe, nil))

	// uint32 width
	v32 := buildIntVec(t, mp, types.T_uint32.ToType(), []uint32{5, 6, 7}, nil)
	defer v32.Free(mp)
	f32, err := NewCRoaringFilter(must(BuildCRoaringBytes(v32)))
	require.NoError(t, err)
	defer f32.Free()
	require.True(t, f32.Test(v32.GetRawBytesAt(0)))
	require.True(t, f32.Test(v32.GetRawBytesAt(2)))

	// share + independent free
	a := f.SharePointer()
	a.Free()
	require.True(t, f.Valid())
	require.True(t, f.Test(v.GetRawBytesAt(0)))
}

// constProbeFilter is the minimal surface the const-vector regression needs.
type constProbeFilter interface {
	TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8
	Free()
}

// runConstVectorFilterTest covers #25621 for a cgo integer membership filter: constant vectors
// carry a single physical value but a logical Length() of the row count, so TestVector must test
// that one value and broadcast the result to all rows (const-null -> all zero, no panic), and
// Build from a constant must contain the value. newFromVec builds the concrete filter from v.
func runConstVectorFilterTest(t *testing.T, newFromVec func(*testing.T, *vector.Vector) constProbeFilter) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()

	// filter over regular values including 42.
	v := buildIntVec(t, mp, typ, []int64{1, 42, 100, 7}, nil)
	defer v.Free(mp)
	f := newFromVec(t, v)
	defer f.Free()

	cPresent, err := vector.NewConstFixed[int64](typ, 42, 3, mp)
	require.NoError(t, err)
	defer cPresent.Free(mp)
	require.Equal(t, []uint8{1, 1, 1}, f.TestVector(cPresent, nil), "const present value must broadcast to all rows")

	cAbsent, err := vector.NewConstFixed[int64](typ, 9999, 3, mp)
	require.NoError(t, err)
	defer cAbsent.Free(mp)
	require.Equal(t, []uint8{0, 0, 0}, f.TestVector(cAbsent, nil))

	cNull := vector.NewConstNull(typ, 3, mp)
	defer cNull.Free(mp)
	var res []uint8
	require.NotPanics(t, func() { res = f.TestVector(cNull, nil) })
	require.Equal(t, []uint8{0, 0, 0}, res)

	// build a filter FROM a constant vector -> it must contain the value.
	cBuild, err := vector.NewConstFixed[int64](typ, 55, 3, mp)
	require.NoError(t, err)
	defer cBuild.Free(mp)
	f2 := newFromVec(t, cBuild)
	defer f2.Free()
	probe, err := vector.NewConstFixed[int64](typ, 55, 2, mp)
	require.NoError(t, err)
	defer probe.Free(mp)
	require.Equal(t, []uint8{1, 1}, f2.TestVector(probe, nil))

	// A non-null constant shrunk to logical length 0 (public SetLength(0)) retains its
	// physical value but has no rows: building a filter FROM it must yield an EMPTY filter,
	// NOT one containing the retained value. Regression: vecFixedArgs used to return nitem=1
	// for it, inserting the value.
	cEmpty, err := vector.NewConstFixed[int64](typ, 77, 1, mp)
	require.NoError(t, err)
	cEmpty.SetLength(0)
	require.Equal(t, 0, cEmpty.Length())
	defer cEmpty.Free(mp)
	fEmpty := newFromVec(t, cEmpty)
	defer fEmpty.Free()
	probe77, err := vector.NewConstFixed[int64](typ, 77, 2, mp)
	require.NoError(t, err)
	defer probe77.Free(mp)
	require.Equal(t, []uint8{0, 0}, fEmpty.TestVector(probe77, nil), "zero-length const must build an empty filter")
}

func TestCRoaringConstVector(t *testing.T) {
	runConstVectorFilterTest(t, func(t *testing.T, v *vector.Vector) constProbeFilter {
		f, err := NewCRoaringFilter(must(BuildCRoaringBytes(v)))
		require.NoError(t, err)
		return f
	})
}

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
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// requireSameRows checks that got holds want's logical rows.
func requireSameRows(t *testing.T, want, got *Vector) {
	t.Helper()
	require.Equal(t, want.Length(), got.Length())
	require.Equal(t, *want.GetType(), *got.GetType())
	require.Equal(t, want.IsConst(), got.IsConst())
	for i := 0; i < want.Length(); i++ {
		require.Equal(t, want.IsNull(uint64(i)), got.IsNull(uint64(i)), "row %d", i)
		if !want.IsNull(uint64(i)) {
			require.Equal(t, want.GetRawBytesAt(i), got.GetRawBytesAt(i), "row %d", i)
		}
	}
}

// trustedMarshalCases builds the vector layouts a trusted plan writes: flat
// fixed and varlen data with and without nulls, constants, empty and sorted
// vectors, and read-only windows whose null bitmaps are shifted.
func trustedMarshalCases(t *testing.T, mp *mpool.MPool) map[string]*Vector {
	long := func(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, types.VarlenaInlineSize+1+i%9) }
	fixed := NewVec(types.T_int64.ToType(), mp)
	varlen := NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < 200; i++ {
		require.NoError(t, AppendFixed(fixed, int64(i), i%7 == 0, mp))
		value := long(i)
		if i%3 == 0 {
			value = []byte("s")
		}
		require.NoError(t, AppendBytes(varlen, value, i%11 == 0, mp))
	}
	sorted := NewVec(types.T_int32.ToType(), mp)
	for i := 0; i < 10; i++ {
		require.NoError(t, AppendFixed(sorted, int32(i), false, mp))
	}
	sorted.SetSorted(true)
	stale := NewVec(types.T_text.ToType(), mp)
	for i := 0; i < 5; i++ {
		require.NoError(t, AppendBytes(stale, long(i), false, mp))
	}
	// A null row whose descriptor still points into the area.
	stale.GetNulls().Add(2)
	constFixed, err := NewConstFixed(types.T_int64.ToType(), int64(42), 9, mp)
	require.NoError(t, err)
	constBytes, err := NewConstBytes(types.T_varchar.ToType(), long(3), 4, mp)
	require.NoError(t, err)
	windowVarlen, err := varlen.Window(3, 150)
	require.NoError(t, err)
	windowFixed, err := fixed.Window(70, 199)
	require.NoError(t, err)
	cases := map[string]*Vector{
		"fixed":         fixed,
		"varlen":        varlen,
		"sorted":        sorted,
		"stale-null":    stale,
		"const-fixed":   constFixed,
		"const-bytes":   constBytes,
		"const-null":    NewConstNull(types.T_varchar.ToType(), 6, mp),
		"empty":         NewVec(types.T_varchar.ToType(), mp),
		"window-varlen": windowVarlen,
		"window-fixed":  windowFixed,
	}
	t.Cleanup(func() {
		for name, vec := range cases {
			if name != "window-varlen" && name != "window-fixed" {
				vec.Free(mp)
			}
		}
		require.Zero(t, mp.CurrNB())
	})
	return cases
}

// A trusted plan writes exactly Size bytes, allocates nothing, and decodes,
// checked and trusted, to the vector's rows.
func TestMarshalToBytesWritesEveryLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	for name, vec := range trustedMarshalCases(t, mp) {
		t.Run(name, func(t *testing.T) {
			plan, err := vec.PrepareMarshalBinaryTrusted()
			require.NoError(t, err)
			dst := bytes.Repeat([]byte{0xAA}, plan.Size()+8)
			var n int
			require.Zero(t, testing.AllocsPerRun(5, func() {
				n, err = plan.MarshalToBytes(dst)
			}))
			require.NoError(t, err)
			require.Equal(t, plan.Size(), n)
			require.Equal(t, bytes.Repeat([]byte{0xAA}, 8), dst[n:], "nothing is written past Size")

			for _, decode := range []func(*Vector, []byte) error{
				(*Vector).UnmarshalBinary, (*Vector).UnmarshalBinaryTrusted,
			} {
				got := NewVec(types.T_any.ToType(), nil)
				require.NoError(t, decode(got, dst[:n]))
				requireSameRows(t, vec, got)
				require.Equal(t, vec.GetSorted(), got.GetSorted())
			}

			// The trusted bytes match the checked encoding whenever the
			// checked codec keeps the area as is.
			if !vec.requiresCanonicalVarlenMarshal(uint64(plan.dataLength)) {
				want, err := vec.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, want, dst[:n], "same bytes as MarshalBinary")
			}
		})
	}
}

func TestMarshalToBytesRejectsMisuse(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType(), mp)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	for i := 0; i < 3; i++ {
		require.NoError(t, AppendBytes(vec, bytes.Repeat([]byte("x"), 40), false, mp))
	}
	plan, err := vec.PrepareMarshalBinaryTrusted()
	require.NoError(t, err)
	_, err = plan.MarshalToBytes(make([]byte, plan.Size()-1))
	require.Error(t, err, "short buffer")
	_, err = MarshalBinaryPlan{}.MarshalToBytes(make([]byte, 10))
	require.Error(t, err, "no vector")

	// A checked plan that rewrites a shared area is MarshalTo's only.
	descriptors := MustFixedColNoTypeCheck[types.Varlena](vec)
	descriptors[2] = descriptors[0]
	checked, err := vec.PrepareMarshalBinary()
	require.NoError(t, err)
	_, err = checked.MarshalToBytes(make([]byte, checked.Size()))
	require.Error(t, err)

	// A plan is a snapshot: a null bitmap that grew after planning no longer
	// fits its size, and the write fails instead of overrunning it. (The
	// batch encoder plans and writes in one call, and checks the total size
	// fixed at Encode.)
	vec.GetNulls().Add(0)
	plan, err = vec.PrepareMarshalBinaryTrusted()
	require.NoError(t, err)
	vec.GetNulls().Add(1000)
	dst := bytes.Repeat([]byte{0xAA}, plan.Size()+1024)
	_, err = plan.MarshalToBytes(dst)
	require.ErrorContains(t, err, "changed while marshaling")
	require.Equal(t, bytes.Repeat([]byte{0xAA}, 1024), dst[plan.Size():],
		"a changed vector never writes past Size, whatever the capacity")
	vec.GetNulls().Del(1000)
}

func ExampleMarshalBinaryPlan_MarshalToBytes() {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int32.ToType(), mp)
	_ = AppendFixed(vec, int32(7), false, mp)
	plan, _ := vec.PrepareMarshalBinaryTrusted()
	dst := make([]byte, plan.Size())
	n, err := plan.MarshalToBytes(dst)
	fmt.Println(n == plan.Size(), err)
	vec.Free(mp)
	// Output: true <nil>
}

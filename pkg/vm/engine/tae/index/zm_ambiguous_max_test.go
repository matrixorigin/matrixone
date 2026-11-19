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

package index

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// legacyObjectSummaryForTest reproduces the historical V3 writer's min/max-only merge; it
// has no truncation bit even though a represented row has a longer FF prefix.
func legacyObjectSummaryForTest(rows ...[]byte) ZM {
	block := NewZM(types.T_varbinary, 0)
	for _, row := range rows {
		UpdateZM(block, row)
	}
	object := NewZM(types.T_varbinary, 0)
	UpdateZM(object, block.GetMinBuf())
	UpdateZM(object, block.GetMaxBuf())
	return object
}

func TestByteZoneMapScalarAmbiguousMax(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xff}, 30)
	lower, upper := append(bytes.Clone(prefix), 1), append(bytes.Clone(prefix), 2)
	for _, tc := range []struct {
		name string
		zm   ZM
	}{
		{"current", BuildZM(types.T_varbinary, upper)},
		{"legacy summary", legacyObjectSummaryForTest(upper)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, tc.zm.ContainsKey(upper))
			require.True(t, tc.zm.AnyGTByValue(lower))
			// Even exact 30 FF is indistinguishable from an old summary.
			require.True(t, tc.zm.AnyGTByValue(prefix))
			bound := BuildZM(types.T_varbinary, lower)
			got, ok := tc.zm.AnyGT(bound)
			require.True(t, ok)
			require.True(t, got)
		})
	}
	for _, tc := range []struct {
		name string
		zm   ZM
	}{
		{"current", BuildZM(types.T_varbinary, lower)},
		{"legacy summary", legacyObjectSummaryForTest(lower)},
	} {
		t.Run("lt/"+tc.name, func(t *testing.T) {
			require.True(t, tc.zm.AnyLTByValue(upper))
			bound := BuildZM(types.T_varbinary, upper)
			got, ok := tc.zm.AnyLT(bound)
			require.True(t, ok)
			require.True(t, got)
		})
	}
	// A finite, non-saturated maximum must retain normal exclusion.
	ordinary := BuildZM(types.T_varbinary, []byte("middle"))
	require.False(t, ordinary.AnyGTByValue([]byte("z")))
	require.False(t, ordinary.AnyLTByValue([]byte("a")))
	require.False(t, ordinary.ContainsKey([]byte("z")))
}

func TestByteZoneMapPrefixAndMembershipAmbiguousMax(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xff}, 30)
	lower, actual, upper := append(bytes.Clone(prefix), 1), append(bytes.Clone(prefix), 2), append(bytes.Clone(prefix), 3)
	for _, tc := range []struct {
		name string
		zm   ZM
	}{
		{"current", BuildZM(types.T_varbinary, actual)},
		{"legacy summary", legacyObjectSummaryForTest(actual)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, tc.zm.PrefixEq(actual))
			require.True(t, tc.zm.PrefixBetween(lower, upper))
			for hint := uint8(0); hint < 4; hint++ {
				require.True(t, tc.zm.PrefixInRange(lower, upper, hint), "hint %d", hint)
			}
			require.True(t, AnyPrefixGTByValue(tc.zm, lower, types.T_varbinary).MayMatch())
			require.True(t, AnyPrefixGEByValue(tc.zm, actual, types.T_varbinary).MayMatch())
			require.False(t, AnyPrefixGTByValue(tc.zm, prefix, types.T_varbinary).MayMatch(), "no 30-byte prefix exceeds all-FF")

			mp := mpool.MustNewZero()
			v := vector.NewVec(types.T_varbinary.ToType())
			defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
			for _, x := range [][]byte{[]byte("a"), []byte("z"), lower, actual, upper} {
				require.NoError(t, vector.AppendBytes(v, x, false, mp))
			}
			require.True(t, tc.zm.AnyIn(v))
			lo, hi := tc.zm.SubVecIn(v)
			require.LessOrEqual(t, lo, 3)
			require.Greater(t, hi, 3)
			require.True(t, tc.zm.PrefixIn(v))
		})
	}
}

func TestByteZoneMapLegacyVectorRoundTrip(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xff}, 30)
	value := append(bytes.Clone(prefix), 2)
	zm := legacyObjectSummaryForTest(value)
	require.False(t, zm.MaxTruncated())
	mp := mpool.MustNewZero()
	v, err := ZMToVector(zm, nil, mp)
	require.NoError(t, err)
	defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
	require.True(t, v.GetNulls().Contains(1), "ambiguous upper endpoint is not an exact value")
	restored := VectorToZM(v, nil)
	require.True(t, restored.ContainsKey(value))
}

func TestByteZoneMapAmbiguousMaxBoundariesAndMerge(t *testing.T) {
	ff := func(n int) []byte { return bytes.Repeat([]byte{0xff}, n) }
	rounded := append(ff(29), 0xfe, 0x01)
	for _, tc := range []struct {
		name                 string
		value                []byte
		truncated, ambiguous bool
	}{
		{"29 bytes", ff(29), false, false},
		{"exact sentinel", ff(30), false, true},
		{"flagged suffix", ff(31), true, true},
		{"rounded to sentinel", rounded, false, true},
		{"finite truncated", bytes.Repeat([]byte{'a'}, 31), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zm := BuildZM(types.T_varbinary, tc.value)
			require.Equal(t, tc.truncated, zm.MaxTruncated())
			require.Equal(t, tc.ambiguous, zm.MayHaveUnboundedByteMax())
			require.True(t, zm.ContainsKey(tc.value))
		})
	}

	low := BuildZM(types.T_varbinary, []byte("a"))
	high := BuildZM(types.T_varbinary, append(ff(30), 2))
	lowBefore, highBefore := low.Clone(), high.Clone()
	for _, order := range [][]ZM{{low, high}, {high, low}} {
		merged := NewZM(types.T_varbinary, 0)
		for _, source := range order {
			MergeZM(merged, source)
		}
		require.True(t, merged.MaxTruncated())
		require.True(t, merged.ContainsKey([]byte("a")))
		require.True(t, merged.ContainsKey(append(ff(30), 2)))
	}
	require.Equal(t, lowBefore, low)
	require.Equal(t, highBefore, high)

	// The old writer lost only the flag, not the ambiguous upper bytes.
	legacy := legacyObjectSummaryForTest(append(ff(30), 2))
	merged := NewZM(types.T_varbinary, 0)
	MergeZM(merged, legacy)
	require.False(t, merged.MaxTruncated())
	require.True(t, merged.MayHaveUnboundedByteMax())
}

func TestByteZoneMapAmbiguousEncodingAndLowerPruning(t *testing.T) {
	value := append(bytes.Repeat([]byte{0xff}, 30), 2)
	for _, typ := range []types.T{
		types.T_varbinary, types.T_varchar, types.T_json, types.T_datalink,
		types.T_geometry, types.T_geometry32,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			current := BuildZM(typ, value)
			legacy := NewZM(typ, 0)
			UpdateZM(legacy, current.GetMinBuf())
			UpdateZM(legacy, current.GetMaxBuf())
			for _, zm := range []ZM{current, legacy} {
				require.True(t, zm.MayHaveUnboundedByteMax())
				require.True(t, zm.ContainsKey(value))
				require.False(t, zm.PrefixEq([]byte("a")))
				require.False(t, zm.PrefixBetween([]byte("a"), []byte("z")))
			}
		})
	}

	zm := legacyObjectSummaryForTest(value)
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_varbinary.ToType())
	defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
	for _, x := range []string{"a", "b", "c", "d"} {
		require.NoError(t, vector.AppendBytes(v, []byte(x), false, mp))
	}
	require.False(t, zm.AnyIn(v))
	lo, hi := zm.SubVecIn(v)
	require.Equal(t, v.Length(), lo)
	require.Equal(t, v.Length(), hi)
}

func TestByteEqualityAmbiguousMaxKeepsLowerProof(t *testing.T) {
	ff := bytes.Repeat([]byte{0xff}, 30)
	for _, tc := range []struct {
		name   string
		value  []byte
		legacy bool
	}{
		{"flagged", append(bytes.Clone(ff), 2), false},
		{"legacy", append(bytes.Clone(ff), 2), true},
		{"exact", ff, false},
		{"rounded", append(append(bytes.Clone(ff[:29]), 0xfe), 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zm := BuildZM(types.T_varbinary, tc.value)
			if tc.legacy {
				zm = legacyObjectSummaryForTest(tc.value)
			}
			typ := types.New(types.T_varbinary, 64, 0)
			match, _, ok := MakeScalarZoneMapMatcher(typ, []byte("a"), typ, 0, nil, types.ComparisonEqual)
			require.True(t, ok)
			require.True(t, match(zm).Excludes(), "a key below the minimum is absent even when the maximum is ambiguous")
			match, _, ok = MakeScalarZoneMapMatcher(typ, tc.value, typ, 0, nil, types.ComparisonEqual)
			require.True(t, ok)
			require.True(t, match(zm).MayMatch(), "the represented value must remain a candidate")
		})
	}
	typ := types.New(types.T_varbinary, 64, 0)
	min := append(bytes.Repeat([]byte{'a'}, 29), 'c')
	zm := BuildZM(types.T_varbinary, min)
	UpdateZM(zm, append(bytes.Clone(ff), 2))
	longLowerKey := append(append(bytes.Repeat([]byte{'a'}, 29), 'b'), 0)
	match, _, ok := MakeScalarZoneMapMatcher(typ, longLowerKey, typ, 0, nil, types.ComparisonEqual)
	require.True(t, ok)
	require.True(t, match(zm).Excludes(), "a long bound is compared directly, without rounding it upward")
	require.False(t, match(nil).Known)
	require.False(t, match(BuildZM(types.T_varchar, []byte("a"))).Known)

	finite := BuildZM(types.T_varbinary, []byte("m"))
	for _, tc := range []struct {
		value string
		match bool
	}{{"a", false}, {"m", true}, {"z", false}} {
		m, _, valid := MakeScalarZoneMapMatcher(typ, []byte(tc.value), typ, 0, nil, types.ComparisonEqual)
		require.True(t, valid)
		require.Equal(t, tc.match, m(finite).MayMatch())
	}
	mixed, _, ok := MakeScalarZoneMapMatcher(typ, []byte("a"), types.T_varchar.ToType(), 0, nil, types.ComparisonEqual)
	require.True(t, ok)
	require.True(t, mixed(zm).MayMatch(), "mixed carriers retain their conservative behavior")

	// A raw lower bound cannot disprove CHAR equality: trailing spaces are
	// insignificant to SQL even when a different row truncates the maximum.
	charType := types.New(types.T_char, 64, 0)
	charZM := BuildZM(types.T_char, append([]byte("a"), bytes.Repeat([]byte{' '}, 31)...))
	UpdateZM(charZM, append(bytes.Clone(ff), 2))
	match, _, ok = MakeScalarZoneMapMatcher(charType, []byte("a"), charType, 0, nil, types.ComparisonEqual)
	require.True(t, ok)
	require.True(t, match(charZM).MayMatch())
}

func TestByteZoneMapAmbiguousMaxNullableMembership(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xff}, 30)
	value := append(bytes.Clone(prefix), 2)
	zm := legacyObjectSummaryForTest(value)
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_varbinary.ToType())
	defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
	require.NoError(t, vector.AppendBytes(v, nil, true, mp))
	require.NoError(t, vector.AppendBytes(v, value, false, mp))
	require.True(t, zm.AnyIn(v))
	lo, hi := zm.SubVecIn(v)
	require.Equal(t, 0, lo)
	require.Equal(t, v.Length(), hi)
}

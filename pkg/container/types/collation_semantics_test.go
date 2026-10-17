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

package types

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/stretchr/testify/require"
)

func TestUnicodeCollationComparisonKeys(t *testing.T) {
	mb4 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB4UnicodeCI)
	mb3 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI)

	// UCA primary weights are case and accent insensitive for these pairs.
	require.Equal(t, 0, CompareStringValues(mb4, []byte("A"), []byte("a")))
	require.Equal(t, 0, CompareStringValues(mb4, []byte("é"), []byte("e")))
	require.Equal(t, 0, CompareStringValues(mb4, []byte("ß"), []byte("ss")))
	require.Less(t, CompareStringValues(mb4, []byte("a"), []byte("b")), 0)
	legacyChar := NewWithCharset(T_char, 4, 0, CharsetUTF8MB4Bin)
	require.Equal(t, 0, CompareStringValues(legacyChar, []byte("a "), []byte("a")))
	require.NotEqual(t, 0, CompareStringOrderValues(legacyChar, []byte("a "), []byte("a")))

	key, err := CollationKey(mb3, nil, []byte("😀"))
	require.ErrorIs(t, err, collation.ErrRepertoire)
	require.Nil(t, key)

	// The supplementary character is valid in the utf8mb4 domain.
	key, err = CollationKey(mb4, nil, []byte("😀"))
	require.NoError(t, err)
	require.NotEmpty(t, key)
	physical, err := PhysicalCollationKey(mb4, []byte("😀"))
	require.NoError(t, err)
	require.Equal(t, byte(0), physical[0])
	require.Equal(t, key, physical[1:])
	_, err = PhysicalCollationKey(NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI), []byte("😀"))
	require.ErrorIs(t, err, collation.ErrRepertoire)

	invalid := []byte{0x5a, 0xff}
	invalidKey := CollationKeyOrOriginal(mb3, invalid)
	require.Equal(t, byte(1), invalidKey[0])
	require.Equal(t, invalid, invalidKey[1:])
	unicode := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI)
	require.Equal(t, bytes.Compare(invalid, []byte{0xff}),
		CompareStringValues(unicode, invalid, []byte{0xff}))

	// Comparison-only callers can still receive an invalid value from an
	// opaque/binary source. Its comparison class must be chosen per value rather
	// than by falling back for both operands. In particular, equivalent valid
	// values must have the same ordering against every third value.
	require.Equal(t, 0, CompareStringValues(unicode, []byte("A"), []byte("a")))
	leftInvalid := CompareStringValues(unicode, []byte("A"), invalid)
	rightInvalid := CompareStringValues(unicode, []byte("a"), invalid)
	require.Equal(t, leftInvalid, rightInvalid)

	// A malformed value whose bytes happen to equal a valid UCA key must stay
	// in a separate comparison class. The comparator and hash/membership key
	// owner therefore agree on both equality and ordering.
	validKey, err := CollationKey(mb3, nil, []byte("A"))
	require.NoError(t, err)
	rawKey := append([]byte(nil), validKey...)
	require.NotEqual(t, 0, CompareStringValues(unicode, rawKey, []byte("A")))
	require.NotEqual(t, 0, CompareStringValues(unicode, []byte("A"), rawKey))
}

func TestVersionedCollationComparison(t *testing.T) {
	for _, tc := range []struct {
		name    string
		charset uint8
		version uint8
		left    string
		right   string
		want    int
	}{
		{"legacy_case", CharsetUTF8, 0, "A", "a", -1},
		{"legacy_pad", CharsetUTF8, 0, "a ", "a", 1},
		{"general_case", CharsetUTF8, 1, "A", "a", 0},
		{"general_accent", CharsetUTF8, 1, "é", "e", 0},
		{"general_pad", CharsetUTF8, 1, "a ", "a", 0},
		{"general_not_uca", CharsetUTF8, 1, "ß", "ss", -1},
		{"general_nul", CharsetUTF8, 1, "", "\x00", 1},
		{"bin_case", CharsetUTF8MB4Bin, 1, "A", "a", -1},
		{"bin_accent", CharsetUTF8MB4Bin, 1, "é", "e", 1},
		{"bin_pad", CharsetUTF8MB4Bin, 1, "a ", "a", 0},
		{"bin_nul", CharsetUTF8MB4Bin, 1, "", "\x00", 1},
		{"legacy_bin_pad", CharsetUTF8MB4Bin, 0, "a ", "a", 1},
		{"binary_pad", CharsetBinary, 0, "a ", "a", 1},
		{"uca_expansion", CharsetUTF8MB4UnicodeCI, 1, "ß", "ss", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := NewWithCharset(T_varchar, 64, 0, tc.charset)
			typ.CollationVersion = tc.version
			left, right := []byte(tc.left), []byte(tc.right)
			require.Equal(t, tc.want, CompareStringValues(typ, left, right))
			require.Equal(t, -tc.want, CompareStringValues(typ, right, left))
			require.Equal(t, tc.want, CompareStringOrderValues(typ, left, right))
		})
	}
}

func TestCollationDomainRevisionAndAdmission(t *testing.T) {
	for _, charset := range []uint8{CharsetUTF8, CharsetUTF8MB4Bin} {
		typ := NewWithCharset(T_varchar, 64, 0, charset)
		domain, transformed, err := CollationDomainForType(typ)
		require.NoError(t, err)
		require.False(t, transformed)
		require.Equal(t, collation.Raw, domain)
		typ.CollationVersion = 1
		domain, transformed, err = CollationDomainForType(typ)
		require.NoError(t, err)
		require.True(t, transformed)
		require.True(t, HasCollationKey(typ))
		require.NotEqual(t, collation.Raw, domain)
		// 公共比较能力不能意外放开生产 SQL 或物理索引写入。
		require.Error(t, collation.RequireLegacy(uint32(charset), 1, 0))
		_, err = PhysicalCollationKey(typ, []byte("a"))
		require.Error(t, err)
		require.ErrorIs(t, ValidateCollationValue(typ, []byte{0xff}), collation.ErrUTF8)
		require.NoError(t, ValidateCollationValue(typ, []byte("😀")))
		for _, oid := range []T{T_binary, T_varbinary} {
			bytesType := typ
			bytesType.Oid = oid
			raw := []byte{0xff}
			key, err := CollationKey(bytesType, nil, raw)
			require.NoError(t, err)
			require.False(t, HasCollationKey(bytesType))
			require.Same(t, &raw[0], &key[0])
			require.NoError(t, ValidateCollationValue(bytesType, raw))
		}
	}
	for _, typ := range []Type{
		{Oid: T_varchar, Charset: CharsetUTF8, CollationVersion: 2},
		{Oid: T_varchar, Charset: 255},
		{Oid: T_varchar, Charset: CharsetUTF8MB4UnicodeCI},
		{Oid: T_varchar, Charset: uint8(collation.UTF8MB40900AIIdentity), CollationVersion: 1},
		{Oid: T_int64, Charset: CharsetUTF8, CollationVersion: 1},
	} {
		_, _, err := CollationDomainForType(typ)
		require.Error(t, err)
		_, err = CollationKey(typ, nil, []byte("a"))
		require.Error(t, err)
		_, err = PhysicalCollationKey(typ, []byte("a"))
		require.Error(t, err)
	}
}

func TestVersionedCollationKeyOwnership(t *testing.T) {
	for _, charset := range []uint8{CharsetUTF8, CharsetUTF8MB4Bin} {
		typ := NewWithCharset(T_varchar, 64, 0, charset)
		typ.CollationVersion = 1
		for _, value := range [][]byte{nil, []byte(" "), []byte("A "), []byte(" a\x00"), []byte("é😀"), {0xff}, []byte(strings.Repeat("a ", 2048))} {
			original := bytes.Clone(value)
			key := CollationKeyOrOriginal(typ, value)
			size := CollationKeyOrOriginalSize(typ, value)
			require.Len(t, key, size)
			require.Equal(t, original, value)
			require.LessOrEqual(t, size, 4*len(value)+2)
			for _, capacity := range []int{0, size - 1, size, size + 16} {
				dst := make([]byte, 2, capacity+2)
				copy(dst, "xy")
				got := AppendCollationKeyOrOriginal(dst, typ, value)
				require.Equal(t, append([]byte("xy"), key...), got)
				require.Equal(t, original, value)
			}
			buffer := make([]byte, 0, size)
			require.Zero(t, testing.AllocsPerRun(10, func() {
				_ = CollationKeyOrOriginalSize(typ, value)
				_ = AppendCollationKeyOrOriginal(buffer, typ, value)
			}))
		}
	}
	for _, charset := range []uint8{CharsetLegacy, CharsetBinary, CharsetUTF8, CharsetUTF8MB4Bin} {
		typ := NewWithCharset(T_varchar, 64, 0, charset)
		value := []byte("raw ")
		key := CollationKeyOrOriginal(typ, value)
		require.Same(t, &value[0], &key[0])
		require.Zero(t, testing.AllocsPerRun(10, func() {
			_ = CollationKeyOrOriginal(typ, value)
			_ = CompareStringValues(typ, value, value)
		}))
	}
}

//go:noinline
func legacyRawCollationCompare(typ Type, left, right []byte) int {
	// 固定基线的 raw 分支，用于同一进程/工具链内的成本对照；不是旧服务证据。
	if !IsUnicodeCollation(typ.Charset) {
		if typ.Oid == T_char {
			return bytes.Compare(bytes.TrimRight(left, " "), bytes.TrimRight(right, " "))
		}
		return bytes.Compare(left, right)
	}
	return CompareStringValues(typ, left, right)
}

func BenchmarkCollationRawComparison(b *testing.B) {
	typ := NewWithCharset(T_varchar, 256, 0, CharsetUTF8)
	for _, size := range []int{16, 256} {
		left := bytes.Repeat([]byte("a"), size)
		right := bytes.Clone(left)
		right[len(right)-1] = 'b'
		for _, before := range []bool{true, false} {
			b.Run(fmt.Sprintf("size=%d/baseline=%t", size, before), func(b *testing.B) {
				b.ReportAllocs()
				if before {
					for b.Loop() {
						_ = legacyRawCollationCompare(typ, left, right)
					}
				} else {
					for b.Loop() {
						_ = CompareStringValues(typ, left, right)
					}
				}
			})
		}
	}
}

func BenchmarkCollationPADKey(b *testing.B) {
	for _, charset := range []uint8{CharsetUTF8, CharsetUTF8MB4Bin} {
		for _, size := range []int{16, 1024} {
			b.Run(fmt.Sprintf("charset=%d/size=%d", charset, size), func(b *testing.B) {
				typ := NewWithCharset(T_varchar, 1024, 0, charset)
				typ.CollationVersion = 1
				value := bytes.Repeat([]byte("a"), size)
				scratch := make([]byte, 0, CollationKeyOrOriginalSize(typ, value))
				b.ReportAllocs()
				for b.Loop() {
					_ = AppendCollationKeyOrOriginal(scratch, typ, value)
				}
			})
		}
	}
}

func TestMergeStringCharsetKeepsSupplementaryRepertoire(t *testing.T) {
	mb3 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI)
	mb4 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB4UnicodeCI)
	require.Equal(t, CharsetUTF8MB4UnicodeCI,
		MergeStringCharset([]Type{mb4, mb3}, CharsetUTF8))
	require.Equal(t, CharsetUTF8MB4UnicodeCI,
		MergeStringCharset([]Type{mb3, mb4}, CharsetUTF8))
}

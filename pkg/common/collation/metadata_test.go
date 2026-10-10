// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollationIdentityAndCompatibility(t *testing.T) {
	cases := []struct {
		name     string
		id       uint16
		charset  Charset
		identity Identity
		admitted Identity
	}{
		{"binary", 63, CharsetBinary, BinaryIdentity, BinaryIdentity},
		{"ascii_bin", 65, CharsetASCII, ASCIIBinIdentity, 0},
		{"utf8_bin", 83, CharsetUTF8MB3, UTF8BinIdentity, UTF8MB4BinIdentity},
		{"utf8_general_ci", 33, CharsetUTF8MB3, UTF8GeneralCIIdentity, UTF8MB4GeneralCIIdentity},
		{"utf8_unicode_ci", 192, CharsetUTF8MB3, UTF8UnicodeCIIdentity, 0},
		{"utf8mb4_bin", 46, CharsetUTF8MB4, UTF8MB4BinIdentity, UTF8MB4BinIdentity},
		{"utf8mb4_general_ci", 45, CharsetUTF8MB4, UTF8MB4GeneralCIIdentity, UTF8MB4GeneralCIIdentity},
		{"utf8mb4_unicode_ci", 224, CharsetUTF8MB4, UTF8MB4UnicodeCIIdentity, 0},
		{"utf8mb4_0900_ai_ci", 255, CharsetUTF8MB4, UTF8MB40900AIIdentity, UTF8MB4GeneralCIIdentity},
		{"utf8mb4_0900_bin", 309, CharsetUTF8MB4, UTF8MB40900BinIdentity, 0},
		{"gbk_bin", 87, CharsetGBK, GBKBinIdentity, 0},
		{"gbk_chinese_ci", 28, CharsetGBK, GBKChineseCIIdentity, 0},
	}
	require.Len(t, Definitions(), len(cases))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, ok := Lookup(strings.ToUpper(tc.name))
			require.True(t, ok)
			require.Equal(t, tc.id, d.ProtocolID)
			require.Equal(t, tc.charset, d.Charset)
			require.Equal(t, tc.identity, d.Identity)
			byID, ok := LookupProtocol(tc.id)
			require.True(t, ok)
			require.Equal(t, d, byID)
			native, err := EffectiveDefinition(uint32(d.Identity), 1)
			require.NoError(t, err)
			require.Equal(t, d, native)
			identity, ok := ResolveSQL(tc.name)
			require.Equal(t, tc.admitted != 0, ok)
			require.Equal(t, tc.admitted, identity)
			if d.Semantics == UCA400 {
				require.NoError(t, RequireLegacy(uint32(d.Identity), 1, 0))
			} else {
				require.Error(t, RequireLegacy(uint32(d.Identity), 1, 0))
			}
		})
	}
	for _, name := range []string{"utf8", "utf8mb3", "UTF8MB4"} {
		id, ok := ResolveCharset(name)
		require.True(t, ok)
		require.Equal(t, UTF8MB4GeneralCIIdentity, id)
	}
	for _, name := range []string{"utf8mb3_bin", "UTF8_BIN"} {
		id, ok := ResolveSQL(name)
		require.True(t, ok)
		require.Equal(t, UTF8MB4BinIdentity, id)
	}
	for _, name := range []string{"latin1", "ascii", "gbk", "unknown", "utf16", ""} {
		_, ok := ResolveCharset(name)
		require.False(t, ok, name)
	}
	for _, name := range []string{"latin1_bin", "LATIN1_SWEDISH_CI", "ascii_general_ci", "unknown", ""} {
		_, ok := ResolveSQL(name)
		require.False(t, ok, name)
	}
	_, ok := LookupProtocol(65535)
	require.False(t, ok)
	defs := Definitions()
	defs[0].LegacyIdentity = 99
	id, ok := ResolveSQL("binary")
	require.True(t, ok)
	require.Equal(t, BinaryIdentity, id, "callers cannot mutate admission")
}

func TestDDLCompatibilityAliasesDoNotActivateSessionDomains(t *testing.T) {
	for _, name := range []string{"utf32", "UTF32", "utf8mb4", "utf8mb3"} {
		identity, ok := ResolveDDLCharset(name)
		require.True(t, ok)
		require.Equal(t, UTF8MB4GeneralCIIdentity, identity)
	}
	for _, tc := range []struct {
		name string
		want Identity
	}{
		{"utf32_bin", UTF8MB4BinIdentity},
		{"UTF32_GENERAL_CI", UTF8MB4GeneralCIIdentity},
		{"utf8mb4_bin", UTF8MB4BinIdentity},
	} {
		identity, ok := ResolveDDLCollation(tc.name)
		require.True(t, ok)
		require.Equal(t, tc.want, identity)
		require.NoError(t, RequireLegacy(uint32(identity), 0, 0))
	}
	_, ok := ResolveCharset("utf32")
	require.False(t, ok)
	for _, name := range []string{"utf32_bin", "utf32_general_ci", "utf32_unicode_ci"} {
		_, ok := ResolveSQL(name)
		require.False(t, ok)
		_, ok = Lookup(name)
		require.False(t, ok)
	}
	for _, name := range []string{"latin1", "ascii", "utf16", "unknown"} {
		_, ok := ResolveDDLCharset(name)
		require.False(t, ok)
	}
	for _, name := range []string{"utf32_unicode_ci", "utf8mb4_unicode_ci", "unknown"} {
		_, ok := ResolveDDLCollation(name)
		require.False(t, ok)
	}
	require.Len(t, Definitions(), 12)
}

func TestCollationMetadataClosedDomain(t *testing.T) {
	for _, pair := range [][2]uint32{{0, 1}, {4, 0}, {13, 1}, {255, 0}, {257, 0}, {3, 256}, {3, 2}} {
		require.Error(t, ValidateMetadata(pair[0], pair[1]), "%v", pair)
		require.Error(t, RequireLegacy(pair[0], pair[1], 0))
	}
	for id := uint32(0); id <= 3; id++ {
		require.NoError(t, RequireLegacy(id, 0, 0))
		require.Error(t, RequireLegacy(id, 0, 1))
		require.Error(t, RequireLegacy(id, 0, 256))
	}
	require.NoError(t, ValidateKeyFormat(1))
	for c, expected := range map[Charset]int32{CharsetUnspecified: 0, CharsetBinary: 1, CharsetASCII: 1, CharsetUTF8MB3: 3, CharsetUTF8MB4: 4, CharsetGBK: 2} {
		require.Equal(t, expected, c.MaxBytes())
		if c == CharsetUnspecified {
			require.Empty(t, c.Name())
		} else {
			require.NotEmpty(t, c.Name())
		}
	}
	legacy, err := EffectiveDefinition(0, 0)
	require.NoError(t, err)
	require.Equal(t, LegacyBytes, legacy.Semantics)
	require.Equal(t, float64(0), testing.AllocsPerRun(100, func() {
		if err := RequireLegacy(3, 0, 0); err != nil {
			panic(err)
		}
	}))
}

func TestAdvertisedCollationsUseEffectiveSemantics(t *testing.T) {
	rows := Advertised()
	require.Len(t, rows, 8)
	for _, row := range rows {
		revision := uint8(RevisionLegacy)
		id := row.Identity
		if row.LegacyIdentity == LegacyIdentity {
			revision = RevisionV1
		} else {
			id = row.LegacyIdentity
		}
		effective, err := EffectiveDefinition(uint32(id), uint32(revision))
		require.NoError(t, err)
		require.Equal(t, effective.Semantics, row.Semantics)
		require.Equal(t, effective.PadSpace, row.PadSpace)
		if row.LegacyIdentity != LegacyIdentity {
			admitted, ok := ResolveSQL(row.Name)
			require.True(t, ok)
			require.Equal(t, row.LegacyIdentity, admitted)
		}
		if row.Name == "utf8mb4_0900_ai_ci" {
			native, _ := Lookup(row.Name)
			require.False(t, native.PadSpace)
			require.True(t, row.PadSpace)
			require.NotEqual(t, native.Semantics, row.Semantics)
		}
	}
}

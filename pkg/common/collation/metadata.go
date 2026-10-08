// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collation

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Identity is the compact schema identity, NOT a MySQL protocol collation ID.
// Values 0..3 are persisted by released binaries; 4..5 retain the PR2 allocation.
// 255 is reserved for an existing numeric CAST marker outside string types.
type Identity uint8

const (
	LegacyIdentity Identity = iota
	BinaryIdentity
	UTF8MB4BinIdentity
	UTF8MB4GeneralCIIdentity
	UTF8MB40900AIIdentity
	UTF8MB40900BinIdentity
	ASCIIBinIdentity
	UTF8BinIdentity
	UTF8GeneralCIIdentity
	UTF8UnicodeCIIdentity
	UTF8MB4UnicodeCIIdentity
	GBKBinIdentity
	GBKChineseCIIdentity
)

const (
	RevisionLegacy  uint8  = 0
	RevisionV1      uint8  = 1
	KeyFormatLegacy uint32 = 0
	KeyFormatV1     uint32 = 1
)

type Charset uint8

const (
	CharsetUnspecified Charset = iota
	CharsetBinary
	CharsetASCII
	CharsetUTF8MB3
	CharsetUTF8MB4
	CharsetGBK
)

func (c Charset) Name() string {
	switch c {
	case CharsetBinary:
		return "binary"
	case CharsetASCII:
		return "ascii"
	case CharsetUTF8MB3:
		return "utf8"
	case CharsetUTF8MB4:
		return "utf8mb4"
	case CharsetGBK:
		return "gbk"
	default:
		return ""
	}
}

func (c Charset) MaxBytes() int32 {
	switch c {
	case CharsetBinary, CharsetASCII:
		return 1
	case CharsetUTF8MB3:
		return 3
	case CharsetUTF8MB4:
		return 4
	case CharsetGBK:
		return 2
	default:
		return 0
	}
}

// Semantics names the effective comparison contract independently of encoding.
type Semantics uint8

const (
	LegacyBytes Semantics = iota
	BinaryBytes
	LegacyBin
	LegacyGeneralCI
	BinPadSpace
	GeneralCI
	UCA400
	UCA900AI
	UCA900Bin
	GBKChineseCI
)

// Definition is immutable by convention: all public accessors return copies.
// LegacyIdentity identifies the CURRENT admission policy for this spelling;
// zero means known but disabled, not an invitation to fall back to legacy.
// Native identity and semantics remain available without enabling them in SQL.
type Definition struct {
	Identity       Identity
	Name           string
	Charset        Charset
	ProtocolID     uint16
	Semantics      Semantics
	PadSpace       bool
	Default        bool
	SortLen        int32
	LegacyIdentity Identity
}

// This is the only name/protocol/capability definition. Compatibility aliases
// live alongside their native identities, not in another registry or per key.
var definitions = [...]Definition{
	{Identity: BinaryIdentity, Name: "binary", Charset: CharsetBinary, ProtocolID: 63, Semantics: BinaryBytes, Default: true, SortLen: 1, LegacyIdentity: BinaryIdentity},
	{Identity: UTF8MB4BinIdentity, Name: "utf8mb4_bin", Charset: CharsetUTF8MB4, ProtocolID: 46, Semantics: BinPadSpace, PadSpace: true, SortLen: 1, LegacyIdentity: UTF8MB4BinIdentity},
	{Identity: UTF8MB4GeneralCIIdentity, Name: "utf8mb4_general_ci", Charset: CharsetUTF8MB4, ProtocolID: 45, Semantics: GeneralCI, PadSpace: true, Default: true, SortLen: 1, LegacyIdentity: UTF8MB4GeneralCIIdentity},
	{Identity: UTF8MB40900AIIdentity, Name: "utf8mb4_0900_ai_ci", Charset: CharsetUTF8MB4, ProtocolID: 255, Semantics: UCA900AI, SortLen: 0, LegacyIdentity: UTF8MB4GeneralCIIdentity},
	{Identity: UTF8MB40900BinIdentity, Name: "utf8mb4_0900_bin", Charset: CharsetUTF8MB4, ProtocolID: 309, Semantics: UCA900Bin, SortLen: 1},
	{Identity: ASCIIBinIdentity, Name: "ascii_bin", Charset: CharsetASCII, ProtocolID: 65, Semantics: BinPadSpace, PadSpace: true, Default: true, SortLen: 1},
	{Identity: UTF8BinIdentity, Name: "utf8_bin", Charset: CharsetUTF8MB3, ProtocolID: 83, Semantics: BinPadSpace, PadSpace: true, SortLen: 1, LegacyIdentity: UTF8MB4BinIdentity},
	{Identity: UTF8GeneralCIIdentity, Name: "utf8_general_ci", Charset: CharsetUTF8MB3, ProtocolID: 33, Semantics: GeneralCI, PadSpace: true, Default: true, SortLen: 1, LegacyIdentity: UTF8MB4GeneralCIIdentity},
	{Identity: UTF8UnicodeCIIdentity, Name: "utf8_unicode_ci", Charset: CharsetUTF8MB3, ProtocolID: 192, Semantics: UCA400, PadSpace: true, SortLen: 8},
	{Identity: UTF8MB4UnicodeCIIdentity, Name: "utf8mb4_unicode_ci", Charset: CharsetUTF8MB4, ProtocolID: 224, Semantics: UCA400, PadSpace: true, SortLen: 8},
	{Identity: GBKBinIdentity, Name: "gbk_bin", Charset: CharsetGBK, ProtocolID: 87, Semantics: BinPadSpace, PadSpace: true, SortLen: 1},
	{Identity: GBKChineseCIIdentity, Name: "gbk_chinese_ci", Charset: CharsetGBK, ProtocolID: 28, Semantics: GBKChineseCI, PadSpace: true, Default: true, SortLen: 1},
}

func Definitions() []Definition {
	return append([]Definition(nil), definitions[:]...)
}

func CanonicalCharsetName(name string) string {
	name = strings.ToLower(name)
	if name == "utf8mb3" {
		return "utf8"
	}
	return name
}

func Lookup(name string) (Definition, bool) {
	name = strings.ToLower(name)
	if strings.HasPrefix(name, "utf8mb3_") {
		name = "utf8_" + strings.TrimPrefix(name, "utf8mb3_")
	}
	for _, d := range definitions {
		if d.Name == name {
			return d, true
		}
	}
	return Definition{}, false
}

func LookupProtocol(id uint16) (Definition, bool) {
	for _, d := range definitions {
		if d.ProtocolID == id {
			return d, true
		}
	}
	return Definition{}, false
}

// ResolveSQL retains MO's historical utf8/utf8mb3 -> utf8mb4 and 0900 ->
// general-ci aliases. Lookup, in contrast, describes the native named domain.
func ResolveSQL(name string) (Identity, bool) {
	d, ok := Lookup(name)
	return d.LegacyIdentity, ok && d.LegacyIdentity != LegacyIdentity
}

// ResolveDDLCollation preserves DDL-only compatibility spellings without
// advertising a native UTF-32 domain or enabling them for session requests.
func ResolveDDLCollation(name string) (Identity, bool) {
	switch strings.ToLower(name) {
	case "utf32_bin":
		name = "utf8mb4_bin"
	case "utf32_general_ci":
		name = "utf8mb4_general_ci"
	}
	return ResolveSQL(name)
}

func ResolveDDLCharset(name string) (Identity, bool) {
	if strings.EqualFold(name, "utf32") {
		name = "utf8mb4"
	}
	return ResolveCharset(name)
}

func ResolveCharset(name string) (Identity, bool) {
	name = CanonicalCharsetName(name)
	for _, d := range definitions {
		if d.Default && d.Charset.Name() == name {
			return d.LegacyIdentity, d.LegacyIdentity != LegacyIdentity
		}
	}
	return LegacyIdentity, false
}

// EffectiveDefinition validates the CLOSED metadata domain before narrowing
// wire values. No unknown identity/revision may silently become old metadata.
func EffectiveDefinition(identity, revision uint32) (Definition, error) {
	if revision > uint32(RevisionV1) || identity > uint32(GBKChineseCIIdentity) ||
		(identity == uint32(LegacyIdentity) && revision != 0) ||
		(identity > uint32(UTF8MB4GeneralCIIdentity) && revision == 0) {
		return Definition{}, moerr.NewNotSupportedNoCtxf("unknown collation identity/revision %d/%d", identity, revision)
	}
	if identity == uint32(LegacyIdentity) {
		return Definition{Semantics: LegacyBytes}, nil
	}
	d := definitions[identity-1]
	if revision == 0 {
		switch Identity(identity) {
		case UTF8MB4BinIdentity:
			d.Semantics = LegacyBin
		case UTF8MB4GeneralCIIdentity:
			d.Semantics = LegacyGeneralCI
		}
	}
	return d, nil
}

func ValidateMetadata(identity, revision uint32) error {
	_, err := EffectiveDefinition(identity, revision)
	return err
}

func ValidateKeyFormat(format uint32) error {
	if format > KeyFormatV1 {
		return moerr.NewNotSupportedNoCtxf("unknown collation key format %d", format)
	}
	return nil
}

// RequireLegacy is a production fence, not a feature switch. C11 owns future
// cluster activation; knowing a backend or protocol ID cannot enable a domain.
func RequireLegacy(identity, revision, format uint32) error {
	if err := ValidateMetadata(identity, revision); err != nil {
		return err
	}
	if err := ValidateKeyFormat(format); err != nil {
		return err
	}
	if identity > uint32(UTF8MB4GeneralCIIdentity) || revision != 0 || format != 0 {
		return moerr.NewNotSupportedNoCtx("versioned collations and key formats are disabled until SQL, storage and upgrade gates pass")
	}
	return nil
}

// Advertised returns compatibility spellings with EFFECTIVE padding/semantics.
// Charset and ProtocolID describe the spelling, not the admitted runtime type.
func Advertised() []Definition {
	result := make([]Definition, 0, 6)
	for _, d := range definitions {
		if d.LegacyIdentity == LegacyIdentity {
			continue
		}
		effective, _ := EffectiveDefinition(uint32(d.LegacyIdentity), 0)
		d.Semantics, d.PadSpace, d.SortLen = effective.Semantics, effective.PadSpace, effective.SortLen
		result = append(result, d)
	}
	return result
}

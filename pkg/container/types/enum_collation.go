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
	"context"
	"encoding/binary"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	EnumCollationLegacy           uint32 = 0
	EnumCollationUTF8Bin          uint32 = 2
	EnumCollationGeneralCI        uint32 = 3
	EnumCollationUTF8UnicodeCI    uint32 = 9 // Reserved; utf8mb3 ENUM conversion is not supported.
	EnumCollationUTF8MB4UnicodeCI uint32 = 10
)

func ValidateEnumCollation(id uint32) error {
	switch id {
	case EnumCollationLegacy, EnumCollationUTF8Bin, EnumCollationGeneralCI, EnumCollationUTF8MB4UnicodeCI:
		return nil
	default:
		return moerr.NewInvalidInputNoCtxf("unsupported ENUM collation %d", id)
	}
}

func (t Type) GetEnumCollation() uint32 {
	if t.Oid != T_enum {
		return 0
	}
	return uint32(t.Charset)
}

// EnumMemberKey uses non-padding weights. Input-only ASCII space handling is
// performed by the lookup owner, not by the key's stored-member side.
func EnumMemberKey(id uint32, value string) (string, error) {
	switch id {
	case EnumCollationUTF8Bin:
		return value, nil
	case EnumCollationGeneralCI:
		if len(value) > math.MaxInt/4 {
			return "", moerr.NewInvalidInputNoCtx("ENUM member key is too large")
		}
		out := make([]byte, 0, len(value)*4)
		for len(value) != 0 {
			w, n := collation.NextGeneralCIWeight([]byte(value))
			out = binary.BigEndian.AppendUint32(out, w)
			value = value[n:]
		}
		return string(out), nil
	case EnumCollationUTF8MB4UnicodeCI:
		// MySQL UCA4 ENUM comparison stops at the first malformed UTF-8 byte.
		// Apply the same rule to both stored labels and input before weighting.
		if !utf8.ValidString(value) {
			for i := 0; i < len(value); {
				r, size := utf8.DecodeRuneInString(value[i:])
				if r == utf8.RuneError && size == 1 {
					value = value[:i]
					break
				}
				i += size
			}
		}
		key, err := collation.EnumMemberWeights(collation.UTF8MB4UnicodeCI, []byte(value))
		return string(key), err
	default:
		return "", moerr.NewInvalidInputNoCtxf("unsupported ENUM collation %d", id)
	}
}

// EnumCollatedIndex is scoped to a constant evaluation batch.
type EnumCollatedIndex struct {
	members  []string
	identity uint32
	all      map[string]Enum
	padded   map[string]Enum
}

func NewEnumCollatedIndex(members []string, id uint32) (*EnumCollatedIndex, error) {
	if err := ValidateEnumCollation(id); err != nil || id == EnumCollationLegacy {
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInvalidInputNoCtx("legacy ENUM lookup has its own index")
	}
	if err := validateEnumMemberCount(len(members)); err != nil {
		return nil, err
	}
	index := &EnumCollatedIndex{members: members, identity: id, all: make(map[string]Enum, len(members)), padded: make(map[string]Enum)}
	for i, member := range members {
		key, err := EnumMemberKey(id, member)
		if err != nil {
			return nil, err
		}
		if _, ok := index.all[key]; !ok {
			index.all[key] = Enum(i + 1)
		}
		if strings.HasSuffix(member, " ") {
			if _, ok := index.padded[key]; !ok {
				index.padded[key] = Enum(i + 1)
			}
		}
	}
	return index, nil
}

func (index *EnumCollatedIndex) Parse(name string) (Enum, bool, error) {
	trimmed := strings.TrimRight(name, " ")
	if trimmed != name {
		key, err := EnumMemberKey(index.identity, name)
		if err != nil {
			return 0, false, err
		}
		if value := index.padded[key]; value != 0 {
			return value, false, nil
		}
	}
	key, err := EnumMemberKey(index.identity, trimmed)
	if err != nil {
		return 0, false, err
	}
	if value := index.all[key]; value != 0 {
		return value, false, nil
	}
	return ParseEnumNumericText(trimmed, len(index.members))
}

func ParseEnumCollatedAssignment(definition, name string, id uint32) (Enum, bool, error) {
	if err := ValidateEnumCollation(id); err != nil {
		return 0, false, err
	}
	if id == EnumCollationLegacy {
		return ParseEnumAssignment(definition, name)
	}
	members := strings.Split(definition, ",")
	if err := validateEnumMemberCount(len(members)); err != nil {
		return 0, false, err
	}
	match := func(value string, paddedOnly bool) (Enum, error) {
		key, err := EnumMemberKey(id, value)
		if err != nil {
			return 0, err
		}
		for i, member := range members {
			if paddedOnly && !strings.HasSuffix(member, " ") {
				continue
			}
			memberKey, err := EnumMemberKey(id, member)
			if err != nil {
				return 0, err
			}
			if key == memberKey {
				return Enum(i + 1), nil
			}
		}
		return 0, nil
	}
	trimmed := strings.TrimRight(name, " ")
	if trimmed != name {
		v, err := match(name, true)
		if err != nil || v != 0 {
			return v, false, err
		}
	}
	v, err := match(trimmed, false)
	if err != nil || v != 0 {
		return v, false, err
	}
	return ParseEnumNumericText(trimmed, len(members))
}

// ParseEnumCollated keeps the existing strict conversion API for consumers
// that do not own SQL-mode warnings, such as external scan validation.
func ParseEnumCollated(definition, name string, id uint32) (Enum, error) {
	return ParseEnumCollatedWithContext(defaultParseContext, definition, name, id)
}

func ParseEnumCollatedWithContext(ctx context.Context, definition, name string, id uint32) (Enum, error) {
	if id == EnumCollationLegacy {
		return ParseEnumWithContext(ctx, definition, name)
	}
	value, truncated, err := ParseEnumCollatedAssignment(definition, name, id)
	if err != nil {
		return 0, err
	}
	if truncated {
		return 0, moerr.NewInternalErrorf(parseErrorContext(ctx), "convert to MySQL enum failed: item %s is not in enum %v", name, strings.Split(definition, ","))
	}
	return value, nil
}

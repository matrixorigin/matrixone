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
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// These values describe ENUM member lookup, not the physical string type.
const (
	EnumCollationLegacy    uint32 = 0
	EnumCollationUTF8Bin   uint32 = 2
	EnumCollationGeneralCI uint32 = 3
)

func ValidateEnumCollation(id uint32) error {
	switch id {
	case EnumCollationLegacy, EnumCollationUTF8Bin, EnumCollationGeneralCI:
		return nil
	default:
		return moerr.NewInvalidInputNoCtxf("unsupported ENUM collation %d", id)
	}
}

// GetEnumCollation only interprets the physical charset byte for ENUM values.
func (t Type) GetEnumCollation() uint32 {
	if t.Oid != T_enum {
		return 0
	}
	return uint32(t.Charset)
}

// SetEnumCollation retains invalid wide tags as invalid instead of narrowing
// them into a supported identity. Admission must call ValidateEnumCollation.
func (t *Type) SetEnumCollation(id uint32) {
	if t.Oid != T_enum {
		return
	}
	if id > math.MaxUint8 {
		t.Charset = math.MaxUint8
	} else {
		t.Charset = uint8(id)
	}
}

// EnumMemberKey is a non-padding key for a declared member or input value.
// Callers handle the input-only trailing-space rule before constructing keys.
func EnumMemberKey(id uint32, value string) (string, error) {
	switch id {
	case EnumCollationUTF8Bin:
		return value, nil
	case EnumCollationGeneralCI:
		if len(value) > math.MaxInt/4 {
			return "", moerr.NewInvalidInputNoCtx("ENUM member key is too large")
		}
		out := make([]byte, 0, len(value)*4)
		for len(value) > 0 {
			weight, size := collation.NextGeneralCIWeight([]byte(value))
			out = binary.BigEndian.AppendUint32(out, weight)
			value = value[size:]
		}
		return string(out), nil
	default:
		return "", moerr.NewInvalidInputNoCtxf("unsupported ENUM collation %d", id)
	}
}

// ParseCollated resolves an input against the ordered domain. An explicitly
// padded MO member retains precedence for a matching padded input. Otherwise
// only the input's trailing ASCII spaces are discarded before member lookup.
func (elems EnumMembers) ParseCollated(name string, id uint32) (Enum, error) {
	index, err := elems.ParseCollatedName(name, id)
	if err != nil || index != 0 {
		return index, err
	}
	input := name
	if id != EnumCollationLegacy {
		input = strings.TrimRight(input, " ")
	}
	num, err := strconv.ParseUint(input, 0, 64)
	if err == nil {
		if id == EnumCollationLegacy {
			return parseEnumValue(elems, uint16(num))
		}
		return parseCollatedEnumOrdinal(elems, num)
	}
	return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: item %s is not in enum %v", name, elems)
}

// ParseCollatedName matches declared text only; callers retain their own
// numeric coercion and diagnostic policies when no member matched.
func (elems EnumMembers) ParseCollatedName(name string, id uint32) (Enum, error) {
	if err := ValidateEnumCollation(id); err != nil {
		return 0, err
	}
	if id == EnumCollationLegacy {
		return elems.ParseName(name)
	}
	match := func(value string, paddedOnly bool) (Enum, error) {
		key, err := EnumMemberKey(id, value)
		if err != nil {
			return 0, err
		}
		for i, member := range elems {
			if paddedOnly && !strings.HasSuffix(member, " ") {
				continue
			}
			memberKey, err := EnumMemberKey(id, member)
			if err != nil {
				return 0, err
			}
			if memberKey == key {
				return Enum(i + 1), nil
			}
		}
		return 0, nil
	}
	trimmed := strings.TrimRight(name, " ")
	if trimmed != name {
		index, err := match(name, true)
		if err != nil || index != 0 {
			return index, err
		}
	}
	index, err := match(trimmed, false)
	return index, err
}

// EnumCollatedIndex owns keys for one decoded, constant domain. It is scoped to
// an evaluation batch; it must not be shared with a mutable definition.
type EnumCollatedIndex struct {
	members  EnumMembers
	identity uint32
	all      map[string]Enum
	padded   map[string]Enum
}

func NewEnumCollatedIndex(members EnumMembers, id uint32) (*EnumCollatedIndex, error) {
	if id == EnumCollationLegacy {
		return nil, moerr.NewInvalidInputNoCtx("legacy ENUM lookup has its own index")
	}
	if err := ValidateEnumCollation(id); err != nil {
		return nil, err
	}
	index := &EnumCollatedIndex{members: members, identity: id, all: make(map[string]Enum, len(members)), padded: make(map[string]Enum)}
	for i, member := range members {
		key, err := EnumMemberKey(id, member)
		if err != nil {
			return nil, err
		}
		ordinal := Enum(i + 1)
		if _, ok := index.all[key]; !ok {
			index.all[key] = ordinal
		}
		if strings.HasSuffix(member, " ") {
			if _, ok := index.padded[key]; !ok {
				index.padded[key] = ordinal
			}
		}
	}
	return index, nil
}

func (index *EnumCollatedIndex) Parse(name string) (Enum, error) {
	trimmed := strings.TrimRight(name, " ")
	if trimmed != name {
		key, err := EnumMemberKey(index.identity, name)
		if err != nil {
			return 0, err
		}
		if value := index.padded[key]; value != 0 {
			return value, nil
		}
	}
	key, err := EnumMemberKey(index.identity, trimmed)
	if err != nil {
		return 0, err
	}
	if value := index.all[key]; value != 0 {
		return value, nil
	}
	num, err := strconv.ParseUint(trimmed, 0, 64)
	if err == nil {
		return parseCollatedEnumOrdinal(index.members, num)
	}
	return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: item %s is not in enum %v", name, index.members)
}

// Validate the original number before narrowing to ENUM's 16-bit storage.
func parseCollatedEnumOrdinal(members EnumMembers, number uint64) (Enum, error) {
	if number == 0 || number > MaxEnumLen || number > uint64(len(members)) {
		return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: number %d overflow enum boundary [1, %d]", number, len(members))
	}
	return Enum(number), nil
}

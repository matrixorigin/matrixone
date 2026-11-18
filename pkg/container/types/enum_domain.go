// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	EnumEncodingLegacy uint32 = iota
	EnumEncodingFramed
)

func ValidateEnumEncoding(encoding uint32) error {
	if encoding > EnumEncodingFramed {
		return moerr.NewInvalidInputNoCtxf("unsupported ENUM member encoding %d", encoding)
	}
	return nil
}

func (t Type) GetEnumEncoding() uint32 {
	return uint32(t.enumEncoding)
}

// SetEnumEncoding preserves unsupported tags as unsupported, including plan tags
// wider than the native field. Complete-domain owners validate before use/write.
func (t *Type) SetEnumEncoding(encoding uint32) {
	if encoding > math.MaxUint8 {
		t.enumEncoding = math.MaxUint8
	} else {
		t.enumEncoding = uint8(encoding)
	}
}

// EncodeEnumMembers writes a new definition using the tagged member format.
func EncodeEnumMembers(members []string) (string, uint32, error) {
	if len(members) == 0 || len(members) > MaxEnumLen {
		return "", 0, moerr.NewInvalidInputNoCtxf("invalid ENUM member count %d", len(members))
	}
	size := uint64(len(strconv.Itoa(len(members))) + 1)
	for _, member := range members {
		size += uint64(len(strconv.Itoa(len(member)))+1) + uint64(len(member))
		if size > math.MaxUint32 {
			return "", 0, moerr.NewInvalidInputNoCtx("ENUM member definition is too large")
		}
	}
	if size > uint64(^uint(0)>>1) {
		return "", 0, moerr.NewInvalidInputNoCtx("ENUM member definition exceeds addressable size")
	}
	var out strings.Builder
	out.Grow(int(size))
	out.WriteString(strconv.Itoa(len(members)))
	out.WriteByte('#')
	for _, member := range members {
		out.WriteString(strconv.Itoa(len(member)))
		out.WriteByte(':')
		out.WriteString(member)
	}
	return out.String(), EnumEncodingFramed, nil
}

// DecodeEnumMembers decodes a complete domain, including a legacy singleton
// empty member. Generic physical ENUM types do not by themselves define a domain.
func DecodeEnumMembers(payload string, encoding uint32) (EnumMembers, error) {
	if err := ValidateEnumEncoding(encoding); err != nil {
		return nil, err
	}
	if encoding == EnumEncodingLegacy {
		count, err := legacyEnumMemberCount(payload)
		if err != nil {
			return nil, err
		}
		// The checked count gives the exact number of comma boundaries.
		// Read those byte boundaries directly, preserving empty members.
		members := make(EnumMembers, count)
		for i := 0; i < count-1; i++ {
			end := strings.IndexByte(payload, ',')
			members[i] = payload[:end]
			payload = payload[end+1:]
		}
		members[count-1] = payload
		return members, nil
	}
	count, rest, err := readEnumDomainSize(payload, '#', MaxEnumLen)
	if err != nil || count == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid ENUM member count framing")
	}
	// Even an empty member requires "0:". Bound allocation by actual input.
	if count > uint64(len(rest)/2) {
		return nil, moerr.NewInvalidInputNoCtx("truncated ENUM member definition")
	}
	members := make(EnumMembers, 0, int(count))
	for range int(count) {
		length, tail, err := readEnumDomainSize(rest, ':', math.MaxUint32)
		if err != nil {
			return nil, err
		}
		if length > uint64(len(tail)) {
			return nil, moerr.NewInvalidInputNoCtx("truncated ENUM member value")
		}
		members = append(members, tail[:int(length)])
		rest = tail[int(length):]
	}
	if rest != "" {
		return nil, moerr.NewInvalidInputNoCtx("trailing bytes in ENUM member definition")
	}
	return members, nil
}

func readEnumDomainSize(input string, delimiter byte, limit uint64) (uint64, string, error) {
	var value uint64
	for i := 0; i < len(input); i++ {
		c := input[i]
		if c == delimiter {
			if i == 0 || (i > 1 && input[0] == '0') {
				break
			}
			return value, input[i+1:], nil
		}
		if c < '0' || c > '9' {
			break
		}
		digit := uint64(c - '0')
		if value > (limit-digit)/10 {
			break
		}
		value = value*10 + digit
		if value > limit {
			break
		}
	}
	return 0, "", moerr.NewInvalidInputNoCtx("invalid ENUM member length framing")
}

// EqualEnumMembers compares complete domain definitions, not physical types.
func EqualEnumMembers(left string, leftEncoding uint32, right string, rightEncoding uint32) (bool, error) {
	if err := ValidateEnumEncoding(leftEncoding); err != nil {
		return false, err
	}
	if err := ValidateEnumEncoding(rightEncoding); err != nil {
		return false, err
	}
	if leftEncoding == EnumEncodingLegacy && rightEncoding == EnumEncodingLegacy && left == right {
		if _, err := legacyEnumMemberCount(left); err != nil {
			return false, err
		}
		return true, nil
	}
	a, err := DecodeEnumMembers(left, leftEncoding)
	if err != nil {
		return false, err
	}
	if leftEncoding == rightEncoding && left == right {
		return true, nil
	}
	b, err := DecodeEnumMembers(right, rightEncoding)
	if err != nil || len(a) != len(b) {
		return false, err
	}
	for i := range a {
		if a[i] != b[i] {
			return false, nil
		}
	}
	return true, nil
}

// EnumMembersEqual is the predicate form used by schema compatibility checks.
// Invalid definitions never compare equal.
func EnumMembersEqual(left string, leftEncoding uint32, right string, rightEncoding uint32) bool {
	equal, err := EqualEnumMembers(left, leftEncoding, right, rightEncoding)
	return err == nil && equal
}

func legacyEnumMemberCount(payload string) (int, error) {
	count := strings.Count(payload, ",")
	if uint64(len(payload)) > math.MaxUint32 || count >= MaxEnumLen {
		return 0, moerr.NewInvalidInputNoCtx("ENUM member definition exceeds capacity")
	}
	return count + 1, nil
}

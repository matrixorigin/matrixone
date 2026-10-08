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

	"github.com/matrixorigin/matrixone/pkg/common/collation"
)

const (
	// Collation keys used by equality/hash consumers carry a small domain
	// marker.  A raw fallback for malformed/repertoire-invalid input must not
	// be able to collide with a valid UCA weight string.
	validCollationKeyTag   byte = 0
	invalidCollationKeyTag byte = 1
)

// CollationDomainForCharset resolves the executable key domain for a text
// identity. Keeping this mapping beside Type prevents SQL consumers from
// silently treating utf8*_unicode_ci as the existing general_ci class.
func CollationDomainForCharset(charset uint8) (collation.Domain, bool) {
	switch charset {
	case CharsetUTF8MB4UnicodeCI:
		return collation.UTF8MB4UnicodeCI, true
	case CharsetUTF8MB3UnicodeCI:
		return collation.UTF8MB3UnicodeCI, true
	default:
		return collation.Raw, false
	}
}

// CollationKey returns the opaque comparison key for a native Unicode
// collation. Non-Unicode identities are returned unchanged so callers can use
// this helper in shared serialization paths without changing legacy bytes.
func CollationKey(charset uint8, scratch, value []byte) ([]byte, error) {
	domain, ok := CollationDomainForCharset(charset)
	if !ok {
		return value, nil
	}
	return domain.Key(scratch, value)
}

// CollationKeyOrOriginal returns the canonical equality/hash/physical-key
// representation for a native Unicode collation. The leading domain marker
// keeps a malformed or repertoire-invalid raw fallback disjoint from valid UCA
// weight bytes. Callers that need generic SERIAL's lossless value contract must
// keep using the original bytes instead.
func CollationKeyOrOriginal(charset uint8, value []byte) []byte {
	if !IsUnicodeCollation(charset) {
		return value
	}
	key, err := CollationKey(charset, nil, value)
	if err != nil {
		out := make([]byte, 1+len(value))
		out[0] = invalidCollationKeyTag
		copy(out[1:], value)
		return out
	}
	out := make([]byte, 1+len(key))
	out[0] = validCollationKeyTag
	copy(out[1:], key)
	return out
}

// CompareStringValues compares two values in one resolved text identity. The
// native UCA domains compare their transformed keys; all other identities keep
// the caller's historical byte order. Invalid native input uses a deterministic
// per-value fallback because vector comparator interfaces cannot return errors.
//
// Valid and invalid values are separate comparison classes. Choosing the raw
// byte order only when both values are invalid keeps the fallback independent
// of the other operand, so an invalid value cannot change the ordering of two
// otherwise equal valid values.
func CompareStringValues(typ Type, left, right []byte) int {
	if !IsUnicodeCollation(typ.Charset) {
		if typ.Oid == T_char {
			return bytes.Compare(bytes.TrimRight(left, " "), bytes.TrimRight(right, " "))
		}
		return bytes.Compare(left, right)
	}
	leftKey, leftErr := CollationKey(typ.Charset, nil, left)
	rightKey, rightErr := CollationKey(typ.Charset, nil, right)
	if leftErr != nil {
		if rightErr == nil {
			return 1
		}
		return bytes.Compare(left, right)
	}
	if rightErr != nil {
		return -1
	}
	return bytes.Compare(leftKey, rightKey)
}

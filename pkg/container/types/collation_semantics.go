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

// PhysicalCollationKey returns the tagged key used by a versioned physical
// index boundary.  Unlike CollationKeyOrOriginal, it never turns an invalid
// or out-of-repertoire value into a tagged raw-value fallback.  Physical key
// producers, probes and locks must either use the same canonical bytes or
// fail before publishing a row; a fallback would create two identities for
// one relation key format.
func PhysicalCollationKey(typ Type, value []byte) ([]byte, error) {
	if !IsUnicodeCollation(typ.Charset) || !isUnicodeStringType(typ.Oid) {
		return value, nil
	}
	key, err := CollationKey(typ.Charset, nil, value)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 1+len(key))
	out[0] = validCollationKeyTag
	copy(out[1:], key)
	return out, nil
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

// ValidateCollationValue checks the admission contract for a typed native
// Unicode string. Unlike CollationKeyOrOriginal, which deliberately retains a
// tagged fallback for comparison-only callers, typed values must not carry
// malformed UTF-8 or a value outside the charset repertoire. Rejecting those
// bytes at the typed write boundary prevents a binary payload from aliasing a
// valid UCA key after a later vector loses its collation metadata.
func ValidateCollationValue(typ Type, value []byte) error {
	if !IsUnicodeCollation(typ.Charset) || !isUnicodeStringType(typ.Oid) {
		return nil
	}
	_, err := CollationKey(typ.Charset, nil, value)
	return err
}

func isUnicodeStringType(oid T) bool {
	switch oid {
	case T_char, T_varchar, T_blob, T_text:
		return true
	default:
		// BINARY and VARBINARY are opaque byte domains even when a caller
		// happens to carry a non-zero charset in their physical metadata.
		return false
	}
}

// CompareStringValues compares two values in one resolved text identity. The
// native UCA domains compare the same tagged representation used by hash and
// membership consumers; all other identities keep the caller's historical byte
// order. Using one canonical representation is important for values that the
// native domain cannot encode: a raw fallback must not compare equal to a valid
// UCA key, and its ordering must not depend on the other operand.
func CompareStringValues(typ Type, left, right []byte) int {
	if !IsUnicodeCollation(typ.Charset) {
		if typ.Oid == T_char {
			return bytes.Compare(bytes.TrimRight(left, " "), bytes.TrimRight(right, " "))
		}
		return bytes.Compare(left, right)
	}
	return bytes.Compare(
		CollationKeyOrOriginal(typ.Charset, left),
		CollationKeyOrOriginal(typ.Charset, right),
	)
}

// CompareStringOrderValues is the comparator used by SQL ORDER BY consumers.
// Native Unicode identities use the same UCA key relation as scalar equality.
// Legacy CHAR keeps the historical raw-byte order so TopN/merge ordering stays
// aligned with full sort and peer partitioning; physical/storage callers keep
// their existing raw-byte contract as well.
func CompareStringOrderValues(typ Type, left, right []byte) int {
	if !IsUnicodeCollation(typ.Charset) {
		return bytes.Compare(left, right)
	}
	return CompareStringValues(typ, left, right)
}

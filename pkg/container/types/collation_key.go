// Copyright 2026 Matrix Origin
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
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
)

// KeyFormat is relation/index metadata, not part of a tuple field. A missing
// format is LegacyKeyFormat even when a column has explicit collation metadata.
type KeyFormat uint8

const (
	LegacyKeyFormat KeyFormat = iota
	PADSpaceKeyV1
)

// ValidateCollationVersion validates the persisted semantic-version field
// before a plan/catalog uint32 is narrowed to the in-memory uint8 layout.
// Unknown versions must never be interpreted as the nearest known contract.
func ValidateCollationVersion(version uint32) error {
	if version > uint32(CollationVersionV1) {
		return fmt.Errorf("unsupported collation semantic version %d", version)
	}
	return nil
}

// ValidateCollationCharset validates the persisted collation identity before
// it is narrowed to the in-memory uint8 layout.
func ValidateCollationCharset(charset uint32) error {
	if charset > uint32(CharsetUTF8MB40900Bin) {
		return fmt.Errorf("unsupported collation identity %d", charset)
	}
	return nil
}

// ValidateCollationTypeMetadata validates the schema-facing portion of a
// string identity. Non-string types may only carry the legacy/binary charset
// slots and may not carry a semantic collation version.
func ValidateCollationTypeMetadata(oid T, charset, version uint32) error {
	if err := ValidateCollationCharset(charset); err != nil {
		return err
	}
	if err := ValidateCollationVersion(version); err != nil {
		return err
	}
	if !oid.IsMySQLString() && version != uint32(CollationVersionLegacy) {
		return fmt.Errorf("collation semantic version %d on non-string type %s", version, oid)
	}
	if !oid.IsMySQLString() && charset != uint32(CharsetLegacy) && charset != uint32(CharsetBinary) {
		return fmt.Errorf("collation identity %d on non-string type %s", charset, oid)
	}
	return nil
}

// ValidateKeyFormat validates the relation/index physical key format before
// it is narrowed to KeyFormat. Zero remains the legacy format for old data.
func ValidateKeyFormat(format uint32) error {
	if format > uint32(PADSpaceKeyV1) {
		return fmt.Errorf("unsupported collation key format %d", format)
	}
	return nil
}

// StringKeyPart is an immutable, schema-resolved encoder shared by stored key
// writers and query probes. Storage coercion and character-prefix extraction
// must happen before Key/Encode; they never truncate an over-width probe.
type StringKeyPart struct {
	domain collation.Domain
}

func ResolveStringKeyPart(typ Type, format KeyFormat) (StringKeyPart, error) {
	if err := ValidateKeyFormat(uint32(format)); err != nil {
		return StringKeyPart{}, err
	}
	if err := ValidateCollationTypeMetadata(typ.Oid, uint32(typ.Charset), uint32(typ.CollationVersion)); err != nil {
		return StringKeyPart{}, err
	}
	switch typ.Oid {
	case T_binary, T_varbinary, T_blob:
		return StringKeyPart{}, nil
	case T_char, T_varchar, T_text:
	default:
		return StringKeyPart{}, fmt.Errorf("not a text or binary key part: %s", typ.Oid)
	}
	if format == LegacyKeyFormat {
		return StringKeyPart{}, nil
	}
	switch typ.Charset {
	case CharsetLegacy, CharsetBinary:
		return StringKeyPart{}, nil
	case CharsetUTF8MB4Bin:
		return StringKeyPart{domain: collation.UTF8MB4Bin}, nil
	case CharsetUTF8:
		return StringKeyPart{domain: collation.UTF8MB4GeneralCI}, nil
	case CharsetUTF8MB40900AI:
		return StringKeyPart{domain: collation.UTF8MB40900AI}, nil
	case CharsetUTF8MB40900Bin:
		return StringKeyPart{domain: collation.UTF8MB40900Bin}, nil
	default:
		return StringKeyPart{}, collation.ErrDomain
	}
}

// NeedsCollationKey reports whether a text value must be represented by the
// schema-aware V1 identity for a physical key, hash or comparison probe. Native
// 0900 identities are intrinsically versioned for compatibility with the
// existing integration path; legacy general-ci and utf8mb4_bin only opt in
// when their persisted semantic version is explicit.
func NeedsCollationKey(typ Type, format KeyFormat) bool {
	if !typ.Oid.IsMySQLString() || format == LegacyKeyFormat {
		// A malformed semantic identity is still unsafe even on the legacy
		// physical path; callers with an error channel validate it explicitly.
		return ValidateCollationTypeMetadata(typ.Oid, uint32(typ.Charset), uint32(typ.CollationVersion)) != nil
	}
	if !IsNative0900Collation(typ.Charset) &&
		typ.CollationVersion == CollationVersionLegacy {
		return false
	}
	part, err := ResolveStringKeyPart(typ, format)
	if err != nil {
		// Do not downgrade an unknown identity to the raw-byte fast path. Plan
		// and execution admission report the underlying validation error before
		// a comparator/hash consumer can run.
		return true
	}
	return part.Transformed()
}

func (part StringKeyPart) Transformed() bool { return part.domain != collation.Raw }

// KeySizeUpperBound returns a conservative payload bound for one source byte
// length. It is used by execution admission before an opaque key is materialized
// and therefore does not inspect or allocate the source value.
func (part StringKeyPart) KeySizeUpperBound(inputBytes int) (int, error) {
	return part.domain.KeySizeUpperBound(inputBytes)
}

// Key returns a borrowed key; copy it before reusing scratch or the input.
// Already transformed expressions have binary type metadata and take the raw
// path. Callers must not relabel an opaque key with its source text's charset.
func (part StringKeyPart) Key(scratch, value []byte) ([]byte, error) {
	return part.domain.Key(scratch, value)
}

// CompareStringValues compares two values in the resolved schema identity.
// Callers that need an error (for example key construction) should use
// StringKeyPart.Key directly. SQL comparator interfaces return only an int;
// invalid values therefore use a deterministic byte fallback while write and
// hash paths still validate and report the original encoding error.
func CompareStringValues(typ Type, left, right []byte) int {
	if !NeedsCollationKey(typ, PADSpaceKeyV1) {
		return bytes.Compare(left, right)
	}
	part, err := ResolveStringKeyPart(typ, PADSpaceKeyV1)
	if err != nil || !part.Transformed() {
		return bytes.Compare(left, right)
	}
	leftKey, leftErr := part.Key(nil, left)
	rightKey, rightErr := part.Key(nil, right)
	if leftErr != nil || rightErr != nil {
		return bytes.Compare(left, right)
	}
	return bytes.Compare(leftKey, rightKey)
}

// Encode appends one field using the unchanged tuple string framing. It returns
// the scratch buffer for reuse. No partial field is appended on admission or
// fixed-capacity errors; an already-failed packer remains failed.
func (part StringKeyPart) Encode(p *Packer, scratch, value []byte) ([]byte, error) {
	if err := p.Err(); err != nil {
		return scratch, err
	}
	key, err := part.Key(scratch, value)
	if err != nil {
		return scratch, err
	}
	if part.Transformed() {
		scratch = key
	}
	if p.fixed {
		available := cap(p.buffer) - len(p.buffer)
		// Avoid addition overflow for arbitrarily large input.
		if available < 3 || len(key) > available-3 || bytes.Count(key, []byte{0}) > available-3-len(key) {
			return scratch, ErrPackerCapacity
		}
	}
	p.EncodeStringType(key)
	return scratch, p.Err()
}

// DecodedStringKey distinguishes opaque weights from recoverable original
// bytes. Bytes is owned by the result, independent of the input tuple buffer.
type DecodedStringKey struct {
	Bytes  []byte
	Opaque bool
}

// Decode consumes exactly one field. It reverses tuple escaping, not collation:
// a transformed value can only be returned to SQL by fetching the original row.
func (part StringKeyPart) Decode(tuple []byte) (DecodedStringKey, int, error) {
	if len(tuple) < 3 || tuple[0] != stringTypeCode || tuple[1] != bytesCode {
		return DecodedStringKey{}, 0, collation.ErrKey
	}
	key := make([]byte, 0)
	for i := 2; i < len(tuple); i++ {
		b := tuple[i]
		if b != 0 {
			key = append(key, b)
			continue
		}
		if i+1 < len(tuple) && tuple[i+1] == 0xff {
			key = append(key, 0)
			i++
			continue
		}
		if err := part.domain.ValidateKey(key); err != nil {
			return DecodedStringKey{}, 0, err
		}
		return DecodedStringKey{Bytes: key, Opaque: part.Transformed()}, i + 1, nil
	}
	return DecodedStringKey{}, 0, collation.ErrKey
}

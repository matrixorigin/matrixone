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

package collationkey

import (
	"encoding/binary"
	"errors"
	"testing"
)

// These cases exercise the wire-level rejection paths that are easy to miss
// when only round-trip examples are used.  They are part of the v2 contract:
// a malformed identity must never be accepted as a valid unique-key entry.
func TestContractBoundaryRejectsNonCanonicalPayloads(t *testing.T) {
	general, err := EncodePart(nil, Part{
		Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
		Value:  []byte("alpha"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The general-ci payload is a sequence of four-byte weights.  A declared
	// one-byte payload must therefore fail the canonical payload check.
	badLength := append([]byte(nil), general...)
	binary.BigEndian.PutUint32(badLength[19:23], 1)
	if err := ValidateEncoded(badLength); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("non-four-byte general payload = %v", err)
	}

	binKey, err := EncodePart(nil, Part{
		Domain: Domain{Type: Text, Charset: CharsetUTF8MB4Bin, Unit: PrefixCharacters},
		Value:  []byte("alpha"),
	})
	if err != nil {
		t.Fatal(err)
	}
	binKey[len(binKey)-1] = ' '
	if err := ValidateEncoded(binKey); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("utf8-bin PAD SPACE payload = %v", err)
	}

	zero, err := EncodePart(nil, Part{
		Domain: Domain{Type: Decimal, Width: 8, Scale: 2},
		Value:  []byte("0.00"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateEncoded(zero); err != nil {
		t.Fatalf("canonical decimal zero = %v", err)
	}

	decimal, err := EncodePart(nil, Part{
		Domain: Domain{Type: Decimal, Width: 8, Scale: 2},
		Value:  []byte("1.23"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The final coefficient byte is 0x7b for 123.  Replacing it with ten
	// creates a non-zero coefficient divisible by ten, which is not canonical
	// because the encoder would have reduced its scale.
	decimal[len(decimal)-1] = 10
	if err := ValidateEncoded(decimal); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("nonminimal decimal coefficient = %v", err)
	}
}

func TestContractBoundaryRejectsLocatorAndSidecarIdentityMismatches(t *testing.T) {
	key, err := EncodePart(nil, Part{
		Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
		Value:  []byte("alpha"),
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := EncodeSidecarEntry(nil, SidecarEntry{
		Key:     key,
		Locator: RowLocator{RelationID: 7, PrimaryKey: []byte("pk")},
	})
	if err != nil {
		t.Fatal(err)
	}
	locatorStart := len(entry) - locatorHeader - len("pk")
	badRelation := append([]byte(nil), entry...)
	binary.BigEndian.PutUint64(badRelation[locatorStart+5:locatorStart+13], 0)
	if _, err := DecodeSidecarEntry(badRelation); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("zero locator relation = %v", err)
	}

	badLength := append([]byte(nil), entry...)
	binary.BigEndian.PutUint32(badLength[locatorStart+21:locatorStart+25], 99)
	if _, err := DecodeSidecarEntry(badLength); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("locator length mismatch = %v", err)
	}

	badKey := append([]byte(nil), entry...)
	binary.BigEndian.PutUint32(badKey[5:9], 0)
	if _, err := DecodeSidecarEntry(badKey); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("zero sidecar key length = %v", err)
	}
}

func TestContractBoundaryRejectsInvalidMetadataAndAdmission(t *testing.T) {
	metadata := NewCollationAwareMetadataAtGeneration(2)
	badVersion := metadata
	badVersion.Version = 99
	if err := badVersion.Validate(); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("unknown relation version = %v", err)
	}
	badCapability := Capability{
		ReadableVersions:   1 << CollationAwareVersion,
		WritableVersions:   1 << CollationAwareVersion,
		RegistryVersion:    uint32(RegistryVersion),
		RegistryDigest:     []byte("wrong"),
		MaxEncodedKeyBytes: MaxKeyBytes,
	}
	if err := badCapability.Validate(); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("capability digest mismatch = %v", err)
	}
	if err := (Admission{}).Validate(metadata, true); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("missing activation admission = %v", err)
	}
}

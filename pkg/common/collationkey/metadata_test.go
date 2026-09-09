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
	"bytes"
	"errors"
	"testing"
)

func TestRelationMetadataV2RequiresExactRegistry(t *testing.T) {
	metadata := NewCollationAwareMetadata()
	if err := metadata.Validate(); err != nil {
		t.Fatalf("valid v2 metadata rejected: %v", err)
	}
	metadata.RegistryDigest[0] ^= 1
	if err := metadata.Validate(); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("digest mismatch error = %v, want ErrMalformedKey", err)
	}

	legacy := RelationMetadata{}
	if err := legacy.Validate(); err != nil {
		t.Fatalf("zero legacy metadata rejected: %v", err)
	}
	legacy.RegistryVersion = uint32(RegistryVersion)
	if err := legacy.Validate(); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("legacy with v2 fields error = %v, want ErrMalformedKey", err)
	}

	withoutGeneration := NewCollationAwareMetadataAtGeneration(0)
	if err := withoutGeneration.Validate(); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("zero activation generation error = %v, want ErrMalformedKey", err)
	}
	withGeneration := NewCollationAwareMetadataAtGeneration(9)
	if err := withGeneration.Validate(); err != nil {
		t.Fatalf("non-zero activation generation rejected: %v", err)
	}
}

func TestCapabilitySupportsReadAndWriteIndependently(t *testing.T) {
	metadata := NewCollationAwareMetadata()
	caps := Capability{
		ReadableVersions:   uint32(1) << CollationAwareVersion,
		WritableVersions:   uint32(1) << CollationAwareVersion,
		RegistryVersion:    uint32(RegistryVersion),
		RegistryDigest:     RegistryDigest(),
		MaxEncodedKeyBytes: MaxKeyBytes,
	}
	if !caps.Supports(metadata, false) || !caps.Supports(metadata, true) {
		t.Fatal("matching v2 capability did not support read and write")
	}
	caps.WritableVersions = 0
	if !caps.Supports(metadata, false) {
		t.Fatal("read capability was affected by writable bit")
	}
	if caps.Supports(metadata, true) {
		t.Fatal("missing writable bit accepted")
	}
	caps.RegistryDigest = bytes.Repeat([]byte{0}, len(caps.RegistryDigest))
	if caps.Supports(metadata, false) {
		t.Fatal("mismatched registry accepted")
	}
}

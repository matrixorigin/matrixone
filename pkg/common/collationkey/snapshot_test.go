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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func TestSidecarSnapshotWireRoundTripAndOwnership(t *testing.T) {
	metadata := NewCollationAwareMetadataAtGeneration(8)
	store, err := NewSidecarStore(77, metadata)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Begin(sidecarTestAdmission(8))
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range []string{"alpha", "beta"} {
		key, keyErr := EncodePart(nil, Part{
			Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
			Value:  []byte(value),
		})
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		if err := tx.Put(key, RowLocator{RelationID: 77, PartitionID: 3, PrimaryKey: []byte{byte(i + 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshot := store.Snapshot()
	wire, err := EncodeSidecarSnapshot(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSidecarSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.RelationID != snapshot.RelationID || decoded.Revision != snapshot.Revision ||
		!bytes.Equal(decoded.Metadata.RegistryDigest, snapshot.Metadata.RegistryDigest) ||
		len(decoded.Entries) != len(snapshot.Entries) {
		t.Fatalf("decoded snapshot differs: got=%+v want=%+v", decoded, snapshot)
	}
	restored, err := RestoreSidecarStore(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); got.Revision != snapshot.Revision || len(got.Entries) != len(snapshot.Entries) {
		t.Fatalf("restored snapshot differs: %+v", got)
	}
	decoded.Entries[0].Key[0] = 'X'
	decoded.Entries[0].Locator.PrimaryKey[0] = 'X'
	if bytes.Equal(decoded.Entries[0].Key, snapshot.Entries[0].Key) ||
		bytes.Equal(decoded.Entries[0].Locator.PrimaryKey, snapshot.Entries[0].Locator.PrimaryKey) {
		t.Fatal("decoded snapshot aliases the input snapshot")
	}
}

func TestSidecarSnapshotRejectsCorruptionAndAmbiguousOrdering(t *testing.T) {
	metadata := NewCollationAwareMetadataAtGeneration(9)
	keyA, err := EncodePart(nil, Part{
		Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
		Value:  []byte("a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := EncodePart(nil, Part{
		Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
		Value:  []byte("b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := SidecarSnapshot{
		Metadata:   metadata,
		RelationID: 99,
		Revision:   1,
		Entries: []SidecarEntry{
			{Key: keyA, Locator: RowLocator{RelationID: 99, PrimaryKey: []byte("a")}},
			{Key: keyB, Locator: RowLocator{RelationID: 99, PrimaryKey: []byte("b")}},
		},
	}
	wire, err := EncodeSidecarSnapshot(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := DecodeSidecarSnapshot(corrupt); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("checksum corruption error = %v", err)
	}
	if _, err := DecodeSidecarSnapshot(wire[:len(wire)-1]); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("truncated snapshot error = %v", err)
	}
	unsorted := snapshot
	unsorted.Entries = []SidecarEntry{snapshot.Entries[1], snapshot.Entries[0]}
	if _, err := EncodeSidecarSnapshot(nil, unsorted); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("unsorted snapshot error = %v", err)
	}

	// Recompute the checksum after changing the entry count so the decoder
	// reaches the structural count check rather than stopping at the checksum.
	badCount := append([]byte(nil), wire...)
	entryCountOffset := 5 + 4 + 4 + 4 + len(snapshot.Metadata.RegistryDigest) + 4 + 8 + 8 + 8
	binary.BigEndian.PutUint32(badCount[entryCountOffset:entryCountOffset+4], uint32(MaxSidecarSnapshotEntries)+1)
	digest := sha256.Sum256(badCount[:len(badCount)-sidecarSnapshotDigest])
	copy(badCount[len(badCount)-sidecarSnapshotDigest:], digest[:])
	if _, err := DecodeSidecarSnapshot(badCount); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("oversized entry count error = %v", err)
	}
}

func TestSidecarSnapshotRejectsInvalidDestinationAndMetadata(t *testing.T) {
	snapshot := SidecarSnapshot{
		Metadata:   RelationMetadata{},
		RelationID: 1,
	}
	if _, err := EncodeSidecarSnapshot(nil, snapshot); !errors.Is(err, ErrUnsupportedDomain) {
		t.Fatalf("legacy metadata error = %v", err)
	}
	valid := SidecarSnapshot{
		Metadata:   NewCollationAwareMetadataAtGeneration(10),
		RelationID: 1,
	}
	dst := make([]byte, MaxSidecarSnapshotBytes+1)
	if _, err := EncodeSidecarSnapshot(dst, valid); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("oversized destination error = %v", err)
	}
}

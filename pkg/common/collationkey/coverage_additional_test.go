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

// TestAdditionalMalformedWireBranches exercises recovery and sidecar guards
// that are intentionally hard to reach through the happy-path round trips.
// These cases keep the production contract fail-closed while making the
// exact nested error boundaries visible to coverage and future refactors.
func TestAdditionalMalformedWireBranches(t *testing.T) {
	t.Run("general-ci unknown BMP plane", func(t *testing.T) {
		// Plane 0x06 is deliberately not populated in the frozen table. The
		// fallback must preserve the code point rather than panic or allocate a
		// table entry at runtime.
		key, err := EncodePart(nil, Part{
			Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
			Value:  []byte("\u0600"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateEncoded(key); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("sidecar entry validates nested key", func(t *testing.T) {
		key := sidecarTestKey(t)
		wire, err := EncodeSidecarEntry(nil, SidecarEntry{
			Key:     key,
			Locator: RowLocator{RelationID: 7, PrimaryKey: []byte("pk")},
		})
		if err != nil {
			t.Fatal(err)
		}
		bad := append([]byte(nil), wire...)
		// Keep the MOKS envelope and lengths valid, but make the nested MOKY
		// envelope invalid so DecodeSidecarEntry reaches ValidateEncoded.
		bad[sidecarEntryHeader] = 'X'
		if _, err := DecodeSidecarEntry(bad); !errors.Is(err, ErrMalformedKey) {
			t.Fatalf("nested key error = %v", err)
		}
	})

	t.Run("snapshot validates nested entry", func(t *testing.T) {
		key := sidecarTestKey(t)
		snapshot := SidecarSnapshot{
			Metadata:   NewCollationAwareMetadataAtGeneration(12),
			RelationID: 12,
			Revision:   1,
			Entries: []SidecarEntry{{
				Key:     key,
				Locator: RowLocator{RelationID: 12, PrimaryKey: []byte("pk")},
			}},
		}
		wire, err := EncodeSidecarSnapshot(nil, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		bad := append([]byte(nil), wire...)
		offset := bytes.Index(bad, key)
		if offset < 0 {
			t.Fatal("encoded snapshot did not contain its key")
		}
		bad[offset] = 'X'
		digest := sha256.Sum256(bad[:len(bad)-sidecarSnapshotDigest])
		copy(bad[len(bad)-sidecarSnapshotDigest:], digest[:])
		if _, err := DecodeSidecarSnapshot(bad); !errors.Is(err, ErrMalformedKey) {
			t.Fatalf("nested entry error = %v", err)
		}
	})

	t.Run("transaction observes staged delete and conflict", func(t *testing.T) {
		store, err := NewSidecarStore(13, NewCollationAwareMetadataAtGeneration(13))
		if err != nil {
			t.Fatal(err)
		}
		key := sidecarTestKey(t)
		first := RowLocator{RelationID: 13, PrimaryKey: []byte("first")}
		second := RowLocator{RelationID: 13, PrimaryKey: []byte("second")}

		tx, err := store.Begin(sidecarTestAdmission(13))
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Put(key, first); err != nil {
			t.Fatal(err)
		}
		if err := tx.Delete(key, nil); err != nil {
			t.Fatal(err)
		}
		if _, found, err := tx.Lookup(key); err != nil || found {
			t.Fatalf("staged delete lookup = found:%v err:%v", found, err)
		}
		if err := tx.Delete(key, &second); err != nil {
			t.Fatal(err)
		}
		tx.Rollback()

		conflict, err := store.Begin(sidecarTestAdmission(13))
		if err != nil {
			t.Fatal(err)
		}
		if err := conflict.Put(key, first); err != nil {
			t.Fatal(err)
		}
		if err := conflict.Put(key, second); !errors.Is(err, ErrSidecarConflict) {
			t.Fatalf("staged locator conflict = %v", err)
		}
		conflict.Rollback()
	})

	t.Run("migration map count is bounded", func(t *testing.T) {
		gate, err := NewMigrationGate(14, 1)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := EncodeMigrationGate(nil, gate)
		if err != nil {
			t.Fatal(err)
		}
		// The first map follows the fixed gate fields and empty owner/token:
		// magic/version + eight uint64s + phase + owner length/incarnation +
		// claim-token length.
		const firstMapOffset = 5 + 8*8 + 1 + 4 + 8 + 4
		if len(wire) < firstMapOffset+4+migrationSnapshotDigest {
			t.Fatalf("migration wire too short: %d", len(wire))
		}
		bad := append([]byte(nil), wire...)
		binary.BigEndian.PutUint32(bad[firstMapOffset:firstMapOffset+4], MaxMigrationGateEntries+1)
		digest := sha256.Sum256(bad[:len(bad)-migrationSnapshotDigest])
		copy(bad[len(bad)-migrationSnapshotDigest:], digest[:])
		if _, err := DecodeMigrationGate(bad); !errors.Is(err, ErrMigrationGate) {
			t.Fatalf("oversized migration map error = %v", err)
		}
	})
}

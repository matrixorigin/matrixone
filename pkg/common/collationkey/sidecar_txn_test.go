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

func sidecarTestAdmission(generation uint64) Admission {
	metadata := NewCollationAwareMetadataAtGeneration(generation)
	capability := Capability{
		ReadableVersions:   1 << metadata.Version,
		WritableVersions:   1 << metadata.Version,
		RegistryVersion:    metadata.RegistryVersion,
		RegistryDigest:     metadata.RegistryDigest,
		MaxEncodedKeyBytes: metadata.MaxEncodedKeyBytes,
	}
	return Admission{
		Activation: Activation{
			RequestedVersion: CollationAwareVersion,
			RegistryVersion:  uint32(RegistryVersion),
			RegistryDigest:   metadata.RegistryDigest,
			Phase:            ActivationEnabled,
			Generation:       generation,
			CnTargets:        map[string]uint64{"cn-1": 7},
			TnTargets:        map[string]uint64{"tn-1": 9},
		},
		Kind: NodeCN,
		Node: NodeAcknowledgement{NodeID: "cn-1", Incarnation: 7, Capability: capability},
	}
}

func TestSidecarTxnCommitRollbackAndCopies(t *testing.T) {
	metadata := NewCollationAwareMetadataAtGeneration(3)
	store, err := NewSidecarStore(42, metadata)
	if err != nil {
		t.Fatal(err)
	}
	key := sidecarTestKey(t)
	locator := RowLocator{RelationID: 42, PartitionID: 4, PrimaryKey: []byte("pk")}
	tx, err := store.Begin(sidecarTestAdmission(3))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(key, locator); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	key[0] = 'X'
	snapshot := store.Snapshot()
	if len(snapshot.Entries) != 1 || !bytes.Equal(snapshot.Entries[0].Key, sidecarTestKey(t)) {
		t.Fatalf("snapshot did not copy key: %+v", snapshot)
	}
	snapshot.Entries[0].Locator.PrimaryKey[0] = 'X'
	readTx, err := store.Begin(sidecarTestAdmission(3))
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := readTx.Lookup(snapshot.Entries[0].Key)
	if err != nil || !ok || string(got.Locator.PrimaryKey) != "pk" {
		t.Fatalf("lookup = %+v, %v, %v", got, ok, err)
	}
	readTx.Rollback()

	rollbackTx, err := store.Begin(sidecarTestAdmission(3))
	if err != nil {
		t.Fatal(err)
	}
	if err := rollbackTx.Delete(snapshot.Entries[0].Key, &locator); err != nil {
		t.Fatal(err)
	}
	rollbackTx.Rollback()
	checkTx, err := store.Begin(sidecarTestAdmission(3))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := checkTx.Lookup(snapshot.Entries[0].Key); err != nil || !ok {
		t.Fatalf("rollback lost mapping: %v, %v", ok, err)
	}
	checkTx.Rollback()
}

func TestSidecarTxnConflictsOnSameKeyAndAllowsFreshRetry(t *testing.T) {
	metadata := NewCollationAwareMetadataAtGeneration(4)
	store, err := NewSidecarStore(9, metadata)
	if err != nil {
		t.Fatal(err)
	}
	tx1, err := store.Begin(sidecarTestAdmission(4))
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := store.Begin(sidecarTestAdmission(4))
	if err != nil {
		t.Fatal(err)
	}
	key := sidecarTestKey(t)
	if err := tx1.Put(key, RowLocator{RelationID: 9, PrimaryKey: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Put(key, RowLocator{RelationID: 9, PrimaryKey: []byte("two")}); err != nil {
		// The store is empty, so both writes can stage; the second commit is
		// expected to lose the optimistic race.
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(tx2.Commit(), ErrSidecarConflict) {
		t.Fatal("concurrent same-key commit was not fenced")
	}

	retry, err := store.Begin(sidecarTestAdmission(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := retry.Delete(key, nil); err != nil {
		t.Fatal(err)
	}
	if err := retry.Put(key, RowLocator{RelationID: 9, PrimaryKey: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := retry.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestSidecarTxnRejectsStaleGenerationAndWrongRelation(t *testing.T) {
	metadata := NewCollationAwareMetadataAtGeneration(5)
	store, err := NewSidecarStore(12, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(sidecarTestAdmission(4)); err == nil {
		t.Fatal("stale activation generation was accepted")
	}
	tx, err := store.Begin(sidecarTestAdmission(5))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(sidecarTestKey(t), RowLocator{RelationID: 13}); !errors.Is(err, ErrSidecarRelation) {
		t.Fatalf("wrong relation error = %v", err)
	}
	tx.Rollback()
}

func TestSidecarSnapshotRestoresAndRejectsMixedEntries(t *testing.T) {
	metadata := NewCollationAwareMetadataAtGeneration(6)
	store, err := NewSidecarStore(21, metadata)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Begin(sidecarTestAdmission(6))
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range []string{"a", "b"} {
		key, keyErr := EncodePart(nil, Part{
			Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters},
			Value:  []byte(value),
		})
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		if err := tx.Put(key, RowLocator{RelationID: 21, PartitionID: 1, PrimaryKey: []byte{byte(i + 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshot := store.Snapshot()
	restored, err := RestoreSidecarStore(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); got.Revision != snapshot.Revision || len(got.Entries) != 2 {
		t.Fatalf("restored snapshot = %+v", got)
	}
	bad := snapshot
	bad.Entries = append([]SidecarEntry(nil), snapshot.Entries...)
	bad.Entries[1] = bad.Entries[0]
	if _, err := RestoreSidecarStore(bad); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("duplicate snapshot error = %v", err)
	}
	bad = snapshot
	bad.RelationID++
	if _, err := RestoreSidecarStore(bad); !errors.Is(err, ErrSidecarRelation) {
		t.Fatalf("mixed-relation snapshot error = %v", err)
	}
}

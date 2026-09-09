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
	"sort"
	"sync"
)

// SidecarStore is a small storage-owner reference implementation of the
// encoded-key -> stable-locator contract.  It is intentionally independent of
// the engine: a storage adapter can use the same transaction rules while
// persisting entries in a hidden relation.  The map is not a replacement for
// that relation and is never enabled by the SQL planner on its own.
//
// Entries are addressed by the complete encoded key, never by a hash.  A
// transaction uses optimistic per-key fencing: a concurrent commit touching a
// key read or written by this transaction makes the commit fail atomically.
// This is the same safety property required of a TN sidecar writer, while
// allowing unrelated keys to commit concurrently in a real adapter.
type SidecarStore struct {
	mu         sync.RWMutex
	metadata   RelationMetadata
	relationID uint64
	revision   uint64
	entries    map[string]SidecarEntry
	keyVersion map[string]uint64
}

// SidecarSnapshot is the deterministic export form used by a checkpoint,
// clone, or recovery adapter.  Entries are sorted by their complete key bytes.
// The metadata must be persisted alongside the entries by the owning catalog.
type SidecarSnapshot struct {
	Metadata   RelationMetadata
	RelationID uint64
	Revision   uint64
	Entries    []SidecarEntry
}

var (
	ErrSidecarConflict = sidecarError("collationkey: sidecar transaction conflict")
	ErrSidecarClosed   = sidecarError("collationkey: sidecar transaction is closed")
	ErrSidecarRelation = sidecarError("collationkey: sidecar locator relation mismatch")
)

type sidecarError string

func (e sidecarError) Error() string { return string(e) }

// NewSidecarStore creates a v2-only sidecar owner.  Legacy or malformed
// metadata is rejected so an adapter cannot accidentally put bytewise and
// framed identities in the same physical relation.
func NewSidecarStore(relationID uint64, metadata RelationMetadata) (*SidecarStore, error) {
	if relationID == 0 {
		return nil, wrapCodecError(ErrInvalidValue, "sidecar relation id is zero")
	}
	if err := metadata.Validate(); err != nil {
		return nil, err
	}
	if !metadata.IsV2() {
		return nil, wrapCodecError(ErrUnsupportedDomain, "sidecar requires v2 metadata")
	}
	return &SidecarStore{
		metadata:   cloneRelationMetadata(metadata),
		relationID: relationID,
		entries:    make(map[string]SidecarEntry),
		keyVersion: make(map[string]uint64),
	}, nil
}

// Metadata returns an ownership-safe copy of the sidecar identity.
func (s *SidecarStore) Metadata() RelationMetadata {
	if s == nil {
		return RelationMetadata{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneRelationMetadata(s.metadata)
}

// Begin admits a transactional sidecar operation only when the durable
// activation and local node acknowledgement match the relation generation.
// The caller must carry the returned transaction through the same commit or
// rollback boundary as the base-table write.
func (s *SidecarStore) Begin(admission Admission) (*SidecarTxn, error) {
	if s == nil {
		return nil, ErrSidecarClosed
	}
	s.mu.RLock()
	metadata := cloneRelationMetadata(s.metadata)
	baseRevision := s.revision
	s.mu.RUnlock()
	if err := admission.Validate(metadata, true); err != nil {
		return nil, err
	}
	return &SidecarTxn{
		store:        s,
		baseRevision: baseRevision,
		writes:       make(map[string]*SidecarEntry),
		touched:      make(map[string]struct{}),
	}, nil
}

// Snapshot returns a deterministic, deep-copied view suitable for persistence
// or verification.  A snapshot is not a transaction and cannot be published
// back without the caller performing its own version check.
func (s *SidecarStore) Snapshot() SidecarSnapshot {
	if s == nil {
		return SidecarSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]SidecarEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, cloneSidecarEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	return SidecarSnapshot{
		Metadata:   cloneRelationMetadata(s.metadata),
		RelationID: s.relationID,
		Revision:   s.revision,
		Entries:    entries,
	}
}

// RestoreSidecarStore reconstructs a sidecar from a persisted snapshot.  It
// validates every entry before publishing the new map and rejects duplicate
// encoded identities or a snapshot from another relation/generation.  The
// operation is intended for checkpoint/clone recovery; it does not mutate an
// already-open store in place.
func RestoreSidecarStore(snapshot SidecarSnapshot) (*SidecarStore, error) {
	store, err := NewSidecarStore(snapshot.RelationID, snapshot.Metadata)
	if err != nil {
		return nil, err
	}
	if snapshot.Revision == 0 && len(snapshot.Entries) != 0 {
		return nil, wrapCodecError(ErrMalformedKey, "non-empty sidecar snapshot has zero revision")
	}
	var previous []byte
	for _, entry := range snapshot.Entries {
		if len(previous) != 0 && bytes.Compare(previous, entry.Key) >= 0 {
			return nil, wrapCodecError(ErrMalformedKey, "sidecar snapshot keys are not strictly ordered")
		}
		if entry.Locator.RelationID != snapshot.RelationID {
			return nil, ErrSidecarRelation
		}
		if err := ValidateEncoded(entry.Key); err != nil {
			return nil, err
		}
		if hasNull, err := HasNullPart(entry.Key); err != nil {
			return nil, err
		} else if hasNull {
			return nil, wrapCodecError(ErrUnsupportedDomain, "NULL-bearing unique key has no sidecar identity")
		}
		if _, err := EncodeLocator(nil, entry.Locator); err != nil {
			return nil, err
		}
		name := string(entry.Key)
		if _, exists := store.entries[name]; exists {
			return nil, wrapCodecError(ErrMalformedKey, "duplicate sidecar key")
		}
		store.entries[name] = cloneSidecarEntry(entry)
		store.keyVersion[name] = snapshot.Revision
		previous = append(previous[:0], entry.Key...)
	}
	store.revision = snapshot.Revision
	return store, nil
}

// SidecarTxn is an optimistic per-key transaction.  It owns all byte slices
// passed to it and returns copies, so storage callers may safely use pooled
// vectors and WAL buffers around the transaction boundary.
type SidecarTxn struct {
	store        *SidecarStore
	baseRevision uint64
	writes       map[string]*SidecarEntry
	touched      map[string]struct{}
	closed       bool
}

func (tx *SidecarTxn) ensureOpen() error {
	if tx == nil || tx.store == nil || tx.closed {
		return ErrSidecarClosed
	}
	return nil
}

func (tx *SidecarTxn) touch(key string) {
	tx.touched[key] = struct{}{}
}

// Lookup returns the transaction's staged value first, then the current
// sidecar value.  A key read after a concurrent commit is still fenced at
// Commit; it cannot silently publish against a mixed snapshot.
func (tx *SidecarTxn) Lookup(key []byte) (SidecarEntry, bool, error) {
	if err := tx.ensureOpen(); err != nil {
		return SidecarEntry{}, false, err
	}
	if err := ValidateEncoded(key); err != nil {
		return SidecarEntry{}, false, err
	}
	if hasNull, err := HasNullPart(key); err != nil {
		return SidecarEntry{}, false, err
	} else if hasNull {
		// Nullable UNIQUE parts intentionally do not participate in ordinary
		// uniqueness. Do not touch this key, otherwise concurrent NULL rows
		// would be fenced as if they shared one comparable identity.
		return SidecarEntry{}, false, nil
	}
	name := string(key)
	tx.touch(name)
	if staged, ok := tx.writes[name]; ok {
		if staged == nil {
			return SidecarEntry{}, false, nil
		}
		return cloneSidecarEntry(*staged), true, nil
	}
	tx.store.mu.RLock()
	entry, ok := tx.store.entries[name]
	tx.store.mu.RUnlock()
	if !ok {
		return SidecarEntry{}, false, nil
	}
	return cloneSidecarEntry(entry), true, nil
}

// Put stages a mapping.  Existing mappings may only be re-published for the
// same stable locator; a different locator is a unique-key conflict.  To
// replace a mapping, delete the old key in this transaction and then put the
// new one, so both changes commit atomically with the base row.
func (tx *SidecarTxn) Put(key []byte, locator RowLocator) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if err := ValidateEncoded(key); err != nil {
		return err
	}
	if hasNull, err := HasNullPart(key); err != nil {
		return err
	} else if hasNull {
		// The base row still carries the original NULL value, but no sidecar
		// mapping is needed (or allowed) for a NULL-bearing UNIQUE identity.
		return nil
	}
	if locator.RelationID != tx.store.relationID {
		return ErrSidecarRelation
	}
	if _, err := EncodeLocator(nil, locator); err != nil {
		return err
	}
	name := string(key)
	tx.touch(name)
	if staged, ok := tx.writes[name]; ok && staged != nil && !sameLocator(staged.Locator, locator) {
		return ErrSidecarConflict
	}
	if staged, ok := tx.writes[name]; !ok || staged == nil {
		tx.store.mu.RLock()
		current, exists := tx.store.entries[name]
		tx.store.mu.RUnlock()
		if exists && !sameLocator(current.Locator, locator) {
			return ErrSidecarConflict
		}
	}
	entry := SidecarEntry{Key: append([]byte(nil), key...), Locator: cloneLocator(locator)}
	tx.writes[name] = &entry
	return nil
}

// Delete stages removal.  When expected is non-nil, the mapping must belong
// to that locator; this prevents a stale update from deleting a newer row.
// Deleting an already absent key is idempotent.
func (tx *SidecarTxn) Delete(key []byte, expected *RowLocator) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if err := ValidateEncoded(key); err != nil {
		return err
	}
	if hasNull, err := HasNullPart(key); err != nil {
		return err
	} else if hasNull {
		return nil
	}
	name := string(key)
	tx.touch(name)
	var current SidecarEntry
	var exists bool
	if staged, ok := tx.writes[name]; ok {
		if staged != nil {
			current, exists = cloneSidecarEntry(*staged), true
		}
	} else {
		tx.store.mu.RLock()
		current, exists = tx.store.entries[name]
		tx.store.mu.RUnlock()
	}
	if expected != nil && exists && !sameLocator(current.Locator, *expected) {
		return ErrSidecarConflict
	}
	tx.writes[name] = nil
	return nil
}

// Commit atomically applies all staged key mappings.  A conflict closes the
// transaction without applying any write; callers must retry from a fresh
// snapshot and transaction.
func (tx *SidecarTxn) Commit() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	defer func() { tx.closed = true }()
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	for key := range tx.touched {
		if tx.store.keyVersion[key] > tx.baseRevision {
			return ErrSidecarConflict
		}
	}
	if len(tx.writes) == 0 {
		return nil
	}
	tx.store.revision++
	commitRevision := tx.store.revision
	for key, entry := range tx.writes {
		if entry == nil {
			delete(tx.store.entries, key)
		} else {
			tx.store.entries[key] = cloneSidecarEntry(*entry)
		}
		tx.store.keyVersion[key] = commitRevision
	}
	return nil
}

// Rollback discards staged changes.  It is safe to call more than once.
func (tx *SidecarTxn) Rollback() {
	if tx == nil || tx.closed {
		return
	}
	tx.closed = true
	tx.writes = nil
	tx.touched = nil
}

func sameLocator(left, right RowLocator) bool {
	return left.RelationID == right.RelationID && left.PartitionID == right.PartitionID &&
		bytes.Equal(left.PrimaryKey, right.PrimaryKey)
}

func cloneLocator(locator RowLocator) RowLocator {
	return RowLocator{
		RelationID:  locator.RelationID,
		PartitionID: locator.PartitionID,
		PrimaryKey:  append([]byte(nil), locator.PrimaryKey...),
	}
}

func cloneSidecarEntry(entry SidecarEntry) SidecarEntry {
	return SidecarEntry{Key: append([]byte(nil), entry.Key...), Locator: cloneLocator(entry.Locator)}
}

func cloneRelationMetadata(metadata RelationMetadata) RelationMetadata {
	metadata.RegistryDigest = append([]byte(nil), metadata.RegistryDigest...)
	return metadata
}
